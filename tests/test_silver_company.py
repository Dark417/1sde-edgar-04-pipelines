"""Feature F-7: ``silver.company`` -- SCD-2 against real-shaped submissions payloads."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Any

import pytest

from pipelines.bronze import company_submissions
from pipelines.config import Settings
from pipelines.framework.dq import DQBatchFailure
from pipelines.silver import company

from .conftest import envelope, submissions_payload

pytestmark = pytest.mark.spark


def _land(
    spark: Any, settings: Settings, landing: Any, run: Any, payloads: list[dict], *, part: str
) -> None:
    landing.write(
        "company_submissions",
        [envelope("company_submissions", p["cik"], p) for p in payloads],
        part=part,
    )
    company_submissions.ingest(spark, settings, run)


def test_payload_is_parsed_into_the_dimension(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [submissions_payload("0000320193", "Apple Inc.", tickers=["AAPL"], exchanges=["Nasdaq"])],
        part="part-00000",
    )
    company.run(spark, settings, job_run_ctx)
    row = spark.table(settings.table("edgar.silver.company")).collect()[0]
    assert row["cik"] == "0000320193"
    assert row["company_name"] == "Apple Inc."
    assert row["tickers"] == ["AAPL"]
    assert row["is_current"] is True
    assert row["valid_from"] == date(2026, 7, 31)
    assert row["valid_to"] is None
    assert row["_hash_diff"]


def test_scd2_over_real_payloads_no_change_then_change(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """F-4's four cases, re-run against fixture payloads as F-7 requires."""
    target = settings.table("edgar.silver.company")
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            submissions_payload(
                "0000320193", "Apple Inc.", tickers=["AAPL", "APC"], exchanges=["Nasdaq", "NYSE"]
            )
        ],
        part="part-00000",
    )
    company.run(spark, settings, job_run_ctx)
    assert spark.table(target).count() == 1

    # (a) run again with identical content -> no new version
    company.run(spark, settings, job_run_ctx)
    assert spark.table(target).count() == 1

    # (c) array members reordered -> no new version. This is the one that keeps the
    # dimension from growing by a version a day forever.
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            submissions_payload(
                "0000320193", "Apple Inc.", tickers=["APC", "AAPL"], exchanges=["NYSE", "Nasdaq"]
            )
        ],
        part="part-00001",
    )
    company.run(spark, settings, job_run_ctx)
    assert spark.table(target).count() == 1

    # (b) tracked column changed -> old closed, new inserted
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            submissions_payload(
                "0000320193",
                "Apple Computer Inc.",
                tickers=["APC", "AAPL"],
                exchanges=["NYSE", "Nasdaq"],
            )
        ],
        part="part-00002",
    )
    settings_next_day = replace(settings, logical_date="2026-08-05")
    company.run(spark, settings_next_day, job_run_ctx)
    rows = sorted(spark.table(target).collect(), key=lambda r: r["valid_from"])
    assert len(rows) == 2
    assert rows[0]["is_current"] is False
    assert rows[0]["valid_to"] == date(2026, 8, 4)
    assert rows[1]["is_current"] is True


def test_bad_cik_is_quarantined(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    payload = submissions_payload("0000320193", "Apple Inc.")
    payload["cik"] = "not-a-cik"
    _land(spark, settings, landing, job_run_ctx, [payload], part="part-00000")
    company.run(spark, settings, job_run_ctx)
    assert spark.table(settings.table("edgar.silver.company")).count() == 0
    row = spark.table(settings.table("edgar.silver.company_quarantine")).collect()[0]
    assert row["_dq_check_name"] == "company_cik_zero_padded"


def test_two_current_rows_fail_the_batch_and_the_table_is_restored(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """The SCD-2 invariant is reject_batch: one bad row fans out every downstream join.

    Because the invariant can only be evaluated after the merge, the failure path also
    has to undo it -- otherwise "the batch is abandoned" is a slogan, not behavior.
    """
    target = settings.table("edgar.silver.company")
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [submissions_payload("0000320193", "Apple Inc.")],
        part="part-00000",
    )
    company.run(spark, settings, job_run_ctx)
    before = spark.table(target).count()

    # Corrupt the dimension the way a bad merge would: a second current row.
    #
    # The projection is derived from the live schema rather than spelled out. It used to
    # be a hand-written column list, which broke the moment contracts v1.1.0 added
    # company_sk and version_number -- the test failed on INSERT arity while asserting
    # nothing about the invariant it exists to cover. A fixture that has to be edited
    # every time a column is added will eventually be edited wrongly.
    def _clone(**overrides: str) -> str:
        return ", ".join(overrides.get(c, f"`{c}`") for c in spark.table(target).columns)

    spark.sql(f"INSERT INTO {target} SELECT {_clone(cik=chr(39) + 'ZZZZ' + chr(39))} FROM {target}")
    spark.sql(
        f"INSERT INTO {target} "
        f"SELECT {_clone(cik=chr(39) + 'ZZZZ' + chr(39), _hash_diff=chr(39) + 'other-hash' + chr(39))} "
        f"FROM {target} WHERE cik = 'ZZZZ'"
    )
    corrupted = spark.table(target).count()
    assert corrupted == before + 2

    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [submissions_payload("0000320193", "Apple Inc.")],
        part="part-00003",
    )
    with pytest.raises(DQBatchFailure, match="company_exactly_one_current"):
        company.run(spark, settings, job_run_ctx)

    # Restored to the pre-merge version: the merge's effects are gone.
    assert spark.table(target).count() == corrupted


def test_invariant_query_reports_one_current_per_cik(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            submissions_payload("0000320193", "Apple Inc."),
            submissions_payload("0000789019", "Microsoft"),
        ],
        part="part-00000",
    )
    company.run(spark, settings, job_run_ctx)
    rows = company.scd2_invariants(spark, settings.table("edgar.silver.company")).collect()
    assert {r["cik"] for r in rows} == {"0000320193", "0000789019"}
    assert all(r["current_count"] == 1 and r["overlap_count"] == 0 for r in rows)
