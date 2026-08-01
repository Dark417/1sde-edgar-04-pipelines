"""Feature F-6: ``silver.filing``.

Contains the idempotency test that decides whether anything downstream is trustworthy.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Any

import pytest

from pipelines.bronze import filing_index
from pipelines.config import Settings
from pipelines.silver import filing
from pipelines.silver.common import (
    base_form_type,
    is_amendment,
    normalize_accession,
    normalize_form_type,
    pad_cik,
    parse_edgar_date,
)

from .conftest import envelope, index_payload

pytestmark = pytest.mark.spark


def _one_col(spark: Any, value: Any, column: Any, dtype: str = "STRING") -> Any:
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(value,)], f"v {dtype}")
    return df.select(column(F.col("v")).alias("out")).collect()[0]["out"]


# ------------------------------------------------------------------- normalizers


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("0001234567-26-000001", "0001234567-26-000001"),
        ("000123456726000001", "0001234567-26-000001"),
        ("  0001234567-26-000001  ", "0001234567-26-000001"),
        ("NOT-AN-ACCESSION", None),
        ("123", None),
    ],
)
def test_normalize_accession(spark: Any, raw: str, expected: str | None) -> None:
    assert _one_col(spark, raw, normalize_accession) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("320193", "0000320193"), ("0000320193", "0000320193"), ("abc", None), ("", None)],
)
def test_pad_cik(spark: Any, raw: str, expected: str | None) -> None:
    assert _one_col(spark, raw, pad_cik) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("10-K", "10-K"),
        ("10-K/A", "10-K"),
        ("8-K", "8-K"),
        ("S-1/A", "S-1"),
        ("10-k/a", "10-K"),
    ],
)
def test_base_form_type_covers_the_named_cases(spark: Any, raw: str, expected: str) -> None:
    """AGENTS.md F-6 names these exact inputs, lowercase included."""
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(raw,)], "v STRING")
    normalized = normalize_form_type(F.col("v"))
    row = df.select(
        base_form_type(normalized).alias("base"), is_amendment(normalized).alias("amend")
    ).collect()[0]
    assert row["base"] == expected
    assert row["amend"] is raw.upper().endswith("/A")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("20260731", date(2026, 7, 31)), ("2026-07-31", date(2026, 7, 31)), ("not a date", None)],
)
def test_parse_edgar_date(spark: Any, raw: str, expected: date | None) -> None:
    assert _one_col(spark, raw, parse_edgar_date) == expected


# ------------------------------------------------------------------------ pipeline


def _ingest(spark: Any, settings: Settings, landing: Any, run: Any, payloads: list[dict]) -> None:
    landing.write(
        "filing_index",
        [envelope("filing_index", p["accession_number"], p) for p in payloads],
    )
    filing_index.ingest(spark, settings, run)


def test_silver_filing_run_twice_is_identical(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """🔴 The single most important test in the project.

    Identical row count *and* identical _first_seen_ts. If the count grows, the MERGE
    key is wrong and everything downstream is built on sand.
    """
    _ingest(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            index_payload("0001234567-26-000001", "1234567"),
            index_payload("0001234567-26-000002", "1234567", form_type="10-K/A"),
        ],
    )
    target = settings.table("edgar.silver.filing")

    filing.run(spark, settings, job_run_ctx)
    first = {r["accession_number"]: r["_first_seen_ts"] for r in spark.table(target).collect()}

    filing.run(spark, settings, job_run_ctx)
    second = {r["accession_number"]: r["_first_seen_ts"] for r in spark.table(target).collect()}

    assert len(first) == 2
    assert first == second
    assert spark.table(target).select("_first_seen_ts").distinct().count() <= 2


def test_malformed_accession_lands_in_quarantine_not_filing(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    _ingest(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            index_payload("0001234567-26-000001", "1234567"),
            index_payload("NOT-AN-ACCESSION", "1234567", company_name="BAD FILER"),
        ],
    )
    filing.run(spark, settings, job_run_ctx)

    assert spark.table(settings.table("edgar.silver.filing")).count() == 1
    quarantine = spark.table(settings.table("edgar.silver.filing_quarantine")).collect()
    assert len(quarantine) == 1
    assert quarantine[0]["_dq_check_name"] == "filing_accession_format"
    assert quarantine[0]["_source_table"] == "edgar.silver.filing"
    assert "BAD FILER" in quarantine[0]["record_json"]


def test_quarantine_does_not_double_on_replay(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """Quarantine is part of silver, so "run it twice" has to hold for the rejected
    rows too -- otherwise a replay doubles the count and every "how bad is the data"
    number goes with it."""
    _ingest(
        spark,
        settings,
        landing,
        job_run_ctx,
        [index_payload("NOT-AN-ACCESSION", "1234567", company_name="BAD FILER")],
    )
    quarantine = settings.table("edgar.silver.filing_quarantine")
    filing.run(spark, settings, job_run_ctx)
    filing.run(spark, settings, job_run_ctx)
    filing.run(spark, replace(settings, logical_date="2026-08-01"), job_run_ctx)
    assert spark.table(quarantine).count() == 1


def test_normalization_reaches_the_table(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    _ingest(
        spark,
        settings,
        landing,
        job_run_ctx,
        [index_payload("0001234567-26-000003", "1234567", form_type="10-k/a")],
    )
    filing.run(spark, settings, job_run_ctx)
    row = spark.table(settings.table("edgar.silver.filing")).collect()[0]
    assert row["cik"] == "0001234567"
    assert row["form_type"] == "10-K/A"
    assert row["base_form_type"] == "10-K"
    assert row["is_amendment"] is True
    assert row["filed_date"] == date(2026, 7, 31)
    assert row["primary_doc_url"].startswith("https://www.sec.gov/Archives/")


def test_the_same_filing_in_two_daily_indexes_produces_one_row(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    payload = index_payload("0001234567-26-000004", "1234567")
    landing.write("filing_index", [envelope("filing_index", "a", payload)])
    landing.write(
        "filing_index",
        [envelope("filing_index", "a", payload, logical_date="2026-07-30")],
        logical_date="2026-07-30",
    )
    filing_index.ingest(spark, settings, job_run_ctx)
    filing.run(spark, settings, job_run_ctx)
    assert spark.table(settings.table("edgar.silver.filing")).count() == 1


def test_a_future_filed_date_is_quarantined(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """Compared against logical_date, not current_date, so replay is deterministic."""
    _ingest(
        spark,
        settings,
        landing,
        job_run_ctx,
        [index_payload("0001234567-26-000005", "1234567", date_filed="20991231")],
    )
    filing.run(spark, settings, job_run_ctx)
    assert spark.table(settings.table("edgar.silver.filing")).count() == 0
    row = spark.table(settings.table("edgar.silver.filing_quarantine")).collect()[0]
    assert row["_dq_check_name"] == "filing_filed_date_not_after_logical_date"
