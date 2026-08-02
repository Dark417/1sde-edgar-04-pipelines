"""Feature F-10: the remaining gold marts and the serving export."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pipelines.config import Settings
from pipelines.export import serving
from pipelines.gold import company_profile, filing_activity_daily, financials_current

pytestmark = pytest.mark.spark

FACT_DDL = (
    "cik STRING, concept_canonical STRING, unit STRING, period_start DATE, period_end DATE, "
    "period_type STRING, accession_number STRING, value DECIMAL(38,6), decimals INT, "
    "fiscal_year INT, fiscal_period STRING, form_type STRING, filed_date DATE"
)

# `is_current` is required because silver.filing became SCD-2 in contracts v1.1.0 and the
# gold builders now filter on it. These frames stand in for the current version of each
# filing, so it is always true here; the SCD-2 behaviour itself is covered in
# tests/test_framework_merge.py rather than re-tested through gold.
FILING_DDL = (
    "accession_number STRING, cik STRING, company_name STRING, form_type STRING, "
    "base_form_type STRING, is_amendment BOOLEAN, filed_date DATE, is_current BOOLEAN"
)


def _facts(spark: Any) -> Any:
    return spark.createDataFrame(
        [
            (
                "0001234567",
                "revenue_total",
                "USD",
                date(2025, 1, 1),
                date(2025, 12, 31),
                "duration",
                accn,
                Decimal(str(value)),
                None,
                2025,
                "FY",
                form,
                filed,
            )
            for accn, filed, value, form in [
                ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "10-K"),
                ("0001234567-26-000002", date(2026, 6, 1), 1_100_000, "10-K/A"),
            ]
        ],
        FACT_DDL,
    )


def test_financials_current_keeps_only_the_latest_assertion(spark: Any) -> None:
    rows = financials_current.build(_facts(spark)).collect()
    assert len(rows) == 1
    assert rows[0]["accession_number"] == "0001234567-26-000002"
    assert rows[0]["value"] == Decimal("1100000.000000")
    assert rows[0]["assertion_count"] == 2
    assert rows[0]["was_restated"] is False


def test_financials_current_same_day_tie_is_broken_deterministically(spark: Any) -> None:
    """Two accessions filed the same day is ordinary. Without a total ordering the
    winner would depend on scan order."""
    df = spark.createDataFrame(
        [
            (
                "0001234567",
                "revenue_total",
                "USD",
                date(2025, 1, 1),
                date(2025, 12, 31),
                "duration",
                accn,
                Decimal("1"),
                None,
                2025,
                "FY",
                "10-K",
                date(2026, 3, 1),
            )
            for accn in ("0001234567-26-000001", "0001234567-26-000002")
        ],
        FACT_DDL,
    )
    winners = {financials_current.build(df).collect()[0]["accession_number"] for _ in range(3)}
    assert winners == {"0001234567-26-000002"}


def test_filing_activity_groups_amendments_with_their_base_form(spark: Any) -> None:
    filings = spark.createDataFrame(
        [
            ("a", "0000000001", "A", "10-K", "10-K", False, date(2026, 7, 31), True),
            ("b", "0000000002", "B", "10-K/A", "10-K", True, date(2026, 7, 31), True),
            ("c", "0000000001", "A", "8-K", "8-K", False, date(2026, 7, 31), True),
            # A superseded version of "a". Gold must not count it: silver.filing holds
            # one row per version now, so an unfiltered count inflates exactly on the
            # days these marts exist to explain.
            ("a", "0000000001", "A", "10-K", "10-K", False, date(2026, 7, 31), False),
        ],
        FILING_DDL,
    )
    rows = {r["base_form_type"]: r for r in filing_activity_daily.build(filings).collect()}
    assert rows["10-K"]["filing_count"] == 2
    assert rows["10-K"]["amendment_count"] == 1
    assert rows["10-K"]["distinct_cik_count"] == 2
    assert rows["8-K"]["amendment_count"] == 0


def test_company_profile_keeps_companies_with_no_filings(spark: Any) -> None:
    """An inner join would make the company disappear, and a missing company is a much
    harder bug to notice than a zero count."""
    companies = spark.createDataFrame(
        [("0000000001", "A", True), ("0000000002", "B", True)],
        "cik STRING, company_name STRING, is_current BOOLEAN",
    )
    filings = spark.createDataFrame(
        [("a", "0000000001", "A", "10-K", "10-K", False, date(2026, 7, 31))], FILING_DDL
    )
    restatements = spark.createDataFrame([], "cik STRING")
    rows = {r["cik"]: r for r in company_profile.build(companies, filings, restatements).collect()}
    assert set(rows) == {"0000000001", "0000000002"}
    assert rows["0000000002"]["filing_count"] == 0
    assert rows["0000000002"]["restatement_count"] == 0
    assert rows["0000000001"]["first_filed_date"] == date(2026, 7, 31)


# ------------------------------------------------------------------------- export


def _seed_gold(spark: Any, settings: Settings) -> None:
    spark.sql(
        f"INSERT INTO {settings.table('edgar.silver.filing')} VALUES "
        "('0001234567-26-000001', '0001234567', 'A', '10-K', '10-K', false, DATE'2026-07-31', "
        "NULL, DATE'2026-07-31', current_timestamp(), current_timestamp(), 'b', 'f')"
    )
    spark.sql(
        f"INSERT INTO {settings.table('edgar.gold.filing_activity_daily')} VALUES "
        "(DATE'2026-07-31', '10-K', 1, 0, 1, current_timestamp(), 'run')"
    )


def test_export_writes_one_object_per_table_and_a_manifest(
    spark: Any, settings: Settings, job_run_ctx: Any
) -> None:
    _seed_gold(spark, settings)
    manifest = serving.export_all(spark, settings, job_run_ctx)

    root = Path(settings.export_root)
    assert {t.name for t in manifest.tables} == {
        "financials_current",
        "restatement_event",
        "filing_activity_daily",
        "company_profile",
    }
    for table in manifest.tables:
        path = root / table.path
        assert path.is_file(), path
        assert path.name == "data.parquet"
        assert len(table.sha256) == 64
        assert table.bytes > 0
    # Staging is cleaned up; only v1/ survives.
    assert not (root / "_staging").exists()

    written = json.loads((root / "v1" / "_manifest.json").read_text())
    assert written["manifest_version"] == "1"
    assert written["gold_max_filed_date"] == "2026-07-31"
    assert written["logical_date"] == settings.logical_date


def test_manifest_freshness_comes_from_silver_not_from_the_export(
    spark: Any, settings: Settings, job_run_ctx: Any
) -> None:
    """An export timestamp here would claim the data is fresh on every run, including
    the runs that ingested nothing."""
    _seed_gold(spark, settings)
    manifest = serving.export_all(spark, settings, job_run_ctx)
    expected = (
        spark.table(settings.table("edgar.silver.filing"))
        .selectExpr("max(filed_date) AS m")
        .collect()[0]["m"]
    )
    assert manifest.gold_max_filed_date == str(expected)


def test_export_is_idempotent(spark: Any, settings: Settings, job_run_ctx: Any) -> None:
    _seed_gold(spark, settings)
    first = serving.export_all(spark, settings, job_run_ctx)
    second = serving.export_all(spark, settings, job_run_ctx)
    assert [(t.name, t.row_count) for t in first.tables] == [
        (t.name, t.row_count) for t in second.tables
    ]
    parquet_files = list((Path(settings.export_root) / "v1").rglob("*.parquet"))
    assert len(parquet_files) == 4


def test_manifest_table_order_is_fixed(spark: Any, settings: Settings, job_run_ctx: Any) -> None:
    """Identical data must produce an identical manifest."""
    _seed_gold(spark, settings)
    manifest = serving.export_all(spark, settings, job_run_ctx)
    assert [t.name for t in manifest.tables] == [
        "financials_current",
        "restatement_event",
        "filing_activity_daily",
        "company_profile",
    ]
