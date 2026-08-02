"""Feature F-8: ``silver.financial_fact``.

Contains the test that decides whether restatement detection is possible at all.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from pipelines.bronze import company_concept
from pipelines.config import Settings
from pipelines.silver import financial_fact

from .conftest import concept_payload, envelope, fact

pytestmark = pytest.mark.spark


def _land(
    spark: Any,
    settings: Settings,
    landing: Any,
    run: Any,
    payloads: list[dict],
    *,
    part: str = "part-00000",
) -> None:
    landing.write(
        "company_concept",
        [envelope("company_concept", f"{p['cik']:010d}/us-gaap/{p['tag']}", p) for p in payloads],
        part=part,
    )
    company_concept.ingest(spark, settings, run)


def test_two_accessions_for_one_period_produce_two_rows(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """🔴 If this collapses to one row, restatement detection is impossible and there
    is no point continuing. The obvious dedup is the bug."""
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            concept_payload(
                "1234567",
                "Revenues",
                {
                    "USD": [
                        fact(
                            start="2025-01-01",
                            end="2025-12-31",
                            val=1_000_000,
                            accn="0001234567-26-000001",
                            filed="2026-03-01",
                        ),
                        fact(
                            start="2025-01-01",
                            end="2025-12-31",
                            val=1_100_000,
                            accn="0001234567-26-000002",
                            filed="2026-06-01",
                            form="10-K/A",
                        ),
                    ]
                },
            )
        ],
    )
    financial_fact.run(spark, settings, job_run_ctx)
    rows = spark.table(settings.table("edgar.silver.financial_fact")).collect()
    assert len(rows) == 2
    assert {r["accession_number"] for r in rows} == {
        "0001234567-26-000001",
        "0001234567-26-000002",
    }


def test_instant_facts_survive_the_period_order_check(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """`period_start IS NULL OR ...` is load-bearing, not defensive."""
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            concept_payload(
                "1234567",
                "Assets",
                {
                    "USD": [
                        fact(
                            start=None,
                            end="2025-12-31",
                            val=5_000_000,
                            accn="0001234567-26-000001",
                            filed="2026-03-01",
                        )
                    ]
                },
            )
        ],
    )
    financial_fact.run(spark, settings, job_run_ctx)
    rows = spark.table(settings.table("edgar.silver.financial_fact")).collect()
    assert spark.table(settings.table("edgar.silver.financial_fact_quarantine")).count() == 0
    assert len(rows) == 1
    assert rows[0]["period_start"] is None
    assert rows[0]["period_type"] == "instant"
    assert rows[0]["period_end"] == date(2025, 12, 31)


def test_units_map_is_exploded_per_unit(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            concept_payload(
                "1234567",
                "EarningsPerShareBasic",
                {
                    "USD/shares": [
                        fact(
                            start="2025-01-01",
                            end="2025-12-31",
                            val=1.25,
                            accn="0001234567-26-000001",
                            filed="2026-03-01",
                        )
                    ],
                    "USD": [
                        fact(
                            start="2025-01-01",
                            end="2025-12-31",
                            val=1.25,
                            accn="0001234567-26-000001",
                            filed="2026-03-01",
                        )
                    ],
                },
            )
        ],
    )
    financial_fact.run(spark, settings, job_run_ctx)
    rows = spark.table(settings.table("edgar.silver.financial_fact")).collect()
    assert {r["unit"] for r in rows} == {"USD", "USD/shares"}


def test_unmapped_tags_are_kept_with_a_null_canonical(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """Dropping them would make adding a concept later require a bronze replay."""
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            concept_payload(
                "1234567",
                "SomeTagWeHaveNotMappedYet",
                {
                    "USD": [
                        fact(
                            start="2025-01-01",
                            end="2025-12-31",
                            val=7.0,
                            accn="0001234567-26-000001",
                            filed="2026-03-01",
                        )
                    ]
                },
            )
        ],
    )
    financial_fact.run(spark, settings, job_run_ctx)
    row = spark.table(settings.table("edgar.silver.financial_fact")).collect()[0]
    assert row["concept_canonical"] is None
    assert row["concept_tag"] == "SomeTagWeHaveNotMappedYet"
    assert job_run_ctx.metrics["silver.financial_fact.dq.fact_concept_mapped.failed"] == 1


def test_decimals_is_null_because_the_api_does_not_return_it(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """Documented in ADR-002: the column exists for a future raw-XBRL ingest path."""
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            concept_payload(
                "1234567",
                "Revenues",
                {
                    "USD": [
                        fact(
                            start="2025-01-01",
                            end="2025-12-31",
                            val=1.0,
                            accn="0001234567-26-000001",
                            filed="2026-03-01",
                        )
                    ]
                },
            )
        ],
    )
    financial_fact.run(spark, settings, job_run_ctx)
    assert (
        spark.table(settings.table("edgar.silver.financial_fact")).collect()[0]["decimals"] is None
    )


def test_running_twice_is_idempotent(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            concept_payload(
                "1234567",
                "Revenues",
                {
                    "USD": [
                        fact(
                            start="2025-01-01",
                            end="2025-12-31",
                            val=1_000_000,
                            accn="0001234567-26-000001",
                            filed="2026-03-01",
                        )
                    ]
                },
            )
        ],
    )
    target = settings.table("edgar.silver.financial_fact")
    financial_fact.run(spark, settings, job_run_ctx)
    first = {r["accession_number"]: r["_first_seen_ts"] for r in spark.table(target).collect()}
    financial_fact.run(spark, settings, job_run_ctx)
    second = {r["accession_number"]: r["_first_seen_ts"] for r in spark.table(target).collect()}
    assert first == second


def test_malformed_accession_is_quarantined(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """A malformed accession would silently merge two assertions and hide a
    restatement, so it is a reject rather than a repair."""
    _land(
        spark,
        settings,
        landing,
        job_run_ctx,
        [
            concept_payload(
                "1234567",
                "Revenues",
                {
                    "USD": [
                        fact(
                            start="2025-01-01",
                            end="2025-12-31",
                            val=1.0,
                            accn="GARBAGE",
                            filed="2026-03-01",
                        )
                    ]
                },
            )
        ],
    )
    financial_fact.run(spark, settings, job_run_ctx)
    assert spark.table(settings.table("edgar.silver.financial_fact")).count() == 0
    row = spark.table(settings.table("edgar.silver.financial_fact_quarantine")).collect()[0]
    assert row["_dq_check_name"] == "fact_accession_format"
