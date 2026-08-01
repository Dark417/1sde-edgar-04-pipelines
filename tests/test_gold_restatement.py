"""Feature F-9: ``gold.restatement_event`` -- the differentiator.

A restatement table that flags everything is worse than no table, so most of these
tests are about what must **not** appear.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import functions as F

from pipelines.gold.restatement_event import RestatementTolerance, build

pytestmark = pytest.mark.spark

FACT_DDL = (
    "cik STRING, taxonomy STRING, concept_tag STRING, concept_canonical STRING, unit STRING, "
    "period_start DATE, period_end DATE, period_type STRING, accession_number STRING, "
    "value DECIMAL(38,6), decimals INT, form_type STRING, filed_date DATE"
)


def _facts(spark: Any, rows: list[tuple]) -> Any:
    """rows: (accn, filed, value, unit, decimals, form, period_start, period_end)"""
    return spark.createDataFrame(
        [
            (
                "0001234567",
                "us-gaap",
                "Revenues",
                "revenue_total",
                unit,
                period_start,
                period_end,
                "duration",
                accn,
                Decimal(str(value)),
                decimals,
                form,
                filed,
            )
            for accn, filed, value, unit, decimals, form, period_start, period_end in rows
        ],
        FACT_DDL,
    )


FY25 = (date(2025, 1, 1), date(2025, 12, 31))


def test_a_real_restatement_produces_exactly_one_row(spark: Any) -> None:
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 1_100_000, "USD", None, "10-K/A", *FY25),
        ],
    )
    rows = build(facts).collect()
    assert len(rows) == 1
    event = rows[0]
    assert event["original_accession_number"] == "0001234567-26-000001"
    assert event["restated_accession_number"] == "0001234567-26-000002"
    assert event["original_value"] == Decimal("1000000.000000")
    assert event["restated_value"] == Decimal("1100000.000000")
    assert event["delta_abs"] == Decimal("100000.000000")
    assert event["delta_pct"] == pytest.approx(0.1)
    assert event["days_to_restatement"] == 92
    assert event["materiality_band"] == "material"


def test_rounding_only_difference_produces_zero_rows_with_explicit_decimals(spark: Any) -> None:
    """🔴 The test the flagship feature lives or dies on."""
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_234_567, "USD", 0, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 1_235_000, "USD", -3, "10-K/A", *FY25),
        ],
    )
    assert build(facts).count() == 0


def test_rounding_only_difference_produces_zero_rows_without_decimals(spark: Any) -> None:
    """🔴 The same case as it actually arrives from EDGAR.

    These are the real values Dream Finders Homes (CIK 0001825088) reported for FY2020
    NetIncomeLoss: 79,093,455 in the original 10-K and 79,093,000 in a later one. The
    companyconcept API returns no `decimals`, so the precision has to be inferred --
    see ADR-002.
    """
    facts = _facts(
        spark,
        [
            ("0001234567-22-000001", date(2022, 3, 16), 79_093_455, "USD", None, "10-K", *FY25),
            ("0001234567-23-000011", date(2023, 3, 2), 79_093_000, "USD", None, "10-K", *FY25),
        ],
    )
    assert build(facts).count() == 0


def test_the_literal_rule_six_expression_flags_that_same_rounding(spark: Any) -> None:
    """Documents the deviation in ADR-002 rather than hiding it.

    `abs(a-b) > greatest(abs(a)*1e-6, 1e-6)` alone flags 79,093,455 -> 79,093,000: the
    relative difference is 5.75e-6, nearly six times the tolerance. Keeping this path
    executable is what makes the deviation reviewable.
    """
    facts = _facts(
        spark,
        [
            ("0001234567-22-000001", date(2022, 3, 16), 79_093_455, "USD", None, "10-K", *FY25),
            ("0001234567-23-000011", date(2023, 3, 2), 79_093_000, "USD", None, "10-K", *FY25),
        ],
    )
    literal = RestatementTolerance(decimals_aware=False)
    assert build(facts, tolerance=literal).count() == 1


def test_truncation_to_a_coarser_scale_produces_zero_rows(spark: Any) -> None:
    """🔴 Also from live EDGAR: Dream Finders reported 44,694,524 and later 44,694,000.

    Correct rounding to thousands would be 44,695,000 -- the filer *truncated*. So the
    floor is a whole reporting unit, not half of one; a half-unit floor catches the
    rounders and flags the truncators as restatements.
    """
    facts = _facts(
        spark,
        [
            ("0001234567-22-000001", date(2022, 3, 16), 44_694_524, "USD", None, "10-K", *FY25),
            ("0001234567-23-000011", date(2023, 3, 2), 44_694_000, "USD", None, "10-K", *FY25),
        ],
    )
    assert build(facts).count() == 0


def test_a_different_value_at_the_same_scale_is_still_a_restatement(spark: Any) -> None:
    """The floor must not swallow real changes. Both of these are stated to the nearest
    thousand, so their difference is a genuine disagreement, not a scale artifact."""
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 2_890_854_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 2_889_972_000, "USD", None, "10-K/A", *FY25),
        ],
    )
    assert build(facts).count() == 1


def test_precision_floor_does_not_swallow_a_genuine_restatement(spark: Any) -> None:
    """The cap earns its keep: without it a value that happens to be round -- 2,000,000
    -- would carry a +/-2,000,000 tolerance and every restatement of it would vanish."""
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 2_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 2_010_000, "USD", None, "10-K/A", *FY25),
        ],
    )
    assert build(facts).count() == 1


def test_the_documented_blind_spot_is_asserted_not_assumed(spark: Any) -> None:
    """Without `decimals`, a sub-0.1% difference on a coarsely-reported value cannot be
    told apart from the same figure re-stated at a coarser scale, so it is not flagged.

    This test exists so the limitation is visible in the suite rather than discovered
    in production. It is an order of magnitude below the `immaterial` band's own 1%
    floor, and it disappears if a source ever supplies `decimals` (ADR-002).
    """
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 2_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 2_001_000, "USD", None, "10-K/A", *FY25),
        ],
    )
    assert build(facts).count() == 0
    # With decimals present, the same pair is unambiguous and IS a restatement.
    with_decimals = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 2_000_000, "USD", 0, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 2_001_000, "USD", 0, "10-K/A", *FY25),
        ],
    )
    assert build(with_decimals).count() == 1


def test_identical_values_produce_zero_rows(spark: Any) -> None:
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 1_000_000, "USD", None, "10-K/A", *FY25),
        ],
    )
    assert build(facts).count() == 0


def test_different_units_are_not_compared(spark: Any) -> None:
    """Comparing across units is meaningless, not a restatement."""
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 900_000, "EUR", None, "10-K/A", *FY25),
        ],
    )
    assert build(facts).count() == 0


def test_different_periods_are_not_compared(spark: Any) -> None:
    """A Q4 duration against an FY duration is a bug, not a restatement."""
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25),
            (
                "0001234567-26-000002",
                date(2026, 6, 1),
                300_000,
                "USD",
                None,
                "10-K",
                date(2025, 10, 1),
                date(2025, 12, 31),
            ),
        ],
    )
    assert build(facts).count() == 0


def test_delta_pct_is_null_not_an_exception_when_the_original_is_zero(spark: Any) -> None:
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 0, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 500_000, "USD", None, "10-K/A", *FY25),
        ],
    )
    rows = build(facts).collect()
    assert len(rows) == 1
    assert rows[0]["delta_pct"] is None
    # "We previously said zero" is the strongest version of the claim.
    assert rows[0]["materiality_band"] == "material"


@pytest.mark.parametrize(
    ("restated", "band"),
    [(1_002_000, "immaterial"), (1_020_000, "notable"), (1_500_000, "material")],
)
def test_materiality_bands(spark: Any, restated: int, band: str) -> None:
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), restated, "USD", None, "10-K/A", *FY25),
        ],
    )
    assert build(facts).collect()[0]["materiality_band"] == band


def test_three_assertions_produce_two_consecutive_comparisons(spark: Any) -> None:
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 1_100_000, "USD", None, "10-K/A", *FY25),
            ("0001234567-26-000003", date(2026, 9, 1), 1_200_000, "USD", None, "10-K/A", *FY25),
        ],
    )
    rows = sorted(build(facts).collect(), key=lambda r: r["restated_filed_date"])
    assert len(rows) == 2
    assert rows[0]["original_value"] == Decimal("1000000.000000")
    assert rows[1]["original_value"] == Decimal("1100000.000000")


def test_restatement_id_is_deterministic(spark: Any) -> None:
    """A re-run must merge, not duplicate."""
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 1_100_000, "USD", None, "10-K/A", *FY25),
        ],
    )
    assert build(facts).collect()[0]["restatement_id"] == build(facts).collect()[0]["restatement_id"]


def test_unmapped_concepts_do_not_participate(spark: Any) -> None:
    facts = _facts(
        spark,
        [
            ("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25),
            ("0001234567-26-000002", date(2026, 6, 1), 1_100_000, "USD", None, "10-K/A", *FY25),
        ],
    ).withColumn("concept_canonical", F.lit(None).cast("string"))
    assert build(facts).count() == 0


def test_a_single_assertion_produces_nothing(spark: Any) -> None:
    facts = _facts(
        spark, [("0001234567-26-000001", date(2026, 3, 1), 1_000_000, "USD", None, "10-K", *FY25)]
    )
    assert build(facts).count() == 0
