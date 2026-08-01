"""Feature F-4: SCD-1 and SCD-2 merges.

The four SCD-2 cases from AGENTS.md are all here and all required:
(a) no change, (b) tracked column changed, (c) array reordered, (d) run twice.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from pipelines.framework.merge import hash_diff, merge_scd1, merge_scd2

pytestmark = pytest.mark.spark

COMPANY_DDL = (
    "cik STRING, company_name STRING, sic STRING, sic_description STRING, ein STRING, "
    "entity_type STRING, state_of_incorporation STRING, fiscal_year_end STRING, "
    "tickers ARRAY<STRING>, exchanges ARRAY<STRING>, former_names ARRAY<STRING>, "
    "_ingest_batch_id STRING, _source_file STRING"
)

TRACKED = (
    "company_name",
    "sic",
    "sic_description",
    "ein",
    "entity_type",
    "state_of_incorporation",
    "fiscal_year_end",
    "tickers",
    "exchanges",
    "former_names",
)


def _company(
    spark: Any,
    *,
    name: str = "TEST FILER INC",
    tickers: list[str] | None = None,
    exchanges: list[str] | None = None,
) -> Any:
    return spark.createDataFrame(
        [
            (
                "0000000001",
                name,
                "1311",
                "Oil and gas",
                "123456789",
                "operating",
                "DE",
                "1231",
                tickers if tickers is not None else ["AAA", "BBB"],
                exchanges if exchanges is not None else ["Nasdaq"],
                [],
                "batch-1",
                "file-1",
            )
        ],
        COMPANY_DDL,
    )


def _filings(spark: Any, rows: list[tuple[str, str, str]]) -> Any:
    return spark.createDataFrame(
        [
            (
                accession,
                cik,
                name,
                "10-K",
                "10-K",
                False,
                date(2026, 7, 31),
                None,
                date(2026, 7, 31),
                "batch-1",
                "file-1",
            )
            for accession, cik, name in rows
        ],
        "accession_number STRING, cik STRING, company_name STRING, form_type STRING, "
        "base_form_type STRING, is_amendment BOOLEAN, filed_date DATE, "
        "primary_doc_url STRING, logical_date DATE, _ingest_batch_id STRING, _source_file STRING",
    )


# ----------------------------------------------------------------------------- hash


def test_hash_diff_is_stable_under_array_reordering(spark: Any) -> None:
    """AGENTS.md rule 5. Unsorted hashing makes the dimension grow every single day."""
    a = _company(spark, tickers=["AAA", "BBB"]).select(hash_diff(_company(spark), TRACKED))
    b = _company(spark, tickers=["BBB", "AAA"])
    b_hash = b.select(hash_diff(b, TRACKED)).collect()[0][0]
    assert a.collect()[0][0] == b_hash


def test_hash_diff_changes_when_a_tracked_value_changes(spark: Any) -> None:
    base = _company(spark)
    changed = _company(spark, name="RENAMED INC")
    assert (
        base.select(hash_diff(base, TRACKED)).collect()[0][0]
        != changed.select(hash_diff(changed, TRACKED)).collect()[0][0]
    )


def test_hash_diff_distinguishes_null_position(spark: Any) -> None:
    """Without a null sentinel, ('a', None) and (None, 'a') collide."""
    df = spark.createDataFrame([("a", None), (None, "a")], "x STRING, y STRING")
    hashes = [r[0] for r in df.select(hash_diff(df, ["x", "y"])).collect()]
    assert hashes[0] != hashes[1]


# ---------------------------------------------------------------------------- SCD-1


def test_scd1_inserts_then_is_a_no_op(spark: Any, tables: str) -> None:
    target = f"{tables}.silver.filing"
    source = _filings(spark, [("0000000001-26-000001", "0000000001", "A")])

    first = merge_scd1(spark, source, target, keys=("accession_number",))
    assert first.rows_inserted == 1

    before = {
        r["accession_number"]: r["_first_seen_ts"] for r in spark.table(target).collect()
    }
    second = merge_scd1(spark, source, target, keys=("accession_number",))
    after = {r["accession_number"]: r["_first_seen_ts"] for r in spark.table(target).collect()}

    assert second.rows_inserted == 0
    assert spark.table(target).count() == 1
    # Rule 4: _first_seen_ts is written on insert and never touched again.
    assert before == after


def test_scd1_updates_last_seen_but_not_first_seen(spark: Any, tables: str) -> None:
    target = f"{tables}.silver.filing"
    merge_scd1(
        spark,
        _filings(spark, [("0000000001-26-000001", "0000000001", "OLD NAME")]),
        target,
        keys=("accession_number",),
    )
    original = spark.table(target).collect()[0]
    merge_scd1(
        spark,
        _filings(spark, [("0000000001-26-000001", "0000000001", "NEW NAME")]),
        target,
        keys=("accession_number",),
    )
    updated = spark.table(target).collect()[0]
    assert updated["company_name"] == "NEW NAME"
    assert updated["_first_seen_ts"] == original["_first_seen_ts"]
    assert updated["_last_seen_ts"] >= original["_last_seen_ts"]


def test_scd1_refuses_to_update_first_seen_ts(spark: Any, tables: str) -> None:
    with pytest.raises(ValueError, match="_first_seen_ts must never be updated"):
        merge_scd1(
            spark,
            _filings(spark, [("0000000001-26-000001", "0000000001", "A")]),
            f"{tables}.silver.filing",
            keys=("accession_number",),
            update_cols=("company_name", "_first_seen_ts"),
        )


def test_scd1_dedupes_the_source_on_the_business_key(spark: Any, tables: str) -> None:
    """Delta refuses a MERGE with two source rows per target row, and rightly so."""
    target = f"{tables}.silver.filing"
    source = _filings(
        spark,
        [("0000000001-26-000001", "0000000001", "A"), ("0000000001-26-000001", "0000000001", "B")],
    )
    stats = merge_scd1(spark, source, target, keys=("accession_number",))
    assert stats.rows_inserted == 1
    assert spark.table(target).count() == 1


def test_scd1_requires_a_key(spark: Any, tables: str) -> None:
    with pytest.raises(ValueError, match="at least one business key"):
        merge_scd1(spark, _filings(spark, []), f"{tables}.silver.filing", keys=())


# ---------------------------------------------------------------------------- SCD-2


def test_scd2_a_no_change_produces_no_new_rows(spark: Any, tables: str) -> None:
    target = f"{tables}.silver.company"
    merge_scd2(spark, _company(spark), target, ("cik",), TRACKED, "2026-07-31")
    merge_scd2(spark, _company(spark), target, ("cik",), TRACKED, "2026-08-01")
    assert spark.table(target).count() == 1


def test_scd2_b_tracked_change_closes_the_old_row_and_inserts_a_new_one(
    spark: Any, tables: str
) -> None:
    target = f"{tables}.silver.company"
    merge_scd2(spark, _company(spark), target, ("cik",), TRACKED, "2026-07-31")
    merge_scd2(
        spark, _company(spark, name="RENAMED INC"), target, ("cik",), TRACKED, "2026-08-05"
    )

    rows = sorted(spark.table(target).collect(), key=lambda r: r["valid_from"])
    assert len(rows) == 2
    old, new = rows
    assert old["is_current"] is False
    assert old["valid_to"] == date(2026, 8, 4)  # logical_date - 1
    assert old["company_name"] == "TEST FILER INC"
    assert new["is_current"] is True
    assert new["valid_from"] == date(2026, 8, 5)
    assert new["valid_to"] is None
    assert new["company_name"] == "RENAMED INC"


def test_scd2_c_array_reorder_produces_no_new_version(spark: Any, tables: str) -> None:
    """The test that keeps the dimension from exploding. Source array ordering is not
    stable; the same members in a different order are the same company."""
    target = f"{tables}.silver.company"
    merge_scd2(
        spark,
        _company(spark, tickers=["AAA", "BBB"], exchanges=["Nasdaq", "NYSE"]),
        target,
        ("cik",),
        TRACKED,
        "2026-07-31",
    )
    merge_scd2(
        spark,
        _company(spark, tickers=["BBB", "AAA"], exchanges=["NYSE", "Nasdaq"]),
        target,
        ("cik",),
        TRACKED,
        "2026-08-01",
    )
    assert spark.table(target).count() == 1
    assert spark.table(target).collect()[0]["is_current"] is True


def test_scd2_d_running_the_same_batch_twice_is_identical(spark: Any, tables: str) -> None:
    target = f"{tables}.silver.company"
    merge_scd2(spark, _company(spark), target, ("cik",), TRACKED, "2026-07-31")
    first = [
        (r["cik"], r["valid_from"], r["valid_to"], r["is_current"], r["_first_seen_ts"])
        for r in spark.table(target).collect()
    ]
    merge_scd2(spark, _company(spark), target, ("cik",), TRACKED, "2026-07-31")
    second = [
        (r["cik"], r["valid_from"], r["valid_to"], r["is_current"], r["_first_seen_ts"])
        for r in spark.table(target).collect()
    ]
    assert first == second


def test_scd2_same_day_change_replaces_the_version_in_place(spark: Any, tables: str) -> None:
    """Closing it would give valid_to = valid_from - 1: a negative-length interval that
    fails the no-overlap invariant and cannot be point-in-time queried."""
    target = f"{tables}.silver.company"
    merge_scd2(spark, _company(spark), target, ("cik",), TRACKED, "2026-07-31")
    merge_scd2(
        spark, _company(spark, name="RENAMED INC"), target, ("cik",), TRACKED, "2026-07-31"
    )
    rows = spark.table(target).collect()
    assert len(rows) == 1
    assert rows[0]["company_name"] == "RENAMED INC"
    assert rows[0]["is_current"] is True
    assert rows[0]["valid_to"] is None


def test_scd2_exactly_one_current_row_per_key_after_several_changes(
    spark: Any, tables: str
) -> None:
    target = f"{tables}.silver.company"
    for i, day in enumerate(("2026-07-31", "2026-08-05", "2026-08-10")):
        merge_scd2(spark, _company(spark, name=f"NAME {i}"), target, ("cik",), TRACKED, day)
    rows = spark.table(target).collect()
    assert len(rows) == 3
    assert sum(1 for r in rows if r["is_current"]) == 1


def test_scd2_validates_its_arguments(spark: Any, tables: str) -> None:
    target = f"{tables}.silver.company"
    with pytest.raises(ValueError, match="natural key"):
        merge_scd2(spark, _company(spark), target, (), TRACKED, "2026-07-31")
    with pytest.raises(ValueError, match="tracked column"):
        merge_scd2(spark, _company(spark), target, ("cik",), (), "2026-07-31")
