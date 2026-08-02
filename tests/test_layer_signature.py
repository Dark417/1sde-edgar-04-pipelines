"""F-11 -- the medallion layers are enforceable, not just a naming convention.

Every table carries the audit columns of its layer and none belonging to another. That
makes a table's layer readable from its schema alone, and makes a table drifting out of
its layer a failing test rather than something a reviewer has to notice.

The cross-layer assertions are the load-bearing half. A ``valid_from`` in gold means
someone built a slowly-changing mart that nothing recomputes; a ``_rescued_data`` in
silver means raw Auto Loader spill leaked past the typed boundary. Both are the kind of
mistake that works fine until the day it silently does not.

No Spark: these are assertions about the specs, so they run on every PR.
"""

from __future__ import annotations

import pytest

from pipelines.contracts import schemas
from pipelines.contracts.models import Layer, TableSpec

BRONZE_SIGNATURE = {c.name for c in schemas.BRONZE_METADATA_COLUMNS}
SILVER_SIGNATURE = {c.name for c in schemas.SILVER_LINEAGE_COLUMNS}
GOLD_SIGNATURE = {"_generated_at", "_run_id"}

#: Columns that may only ever appear in one layer. The value is the layer that owns it.
EXCLUSIVE_TO: dict[str, Layer] = {
    "_rescued_data": Layer.BRONZE,
    "_envelope_version": Layer.BRONZE,
    "_source_system": Layer.BRONZE,
    "_ingest_ts": Layer.BRONZE,
    "_first_seen_ts": Layer.SILVER,
    "_last_seen_ts": Layer.SILVER,
    "_hash_diff": Layer.SILVER,
    "valid_from": Layer.SILVER,
    "valid_to": Layer.SILVER,
    "is_current": Layer.SILVER,
    "version_number": Layer.SILVER,
    "_generated_at": Layer.GOLD,
    "_run_id": Layer.GOLD,
}

ALL = sorted(schemas.TABLES.values(), key=lambda s: s.fqn)
QUARANTINE_COLUMN_NAMES = {c.name for c in schemas.QUARANTINE_COLUMNS}


def _is_quarantine(spec: TableSpec) -> bool:
    """Quarantine tables are silver by location but carry their own shape."""
    return spec.name.endswith("_quarantine")


def _ids(specs: list[TableSpec]) -> list[str]:
    return [s.fqn for s in specs]


BRONZE = [s for s in ALL if s.layer is Layer.BRONZE]
SILVER = [s for s in ALL if s.layer is Layer.SILVER and not _is_quarantine(s)]
GOLD = [s for s in ALL if s.layer is Layer.GOLD]
QUARANTINE = [s for s in ALL if _is_quarantine(s)]


def test_the_layers_are_all_populated() -> None:
    """Guards the guard: if a filter above silently matched nothing, every
    parametrized test below would vacuously pass."""
    assert BRONZE and SILVER and GOLD and QUARANTINE


@pytest.mark.parametrize("spec", BRONZE, ids=_ids(BRONZE))
def test_bronze_carries_the_bronze_signature(spec: TableSpec) -> None:
    missing = BRONZE_SIGNATURE - set(spec.column_names)
    assert not missing, f"{spec.fqn} is bronze but lacks {sorted(missing)}"


@pytest.mark.parametrize("spec", SILVER, ids=_ids(SILVER))
def test_silver_carries_the_lineage_signature(spec: TableSpec) -> None:
    missing = SILVER_SIGNATURE - set(spec.column_names)
    assert not missing, f"{spec.fqn} is silver but lacks {sorted(missing)}"


@pytest.mark.parametrize("spec", GOLD, ids=_ids(GOLD))
def test_gold_carries_the_provenance_signature(spec: TableSpec) -> None:
    missing = GOLD_SIGNATURE - set(spec.column_names)
    assert not missing, f"{spec.fqn} is gold but lacks {sorted(missing)}"


@pytest.mark.parametrize("spec", ALL, ids=_ids(ALL))
def test_no_table_carries_another_layers_audit_column(spec: TableSpec) -> None:
    """The half that catches design drift rather than a missing column."""
    if _is_quarantine(spec):
        pytest.skip("quarantine tables have their own shape, asserted separately")
    for column in spec.column_names:
        owner = EXCLUSIVE_TO.get(column)
        if owner is not None:
            assert owner is spec.layer, (
                f"{spec.fqn} is {spec.layer.value} but carries {column!r}, "
                f"which belongs to {owner.value}"
            )


@pytest.mark.parametrize("spec", QUARANTINE, ids=_ids(QUARANTINE))
def test_quarantine_tables_share_one_shape(spec: TableSpec) -> None:
    """``apply_dq`` stays domain-agnostic only while this holds."""
    assert set(spec.column_names) == QUARANTINE_COLUMN_NAMES


@pytest.mark.parametrize("spec", ALL, ids=_ids(ALL))
def test_every_table_names_the_changeset_that_creates_it(spec: TableSpec) -> None:
    """Preflight turns "table not found" into "changeset X was never applied"."""
    assert spec.changeset.endswith(".yaml")


def test_scd2_tables_carry_the_whole_versioning_set_not_part_of_it(spec: None = None) -> None:
    """Half an SCD-2 is worse than none: an is_current with no interval cannot be
    point-in-time queried, and an interval with no version_number cannot be ordered."""
    scd2_columns = {"valid_from", "valid_to", "is_current", "_hash_diff", "version_number"}
    for spec_ in SILVER:
        present = scd2_columns & set(spec_.column_names)
        if present:
            assert present == scd2_columns, (
                f"{spec_.fqn} has a partial SCD-2 set; missing {sorted(scd2_columns - present)}"
            )
