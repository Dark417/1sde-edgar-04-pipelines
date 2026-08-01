"""L1 -- the table-existence gate (feature F-1).

This repo never issues ``CREATE TABLE``. Liquibase in repo 1 owns every DDL statement,
and ``CREATE TABLE IF NOT EXISTS`` here would fork the schema away from Liquibase's
``DATABASECHANGELOG`` and destroy the migration audit trail. So when a table is
missing, the correct behavior is to fail -- loudly, at startup, naming the changeset
that was never applied.

Without this gate the symptom is an ``AnalysisException`` 200 lines into a job,
pointing at whichever query happened to touch the table first.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

__all__ = ["MissingTableError", "assert_tables_exist", "table_exists"]

_CHANGESET_HINT = {
    "bronze": "010-bronze.yaml",
    "silver": "020-silver.yaml",
    "gold": "030-gold.yaml",
}


class MissingTableError(RuntimeError):
    """A table this job writes does not exist."""

    def __init__(self, missing: Sequence[tuple[str, str]]) -> None:
        lines = [
            f"  - {fqn}  (created by repo 1 Liquibase changeset {changeset})"
            for fqn, changeset in missing
        ]
        super().__init__(
            "preflight failed: "
            f"{len(missing)} table(s) this job writes do not exist:\n"
            + "\n".join(lines)
            + "\n\nThis repo does not create tables. Apply the migration in repo 1 "
            "(`liquibase update`) and re-run. Do NOT add CREATE TABLE here: it forks "
            "the schema away from DATABASECHANGELOG and loses the audit trail."
        )
        self.missing = list(missing)


def _changeset_for(fqn: str) -> str:
    """Best-known changeset for a table, from the contract or from its schema name."""
    from pipelines.contracts import schemas

    spec = schemas.TABLES.get(fqn)
    if spec is not None:
        return spec.changeset
    parts = fqn.split(".")
    schema = parts[1] if len(parts) == 3 else ""
    return _CHANGESET_HINT.get(schema, "see repo 1 db/changelog/")


def table_exists(spark: Any, fqn: str) -> bool:
    """True when ``fqn`` resolves to a table.

    ``information_schema`` is the documented Unity Catalog path but is not present in a
    plain local metastore, so the local path falls back to the catalog API. Both answer
    the same question; only the local one is allowed to be slower.
    """
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"expected catalog.schema.table, got {fqn!r}")
    catalog, schema, name = parts

    try:
        rows = spark.sql(
            "SELECT 1 FROM system.information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? LIMIT 1",
            args=[catalog, schema, name],
        ).take(1)
        return len(rows) > 0
    except Exception:
        # No information_schema in a plain local metastore; fall through.
        pass

    try:
        return bool(spark.catalog.tableExists(fqn))
    except Exception:
        # A missing catalog or schema is, for this question, a missing table.
        return False


def assert_tables_exist(spark: Any, tables: Sequence[str]) -> None:
    """Raise :class:`MissingTableError` naming every missing table and its changeset."""
    missing = [(fqn, _changeset_for(fqn)) for fqn in tables if not table_exists(spark, fqn)]
    if missing:
        raise MissingTableError(missing)
