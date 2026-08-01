#!/usr/bin/env python3
"""Create the contract tables in a local metastore. **Test harness, not a pipeline.**

`AGENTS.md` rule 1: this repo never issues `CREATE TABLE` -- repo 1's Liquibase owns
every DDL statement, and a `CREATE TABLE IF NOT EXISTS` inside a pipeline forks the
schema away from `DATABASECHANGELOG` and destroys the migration audit trail.

The local test suite still needs tables to exist, and Liquibase cannot run against a
laptop's Delta warehouse. So this module renders `CREATE TABLE` from the *same*
`TableSpec` objects preflight validates against, and lives **outside `src/pipelines`**.
It is imported only by `tests/conftest.py` and `tools/run_local_pipeline.py`.

Nothing that ships in the wheel can create a table, and `tests/test_no_create_table.py`
greps the package to keep it that way. See ADR-004.

Usage::

    python tools/local_ddl.py --catalog spark_catalog --warehouse /tmp/edgar-wh
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

from pipelines.contracts import names, schemas  # noqa: E402
from pipelines.contracts.models import TableSpec  # noqa: E402

__all__ = ["create_all", "drop_all", "render_create_table", "schemas_for"]


def schemas_for(catalog: str) -> list[str]:
    return [
        f"{catalog}.{names.BRONZE_SCHEMA}",
        f"{catalog}.{names.SILVER_SCHEMA}",
        f"{catalog}.{names.GOLD_SCHEMA}",
    ]


def render_create_table(spec: TableSpec, catalog: str) -> str:
    """Render Delta DDL for a spec.

    ``NOT NULL`` is rendered faithfully. It would be easier to drop it locally -- a
    couple of tests would stop being fussy -- but then the local suite would accept
    writes the workspace rejects, which is the opposite of what a test harness is for.
    """
    bound = spec.with_catalog(catalog)
    columns = ",\n  ".join(c.ddl() for c in bound.columns)
    clauses = [f"CREATE TABLE IF NOT EXISTS {bound.fqn} (\n  {columns}\n) USING DELTA"]
    if bound.partition_by:
        clauses.append(f"PARTITIONED BY ({', '.join(bound.partition_by)})")
    if bound.comment:
        escaped = bound.comment.replace("'", "''")
        clauses.append(f"COMMENT '{escaped}'")
    return "\n".join(clauses)


def create_all(spark: Any, catalog: str = "spark_catalog") -> list[str]:
    """Create every contract schema and table. Returns the FQNs created."""
    for schema in schemas_for(catalog):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    created = []
    for spec in schemas.ALL_TABLES:
        spark.sql(render_create_table(spec, catalog))
        created.append(spec.with_catalog(catalog).fqn)
    return created


def attach_all(spark: Any, warehouse_dir: str, catalog: str = "spark_catalog") -> list[str]:
    """Re-register tables that already exist on disk under ``warehouse_dir``.

    The local Spark build has no persistent metastore: the catalog is in-memory and
    dies with the session, while the Delta data outlives it. Attaching by ``LOCATION``
    (no column list) lets Delta read the schema back out of its own transaction log,
    which is also the only form that does not fight with the existing partitioning.

    None of this applies on Databricks, where Unity Catalog persists -- it is one more
    reason the DDL harness lives outside the shipped package (ADR-004).
    """
    root = Path(warehouse_dir).resolve()
    for schema in schemas_for(catalog):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    attached = []
    for spec in schemas.ALL_TABLES:
        bound = spec.with_catalog(catalog)
        location = root / f"{spec.schema}.db" / spec.name
        if not (location / "_delta_log").is_dir():
            continue
        spark.sql(f"CREATE TABLE IF NOT EXISTS {bound.fqn} USING DELTA LOCATION '{location}'")
        attached.append(bound.fqn)
    return attached


def drop_all(spark: Any, catalog: str = "spark_catalog") -> None:
    for spec in schemas.ALL_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {spec.with_catalog(catalog).fqn}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="spark_catalog")
    parser.add_argument("--warehouse", default=None, help="local warehouse dir")
    parser.add_argument("--print-only", action="store_true", help="render DDL and exit")
    args = parser.parse_args(argv)

    if args.print_only:
        for spec in schemas.ALL_TABLES:
            print(render_create_table(spec, args.catalog) + ";\n")
        return 0

    from pipelines.session import local_session

    spark = local_session(warehouse_dir=args.warehouse)
    for fqn in create_all(spark, args.catalog):
        print(f"created {fqn}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
