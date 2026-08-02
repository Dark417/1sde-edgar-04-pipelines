"""L4 -- shared gold write path.

Gold is disposable: if a gold table is wrong you fix the transform and rebuild, you
never patch it. So gold writes are a full overwrite, which is idempotent by
construction -- unlike silver, where rule 3 requires a MERGE because silver rows carry
first-seen history that a rebuild would destroy.

``overwriteSchema`` is never enabled. The schema belongs to Liquibase; an overwrite
that also rewrites the schema is a ``CREATE TABLE`` wearing a disguise.
"""

from __future__ import annotations

from typing import Any

from pipelines.contracts.models import TableSpec

__all__ = ["align_to_spec", "stamp", "write_gold"]


def align_to_spec(df: Any, spec: TableSpec) -> Any:
    from pyspark.sql import functions as F

    cols = []
    for column in spec.columns:
        source = F.col(f"`{column.name}`") if column.name in df.columns else F.lit(None)
        cols.append(source.cast(column.type_sql).alias(column.name))
    return df.select(*cols)


def delta_version(spark: Any, table: str) -> int | None:
    """Current Delta version of ``table``, or None if it cannot be determined.

    Returns None rather than raising: this is provenance, and a gold rebuild that
    otherwise succeeded should not fail because history was unreadable. A null
    ``_source_version`` is an honest "we do not know", which is strictly better than a
    fabricated number that someone will later trust enough to time-travel to.
    """
    try:
        row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("version").collect()
    except Exception:
        return None
    return int(row[0]["version"]) if row else None


def stamp(df: Any, run_id: str, source_version: int | None = None) -> Any:
    """Attach ``_generated_at`` / ``_run_id`` / ``_source_version``."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("_generated_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_version", F.lit(source_version).cast("bigint"))
    )


def write_gold(
    df: Any, spec: TableSpec, table: str, run_id: str, source_version: int | None = None
) -> int:
    """Rebuild ``table`` from ``df``. Returns the row count written.

    ``source_version`` is the Delta version of the silver input this mart was built
    from, captured by the caller *before* the read. It is the reproducibility hook: it
    turns "gold looked wrong last Tuesday" into
    ``SELECT * FROM edgar.silver.<t> VERSION AS OF <n>`` instead of a guess, which
    matters precisely because gold is rebuilt rather than versioned -- once a rebuild
    replaces it, the inputs are the only way back.
    """
    aligned = align_to_spec(stamp(df, run_id, source_version), spec)
    count = int(aligned.count())
    aligned.write.format("delta").mode("overwrite").option("overwriteSchema", "false").saveAsTable(
        table
    )
    return count
