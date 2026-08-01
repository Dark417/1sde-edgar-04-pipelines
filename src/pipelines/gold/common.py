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


def stamp(df: Any, run_id: str) -> Any:
    """Attach ``_generated_at`` / ``_run_id``."""
    from pyspark.sql import functions as F

    return df.withColumn("_generated_at", F.current_timestamp()).withColumn(
        "_run_id", F.lit(run_id)
    )


def write_gold(df: Any, spec: TableSpec, table: str, run_id: str) -> int:
    """Rebuild ``table`` from ``df``. Returns the row count written."""
    aligned = align_to_spec(stamp(df, run_id), spec).cache()
    count = int(aligned.count())
    aligned.write.format("delta").mode("overwrite").option("overwriteSchema", "false").saveAsTable(
        table
    )
    aligned.unpersist()
    return count
