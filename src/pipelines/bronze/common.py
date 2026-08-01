"""L2 -- the shared bronze ingest path (feature F-5).

Bronze is append-only. No `UPDATE`, no `DELETE`, no dedup beyond the landing file
checkpoint (AGENTS.md rule 2). Bronze is what you replay from, and a bronze you have
edited is not a replay source.

Every bronze table gets the same six metadata columns and differs only in how the
envelope's ``payload_json`` is projected -- which is the ``project`` callback each
stream module supplies.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pipelines.config import Settings, batch_id_for
from pipelines.contracts import names, schemas
from pipelines.contracts.envelope import ENVELOPE_VERSION, SOURCE_SYSTEM
from pipelines.framework import autoloader
from pipelines.framework.metrics import JobRun

__all__ = ["BronzeStats", "ingest_stream"]

Projector = Callable[[Any], Any]


class BronzeStats:
    """Row counts for one stream's ingest."""

    __slots__ = ("files_read", "rescued_rows", "rows_appended", "stream")

    def __init__(self, stream: str, files_read: int, rows_appended: int, rescued_rows: int) -> None:
        self.stream = stream
        self.files_read = files_read
        self.rows_appended = rows_appended
        self.rescued_rows = rescued_rows

    def as_metrics(self) -> dict[str, int]:
        return {
            f"bronze.{self.stream}.files_read": self.files_read,
            f"bronze.{self.stream}.rows_appended": self.rows_appended,
            f"bronze.{self.stream}.rescued_row_count": self.rescued_rows,
        }


def _with_metadata(df: Any, stream: str, logical_date: str) -> Any:
    """Attach the six bronze metadata columns."""
    from pyspark.sql import functions as F

    return (
        df.withColumn(
            "_ingest_batch_id",
            F.coalesce(F.col("batch_id"), F.lit(batch_id_for(stream, logical_date))),
        )
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_system", F.coalesce(F.col("source_system"), F.lit(SOURCE_SYSTEM)))
        .withColumn(
            "_envelope_version", F.coalesce(F.col("envelope_version"), F.lit(ENVELOPE_VERSION))
        )
    )


def _align_to_spec(df: Any, spec: Any) -> Any:
    """Project and cast to the contract's column order.

    By name and by type, never by position: an ``INSERT`` that lines up by position is
    a column-order change away from silently writing CIKs into the company-name column.
    """
    from pyspark.sql import functions as F

    cols = []
    for column in spec.columns:
        source = F.col(f"`{column.name}`") if column.name in df.columns else F.lit(None)
        cols.append(source.cast(column.type_sql).alias(column.name))
    return df.select(*cols)


def ingest_stream(
    spark: Any,
    settings: Settings,
    run: JobRun,
    stream_name: str,
    project: Projector,
) -> BronzeStats:
    """Read one landing stream and append it to its bronze table.

    Returns without writing when there is nothing new -- re-processing a landing file
    adds zero rows, which is F-5's acceptance criterion and the reason the ledger
    (or Auto Loader's checkpoint) exists.
    """
    stream = names.stream(stream_name)
    spec = schemas.table(f"{names.CATALOG}.{names.BRONZE_SCHEMA}.{stream.bronze_table}")
    target = settings.table(spec.fqn)

    if settings.ingest_mode == "batch":
        batch = autoloader.read_landing_batch(
            spark, stream_name, settings.landing_root, settings.checkpoint_root
        )
        if batch.is_empty:
            run.add(**{f"bronze.{stream_name}.files_read": 0})
            return BronzeStats(stream_name, 0, 0, 0)

        autoloader.assert_known_envelope_versions(batch.df, [ENVELOPE_VERSION])
        prepared = _align_to_spec(
            project(_with_metadata(batch.df, stream_name, settings.logical_date)), spec
        ).cache()
        rescued = int(prepared.filter(prepared["_rescued_data"].isNotNull()).count())
        rows = int(prepared.count())
        prepared.write.format("delta").mode("append").saveAsTable(target)
        prepared.unpersist()

        # Commit the ledger only after the append succeeded. A crash before this point
        # replays the file; a crash after it would have lost it.
        batch.commit()
        stats = BronzeStats(stream_name, len(batch.files), rows, rescued)
    else:  # pragma: no cover - Auto Loader only exists on Databricks
        source = autoloader.read_landing_stream(
            spark, stream_name, settings.landing_root, settings.checkpoint_root
        )
        prepared = _align_to_spec(
            project(_with_metadata(source, stream_name, settings.logical_date)), spec
        )
        query = (
            prepared.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", names.checkpoint_path(settings.checkpoint_root, stream_name))
            .trigger(availableNow=True)
            .toTable(target)
        )
        query.awaitTermination()
        progress = query.lastProgress or {}
        rows = int(progress.get("numInputRows", 0))
        rescued = int(
            spark.table(target)
            .filter(f"_ingest_batch_id = '{batch_id_for(stream_name, settings.logical_date)}'")
            .filter("_rescued_data IS NOT NULL")
            .count()
        )
        stats = BronzeStats(stream_name, 0, rows, rescued)

    if stats.rescued_rows:
        # Rule 11: a non-null _rescued_data is the only signal that the source changed
        # shape. Never a silent pass.
        run.warn(
            f"{stream_name}: {stats.rescued_rows} row(s) carry _rescued_data -- the landing "
            "payload has a field the contract does not name. Check the source before trusting silver."
        )
    run.record(stats.as_metrics())
    return stats
