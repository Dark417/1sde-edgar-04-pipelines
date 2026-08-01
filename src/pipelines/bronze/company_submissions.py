"""L2 -- ``bronze.company_submissions_raw``.

``payload_json`` stays a single opaque STRING. The submissions document is deeply
nested, uses a parallel-array encoding for `filings.recent`, and the SEC changes its
shape without notice. Exploding it here would couple a *table* to a shape nobody in
this project controls, turning every upstream change into a migration. The explode
belongs in silver, where the same change is a transform fix.
"""

from __future__ import annotations

from typing import Any

from pipelines.config import Settings
from pipelines.framework.metrics import JobRun

from .common import BronzeStats, ingest_stream

__all__ = ["ingest", "project"]


def project(df: Any) -> Any:
    from pyspark.sql import functions as F

    return df.select(
        F.to_date(F.col("logical_date")).alias("logical_date"),
        F.col("resource_id"),
        F.to_timestamp(F.col("fetched_at")).alias("fetched_at"),
        # resource_id is the zero-padded CIK for this stream; keep it raw here anyway.
        F.col("resource_id").alias("cik"),
        F.col("payload_json"),
        F.col("_ingest_batch_id"),
        F.col("_ingest_ts"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_envelope_version"),
        F.col("_rescued_data"),
    )


def ingest(spark: Any, settings: Settings, run: JobRun) -> BronzeStats:
    return ingest_stream(spark, settings, run, "company_submissions", project)
