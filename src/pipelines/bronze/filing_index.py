"""L2 -- ``bronze.filing_index_raw``.

Daily-index rows are flat and stable, so this is the one stream whose payload is
projected into passthrough columns rather than kept opaque. Values stay **raw**: `cik`
is not padded and `date_filed` is still text. Typing happens in silver so that a value
that fails to parse can be quarantined with its original bytes visible, instead of
becoming a null nobody can explain.
"""

from __future__ import annotations

from typing import Any

from pipelines.config import Settings
from pipelines.framework.metrics import JobRun

from .common import BronzeStats, ingest_stream

__all__ = ["PAYLOAD_DDL", "ingest", "project"]

PAYLOAD_DDL = (
    "form_type STRING, company_name STRING, cik STRING, "
    "date_filed STRING, accession_number STRING, file_name STRING"
)


def project(df: Any) -> Any:
    from pyspark.sql import functions as F

    payload = F.from_json(F.col("payload_json"), PAYLOAD_DDL)
    return df.select(
        F.to_date(F.col("logical_date")).alias("logical_date"),
        F.col("resource_id"),
        F.to_timestamp(F.col("fetched_at")).alias("fetched_at"),
        payload.getField("form_type").alias("form_type"),
        payload.getField("company_name").alias("company_name"),
        payload.getField("cik").alias("cik"),
        payload.getField("date_filed").alias("date_filed"),
        payload.getField("accession_number").alias("accession_number"),
        payload.getField("file_name").alias("file_name"),
        F.col("_ingest_batch_id"),
        F.col("_ingest_ts"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_envelope_version"),
        F.col("_rescued_data"),
    )


def ingest(spark: Any, settings: Settings, run: JobRun) -> BronzeStats:
    return ingest_stream(spark, settings, run, "filing_index", project)
