"""L2 -- ``bronze.company_concept_raw``.

Payload kept opaque, same reasoning as ``company_submissions``: the ``units`` map is
keyed by unit string, so its *schema* changes whenever a filer reports a new unit.
Inferring it at bronze would make the table's schema a function of the data.

``resource_id`` is ``<cik>/<taxonomy>/<tag>``; it is split here rather than re-derived
from the URL, because the URL is a fetch detail and the resource id is contract.
"""

from __future__ import annotations

from typing import Any

from pipelines.config import Settings
from pipelines.framework.metrics import JobRun

from .common import BronzeStats, ingest_stream

__all__ = ["ingest", "project"]


def project(df: Any) -> Any:
    from pyspark.sql import functions as F

    parts = F.split(F.col("resource_id"), "/")
    return df.select(
        F.to_date(F.col("logical_date")).alias("logical_date"),
        F.col("resource_id"),
        F.to_timestamp(F.col("fetched_at")).alias("fetched_at"),
        # F.get, not getItem: an out-of-range index raises in Spark 4, and a landing
        # record with a short resource_id must produce nulls that silver's DQ can
        # quarantine -- not a crash that takes down the whole bronze batch. Bronze is
        # not the layer that decides a record is bad.
        F.get(parts, 0).alias("cik"),
        F.get(parts, 1).alias("taxonomy"),
        F.get(parts, 2).alias("tag"),
        F.col("payload_json"),
        F.col("_ingest_batch_id"),
        F.col("_ingest_ts"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_envelope_version"),
        F.col("_rescued_data"),
    )


def ingest(spark: Any, settings: Settings, run: JobRun) -> BronzeStats:
    return ingest_stream(spark, settings, run, "company_concept", project)
