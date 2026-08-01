"""L0 -- SparkSession accessor.

On Databricks the session already exists and this is a one-line getter. Locally it
builds a session with Delta wired in. Both paths return the same object type, so no
module below this one needs to know where it is running.

No ``dbutils`` here and none anywhere outside ``entrypoints`` (AGENTS.md rule 12):
anything that touches ``dbutils`` cannot be exercised by the local test suite, and the
local test suite is the only place the two tests that decide this project can run.
"""

from __future__ import annotations

import os
from typing import Any

__all__ = ["get_spark", "is_databricks", "local_session"]


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark() -> Any:
    """Return the active SparkSession, building a local one if there is none."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if is_databricks():  # pragma: no cover - only reachable on a cluster
        return SparkSession.builder.getOrCreate()
    return local_session()


def local_session(
    *,
    app_name: str = "edgar-pipelines-local",
    warehouse_dir: str | None = None,
    shuffle_partitions: int = 2,
) -> Any:
    """Build a local SparkSession with Delta.

    ``shuffle_partitions`` defaults to 2 rather than the stock 200: on a laptop the
    scheduling overhead of 200 mostly-empty partitions dominates every test in the
    suite and turns a 40-second run into a five-minute one.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_LOCAL_MASTER", "local[2]"))
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        # Delta MERGE on a table whose schema evolved mid-suite is a real failure mode;
        # keep it explicit rather than letting a stray write widen a table silently.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.ui.enabled", "false")
    )
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    return configure_spark_with_delta_pip(builder).getOrCreate()
