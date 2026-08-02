"""L4 -- ``gold.filing_activity_daily`` (feature F-10, data contracts section 4.3)."""

from __future__ import annotations

from typing import Any

from pipelines.config import Settings
from pipelines.contracts import schemas
from pipelines.framework.metrics import JobRun

from .common import delta_version, write_gold

__all__ = ["build", "run"]

SPEC = schemas.GOLD_FILING_ACTIVITY_DAILY


def build(filings: Any) -> Any:
    """Counts per ``(filed_date, base_form_type)``.

    Grouped on ``base_form_type``, not ``form_type``, so a form and its amendment land
    in the same series -- ``amendment_count`` is what separates them. Splitting them
    into two rows makes a filing-volume chart look like activity dropped whenever
    amendments spiked.
    """
    from pyspark.sql import functions as F

    # One row per *version* since silver.filing became SCD-2 (contracts v1.1.0). Counting
    # unfiltered would double-count every amended filing -- and because an amendment is
    # precisely what opens a new version, the inflation lands exactly on the days this
    # series is meant to explain.
    return (
        filings.filter(F.col("is_current"))
        .groupBy("filed_date", "base_form_type")
        .agg(
            F.count(F.lit(1)).cast("int").alias("filing_count"),
            F.sum(F.col("is_amendment").cast("int")).cast("int").alias("amendment_count"),
            F.count_distinct(F.col("cik")).cast("int").alias("distinct_cik_count"),
        )
    )


def run(spark: Any, settings: Settings, run_ctx: JobRun) -> int:
    filings = spark.table(settings.table(schemas.SILVER_FILING.fqn))
    count = write_gold(
        build(filings),
        SPEC,
        settings.table(SPEC.fqn),
        run_ctx.run_id,
        source_version=delta_version(spark, settings.table(schemas.SILVER_FILING.fqn)),
    )
    run_ctx.record({"gold.filing_activity_daily.rows": count})
    return count
