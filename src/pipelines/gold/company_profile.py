"""L4 -- ``gold.company_profile`` (feature F-10, data contracts section 4.4)."""

from __future__ import annotations

from typing import Any

from pipelines.config import Settings
from pipelines.contracts import schemas
from pipelines.framework.metrics import JobRun

from .common import write_gold

__all__ = ["build", "run"]

SPEC = schemas.GOLD_COMPANY_PROFILE


def build(companies: Any, filings: Any, restatements: Any) -> Any:
    """Current company attributes plus filing and restatement counts.

    Left joins throughout: a company we hold submissions for but no filings still
    belongs in the profile with a zero count. An inner join would make it disappear,
    and "the company is missing" is a much harder bug to notice than "the count is 0".
    """
    from pyspark.sql import functions as F

    current = companies.filter(F.col("is_current"))
    filing_stats = filings.groupBy("cik").agg(
        F.count(F.lit(1)).cast("int").alias("filing_count"),
        F.min("filed_date").alias("first_filed_date"),
        F.max("filed_date").alias("last_filed_date"),
    )
    restatement_stats = restatements.groupBy("cik").agg(
        F.count(F.lit(1)).cast("int").alias("restatement_count")
    )
    return (
        current.join(filing_stats, on="cik", how="left")
        .join(restatement_stats, on="cik", how="left")
        .withColumn("filing_count", F.coalesce(F.col("filing_count"), F.lit(0)))
        .withColumn("restatement_count", F.coalesce(F.col("restatement_count"), F.lit(0)))
    )


def run(spark: Any, settings: Settings, run_ctx: JobRun) -> int:
    companies = spark.table(settings.table(schemas.SILVER_COMPANY.fqn))
    filings = spark.table(settings.table(schemas.SILVER_FILING.fqn))
    restatements = spark.table(settings.table(schemas.GOLD_RESTATEMENT_EVENT.fqn))
    count = write_gold(
        build(companies, filings, restatements), SPEC, settings.table(SPEC.fqn), run_ctx.run_id
    )
    run_ctx.record({"gold.company_profile.rows": count})
    return count
