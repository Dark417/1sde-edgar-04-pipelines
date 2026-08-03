"""L4 -- ``gold.financials_current`` (feature F-10, data contracts section 4.1).

The winning assertion per ``(cik, concept_canonical, unit, period_start, period_end,
period_type)``: latest ``filed_date``, ties broken by the greater ``accession_number``.

The tie-break is not decoration. Two accessions filed the same day for the same period
is ordinary -- an original and a same-day correction -- and without a total ordering
the winner would depend on scan order, which violates the determinism law.
"""

from __future__ import annotations

from typing import Any

from edgar_lakehouse_contracts import schemas

from pipelines.config import Settings
from pipelines.framework.metrics import JobRun

from .common import delta_version, write_gold
from .restatement_event import GRAIN

__all__ = ["build", "run"]

SPEC = schemas.GOLD_FINANCIALS_CURRENT


def build(facts: Any, *, company_names: Any = None, restatements: Any = None) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    grain_cols = [F.col(f"`{c}`") for c in GRAIN]
    mapped = facts.filter(F.col("concept_canonical").isNotNull())

    ordering = Window.partitionBy(*grain_cols).orderBy(
        F.col("filed_date").desc(), F.col("accession_number").desc()
    )
    counting = Window.partitionBy(*grain_cols)

    latest = (
        mapped.withColumn("_rank", F.row_number().over(ordering))
        # collect_set, not approx_count_distinct: assertion counts are single digits and
        # an HLL estimate that says 2 when the answer is 3 is worse than useless here.
        .withColumn(
            "assertion_count",
            F.size(F.collect_set(F.col("accession_number")).over(counting)).cast("int"),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    if restatements is not None:
        restated_keys = (
            restatements.select(*GRAIN).distinct().withColumn("was_restated", F.lit(True))
        )
        latest = latest.join(restated_keys, on=list(GRAIN), how="left")
    else:
        latest = latest.withColumn("was_restated", F.lit(None).cast("boolean"))
    latest = latest.withColumn("was_restated", F.coalesce(F.col("was_restated"), F.lit(False)))

    if company_names is not None:
        latest = latest.join(F.broadcast(company_names), on="cik", how="left")
    else:
        latest = latest.withColumn("company_name", F.lit(None).cast("string"))
    return latest


def run(spark: Any, settings: Settings, run_ctx: JobRun) -> int:
    from pyspark.sql import functions as F

    facts = spark.table(settings.table(schemas.SILVER_FINANCIAL_FACT.fqn))
    companies = (
        spark.table(settings.table(schemas.SILVER_COMPANY.fqn))
        .filter(F.col("is_current"))
        .select("cik", "company_name")
    )
    restatements = spark.table(settings.table(schemas.GOLD_RESTATEMENT_EVENT.fqn))
    built = build(facts, company_names=companies, restatements=restatements)
    count = write_gold(
        built,
        SPEC,
        settings.table(SPEC.fqn),
        run_ctx.run_id,
        source_version=delta_version(spark, settings.table(schemas.SILVER_FINANCIAL_FACT.fqn)),
    )
    run_ctx.record({"gold.financials_current.rows": count})
    return count
