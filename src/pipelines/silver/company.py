"""L3 -- ``silver.company`` (feature F-7, data contracts section 3.2).

SCD-2 company dimension, parsed out of the opaque ``payload_json`` bronze keeps.

The structural invariant -- exactly one ``is_current`` row per ``cik`` -- is a
``reject_batch`` check, and it can only be evaluated after the merge. It runs inside
``rollback_on_failure``, so a violation restores the table to its pre-merge version
instead of leaving a dimension that fans out every downstream join.
"""

from __future__ import annotations

from typing import Any

from edgar_lakehouse_contracts import names, schemas

from pipelines import dq_rules as dq_registry
from pipelines.config import Settings
from pipelines.framework.delta_ops import rollback_on_failure
from pipelines.framework.dq import apply_dq
from pipelines.framework.keys import surrogate_key
from pipelines.framework.merge import MergeStats, merge_scd2
from pipelines.framework.metrics import JobRun

from .common import align_to_spec, pad_cik, run_dq_and_quarantine

__all__ = ["SUBMISSIONS_DDL", "TRACKED_COLUMNS", "build", "run", "scd2_invariants"]

SPEC = schemas.SILVER_COMPANY

#: Only the fields silver actually uses. Naming them explicitly rather than inferring
#: keeps a new key in the source from changing this table's schema.
SUBMISSIONS_DDL = (
    "cik STRING, entityType STRING, sic STRING, sicDescription STRING, name STRING, "
    "tickers ARRAY<STRING>, exchanges ARRAY<STRING>, ein STRING, fiscalYearEnd STRING, "
    "stateOfIncorporation STRING, "
    "formerNames ARRAY<STRUCT<name STRING, `from` STRING, `to` STRING>>"
)

#: A change in any of these opens a new SCD-2 version.
TRACKED_COLUMNS: tuple[str, ...] = (
    "company_name",
    "sic",
    "sic_description",
    "ein",
    "entity_type",
    "state_of_incorporation",
    "fiscal_year_end",
    "tickers",
    "exchanges",
    "former_names",
)


def build(bronze_df: Any) -> Any:
    """Parse submissions payloads into the silver company shape."""
    from pyspark.sql import functions as F

    doc = F.from_json(F.col("payload_json"), SUBMISSIONS_DDL)
    return bronze_df.select(
        # company_sk identifies the company, not the version -- it is derived from the
        # natural key alone, so every SCD-2 version of one company shares it and gold can
        # join on a single column then filter is_current.
        surrogate_key(pad_cik(F.coalesce(doc.getField("cik"), F.col("cik")))).alias("company_sk"),
        pad_cik(F.coalesce(doc.getField("cik"), F.col("cik"))).alias("cik"),
        F.trim(doc.getField("name")).alias("company_name"),
        doc.getField("sic").alias("sic"),
        doc.getField("sicDescription").alias("sic_description"),
        doc.getField("ein").alias("ein"),
        doc.getField("entityType").alias("entity_type"),
        doc.getField("stateOfIncorporation").alias("state_of_incorporation"),
        doc.getField("fiscalYearEnd").alias("fiscal_year_end"),
        F.coalesce(doc.getField("tickers"), F.array()).alias("tickers"),
        F.coalesce(doc.getField("exchanges"), F.array()).alias("exchanges"),
        F.coalesce(
            F.transform(doc.getField("formerNames"), lambda x: x.getField("name")), F.array()
        ).alias("former_names"),
        F.col("logical_date"),
        F.col("_ingest_batch_id"),
        F.col("_source_file"),
    )


def scd2_invariants(spark: Any, table: str) -> Any:
    """One row per ``cik`` carrying the two structural counts the invariants check.

    ``overlap_count`` counts version pairs whose validity intervals intersect. An open
    version (``valid_to`` null) is treated as extending to ``9999-12-31``, which is
    what "still current" means for an interval comparison.
    """
    from pyspark.sql import functions as F

    versions = spark.table(table).select(
        "cik",
        "valid_from",
        F.coalesce(F.col("valid_to"), F.to_date(F.lit("9999-12-31"))).alias("valid_to_open"),
        "is_current",
    )
    left = versions.alias("a")
    right = versions.alias("b")
    overlaps = (
        left.join(
            right,
            (F.col("a.cik") == F.col("b.cik"))
            & (F.col("a.valid_from") < F.col("b.valid_from"))
            & (F.col("a.valid_to_open") >= F.col("b.valid_from")),
        )
        .groupBy(F.col("a.cik").alias("cik"))
        .agg(F.count(F.lit(1)).cast("int").alias("overlap_count"))
    )
    currents = versions.groupBy("cik").agg(
        F.sum(F.col("is_current").cast("int")).cast("int").alias("current_count")
    )
    return currents.join(overlaps, on="cik", how="left").withColumn(
        "overlap_count", F.coalesce(F.col("overlap_count"), F.lit(0))
    )


def run(spark: Any, settings: Settings, run_ctx: JobRun) -> MergeStats:
    bronze_table = settings.table(schemas.BRONZE_COMPANY_SUBMISSIONS_RAW.fqn)
    target = settings.table(SPEC.fqn)
    quarantine = settings.table(schemas.SILVER_COMPANY_QUARANTINE.fqn)
    logical_table = f"{names.SCHEMA_SILVER}.{SPEC.name}"

    built = build(spark.table(bronze_table))
    run_ctx.add(rows_in=int(built.count()))

    # valid_from is set by merge_scd2; the row-level check runs against the merged
    # source, so give it the column it checks.
    from pyspark.sql import functions as F

    checked = built.withColumn("valid_from", F.to_date(F.lit(settings.logical_date)))
    passed = run_dq_and_quarantine(
        spark,
        run_ctx,
        checked,
        dq_registry.checks_for(logical_table, kind="row"),
        target_spec=SPEC,
        quarantine_table=quarantine,
        metrics_prefix="silver.company",
    ).drop("valid_from")

    with rollback_on_failure(spark, target):
        stats = merge_scd2(
            spark,
            passed,
            target,
            natural_key=("cik",),
            tracked_cols=TRACKED_COLUMNS,
            logical_date=settings.logical_date,
            # _source_file breaks the tie when one logical date lands the same key
            # twice (a re-fetch). Without a total ordering, row_number() picks
            # whichever row the scan reached first and the result stops being
            # reproducible.
            dedupe_order_by=("logical_date", "_ingest_batch_id", "_source_file"),
        )
        _, _, invariant_metrics = apply_dq(
            scd2_invariants(spark, target),
            dq_registry.checks_for(logical_table, kind="aggregate"),
            run_ctx.run_id,
            source_table=SPEC.fqn,
        )
    run_ctx.record(invariant_metrics, prefix="silver.company.invariants.")
    run_ctx.record(stats.as_metrics("silver.company.merge"))
    run_ctx.add(rows_out=stats.rows_target_after)
    return stats


def align(df: Any) -> Any:
    """Public alias used by tests that need the contract projection."""
    return align_to_spec(df, SPEC)
