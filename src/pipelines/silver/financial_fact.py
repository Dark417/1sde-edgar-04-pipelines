"""L3 -- ``silver.financial_fact`` (feature F-8, data contracts section 3.3).

Explodes the XBRL ``units`` map into one row per **assertion**.

``accession_number`` is part of the grain. The same ``(cik, concept, period)`` reported
by two accessions must produce **two rows, not one** -- the difference between them is
the restatement. Collapsing it, which is the obvious-looking dedup, destroys the
feature silently: the table still looks correct.
"""

from __future__ import annotations

from typing import Any

from pipelines.config import Settings
from pipelines.contracts import concepts as concept_registry
from pipelines.contracts import dq as dq_registry
from pipelines.contracts import names, schemas
from pipelines.framework.dq import apply_dq
from pipelines.framework.merge import MergeStats, dedupe_on, merge_scd1
from pipelines.framework.metrics import JobRun

from .common import align_to_spec, normalize_accession, pad_cik, run_dq_and_quarantine

__all__ = ["CONCEPT_DDL", "GRAIN", "build", "canonical_concept", "grain_counts", "run"]

SPEC = schemas.SILVER_FINANCIAL_FACT

#: ``decimals`` is declared even though the ``companyconcept`` API never returns it
#: (verified against the live API: the fact object carries start/end/val/accn/fy/fp/
#: form/filed/frame and nothing else). Declaring it means a future ingest path reading
#: raw XBRL instances populates the column without a schema change -- and it documents
#: at the parse site why the column is always null today. See ADR-002.
CONCEPT_DDL = (
    "cik STRING, taxonomy STRING, tag STRING, entityName STRING, "
    "units MAP<STRING, ARRAY<STRUCT<"
    "`start` STRING, `end` STRING, val DECIMAL(38,6), accn STRING, "
    "fy INT, fp STRING, form STRING, filed STRING, frame STRING, decimals INT"
    ">>>"
)

GRAIN: tuple[str, ...] = SPEC.business_key


def canonical_concept(taxonomy: Any, tag: Any) -> Any:
    """Map ``(taxonomy, tag)`` to the canonical concept, or null when unmapped.

    Unmapped is not an error: the row is kept with a null ``concept_canonical`` and
    counted as a WARN. Dropping unmapped tags would make adding a concept later
    require a full bronze replay.
    """
    from pyspark.sql import functions as F

    pairs: list[Any] = []
    for mapping in concept_registry.CONCEPT_MAPPINGS:
        pairs.append(F.lit(f"{mapping.taxonomy}|{mapping.tag}"))
        pairs.append(F.lit(mapping.canonical))
    lookup = F.create_map(*pairs)
    return lookup[F.concat_ws("|", taxonomy, tag)]


def build(bronze_df: Any) -> Any:
    """Explode companyconcept payloads into one row per assertion."""
    from pyspark.sql import functions as F

    doc = F.from_json(F.col("payload_json"), CONCEPT_DDL)
    exploded = (
        bronze_df.select(
            F.coalesce(doc.getField("cik").cast("string"), F.col("cik")).alias("_raw_cik"),
            F.coalesce(doc.getField("taxonomy"), F.col("taxonomy")).alias("taxonomy"),
            F.coalesce(doc.getField("tag"), F.col("tag")).alias("concept_tag"),
            F.col("logical_date"),
            F.col("_ingest_batch_id"),
            F.col("_source_file"),
            F.explode(doc.getField("units")).alias("unit", "_facts"),
        )
        .select("*", F.explode(F.col("_facts")).alias("_fact"))
        .drop("_facts")
    )

    fact = F.col("_fact")
    period_start = F.to_date(fact.getField("start"))
    period_end = F.to_date(fact.getField("end"))
    return exploded.select(
        pad_cik(F.col("_raw_cik")).alias("cik"),
        F.col("taxonomy"),
        F.col("concept_tag"),
        canonical_concept(F.col("taxonomy"), F.col("concept_tag")).alias("concept_canonical"),
        F.col("unit"),
        period_start.alias("period_start"),
        period_end.alias("period_end"),
        # An instant fact has no start. period_type is derived from that, never from
        # the form type, and the two must agree (see fact_period_type_valid).
        F.when(period_start.isNull(), F.lit("instant"))
        .otherwise(F.lit("duration"))
        .alias("period_type"),
        normalize_accession(fact.getField("accn")).alias("accession_number"),
        fact.getField("val").cast("decimal(38,6)").alias("value"),
        fact.getField("decimals").cast("int").alias("decimals"),
        fact.getField("fy").cast("int").alias("fiscal_year"),
        fact.getField("fp").alias("fiscal_period"),
        fact.getField("form").alias("form_type"),
        F.to_date(fact.getField("filed")).alias("filed_date"),
        fact.getField("frame").alias("frame"),
        F.col("logical_date"),
        F.col("_ingest_batch_id"),
        F.col("_source_file"),
    )


def grain_counts(df: Any) -> Any:
    """Rows per business key, for the ``fact_grain_unique`` structural check.

    Evaluated on the *source*, before the merge: grain uniqueness is a property of the
    explode, so catching it here means nothing bad is ever written.
    """
    from pyspark.sql import functions as F

    return df.groupBy(*[F.col(f"`{c}`") for c in GRAIN]).agg(
        F.count(F.lit(1)).cast("int").alias("row_count")
    )


def run(spark: Any, settings: Settings, run_ctx: JobRun) -> MergeStats:
    from pyspark.sql import functions as F

    bronze_table = settings.table(schemas.BRONZE_COMPANY_CONCEPT_RAW.fqn)
    target = settings.table(SPEC.fqn)
    quarantine = settings.table(schemas.SILVER_FINANCIAL_FACT_QUARANTINE.fqn)
    logical_table = f"{names.SILVER_SCHEMA}.{SPEC.name}"

    built = build(spark.table(bronze_table))
    run_ctx.add(rows_in=int(built.count()))

    passed = run_dq_and_quarantine(
        spark,
        run_ctx,
        built,
        dq_registry.checks_for(logical_table, kind="row"),
        target_spec=SPEC,
        quarantine_table=quarantine,
        metrics_prefix="silver.financial_fact",
    )

    # The same document can be landed twice on one logical date (a re-fetch); that is a
    # duplicate of the *landing object*, not of the assertion, so collapse it here
    # before the structural check rather than failing the batch over it.
    #
    # Not dropDuplicates: that keeps an arbitrary row, so a re-landed document could
    # change which value survives between runs. dedupe_on picks by an explicit ordering.
    deduped = dedupe_on(passed, GRAIN, ("logical_date", "_ingest_batch_id", "_source_file"))
    _, _, grain_metrics = apply_dq(
        grain_counts(deduped),
        dq_registry.checks_for(logical_table, kind="aggregate"),
        run_ctx.run_id,
        source_table=SPEC.fqn,
    )
    run_ctx.record(grain_metrics, prefix="silver.financial_fact.grain.")

    source = align_to_spec(
        deduped.withColumn("_first_seen_ts", F.current_timestamp()).withColumn(
            "_last_seen_ts", F.current_timestamp()
        ),
        SPEC,
    )
    stats = merge_scd1(
        spark,
        source,
        target,
        keys=GRAIN,
        # _source_file breaks the tie when one logical date lands the same key
        # twice (a re-fetch). Without a total ordering, row_number() picks whichever
        # row the scan reached first and the result stops being reproducible.
        dedupe_order_by=("logical_date", "_ingest_batch_id", "_source_file"),
    )
    run_ctx.record(stats.as_metrics("silver.financial_fact.merge"))
    run_ctx.add(rows_out=stats.rows_target_after)
    return stats
