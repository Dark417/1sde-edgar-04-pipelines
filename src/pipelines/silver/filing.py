"""L3 -- ``silver.filing`` (feature F-6, data contracts section 3.1).

The table whose idempotency test decides whether anything downstream is trustworthy:
running this twice must produce an identical row count **and** identical
``_first_seen_ts`` values. It does so because the write is a MERGE on
``accession_number`` and ``_first_seen_ts`` is never in an UPDATE set.
"""

from __future__ import annotations

from typing import Any

from pipelines.config import Settings
from pipelines.contracts import dq as dq_registry
from pipelines.contracts import names, schemas
from pipelines.framework.delta_ops import rollback_on_failure
from pipelines.framework.keys import surrogate_key
from pipelines.framework.merge import MergeStats, merge_scd2
from pipelines.framework.metrics import JobRun

from .common import (
    base_form_type,
    is_amendment,
    normalize_accession,
    normalize_form_type,
    pad_cik,
    parse_edgar_date,
    primary_doc_url,
    run_dq_and_quarantine,
)

__all__ = ["TRACKED_COLUMNS", "build", "run"]

SPEC = schemas.SILVER_FILING

#: A change in any of these opens a new SCD-2 version of the filing.
#:
#: Declared, not inferred. Inferring "every non-key column" would mean a new upstream
#: field silently rewrites history the first time it appears. These three are the ones
#: that actually change when a filing is amended: the form gains an /A suffix, the filer
#: name can be corrected, and the primary document is republished at a new URL.
#:
#: `filed_date` is deliberately absent -- the original filing date is a property of the
#: original submission, and an amendment is a *new* version, not a re-dating of the old.
TRACKED_COLUMNS: tuple[str, ...] = (
    "form_type",
    "base_form_type",
    "is_amendment",
    "company_name",
    "primary_doc_url",
)


def build(bronze_df: Any) -> Any:
    """Normalize bronze index rows into the silver shape.

    Deduped on ``accession_number`` keeping the latest ``logical_date`` -- the same
    filing appears in more than one daily index when a submission is disseminated
    across a date boundary, and Delta refuses a MERGE whose source has two rows per
    target row.
    """
    from pyspark.sql import functions as F

    form = normalize_form_type(F.col("form_type"))
    return bronze_df.select(
        # filing_sk identifies the filing across all of its versions.
        surrogate_key(normalize_accession(F.col("accession_number"))).alias("filing_sk"),
        normalize_accession(F.col("accession_number")).alias("accession_number"),
        pad_cik(F.col("cik")).alias("cik"),
        F.trim(F.col("company_name")).alias("company_name"),
        form.alias("form_type"),
        base_form_type(form).alias("base_form_type"),
        is_amendment(form).alias("is_amendment"),
        parse_edgar_date(F.col("date_filed")).alias("filed_date"),
        primary_doc_url(F.col("file_name")).alias("primary_doc_url"),
        F.col("logical_date"),
        F.col("_ingest_batch_id"),
        F.col("_source_file"),
    )


def run(spark: Any, settings: Settings, run_ctx: JobRun) -> MergeStats:
    """Read bronze, normalize, run DQ, MERGE into ``silver.filing``."""

    bronze_table = settings.table(schemas.BRONZE_FILING_INDEX_RAW.fqn)
    target = settings.table(SPEC.fqn)
    quarantine = settings.table(schemas.SILVER_FILING_QUARANTINE.fqn)

    bronze_df = spark.table(bronze_table)
    built = build(bronze_df)
    run_ctx.add(rows_in=int(built.count()))

    passed = run_dq_and_quarantine(
        spark,
        run_ctx,
        built,
        dq_registry.checks_for(f"{names.SILVER_SCHEMA}.{SPEC.name}", kind="row"),
        target_spec=SPEC,
        quarantine_table=quarantine,
        metrics_prefix="silver.filing",
    )

    # SCD-2 since contracts v1.1.0. This was merge_scd1, which overwrote in place -- so
    # when a 10-K/A superseded a 10-K, the original form_type and primary_doc_url were
    # gone. That is the history gold.restatement_event reads, so losing it quietly
    # removed the evidence for the restatements the mart is supposed to report.
    with rollback_on_failure(spark, target):
        stats = merge_scd2(
            spark,
            passed,
            target,
            natural_key=("accession_number",),
            tracked_cols=TRACKED_COLUMNS,
            logical_date=settings.logical_date,
            # Latest sighting wins when a filing appears in two daily indexes.
            # _source_file breaks the tie when one logical date lands the same key
            # twice (a re-fetch). Without a total ordering, row_number() picks whichever
            # row the scan reached first and the result stops being reproducible.
            dedupe_order_by=("logical_date", "_ingest_batch_id", "_source_file"),
        )
    run_ctx.record(stats.as_metrics("silver.filing.merge"))
    run_ctx.add(rows_out=stats.rows_target_after)
    return stats
