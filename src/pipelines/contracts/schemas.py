"""Table specifications for every table this repo reads or writes.

This repo **never** issues ``CREATE TABLE`` (AGENTS.md rule 1) -- repo 1's Liquibase
owns DDL. These specs exist for three read-only purposes:

1. preflight can name the missing table *and* the changeset that creates it;
2. writers can align columns by name before a MERGE instead of by position;
3. CI can assert that every column this repo touches exists in the pinned contracts
   version.

``local_ddl`` in ``tools/`` also renders these specs, but only to stand in for
Liquibase on a laptop where no Databricks workspace exists. That is a test harness,
not a pipeline: nothing under ``src/pipelines`` creates a table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from .models import ColumnSpec, Layer, TableSpec
from .names import BRONZE_SCHEMA, CATALOG, GOLD_SCHEMA, SILVER_SCHEMA

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps this module JVM-free
    from pyspark.sql.types import StructType

__all__ = [
    "ALL_TABLES",
    "BRONZE_METADATA_COLUMNS",
    "QUARANTINE_COLUMNS",
    "SILVER_LINEAGE_COLUMNS",
    "TABLES",
    "struct_for",
    "table",
]

C = ColumnSpec

# --------------------------------------------------------------------------------------
# Shared column blocks
# --------------------------------------------------------------------------------------

#: The six metadata columns present on every bronze table (data contracts section 2.1).
BRONZE_METADATA_COLUMNS: Final[tuple[ColumnSpec, ...]] = (
    C("_ingest_batch_id", "STRING", False, "Deterministic batch id: <stream>-<logical_date>"),
    C("_ingest_ts", "TIMESTAMP", False, "When this row was appended to bronze"),
    C("_source_file", "STRING", False, "_metadata.file_path of the landing object"),
    C("_source_system", "STRING", False, "Always sec_edgar"),
    C("_envelope_version", "STRING", False, "Landing envelope version the row was parsed as"),
    C("_rescued_data", "STRING", True, "Auto Loader rescued columns; non-null is a WARN"),
)

#: Lineage columns on every silver table. ``_first_seen_ts`` is written on insert and
#: never updated (AGENTS.md rule 4) -- it is the answer to "when did we first see this",
#: which is half the reason the table exists.
SILVER_LINEAGE_COLUMNS: Final[tuple[ColumnSpec, ...]] = (
    C("_first_seen_ts", "TIMESTAMP", False, "Set on INSERT, NEVER updated"),
    C("_last_seen_ts", "TIMESTAMP", False, "Updated on every MERGE that matches"),
    C("_ingest_batch_id", "STRING", False, "Batch that last touched the row"),
    C("_source_file", "STRING", True, "Landing object the row was last derived from"),
)

#: Quarantine tables share one shape across all silver entities so that ``apply_dq``
#: stays domain-agnostic. The rejected record is preserved verbatim as JSON: a
#: quarantine row you cannot replay is a log line with extra steps.
QUARANTINE_COLUMNS: Final[tuple[ColumnSpec, ...]] = (
    C(
        "_dq_record_id",
        "STRING",
        False,
        "sha2(source_table|check|record_json). Business key: MERGE, not append",
    ),
    C("_dq_run_id", "STRING", False, "Run that most recently quarantined the row"),
    C("_dq_check_name", "STRING", False, "First failing check"),
    C("_dq_failure_reason", "STRING", False, "Human-readable reason incl. the expression"),
    C("_quarantined_at", "TIMESTAMP", False, "Quarantine write time"),
    C("_source_table", "STRING", False, "Silver table the row was destined for"),
    C("_source_file", "STRING", True, "Landing object the row came from"),
    C("_ingest_batch_id", "STRING", True, "Batch that produced the row"),
    C("record_json", "STRING", False, "The rejected record, serialized verbatim"),
)


def _quarantine(name: str, changeset: str, source_table: str) -> TableSpec:
    return TableSpec(
        catalog=CATALOG,
        schema=SILVER_SCHEMA,
        name=name,
        layer=Layer.SILVER,
        columns=QUARANTINE_COLUMNS,
        changeset=changeset,
        business_key=("_dq_record_id",),
        comment=f"Rows rejected on the way into {source_table}.",
    )


# --------------------------------------------------------------------------------------
# Bronze -- append only, one table per landing stream
# --------------------------------------------------------------------------------------

_BRONZE_CHANGESET = "010-bronze.yaml"

BRONZE_FILING_INDEX_RAW = TableSpec(
    catalog=CATALOG,
    schema=BRONZE_SCHEMA,
    name="filing_index_raw",
    layer=Layer.BRONZE,
    columns=(
        C("logical_date", "DATE", False, "Business date of the landing partition"),
        C("resource_id", "STRING", True, "Envelope resource id"),
        C("fetched_at", "TIMESTAMP", True, "When repo 3 fetched the index file"),
        C("form_type", "STRING", True, "Raw form type from the index, untouched"),
        C("company_name", "STRING", True),
        C("cik", "STRING", True, "Raw cik from the index; NOT yet zero-padded"),
        C("date_filed", "STRING", True, "Raw YYYYMMDD text; typed in silver"),
        C("accession_number", "STRING", True, "Raw accession; normalized in silver"),
        C("file_name", "STRING", True, "Archives path of the submission text file"),
        *BRONZE_METADATA_COLUMNS,
    ),
    changeset=_BRONZE_CHANGESET,
    partition_by=("logical_date",),
    comment="Daily-index rows, passthrough. Append only.",
)

BRONZE_COMPANY_SUBMISSIONS_RAW = TableSpec(
    catalog=CATALOG,
    schema=BRONZE_SCHEMA,
    name="company_submissions_raw",
    layer=Layer.BRONZE,
    columns=(
        C("logical_date", "DATE", False),
        C("resource_id", "STRING", True, "cik as fetched"),
        C("fetched_at", "TIMESTAMP", True),
        C("cik", "STRING", True),
        C("payload_json", "STRING", True, "Whole submissions document, unexploded"),
        *BRONZE_METADATA_COLUMNS,
    ),
    changeset=_BRONZE_CHANGESET,
    partition_by=("logical_date",),
    comment="Company submissions documents. Payload kept opaque on purpose.",
)

BRONZE_COMPANY_CONCEPT_RAW = TableSpec(
    catalog=CATALOG,
    schema=BRONZE_SCHEMA,
    name="company_concept_raw",
    layer=Layer.BRONZE,
    columns=(
        C("logical_date", "DATE", False),
        C("resource_id", "STRING", True, "cik/taxonomy/tag"),
        C("fetched_at", "TIMESTAMP", True),
        C("cik", "STRING", True),
        C("taxonomy", "STRING", True),
        C("tag", "STRING", True),
        C("payload_json", "STRING", True, "Whole companyconcept document, unexploded"),
        *BRONZE_METADATA_COLUMNS,
    ),
    changeset=_BRONZE_CHANGESET,
    partition_by=("logical_date",),
    comment="XBRL companyconcept documents. Payload kept opaque on purpose.",
)

# --------------------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------------------

_SILVER_CHANGESET = "020-silver.yaml"

SILVER_FILING = TableSpec(
    catalog=CATALOG,
    schema=SILVER_SCHEMA,
    name="filing",
    layer=Layer.SILVER,
    columns=(
        C("accession_number", "STRING", False, "Normalized ##########-##-######"),
        C("cik", "STRING", False, "Zero-padded to 10"),
        C("company_name", "STRING", True),
        C("form_type", "STRING", False, "Upper-cased, e.g. 10-K/A"),
        C("base_form_type", "STRING", False, "form_type with the amendment suffix removed"),
        C("is_amendment", "BOOLEAN", False, "True when form_type ends in /A"),
        C("filed_date", "DATE", False),
        C("primary_doc_url", "STRING", True),
        C("logical_date", "DATE", False, "Landing partition the row was last seen in"),
        # SCD-2, added in contracts v1.1.0 (changeset 060). Before this the table was
        # overwrite-on-merge, so an amendment destroyed the pre-amendment row -- which is
        # the history gold.restatement_event is built from.
        C("filing_sk", "STRING", False, "sha2 of accession_number. Identifies the filing."),
        C("version_number", "INT", False, "Dense, 1-based per accession_number"),
        C("valid_from", "DATE", False, "SCD-2 open bound, inclusive"),
        C("valid_to", "DATE", True, "SCD-2 close bound; null while current"),
        C("is_current", "BOOLEAN", False, "Exactly one true per accession -- reject_batch"),
        C("_hash_diff", "STRING", False, "sha2 over FILING_TRACKED_COLUMNS"),
        *SILVER_LINEAGE_COLUMNS,
    ),
    changeset=_SILVER_CHANGESET,
    business_key=("accession_number",),
    comment="SCD-2 filing dimension. One row per version of a filing.",
)

SILVER_FILING_QUARANTINE = _quarantine("filing_quarantine", _SILVER_CHANGESET, "silver.filing")

SILVER_COMPANY = TableSpec(
    catalog=CATALOG,
    schema=SILVER_SCHEMA,
    name="company",
    layer=Layer.SILVER,
    columns=(
        C("cik", "STRING", False, "Zero-padded to 10. Natural key."),
        C("company_name", "STRING", True),
        C("sic", "STRING", True),
        C("sic_description", "STRING", True),
        C("ein", "STRING", True),
        C("entity_type", "STRING", True),
        C("state_of_incorporation", "STRING", True),
        C("fiscal_year_end", "STRING", True, "MMDD"),
        C("tickers", "ARRAY<STRING>", True, "Sorted before hashing -- see _hash_diff"),
        C("exchanges", "ARRAY<STRING>", True, "Sorted before hashing -- see _hash_diff"),
        C("former_names", "ARRAY<STRING>", True, "Sorted before hashing -- see _hash_diff"),
        # v1.1.0 (changeset 060): the interval was already here, the ordinal and the
        # surrogate key are new.
        C("company_sk", "STRING", False, "sha2 of cik. Identifies the company, not the version."),
        C("version_number", "INT", False, "Dense, 1-based per cik"),
        C("valid_from", "DATE", False, "SCD-2 open bound, inclusive"),
        C("valid_to", "DATE", True, "SCD-2 close bound, inclusive; null while current"),
        C("is_current", "BOOLEAN", False, "Exactly one true per cik -- reject_batch"),
        C("_hash_diff", "STRING", False, "sha2 over sorted tracked columns"),
        *SILVER_LINEAGE_COLUMNS,
    ),
    changeset=_SILVER_CHANGESET,
    business_key=("cik",),
    comment="SCD-2 company dimension.",
)

SILVER_COMPANY_QUARANTINE = _quarantine("company_quarantine", _SILVER_CHANGESET, "silver.company")

#: The grain that makes restatement detection possible. ``accession_number`` is part of
#: it: two accessions asserting the same (cik, concept, period) must produce two rows,
#: because the difference between them *is* the restatement (AGENTS.md F-8).
SILVER_FINANCIAL_FACT = TableSpec(
    catalog=CATALOG,
    schema=SILVER_SCHEMA,
    name="financial_fact",
    layer=Layer.SILVER,
    columns=(
        C("cik", "STRING", False, "Zero-padded to 10"),
        C("taxonomy", "STRING", False, "e.g. us-gaap"),
        C("concept_tag", "STRING", False, "Source tag as reported"),
        C("concept_canonical", "STRING", True, "Canonical concept, null when unmapped"),
        C("unit", "STRING", False, "e.g. USD, shares, USD/shares"),
        C("period_start", "DATE", True, "Null for instant facts"),
        C("period_end", "DATE", False),
        C("period_type", "STRING", False, "instant | duration"),
        C("accession_number", "STRING", False, "PART OF THE GRAIN. Do not collapse."),
        C("value", "DECIMAL(38,6)", True),
        C("decimals", "INT", True, "XBRL decimals; negative means rounded to 10^-decimals"),
        C("fiscal_year", "INT", True),
        C("fiscal_period", "STRING", True),
        C("form_type", "STRING", True),
        C("filed_date", "DATE", False),
        C("frame", "STRING", True),
        C("logical_date", "DATE", False),
        # Assertion versioning, contracts v1.1.0 (changeset 060). A restatement is a new
        # assertion about the same period, not a correction to a row, so both rows are
        # kept and ordered rather than one overwriting the other.
        #
        # fact_sk is the *period* identity and deliberately excludes accession_number,
        # even though accession IS part of the row grain. That is the point: the rows
        # that share a fact_sk are the competing assertions, and the difference between
        # them is the restatement.
        C("fact_sk", "STRING", False, "sha2 of cik|concept_canonical|period_end|unit"),
        C(
            "assertion_version",
            "INT",
            False,
            "Dense, 1-based per fact_sk by (filed_date, accession)",
        ),
        C("is_current_assertion", "BOOLEAN", False, "Exactly one true per fact_sk"),
        C(
            "superseded_by_accession",
            "STRING",
            True,
            "Accession that replaced this one; null when current",
        ),
        *SILVER_LINEAGE_COLUMNS,
    ),
    changeset=_SILVER_CHANGESET,
    business_key=(
        "cik",
        "taxonomy",
        "concept_tag",
        "unit",
        "period_start",
        "period_end",
        "period_type",
        "accession_number",
    ),
    comment="Bitemporal XBRL facts: one row per assertion, not per period.",
)

SILVER_FINANCIAL_FACT_QUARANTINE = _quarantine(
    "financial_fact_quarantine", _SILVER_CHANGESET, "silver.financial_fact"
)

# --------------------------------------------------------------------------------------
# Gold
# --------------------------------------------------------------------------------------

_GOLD_CHANGESET = "030-gold.yaml"

GOLD_FINANCIALS_CURRENT = TableSpec(
    catalog=CATALOG,
    schema=GOLD_SCHEMA,
    name="financials_current",
    layer=Layer.GOLD,
    columns=(
        C("cik", "STRING", False),
        C("company_name", "STRING", True),
        C("concept_canonical", "STRING", False),
        C("unit", "STRING", False),
        C("period_start", "DATE", True),
        C("period_end", "DATE", False),
        C("period_type", "STRING", False),
        C("value", "DECIMAL(38,6)", True),
        C("decimals", "INT", True),
        C("fiscal_year", "INT", True),
        C("fiscal_period", "STRING", True),
        C("accession_number", "STRING", False, "The accession the winning value came from"),
        C("form_type", "STRING", True),
        C("filed_date", "DATE", False),
        C("assertion_count", "INT", False, "How many accessions asserted this period"),
        C("was_restated", "BOOLEAN", False, "True when a restatement event exists"),
        C("_generated_at", "TIMESTAMP", False),
        C("_run_id", "STRING", False),
    ),
    changeset=_GOLD_CHANGESET,
    business_key=("cik", "concept_canonical", "unit", "period_start", "period_end", "period_type"),
    comment="Latest assertion per (cik, concept, period). Rebuilt each run.",
)

GOLD_RESTATEMENT_EVENT = TableSpec(
    catalog=CATALOG,
    schema=GOLD_SCHEMA,
    name="restatement_event",
    layer=Layer.GOLD,
    columns=(
        C("restatement_id", "STRING", False, "Deterministic sha2 of the grain + accessions"),
        C("cik", "STRING", False),
        C("company_name", "STRING", True),
        C("concept_canonical", "STRING", False),
        C("unit", "STRING", False),
        C("period_start", "DATE", True),
        C("period_end", "DATE", False),
        C("period_type", "STRING", False),
        C("original_accession_number", "STRING", False),
        C("original_form_type", "STRING", True),
        C("original_filed_date", "DATE", False),
        C("original_value", "DECIMAL(38,6)", False),
        C("original_decimals", "INT", True),
        C("restated_accession_number", "STRING", False),
        C("restated_form_type", "STRING", True),
        C("restated_filed_date", "DATE", False),
        C("restated_value", "DECIMAL(38,6)", False),
        C("restated_decimals", "INT", True),
        C("delta_abs", "DECIMAL(38,6)", False),
        C("delta_pct", "DOUBLE", True, "Null when original_value = 0, never an exception"),
        C("materiality_band", "STRING", False, "immaterial | notable | material -- heuristic"),
        C("days_to_restatement", "INT", False),
        C("_generated_at", "TIMESTAMP", False),
        C("_run_id", "STRING", False),
    ),
    changeset=_GOLD_CHANGESET,
    business_key=("restatement_id",),
    comment="Consecutive assertions of one period that disagree beyond tolerance.",
)

GOLD_FILING_ACTIVITY_DAILY = TableSpec(
    catalog=CATALOG,
    schema=GOLD_SCHEMA,
    name="filing_activity_daily",
    layer=Layer.GOLD,
    columns=(
        C("filed_date", "DATE", False),
        C("base_form_type", "STRING", False),
        C("filing_count", "INT", False),
        C("amendment_count", "INT", False),
        C("distinct_cik_count", "INT", False),
        C("_generated_at", "TIMESTAMP", False),
        C("_run_id", "STRING", False),
    ),
    changeset=_GOLD_CHANGESET,
    business_key=("filed_date", "base_form_type"),
    comment="Filing counts per day per base form type.",
)

GOLD_COMPANY_PROFILE = TableSpec(
    catalog=CATALOG,
    schema=GOLD_SCHEMA,
    name="company_profile",
    layer=Layer.GOLD,
    columns=(
        C("cik", "STRING", False),
        C("company_name", "STRING", True),
        C("sic", "STRING", True),
        C("sic_description", "STRING", True),
        C("entity_type", "STRING", True),
        C("state_of_incorporation", "STRING", True),
        C("fiscal_year_end", "STRING", True),
        C("tickers", "ARRAY<STRING>", True),
        C("exchanges", "ARRAY<STRING>", True),
        C("filing_count", "INT", False),
        C("first_filed_date", "DATE", True),
        C("last_filed_date", "DATE", True),
        C("restatement_count", "INT", False),
        C("_generated_at", "TIMESTAMP", False),
        C("_run_id", "STRING", False),
    ),
    changeset=_GOLD_CHANGESET,
    business_key=("cik",),
    comment="Current company attributes plus filing and restatement counts.",
)

TABLES: Final[dict[str, TableSpec]] = {
    t.fqn: t
    for t in (
        BRONZE_FILING_INDEX_RAW,
        BRONZE_COMPANY_SUBMISSIONS_RAW,
        BRONZE_COMPANY_CONCEPT_RAW,
        SILVER_FILING,
        SILVER_FILING_QUARANTINE,
        SILVER_COMPANY,
        SILVER_COMPANY_QUARANTINE,
        SILVER_FINANCIAL_FACT,
        SILVER_FINANCIAL_FACT_QUARANTINE,
        GOLD_FINANCIALS_CURRENT,
        GOLD_RESTATEMENT_EVENT,
        GOLD_FILING_ACTIVITY_DAILY,
        GOLD_COMPANY_PROFILE,
    )
}

ALL_TABLES: Final[tuple[TableSpec, ...]] = tuple(TABLES.values())

#: Gold tables exported to Parquet for repo 5, in a fixed order so the manifest is
#: byte-stable across runs with identical data.
EXPORT_TABLES: Final[tuple[TableSpec, ...]] = (
    GOLD_FINANCIALS_CURRENT,
    GOLD_RESTATEMENT_EVENT,
    GOLD_FILING_ACTIVITY_DAILY,
    GOLD_COMPANY_PROFILE,
)


def table(fqn: str) -> TableSpec:
    """Look up a table spec by fully-qualified name."""
    try:
        return TABLES[fqn]
    except KeyError:
        raise KeyError(f"{fqn!r} is not a contract table") from None


def struct_for(spec: TableSpec) -> StructType:
    """Build the Spark ``StructType`` for a spec.

    Imported lazily so that this module keeps working without a JVM -- CI's
    contract-compat job and the DDL renderer both run without Spark.
    """
    from pyspark.sql.types import StructType as _StructType

    ddl = ", ".join(f"`{c.name}` {c.type_sql}" for c in spec.columns)
    parsed: Any = _StructType.fromDDL(ddl)
    return parsed  # type: ignore[no-any-return]
