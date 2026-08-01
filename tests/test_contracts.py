"""Contract-package tests. No Spark, no JVM -- these run in the fast suite."""

from __future__ import annotations

import pytest

from pipelines.contracts import concepts, dq, envelope, names, provenance, schemas
from pipelines.contracts.models import DQCheck, Layer, Severity


def test_every_table_names_a_changeset() -> None:
    """Preflight's whole value is naming the migration; a blank changeset kills it."""
    for spec in schemas.ALL_TABLES:
        assert spec.changeset, f"{spec.fqn} has no changeset"


def test_fqn_is_three_parts() -> None:
    for spec in schemas.ALL_TABLES:
        assert spec.fqn.count(".") == 2


def test_bronze_tables_carry_the_six_metadata_columns() -> None:
    required = {c.name for c in schemas.BRONZE_METADATA_COLUMNS}
    assert len(required) == 6
    for spec in schemas.ALL_TABLES:
        if spec.layer is Layer.BRONZE:
            assert required <= set(spec.column_names), spec.fqn


def test_silver_tables_carry_lineage_columns() -> None:
    required = {c.name for c in schemas.SILVER_LINEAGE_COLUMNS}
    for spec in schemas.ALL_TABLES:
        if spec.layer is Layer.SILVER and not spec.name.endswith("_quarantine"):
            assert required <= set(spec.column_names), spec.fqn


def test_first_seen_ts_is_not_nullable_anywhere() -> None:
    for spec in schemas.ALL_TABLES:
        if "_first_seen_ts" in spec.column_names:
            assert not spec.column("_first_seen_ts").nullable


def test_cik_is_a_string_in_every_table() -> None:
    """AGENTS.global.md rule 4. An int cik loses its leading zeros."""
    for spec in schemas.ALL_TABLES:
        if "cik" in spec.column_names:
            assert spec.column("cik").type_sql == "STRING", spec.fqn


def test_monetary_columns_are_decimal_not_double() -> None:
    for spec in schemas.ALL_TABLES:
        for name in ("value", "original_value", "restated_value", "delta_abs"):
            if name in spec.column_names:
                assert spec.column(name).type_sql.startswith("DECIMAL"), f"{spec.fqn}.{name}"


def test_financial_fact_grain_includes_accession_number() -> None:
    """Feature F-8. Without it, two assertions collapse into one and restatement
    detection becomes impossible -- silently."""
    assert "accession_number" in schemas.SILVER_FINANCIAL_FACT.business_key


def test_table_lookup_rejects_unknown() -> None:
    with pytest.raises(KeyError):
        schemas.table("edgar.silver.does_not_exist")


def test_with_catalog_rebinds_only_the_catalog() -> None:
    rebound = schemas.SILVER_FILING.with_catalog("other")
    assert rebound.fqn == "other.silver.filing"
    assert rebound.columns == schemas.SILVER_FILING.columns


def test_stream_registry() -> None:
    assert set(names.STREAMS) == {"filing_index", "company_submissions", "company_concept"}
    with pytest.raises(KeyError, match="unknown landing stream"):
        names.stream("nope")


def test_landing_and_checkpoint_paths() -> None:
    assert names.landing_path("/root/", "filing_index") == "/root/filing_index"
    assert (
        names.landing_path("s3://b/edgar", "company_concept", "2026-07-31")
        == "s3://b/edgar/company_concept/logical_date=2026-07-31"
    )
    # Per-stream, never shared: a shared schemaLocation merges the streams' schemas.
    a = names.checkpoint_path("/cp", "filing_index")
    b = names.checkpoint_path("/cp", "company_concept")
    assert a != b


def test_envelope_ddl_covers_every_field() -> None:
    ddl = envelope.envelope_json_schema_ddl()
    for field in envelope.ENVELOPE_FIELDS:
        assert field in ddl
    assert "payload_json STRING" in ddl


def test_dq_registry_has_all_three_severities() -> None:
    present = {c.severity for c in dq.CHECKS}
    assert present == {Severity.REJECT, Severity.WARN, Severity.REJECT_BATCH}


def test_scd2_invariants_are_reject_batch() -> None:
    """Rule 8: one bad row means the dimension is structurally broken."""
    for name in ("company_exactly_one_current", "company_no_overlapping_versions"):
        check = next(c for c in dq.CHECKS if c.name == name)
        assert check.severity is Severity.REJECT_BATCH


def test_dq_check_names_are_unique() -> None:
    seen = [c.name for c in dq.CHECKS]
    assert len(seen) == len(set(seen))


def test_checks_for_filters_by_table_and_kind() -> None:
    row_checks = dq.checks_for("silver.financial_fact", kind="row")
    agg_checks = dq.checks_for("silver.financial_fact", kind="aggregate")
    assert row_checks and agg_checks
    assert all(c.kind == "row" for c in row_checks)
    assert {c.name for c in agg_checks} == {"fact_grain_unique"}


def test_period_order_check_permits_instant_facts() -> None:
    """The null branch is load-bearing: instant facts have no period_start."""
    check = next(c for c in dq.CHECKS if c.name == "fact_period_order")
    assert "period_start IS NULL" in check.expression


def test_rescued_data_check_is_warn_not_silent() -> None:
    assert dq.BRONZE_RESCUED_CHECK.severity is Severity.WARN


def test_concept_mapping_lookup() -> None:
    assert concepts.mappings_for_tag("us-gaap", "Revenues").canonical == "revenue_total"
    assert concepts.mappings_for_tag("us-gaap", "NotATag") is None


def test_concept_preferences_are_unique_within_a_canonical() -> None:
    """The tie-break must be total, or the winning tag depends on join order."""
    by_canonical: dict[str, list[int]] = {}
    for mapping in concepts.CONCEPT_MAPPINGS:
        by_canonical.setdefault(mapping.canonical, []).append(mapping.preference)
    for canonical, prefs in by_canonical.items():
        assert len(prefs) == len(set(prefs)), canonical


def test_tags_to_fetch_is_deduped_and_ordered() -> None:
    tags = concepts.tags_to_fetch()
    assert len(tags) == len(set(tags))
    assert ("us-gaap", "Revenues") in tags


def test_provenance_reports_mirror_until_repo_one_publishes() -> None:
    assert provenance() in ("mirror", "published")


def test_column_ddl_escapes_quotes() -> None:
    from pipelines.contracts.models import ColumnSpec

    rendered = ColumnSpec("x", "STRING", False, "it's fine").ddl()
    assert "NOT NULL" in rendered
    assert "it''s fine" in rendered


def test_dq_check_defaults_to_row_kind() -> None:
    check = DQCheck("n", "t", "true", Severity.WARN, "d")
    assert check.kind == "row"
