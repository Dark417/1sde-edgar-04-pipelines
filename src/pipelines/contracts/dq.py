"""The data-quality registry.

Rule text lives here, not in the transform modules, so that changing a threshold is a
contract change with a diff someone can review -- not a one-character edit buried in a
200-line transform.

Reading the expressions: **a row passes when the expression evaluates to true.** Null
counts as a failure, which is why every check spells out its own null handling instead
of leaning on three-valued logic.
"""

from __future__ import annotations

from typing import Final

from .models import DQCheck, Severity

__all__ = ["BRONZE_RESCUED_CHECK", "CHECKS", "checks_for"]

R = Severity.REJECT
W = Severity.WARN
RB = Severity.REJECT_BATCH

_ACCESSION_RE = r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$"
_CIK_RE = r"^[0-9]{10}$"

#: Applied to every bronze stream. ``_rescued_data`` non-null means the source grew a
#: field the contract does not know about -- a WARN, never a silent pass, because it is
#: the only signal we get that the SEC changed a payload shape (AGENTS.md rule 11).
BRONZE_RESCUED_CHECK: Final[DQCheck] = DQCheck(
    name="bronze_rescued_data_absent",
    table="bronze.*",
    expression="_rescued_data IS NULL",
    severity=W,
    description="Auto Loader rescued one or more columns; the landing payload changed shape.",
)

CHECKS: Final[tuple[DQCheck, ...]] = (
    # ---------------------------------------------------------------- silver.filing
    DQCheck(
        name="filing_accession_format",
        table="silver.filing",
        expression=f"accession_number IS NOT NULL AND accession_number RLIKE '{_ACCESSION_RE}'",
        severity=R,
        description="accession_number must be normalized to ##########-##-######.",
    ),
    DQCheck(
        name="filing_cik_zero_padded",
        table="silver.filing",
        expression=f"cik IS NOT NULL AND cik RLIKE '{_CIK_RE}'",
        severity=R,
        description="cik is a STRING zero-padded to 10 everywhere in the project.",
    ),
    DQCheck(
        name="filing_form_type_present",
        table="silver.filing",
        expression="form_type IS NOT NULL AND length(trim(form_type)) > 0",
        severity=R,
        description="A filing with no form type cannot be classified downstream.",
    ),
    DQCheck(
        name="filing_filed_date_present",
        table="silver.filing",
        expression="filed_date IS NOT NULL",
        severity=R,
        description="filed_date drives every gold time series.",
    ),
    DQCheck(
        name="filing_filed_date_not_after_logical_date",
        table="silver.filing",
        expression="filed_date IS NULL OR filed_date <= date_add(logical_date, 1)",
        severity=R,
        description=(
            "A filing cannot be filed after the index that lists it. Compared against "
            "logical_date rather than current_date so the check is deterministic on replay."
        ),
    ),
    DQCheck(
        name="filing_company_name_present",
        table="silver.filing",
        expression="company_name IS NOT NULL AND length(trim(company_name)) > 0",
        severity=W,
        description="Missing filer name is survivable; the filing is still countable.",
    ),
    # --------------------------------------------------------------- silver.company
    DQCheck(
        name="company_cik_zero_padded",
        table="silver.company",
        expression=f"cik IS NOT NULL AND cik RLIKE '{_CIK_RE}'",
        severity=R,
        description="cik is a STRING zero-padded to 10 everywhere in the project.",
    ),
    DQCheck(
        name="company_name_present",
        table="silver.company",
        expression="company_name IS NOT NULL AND length(trim(company_name)) > 0",
        severity=W,
        description="Missing name is survivable; the dimension row is still joinable.",
    ),
    DQCheck(
        name="company_valid_from_present",
        table="silver.company",
        expression="valid_from IS NOT NULL",
        severity=R,
        description="An SCD-2 row with no open bound cannot be point-in-time queried.",
    ),
    DQCheck(
        name="company_exactly_one_current",
        table="silver.company",
        expression="current_count = 1",
        severity=RB,
        kind="aggregate",
        description=(
            "Exactly one is_current row per cik. Two current rows fan out every "
            "downstream join and double every aggregate, so this fails the batch."
        ),
    ),
    DQCheck(
        name="company_no_overlapping_versions",
        table="silver.company",
        expression="overlap_count = 0",
        severity=RB,
        kind="aggregate",
        description="SCD-2 validity intervals for one cik must not overlap.",
    ),
    # -------------------------------------------------------- silver.financial_fact
    DQCheck(
        name="fact_cik_zero_padded",
        table="silver.financial_fact",
        expression=f"cik IS NOT NULL AND cik RLIKE '{_CIK_RE}'",
        severity=R,
        description="cik is a STRING zero-padded to 10 everywhere in the project.",
    ),
    DQCheck(
        name="fact_accession_format",
        table="silver.financial_fact",
        expression=f"accession_number IS NOT NULL AND accession_number RLIKE '{_ACCESSION_RE}'",
        severity=R,
        description=(
            "accession_number is part of the grain; a malformed one silently merges two "
            "assertions into one and hides a restatement."
        ),
    ),
    DQCheck(
        name="fact_unit_present",
        table="silver.financial_fact",
        expression="unit IS NOT NULL AND length(trim(unit)) > 0",
        severity=R,
        description="Comparing values across units is meaningless; the unit is required.",
    ),
    DQCheck(
        name="fact_value_present",
        table="silver.financial_fact",
        expression="value IS NOT NULL",
        severity=R,
        description="A fact with no value carries no information.",
    ),
    DQCheck(
        name="fact_period_end_present",
        table="silver.financial_fact",
        expression="period_end IS NOT NULL",
        severity=R,
        description="period_end anchors both instant and duration facts.",
    ),
    DQCheck(
        name="fact_period_order",
        table="silver.financial_fact",
        expression="period_start IS NULL OR period_end >= period_start",
        severity=R,
        description=(
            "Duration facts must not end before they start. Instant facts have a null "
            "period_start and MUST pass -- the null branch is load-bearing, not defensive."
        ),
    ),
    DQCheck(
        name="fact_period_type_valid",
        table="silver.financial_fact",
        expression=(
            "period_type IN ('instant', 'duration') AND "
            "(period_type = 'instant') = (period_start IS NULL)"
        ),
        severity=R,
        description="period_type must agree with the presence of period_start.",
    ),
    DQCheck(
        name="fact_concept_mapped",
        table="silver.financial_fact",
        expression="concept_canonical IS NOT NULL",
        severity=W,
        description=(
            "Unmapped tags are kept with a null canonical concept. The WARN count is how "
            "we notice a taxonomy tag we should add to the mapping."
        ),
    ),
    DQCheck(
        name="fact_grain_unique",
        table="silver.financial_fact",
        expression="row_count = 1",
        severity=RB,
        kind="aggregate",
        description=(
            "One row per (cik, taxonomy, tag, unit, period, accession). A duplicate means "
            "the explode double-counted and every gold aggregate is wrong."
        ),
    ),
)


def checks_for(table: str, *, kind: str | None = None) -> tuple[DQCheck, ...]:
    """Checks registered for a logical table name such as ``silver.filing``."""
    return tuple(c for c in CHECKS if c.table == table and (kind is None or c.kind == kind))
