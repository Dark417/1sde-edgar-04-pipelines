"""L3 -- normalization primitives and the shared silver write path.

The normalizers are small enough to look obvious and are the reason silver can have a
business key at all. Each one is unit-tested against the exact inputs named in
`AGENTS.md` F-6, including lowercase input, because "obvious" transformations are
exactly the ones that get quietly broken.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from edgar_lakehouse_contracts import schemas
from edgar_lakehouse_contracts.models import TableSpec

from pipelines.dq_model import DQCheck
from pipelines.framework.dq import apply_dq
from pipelines.framework.merge import merge_scd1
from pipelines.framework.metrics import JobRun

__all__ = [
    "align_to_spec",
    "base_form_type",
    "is_amendment",
    "normalize_accession",
    "normalize_form_type",
    "pad_cik",
    "parse_edgar_date",
    "primary_doc_url",
    "run_dq_and_quarantine",
]


def normalize_accession(col: Any) -> Any:
    """Normalize an accession number to ``##########-##-######``.

    Accepts the hyphenated form and the bare 18-digit form (which is what appears in
    Archives URLs). Anything else becomes null, which the ``filing_accession_format``
    check then quarantines with the original bytes intact -- silently "fixing" a
    malformed accession would merge two filings into one.
    """
    from pyspark.sql import functions as F

    trimmed = F.trim(col)
    digits = F.regexp_replace(trimmed, "[^0-9]", "")
    return (
        F.when(trimmed.rlike(r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$"), trimmed)
        .when(
            F.length(digits) == 18,
            F.concat_ws(
                "-",
                F.substring(digits, 1, 10),
                F.substring(digits, 11, 2),
                F.substring(digits, 13, 6),
            ),
        )
        .otherwise(F.lit(None).cast("string"))
    )


def pad_cik(col: Any) -> Any:
    """Zero-pad a CIK to 10 characters as a STRING.

    ``cik`` is a STRING everywhere in the project (AGENTS.global.md rule 4). An int
    CIK loses its leading zeros, and half the joins in this project are string joins
    against a padded value.
    """
    from pyspark.sql import functions as F

    digits = F.regexp_replace(F.trim(col), "[^0-9]", "")
    return F.when(
        (F.length(digits) > 0) & (F.length(digits) <= 10), F.lpad(digits, 10, "0")
    ).otherwise(F.lit(None).cast("string"))


def normalize_form_type(col: Any) -> Any:
    """Upper-case and trim. Filing agents submit ``10-k`` and ``10-K`` both."""
    from pyspark.sql import functions as F

    return F.when(F.length(F.trim(col)) > 0, F.upper(F.trim(col))).otherwise(
        F.lit(None).cast("string")
    )


def is_amendment(form_type: Any) -> Any:
    """True when the (already normalized) form type ends in ``/A``."""
    from pyspark.sql import functions as F

    return F.coalesce(form_type.rlike(r"/A$"), F.lit(False))


def base_form_type(form_type: Any) -> Any:
    """Strip the amendment suffix: ``10-K/A -> 10-K``, ``S-1/A -> S-1``, ``8-K -> 8-K``."""
    from pyspark.sql import functions as F

    return F.regexp_replace(form_type, r"/A$", "")


def parse_edgar_date(col: Any) -> Any:
    """Parse ``YYYYMMDD`` or ``YYYY-MM-DD``; anything else is null (and quarantined)."""
    from pyspark.sql import functions as F

    trimmed = F.trim(col)
    return F.coalesce(
        F.to_date(F.when(trimmed.rlike(r"^[0-9]{8}$"), trimmed), "yyyyMMdd"),
        F.to_date(F.when(trimmed.rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"), trimmed), "yyyy-MM-dd"),
    )


def primary_doc_url(file_name: Any) -> Any:
    """Public Archives URL for the submission text file."""
    from pyspark.sql import functions as F

    return F.when(
        F.length(F.trim(file_name)) > 0,
        F.concat(F.lit("https://www.sec.gov/Archives/"), F.trim(file_name)),
    )


def align_to_spec(df: Any, spec: TableSpec) -> Any:
    """Project and cast to a contract's columns, by name."""
    from pyspark.sql import functions as F

    cols = []
    for column in spec.columns:
        source = F.col(f"`{column.name}`") if column.name in df.columns else F.lit(None)
        cols.append(source.cast(column.type_sql).alias(column.name))
    return df.select(*cols)


def run_dq_and_quarantine(
    spark: Any,
    run: JobRun,
    df: Any,
    checks: Sequence[DQCheck],
    *,
    target_spec: TableSpec,
    quarantine_table: str,
    metrics_prefix: str,
) -> Any:
    """Apply DQ, write the rejects, return the rows that passed.

    The quarantine write happens unconditionally -- writing zero rows is cheap, and
    branching on a count is how a quarantine table quietly stops being written to.
    """
    passed, quarantined, metrics = apply_dq(df, checks, run.run_id, source_table=target_spec.fqn)
    run.record(metrics, prefix=f"{metrics_prefix}.")
    quarantine_spec = schemas.table(
        f"{target_spec.catalog}.{target_spec.schema}.{target_spec.name}_quarantine"
    )
    # MERGE, not append: quarantine is part of silver, and rule 3's "re-running a batch
    # must be a no-op" has to hold for the rejected rows too, or a replay doubles the
    # quarantine count and every "how bad is the data" number goes with it.
    merge_scd1(
        spark,
        align_to_spec(quarantined, quarantine_spec),
        quarantine_table,
        keys=("_dq_record_id",),
    )
    quarantined_rows = int(metrics.get("dq.rows_quarantined", 0))
    if quarantined_rows:
        run.warn(f"{target_spec.fqn}: {quarantined_rows} row(s) quarantined")
    run.add(rows_quarantined=quarantined_rows)
    return passed
