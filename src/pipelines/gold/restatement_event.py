"""L4 -- ``gold.restatement_event`` (feature F-9). The differentiator.

A restatement is a company later asserting a *different* value for a period it had
already reported. Detecting it means comparing consecutive assertions of one period and
deciding, for each pair, whether the difference is real.

Two rules do all the work, and getting either wrong makes the table worthless:

**1. Never compare with ``!=``.** Filers re-report the same figure at different
rounding scales. Equality flags every one of them and the table becomes pure noise.
The comparison is a tolerance -- see :class:`RestatementTolerance` and ADR-002.

**2. Compare only within identical ``(unit, period_start, period_end, period_type)``.**
A Q4 duration against an FY duration is not a restatement, it is a bug.

``materiality_band`` is a **product heuristic, not an accounting standard.** Nothing
in GAAP defines 1% and 5% as the boundaries; they are a presentation choice, and that
qualification travels with the number all the way to the UI.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from edgar_lakehouse_contracts import concepts as concept_registry
from edgar_lakehouse_contracts import schemas

from pipelines.config import Settings
from pipelines.framework.metrics import JobRun

from .common import delta_version, write_gold

__all__ = [
    "GRAIN",
    "RestatementTolerance",
    "build",
    "reporting_scale",
    "run",
    "tolerance_expr",
]

SPEC = schemas.GOLD_RESTATEMENT_EVENT

#: The comparison scope. Anything not in this tuple is *not* held constant, and
#: comparing across it produces a bug rather than a restatement.
GRAIN: tuple[str, ...] = (
    "cik",
    "concept_canonical",
    "unit",
    "period_start",
    "period_end",
    "period_type",
)

_MATERIAL_PCT = 0.05
_NOTABLE_PCT = 0.01


@dataclass(frozen=True, slots=True)
class RestatementTolerance:
    """How much two assertions may differ before it counts as a restatement.

    ``rel_tol`` and ``abs_tol`` are AGENTS.md rule 6 exactly:
    ``abs(a - b) > greatest(abs(a) * 1e-6, 1e-6)``.

    ``decimals_aware`` adds one more term to the same ``greatest``: half a unit at the
    coarser of the two values' reported precisions. It exists because rule 6's own
    stated purpose -- "filers report identical figures at different ``decimals``
    scales" -- is not achieved by a ``1e-6`` relative tolerance on real data.

    The counterexample is in the repo's docs (ADR-002): Dream Finders Homes reported
    FY2020 ``NetIncomeLoss`` as 79,093,455 and later as 79,093,000. That is one figure
    rounded to the nearest thousand, a relative difference of 5.75e-6 -- nearly six
    times ``rel_tol``. Rounding an n-digit figure to the nearest 10^3 moves it by up to
    500, so *any* value below ~5e8 re-reported at a coarser scale breaks a 1e-6
    tolerance.

    **What this costs.** With ``decimals`` absent, a difference smaller than
    ``max_scale_fraction`` of the value (0.1%) cannot be told apart from the same
    figure re-stated at a coarser scale, and is not flagged. That is an order of
    magnitude below the ``immaterial`` band's own 1% floor, so nothing a user would
    call a restatement falls into the gap -- but it is a real limit, not a rounding
    detail, and it disappears the day a source supplies ``decimals``.

    Set ``decimals_aware=False`` to get rule 6's literal expression back. That path is
    tested, so the spec as written stays executable.
    """

    rel_tol: float = 1e-6
    abs_tol: float = 1e-6
    decimals_aware: bool = True
    #: How much of the reporting scale a re-report may move the value by. **1.0, not
    #: 0.5**: filers *truncate* to a scale as often as they round to it. Dream Finders
    #: reported 44,694,524 and later 44,694,000 -- the same figure cut to thousands,
    #: 524 away, where correct rounding would have given 44,695,000. A half-unit floor
    #: catches the rounders and flags the truncators.
    scale_multiplier: float = 1.0
    #: Cap on inferred precision, as a fraction of the value. Without it a figure that
    #: happens to be round -- 2,000,000 -- would carry a +/-2,000,000 tolerance and
    #: every restatement of it would vanish. 1e-3 keeps four significant digits, which
    #: is the coarsest real reporting seen in the sample (1,034,000 stated to the
    #: nearest thousand).
    max_scale_fraction: float = 1e-3


def reporting_scale(value: Any, decimals: Any, cfg: RestatementTolerance) -> Any:
    """The rounding step a value appears to have been reported at.

    Uses XBRL ``decimals`` when present -- ``decimals = -3`` means "rounded to the
    nearest thousand", so the scale is ``10^3``. The ``companyconcept`` API does not
    return ``decimals`` (verified against the live API), so the fallback infers the
    scale from the value's own trailing zeros, capped at ``max_scale_fraction`` of the
    value.
    """
    from pyspark.sql import functions as F

    magnitude = F.abs(value)
    cap = F.greatest(magnitude * F.lit(cfg.max_scale_fraction), F.lit(1e-6))

    # A ladder rather than a UDF: a Python UDF here would serialize every row of the
    # largest table in the project across the JVM boundary.
    #
    # Built low power to high so that the *outermost* `when` tests the highest power.
    # `when` short-circuits on the first true branch, and we want the largest power of
    # ten that divides the value -- 79,093,000 must resolve to 1000, not to 10.
    inferred = F.lit(1.0)
    for power in range(1, 13):
        step = Decimal(10) ** power
        inferred = F.when(
            (magnitude > 0) & (magnitude % F.lit(step) == 0), F.lit(float(step))
        ).otherwise(inferred)

    from_decimals = F.pow(F.lit(10.0), -decimals.cast("double"))
    return F.coalesce(from_decimals, F.least(inferred, cap))


def tolerance_expr(
    earlier_value: Any,
    later_value: Any,
    earlier_decimals: Any,
    later_decimals: Any,
    cfg: RestatementTolerance,
) -> Any:
    """The threshold a difference must exceed to be a restatement."""
    from pyspark.sql import functions as F

    terms = [
        F.abs(earlier_value).cast("double") * F.lit(cfg.rel_tol),
        F.lit(cfg.abs_tol),
    ]
    if cfg.decimals_aware:
        terms.append(
            F.lit(cfg.scale_multiplier)
            * F.greatest(
                reporting_scale(earlier_value, earlier_decimals, cfg),
                reporting_scale(later_value, later_decimals, cfg),
            )
        )
    return F.greatest(*terms)


def _preference_lookup() -> Any:
    """``taxonomy|tag -> preference``, for choosing one tag per canonical concept."""
    from pyspark.sql import functions as F

    pairs: list[Any] = []
    for mapping in concept_registry.CONCEPT_MAPPINGS:
        pairs.append(F.lit(f"{mapping.taxonomy}|{mapping.tag}"))
        pairs.append(F.lit(mapping.preference))
    return F.create_map(*pairs)


def build(
    facts: Any,
    *,
    tolerance: RestatementTolerance | None = None,
    company_names: Any = None,
) -> Any:
    """Detect restatements in ``silver.financial_fact``.

    Only mapped concepts participate: comparing a value asserted under one tag against
    the same value under a different tag is a taxonomy change, not a restatement, and
    the canonical concept is what makes the two comparable in the first place.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    cfg = tolerance or RestatementTolerance()
    grain_cols = [F.col(f"`{c}`") for c in GRAIN]

    mapped = facts.filter(F.col("concept_canonical").isNotNull() & F.col("value").isNotNull())

    # One assertion per accession per grain. A filer can report the same canonical
    # concept under two tags in one filing; the contract's explicit `preference`
    # ordering decides which wins, so the result does not depend on join order.
    per_accession = Window.partitionBy(*grain_cols, F.col("accession_number")).orderBy(
        F.coalesce(
            _preference_lookup()[F.concat_ws("|", F.col("taxonomy"), F.col("concept_tag"))],
            F.lit(9999),
        ).asc(),
        F.col("concept_tag").asc(),
    )
    one_per_accession = (
        mapped.withColumn("_tag_rank", F.row_number().over(per_accession))
        .filter(F.col("_tag_rank") == 1)
        .drop("_tag_rank")
    )

    # Consecutive assertions, oldest first. accession_number breaks same-day ties so
    # the ordering is total and the result is reproducible.
    ordering = Window.partitionBy(*grain_cols).orderBy(
        F.col("filed_date").asc(), F.col("accession_number").asc()
    )
    with_prior = (
        one_per_accession.withColumn("prev_value", F.lag("value").over(ordering))
        .withColumn("prev_decimals", F.lag("decimals").over(ordering))
        .withColumn("prev_accession_number", F.lag("accession_number").over(ordering))
        .withColumn("prev_filed_date", F.lag("filed_date").over(ordering))
        .withColumn("prev_form_type", F.lag("form_type").over(ordering))
        .filter(F.col("prev_value").isNotNull())
    )

    threshold = tolerance_expr(
        F.col("prev_value"), F.col("value"), F.col("prev_decimals"), F.col("decimals"), cfg
    )
    delta_abs = F.col("value") - F.col("prev_value")
    changed = with_prior.filter(F.abs(delta_abs).cast("double") > threshold)

    delta_pct = F.when(
        F.col("prev_value") != 0, (delta_abs / F.abs(F.col("prev_value"))).cast("double")
    ).otherwise(F.lit(None).cast("double"))

    events = changed.select(
        F.sha2(
            F.concat_ws(
                "|",
                *[F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("")) for c in GRAIN],
                F.col("prev_accession_number"),
                F.col("accession_number"),
            ),
            256,
        ).alias("restatement_id"),
        *grain_cols,
        F.col("prev_accession_number").alias("original_accession_number"),
        F.col("prev_form_type").alias("original_form_type"),
        F.col("prev_filed_date").alias("original_filed_date"),
        F.col("prev_value").alias("original_value"),
        F.col("prev_decimals").alias("original_decimals"),
        F.col("accession_number").alias("restated_accession_number"),
        F.col("form_type").alias("restated_form_type"),
        F.col("filed_date").alias("restated_filed_date"),
        F.col("value").alias("restated_value"),
        F.col("decimals").alias("restated_decimals"),
        delta_abs.alias("delta_abs"),
        delta_pct.alias("delta_pct"),
        F.datediff(F.col("filed_date"), F.col("prev_filed_date"))
        .cast("int")
        .alias("days_to_restatement"),
    )

    # Bands are a product heuristic, not an accounting standard. A restatement away
    # from a reported zero has no defined percentage; it is treated as material,
    # because "we previously said zero" is the strongest version of the claim.
    magnitude = F.abs(F.col("delta_pct"))
    band = (
        F.when(F.col("delta_pct").isNull(), F.lit("material"))
        .when(magnitude > F.lit(_MATERIAL_PCT), F.lit("material"))
        .when(magnitude >= F.lit(_NOTABLE_PCT), F.lit("notable"))
        .otherwise(F.lit("immaterial"))
    )
    events = events.withColumn("materiality_band", band)

    if company_names is not None:
        events = events.join(F.broadcast(company_names), on="cik", how="left")
    else:
        events = events.withColumn("company_name", F.lit(None).cast("string"))
    return events


def run(spark: Any, settings: Settings, run_ctx: JobRun) -> int:
    from pyspark.sql import functions as F

    facts = spark.table(settings.table(schemas.SILVER_FINANCIAL_FACT.fqn))
    companies = (
        spark.table(settings.table(schemas.SILVER_COMPANY.fqn))
        .filter(F.col("is_current"))
        .select("cik", "company_name")
    )
    events = build(facts, company_names=companies)
    count = write_gold(
        events,
        SPEC,
        settings.table(SPEC.fqn),
        run_ctx.run_id,
        source_version=delta_version(spark, settings.table(schemas.SILVER_FINANCIAL_FACT.fqn)),
    )
    run_ctx.record({"gold.restatement_event.rows": count})
    run_ctx.add(rows_out=count)
    return count
