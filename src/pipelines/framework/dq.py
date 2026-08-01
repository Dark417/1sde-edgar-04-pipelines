"""L1 -- data-quality execution (feature F-3).

Free Edition allows one active Lakeflow pipeline per type, so Declarative
Pipelines/DLT expectations are off the table (design doc section 4.1). This module is
the replacement: explicit DQ, with the same three severities and an explicit
quarantine.

Two properties are worth stating because they are the ones that get lost in a rewrite:

* **Metrics are emitted for every check, including checks that failed nothing**
  (AGENTS.md rule 9). A missing key and a zero are the same thing to a dashboard, and
  they mean opposite things: "nothing failed" versus "the check never ran".
* **A `warn` check never removes a row.** It counts. If a warn check is quietly
  dropping rows, every count downstream is wrong and nothing says so.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from pipelines.contracts.models import DQCheck, Severity

__all__ = ["DQBatchFailure", "DQResult", "apply_dq", "quarantine_frame"]


class DQBatchFailure(RuntimeError):
    """A ``reject_batch`` check failed: the batch is abandoned, nothing is written."""

    def __init__(self, failures: Sequence[tuple[DQCheck, int]], sample: Sequence[str] = ()) -> None:
        lines = [f"  - {c.name}: {n} row(s) failed -- {c.description}" for c, n in failures]
        extra = ("\n  sample: " + "; ".join(sample)) if sample else ""
        super().__init__(
            "batch rejected by structural data-quality check(s):\n" + "\n".join(lines) + extra
        )
        self.failures = list(failures)


@dataclass(frozen=True, slots=True)
class DQResult:
    """Everything ``apply_dq`` learned, beyond the two DataFrames."""

    rows_in: int
    rows_passed: int
    rows_quarantined: int
    metrics: dict[str, int] = field(default_factory=dict)

    @property
    def has_warnings(self) -> bool:
        return any(v > 0 for k, v in self.metrics.items() if k.endswith(".warn"))


def _flag_column(check: DQCheck) -> Any:
    """Boolean column that is true when the row **fails** the check.

    ``coalesce(expr, false)`` makes null-means-fail explicit. Leaving Spark's
    three-valued logic in place would let a null silently pass a check written as
    ``value > 0``, which is precisely the row you wanted quarantined.
    """
    from pyspark.sql import functions as F

    return ~F.coalesce(F.expr(check.expression), F.lit(False))


def quarantine_frame(
    df: Any,
    checks: Sequence[DQCheck],
    run_id: str,
    source_table: str,
) -> Any:
    """Project failing rows into the shared quarantine shape.

    The rejected record is preserved verbatim as JSON. A quarantine row you cannot
    replay is just a log line that costs storage.
    """
    from pyspark.sql import functions as F

    payload_cols = [c for c in df.columns if not c.startswith("_dq_")]
    reason = F.lit(None).cast("string")
    check_name = F.lit(None).cast("string")
    for check in reversed(checks):
        reason = F.when(
            _flag_column(check), F.lit(f"{check.name}: {check.description} [{check.expression}]")
        ).otherwise(reason)
        check_name = F.when(_flag_column(check), F.lit(check.name)).otherwise(check_name)

    record_json = F.to_json(F.struct(*[F.col(f"`{c}`") for c in payload_cols]))
    # Deterministic id over what was rejected and why -- deliberately NOT over run_id.
    # Quarantine is MERGEd on it, so replaying a batch refreshes the row instead of
    # appending a second copy. Without this the quarantine table is the one place in
    # silver where "run it twice" changes the answer.
    record_id = F.sha2(F.concat_ws("|", F.lit(source_table), check_name, record_json), 256)

    return df.select(
        record_id.alias("_dq_record_id"),
        F.lit(run_id).alias("_dq_run_id"),
        check_name.alias("_dq_check_name"),
        reason.alias("_dq_failure_reason"),
        F.current_timestamp().alias("_quarantined_at"),
        F.lit(source_table).alias("_source_table"),
        (F.col("_source_file") if "_source_file" in df.columns else F.lit(None).cast("string")).alias(
            "_source_file"
        ),
        (
            F.col("_ingest_batch_id")
            if "_ingest_batch_id" in df.columns
            else F.lit(None).cast("string")
        ).alias("_ingest_batch_id"),
        record_json.alias("record_json"),
    )


def apply_dq(
    df: Any,
    checks: Sequence[DQCheck],
    run_id: str,
    *,
    source_table: str = "",
) -> tuple[Any, Any, dict[str, int]]:
    """Run ``checks`` over ``df``.

    Returns ``(passed, quarantined, metrics)``.

    ``quarantined`` always has the shared quarantine schema, even when empty, so the
    caller can write it unconditionally instead of branching on a count.

    Raises :class:`DQBatchFailure` when a ``reject_batch`` check finds anything --
    before returning, so no caller can accidentally write a batch that failed a
    structural invariant.
    """
    from pyspark.sql import functions as F

    checks = tuple(checks)
    metrics: dict[str, int] = {}

    if not checks:
        empty_q = quarantine_frame(df.limit(0), (), run_id, source_table)
        rows_in = int(df.count())
        return df, empty_q, {"dq.rows_in": rows_in, "dq.rows_passed": rows_in, "dq.rows_quarantined": 0}

    flagged = df
    flag_names: dict[str, DQCheck] = {}
    for i, check in enumerate(checks):
        flag = f"_dq_flag_{i}"
        flag_names[flag] = check
        flagged = flagged.withColumn(flag, _flag_column(check))
    flagged = flagged.cache()

    # One aggregation for every check, so N checks cost one pass and not N.
    agg_exprs = [F.count(F.lit(1)).alias("_rows_in")] + [
        F.sum(F.col(flag).cast("long")).alias(flag) for flag in flag_names
    ]
    counts = flagged.agg(*agg_exprs).collect()[0].asDict()
    rows_in = int(counts["_rows_in"] or 0)

    batch_failures: list[tuple[DQCheck, int]] = []
    for flag, check in flag_names.items():
        failed = int(counts[flag] or 0)
        # One key per check even when zero failed -- rule 9.
        metrics[f"dq.{check.name}.failed"] = failed
        metrics[f"dq.{check.name}.severity_{check.severity.value}"] = failed
        if check.severity is Severity.WARN:
            metrics[f"dq.{check.name}.warn"] = failed
        if check.severity is Severity.REJECT_BATCH and failed > 0:
            batch_failures.append((check, failed))

    if batch_failures:
        sample = _sample_failures(flagged, flag_names, batch_failures)
        flagged.unpersist()
        raise DQBatchFailure(batch_failures, sample)

    reject_flags = [f for f, c in flag_names.items() if c.severity is Severity.REJECT]
    if reject_flags:
        any_reject = F.lit(False)
        for flag in reject_flags:
            any_reject = any_reject | F.col(flag)
    else:
        any_reject = F.lit(False)

    original_cols = list(df.columns)
    passed = flagged.filter(~any_reject).select(*[F.col(f"`{c}`") for c in original_cols])
    failing = flagged.filter(any_reject).select(*[F.col(f"`{c}`") for c in original_cols])
    reject_checks = [c for c in checks if c.severity is Severity.REJECT]
    quarantined = quarantine_frame(failing, reject_checks, run_id, source_table)

    # Count the failing rows rather than summing per-check counts: one row can fail two
    # reject checks at once, and the sum would count it twice.
    rows_quarantined = int(failing.count())
    metrics["dq.rows_in"] = rows_in
    metrics["dq.rows_quarantined"] = rows_quarantined
    metrics["dq.rows_passed"] = rows_in - rows_quarantined

    return passed, quarantined, metrics


def _sample_failures(
    flagged: Any,
    flag_names: dict[str, DQCheck],
    batch_failures: Sequence[tuple[DQCheck, int]],
) -> list[str]:
    """A few offending rows, so the operator does not have to go find them."""
    from pyspark.sql import functions as F

    failed_names = {c.name for c, _ in batch_failures}
    flags = [f for f, c in flag_names.items() if c.name in failed_names]
    if not flags:
        return []
    predicate = F.lit(False)
    for flag in flags:
        predicate = predicate | F.col(flag)
    cols = [c for c in flagged.columns if not c.startswith("_dq_flag_")]
    try:
        rows = flagged.filter(predicate).select(*[F.col(f"`{c}`") for c in cols]).limit(3).collect()
    except Exception:
        # A sample is a nicety. It must never turn one failure into two.
        return []
    return [str(r.asDict()) for r in rows]
