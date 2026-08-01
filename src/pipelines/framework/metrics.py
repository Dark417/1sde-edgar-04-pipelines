"""L1 -- job metrics and the one-line structured summary (AGENTS.md rule 13).

Every entrypoint wraps its work in :class:`JobRun`. On exit it emits exactly one JSON
line: rows in, rows out, rows quarantined, duration, status, and every metric any stage
recorded. One line, because that is what survives a log aggregator; JSON, because
grepping a summary out of prose is how observability rots.

There is no metrics *table*: repo 1 does not define one, and this repo does not create
tables (rule 1). The line goes to stdout, which the Databricks job run page captures.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

__all__ = ["JobRun", "Status", "job_run"]

Status = Literal["OK", "WARN", "FAILED"]

_LOG = logging.getLogger("pipelines")


@dataclass(slots=True)
class JobRun:
    """Accumulates metrics for one job task."""

    job: str
    run_id: str
    logical_date: str
    metrics: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    status: Status = "OK"
    started_at: float = field(default_factory=time.monotonic)

    def add(self, **counts: int) -> None:
        for key, value in counts.items():
            self.metrics[key] = self.metrics.get(key, 0) + int(value)

    def record(self, metrics: Mapping[str, int], *, prefix: str = "") -> None:
        """Merge a metrics mapping, e.g. the third element of ``apply_dq``.

        Values are *set*, not accumulated: DQ counts describe one stage, and adding
        them across stages would produce a number that means nothing.
        """
        for key, value in metrics.items():
            self.metrics[f"{prefix}{key}" if prefix else key] = int(value)

    def warn(self, message: str) -> None:
        """Record a warning and move the run's status to WARN.

        A non-null ``_rescued_data`` count comes through here: the job succeeded, but
        the source changed shape and somebody needs to look (rule 11).
        """
        self.warnings.append(message)
        if self.status == "OK":
            self.status = "WARN"

    def summary(self) -> dict[str, Any]:
        return {
            "event": "job_summary",
            "job": self.job,
            "run_id": self.run_id,
            "logical_date": self.logical_date,
            "status": self.status,
            "duration_seconds": round(time.monotonic() - self.started_at, 3),
            "rows_in": self.metrics.get("rows_in", 0),
            "rows_out": self.metrics.get("rows_out", 0),
            "rows_quarantined": self.metrics.get("rows_quarantined", 0),
            "warnings": self.warnings,
            "metrics": dict(sorted(self.metrics.items())),
        }

    def emit(self) -> str:
        line = json.dumps(self.summary(), default=str, sort_keys=False)
        _LOG.info(line)
        print(line)
        return line


@contextmanager
def job_run(job: str, run_id: str, logical_date: str) -> Iterator[JobRun]:
    """Context manager that always emits the summary, success or failure."""
    run = JobRun(job=job, run_id=run_id, logical_date=logical_date)
    try:
        yield run
    except BaseException as exc:
        run.status = "FAILED"
        run.warnings.append(f"{type(exc).__name__}: {exc}")
        run.emit()
        raise
    run.emit()
