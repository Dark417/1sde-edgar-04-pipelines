"""The one-line structured job summary (AGENTS.md rule 13). No Spark."""

from __future__ import annotations

import json

import pytest

from pipelines.framework.metrics import JobRun, job_run


def test_summary_is_one_json_line_with_the_required_fields(capsys: pytest.CaptureFixture) -> None:
    with job_run("silver_transform", "run-1", "2026-07-31") as run:
        run.add(rows_in=10, rows_out=9, rows_quarantined=1)
    printed = capsys.readouterr().out.strip().splitlines()
    assert len(printed) == 1
    summary = json.loads(printed[0])
    assert summary["job"] == "silver_transform"
    assert summary["status"] == "OK"
    assert summary["rows_in"] == 10
    assert summary["rows_out"] == 9
    assert summary["rows_quarantined"] == 1
    assert "duration_seconds" in summary


def test_a_warning_moves_the_status_to_warn() -> None:
    run = JobRun("j", "r", "2026-07-31")
    run.warn("rescued data present")
    assert run.status == "WARN"
    assert run.summary()["warnings"] == ["rescued data present"]


def test_failure_still_emits_a_summary_then_re_raises(capsys: pytest.CaptureFixture) -> None:
    """A job that dies without a summary line is a job nobody can triage."""
    with pytest.raises(RuntimeError, match="boom"), job_run("j", "r", "2026-07-31") as run:
        run.add(rows_in=5)
        raise RuntimeError("boom")
    summary = json.loads(capsys.readouterr().out.strip())
    assert summary["status"] == "FAILED"
    assert summary["rows_in"] == 5
    assert "RuntimeError: boom" in summary["warnings"]


def test_add_accumulates_and_record_sets() -> None:
    run = JobRun("j", "r", "2026-07-31")
    run.add(rows_in=1)
    run.add(rows_in=2)
    assert run.metrics["rows_in"] == 3
    run.record({"dq.x.failed": 4}, prefix="silver.")
    run.record({"dq.x.failed": 7}, prefix="silver.")
    # Set, not accumulated: DQ counts describe one stage.
    assert run.metrics["silver.dq.x.failed"] == 7


def test_metrics_are_sorted_in_the_summary() -> None:
    run = JobRun("j", "r", "2026-07-31")
    run.add(z=1, a=1)
    assert list(run.summary()["metrics"]) == sorted(run.summary()["metrics"])
