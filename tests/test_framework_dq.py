"""Feature F-3: DQ execution, quarantine and metrics."""

from __future__ import annotations

from typing import Any

import pytest
from edgar_lakehouse_contracts.dq import DQCheck
from edgar_lakehouse_contracts.models import Severity

from pipelines.framework.dq import DQBatchFailure, apply_dq

pytestmark = pytest.mark.spark


def _frame(spark: Any) -> Any:
    return spark.createDataFrame(
        [(1, "ok", "f1"), (2, None, "f1"), (3, "ok", "f2"), (-4, "ok", "f2")],
        "id INT, label STRING, _source_file STRING",
    )


REJECT_NEGATIVE = DQCheck("id_positive", "t", "id > 0", Severity.REJECT, "ids must be positive")
WARN_LABEL = DQCheck("label_present", "t", "label IS NOT NULL", Severity.WARN, "label missing")


def test_warn_check_never_removes_rows(spark: Any) -> None:
    """A warn check that quietly drops rows makes every downstream count wrong."""
    passed, quarantined, metrics = apply_dq(_frame(spark), [WARN_LABEL], "run-1")
    assert passed.count() == 4
    assert quarantined.count() == 0
    assert metrics["dq.label_present.failed"] == 1
    assert metrics["dq.rows_quarantined"] == 0


def test_reject_check_moves_exactly_the_failing_rows(spark: Any) -> None:
    passed, quarantined, metrics = apply_dq(_frame(spark), [REJECT_NEGATIVE], "run-1")
    assert passed.count() == 3
    assert quarantined.count() == 1
    assert metrics["dq.rows_in"] == 4
    assert metrics["dq.rows_passed"] == 3
    row = quarantined.collect()[0]
    assert row["_dq_check_name"] == "id_positive"
    assert "id > 0" in row["_dq_failure_reason"]
    assert row["_dq_run_id"] == "run-1"
    # The rejected record is replayable, not just countable.
    assert '"id":-4' in row["record_json"].replace(" ", "")


def test_reject_batch_raises_before_anything_is_returned(spark: Any) -> None:
    check = DQCheck("no_negatives", "t", "id > 0", Severity.REJECT_BATCH, "structural")
    with pytest.raises(DQBatchFailure) as exc:
        apply_dq(_frame(spark), [check], "run-1")
    assert "no_negatives" in str(exc.value)
    assert "1 row(s) failed" in str(exc.value)


def test_metrics_have_one_key_per_check_even_when_all_pass(spark: Any) -> None:
    """Rule 9: a missing key and a zero look identical and mean opposite things."""
    always = DQCheck("always_true", "t", "1 = 1", Severity.REJECT, "never fails")
    _, _, metrics = apply_dq(_frame(spark), [always, WARN_LABEL], "run-1")
    assert metrics["dq.always_true.failed"] == 0
    assert metrics["dq.label_present.failed"] == 1


def test_null_expression_result_counts_as_failure(spark: Any) -> None:
    """Three-valued logic would let a null slip past `value > 0` -- exactly the row
    you wanted quarantined."""
    df = spark.createDataFrame([(None,), (5,)], "value INT")
    check = DQCheck("positive", "t", "value > 0", Severity.REJECT, "positive")
    passed, quarantined, _ = apply_dq(df, [check], "run-1")
    assert passed.count() == 1
    assert quarantined.count() == 1


def test_row_failing_two_reject_checks_is_counted_once(spark: Any) -> None:
    df = spark.createDataFrame([(-1, None)], "id INT, label STRING")
    checks = [
        REJECT_NEGATIVE,
        DQCheck("label_required", "t", "label IS NOT NULL", Severity.REJECT, "label"),
    ]
    _, quarantined, metrics = apply_dq(df, checks, "run-1")
    assert quarantined.count() == 1
    assert metrics["dq.rows_quarantined"] == 1


def test_first_failing_check_wins_in_registry_order(spark: Any) -> None:
    df = spark.createDataFrame([(-1, None)], "id INT, label STRING")
    checks = [
        REJECT_NEGATIVE,
        DQCheck("label_required", "t", "label IS NOT NULL", Severity.REJECT, "label"),
    ]
    _, quarantined, _ = apply_dq(df, checks, "run-1")
    assert quarantined.collect()[0]["_dq_check_name"] == "id_positive"


def test_no_checks_returns_everything_with_an_empty_quarantine(spark: Any) -> None:
    passed, quarantined, metrics = apply_dq(_frame(spark), [], "run-1")
    assert passed.count() == 4
    assert quarantined.count() == 0
    assert metrics["dq.rows_in"] == 4
    assert "record_json" in quarantined.columns
