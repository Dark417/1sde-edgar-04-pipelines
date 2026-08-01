"""L1 -- the few Delta operations that are not reads or merges.

Exists for one reason: a ``reject_batch`` invariant that can only be evaluated **after**
a MERGE (an SCD-2 dimension having exactly one current row per key is a property of the
merged result, not of the source) would otherwise leave the broken state written.

Delta gives an honest way out. Record the table version before the merge, and restore
to it if the invariant fails. That turns "the batch is abandoned" from a slogan into
something the code actually does.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

__all__ = ["restore_to_version", "rollback_on_failure", "table_version"]


def table_version(spark: Any, table: str) -> int:
    """Current Delta version of ``table``, or ``-1`` when it cannot be determined."""
    try:
        row = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("version").collect()
        return int(row[0]["version"]) if row else -1
    except Exception:
        # Any failure means the version is unknown, which disables rollback. Reported
        # by returning -1 rather than raising: losing the safety net must not also
        # lose the batch.
        return -1


def restore_to_version(spark: Any, table: str, version: int) -> None:
    """Restore ``table`` to ``version``. No-op for a negative version."""
    if version < 0:
        return
    spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {version}")


@contextmanager
def rollback_on_failure(spark: Any, table: str) -> Iterator[int]:
    """Restore ``table`` to its pre-block version if the block raises.

    Not a transaction -- a concurrent writer between the snapshot and the restore would
    lose its write. That is acceptable here because ``max_concurrent_runs`` is 1 on
    Free Edition (a second concurrent run is how you lose a day's quota anyway), and
    the alternative is leaving a structurally broken dimension in place.
    """
    version = table_version(spark, table)
    try:
        yield version
    except BaseException:
        restore_to_version(spark, table, version)
        raise
