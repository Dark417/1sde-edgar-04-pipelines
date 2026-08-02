"""L1 -- MERGE helpers, SCD-1 and SCD-2 (feature F-4).

Every silver write goes through this module. Never ``overwrite``, never ``append``
(AGENTS.md rule 3): re-running a batch must be a no-op, and only a MERGE on the
business key gives you that.

The two invariants that break silently if you get them backwards (rule 4):

* ``_first_seen_ts`` is written on INSERT and **never** appears in an UPDATE set.
* ``_last_seen_ts`` is written on both.

Reversing them turns "when did we first see this filing" -- half the reason silver
exists -- into "when did we last run the job", and nothing fails, so nobody notices
until someone builds a report on it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

__all__ = [
    "NULL_SENTINEL",
    "MergeStats",
    "dedupe_on",
    "hash_diff",
    "merge_scd1",
    "merge_scd2",
]

#: Stand-in for NULL when building a hash. Without it, ``('a', None)`` and ``(None,
#: 'a')`` concatenate to the same string and two different rows share a hash.
NULL_SENTINEL = "␀"


@dataclass(frozen=True, slots=True)
class MergeStats:
    """What a merge did. Emitted as job metrics and asserted on in tests."""

    target_table: str
    rows_source: int
    rows_inserted: int
    rows_updated: int
    rows_deleted: int = 0
    rows_target_after: int = 0

    def as_metrics(self, prefix: str) -> dict[str, int]:
        return {
            f"{prefix}.rows_source": self.rows_source,
            f"{prefix}.rows_inserted": self.rows_inserted,
            f"{prefix}.rows_updated": self.rows_updated,
            f"{prefix}.rows_deleted": self.rows_deleted,
            f"{prefix}.rows_target_after": self.rows_target_after,
        }


def _delta_table(spark: Any, target_table: str) -> Any:
    from delta.tables import DeltaTable

    return DeltaTable.forName(spark, target_table)


def _last_operation_metrics(dt: Any) -> dict[str, int]:
    try:
        row = dt.history(1).select("operationMetrics").collect()[0]
        raw = row["operationMetrics"] or {}
    except Exception:
        # Operation metrics are reporting, never correctness.
        return {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        try:
            out[key] = int(value)
        except (TypeError, ValueError):
            continue
    return out


def hash_diff(df: Any, columns: Sequence[str], *, alias: str = "_hash_diff") -> Any:
    """SHA-256 over ``columns``, with **array columns sorted first**.

    Sort array columns before hashing. Source array ordering is not stable: the SEC
    returns ``tickers`` and ``exchanges`` in whatever order the document happened to
    serialize them, so hashing them unsorted generates a spurious new SCD-2 version
    every single day and the dimension explodes. This is the most commonly
    re-introduced bug in this repo -- if you are here to "simplify" this function,
    that is the thing you are about to break.
    """
    from pyspark.sql import functions as F

    dtypes = dict(df.dtypes)
    parts = []
    for name in columns:
        col = F.col(f"`{name}`")
        dtype = dtypes.get(name, "string")
        if dtype.startswith("array"):
            normalized = F.concat_ws(
                ",",
                F.transform(
                    F.array_sort(col), lambda x: F.coalesce(x.cast("string"), F.lit(NULL_SENTINEL))
                ),
            )
        elif dtype.startswith("map"):
            # Map iteration order is not stable either; sort by key.
            normalized = F.to_json(F.map_from_entries(F.array_sort(F.map_entries(col))))
        else:
            normalized = col.cast("string")
        parts.append(F.coalesce(normalized, F.lit(NULL_SENTINEL)))
    return F.sha2(F.concat_ws("", *parts), 256).alias(alias)


def _project_to_target(src: Any, dt: Any) -> Any:
    """Drop source columns the target does not have.

    Transforms legitimately carry working columns -- ``logical_date`` used only as a
    dedupe tiebreaker, for instance -- and Delta rejects a MERGE whose insert values
    name a column the target lacks. Dropping them here beats making every caller
    remember to, and it is safe: a column that is not in the contract is not data we
    promised to store.
    """
    target_columns = set(dt.toDF().columns)
    return src.select(*[f"`{c}`" for c in src.columns if c in target_columns])


def dedupe_on(df: Any, keys: Sequence[str], order_by: Sequence[str] | None = None) -> Any:
    """Collapse duplicate business keys deterministically.

    Delta refuses a MERGE whose source has two rows for one target row, and rightly
    so -- which of the two wins would otherwise depend on scan order. When the caller
    does not name a tiebreaker, order by every non-key column so the choice is at
    least reproducible across runs.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if order_by:
        ordering = [F.col(f"`{c}`").desc_nulls_last() for c in order_by]
    else:
        ordering = [F.col(f"`{c}`").asc_nulls_last() for c in sorted(df.columns) if c not in keys]
    if not ordering:
        ordering = [F.lit(1)]
    window = Window.partitionBy(*[F.col(f"`{k}`") for k in keys]).orderBy(*ordering)
    return (
        df.withColumn("_dedupe_rn", F.row_number().over(window))
        .filter(F.col("_dedupe_rn") == 1)
        .drop("_dedupe_rn")
    )


def merge_scd1(
    spark: Any,
    source: Any,
    target_table: str,
    keys: Sequence[str],
    update_cols: Sequence[str] | None = None,
    *,
    dedupe_order_by: Sequence[str] | None = None,
    processing_ts: Any = None,
) -> MergeStats:
    """Upsert ``source`` into ``target_table`` on ``keys``.

    ``update_cols`` defaults to every source column except the keys and
    ``_first_seen_ts``. Passing ``_first_seen_ts`` explicitly is rejected rather than
    honored -- it is never an intentional request.
    """
    from pyspark.sql import functions as F

    keys = tuple(keys)
    if not keys:
        raise ValueError("merge_scd1 requires at least one business key")

    now = processing_ts if processing_ts is not None else F.current_timestamp()
    src = dedupe_on(source, keys, dedupe_order_by)
    if "_first_seen_ts" not in src.columns:
        src = src.withColumn("_first_seen_ts", now)
    if "_last_seen_ts" not in src.columns:
        src = src.withColumn("_last_seen_ts", now)
    src = src.withColumn("_last_seen_ts", now)

    if update_cols is not None and "_first_seen_ts" in update_cols:
        raise ValueError(
            "_first_seen_ts must never be updated (AGENTS.md rule 4): it records when the "
            "row was first observed, not when it was last touched. Use _last_seen_ts."
        )

    rows_source = int(src.count())
    dt = _delta_table(spark, target_table)
    src = _project_to_target(src, dt)
    updatable = tuple(c for c in src.columns if c not in keys and c != "_first_seen_ts")
    update_cols = (
        updatable if update_cols is None else tuple(c for c in update_cols if c in updatable)
    )
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    insert_values = {c: F.col(f"s.`{c}`") for c in src.columns}
    update_values = {c: F.col(f"s.`{c}`") for c in update_cols}

    (
        dt.alias("t")
        .merge(src.alias("s"), condition)
        .whenMatchedUpdate(set=update_values)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

    metrics = _last_operation_metrics(dt)
    return MergeStats(
        target_table=target_table,
        rows_source=rows_source,
        rows_inserted=metrics.get("numTargetRowsInserted", 0),
        rows_updated=metrics.get("numTargetRowsUpdated", 0),
        rows_deleted=metrics.get("numTargetRowsDeleted", 0),
        rows_target_after=int(spark.table(target_table).count()),
    )


def merge_scd2(
    spark: Any,
    source: Any,
    target_table: str,
    natural_key: Sequence[str],
    tracked_cols: Sequence[str],
    logical_date: str,
    *,
    dedupe_order_by: Sequence[str] | None = None,
    processing_ts: Any = None,
) -> MergeStats:
    """Type-2 slowly-changing dimension merge.

    Two passes, because one MERGE cannot both close a row and insert its successor:

    1. **Close**: current rows whose tracked columns changed get
       ``valid_to = logical_date - 1`` and ``is_current = false``.
    2. **Insert**: rows with no open version are inserted with
       ``valid_from = logical_date``, ``valid_to = null``, ``is_current = true``.
       Rows whose version is unchanged get ``_last_seen_ts`` refreshed and nothing else.

    **Same-day re-version.** The grain is one version per natural key per logical date.
    A second change within the same ``logical_date`` updates the day's version in place
    instead of closing it, because closing it would produce ``valid_to = valid_from -
    1`` -- a negative-length interval that fails the no-overlap invariant and cannot be
    point-in-time queried. This is pass 1's second clause.

    Running the same batch twice inserts nothing: after pass 1 the stored hash already
    equals the source hash, so the close clause does not fire and pass 2 finds an open
    version to match.
    """
    from pyspark.sql import functions as F

    natural_key = tuple(natural_key)
    tracked_cols = tuple(tracked_cols)
    if not natural_key:
        raise ValueError("merge_scd2 requires a natural key")
    if not tracked_cols:
        raise ValueError("merge_scd2 requires at least one tracked column")

    now = processing_ts if processing_ts is not None else F.current_timestamp()
    logical = F.to_date(F.lit(logical_date))

    src = dedupe_on(source, natural_key, dedupe_order_by)
    src = src.withColumn("_hash_diff", hash_diff(src, tracked_cols))
    src = (
        src.withColumn("valid_from", logical)
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_first_seen_ts", now)
        .withColumn("_last_seen_ts", now)
    )
    rows_source = int(src.count())

    key_match = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in natural_key)

    # ---- version_number: dense and 1-based per natural key.
    #
    # Computed here rather than in the MERGE because Delta cannot reference an aggregate
    # of the target inside a merge action. The highest stored version per key is read
    # once, before either pass, which is safe because pass 1 only ever closes rows -- it
    # never inserts, so the maximum cannot move underneath us.
    #
    # Only the INSERT path consumes this. An unchanged version keeps its stored number
    # (pass 2 updates just _last_seen_ts), and a same-day re-version updates in place
    # without touching version_number -- both correct: the ordinal identifies the
    # version, and neither of those creates one.
    existing_max = (
        spark.table(target_table)
        .groupBy(*natural_key)
        .agg(F.max("version_number").alias("_prior_version"))
    )
    src = (
        src.join(existing_max, on=list(natural_key), how="left")
        .withColumn("version_number", F.coalesce(F.col("_prior_version"), F.lit(0)) + 1)
        .drop("_prior_version")
    )

    dt = _delta_table(spark, target_table)
    src = _project_to_target(src, dt)

    # ---- pass 1: close (or same-day replace) versions whose tracked columns changed
    #
    # Delta allows at most ONE update action across all whenMatched clauses, so the two
    # behaviors live in one clause and branch per column on `is_prior_day`.
    close_condition = f"{key_match} AND t.is_current = true"
    changed = "t.`_hash_diff` <> s.`_hash_diff`"
    is_prior_day = F.col("t.valid_from") < F.col("s.valid_from")

    close_values: dict[str, Any] = {
        "valid_to": F.when(is_prior_day, F.date_sub(F.col("s.valid_from"), 1)).otherwise(
            F.col("t.valid_to")
        ),
        "is_current": F.when(is_prior_day, F.lit(False)).otherwise(F.col("t.is_current")),
        "_hash_diff": F.when(is_prior_day, F.col("t.`_hash_diff`")).otherwise(
            F.col("s.`_hash_diff`")
        ),
        "_last_seen_ts": F.col("s.`_last_seen_ts`"),
    }
    for name in (*tracked_cols, "_ingest_batch_id", "_source_file"):
        if name in src.columns and name in dt.toDF().columns:
            close_values[name] = F.when(is_prior_day, F.col(f"t.`{name}`")).otherwise(
                F.col(f"s.`{name}`")
            )

    (
        dt.alias("t")
        .merge(src.alias("s"), close_condition)
        .whenMatchedUpdate(condition=changed, set=close_values)
        .execute()
    )
    close_metrics = _last_operation_metrics(dt)

    # ---- pass 2: insert successors and refresh unchanged versions
    dt = _delta_table(spark, target_table)
    insert_values = {c: F.col(f"s.`{c}`") for c in src.columns}
    (
        dt.alias("t")
        .merge(src.alias("s"), close_condition)
        .whenMatchedUpdate(set={"_last_seen_ts": F.col("s.`_last_seen_ts`")})
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    insert_metrics = _last_operation_metrics(dt)

    return MergeStats(
        target_table=target_table,
        rows_source=rows_source,
        rows_inserted=insert_metrics.get("numTargetRowsInserted", 0),
        rows_updated=(
            close_metrics.get("numTargetRowsUpdated", 0)
            + insert_metrics.get("numTargetRowsUpdated", 0)
        ),
        rows_target_after=int(spark.table(target_table).count()),
    )
