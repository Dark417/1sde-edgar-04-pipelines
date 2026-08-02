"""L1 -- reading landing objects (feature F-2).

Two readers, one contract.

``autoloader``
    The production path on Databricks. Options are fixed (AGENTS.md rule 10):
    ``cloudFiles.format=json``, a **per-stream** ``schemaLocation``,
    ``schemaEvolutionMode=rescue``, directory-listing mode. File-notification mode
    needs SNS/SQS wiring that Free Edition cannot provision.

``batch``
    The local path. Auto Loader is a Databricks-only source, so a laptop needs an
    equivalent that preserves the property the acceptance test cares about: *a file
    already processed contributes zero rows on the next run.* It does that with an
    explicit file ledger under the checkpoint root.

    The ledger is committed by the caller **after** the write succeeds, so a crash
    mid-write replays the file rather than losing it. At-least-once into an
    append-only bronze is the correct trade: bronze is what you replay from, and the
    silver MERGE is what makes the end state idempotent.

Where the two differ, and it matters: Auto Loader's ``rescue`` mode captures both
unknown columns *and* values that do not fit the tracked type. The batch reader
captures unknown columns only. Both make ``_rescued_data`` non-null when the source
grows a field, which is the signal rule 11 is about.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from edgar_lakehouse_contracts.envelope import ENVELOPE_FIELDS

from pipelines import streams

__all__ = [
    "LandingBatch",
    "landing_files",
    "read_landing_batch",
    "read_landing_stream",
]

#: Fixed Auto Loader options. Changing these is a contract change, not a tuning knob.
AUTOLOADER_OPTIONS: dict[str, str] = {
    "cloudFiles.format": "json",
    "cloudFiles.schemaEvolutionMode": "rescue",
    "cloudFiles.useNotifications": "false",
    "cloudFiles.inferColumnTypes": "false",
    "cloudFiles.rescuedDataColumn": "_rescued_data",
    "multiLine": "false",
}


def read_landing_stream(spark: Any, stream: str, landing_root: str, checkpoint_root: str) -> Any:
    """Auto Loader stream over one landing prefix. Databricks only."""
    path = streams.landing_path(landing_root, stream)
    reader = spark.readStream.format("cloudFiles")
    for key, value in AUTOLOADER_OPTIONS.items():
        reader = reader.option(key, value)
    # Per-stream schema location. Sharing one location across streams merges their
    # inferred schemas into a single union and every stream starts rescuing the others'
    # columns.
    reader = reader.option(
        "cloudFiles.schemaLocation", streams.checkpoint_path(checkpoint_root, stream)
    )
    from pyspark.sql import functions as F

    return reader.load(path).withColumn("_source_file", F.col("_metadata.file_path"))


@dataclass(slots=True)
class LandingBatch:
    """A set of not-yet-processed landing files plus the DataFrame over them."""

    stream: str
    df: Any
    files: tuple[str, ...]
    ledger_path: str
    _already_processed: frozenset[str] = field(default_factory=frozenset)

    @property
    def is_empty(self) -> bool:
        return len(self.files) == 0

    def commit(self) -> None:
        """Record the files as processed. Call only after the write has succeeded."""
        processed = sorted(self._already_processed | set(self.files))
        Path(self.ledger_path).parent.mkdir(parents=True, exist_ok=True)
        tmp = f"{self.ledger_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"stream": self.stream, "files": processed}, fh, indent=1, sort_keys=True)
        os.replace(tmp, self.ledger_path)


def _ledger_path(checkpoint_root: str, stream: str) -> str:
    return f"{streams.checkpoint_path(checkpoint_root, stream)}/_processed_files.json"


def _load_ledger(path: str) -> frozenset[str]:
    try:
        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return frozenset()
    return frozenset(payload.get("files", []))


def landing_files(landing_root: str, stream: str) -> tuple[str, ...]:
    """Every landing object for a stream, sorted for deterministic batching."""
    root = Path(streams.landing_path(landing_root, stream))
    if not root.exists():
        return ()
    return tuple(sorted(str(p) for p in root.rglob("*.json") if p.is_file()))


def read_landing_batch(
    spark: Any,
    stream: str,
    landing_root: str,
    checkpoint_root: str,
    *,
    file_filter: Callable[[str], bool] | None = None,
) -> LandingBatch:
    """Read landing files not yet in the ledger, emulating ``schemaEvolutionMode=rescue``."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    ledger_path = _ledger_path(checkpoint_root, stream)
    processed = _load_ledger(ledger_path)
    candidates = landing_files(landing_root, stream)
    if file_filter is not None:
        candidates = tuple(p for p in candidates if file_filter(p))
    new_files = tuple(p for p in candidates if p not in processed)

    envelope_ddl = ", ".join(f"`{k}` {v}" for k, v in ENVELOPE_FIELDS.items())
    envelope_struct = StructType.fromDDL(envelope_ddl)

    if not new_files:
        empty = spark.createDataFrame([], envelope_struct)
        empty = empty.withColumn("_rescued_data", F.lit(None).cast("string")).withColumn(
            "_source_file", F.lit(None).cast("string")
        )
        return LandingBatch(stream, empty, (), ledger_path, processed)

    raw = spark.read.option("multiLine", "false").json(list(new_files))
    known = set(ENVELOPE_FIELDS)
    extra = [c for c in raw.columns if c not in known and not c.startswith("_")]

    # Rescue: anything the contract does not name is preserved as JSON rather than
    # dropped. A dropped column is a source change nobody ever finds out about.
    rescued = (
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in extra]))
        if extra
        else F.lit(None).cast("string")
    )
    projected = [
        (F.col(f"`{name}`") if name in raw.columns else F.lit(None)).cast(type_sql).alias(name)
        for name, type_sql in ENVELOPE_FIELDS.items()
    ]
    df = raw.select(
        *projected,
        rescued.alias("_rescued_data"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
    return LandingBatch(stream, df, new_files, ledger_path, processed)


def rescued_row_count(df: Any) -> int:
    """Rows whose ``_rescued_data`` is non-null. Emitted per stream by bronze."""
    from pyspark.sql import functions as F

    return int(df.filter(F.col("_rescued_data").isNotNull()).count())


def assert_known_envelope_versions(df: Any, supported: Iterable[str]) -> None:
    """Fail when landing contains an envelope version this build does not understand."""
    from pyspark.sql import functions as F

    supported_list: Sequence[str] = list(supported)
    bad = (
        df.select("envelope_version")
        .distinct()
        .filter(~F.col("envelope_version").isin(list(supported_list)))
        .limit(5)
        .collect()
    )
    if bad:
        found = ", ".join(sorted({str(r["envelope_version"]) for r in bad}))
        raise ValueError(
            f"landing contains envelope_version(s) [{found}] but this build supports "
            f"[{', '.join(supported_list)}]. Upgrade the wheel or re-run repo 3."
        )
