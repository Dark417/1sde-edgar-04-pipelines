"""L5 -- the serving export (feature F-10, data contracts section 5).

Writes one Parquet **object** per gold table plus a manifest::

    <export_root>/v1/{table}/data.parquet
    <export_root>/v1/_manifest.json

Repo 5 reads these with DuckDB and never connects to Databricks. That is not a
convenience: Free Edition compute shuts down on quota exhaustion, and a demo that
talked to Databricks would go dark whenever the quota did (design doc section 5.4).

**One object, not a directory of parts**, because DuckDB consumers get a stable URL and
because the manifest can then carry a real content hash. Spark writes a directory, so
the single part file is staged and then promoted to the final name.

Export is an **overwrite**. It is a projection of gold; an append diverges from gold on
the first re-run.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pipelines.config import Settings
from pipelines.contracts import schemas
from pipelines.framework.metrics import JobRun

__all__ = ["ExportedTable", "Manifest", "build_manifest", "export_all", "run"]

MANIFEST_VERSION = "1"
EXPORT_PREFIX = "v1"


@dataclass(frozen=True, slots=True)
class ExportedTable:
    name: str
    path: str
    row_count: int
    bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class Manifest:
    manifest_version: str
    generated_at: str
    run_id: str
    logical_date: str
    gold_max_filed_date: str | None
    tables: list[ExportedTable]

    def to_json(self) -> str:
        payload = asdict(self)
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"


# ---------------------------------------------------------------------------------
# Storage backends. Two, because the local suite must exercise the same code path the
# workspace does -- an export that is only tested against a mock is an export nobody
# has actually run.
# ---------------------------------------------------------------------------------


def _is_s3(uri: str) -> bool:
    return uri.startswith(("s3://", "s3a://"))


def _s3_parts(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri.replace("s3a://", "s3://"))
    return parsed.netloc, parsed.path.lstrip("/")


def _read_bytes(uri: str) -> bytes:
    if _is_s3(uri):
        import boto3

        bucket, key = _s3_parts(uri)
        return bytes(boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read())
    return Path(uri.removeprefix("file:")).read_bytes()


def _write_bytes(uri: str, payload: bytes) -> None:
    if _is_s3(uri):
        import boto3

        bucket, key = _s3_parts(uri)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=payload)
        return
    path = Path(uri.removeprefix("file:"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _list_parquet_parts(uri: str) -> list[str]:
    if _is_s3(uri):
        import boto3

        bucket, prefix = _s3_parts(uri)
        resp = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix.rstrip("/") + "/")
        return sorted(
            f"s3://{bucket}/{obj['Key']}"
            for obj in resp.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        )
    root = Path(uri.removeprefix("file:"))
    return sorted(str(p) for p in root.glob("*.parquet"))


def _remove_dir(uri: str) -> None:
    if _is_s3(uri):
        import boto3

        bucket, prefix = _s3_parts(uri)
        client = boto3.client("s3")
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix.rstrip("/") + "/")
        keys = [{"Key": obj["Key"]} for obj in resp.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        return
    shutil.rmtree(Path(uri.removeprefix("file:")), ignore_errors=True)


def export_table(spark: Any, df: Any, name: str, export_root: str, staging_root: str) -> ExportedTable:
    """Write one gold table as a single Parquet object and describe it."""
    staging = f"{staging_root.rstrip('/')}/{name}"
    dest = f"{export_root.rstrip('/')}/{EXPORT_PREFIX}/{name}/data.parquet"

    row_count = int(df.count())
    df.coalesce(1).write.mode("overwrite").parquet(staging)
    parts = _list_parquet_parts(staging)
    if len(parts) != 1:
        raise RuntimeError(
            f"expected exactly one Parquet part for {name} after coalesce(1), found {len(parts)} "
            f"in {staging}. The export contract is one object per table."
        )
    payload = _read_bytes(parts[0])
    _write_bytes(dest, payload)
    _remove_dir(staging)

    return ExportedTable(
        name=name,
        path=f"{EXPORT_PREFIX}/{name}/data.parquet",
        row_count=row_count,
        bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def build_manifest(
    tables: list[ExportedTable],
    *,
    run_id: str,
    logical_date: str,
    gold_max_filed_date: str | None,
    generated_at: str | None = None,
) -> Manifest:
    """Assemble the manifest.

    ``gold_max_filed_date`` is ``max(filed_date)`` from ``silver.filing`` -- the
    freshness of the *source data*, not of the export. Repo 5's ``/health`` reports it,
    and an export timestamp there would claim the data is fresh every time the job
    runs, including the runs that ingested nothing.
    """
    return Manifest(
        manifest_version=MANIFEST_VERSION,
        generated_at=generated_at
        or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        run_id=run_id,
        logical_date=logical_date,
        gold_max_filed_date=gold_max_filed_date,
        tables=tables,
    )


def export_all(spark: Any, settings: Settings, run_ctx: JobRun) -> Manifest:
    """Export every gold table and write the manifest."""
    from pyspark.sql import functions as F

    staging_root = f"{settings.export_root.rstrip('/')}/_staging"
    exported: list[ExportedTable] = []
    # Fixed order (contracts section 5), so identical data yields an identical manifest.
    for spec in schemas.EXPORT_TABLES:
        df = spark.table(settings.table(spec.fqn))
        exported.append(
            export_table(spark, df, spec.name, settings.export_root, staging_root)
        )
        run_ctx.record({f"export.{spec.name}.rows": exported[-1].row_count})
    # Leave nothing behind under the serving prefix but v1/: repo 5 globs this bucket,
    # and a leftover staging directory full of part files is a second source of truth.
    _remove_dir(staging_root)

    max_filed = (
        spark.table(settings.table(schemas.SILVER_FILING.fqn))
        .agg(F.max("filed_date").alias("m"))
        .collect()[0]["m"]
    )
    manifest = build_manifest(
        exported,
        run_id=run_ctx.run_id,
        logical_date=settings.logical_date,
        gold_max_filed_date=str(max_filed) if max_filed is not None else None,
    )
    _write_bytes(
        f"{settings.export_root.rstrip('/')}/{EXPORT_PREFIX}/_manifest.json",
        manifest.to_json().encode(),
    )
    run_ctx.add(rows_out=sum(t.row_count for t in exported))
    return manifest


def run(spark: Any, settings: Settings, run_ctx: JobRun) -> Manifest:
    return export_all(spark, settings, run_ctx)
