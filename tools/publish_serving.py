"""Publish the gold Parquet export from the Volume to the serving bucket.

**Why this exists rather than the job writing S3 directly.** The clean design is a Unity
Catalog external location over ``s3://edgar-lake-serving``, so ``serving_export`` writes
S3 natively. That was attempted and does not work on Free Edition: the storage
credential is created successfully, but every ``external-locations`` create fails
validation with ``PERMISSION_DENIED / 403`` even with a correct trust policy (UC master
role + external id, self-assume without the condition) and a correct inline S3 policy.
The workspace is backed by Databricks-managed storage and does not appear to permit a
customer-managed location.

So the export lands on a Volume, and this tool moves it to S3 from somewhere that *does*
hold AWS credentials -- CI, or a laptop with the ``edgar`` profile. It is one hop, it is
idempotent, and it keeps repo 5's contract intact: repo 5 still reads
``s3://edgar-lake-serving/v1/{table}/data.parquet`` and never talks to Databricks.

Replace this the day the workspace supports an external location; the job already knows
how to write ``s3://`` directly, so that change is a parameter, not a rewrite.

Usage::

    export DATABRICKS_HOST=... DATABRICKS_TOKEN=...   # to read the Volume
    export AWS_PROFILE=edgar                          # to write the bucket
    python -m tools.publish_serving --dry-run
    python -m tools.publish_serving
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_VOLUME_EXPORT = "dbfs:/Volumes/edgar/landing/edgar/_export/v1"
DEFAULT_BUCKET = "s3://edgar-lake-serving"

#: repo 5 reads these four, plus the manifest. A publish that silently drops one leaves
#: the API serving a table that has quietly stopped updating, which is worse than an
#: obvious failure -- so the count is asserted rather than assumed.
EXPECTED_TABLES = (
    "financials_current",
    "restatement_event",
    "filing_activity_daily",
    "company_profile",
)


def _run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"{' '.join(cmd[:3])}… failed:\n{result.stderr.strip()[:500]}")
    return result.stdout


def pull(volume_path: str, into: Path) -> list[Path]:
    """Copy the export off the Volume. Requires DATABRICKS_HOST / DATABRICKS_TOKEN."""
    _run(["databricks", "fs", "cp", "-r", volume_path, str(into)])
    return sorted(p for p in into.rglob("*") if p.is_file())


def verify(files: list[Path]) -> None:
    """Refuse to publish a partial export.

    ``--delete`` on the sync means a missing table here would *remove* it from the
    bucket, turning a bad pipeline run into a data outage for repo 5.
    """
    names = {p.parent.name for p in files}
    missing = [t for t in EXPECTED_TABLES if t not in names]
    if missing:
        raise SystemExit(f"export is missing {missing}; refusing to publish a partial set")
    if not any(p.name == "_manifest.json" for p in files):
        raise SystemExit("export has no _manifest.json; repo 5 uses it for /health")


def push(source: Path, bucket: str, *, dry_run: bool) -> str:
    cmd = ["aws", "s3", "sync", str(source), f"{bucket}/", "--delete"]
    if dry_run:
        cmd.append("--dryrun")
    return _run(cmd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume-export", default=DEFAULT_VOLUME_EXPORT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            raise SystemExit(f"{var} is required to read the Volume export")

    staging = Path(tempfile.mkdtemp(prefix="edgar-serving-"))
    try:
        files = pull(args.volume_export, staging / "v1")
        verify(files)
        total = sum(p.stat().st_size for p in files)
        print(f"  {len(files)} file(s), {total:,} B staged from {args.volume_export}")
        print(push(staging, args.bucket, dry_run=args.dry_run).strip()[-600:])
        if args.dry_run:
            print("\ndry run: nothing uploaded.")
        else:
            print(f"\nPublished to {args.bucket}/v1/. Repo 5 reads this path.")
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
