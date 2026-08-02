"""Upload the committed landing fixtures to a Databricks Volume.

**Why this exists.** ``data/landing/`` holds a small, committed slice of real EDGAR
output so the pipelines can be exercised without repo 3 and without hitting the SEC.
That works locally, where Spark reads the directory straight off disk. It does not work
on Databricks, which cannot see your laptop. This tool is the bridge: it re-encodes the
fixtures into exactly the form repo 3 would have written and PUTs them into a Volume.

**It is a test harness, not an ingest path.** Landing objects belong to repo 3
(AGENTS.global.md law 2), so this writes under a marked ``_seed/`` prefix inside the
volume rather than into the stream directories repo 3 owns. Point the pipeline at it
with ``EDGAR_LANDING_ROOT``. Seeding over repo 3's prefix would not raise an error --
it would give you a landing zone where nobody can tell fixtures from real filings.

A *sibling volume* would be the cleaner separation, but volumes are Terraform's to
create (law 2) and repo 4 does not get to add one; a prefix needs no new object.

**What "exactly the form repo 3 would have written" means.** Two details matter, and
getting either wrong produces a bronze table that looks fine and is not:

* **gzip.** The contract writes ``.json.gz``. Spark infers the codec from the file
  extension, so uploading plain ``.json`` under a ``.gz`` name yields an unreadable
  file, and uploading plain ``.json`` under a plain name works locally but diverges
  from what production will actually contain.
* **``mtime=0``.** The gzip header embeds a timestamp by default, so the same fixture
  would produce different bytes on every run and the "run it twice, get identical
  bytes" property this project tests for would quietly stop holding.

Usage::

    export DATABRICKS_HOST=...            # or --host
    export DATABRICKS_TOKEN=...           # or --token
    python -m tools.dbx_seed_landing --dry-run          # show what would be uploaded
    python -m tools.dbx_seed_landing                    # upload
    python -m tools.dbx_seed_landing --root /Volumes/edgar/landing/edgar_seed

Then run the pipeline against it::

    export EDGAR_LANDING_ROOT=/Volumes/edgar/landing/edgar_seed
    databricks bundle run edgar_pipelines -t dev
"""

from __future__ import annotations

import argparse
import gzip
import io
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FIXTURES = REPO_ROOT / "data" / "landing"

#: A marked prefix INSIDE repo 2's volume, not a sibling volume.
#:
#: `/Volumes/<catalog>/<schema>/<volume>/...` -- so `/Volumes/edgar/landing/edgar_seed`
#: would name a *volume* called `edgar_seed`, which does not exist and which repo 4 has
#: no business creating (volumes are Terraform's, law 2). It 404s, which is the correct
#: refusal rather than a confusing one.
#:
#: `_seed` sits beside the stream directories inside the volume repo 2 already provides.
#: The leading underscore matches the convention already used by `_fetch_report.json`
#: and `_checkpoints`, and it cannot collide with a stream name because streams are
#: named in repo 1's contract and none of them start with an underscore.
DEFAULT_VOLUME_ROOT = "/Volumes/edgar/landing/edgar/_seed"

#: Files API refuses paths outside /Volumes. Catching it here names the mistake.
_VOLUME_PREFIX = "/Volumes/"

TIMEOUT_SECONDS = 120


@dataclass(frozen=True)
class Upload:
    """One fixture file and where it is going."""

    source: Path
    target: str
    raw_bytes: int
    gzip_bytes: int

    @property
    def ratio(self) -> str:
        if not self.raw_bytes:
            return "-"
        return f"{self.gzip_bytes / self.raw_bytes:.0%}"


def encode(path: Path) -> bytes:
    """Return the fixture as gzip NDJSON, byte-identical across runs.

    ``mtime=0`` and a pinned ``compresslevel`` are what make that true; the gzip default
    for both is "whatever the environment felt like", which is fine for a backup and
    fatal for a determinism test.
    """
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as gz:
        gz.write(path.read_bytes())
    return buffer.getvalue()


def plan(fixtures: Path, root: str) -> list[Upload]:
    """Work out every upload without performing any of them.

    The fixture layout is already the contract's layout
    (``<stream>/logical_date=YYYY-MM-DD/<file>``), so the relative path is reused
    verbatim and only the extension changes. That is on purpose: a tool that *rewrites*
    the layout is a second place the partitioning convention can drift from repo 1.
    """
    if not fixtures.is_dir():
        raise SystemExit(f"no fixtures at {fixtures}. Run `make fetch-test-data` first.")

    uploads: list[Upload] = []
    for source in sorted(fixtures.rglob("*.json")):
        relative = source.relative_to(fixtures)
        if relative.name.startswith("_"):
            continue  # _fetch_report.json is provenance for humans, not landing data
        if "logical_date=" not in str(relative.parent):
            continue
        encoded = encode(source)
        target = f"{root.rstrip('/')}/{relative.as_posix()}.gz"
        uploads.append(
            Upload(
                source=source,
                target=target,
                raw_bytes=source.stat().st_size,
                gzip_bytes=len(encoded),
            )
        )
    return uploads


def upload(session: requests.Session, host: str, token: str, item: Upload) -> None:
    """PUT one object through the Files API.

    ``overwrite=true`` is what makes re-seeding idempotent: the object key is derived
    from the fixture path, so a second run replaces rather than accumulates.
    """
    url = f"{host.rstrip('/')}/api/2.0/fs/files{item.target}"
    response = session.put(
        url,
        params={"overwrite": "true"},
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        },
        data=encode(item.source),
        timeout=TIMEOUT_SECONDS,
    )
    if response.status_code >= 400:
        raise SystemExit(
            f"upload failed for {item.target}: HTTP {response.status_code} {response.text[:400]}\n"
            "Common causes: the volume does not exist (repo 1/2 create it), the token "
            "lacks WRITE VOLUME, or the path is not under /Volumes."
        )


def _credentials(args: argparse.Namespace) -> tuple[str, str]:
    host = args.host or os.environ.get("DATABRICKS_HOST", "")
    token = args.token or os.environ.get("DATABRICKS_TOKEN", "")
    missing = [
        name
        for name, value in (("DATABRICKS_HOST", host), ("DATABRICKS_TOKEN", token))
        if not value
    ]
    if missing:
        raise SystemExit(
            f"missing {' and '.join(missing)}. Export them, or pass --host/--token. "
            "The host is the workspace URL including https://."
        )
    if not host.startswith("http"):
        host = f"https://{host}"
    return host, token


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument(
        "--root",
        default=DEFAULT_VOLUME_ROOT,
        help=f"Volume prefix to seed into (default: {DEFAULT_VOLUME_ROOT})",
    )
    parser.add_argument("--host", default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument(
        "--dry-run", action="store_true", help="print the plan and exit without uploading"
    )
    args = parser.parse_args(argv)

    if not args.root.startswith(_VOLUME_PREFIX):
        raise SystemExit(f"--root must start with {_VOLUME_PREFIX}, got {args.root!r}")

    uploads = plan(args.fixtures, args.root)
    if not uploads:
        raise SystemExit(f"no landing fixtures found under {args.fixtures}")

    raw = sum(u.raw_bytes for u in uploads)
    packed = sum(u.gzip_bytes for u in uploads)
    for item in uploads:
        print(f"  {item.source.relative_to(args.fixtures).as_posix():<58} -> {item.target}")
        print(f"     {item.raw_bytes:>9,} B raw  ->{item.gzip_bytes:>9,} B gzip ({item.ratio})")
    print(f"\n{len(uploads)} object(s), {raw:,} B raw -> {packed:,} B gzip")

    if args.dry_run:
        print("\ndry run: nothing uploaded.")
        return 0

    host, token = _credentials(args)
    print(f"\nuploading to {host} ...")
    with requests.Session() as session:
        for item in uploads:
            upload(session, host, token, item)
            print(f"  ok  {item.target}")

    print(
        "\nSeeded. Point the pipeline at it and run:\n"
        f"  export EDGAR_LANDING_ROOT={args.root}\n"
        "  databricks bundle run edgar_pipelines -t dev\n\n"
        "Note this root is NOT repo 3's landing prefix -- fixtures stay separable from "
        "real filings."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
