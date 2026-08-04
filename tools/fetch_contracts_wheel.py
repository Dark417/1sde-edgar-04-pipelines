"""Download the pinned repo-1 contracts wheel into ``dist/contracts/``.

**Why this exists.** ``databricks.yml`` lists ``./dist/contracts/*.whl`` as a serverless
dependency, because ``edgar-lakehouse-contracts`` is not on PyPI and serverless cannot
resolve it from an index. But ``dist/`` is gitignored and nothing populated it, so the
glob was only ever satisfied by whatever a previous session happened to leave on disk:

* on a fresh clone it matches nothing, and the job installs no contracts package at all;
* on a stale checkout it matches an old version. That is what happened -- a deploy
  shipped ``edgar_lakehouse_contracts-1.4.0`` while ``pyproject.toml`` pinned 1.4.1,
  silently, because ``bundle deploy`` prints the filename and nobody reads it.

Both failures are the vendored-contract problem wearing a different hat (root law 11):
two places disagree about one version and neither is checked. So the version is read from
``pyproject.toml`` -- the single pin -- and the download is verified against it.

Usage::

    python -m tools.fetch_contracts_wheel          # before `databricks bundle deploy`
    python -m tools.fetch_contracts_wheel --check  # verify only, exit 1 on drift
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

CONTRACTS_REPO = "Dark417/1sde-edgar-01-contracts"
DEST = Path("dist/contracts")
_PIN = re.compile(r"edgar-lakehouse-contracts==([0-9][0-9A-Za-z.\-]*)")


def pinned_version(pyproject: Path = Path("pyproject.toml")) -> str:
    """Return the exact pinned contracts version, or fail loudly."""
    match = _PIN.search(pyproject.read_text(encoding="utf-8"))
    if not match:
        raise SystemExit(
            f"{pyproject} has no 'edgar-lakehouse-contracts==<version>' pin. "
            "The wheel version is not this repo's to invent -- add the pin first."
        )
    return match.group(1)


def present(version: str) -> list[Path]:
    return sorted(DEST.glob(f"edgar_lakehouse_contracts-{version}-*.whl"))


def stale(version: str) -> list[Path]:
    return sorted(p for p in DEST.glob("edgar_lakehouse_contracts-*.whl") if p not in present(version))


def fetch(version: str) -> None:
    DEST.mkdir(parents=True, exist_ok=True)
    # Remove other versions first: the bundle globs the directory, so leaving an old
    # wheel behind means shipping two versions and letting pip pick.
    for old in stale(version):
        print(f"  removing stale {old.name}")
        old.unlink()
    if present(version):
        print(f"  {version} already present")
        return
    print(f"  downloading v{version} from {CONTRACTS_REPO} releases")
    result = subprocess.run(
        [
            "gh", "release", "download", f"v{version}",
            "--repo", CONTRACTS_REPO,
            "--pattern", "edgar_lakehouse_contracts-*.whl",
            "--dir", str(DEST),
            "--clobber",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"could not download v{version} from {CONTRACTS_REPO}:\n"
            f"{result.stderr.strip()[:400]}\n"
            "Repo 1 attaches the wheel to a GitHub release on every v* tag -- if the "
            "release is missing, tag repo 1 rather than loosening the pin here."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify only; do not download")
    args = parser.parse_args(argv)

    version = pinned_version()
    print(f"pinned contracts version: {version}")

    if args.check:
        found, extra = present(version), stale(version)
        if not found:
            print(f"MISSING: {DEST}/ has no {version} wheel; run without --check", file=sys.stderr)
            return 1
        if extra:
            print(f"STALE: {DEST}/ also holds {[p.name for p in extra]}", file=sys.stderr)
            return 1
        print(f"  OK: {found[0].name}")
        return 0

    fetch(version)
    print(f"  {DEST}/ now holds: {[p.name for p in present(version)]}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
