#!/usr/bin/env python3
"""Check a Databricks workspace against the contract, before deploying anything.

This is `AGENTS.md` §9.2 ("confirm prerequisites before generating") turned into a
command. It answers, in one run:

* Does the token work, and who is it?
* Does the `edgar` catalog exist, with all four schemas? (repo 2's Terraform)
* Does every table this repo writes exist, with the columns the contract names?
  (repo 1's Liquibase)
* Does the landing volume exist, and has repo 3 put anything in it?
* Is the job already defined, and is its schedule paused?

Every one of those is a prerequisite this repo *cannot* create for itself. Finding out
a schema is missing here costs one command; finding out during a job run costs a
mid-job `AnalysisException` and, on Free Edition, a slice of the daily quota.

Read-only. It issues no `CREATE`, no `POST`, and never triggers a run.

Usage::

    export DATABRICKS_HOST=https://dbc-xxxx.cloud.databricks.com
    export DATABRICKS_TOKEN=dapi...          # a workspace PAT, not an account token
    python tools/dbx_verify.py               # exits non-zero if anything is missing
    python tools/dbx_verify.py --json        # machine-readable, for CI
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

from edgar_lakehouse_contracts import names, schemas  # noqa: E402

OK = "ok"
MISSING = "missing"
ERROR = "error"


@dataclass
class Check:
    name: str
    status: str
    detail: str = ""
    items: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.status != OK

    def render(self) -> str:
        mark = {OK: "  ok  ", MISSING: "MISSING", ERROR: " ERROR"}[self.status]
        line = f"[{mark}] {self.name}"
        if self.detail:
            line += f"\n           {self.detail}"
        for item in self.items[:20]:
            line += f"\n             - {item}"
        if len(self.items) > 20:
            line += f"\n             ... and {len(self.items) - 20} more"
        return line


class Workspace:
    """Thin read-only REST client."""

    def __init__(self, host: str, token: str) -> None:
        import requests

        self.host = host.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        )

    def get(self, path: str, **params: Any) -> tuple[int, Any]:
        resp = self.session.get(f"{self.host}{path}", params=params or None, timeout=60)
        try:
            return resp.status_code, resp.json()
        except ValueError:
            return resp.status_code, {"raw": resp.text[:500]}


def check_identity(ws: Workspace) -> Check:
    code, body = ws.get("/api/2.0/preview/scim/v2/Me")
    if code == 200:
        who = body.get("userName") or body.get("displayName") or "unknown"
        return Check("token accepted", OK, f"authenticated as {who}")
    if code == 401:
        return Check(
            "token accepted",
            ERROR,
            "401 from the workspace. A workspace PAT starts with 'dapi' -- an account-level "
            "token, an expired token, or an OAuth secret all fail this way. Generate one at "
            "Settings > Developer > Access tokens.",
        )
    return Check("token accepted", ERROR, f"HTTP {code}: {json.dumps(body)[:200]}")


def check_catalog_and_schemas(ws: Workspace, catalog: str) -> list[Check]:
    code, body = ws.get("/api/2.1/unity-catalog/catalogs")
    if code != 200:
        return [Check("catalog", ERROR, f"HTTP {code} listing catalogs: {json.dumps(body)[:200]}")]
    found = {c["name"] for c in body.get("catalogs", [])}
    if catalog not in found:
        return [
            Check(
                f"catalog {catalog}",
                MISSING,
                "repo 2's Terraform creates it. Catalogs seen: " + ", ".join(sorted(found)),
            )
        ]

    checks = [Check(f"catalog {catalog}", OK)]
    code, body = ws.get("/api/2.1/unity-catalog/schemas", catalog_name=catalog)
    schema_names = {s["name"] for s in body.get("schemas", [])} if code == 200 else set()
    wanted = {
        names.SCHEMA_LANDING,
        names.SCHEMA_BRONZE,
        names.SCHEMA_SILVER,
        names.SCHEMA_GOLD,
    }
    missing = sorted(wanted - schema_names)
    checks.append(
        Check("schemas", OK if not missing else MISSING, "repo 2 owns these", missing)
    )
    return checks


def check_tables(ws: Workspace, catalog: str) -> list[Check]:
    """Every table this repo writes must exist, with the columns the contract names."""
    by_schema: dict[str, dict[str, set[str]]] = {}
    for schema in (names.SCHEMA_BRONZE, names.SCHEMA_SILVER, names.SCHEMA_GOLD):
        code, body = ws.get(
            "/api/2.1/unity-catalog/tables", catalog_name=catalog, schema_name=schema
        )
        if code != 200:
            by_schema[schema] = {}
            continue
        by_schema[schema] = {
            t["name"]: {c["name"] for c in t.get("columns", [])} for t in body.get("tables", [])
        }

    missing_tables: list[str] = []
    missing_columns: list[str] = []
    for spec in schemas.ALL_TABLES:
        present = by_schema.get(spec.schema, {})
        if spec.name not in present:
            missing_tables.append(f"{catalog}.{spec.schema}.{spec.name}  ({spec.changeset})")
            continue
        gaps = sorted(set(spec.column_names) - present[spec.name])
        missing_columns.extend(
            f"{catalog}.{spec.schema}.{spec.name}.{c}  ({spec.changeset})" for c in gaps
        )

    return [
        Check(
            f"tables ({len(schemas.ALL_TABLES)} in the contract)",
            OK if not missing_tables else MISSING,
            "repo 1's Liquibase creates these -- this repo never issues CREATE TABLE",
            missing_tables,
        ),
        Check(
            "table columns match the contract",
            OK if not missing_columns else MISSING,
            "a column the contract names but the table lacks means a changeset was not applied",
            missing_columns,
        ),
    ]


def check_landing(ws: Workspace, catalog: str) -> list[Check]:
    code, body = ws.get(
        "/api/2.1/unity-catalog/volumes", catalog_name=catalog, schema_name=names.SCHEMA_LANDING
    )
    volumes = {v["name"] for v in body.get("volumes", [])} if code == 200 else set()
    checks = [
        Check(
            "landing volume",
            OK if volumes else MISSING,
            f"repo 2 creates it; found: {', '.join(sorted(volumes)) or 'none'}",
        )
    ]

    # Repo 3's output. Absent is not fatal -- it means there is nothing to ingest yet,
    # which is a different problem from a missing table.
    present, empty = [], []
    for stream_name in names.STREAMS:
        path = names.landing_path(names.VOLUME_LANDING, stream_name)
        code, body = ws.get(f"/api/2.0/fs/directories{path}")
        if code == 200 and body.get("contents"):
            present.append(f"{stream_name}: {len(body['contents'])} entries")
        else:
            empty.append(f"{stream_name}: nothing at {path}")
    checks.append(
        Check(
            "landing objects (repo 3 output)",
            OK if present else MISSING,
            "empty landing is not a broken deploy -- it means there is nothing to ingest yet",
            present + empty,
        )
    )
    return checks


def check_jobs(ws: Workspace) -> Check:
    code, body = ws.get("/api/2.1/jobs/list", limit=25)
    if code != 200:
        return Check("job definition", ERROR, f"HTTP {code}")
    jobs = body.get("jobs", [])
    edgar = [j for j in jobs if "edgar" in (j.get("settings", {}).get("name", "").lower())]
    if not edgar:
        return Check(
            "job definition",
            MISSING,
            f"no edgar job yet ({len(jobs)} job(s) in the workspace). "
            "`databricks bundle deploy -t dev` creates it.",
        )
    detail = []
    for job in edgar:
        settings = job.get("settings", {})
        schedule = settings.get("schedule", {})
        state = schedule.get("pause_status", "no schedule")
        detail.append(f"{settings.get('name')} (job {job.get('job_id')}), schedule: {state}")
    return Check("job definition", OK, "; ".join(detail))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default=os.environ.get("DATABRICKS_HOST"))
    parser.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN"))
    parser.add_argument("--catalog", default=os.environ.get("EDGAR_CATALOG", names.CATALOG))
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)

    if not args.host or not args.token:
        parser.error("set DATABRICKS_HOST and DATABRICKS_TOKEN (or pass --host/--token)")

    ws = Workspace(args.host, args.token)
    checks = [check_identity(ws)]
    if not checks[0].failed:
        checks += check_catalog_and_schemas(ws, args.catalog)
        if not any(c.failed for c in checks[1:]):
            checks += check_tables(ws, args.catalog)
            checks += check_landing(ws, args.catalog)
        checks.append(check_jobs(ws))

    if args.json:
        print(json.dumps([c.__dict__ for c in checks], indent=2))
    else:
        print(f"workspace: {args.host}\ncatalog:   {args.catalog}\n")
        for check in checks:
            print(check.render())
        blockers = [c for c in checks if c.failed]
        print()
        if blockers:
            print(f"{len(blockers)} blocker(s). Do NOT deploy until they are resolved:")
            for check in blockers:
                print(f"  - {check.name}")
        else:
            print("all prerequisites present -- safe to `databricks bundle deploy -t dev`")
    return 1 if any(c.failed for c in checks) else 0


if __name__ == "__main__":
    raise SystemExit(main())
