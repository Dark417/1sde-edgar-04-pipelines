"""The workspace prerequisite checker. No Spark, no network -- the client is stubbed.

The point of these tests is that a *missing* prerequisite is reported as missing. A
verifier that quietly returns "ok" when it could not see the tables is worse than no
verifier: it turns a one-command diagnosis into a mid-job AnalysisException.
"""

from __future__ import annotations

from typing import Any

from tools.dbx_verify import (
    MISSING,
    OK,
    check_catalog_and_schemas,
    check_identity,
    check_jobs,
    check_tables,
)

from pipelines.contracts import schemas


class FakeWorkspace:
    """Returns canned responses keyed by path prefix."""

    def __init__(self, responses: dict[str, tuple[int, Any]]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, path: str, **params: Any) -> tuple[int, Any]:
        self.calls.append(path)
        for prefix, response in self.responses.items():
            if path.startswith(prefix):
                body = response[1]
                if callable(body):
                    return response[0], body(params)
                return response
        return 404, {}


def test_a_rejected_token_says_what_kind_of_token_is_needed() -> None:
    """401 here is nearly always the wrong *kind* of credential, not a wrong host --
    the message has to say so or the next hour goes into the URL."""
    ws = FakeWorkspace({"/api/2.0/preview/scim": (401, {"error_code": 401})})
    check = check_identity(ws)
    assert check.failed
    assert "dapi" in check.detail
    assert "Access tokens" in check.detail


def test_a_working_token_reports_who_it_is() -> None:
    ws = FakeWorkspace({"/api/2.0/preview/scim": (200, {"userName": "someone@example.com"})})
    check = check_identity(ws)
    assert check.status == OK
    assert "someone@example.com" in check.detail


def test_missing_catalog_points_at_repo_two() -> None:
    ws = FakeWorkspace({"/api/2.1/unity-catalog/catalogs": (200, {"catalogs": [{"name": "main"}]})})
    checks = check_catalog_and_schemas(ws, "edgar")
    assert checks[0].status == MISSING
    assert "Terraform" in checks[0].detail


def test_missing_schemas_are_listed_by_name() -> None:
    ws = FakeWorkspace(
        {
            "/api/2.1/unity-catalog/catalogs": (200, {"catalogs": [{"name": "edgar"}]}),
            "/api/2.1/unity-catalog/schemas": (
                200,
                {"schemas": [{"name": "bronze"}, {"name": "silver"}]},
            ),
        }
    )
    checks = check_catalog_and_schemas(ws, "edgar")
    assert checks[0].status == OK
    assert checks[1].status == MISSING
    assert sorted(checks[1].items) == ["gold", "landing"]


def _all_tables_response(params: dict[str, Any]) -> dict[str, Any]:
    schema = params["schema_name"]
    return {
        "tables": [
            {"name": s.name, "columns": [{"name": c} for c in s.column_names]}
            for s in schemas.ALL_TABLES
            if s.schema == schema
        ]
    }


def test_a_complete_workspace_passes_the_table_check() -> None:
    ws = FakeWorkspace({"/api/2.1/unity-catalog/tables": (200, _all_tables_response)})
    checks = check_tables(ws, "edgar")
    assert all(c.status == OK for c in checks), [c.items for c in checks]


def test_a_missing_table_is_reported_with_its_changeset() -> None:
    """Naming the changeset is the whole value of the check -- "table missing" alone
    still leaves you bisecting repo 1's changelog."""

    def response(params: dict[str, Any]) -> dict[str, Any]:
        body = _all_tables_response(params)
        body["tables"] = [t for t in body["tables"] if t["name"] != "filing"]
        return body

    ws = FakeWorkspace({"/api/2.1/unity-catalog/tables": (200, response)})
    table_check = check_tables(ws, "edgar")[0]
    assert table_check.status == MISSING
    assert any("edgar.silver.filing" in i and "020-silver.yaml" in i for i in table_check.items)


def test_a_missing_column_is_reported_separately_from_a_missing_table() -> None:
    """A table that exists with the wrong columns is a partially-applied migration, and
    it fails much later and much more confusingly than a missing table."""

    def response(params: dict[str, Any]) -> dict[str, Any]:
        body = _all_tables_response(params)
        for table in body["tables"]:
            if table["name"] == "filing":
                table["columns"] = [c for c in table["columns"] if c["name"] != "_first_seen_ts"]
        return body

    ws = FakeWorkspace({"/api/2.1/unity-catalog/tables": (200, response)})
    tables_check, columns_check = check_tables(ws, "edgar")
    assert tables_check.status == OK
    assert columns_check.status == MISSING
    assert any("_first_seen_ts" in i for i in columns_check.items)


def test_an_unreachable_tables_api_reports_missing_not_ok() -> None:
    """Silence must not read as success."""
    ws = FakeWorkspace({"/api/2.1/unity-catalog/tables": (403, {})})
    assert check_tables(ws, "edgar")[0].status == MISSING


def test_no_job_yet_is_reported_with_the_command_that_creates_it() -> None:
    ws = FakeWorkspace({"/api/2.1/jobs/list": (200, {"jobs": []})})
    check = check_jobs(ws)
    assert check.status == MISSING
    assert "bundle deploy" in check.detail


def test_an_existing_job_reports_its_schedule_state() -> None:
    """The schedule must stay paused until the manual checks pass (AGENTS.md 9.9)."""
    ws = FakeWorkspace(
        {
            "/api/2.1/jobs/list": (
                200,
                {
                    "jobs": [
                        {
                            "job_id": 42,
                            "settings": {
                                "name": "edgar-medallion-dev",
                                "schedule": {"pause_status": "PAUSED"},
                            },
                        }
                    ]
                },
            )
        }
    )
    check = check_jobs(ws)
    assert check.status == OK
    assert "PAUSED" in check.detail
    assert "42" in check.detail
