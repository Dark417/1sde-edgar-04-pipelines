"""Local SparkSession + Delta fixtures.

Zero Databricks in the unit tests (AGENTS.md section 7). Everything -- including the
two tests that decide the project -- runs against a local SparkSession with Delta on a
tmpdir.

Tables are created by ``tools/local_ddl.py``, which renders the *same* ``TableSpec``
objects preflight validates against and lives outside the shipped package. See ADR-004.
"""

from __future__ import annotations

import json
import shutil
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
for extra in (_REPO_ROOT / "src", _REPO_ROOT):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

from pipelines.config import Settings  # noqa: E402
from pipelines.contracts.envelope import ENVELOPE_VERSION, SOURCE_SYSTEM  # noqa: E402

LOCAL_CATALOG = "spark_catalog"
LOGICAL_DATE = "2026-07-31"


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    """One SparkSession for the whole suite: JVM startup dominates otherwise."""
    from pipelines.session import local_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = local_session(warehouse_dir=str(warehouse))
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def tables(spark: Any) -> Iterator[str]:
    """Fresh contract tables for one test."""
    from tools.local_ddl import create_all, drop_all

    drop_all(spark, LOCAL_CATALOG)
    create_all(spark, LOCAL_CATALOG)
    yield LOCAL_CATALOG
    drop_all(spark, LOCAL_CATALOG)


@pytest.fixture()
def settings(tmp_path: Path, tables: str) -> Settings:
    """Local-mode settings pointing at a per-test landing/export root."""
    from pipelines.config import run_id_for

    landing = tmp_path / "landing"
    landing.mkdir(parents=True, exist_ok=True)
    return Settings(
        catalog=tables,
        logical_date=LOGICAL_DATE,
        run_id=run_id_for("test", LOGICAL_DATE),
        ingest_mode="batch",
        storage_mode="local",
        landing_root=str(landing),
        checkpoint_root=str(tmp_path / "checkpoints"),
        export_root=str(tmp_path / "export"),
        environment="test",
    )


@pytest.fixture()
def job_run_ctx() -> Any:
    from pipelines.framework.metrics import JobRun

    return JobRun(job="test", run_id="test-run", logical_date=LOGICAL_DATE)


# ---------------------------------------------------------------------------------
# Landing helpers
# ---------------------------------------------------------------------------------


def envelope(
    stream: str,
    resource_id: str,
    payload: Any,
    *,
    logical_date: str = LOGICAL_DATE,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one landing envelope, matching what repo 3 writes."""
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    record: dict[str, Any] = {
        "envelope_version": ENVELOPE_VERSION,
        "source_system": SOURCE_SYSTEM,
        "stream": stream,
        "resource_id": resource_id,
        "logical_date": logical_date,
        "batch_id": f"{stream}-{logical_date}",
        "fetched_at": f"{logical_date}T12:00:00Z",
        "request_url": f"https://example.invalid/{stream}/{resource_id}",
        "http_status": 200,
        "content_sha256": "0" * 64,
        "payload_json": payload_json,
    }
    if extra:
        record.update(extra)
    return record


def write_landing(
    landing_root: str | Path,
    stream: str,
    envelopes: list[dict[str, Any]],
    *,
    logical_date: str = LOGICAL_DATE,
    part: str = "part-00000",
) -> Path:
    """Write envelopes as one NDJSON landing object; returns its path."""
    path = Path(landing_root) / stream / f"logical_date={logical_date}" / f"{part}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for env in envelopes:
            fh.write(json.dumps(env, separators=(",", ":"), sort_keys=True))
            fh.write("\n")
    return path


def index_payload(
    accession: str,
    cik: str,
    form_type: str = "10-K",
    company_name: str = "TEST FILER INC",
    date_filed: str = "20260731",
) -> dict[str, str]:
    return {
        "form_type": form_type,
        "company_name": company_name,
        "cik": cik,
        "date_filed": date_filed,
        "accession_number": accession,
        "file_name": f"edgar/data/{cik}/{accession}.txt",
    }


def submissions_payload(
    cik: str,
    name: str = "TEST FILER INC",
    *,
    tickers: list[str] | None = None,
    exchanges: list[str] | None = None,
    sic: str = "1311",
) -> dict[str, Any]:
    return {
        "cik": cik,
        "name": name,
        "entityType": "operating",
        "sic": sic,
        "sicDescription": "Crude Petroleum and Natural Gas",
        "ein": "123456789",
        "fiscalYearEnd": "1231",
        "stateOfIncorporation": "DE",
        "tickers": tickers if tickers is not None else ["TST"],
        "exchanges": exchanges if exchanges is not None else ["Nasdaq"],
        "formerNames": [],
        "filings": {"recent": {}, "files": []},
    }


def fact(
    *,
    start: str | None,
    end: str,
    val: float,
    accn: str,
    filed: str,
    form: str = "10-K",
    fy: int = 2025,
    fp: str = "FY",
    frame: str | None = None,
    decimals: int | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "end": end,
        "val": val,
        "accn": accn,
        "fy": fy,
        "fp": fp,
        "form": form,
        "filed": filed,
    }
    if start is not None:
        entry["start"] = start
    if frame is not None:
        entry["frame"] = frame
    if decimals is not None:
        entry["decimals"] = decimals
    return entry


def concept_payload(
    cik: str,
    tag: str,
    units: dict[str, list[dict[str, Any]]],
    *,
    taxonomy: str = "us-gaap",
) -> dict[str, Any]:
    return {
        "cik": int(cik),
        "taxonomy": taxonomy,
        "tag": tag,
        "label": tag,
        "description": "",
        "entityName": "TEST FILER INC",
        "units": units,
    }


@pytest.fixture()
def landing(settings: Settings) -> Any:
    """Convenience writer bound to the test's landing root."""

    class _Landing:
        root = settings.landing_root

        @staticmethod
        def write(stream: str, envelopes: list[dict[str, Any]], **kwargs: Any) -> Path:
            return write_landing(settings.landing_root, stream, envelopes, **kwargs)

        @staticmethod
        def clear() -> None:
            shutil.rmtree(settings.landing_root, ignore_errors=True)

    return _Landing()
