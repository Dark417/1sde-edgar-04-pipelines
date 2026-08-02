"""Shared entrypoint plumbing: widgets, settings, preflight."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from edgar_lakehouse_contracts import schemas

from pipelines import config, session
from pipelines.framework.preflight import assert_tables_exist

__all__ = ["bootstrap", "widget_overrides"]

#: `landing_root` is here so a run can be pointed at a landing prefix other than the
#: default -- specifically the seeded fixture prefix (see tools/dbx_seed_landing.py),
#: which is how this pipeline is exercised on Databricks before repo 3 has ever run.
#: Without it the only way to test against fixtures is to write them into the prefix
#: repo 3 owns, which makes real and synthetic data indistinguishable afterwards.
_WIDGETS = (
    "logical_date",
    "catalog",
    "environment",
    "ingest_mode",
    "storage_mode",
    "landing_root",
)


def widget_overrides() -> dict[str, str]:
    """Read job parameters from Databricks widgets. Empty off-platform."""
    if not session.is_databricks():
        return {}
    try:  # pragma: no cover - requires a Databricks runtime
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(session.get_spark())
        out: dict[str, str] = {}
        for name in _WIDGETS:
            try:
                value = dbutils.widgets.get(name)
            except Exception:
                # An unset widget is not an error; env vars still apply.
                continue
            if value:
                out[name] = value
        return out
    except Exception:
        # No widgets available (notebook run, local run). Env vars still apply.
        return {}


def bootstrap(job: str, write_targets: Sequence[str]) -> tuple[Any, config.Settings]:
    """Resolve settings, get a session, and run the table-existence gate.

    Preflight runs **before** any work. Without it the symptom of a missing migration
    is an ``AnalysisException`` two hundred lines into the job, pointing at whichever
    query happened to touch the table first.
    """
    settings = config.resolve(job, overrides=widget_overrides())
    spark = session.get_spark()
    assert_tables_exist(spark, [settings.table(schemas.table(fqn).fqn) for fqn in write_targets])
    return spark, settings
