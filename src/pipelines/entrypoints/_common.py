"""Shared entrypoint plumbing: widgets, settings, preflight."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pipelines import config, session
from pipelines.contracts import schemas
from pipelines.framework.preflight import assert_tables_exist

__all__ = ["bootstrap", "widget_overrides"]

_WIDGETS = ("logical_date", "catalog", "environment", "ingest_mode", "storage_mode")


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
