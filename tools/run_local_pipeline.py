#!/usr/bin/env python3
"""Run the whole medallion locally: landing -> bronze -> silver -> gold -> export.

**Test harness, not a pipeline.** On Databricks these four stages are four job tasks
wired by ``databricks.yml``; here they run in one process against a local Delta
warehouse so the transforms can be exercised end to end without a workspace and
without burning Free Edition quota.

The tables are created by ``tools/local_ddl.py``, standing in for repo 1's Liquibase
(ADR-004). Nothing under ``src/pipelines`` creates a table.

Usage::

    python tools/fetch_test_data.py --out data/landing        # once, needs network
    python tools/run_local_pipeline.py --landing data/landing --warehouse .local/warehouse

Then inspect the result::

    python tools/run_local_pipeline.py --landing data/landing --sql \\
      "SELECT * FROM spark_catalog.gold.restatement_event LIMIT 20"
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
for _extra in (str(_REPO_ROOT / "src"), str(_REPO_ROOT)):
    if _extra not in sys.path:
        sys.path.insert(0, _extra)

from edgar_lakehouse_contracts import schemas  # noqa: E402

from pipelines.bronze import company_concept, company_submissions, filing_index  # noqa: E402
from pipelines.config import Settings, run_id_for  # noqa: E402
from pipelines.export import serving  # noqa: E402
from pipelines.framework.metrics import job_run  # noqa: E402
from pipelines.framework.preflight import assert_tables_exist  # noqa: E402
from pipelines.gold import (  # noqa: E402
    company_profile,
    filing_activity_daily,
    financials_current,
    restatement_event,
)
from pipelines.silver import company, filing, financial_fact  # noqa: E402
from tools.local_ddl import attach_all, create_all  # noqa: E402

LOCAL_CATALOG = "spark_catalog"


def discover_logical_dates(landing_root: Path) -> list[str]:
    """Every ``logical_date=`` partition present under landing, oldest first.

    Bronze is replayed one logical date at a time so that a multi-day fetch exercises
    the SCD-2 close-and-open path rather than collapsing into a single batch.
    """
    dates = {
        part.name.split("=", 1)[1] for part in landing_root.rglob("logical_date=*") if part.is_dir()
    }
    return sorted(dates)


def build_settings(landing: Path, warehouse: Path, logical_date: str) -> Settings:
    return Settings(
        catalog=LOCAL_CATALOG,
        logical_date=logical_date,
        run_id=run_id_for("local", logical_date),
        ingest_mode="batch",
        storage_mode="local",
        landing_root=str(landing),
        checkpoint_root=str(warehouse / "_checkpoints"),
        export_root=str(warehouse / "export"),
        environment="local",
    )


def run_all(spark: Any, settings: Settings, logical_dates: list[str]) -> dict[str, Any]:
    """Run every stage. Returns a summary dict."""
    summary: dict[str, Any] = {"logical_dates": logical_dates, "stages": {}}

    for logical_date in logical_dates:
        day_settings = replace(
            settings, logical_date=logical_date, run_id=run_id_for("local", logical_date)
        )
        assert_tables_exist(
            spark,
            [
                day_settings.table(s.fqn)
                for s in (
                    schemas.BRONZE_FILING_INDEX_RAW,
                    schemas.BRONZE_COMPANY_SUBMISSIONS_RAW,
                    schemas.BRONZE_COMPANY_CONCEPT_RAW,
                )
            ],
        )
        with job_run("bronze_ingest", day_settings.run_id, logical_date) as run:
            for module in (filing_index, company_submissions, company_concept):
                module.ingest(spark, day_settings, run)
            summary["stages"][f"bronze:{logical_date}"] = run.summary()

        with job_run("silver_transform", day_settings.run_id, logical_date) as run:
            filing.run(spark, day_settings, run)
            company.run(spark, day_settings, run)
            financial_fact.run(spark, day_settings, run)
            summary["stages"][f"silver:{logical_date}"] = run.summary()

    last = replace(
        settings, logical_date=logical_dates[-1], run_id=run_id_for("local", logical_dates[-1])
    )
    with job_run("gold_build", last.run_id, last.logical_date) as run:
        restatement_event.run(spark, last, run)
        financials_current.run(spark, last, run)
        filing_activity_daily.run(spark, last, run)
        company_profile.run(spark, last, run)
        summary["stages"]["gold"] = run.summary()

    with job_run("serving_export", last.run_id, last.logical_date) as run:
        manifest = serving.export_all(spark, last, run)
        summary["stages"]["export"] = run.summary()
        summary["manifest"] = json.loads(manifest.to_json())
    return summary


def report(spark: Any, settings: Settings) -> None:
    """Print the row counts and the checks AGENTS.md section 9 asks you to run by hand."""
    print("\n=== table row counts ===")
    for spec in schemas.ALL_TABLES:
        table = settings.table(spec.fqn)
        print(f"{spec.fqn:<45} {spark.table(table).count():>8}")

    print("\n=== restatement events (top 10 by absolute delta) ===")
    spark.table(settings.table(schemas.GOLD_RESTATEMENT_EVENT.fqn)).select(
        "cik",
        "concept_canonical",
        "period_end",
        "original_value",
        "restated_value",
        "delta_pct",
        "materiality_band",
        "days_to_restatement",
    ).orderBy("delta_abs", ascending=False).show(10, truncate=False)

    print("=== quarantine by check ===")
    for spec in schemas.ALL_TABLES:
        if spec.name.endswith("_quarantine"):
            df = spark.table(settings.table(spec.fqn))
            if df.count():
                print(spec.fqn)
                df.groupBy("_dq_check_name").count().show(truncate=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--landing", default="data/landing")
    parser.add_argument("--warehouse", default=".local/warehouse")
    parser.add_argument("--logical-dates", default="", help="comma-separated; default: all present")
    parser.add_argument(
        "--sql", default=None, help="run one query against the local warehouse and exit"
    )
    parser.add_argument("--summary-out", default=None, help="write the stage summaries as JSON")
    args = parser.parse_args(argv)

    from pipelines.session import local_session

    landing = Path(args.landing).resolve()
    warehouse = Path(args.warehouse).resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    spark = local_session(warehouse_dir=str(warehouse))
    spark.sparkContext.setLogLevel("ERROR")

    if args.sql:
        # The local catalog is in-memory and died with the previous session; the Delta
        # data under --warehouse did not. Re-attach before querying.
        attach_all(spark, str(warehouse), LOCAL_CATALOG)
        spark.sql(args.sql).show(50, truncate=False)
        return 0

    logical_dates = [
        d.strip() for d in args.logical_dates.split(",") if d.strip()
    ] or discover_logical_dates(landing)
    if not logical_dates:
        parser.error(
            f"no logical_date= partitions found under {landing}; run fetch_test_data.py first"
        )

    created = create_all(spark, LOCAL_CATALOG)
    print(f"local DDL applied: {len(created)} tables (stand-in for repo 1 Liquibase)")

    settings = build_settings(landing, warehouse, logical_dates[-1])
    summary = run_all(spark, settings, logical_dates)
    report(spark, settings)

    if args.summary_out:
        Path(args.summary_out).write_text(json.dumps(summary, indent=2, default=str) + "\n")
        print(f"\nwrote {args.summary_out}")
    print(f"\nexport written to {settings.export_root}/v1/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
