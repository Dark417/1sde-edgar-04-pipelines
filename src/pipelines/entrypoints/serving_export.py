"""Job task: gold -> Parquet on S3 + manifest."""

from __future__ import annotations

from edgar_lakehouse_contracts import schemas

from pipelines.export import serving
from pipelines.framework.metrics import job_run

from ._common import bootstrap

JOB = "serving_export"
# The export reads gold and silver.filing (for the freshness stamp) but writes no
# table; preflight still asserts every table it reads, for the same reason.
READ_TARGETS = (*[s.fqn for s in schemas.EXPORT_TABLES], schemas.SILVER_FILING.fqn)


def main() -> None:
    spark, settings = bootstrap(JOB, READ_TARGETS)
    with job_run(JOB, settings.run_id, settings.logical_date) as run:
        manifest = serving.export_all(spark, settings, run)
        run.record({"export.tables": len(manifest.tables)})


if __name__ == "__main__":
    main()
