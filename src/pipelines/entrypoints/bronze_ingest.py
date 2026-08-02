"""Job task: landing -> bronze, all three streams."""

from __future__ import annotations

from edgar_lakehouse_contracts import schemas

from pipelines.bronze import company_concept, company_submissions, filing_index
from pipelines.framework.metrics import job_run

from ._common import bootstrap

JOB = "bronze_ingest"
WRITE_TARGETS = (
    schemas.BRONZE_FILING_INDEX_RAW.fqn,
    schemas.BRONZE_COMPANY_SUBMISSIONS_RAW.fqn,
    schemas.BRONZE_COMPANY_CONCEPT_RAW.fqn,
)


def main() -> None:
    spark, settings = bootstrap(JOB, WRITE_TARGETS)
    with job_run(JOB, settings.run_id, settings.logical_date) as run:
        total = 0
        for module in (filing_index, company_submissions, company_concept):
            stats = module.ingest(spark, settings, run)
            total += stats.rows_appended
        run.add(rows_out=total)


if __name__ == "__main__":
    main()
