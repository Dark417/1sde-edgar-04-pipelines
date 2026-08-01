"""Job task: bronze -> silver, all three entities."""

from __future__ import annotations

from pipelines.contracts import schemas
from pipelines.framework.metrics import job_run
from pipelines.silver import company, filing, financial_fact

from ._common import bootstrap

JOB = "silver_transform"
WRITE_TARGETS = (
    schemas.SILVER_FILING.fqn,
    schemas.SILVER_FILING_QUARANTINE.fqn,
    schemas.SILVER_COMPANY.fqn,
    schemas.SILVER_COMPANY_QUARANTINE.fqn,
    schemas.SILVER_FINANCIAL_FACT.fqn,
    schemas.SILVER_FINANCIAL_FACT_QUARANTINE.fqn,
)


def main() -> None:
    spark, settings = bootstrap(JOB, WRITE_TARGETS)
    with job_run(JOB, settings.run_id, settings.logical_date) as run:
        filing.run(spark, settings, run)
        company.run(spark, settings, run)
        financial_fact.run(spark, settings, run)


if __name__ == "__main__":
    main()
