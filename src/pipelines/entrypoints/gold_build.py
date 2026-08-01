"""Job task: silver -> gold.

Order matters: ``restatement_event`` is built first because ``financials_current`` and
``company_profile`` both read it.
"""

from __future__ import annotations

from pipelines.contracts import schemas
from pipelines.framework.metrics import job_run
from pipelines.gold import (
    company_profile,
    filing_activity_daily,
    financials_current,
    restatement_event,
)

from ._common import bootstrap

JOB = "gold_build"
WRITE_TARGETS = (
    schemas.GOLD_RESTATEMENT_EVENT.fqn,
    schemas.GOLD_FINANCIALS_CURRENT.fqn,
    schemas.GOLD_FILING_ACTIVITY_DAILY.fqn,
    schemas.GOLD_COMPANY_PROFILE.fqn,
)


def main() -> None:
    spark, settings = bootstrap(JOB, WRITE_TARGETS)
    with job_run(JOB, settings.run_id, settings.logical_date) as run:
        restatement_event.run(spark, settings, run)
        financials_current.run(spark, settings, run)
        filing_activity_daily.run(spark, settings, run)
        company_profile.run(spark, settings, run)


if __name__ == "__main__":
    main()
