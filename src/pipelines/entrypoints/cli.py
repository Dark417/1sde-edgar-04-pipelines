"""``edgar-pipelines <task>`` -- one dispatcher, used by the bundle's python_wheel_task."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable, Sequence

from . import bronze_ingest, gold_build, serving_export, silver_transform

__all__ = ["TASKS", "main"]

TASKS: dict[str, Callable[[], None]] = {
    bronze_ingest.JOB: bronze_ingest.main,
    silver_transform.JOB: silver_transform.main,
    gold_build.JOB: gold_build.main,
    serving_export.JOB: serving_export.main,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="edgar-pipelines")
    parser.add_argument("task", choices=sorted(TASKS))
    parser.add_argument(
        "settings",
        nargs="*",
        metavar="key=value",
        help=(
            "Config overrides, e.g. logical_date=2026-07-31. Exported as EDGAR_<KEY> so "
            "they flow through the normal env -> SSM -> default resolution."
        ),
    )
    args = parser.parse_args(list(argv) if argv is not None else sys.argv[1:])

    # A python_wheel_task does NOT receive job parameters as Databricks widgets -- that
    # is a notebook-task behaviour -- so widget_overrides() comes back empty on
    # serverless and every setting falls through to "missing required configuration".
    # The bundle therefore forwards them as `key={{job.parameters.key}}` argv entries and
    # they are re-exported here into the env vars config already reads.
    #
    # Empty values are skipped, not exported: an unset job parameter substitutes to the
    # empty string, and exporting that would shadow SSM and the default with "".
    for pair in args.settings:
        key, _, value = pair.partition("=")
        if value:
            os.environ[f"EDGAR_{key.strip().upper()}"] = value

    TASKS[args.task]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
