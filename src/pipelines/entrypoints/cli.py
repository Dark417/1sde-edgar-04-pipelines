"""``edgar-pipelines <task>`` -- one dispatcher, used by the bundle's python_wheel_task."""

from __future__ import annotations

import argparse
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
    args = parser.parse_args(list(argv) if argv is not None else sys.argv[1:])
    TASKS[args.task]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
