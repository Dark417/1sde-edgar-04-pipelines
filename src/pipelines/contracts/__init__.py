"""Contract access, with an explicit bridge for the not-yet-published repo 1 wheel.

**Read this before changing anything in this package.**

The design (AGENTS.md section 2) is that schemas, the DQ registry, names and concepts
are imported from ``edgar_lakehouse_contracts``, repo 1's published wheel, pinned with
``==``. That wheel does not exist yet, and repo 4 cannot be built or tested without the
definitions it will contain.

So this package is a **mirror**: an in-repo copy of the definitions repo 4 needs,
written to be replaced. It is not a fork and not a second source of truth:

* :func:`provenance` reports whether the published wheel was found.
* :func:`verify_against_published` diffs the mirror against the wheel when it is
  installed and returns every discrepancy. CI runs it as the contract-compat gate
  (AGENTS.md section 8), so the day repo 1 publishes, any drift blocks the merge
  instead of surfacing as a runtime ``AnalysisException``.
* Nothing else in ``src/pipelines`` imports the mirror modules directly; everything
  goes through this package, so the swap is one import site.

See ``docs/10-decisions.md`` ADR-001 for why a mirror rather than a stub or a vendored
copy of repo 1's source.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from types import ModuleType
from typing import Final, Literal

from . import concepts, dq, envelope, names, schemas
from .models import (
    ColumnSpec,
    ConceptMapping,
    DQCheck,
    Layer,
    Severity,
    Stream,
    TableSpec,
)

__all__ = [
    "PUBLISHED_PACKAGE",
    "ColumnSpec",
    "ConceptMapping",
    "DQCheck",
    "Layer",
    "Severity",
    "Stream",
    "TableSpec",
    "concepts",
    "dq",
    "envelope",
    "names",
    "provenance",
    "schemas",
    "verify_against_published",
]

#: The wheel this mirror stands in for. Pinned with ``==`` in pyproject once published.
PUBLISHED_PACKAGE: Final[str] = "edgar_lakehouse_contracts"


def _published() -> ModuleType | None:
    try:
        return importlib.import_module(PUBLISHED_PACKAGE)
    except ImportError:
        return None


def provenance() -> Literal["published", "mirror"]:
    """Where contract definitions are coming from in this process."""
    return "published" if _published() is not None else "mirror"


@dataclass(frozen=True, slots=True)
class Discrepancy:
    """One difference between the mirror and the published wheel."""

    kind: str
    name: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.name}: {self.detail}"


def verify_against_published() -> list[Discrepancy]:
    """Diff the mirror against the published contracts wheel.

    Returns an empty list when the wheel is not installed -- absence is reported by
    :func:`provenance`, not by a fake discrepancy, so that CI can distinguish "not
    published yet" (expected today) from "published and we drifted" (must block).

    The comparison is one-directional on purpose: every table and column **this repo
    touches** must exist in the wheel with the same type. Columns the wheel has and we
    do not are fine -- repo 1 may serve other consumers.
    """
    published = _published()
    if published is None:
        return []

    out: list[Discrepancy] = []
    try:
        published_tables: dict[str, object] = dict(published.schemas.TABLES)
    except AttributeError as exc:
        return [Discrepancy("api", PUBLISHED_PACKAGE, f"missing schemas.TABLES ({exc})")]

    for fqn, spec in schemas.TABLES.items():
        their = published_tables.get(fqn)
        if their is None:
            out.append(Discrepancy("table", fqn, "absent from published contracts"))
            continue
        their_cols = {c.name: c.type_sql for c in their.columns}  # type: ignore[attr-defined]
        for col in spec.columns:
            if col.name not in their_cols:
                out.append(Discrepancy("column", f"{fqn}.{col.name}", "absent from published"))
            elif their_cols[col.name].upper() != col.type_sql.upper():
                out.append(
                    Discrepancy(
                        "type",
                        f"{fqn}.{col.name}",
                        f"mirror={col.type_sql} published={their_cols[col.name]}",
                    )
                )
    return out
