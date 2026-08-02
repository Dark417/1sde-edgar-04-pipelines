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
from collections.abc import Iterable, Mapping
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


def _published_column_specs() -> dict[str, dict[str, tuple[str, bool]]] | Discrepancy:
    """Published table shapes as ``{fqn: {column: (type_sql, nullable)}}``.

    Repo 1 publishes these as ``spark.schemas.COLUMN_SPECS``, a mapping of fqn to a
    tuple of ``(name, type_sql, nullable)`` triples. It is a submodule that the package
    root does not import, so it has to be imported explicitly rather than reached
    through the parent.
    """
    try:
        module = importlib.import_module(f"{PUBLISHED_PACKAGE}.spark.schemas")
        raw: Mapping[str, Iterable[tuple[str, str, bool]]] = module.COLUMN_SPECS
    except (ImportError, AttributeError) as exc:
        return Discrepancy("api", PUBLISHED_PACKAGE, f"no spark.schemas.COLUMN_SPECS ({exc})")

    return {
        fqn: {name: (type_sql, nullable) for name, type_sql, nullable in cols}
        for fqn, cols in raw.items()
    }


def verify_against_published() -> list[Discrepancy]:
    """Diff the mirror against the published contracts wheel.

    Returns an empty list when the wheel is not installed -- absence is reported by
    :func:`provenance`, not by a fake discrepancy.

    **That escape hatch is why v0.1.0 drifted undetected.** The wheel was never a
    declared dependency, so this returned ``[]`` on every CI run and the contract-compat
    gate passed while the mirror disagreed with repo 1 on all eleven envelope field
    names and on every one of the thirteen tables. The wheel is now a dev dependency
    (see pyproject) and ``tests/test_contract_compat.py`` asserts
    ``provenance() == "published"``, so the empty-list path can no longer be reached in
    CI. Keep both halves: the escape hatch is what lets a Databricks job import this
    package without the wheel, and the test is what stops it hiding drift.

    The table comparison is one-directional on purpose: every table and column **this
    repo touches** must exist in the wheel with the same type. Columns the wheel has and
    we do not are fine -- repo 1 may serve other consumers.

    The envelope comparison is bidirectional: the envelope is a wire format shared with
    repo 3, so a field present on only one side is drift in either direction.
    """
    published = _published()
    if published is None:
        return []

    out: list[Discrepancy] = []

    specs = _published_column_specs()
    if isinstance(specs, Discrepancy):
        return [specs]

    for fqn, spec in schemas.TABLES.items():
        their_cols = specs.get(fqn)
        if their_cols is None:
            out.append(Discrepancy("table", fqn, "absent from published contracts"))
            continue
        for col in spec.columns:
            their = their_cols.get(col.name)
            if their is None:
                out.append(Discrepancy("column", f"{fqn}.{col.name}", "absent from published"))
                continue
            their_type, their_nullable = their
            if their_type.upper() != col.type_sql.upper():
                out.append(
                    Discrepancy(
                        "type",
                        f"{fqn}.{col.name}",
                        f"mirror={col.type_sql} published={their_type}",
                    )
                )
            elif their_nullable != col.nullable:
                out.append(
                    Discrepancy(
                        "nullability",
                        f"{fqn}.{col.name}",
                        f"mirror nullable={col.nullable} published nullable={their_nullable}",
                    )
                )

    out.extend(_verify_envelope(published))
    return out


def _verify_envelope(published: ModuleType) -> list[Discrepancy]:
    """Compare the landing envelope field-by-field, in both directions."""
    try:
        their_fields: dict[str, str] = dict(
            importlib.import_module(f"{PUBLISHED_PACKAGE}.envelope").ENVELOPE_FIELDS
        )
    except (ImportError, AttributeError) as exc:
        return [Discrepancy("api", PUBLISHED_PACKAGE, f"no envelope.ENVELOPE_FIELDS ({exc})")]

    out: list[Discrepancy] = []
    ours = envelope.ENVELOPE_FIELDS
    for name, type_sql in ours.items():
        if name not in their_fields:
            out.append(Discrepancy("envelope", name, "bronze reads it; repo 1 does not publish it"))
        elif their_fields[name].upper() != type_sql.upper():
            out.append(
                Discrepancy(
                    "envelope-type", name, f"mirror={type_sql} published={their_fields[name]}"
                )
            )
    for name in their_fields:
        if name not in ours:
            out.append(Discrepancy("envelope", name, "repo 1 publishes it; bronze ignores it"))
    return out
