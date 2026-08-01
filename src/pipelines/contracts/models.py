"""Pure-Python contract primitives.

Deliberately free of any PySpark import: these types are consumed by ``mypy --strict``
and by tooling (CI compatibility checks, local DDL generation) that must run without a
JVM. Spark ``StructType`` objects are *derived* from :class:`TableSpec` on demand by
:func:`pipelines.contracts.schemas.struct_for`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal

__all__ = [
    "ColumnSpec",
    "ConceptMapping",
    "DQCheck",
    "Layer",
    "Severity",
    "Stream",
    "TableSpec",
]


class Layer(str, Enum):
    """Medallion layer a table belongs to."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class Severity(str, Enum):
    """Data-quality severities.

    Three, not two (AGENTS.md rule 8):

    ``reject``
        The failing rows are moved to the quarantine table. The batch continues.
    ``warn``
        Nothing is removed. A metric is emitted and the job status becomes ``WARN``.
    ``reject_batch``
        The whole batch is abandoned by raising ``DQBatchFailure``. Reserved for
        structural invariants -- an SCD-2 dimension with two current rows for one
        natural key fans out every downstream join, so a single bad row is fatal.
    """

    REJECT = "reject"
    WARN = "warn"
    REJECT_BATCH = "reject_batch"


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One column of a contract table."""

    name: str
    type_sql: str
    nullable: bool = True
    comment: str = ""

    def ddl(self) -> str:
        not_null = "" if self.nullable else " NOT NULL"
        comment = f" COMMENT '{self.comment.replace(chr(39), chr(39) * 2)}'" if self.comment else ""
        return f"{self.name} {self.type_sql}{not_null}{comment}"


@dataclass(frozen=True, slots=True)
class TableSpec:
    """A table this repo reads or writes.

    ``changeset`` names the Liquibase changelog file in repo 1 that creates the table.
    It exists so preflight can turn "table not found" into "changeset 020-silver.yaml
    was never applied", which is a one-line diagnosis instead of a bisect.
    """

    catalog: str
    schema: str
    name: str
    layer: Layer
    columns: tuple[ColumnSpec, ...]
    changeset: str
    business_key: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()
    comment: str = ""

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> ColumnSpec:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.fqn} has no column {name!r}")

    def with_catalog(self, catalog: str) -> TableSpec:
        """Return a copy bound to another catalog (used by local tests)."""
        return TableSpec(
            catalog=catalog,
            schema=self.schema,
            name=self.name,
            layer=self.layer,
            columns=self.columns,
            changeset=self.changeset,
            business_key=self.business_key,
            partition_by=self.partition_by,
            comment=self.comment,
        )


@dataclass(frozen=True, slots=True)
class DQCheck:
    """One data-quality rule.

    ``expression`` is a Spark SQL boolean expression evaluated against the DataFrame
    under check. **A row passes when the expression is true.** Null is treated as a
    failure, so write checks that are explicit about nullability rather than relying on
    three-valued logic.

    ``kind`` distinguishes a per-record rule from one evaluated over a derived
    aggregate. Aggregate checks still run through ``apply_dq`` -- the caller passes the
    aggregated DataFrame -- which keeps a single code path and a single metrics shape.
    """

    name: str
    table: str
    expression: str
    severity: Severity
    description: str
    kind: Literal["row", "aggregate"] = "row"


@dataclass(frozen=True, slots=True)
class ConceptMapping:
    """Maps a taxonomy tag to the canonical concept used by gold and the API.

    ``preference`` breaks ties when a filer reports more than one source tag for the
    same canonical concept in the same period: lowest wins. Without an explicit,
    deterministic ordering the chosen tag depends on join order, which violates the
    determinism law (AGENTS.global.md rule 5).
    """

    canonical: str
    taxonomy: str
    tag: str
    preference: int
    unit_class: str
    label: str = ""


@dataclass(frozen=True, slots=True)
class Stream:
    """A landing stream produced by repo 3 and consumed by bronze here."""

    name: str
    landing_prefix: str
    bronze_table: str
    payload_mode: Literal["passthrough", "opaque_json"]
    resource_grain: str = ""
    description: str = ""
    passthrough_columns: tuple[str, ...] = field(default=())
