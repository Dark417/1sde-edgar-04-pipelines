"""L0 -- this repo's DQ check model.

Repo 1 publishes a ``DQCheck`` too, but it has no ``kind``: its registry does not
distinguish a per-row rule from one evaluated over a derived aggregate, and its check
names differ. That distinction is how this pipeline routes work -- row checks feed
quarantine, aggregate checks feed the reject-batch invariants -- so the model and the
registry are pipeline policy and live here. Adopting repo 1's registry wholesale would
silently change *what is validated*, which is the failure this repo has been unwinding
all week.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from edgar_lakehouse_contracts.models import Severity

__all__ = ["DQCheck"]


@dataclass(frozen=True, slots=True)
class DQCheck:
    """One data-quality rule. A row passes when ``expression`` is true; null fails."""

    name: str
    table: str
    expression: str
    severity: Severity
    description: str
    kind: Literal["row", "aggregate"] = "row"
