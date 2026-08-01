"""The contract-compat gate (AGENTS.md section 8).

Today repo 1's wheel is not published and this is a no-op that documents why. The day
it is published, any drift between the in-repo mirror and the wheel blocks the merge
instead of surfacing as a runtime ``AnalysisException``. See ADR-001.
"""

from __future__ import annotations

import pipelines.contracts as contracts


def test_mirror_matches_the_published_contracts_when_it_is_installed() -> None:
    discrepancies = contracts.verify_against_published()
    assert not discrepancies, "contracts drift:\n" + "\n".join(str(d) for d in discrepancies)


def test_provenance_is_reported_not_guessed() -> None:
    """Absence of the wheel is reported by provenance(), never as a fake discrepancy --
    CI must be able to tell "not published yet" from "published and we drifted"."""
    assert contracts.provenance() in ("mirror", "published")
    if contracts.provenance() == "mirror":
        assert contracts.verify_against_published() == []


def test_every_table_this_repo_writes_is_in_the_contract() -> None:
    from pipelines.entrypoints import bronze_ingest, gold_build, silver_transform

    for entrypoint in (bronze_ingest, silver_transform, gold_build):
        for fqn in entrypoint.WRITE_TARGETS:
            assert fqn in contracts.schemas.TABLES, fqn
