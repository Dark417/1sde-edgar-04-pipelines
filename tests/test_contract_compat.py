"""The contract-compat gate (AGENTS.md section 8).

Today repo 1's wheel is not published and this is a no-op that documents why. The day
it is published, any drift between the in-repo mirror and the wheel blocks the merge
instead of surfacing as a runtime ``AnalysisException``. See ADR-001.
"""

from __future__ import annotations

import pipelines.contracts as contracts


def test_the_published_wheel_is_actually_installed() -> None:
    """The gate that makes every other check in this file mean anything.

    `verify_against_published()` returns [] when the wheel is absent. Until 2026-08-01
    the wheel was never a declared dependency, so it returned [] on every CI run and
    this suite reported a green contract-compat gate while the mirror disagreed with
    repo 1 on all eleven envelope field names and on every one of the thirteen tables.

    A gate that cannot fail is not a gate. `edgar-lakehouse-contracts` is now a dev
    dependency, and this assertion is what stops the escape hatch from silently
    reopening if someone removes it.
    """
    assert contracts.provenance() == "published", (
        "edgar-lakehouse-contracts is not installed, so the contract gate is comparing "
        "the mirror against nothing. Install the dev extra."
    )


def test_mirror_matches_the_published_contracts() -> None:
    discrepancies = contracts.verify_against_published()
    assert not discrepancies, "contracts drift:\n" + "\n".join(str(d) for d in discrepancies)


def test_the_gate_can_actually_detect_drift() -> None:
    """Prove the comparison has teeth by feeding it a deliberately wrong spec.

    Without this, `verify_against_published() == []` is equally consistent with "aligned"
    and with "comparing nothing" -- which is precisely the failure being guarded against.
    """
    from dataclasses import replace

    spec = contracts.schemas.SILVER_COMPANY
    tampered = replace(spec, columns=(*spec.columns, contracts.ColumnSpec("not_a_real", "STRING")))
    original = contracts.schemas.TABLES[spec.fqn]
    contracts.schemas.TABLES[spec.fqn] = tampered
    try:
        found = contracts.verify_against_published()
    finally:
        contracts.schemas.TABLES[spec.fqn] = original
    assert any("not_a_real" in str(d) for d in found), (
        "the gate did not notice a column that repo 1 does not publish"
    )


def test_mirrored_ssm_keys_match_the_published_ones() -> None:
    """SSM key names are a cross-repo interface, so a local copy must not drift.

    This repo read `/edgar-lakehouse/dbx/landing_volume` while repo 2 published
    `dbx/volume_path` and repo 3 consumed that. Nothing failed loudly, because the
    lookup falls back to a default on a miss -- the repos just disagreed in silence.
    Contracts v1.2.0 ratified the names; this asserts the mirror still agrees.
    """
    import edgar_lakehouse_contracts.names as upstream

    from pipelines.contracts import names as mirror

    mirrored = [k for k in dir(mirror) if k.startswith("SSM_")]
    assert mirrored, "the mirror declares no SSM keys; did they move?"
    for key in mirrored:
        assert hasattr(upstream, key), f"{key} is mirrored here but not published by repo 1"
        assert getattr(mirror, key) == getattr(upstream, key), (
            f"{key}: mirror={getattr(mirror, key)!r} upstream={getattr(upstream, key)!r}"
        )


def test_every_mirrored_ssm_key_is_actually_published_by_repo_2() -> None:
    """A key repo 1 defines but repo 2 never creates is a ParameterNotFound waiting to
    happen. Repo 1 declares the published set, so check membership rather than guessing."""
    import edgar_lakehouse_contracts.names as upstream

    from pipelines.contracts import names as mirror

    for key in (k for k in dir(mirror) if k.startswith("SSM_") and k != "SSM_PREFIX"):
        value = getattr(mirror, key)
        assert value in upstream.SSM_PUBLISHED, (
            f"{key}={value!r} is read by this repo but is not in repo 1's SSM_PUBLISHED set"
        )


def test_the_envelope_is_compared_in_both_directions() -> None:
    """A field on only one side is drift either way -- the envelope is a wire format
    shared with repo 3, not a table repo 1 may extend for other consumers."""
    published = contracts.envelope.ENVELOPE_FIELDS
    import edgar_lakehouse_contracts.envelope as upstream

    assert set(published) == set(upstream.ENVELOPE_FIELDS)


def test_every_table_this_repo_writes_is_in_the_contract() -> None:
    from pipelines.entrypoints import bronze_ingest, gold_build, silver_transform

    for entrypoint in (bronze_ingest, silver_transform, gold_build):
        for fqn in entrypoint.WRITE_TARGETS:
            assert fqn in contracts.schemas.TABLES, fqn
