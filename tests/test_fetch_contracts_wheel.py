"""``tools/fetch_contracts_wheel`` must catch the two states that ship a wrong contract.

The bundle globs ``dist/contracts/*.whl`` and ``dist/`` is gitignored, so the directory's
contents are whatever the last session left behind. Both bad states are silent at deploy
time -- ``bundle deploy`` prints the filename it uploads and nothing compares it to the
pin -- so they are checked here instead.
"""

from __future__ import annotations

import pytest
from tools import fetch_contracts_wheel as fcw


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A throwaway repo root with a pyproject pin and an empty dist/contracts."""
    (tmp_path / "pyproject.toml").write_text(
        'dependencies = [\n  "edgar-lakehouse-contracts==1.4.1",\n  "pyspark",\n]\n',
        encoding="utf-8",
    )
    (tmp_path / "dist" / "contracts").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _wheel(repo, version: str):
    path = repo / "dist" / "contracts" / f"edgar_lakehouse_contracts-{version}-py3-none-any.whl"
    path.write_bytes(b"not really a wheel")
    return path


def test_reads_the_pin_rather_than_hardcoding_a_version(repo):
    assert fcw.pinned_version() == "1.4.1"


def test_missing_pin_fails_loudly(repo):
    (repo / "pyproject.toml").write_text('dependencies = ["pyspark"]\n', encoding="utf-8")
    with pytest.raises(SystemExit, match="no 'edgar-lakehouse-contracts=="):
        fcw.pinned_version()


def test_check_fails_on_a_fresh_clone(repo):
    """dist/ is gitignored: on a clean checkout the glob matches nothing at all."""
    assert fcw.main(["--check"]) == 1


def test_check_fails_when_only_a_stale_version_is_present(repo):
    """The 1.4.0-shipped-while-1.4.1-was-pinned case, which deployed silently."""
    _wheel(repo, "1.4.0")
    assert fcw.main(["--check"]) == 1


def test_check_fails_when_the_pinned_wheel_sits_beside_a_stale_one(repo):
    """Two wheels means pip chooses, so a correct wheel present is not sufficient."""
    _wheel(repo, "1.4.1")
    _wheel(repo, "1.4.0")
    assert fcw.main(["--check"]) == 1
    assert fcw.stale("1.4.1")


def test_check_passes_on_exactly_the_pinned_wheel(repo):
    _wheel(repo, "1.4.1")
    assert fcw.main(["--check"]) == 0


def test_fetch_removes_stale_wheels_before_downloading(repo, monkeypatch):
    _wheel(repo, "1.4.0")
    _wheel(repo, "1.4.1")
    called: list[list[str]] = []
    monkeypatch.setattr(fcw.subprocess, "run", lambda cmd, **kw: called.append(cmd))
    fcw.fetch("1.4.1")
    assert not fcw.stale("1.4.1"), "the 1.4.0 wheel must be gone"
    assert fcw.present("1.4.1")
    assert not called, "the pinned wheel was already there; no download was needed"


def test_fetch_reports_an_actionable_error_when_the_release_is_missing(repo, monkeypatch):
    class Failed:
        returncode = 1
        stderr = "release not found"

    monkeypatch.setattr(fcw.subprocess, "run", lambda cmd, **kw: Failed())
    with pytest.raises(SystemExit, match="tag repo 1 rather than loosening the pin"):
        fcw.fetch("1.4.1")
