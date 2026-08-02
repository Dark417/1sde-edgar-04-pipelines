"""Configuration resolution. No Spark."""

from __future__ import annotations

import pytest

from pipelines import config


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(config.os.environ):
        if key.startswith("EDGAR_"):
            monkeypatch.delenv(key, raising=False)


def _base_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDGAR_LOGICAL_DATE", "2026-07-31")
    monkeypatch.setenv("EDGAR_EXPORT_ROOT", "s3://serving-bucket")
    monkeypatch.setenv("EDGAR_STORAGE_MODE", "local")
    monkeypatch.setenv("EDGAR_LANDING_ROOT", "/tmp/landing")


def test_resolve_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    settings = config.resolve("silver_transform")
    assert settings.logical_date == "2026-07-31"
    assert settings.catalog == "edgar"
    assert settings.export_root == "s3://serving-bucket"
    assert settings.checkpoint_root == "/tmp/landing/_checkpoints"


def test_missing_required_setting_names_the_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """A message that names the env var and the SSM key is the whole point."""
    monkeypatch.setenv("EDGAR_LOGICAL_DATE", "2026-07-31")
    monkeypatch.setenv("EDGAR_STORAGE_MODE", "local")
    monkeypatch.setenv("EDGAR_LANDING_ROOT", "/tmp/landing")
    monkeypatch.setattr(config, "_from_ssm", lambda key: None)
    with pytest.raises(config.MissingConfigError) as exc:
        config.resolve("serving_export")
    assert "EDGAR_EXPORT_ROOT" in str(exc.value)
    assert "/edgar-lakehouse/s3/serving_bucket" in str(exc.value)


def test_ssm_is_consulted_after_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDGAR_LOGICAL_DATE", "2026-07-31")
    monkeypatch.setenv("EDGAR_STORAGE_MODE", "local")
    monkeypatch.setenv("EDGAR_LANDING_ROOT", "/tmp/landing")
    monkeypatch.setattr(
        config, "_from_ssm", lambda key: "from-ssm-bucket" if "serving_bucket" in key else None
    )
    settings = config.resolve("serving_export")
    # A bare bucket name from SSM is promoted to a URI.
    assert settings.export_root == "s3://from-ssm-bucket"


def test_overrides_beat_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    settings = config.resolve("gold_build", overrides={"logical_date": "2026-01-15"})
    assert settings.logical_date == "2026-01-15"


def test_resolve_does_not_mutate_the_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    config.resolve("gold_build", overrides={"catalog": "scratch"})
    assert "EDGAR_CATALOG" not in config.os.environ


@pytest.mark.parametrize("bad", ["2026-7-31", "31-07-2026", "2026-02-30", "yesterday"])
def test_logical_date_must_be_a_real_iso_date(monkeypatch: pytest.MonkeyPatch, bad: str) -> None:
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGAR_LOGICAL_DATE", bad)
    with pytest.raises(ValueError):
        config.resolve("gold_build")


@pytest.mark.parametrize(
    ("var", "value"),
    [("EDGAR_INGEST_MODE", "streaming"), ("EDGAR_STORAGE_MODE", "gcs")],
)
def test_enum_settings_are_validated(monkeypatch: pytest.MonkeyPatch, var: str, value: str) -> None:
    _base_env(monkeypatch)
    monkeypatch.setenv(var, value)
    with pytest.raises(ValueError):
        config.resolve("gold_build")


def test_run_id_is_deterministic() -> None:
    """AGENTS.global.md rule 5: derived from the logical date, never the wall clock."""
    first = config.run_id_for("silver_transform", "2026-07-31")
    second = config.run_id_for("silver_transform", "2026-07-31")
    assert first == second
    assert first != config.run_id_for("silver_transform", "2026-08-01")
    assert first != config.run_id_for("gold_build", "2026-07-31")


def test_batch_id_is_deterministic() -> None:
    assert config.batch_id_for("filing_index", "2026-07-31") == "filing_index-2026-07-31"


def test_settings_table_rebinds_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    monkeypatch.setenv("EDGAR_CATALOG", "scratch")
    settings = config.resolve("gold_build")
    assert settings.table("edgar.silver.filing") == "scratch.silver.filing"
    with pytest.raises(ValueError):
        settings.table("silver.filing")


def test_logical_date_obj(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    assert config.resolve("gold_build").logical_date_obj.year == 2026
