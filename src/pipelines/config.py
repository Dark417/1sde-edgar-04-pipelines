"""L0 -- runtime settings.

Resolution order is fixed by AGENTS.global.md rule 3: **env var, then SSM, then fail
with a message naming the missing key.** There is no fourth step and no default for
anything environment-specific. A default bucket name is how a dev run writes into the
production prefix.

Nothing here imports PySpark, so config is unit-testable without a JVM.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from typing import Any, Final, Literal

from pipelines.contracts import names

__all__ = [
    "MissingConfigError",
    "Settings",
    "batch_id_for",
    "resolve",
    "run_id_for",
]

IngestMode = Literal["autoloader", "batch"]
StorageMode = Literal["s3", "volume", "local"]

_ENV_PREFIX: Final[str] = "EDGAR_"
_SSM_PREFIX: Final[str] = "/edgar-lakehouse"
_LOGICAL_DATE_RE: Final[re.Pattern[str]] = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class MissingConfigError(RuntimeError):
    """A required setting was found in neither the environment nor SSM."""

    def __init__(self, env_var: str, ssm_key: str | None) -> None:
        where = f"env {env_var}" + (f" or SSM {ssm_key}" if ssm_key else "")
        super().__init__(
            f"missing required configuration: set {where}. "
            "Repo 2 publishes the SSM parameters; see its section 10."
        )
        self.env_var = env_var
        self.ssm_key = ssm_key


@lru_cache(maxsize=1)
def _ssm_client() -> Any:
    import boto3

    # us-east-2 matches the Unity Catalog metastore (AGENTS.global.md law 11).
    # SSM parameters are regional, so the wrong default here surfaces as
    # ParameterNotFound on a parameter that plainly exists.
    return boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "us-east-2"))


def _from_ssm(key: str) -> str | None:
    """Read an SSM parameter, returning ``None`` when it is absent or unreachable.

    Unreachable is deliberately not fatal *here*: the caller raises
    :class:`MissingConfigError` naming the key, which is a far more actionable message
    than a botocore stack trace three frames deep.
    """
    try:
        client = _ssm_client()
        resp = client.get_parameter(Name=key, WithDecryption=True)
        value = resp["Parameter"]["Value"]
        return str(value)
    except Exception:
        # Any failure means "not available from SSM". The caller raises
        # MissingConfigError naming the key, which beats a botocore stack trace three
        # frames deep.
        return None


def _setting(
    name: str,
    env: Mapping[str, str],
    *,
    ssm_key: str | None,
    default: str | None = None,
) -> str:
    env_var = f"{_ENV_PREFIX}{name.upper()}"
    value = env.get(env_var)
    if value:
        return value
    if ssm_key:
        from_ssm = _from_ssm(ssm_key)
        if from_ssm:
            return from_ssm
    if default is not None:
        return default
    raise MissingConfigError(env_var, ssm_key)


@dataclass(frozen=True, slots=True)
class Settings:
    """Everything a job task needs to know about where it is running."""

    catalog: str
    logical_date: str
    run_id: str
    ingest_mode: IngestMode
    storage_mode: StorageMode
    landing_root: str
    checkpoint_root: str
    export_root: str
    environment: str

    def table(self, fqn: str) -> str:
        """Rebind a contract FQN onto the configured catalog.

        Local runs use a throwaway catalog; the contract names the ``edgar`` catalog.
        Rebinding here keeps every call site writing ``schemas.SILVER_FILING.fqn``.
        """
        parts = fqn.split(".")
        if len(parts) != 3:
            raise ValueError(f"expected a three-part name, got {fqn!r}")
        return f"{self.catalog}.{parts[1]}.{parts[2]}"

    @property
    def logical_date_obj(self) -> date:
        return date.fromisoformat(self.logical_date)


def run_id_for(job: str, logical_date: str) -> str:
    """Deterministic run id.

    Derived from the job name and the logical date, never from the wall clock
    (AGENTS.global.md rule 5): replaying 2026-07-31 must produce the same run id it
    produced the first time, or the "run it twice" tests compare apples to oranges.
    """
    digest = hashlib.sha256(f"{job}:{logical_date}".encode()).hexdigest()
    return f"{job}-{logical_date}-{digest[:12]}"


def batch_id_for(stream: str, logical_date: str) -> str:
    """Deterministic ingest batch id for a stream and logical date."""
    return f"{stream}-{logical_date}"


def _validate_logical_date(value: str) -> str:
    if not _LOGICAL_DATE_RE.match(value):
        raise ValueError(f"EDGAR_LOGICAL_DATE must be YYYY-MM-DD, got {value!r}")
    date.fromisoformat(value)  # raises on 2026-02-30 and friends
    return value


def resolve(job: str, *, overrides: dict[str, str] | None = None) -> Settings:
    """Build :class:`Settings` for a job task.

    ``overrides`` carries Databricks job widget values, which take precedence over the
    environment. Entrypoints pass them; nothing else should.
    """
    env: dict[str, str] = dict(os.environ)
    for key, value in (overrides or {}).items():
        if value:
            env[f"{_ENV_PREFIX}{key.upper()}"] = value

    logical_date = _validate_logical_date(_setting("logical_date", env, ssm_key=None))
    catalog = _setting("catalog", env, ssm_key=None, default=names.CATALOG)
    environment = _setting("environment", env, ssm_key=None, default="dev")

    ingest_mode = _setting("ingest_mode", env, ssm_key=None, default="autoloader")
    if ingest_mode not in ("autoloader", "batch"):
        raise ValueError(f"EDGAR_INGEST_MODE must be autoloader|batch, got {ingest_mode!r}")
    storage_mode = _setting("storage_mode", env, ssm_key=None, default="volume")
    if storage_mode not in ("s3", "volume", "local"):
        raise ValueError(f"EDGAR_STORAGE_MODE must be s3|volume|local, got {storage_mode!r}")

    # `dbx/volume_path`, not `dbx/landing_volume`. This repo invented the second name for
    # the same value and read a key nobody publishes; repo 2 publishes the first and
    # repo 3 already consumes it, so this repo was the outlier. It never failed loudly
    # because the lookup falls back to a default on a miss -- the two repos just
    # disagreed in silence until someone diffed them.
    landing_root = _setting(
        "landing_root",
        env,
        ssm_key=f"{_SSM_PREFIX}/dbx/volume_path",
        default=names.LANDING_VOLUME if storage_mode == "volume" else None,
    )
    # No SSM lookup: where this pipeline keeps its Auto Loader checkpoints is an
    # implementation detail, not a cross-repo contract. Publishing it would invite
    # another repo to depend on a path this one should stay free to move.
    checkpoint_root = _setting(
        "checkpoint_root",
        env,
        ssm_key=None,
        default=f"{landing_root.rstrip('/')}/_checkpoints",
    )
    export_root = _setting("export_root", env, ssm_key=f"{_SSM_PREFIX}/s3/serving_bucket")
    if not export_root.startswith(("s3://", "s3a://", "/", "file:")):
        # SSM stores the bare bucket name; the pipeline wants a URI.
        export_root = f"s3://{export_root}"

    return Settings(
        catalog=catalog,
        logical_date=logical_date,
        run_id=run_id_for(job, logical_date),
        ingest_mode=ingest_mode,  # type: ignore[arg-type]
        storage_mode=storage_mode,  # type: ignore[arg-type]
        landing_root=landing_root,
        checkpoint_root=checkpoint_root,
        export_root=export_root.rstrip("/"),
        environment=environment,
    )
