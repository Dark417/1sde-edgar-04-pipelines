"""Object names: catalog, schemas, table names, landing paths, streams.

No hardcoded bucket names or hosts live here -- only the *shape* of names. Anything
environment-specific (bucket, workspace host, volume root) is resolved at runtime by
:mod:`pipelines.config` from an env var, then SSM, then a hard failure naming the key.
"""

from __future__ import annotations

from typing import Final

from .models import Stream

__all__ = [
    "BRONZE_SCHEMA",
    "CATALOG",
    "GOLD_SCHEMA",
    "LANDING_SCHEMA",
    "SILVER_SCHEMA",
    "STREAMS",
    "landing_path",
    "stream",
]

CATALOG: Final[str] = "edgar"
LANDING_SCHEMA: Final[str] = "landing"
BRONZE_SCHEMA: Final[str] = "bronze"
SILVER_SCHEMA: Final[str] = "silver"
GOLD_SCHEMA: Final[str] = "gold"

#: Volume that repo 2 creates and repo 3 writes into.
LANDING_VOLUME: Final[str] = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/edgar"

STREAMS: Final[dict[str, Stream]] = {
    "filing_index": Stream(
        name="filing_index",
        landing_prefix="filing_index",
        bronze_table="filing_index_raw",
        payload_mode="passthrough",
        resource_grain="one record per (form_type, cik, accession_number) in a daily index",
        description="EDGAR daily-index form.YYYYMMDD.idx rows.",
        passthrough_columns=(
            "form_type",
            "company_name",
            "cik",
            "date_filed",
            "accession_number",
            "file_name",
        ),
    ),
    "company_submissions": Stream(
        name="company_submissions",
        landing_prefix="company_submissions",
        bronze_table="company_submissions_raw",
        payload_mode="opaque_json",
        resource_grain="one record per cik per logical_date",
        description="data.sec.gov/submissions/CIK##########.json",
    ),
    "company_concept": Stream(
        name="company_concept",
        landing_prefix="company_concept",
        bronze_table="company_concept_raw",
        payload_mode="opaque_json",
        resource_grain="one record per (cik, taxonomy, tag) per logical_date",
        description="data.sec.gov/api/xbrl/companyconcept/CIK##########/{taxonomy}/{tag}.json",
    ),
}


def stream(name: str) -> Stream:
    try:
        return STREAMS[name]
    except KeyError:
        known = ", ".join(sorted(STREAMS))
        raise KeyError(f"unknown landing stream {name!r}; known streams: {known}") from None


def landing_path(root: str, stream_name: str, logical_date: str | None = None) -> str:
    """Landing path for a stream under ``root``.

    ``root`` is a volume path, an ``s3://`` URI or a local directory -- the layout is
    identical in all three so that the same code reads a laptop and a workspace.
    """
    base = f"{root.rstrip('/')}/{stream(stream_name).landing_prefix}"
    return base if logical_date is None else f"{base}/logical_date={logical_date}"


def checkpoint_path(checkpoint_root: str, stream_name: str) -> str:
    """Per-stream checkpoint location.

    Per-stream, never shared: Auto Loader stores the inferred schema alongside the
    file-processing state, and two streams sharing a location silently merge their
    schemas into one.
    """
    return f"{checkpoint_root.rstrip('/')}/{stream(stream_name).landing_prefix}"
