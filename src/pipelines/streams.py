"""L0 -- the landing streams this pipeline ingests.

Not part of the published contract, and deliberately so. ``landing_prefix`` is shared
with repo 3, but ``bronze_table``, ``payload_mode`` and ``passthrough_columns`` describe
how *this* repo chooses to land and project each stream -- they are pipeline decisions,
not facts about the data. Publishing them from repo 1 would invite another consumer to
depend on choices this repo should stay free to change.

The ``Stream`` type itself comes from the contracts wheel.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Literal

__all__ = ["STREAMS", "Stream", "stream"]


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
