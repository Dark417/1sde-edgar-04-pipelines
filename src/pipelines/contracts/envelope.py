"""The landing envelope written by repo 3 and parsed by bronze.

Repo 3 never writes a bare API response to landing. Every record is wrapped so that
bronze can answer "where did this byte come from, when, and under which logical date"
without re-deriving it from a file path. Landing files are newline-delimited JSON: one
envelope per line.

Only ``envelope_version`` 1 exists. Bronze reads the field rather than assuming it, so
that a future version 2 fails loudly at parse time instead of silently mis-parsing.
"""

from __future__ import annotations

from typing import Final

__all__ = [
    "ENVELOPE_FIELDS",
    "ENVELOPE_VERSION",
    "SOURCE_SYSTEM",
    "envelope_json_schema_ddl",
]

ENVELOPE_VERSION: Final[str] = "1"
SOURCE_SYSTEM: Final[str] = "sec_edgar"

#: Envelope field -> Spark SQL type. ``payload`` stays a string at this level: the
#: three streams disagree about the payload's shape and two of them (submissions,
#: companyconcept) are deeply nested documents whose shape the SEC changes without
#: notice. Parsing them at bronze couples bronze to a shape we do not control.
ENVELOPE_FIELDS: Final[dict[str, str]] = {
    "envelope_version": "STRING",
    "source_system": "STRING",
    "stream": "STRING",
    "resource_id": "STRING",
    "logical_date": "STRING",
    "batch_id": "STRING",
    "fetched_at": "STRING",
    "request_url": "STRING",
    "http_status": "INT",
    "content_sha256": "STRING",
    "payload_json": "STRING",
}


def envelope_json_schema_ddl() -> str:
    """DDL string for ``from_json`` when reading landing as raw text.

    Used by the local batch reader. On Databricks, Auto Loader infers the envelope and
    routes anything unexpected into ``_rescued_data``; the DDL here is the same shape,
    so the two readers agree on column names and types.
    """
    return ", ".join(f"{name} {type_sql}" for name, type_sql in ENVELOPE_FIELDS.items())
