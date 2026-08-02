"""L2 -- deterministic surrogate keys (feature F-7).

A surrogate key here is a **function of the natural key**, never a generated identity.

Identity columns look convenient and are wrong for this pipeline. Delta assigns them at
write time, so a re-run assigns different values to the same logical rows. That breaks
the idempotency guarantee ("run it twice -> identical state", AGENTS.global.md rule 8)
and silently invalidates any key a consumer cached. Hashing the natural key gives the
same answer on every run, in every environment, forever -- which is the property that
makes the key safe to publish.

Two rules make the hash trustworthy:

* **An explicit delimiter.** ``concat`` alone would let ``('ab', 'c')`` and
  ``('a', 'bc')`` collide. ``concat_ws('|', ...)`` cannot, because the delimiter is not
  a legal character in any of the parts we hash (CIKs are digits, accessions are
  digits and dashes, concepts are identifiers, dates are ISO).
* **Explicit null handling.** ``concat_ws`` skips nulls, so ``(null, 'x')`` and
  ``('x', null)`` would produce the same string. Nulls are mapped to a sentinel first,
  so a missing part is distinguishable from a present one.

Does not handle: Spark session management, or writing anything.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps this module JVM-free
    from pyspark.sql import Column

__all__ = ["KEY_DELIMITER", "NULL_SENTINEL", "surrogate_key"]

#: Not a legal character in any natural-key part this project hashes.
KEY_DELIMITER = "|"

#: Distinguishes "this part was null" from "this part was absent". Chosen to be
#: impossible as a real value: no CIK, accession, concept or ISO date looks like this.
NULL_SENTINEL = "\x00null\x00"


def surrogate_key(*parts: Column | str) -> Any:
    """Return a ``Column`` holding the sha2-256 of the delimited natural key.

    ``parts`` may be columns or literal strings; strings are treated as column names so
    that call sites read like the key they describe::

        surrogate_key("cik")                      # company_sk
        surrogate_key("accession_number")         # filing_sk
        surrogate_key("cik", "concept_canonical", "period_end", "unit")   # fact_sk

    The result is deterministic across runs, sessions and clusters. It is *not* a
    security primitive -- sha2 is used here for a stable 64-hex identity, and the inputs
    are public EDGAR identifiers.

    Does not handle: asserting the parts are non-empty. An all-null key hashes to a
    stable value, which is the honest answer; rejecting it is the caller's DQ rule.
    """
    from pyspark.sql import functions as F

    if not parts:
        raise ValueError("surrogate_key() needs at least one part; an empty key is not a key")

    columns = [F.col(p) if isinstance(p, str) else p for p in parts]
    # cast before coalesce: period_end is a DATE and would otherwise stringify per the
    # session's date format rather than ISO, making the key locale-dependent.
    safe = [F.coalesce(c.cast("string"), F.lit(NULL_SENTINEL)) for c in columns]
    return F.sha2(F.concat_ws(KEY_DELIMITER, *safe), 256)
