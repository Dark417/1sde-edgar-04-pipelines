"""Features F-2 and F-5: landing reads and bronze ingest.

The acceptance criterion for both is the same sentence: **the same file processed twice
adds zero rows.**
"""

from __future__ import annotations

from typing import Any

import pytest

from pipelines.bronze import company_concept, company_submissions, filing_index
from pipelines.config import Settings
from pipelines.framework import autoloader

from .conftest import concept_payload, envelope, fact, index_payload, submissions_payload

pytestmark = pytest.mark.spark


def test_reprocessing_a_landing_file_adds_zero_rows(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    landing.write(
        "filing_index",
        [
            envelope(
                "filing_index", "0000000001-26-000001", index_payload("0000000001-26-000001", "1")
            ),
            envelope(
                "filing_index", "0000000002-26-000002", index_payload("0000000002-26-000002", "2")
            ),
        ],
    )
    target = settings.table("edgar.bronze.filing_index_raw")

    first = filing_index.ingest(spark, settings, job_run_ctx)
    assert first.rows_appended == 2
    assert spark.table(target).count() == 2

    second = filing_index.ingest(spark, settings, job_run_ctx)
    assert second.rows_appended == 0
    assert second.files_read == 0
    assert spark.table(target).count() == 2


def test_a_new_landing_file_is_picked_up(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    landing.write(
        "filing_index",
        [envelope("filing_index", "a", index_payload("0000000001-26-000001", "1"))],
    )
    filing_index.ingest(spark, settings, job_run_ctx)
    landing.write(
        "filing_index",
        [envelope("filing_index", "b", index_payload("0000000002-26-000002", "2"))],
        part="part-00001",
    )
    stats = filing_index.ingest(spark, settings, job_run_ctx)
    assert stats.rows_appended == 1
    assert spark.table(settings.table("edgar.bronze.filing_index_raw")).count() == 2


def test_bronze_keeps_values_raw(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """Typing happens in silver so a value that fails to parse can be quarantined with
    its original bytes visible, rather than becoming an unexplainable null."""
    landing.write(
        "filing_index",
        [
            envelope(
                "filing_index", "a", index_payload("0000000001-26-000001", "1234", form_type="10-k")
            )
        ],
    )
    filing_index.ingest(spark, settings, job_run_ctx)
    row = spark.table(settings.table("edgar.bronze.filing_index_raw")).collect()[0]
    assert row["cik"] == "1234"  # not padded
    assert row["form_type"] == "10-k"  # not upper-cased
    assert row["date_filed"] == "20260731"  # still text


def test_six_metadata_columns_are_populated(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    landing.write(
        "filing_index",
        [envelope("filing_index", "a", index_payload("0000000001-26-000001", "1"))],
    )
    filing_index.ingest(spark, settings, job_run_ctx)
    row = spark.table(settings.table("edgar.bronze.filing_index_raw")).collect()[0]
    assert row["_ingest_batch_id"] == "filing_index-2026-07-31"
    assert row["_source_system"] == "sec_edgar"
    assert row["_envelope_version"] == "1"
    assert row["_ingest_ts"] is not None
    assert row["_source_file"].endswith("part-00000.json")
    assert row["_rescued_data"] is None


def test_unknown_landing_field_is_rescued_and_warns(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """Rule 11: a non-null _rescued_data is the only signal the source changed shape."""
    landing.write(
        "filing_index",
        [
            envelope(
                "filing_index",
                "a",
                index_payload("0000000001-26-000001", "1"),
                extra={"brand_new_field": "surprise"},
            )
        ],
    )
    stats = filing_index.ingest(spark, settings, job_run_ctx)
    assert stats.rescued_rows == 1
    assert job_run_ctx.status == "WARN"
    assert any("_rescued_data" in w for w in job_run_ctx.warnings)
    row = spark.table(settings.table("edgar.bronze.filing_index_raw")).collect()[0]
    assert "surprise" in row["_rescued_data"]


def test_unknown_envelope_version_fails_loudly(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    landing.write(
        "filing_index",
        [
            envelope(
                "filing_index",
                "a",
                index_payload("0000000001-26-000001", "1"),
                extra={"envelope_version": "2"},
            )
        ],
    )
    with pytest.raises(ValueError, match="envelope_version"):
        filing_index.ingest(spark, settings, job_run_ctx)


def test_ledger_is_not_committed_when_the_write_fails(
    spark: Any, settings: Settings, landing: Any
) -> None:
    """A crash mid-write must replay the file, not lose it."""
    landing.write(
        "filing_index",
        [envelope("filing_index", "a", index_payload("0000000001-26-000001", "1"))],
    )
    batch = autoloader.read_landing_batch(
        spark, "filing_index", settings.landing_root, settings.checkpoint_root
    )
    assert len(batch.files) == 1
    # No commit() -- simulating a failed write.
    again = autoloader.read_landing_batch(
        spark, "filing_index", settings.landing_root, settings.checkpoint_root
    )
    assert len(again.files) == 1


def test_empty_landing_yields_an_empty_batch_with_the_envelope_schema(
    spark: Any, settings: Settings
) -> None:
    batch = autoloader.read_landing_batch(
        spark, "filing_index", settings.landing_root, settings.checkpoint_root
    )
    assert batch.is_empty
    assert "payload_json" in batch.df.columns
    assert "_rescued_data" in batch.df.columns


def test_a_short_resource_id_yields_nulls_rather_than_crashing(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """company_concept splits resource_id into cik/taxonomy/tag. A record with fewer
    parts must produce nulls for silver's DQ to quarantine -- not an
    ArrayIndexOutOfBounds that takes down the whole bronze batch. Bronze is not the
    layer that decides a record is bad."""
    landing.write(
        "company_concept",
        [envelope("company_concept", "malformed/resource-id", {"units": {}})],
    )
    stats = company_concept.ingest(spark, settings, job_run_ctx)
    assert stats.rows_appended == 1
    row = spark.table(settings.table("edgar.bronze.company_concept_raw")).collect()[0]
    assert row["cik"] == "malformed"
    assert row["taxonomy"] == "resource-id"
    assert row["tag"] is None


def test_submissions_and_concept_payloads_stay_opaque(
    spark: Any, settings: Settings, landing: Any, job_run_ctx: Any
) -> None:
    """Exploding a deeply nested document at bronze couples a *table* to a shape
    nobody in this project controls."""
    landing.write(
        "company_submissions",
        [envelope("company_submissions", "0000000001", submissions_payload("0000000001"))],
    )
    landing.write(
        "company_concept",
        [
            envelope(
                "company_concept",
                "0000000001/us-gaap/Revenues",
                concept_payload(
                    "1",
                    "Revenues",
                    {
                        "USD": [
                            fact(
                                start="2025-01-01",
                                end="2025-12-31",
                                val=1.0,
                                accn="0000000001-26-000001",
                                filed="2026-03-01",
                            )
                        ]
                    },
                ),
            )
        ],
    )
    company_submissions.ingest(spark, settings, job_run_ctx)
    company_concept.ingest(spark, settings, job_run_ctx)

    sub = spark.table(settings.table("edgar.bronze.company_submissions_raw")).collect()[0]
    assert sub["payload_json"].startswith("{")
    assert sub["cik"] == "0000000001"

    con = spark.table(settings.table("edgar.bronze.company_concept_raw")).collect()[0]
    assert con["cik"] == "0000000001"
    assert con["taxonomy"] == "us-gaap"
    assert con["tag"] == "Revenues"
    assert '"units"' in con["payload_json"]
