"""Feature F-1: the table-existence gate."""

from __future__ import annotations

from typing import Any

import pytest
from edgar_lakehouse_contracts import schemas

from pipelines.framework.preflight import MissingTableError, assert_tables_exist, table_exists

pytestmark = pytest.mark.spark


def test_existing_tables_pass(spark: Any, tables: str) -> None:
    assert_tables_exist(spark, [f"{tables}.silver.filing", f"{tables}.gold.restatement_event"])


def test_missing_table_names_the_table_and_the_changeset(spark: Any, tables: str) -> None:
    """The whole point of the gate: a one-line diagnosis instead of a mid-job
    AnalysisException pointing at whichever query touched the table first."""
    spark.sql(f"DROP TABLE {tables}.silver.filing")
    with pytest.raises(MissingTableError) as exc:
        assert_tables_exist(spark, [f"{tables}.silver.filing"])
    message = str(exc.value)
    assert f"{tables}.silver.filing" in message
    assert schemas.SILVER_FILING.changeset in message
    assert "does not create tables" in message


def test_error_lists_every_missing_table(spark: Any, tables: str) -> None:
    spark.sql(f"DROP TABLE {tables}.silver.filing")
    spark.sql(f"DROP TABLE {tables}.gold.company_profile")
    with pytest.raises(MissingTableError) as exc:
        assert_tables_exist(spark, [f"{tables}.silver.filing", f"{tables}.gold.company_profile"])
    assert len(exc.value.missing) == 2
    assert "020-silver.yaml" in str(exc.value)
    assert "030-gold.yaml" in str(exc.value)


def test_table_exists_is_false_for_unknown_schema(spark: Any, tables: str) -> None:
    assert table_exists(spark, f"{tables}.silver.filing") is True
    assert table_exists(spark, f"{tables}.nope.nothing") is False


def test_table_exists_rejects_a_two_part_name(spark: Any) -> None:
    with pytest.raises(ValueError, match=r"catalog\.schema\.table"):
        table_exists(spark, "silver.filing")
