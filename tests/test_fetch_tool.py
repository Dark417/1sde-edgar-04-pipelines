"""The local landing fetcher (``tools/fetch_test_data.py``). No Spark, no network.

It stands in for repo 3, so its output shape is the thing bronze is written against --
a parser bug here produces an empty landing directory and a confusing green test run.
"""

from __future__ import annotations

import json

from tools.fetch_test_data import (
    Envelope,
    parse_daily_index,
    select_index_rows,
    trim_submissions,
)

# A verbatim excerpt of a real form.YYYYMMDD.idx, header wrap included.
IDX = """Description:           Daily Index of EDGAR Dissemination Feed by Form Type
Last Data Received:    Jul 31, 2026
Comments:              webmaster@sec.gov
Anonymous FTP:         ftp://ftp.sec.gov/edgar/



Form Type   Company Name                                                  CIK
      Date Filed  File Name
---------------------------------------------------------------------------------------------
1-A              FJHL Inc.                                                     2135411     20260731    edgar/data/2135411/0002135411-26-000005.txt
10-K/A           MEXCO ENERGY CORP                                             66418       20260730    edgar/data/66418/0001493152-26-035304.txt
10-K             ANOTHER FILER INC                                             999999      20260731    edgar/data/999999/0000999999-26-000009.txt
8-K              THIRD FILER LLC                                               888888      20260731    edgar/data/888888/0000888888-26-000008.txt
SC 13D           Some Holder  With  Spaces LLC                                 1234567     20260731    edgar/data/1234567/0001234567-26-000001.txt
"""


def test_header_wrap_does_not_break_the_parse() -> None:
    """The header spans two lines, so a column-offset parse finds no "Date Filed" and
    silently yields zero rows -- which looks like "no filings that day"."""
    rows = parse_daily_index(IDX)
    assert len(rows) == 5


def test_fields_are_parsed_from_the_right() -> None:
    rows = {r["form_type"]: r for r in parse_daily_index(IDX)}
    assert rows["10-K/A"]["cik"] == "66418"
    assert rows["10-K/A"]["date_filed"] == "20260730"
    assert rows["10-K/A"]["accession_number"] == "0001493152-26-035304"
    assert rows["10-K/A"]["company_name"] == "MEXCO ENERGY CORP"


def test_form_types_with_a_single_space_stay_intact() -> None:
    """"SC 13D" is one form type, not a form type and a company name."""
    forms = {r["form_type"] for r in parse_daily_index(IDX)}
    assert "SC 13D" in forms


def test_company_names_with_double_spaces_survive() -> None:
    row = next(r for r in parse_daily_index(IDX) if r["form_type"] == "SC 13D")
    assert row["company_name"] == "Some Holder  With  Spaces LLC"
    assert row["cik"] == "1234567"


def test_selection_is_deterministic_and_capped() -> None:
    """A fixture that changes shape between runs is not a fixture."""
    rows = parse_daily_index(IDX)
    first = select_index_rows(rows, 2, set())
    second = select_index_rows(rows, 2, set())
    assert first == second
    assert len(first) == 2


def test_selection_keeps_targeted_ciks_even_when_capped() -> None:
    """A cap that drops the companies the fixture exists for is a useless cap."""
    rows = parse_daily_index(IDX)
    picked = select_index_rows(rows, 1, {"0001234567"})
    assert picked[0]["cik"] == "1234567"


def test_selection_prefers_amendments() -> None:
    """10-K/A rows are what make the restatement feature testable against real data."""
    picked = select_index_rows(parse_daily_index(IDX), 1, set())
    assert picked[0]["form_type"] == "10-K/A"


def test_envelope_shape_matches_the_contract() -> None:
    record = json.loads(
        Envelope(
            stream="filing_index",
            resource_id="r",
            logical_date="2026-07-31",
            request_url="https://example.invalid",
            http_status=200,
            payload={"b": 2, "a": 1},
            fetched_at="2026-08-01T00:00:00Z",
        ).to_json_line()
    )
    assert record["envelope_version"] == "1"
    assert record["source_system"] == "sec_edgar"
    assert record["batch_id"] == "filing_index-2026-07-31"
    # payload is a STRING, not a nested object.
    assert isinstance(record["payload_json"], str)
    # Keys sorted, so the same payload always hashes the same.
    assert record["payload_json"] == '{"a":1,"b":2}'
    assert len(record["content_sha256"]) == 64


def test_trim_submissions_keeps_the_document_shape() -> None:
    """Only the row count shrinks. Using real data is pointless if the shape is faked."""
    payload = {
        "cik": "1",
        "filings": {"recent": {"form": ["10-K"] * 100, "accessionNumber": ["x"] * 100}, "files": []},
    }
    trimmed = trim_submissions(payload, 5)
    assert set(trimmed["filings"]["recent"]) == {"form", "accessionNumber"}
    assert len(trimmed["filings"]["recent"]["form"]) == 5


def test_trim_submissions_tolerates_a_missing_filings_block() -> None:
    assert trim_submissions({"cik": "1"}, 5) == {"cik": "1"}
