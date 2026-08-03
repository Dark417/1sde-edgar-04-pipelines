#!/usr/bin/env python3
"""Download a small real subset of EDGAR into a local landing directory.

**This is a test harness, not a pipeline.** In the designed system repo 3 fetches
EDGAR into S3 (system of record) and mirrors it into a Unity Catalog Volume, and this
repo reads landing only -- it never touches the internet (AGENTS.md section 1). Repo 3
does not exist yet, so this script stands in for it: it writes byte-identical landing
objects (same envelope, same layout, same partitioning) into a local directory so the
medallion transforms can be developed and tested end to end on a laptop.

Nothing under ``src/pipelines`` imports this module. When repo 3 ships, delete it.

Layout produced, identical to the volume and S3 layouts::

    <out>/filing_index/logical_date=YYYY-MM-DD/part-00000.json
    <out>/company_submissions/logical_date=YYYY-MM-DD/part-00000.json
    <out>/company_concept/logical_date=YYYY-MM-DD/part-00000.json

Each file is newline-delimited JSON: one landing envelope per line.

Usage::

    export EDGAR_USER_AGENT="Your Name your@email"     # the SEC requires this
    python tools/fetch_test_data.py --out data/landing --index-dates 2026-07-30,2026-07-31
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

from edgar_lakehouse_contracts import concepts  # noqa: E402
from edgar_lakehouse_contracts.envelope import ENVELOPE_VERSION, SOURCE_SYSTEM  # noqa: E402

from pipelines import streams  # noqa: E402

# The SEC's fair-access policy is 10 requests/second with a declared User-Agent.
# Staying under it is not optional: EDGAR blocks by IP, and a blocked laptop cannot be
# unblocked from here.
_MIN_INTERVAL_SECONDS = 0.15

#: Default sample. Three of the four filed a 10-K/A in the last week of July 2026, so
#: the sample really does contain multiple assertions of the same period -- which is
#: what makes the restatement feature testable against real data instead of only
#: against fixtures.
DEFAULT_CIKS: tuple[str, ...] = (
    "0001825088",  # Dream Finders Homes -- 10-K/A filed 2026-07-30
    "0000066418",  # Mexco Energy -- 10-K/A filed 2026-07-30
    "0001673481",  # Sports Entertainment Gaming Global -- 10-K/A filed 2026-07-31
    "0000320193",  # Apple -- large, well-known, useful as a sanity anchor
)

#: Index rows are capped, and 10-K/A rows are kept preferentially, so the subset stays
#: small without becoming uninteresting.
INTERESTING_FORMS: tuple[str, ...] = ("10-K/A", "10-K", "10-Q/A", "10-Q", "8-K/A", "8-K", "S-1/A")


class Fetcher:
    """Rate-limited HTTP client for EDGAR."""

    def __init__(self, user_agent: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
        self._last_request = 0.0
        self.request_count = 0

    def get(self, url: str, *, allow_404: bool = False) -> tuple[int, bytes] | None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < _MIN_INTERVAL_SECONDS:
            time.sleep(_MIN_INTERVAL_SECONDS - elapsed)
        resp = self.session.get(url, timeout=60)
        self._last_request = time.monotonic()
        self.request_count += 1
        if resp.status_code == 404 and allow_404:
            return None
        resp.raise_for_status()
        return resp.status_code, resp.content


@dataclass(slots=True)
class Envelope:
    """The landing envelope repo 3 writes. Shape is fixed by the data contract."""

    stream: str
    resource_id: str
    logical_date: str
    request_url: str
    http_status: int
    payload: Any
    fetched_at: str

    def to_json_line(self) -> str:
        payload_json = json.dumps(self.payload, separators=(",", ":"), sort_keys=True)
        record = {
            "envelope_version": ENVELOPE_VERSION,
            "source_system": SOURCE_SYSTEM,
            "stream": self.stream,
            "resource_id": self.resource_id,
            "logical_date": self.logical_date,
            "batch_id": f"{self.stream}-{self.logical_date}",
            "fetched_at": self.fetched_at,
            "request_url": self.request_url,
            "http_status": self.http_status,
            "content_sha256": hashlib.sha256(payload_json.encode()).hexdigest(),
            "payload_json": payload_json,
        }
        return json.dumps(record, separators=(",", ":"), sort_keys=True)


def _quarter(d: date) -> int:
    return (d.month - 1) // 3 + 1


#: Daily-index rows, parsed from the right.
#:
#: Column *offsets* are not usable: the header wraps onto two lines ("... CIK" then
#: "      Date Filed  File Name"), so ``header.find("Date Filed")`` finds nothing and a
#: fixed-width parse silently yields zero rows. Anchoring on the tail is reliable
#: instead -- CIK is digits, the filing date is eight digits, and the archive path
#: contains no spaces. Form type keeps single spaces ("SC 13D"); company names keep
#: theirs too, because only runs of two-or-more spaces separate columns.
_INDEX_ROW_RE = re.compile(
    r"^(?P<form_type>\S.*?)\s{2,}(?P<company_name>\S.*?)\s{2,}(?P<cik>\d+)\s+(?P<date_filed>\d{8})\s+(?P<file_name>\S+)\s*$"
)


def parse_daily_index(text: str) -> list[dict[str, str]]:
    """Parse an EDGAR ``form.YYYYMMDD.idx`` file into one dict per filing."""
    lines = text.splitlines()
    sep_idx = next((i for i, line in enumerate(lines) if set(line.strip()) == {"-"}), None)
    body = lines[sep_idx + 1 :] if sep_idx is not None else lines

    rows: list[dict[str, str]] = []
    for line in body:
        match = _INDEX_ROW_RE.match(line)
        if match is None:
            continue
        row = match.groupdict()
        row["accession_number"] = Path(row["file_name"]).stem
        rows.append(row)
    return rows


def select_index_rows(
    rows: list[dict[str, str]], limit: int, keep_ciks: set[str]
) -> list[dict[str, str]]:
    """Cap the index to ``limit`` rows without throwing away the interesting ones.

    Ordering is by form-type interest then by accession, so the same input file always
    yields the same subset -- a test fixture that changes shape between runs is not a
    fixture.
    """
    rank = {form: i for i, form in enumerate(INTERESTING_FORMS)}

    def sort_key(row: dict[str, str]) -> tuple[int, int, str]:
        forced = 0 if row["cik"].zfill(10) in keep_ciks else 1
        return (forced, rank.get(row["form_type"], len(rank)), row["accession_number"])

    interesting = [r for r in rows if r["form_type"] in rank or r["cik"].zfill(10) in keep_ciks]
    return sorted(interesting, key=sort_key)[:limit]


def write_stream(out_root: Path, stream: str, envelopes: list[Envelope]) -> int:
    """Write one NDJSON part per ``logical_date`` partition.

    Partitioning by the envelope's own logical_date, not by the run's: a backfill that
    pulls three index days must land three partitions, or the partition value and the
    envelope disagree and every "what did we ingest for that day" query lies.
    """
    total = 0
    by_date: dict[str, list[Envelope]] = {}
    for env in envelopes:
        by_date.setdefault(env.logical_date, []).append(env)
    for logical_date, group in sorted(by_date.items()):
        path = Path(streams.landing_path(str(out_root), stream, logical_date)) / "part-00000.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            for env in group:
                fh.write(env.to_json_line())
                fh.write("\n")
        total += path.stat().st_size
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", default="data/landing", help="landing root to write into")
    parser.add_argument(
        "--logical-date", default=None, help="YYYY-MM-DD; defaults to the last index date"
    )
    parser.add_argument(
        "--index-dates",
        default="",
        help="comma-separated YYYY-MM-DD daily-index dates to pull (default: last 2 business days available)",
    )
    parser.add_argument(
        "--ciks", default=",".join(DEFAULT_CIKS), help="comma-separated 10-digit CIKs"
    )
    parser.add_argument("--max-index-rows", type=int, default=150, help="cap per index date")
    parser.add_argument(
        "--max-recent-filings", type=int, default=40, help="trim submissions filings.recent"
    )
    parser.add_argument("--user-agent", default=None, help="overrides EDGAR_USER_AGENT")
    parser.add_argument(
        "--inject-bad-accession",
        action="store_true",
        help="append one deliberately malformed index row so quarantine can be verified end to end",
    )
    parser.add_argument(
        "--inject-rescue",
        action="store_true",
        help="append one envelope carrying an unknown extra field so _rescued_data can be verified",
    )
    args = parser.parse_args(argv)

    user_agent = args.user_agent or os.environ.get("EDGAR_USER_AGENT")
    if not user_agent:
        parser.error(
            "the SEC requires a declared User-Agent. Set EDGAR_USER_AGENT='Name email@example.com' "
            "or pass --user-agent."
        )

    fetcher = Fetcher(user_agent)
    out_root = Path(args.out)
    fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    ciks = [c.strip().zfill(10) for c in args.ciks.split(",") if c.strip()]

    index_dates = [d.strip() for d in args.index_dates.split(",") if d.strip()]
    if not index_dates:
        index_dates = discover_recent_index_dates(fetcher, count=2)
    logical_date = args.logical_date or max(index_dates)

    report: dict[str, Any] = {
        "generated_at": fetched_at,
        "logical_date": logical_date,
        "index_dates": index_dates,
        "ciks": ciks,
        "streams": {},
    }

    # ---------------------------------------------------------------- filing_index
    index_envelopes: list[Envelope] = []
    for d in index_dates:
        day = date.fromisoformat(d)
        url = (
            f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/"
            f"QTR{_quarter(day)}/form.{day.strftime('%Y%m%d')}.idx"
        )
        got = fetcher.get(url, allow_404=True)
        if got is None:
            print(f"  ! no daily index for {d} (weekend or holiday), skipping")
            continue
        status, body = got
        rows = select_index_rows(
            parse_daily_index(body.decode("utf-8", errors="replace")),
            args.max_index_rows,
            set(ciks),
        )
        for row in rows:
            index_envelopes.append(
                Envelope("filing_index", row["accession_number"], d, url, status, row, fetched_at)
            )
        print(f"  filing_index {d}: {len(rows)} rows")

    if args.inject_bad_accession and index_envelopes:
        bad = dict(index_envelopes[0].payload)
        bad["accession_number"] = "NOT-AN-ACCESSION"
        bad["company_name"] = "QUARANTINE TEST FILER"
        index_envelopes.append(
            Envelope(
                "filing_index",
                "injected-bad-accession",
                index_envelopes[0].logical_date,
                "injected://quarantine-test",
                200,
                bad,
                fetched_at,
            )
        )
        print("  filing_index: injected 1 malformed accession for the quarantine check")

    size = write_stream(out_root, "filing_index", index_envelopes)
    report["streams"]["filing_index"] = {"records": len(index_envelopes), "bytes": size}

    # ----------------------------------------------------------- company_submissions
    sub_envelopes: list[Envelope] = []
    for cik in ciks:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        got = fetcher.get(url, allow_404=True)
        if got is None:
            print(f"  ! no submissions for {cik}")
            continue
        status, body = got
        payload = json.loads(body)
        payload = trim_submissions(payload, args.max_recent_filings)
        sub_envelopes.append(
            Envelope("company_submissions", cik, logical_date, url, status, payload, fetched_at)
        )
    size = write_stream(out_root, "company_submissions", sub_envelopes)
    report["streams"]["company_submissions"] = {"records": len(sub_envelopes), "bytes": size}
    print(f"  company_submissions: {len(sub_envelopes)} documents")

    # -------------------------------------------------------------- company_concept
    concept_envelopes: list[Envelope] = []
    missing = 0
    for cik in ciks:
        for taxonomy, tag in concepts.tags_to_fetch():
            url = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/{taxonomy}/{tag}.json"
            got = fetcher.get(url, allow_404=True)
            if got is None:
                missing += 1
                continue
            status, body = got
            concept_envelopes.append(
                Envelope(
                    "company_concept",
                    f"{cik}/{taxonomy}/{tag}",
                    logical_date,
                    url,
                    status,
                    json.loads(body),
                    fetched_at,
                )
            )
    if args.inject_rescue and concept_envelopes:
        # An envelope carrying a field the contract does not name. Auto Loader (and the
        # local batch reader) must route it to _rescued_data and raise a WARN rather
        # than dropping it silently -- that is the only signal we get that the upstream
        # payload changed shape.
        line_extra = {"_injected_unknown_field": "schema-drift-probe"}
        path = Path(streams.landing_path(str(out_root), "company_concept", logical_date))
        path.mkdir(parents=True, exist_ok=True)
        drift = json.loads(concept_envelopes[0].to_json_line())
        drift.update(line_extra)
        drift["resource_id"] = "injected/schema-drift"
        (path / "part-00001-drift.json").write_text(
            json.dumps(drift, separators=(",", ":"), sort_keys=True) + "\n", encoding="utf-8"
        )
        print("  company_concept: injected 1 schema-drift record for the _rescued_data check")

    size = write_stream(out_root, "company_concept", concept_envelopes)
    report["streams"]["company_concept"] = {
        "records": len(concept_envelopes),
        "bytes": size,
        "not_reported_by_filer": missing,
    }
    print(f"  company_concept: {len(concept_envelopes)} documents ({missing} tags not reported)")

    report["http_requests"] = fetcher.request_count
    report_path = out_root / "_fetch_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nwrote {report_path} ({fetcher.request_count} HTTP requests)")
    return 0


def trim_submissions(payload: dict[str, Any], max_recent: int) -> dict[str, Any]:
    """Trim ``filings.recent`` to keep the committed fixture small.

    The document keeps its real shape -- every key, every column of the parallel-array
    encoding -- because the point of using real data is to exercise the real shape.
    Only the number of rows shrinks.
    """
    recent = payload.get("filings", {}).get("recent")
    if not isinstance(recent, dict):
        return payload
    trimmed = {k: (v[:max_recent] if isinstance(v, list) else v) for k, v in recent.items()}
    payload["filings"] = {**payload["filings"], "recent": trimmed, "files": []}
    return payload


def discover_recent_index_dates(fetcher: Fetcher, count: int) -> list[str]:
    """Find the most recent daily-index dates that actually exist."""
    today = datetime.now(UTC).date()
    year, quarter = today.year, _quarter(today)
    got = fetcher.get(
        f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/index.json",
        allow_404=True,
    )
    if got is None:
        raise SystemExit(f"no daily-index listing for {year} QTR{quarter}")
    listing = json.loads(got[1])
    stamps = sorted(
        {
            item["name"].split(".")[1]
            for item in listing["directory"]["item"]
            if item["name"].startswith("form.") and item["name"].endswith(".idx")
        }
    )
    return [f"{s[0:4]}-{s[4:6]}-{s[6:8]}" for s in stamps[-count:]]


if __name__ == "__main__":
    raise SystemExit(main())
