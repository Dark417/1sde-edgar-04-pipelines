# 03 — The local test harness

> Owned by this repo. Not a repo 1 document.

## Why this exists

The designed data path is:

```
SEC EDGAR ──repo 3──▶ S3 (system of record) ──▶ Volume ──▶ repo 4 reads landing
```

Repo 3 does not exist yet, and neither does a Databricks workspace to point at. But
`AGENTS.md` §7 requires the whole suite — including the two tests that decide the
project — to run in a local `SparkSession` with **zero Databricks**.

So there is a second, explicitly temporary path for development: **download a small
real subset of EDGAR straight to a local landing directory, then load it into local
Delta tables with the same transforms the workspace runs.**

Everything that makes that possible lives in `tools/` and is imported only by tests and
by the harness itself. Nothing under `src/pipelines` knows this path exists — it reads
landing, and landing looks the same whether it is `s3://…`, `/Volumes/…` or
`data/landing`.

| Piece | Stands in for | Lives in |
|---|---|---|
| `tools/fetch_test_data.py` | repo 3's ingest | `tools/` |
| `tools/local_ddl.py` | repo 1's Liquibase | `tools/` (ADR-004) |
| `tools/run_local_pipeline.py` | the Databricks job's four tasks | `tools/` |
| `src/pipelines/contracts/` | repo 1's published wheel | mirror (ADR-001) |

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"

# 1. Pull the test subset. Needs network. The SEC requires a declared User-Agent.
export EDGAR_USER_AGENT="Your Name your@email"
python tools/fetch_test_data.py --out data/landing \
    --index-dates 2026-07-30,2026-07-31 --max-index-rows 120 \
    --inject-bad-accession --inject-rescue

# 2. Run the whole medallion locally.
python tools/run_local_pipeline.py --landing data/landing --warehouse .local/warehouse

# 3. Poke at the result.
python tools/run_local_pipeline.py --warehouse .local/warehouse \
    --sql "SELECT * FROM spark_catalog.gold.restatement_event ORDER BY delta_abs DESC LIMIT 20"
```

## The committed test data

`data/landing/` holds ~1.3 MB of **real** EDGAR responses, committed so the pipeline
can be run and reviewed without network access.

| Stream | Records | What it is |
|---|---|---|
| `filing_index` | 241 | Two days of `form.YYYYMMDD.idx` rows, capped and biased toward amendments |
| `company_submissions` | 4 | Whole submissions documents, `filings.recent` trimmed to 40 rows |
| `company_concept` | 49 | Whole `companyconcept` documents, untrimmed |

Four companies, chosen because three of them filed a **10-K/A** in the last week of
July 2026 — so the sample genuinely contains multiple assertions of the same period,
which is what makes restatement detection testable against real data rather than only
against fixtures:

| CIK | Company | Why |
|---|---|---|
| `0001825088` | Dream Finders Homes | 10-K/A 2026-07-30; **also the source of ADR-002's counterexample** |
| `0000066418` | Mexco Energy | 10-K/A 2026-07-30 |
| `0001673481` | Sports Entertainment Gaming Global | 10-K/A 2026-07-31 |
| `0000320193` | Apple | Large, well-known; its 2008–2009 revenue-recognition restatements are real and show up in gold |

### Two records are deliberately broken

`--inject-bad-accession` appends an index row whose accession is `NOT-AN-ACCESSION`.
`AGENTS.md` §9.6 asks for exactly this: *"deliberately inject a malformed accession
into a landing file and confirm it lands in quarantine"*. Zero quarantine rows on clean
data is expected; zero quarantine rows **forever** means the checks are not wired up.

`--inject-rescue` appends a landing record carrying a field the contract does not name.
It must land in `_rescued_data` and move the job status to WARN (rule 11). A dropped
column is a source change nobody ever finds out about.

## What a local run proves

Run it and you get, from the harness's own output:

* **Bronze re-processing adds zero rows.** The second run reads 0 files.
* **🔴 Silver run twice → identical row count and identical `_first_seen_ts`.** The
  second run reports `rows_inserted: 0` and `rows_target_after` unchanged.
* **Quarantine works.** `filing_quarantine` holds exactly the injected bad row — and
  exactly one copy of it, no matter how many times you re-run.
* **🔴 Restatement detection is not noise.** Against the committed sample the top
  events are Apple's genuine 2008–2009 revenue-recognition restatements, and the Dream
  Finders scale artifacts (`79,093,455` → `79,093,000`) produce **zero** rows.
* **Export + manifest.** `.local/warehouse/export/v1/{table}/data.parquet` plus
  `_manifest.json`, with `gold_max_filed_date` equal to `max(filed_date)` in
  `silver.filing`.

## Differences from the workspace, stated plainly

A harness that quietly differs from production is worse than no harness. These are the
four differences, and each one is a deliberate, documented trade:

1. **Auto Loader → a file ledger.** `cloudFiles` is Databricks-only. The local reader
   lists files, skips any already in a JSON ledger under the checkpoint root, and
   reproduces `schemaEvolutionMode=rescue` for *unknown columns* (Auto Loader also
   rescues type mismatches). ADR-003. Selected by `EDGAR_INGEST_MODE=batch`.
2. **Liquibase → `tools/local_ddl.py`**, rendering the same `TableSpec` objects
   preflight validates against, from outside the shipped package. ADR-004.
3. **No persistent metastore.** The local Spark build's catalog is in-memory: the Delta
   data under `--warehouse` outlives the session but the table registrations do not.
   That is why `--sql` re-attaches tables by `LOCATION` before querying. Unity Catalog
   persists, so this has no production analogue.
4. **The whole medallion runs in one process**, where the job runs it as four tasks.
   The stage boundaries are the same functions; only the process boundary differs.

## Running the tests

```bash
pytest -m "not spark"   # fast: contracts, config, metrics, the fetcher, repo guards
pytest -m spark         # local SparkSession + Delta
pytest                  # everything
ruff check . && mypy src/pipelines
```

`tests/conftest.py` builds one session-scoped `SparkSession` (JVM startup otherwise
dominates) and recreates all 13 contract tables per test.

## Moving to Databricks

Nothing in `src/pipelines` changes. Set the environment and run the same entrypoints:

```bash
export EDGAR_LOGICAL_DATE=2026-07-31
export EDGAR_INGEST_MODE=autoloader     # cloudFiles instead of the file ledger
export EDGAR_STORAGE_MODE=volume
# EDGAR_LANDING_ROOT and EDGAR_EXPORT_ROOT resolve from repo 2's SSM parameters.
edgar-pipelines bronze_ingest
```

Then follow `AGENTS.md` §9.4 onward for the manual checks — in particular §9.5, "run
silver, then run it again", which is the single most important manual check in the
project.
