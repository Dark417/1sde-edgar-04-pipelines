# 04 — Deploying to Databricks

> Owned by this repo. The manual checks come from `AGENTS.md` §9; this is the same
> sequence with the exact commands.

**Nothing here is safe to skip on Free Edition.** The daily compute quota shuts the
workspace down for the rest of the day when exhausted, so every step below is ordered
to fail *before* it costs compute rather than during a job run.

---

## 0. What has to exist first

This repo is 4 of 5 and creates none of its own infrastructure. Deploying before the
prerequisites exist produces a job that fails on its first task.

| Needed | Owned by | Symptom if absent |
|---|---|---|
| `edgar` catalog + 4 schemas + landing volume | repo 2 (Terraform) | preflight fails naming the schema |
| Every contract table | repo 1 (Liquibase) | preflight fails naming the changeset |
| `edgar_lakehouse_contracts` wheel | repo 1 | this repo currently uses an in-repo mirror — ADR-001 |
| Landing objects | repo 3 | job succeeds, ingests zero rows |
| `/edgar-lakehouse/s3/serving_bucket` SSM parameter | repo 2 | `MissingConfigError` naming the key |

One command tells you which of these are missing — see step 3.

---

## 1. Get a workspace token

**Settings → Developer → Access tokens → Generate new token.**

A workspace personal access token **starts with `dapi`** and is about 36 characters.
If yours doesn't, it is not a workspace PAT — an account-level token, an OAuth client
secret, and a GitHub token all produce the same unhelpful error:

```
{"error_code":401,"message":"Credential was not sent or was of an unsupported type for this API."}
```

That message means the request reached Databricks and the credential was rejected. It
does **not** mean the host is wrong.

```bash
cp .env.example .env        # .env is gitignored; never commit a token
$EDITOR .env
set -a && . ./.env && set +a
```

---

## 2. Install the CLI

```bash
brew install databricks          # or: pip install databricks-cli
databricks --version             # needs v0.2xx+ for bundles
```

`DATABRICKS_HOST` and `DATABRICKS_TOKEN` from `.env` are enough; `databricks configure`
is optional.

---

## 3. Verify the workspace — *before* building anything

```bash
python tools/dbx_verify.py
```

Read-only: no `CREATE`, no `POST`, no job run. It reports the token identity, the
catalog and schemas, every contract table **and its columns**, the landing volume,
whether repo 3 has landed anything, and whether the job already exists with a paused
schedule. Exit code is non-zero if anything is missing, so CI can gate on it.

Sample of the output you want:

```
[  ok  ] token accepted
[  ok  ] catalog edgar
[  ok  ] schemas
[  ok  ] tables (13 in the contract)
[  ok  ] table columns match the contract
[  ok  ] landing volume
[  ok  ] landing objects (repo 3 output)
[MISSING] job definition
           no edgar job yet (0 job(s) in the workspace). `databricks bundle deploy -t dev` creates it.
```

A missing job at this point is expected — that is what step 5 creates. A missing table
is not: go back to repo 1 and run `liquibase update`. Generating or deploying pipeline
code against tables that do not exist wastes a full cycle.

---

## 4. Build the wheel

```bash
pip install build
python -m build --wheel        # -> dist/edgar_pipelines-0.1.0-py3-none-any.whl
```

The wheel declares **no PySpark or Delta dependency** — those come from the serverless
runtime, and installing them into a job shadows the runtime build. `pyproject.toml`
keeps them in the `dev` extra, and `tests/test_repo_guards.py` asserts it.

---

## 5. Validate, then deploy the bundle

```bash
python -m tools.fetch_contracts_wheel     # REQUIRED, see below
databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

**The fetch step is not optional.** `databricks.yml` lists `./dist/contracts/*.whl` as a
serverless dependency — `edgar-lakehouse-contracts` is not on PyPI, so serverless cannot
resolve it from an index — but `dist/` is gitignored. Whatever the glob matches is
whatever the last session happened to leave on disk:

- on a **fresh clone** it matches nothing, and the job installs no contracts package;
- on a **stale checkout** it matches an old version. This has already happened: a deploy
  shipped `edgar_lakehouse_contracts-1.4.0` while `pyproject.toml` pinned `1.4.1`. Nothing
  caught it, because `bundle deploy` prints the filename it uploads and no one compares
  that line to the pin.

`fetch_contracts_wheel` reads the version from `pyproject.toml` — the single pin — deletes
any other version so pip is never left choosing between two, and downloads the matching
wheel from repo 1's release. `--check` verifies without downloading and is what to run if
you only want to know whether the directory is honest.

`deploy` uploads the wheel and creates/updates the job definition. It does **not** run
anything. Re-run `tools/dbx_verify.py` afterwards; `job definition` should now be `ok`
with `schedule: UNPAUSED` (it was PAUSED until the first green end-to-end run).

---

## 6. Run bronze interactively — do not schedule yet

`AGENTS.md` §9.4. Run the task from the Jobs UI ("Run now" on `bronze_ingest` only), or
in a notebook with the wheel attached:

```python
%pip install /Volumes/.../edgar_pipelines-0.1.0-py3-none-any.whl
dbutils.library.restartPython()

import os
os.environ["EDGAR_LOGICAL_DATE"] = "2026-07-31"
os.environ["EDGAR_INGEST_MODE"]  = "autoloader"
os.environ["EDGAR_STORAGE_MODE"] = "volume"
from pipelines.entrypoints import bronze_ingest
bronze_ingest.main()
```

Then check by hand:

```sql
SELECT count(*), count(_rescued_data) FROM edgar.bronze.filing_index_raw;
```

`_rescued_data` must be null for every row on the first run. If it is not, the contract
is already wrong — fix repo 1 before touching silver.

---

## 7. 🔴 Run silver, then run it again

The single most important manual check in the project (`AGENTS.md` §9.5).

```sql
SELECT count(*) FROM edgar.silver.filing;
-- re-run the silver task
SELECT count(*) FROM edgar.silver.filing;                        -- must be IDENTICAL
SELECT count(DISTINCT _first_seen_ts) FROM edgar.silver.filing;  -- must not grow
```

If the count grows, the MERGE key is wrong. Stop and fix it — everything downstream is
built on this being true. The local suite covers the same property
(`test_silver_filing_run_twice_is_identical`), so a failure here means the workspace
differs from the contract, not that the transform is wrong.

---

## 8. Verify quarantine and the restatement feature

```sql
SELECT _dq_check_name, count(*) FROM edgar.silver.filing_quarantine GROUP BY 1;
```

Zero rows on clean data is expected. Zero rows *forever* means the checks are not wired
up — land a file with a malformed accession and confirm it appears. `tools/fetch_test_data.py
--inject-bad-accession` produces exactly that record if you want one to copy up.

Then pick a company you know amended a filing (`AGENTS.md` §9.7):

```sql
SELECT accession_number, filed_date, value
FROM edgar.silver.financial_fact
WHERE cik = '<cik>' AND concept_canonical = 'revenue_total' AND period_end = '<date>'
ORDER BY filed_date;
```

Two rows with different values, then confirm `gold.restatement_event` caught it. Then
find a company with *no* amendment and confirm it produces zero rows. **A restatement
table that flags everything is worse than no table.**

---

## 9. Export, then hand back to repo 2

```bash
aws s3 ls s3://<serving-bucket>/v1/ --recursive
aws s3 cp s3://<serving-bucket>/v1/_manifest.json - | jq
```

`gold_max_filed_date` must equal `max(filed_date)` in `silver.filing`.

Only once every check above passes, go to repo 2 §9.9 and set
`schedule_enabled=true`.

---

## Environment reference

Resolution is always `env var → SSM → fail naming the key`. There is no default for
anything environment-specific.

| Variable | Required | Notes |
|---|---|---|
| `EDGAR_LOGICAL_DATE` | yes | `YYYY-MM-DD`. The business date, not today. |
| `EDGAR_EXPORT_ROOT` | yes | Falls back to SSM `/edgar-lakehouse/s3/serving_bucket` |
| `EDGAR_CATALOG` | no | Defaults to `edgar` |
| `EDGAR_INGEST_MODE` | no | `autoloader` (default) or `batch` (local only) |
| `EDGAR_STORAGE_MODE` | no | `volume` (default), `s3`, or `local` |
| `EDGAR_LANDING_ROOT` | no | Falls back to SSM `/edgar-lakehouse/dbx/landing_volume` |
| `EDGAR_CHECKPOINT_ROOT` | no | Defaults to `<landing_root>/_checkpoints` |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | deploy only | Never read by pipeline code |

---

## Things that will cost you a day

* **`databricks bundle run`.** Never in CI. Deploying a definition is free; triggering a
  run burns quota on every merge. The CI workflow deliberately stops at `deploy`.
* **Retries.** The job sets no retry policy on purpose. A failed task here means bad
  data or a missing migration; retrying burns quota to reproduce the same failure.
* **Overlapping runs.** `max_concurrent_runs: 1`. Two concurrent runs race each other's
  MERGEs *and* double the compute.
* **Enabling the schedule early.** Leave it paused until §7 and §8 pass by hand.
