# Repo 4 / 6 — `1sde-edgar-04-pipelines`

> ## ⚠️ Read `AGENTS.global.md` first
>
> This file covers **this repo only**. The project-wide rules — repo boundaries,
> the sensitive-values policy, the required response format, region, and the
> cross-repo laws — live in [`AGENTS.global.md`](AGENTS.global.md) beside this
> file, propagated from the workspace root. **Read it before acting.**
>
> Precedence: global rules bind everywhere; where this file and the global rules
> genuinely conflict, this file wins **for this repo** and the conflict is worth
> reporting rather than silently resolving.


> Sections 0–8 are agent instructions. Section 9 is
> yours, by hand. Section 10 is what repo 5 consumes.
>
> GitHub: `github.com/Dark417/1sde-edgar-04-pipelines`
> Build order position: **4 of 5.** Requires repos 1, 2, 3 complete and at least one
> landing object present.

---

## 0. Read first

This repo is the medallion transform: landing → bronze → silver → gold → serving
export. It is the largest and most consequential repo in the project, and it contains
the two tests that decide whether the whole thing works.

**Authoritative docs** in `docs/`: `00-design-doc.md` (§4.1 Free Edition, §8
idempotency/schema-evolution/DQ), `02-data-contracts.md` (**all of it** — §2, §3, §4,
§5 are this repo's specification).

**The two tests that decide the project:**
1. Running silver twice produces an identical row count *and* identical
   `_first_seen_ts` values. If this fails, nothing downstream is trustworthy.
2. A rounding-only difference produces **zero** restatement events. If this fails,
   `gold.restatement_event` is noise and the flagship feature is worthless.

Build those two tests before building the features they test.

---

## 1. Scope

### Owns
- Auto Loader ingestion from landing into bronze.
- Silver transforms: typing, dedup, MERGE, SCD-2, DQ execution, quarantine.
- Gold marts including restatement detection.
- The serving export job.
- Databricks Asset Bundle / wheel packaging and job task definitions.

### Does NOT own
- Table DDL. Repo 1's Liquibase created every table. **This repo never issues
  `CREATE TABLE`.** If a table is missing, the job fails loudly — that is correct
  behavior, because it means a migration was not applied.
- Schema definitions or DQ rule text. Both imported from `edgar_lakehouse_contracts`.
- Infrastructure, job scheduling, catalog/schema creation. Repo 2.
- Fetching anything from the internet. Repo 3. This repo reads landing only.

### The boundary that will tempt you
"The table doesn't exist, I'll just `CREATE TABLE IF NOT EXISTS`." **No.** That
silently forks the schema away from Liquibase's `DATABASECHANGELOG` and you lose the
migration audit trail entirely. Fail instead, with a message saying which changeset
is missing.

---

## 2. Prerequisites from repos 1, 2, 3

| Input | Source | Used for |
|---|---|---|
| `edgar_lakehouse_contracts==<version>` | repo 1 | schemas, DQ registry, names, concepts |
| Tables created by Liquibase | repo 1 §9.6 | every write target |
| `edgar` catalog + 4 schemas + volume | repo 2 | write targets |
| `/edgar-lakehouse/s3/serving_bucket` | repo 2 SSM | export destination |
| `/edgar-lakehouse/dbx/host`, job id | repo 2 SSM | deploy target |
| Landing objects | repo 3 | Auto Loader source |
| `LandingEnvelope` shape | repos 1+3 | bronze parsing |

**Verification gate, run at job startup:** assert every table this job writes exists in
`information_schema.tables`. Fail with a message naming the missing table and the
Liquibase changeset that creates it. This turns a confusing mid-job `AnalysisException`
into a one-line diagnosis.

---

## 3. Tech baseline

```
Python        3.11 (match the Databricks serverless runtime)
Spark         PySpark, version from the DBR serverless runtime
Delta         delta-spark (local tests), native on Databricks
Packaging     wheel; deployed via Databricks Asset Bundles (databricks.yml)
Tests         pytest + local SparkSession with Delta
Lint/types    ruff, mypy --strict (Spark code: mypy on non-Spark modules only)
```

**Free Edition constraints that shape this repo (design doc §4.1):**
- Serverless compute only — no cluster config, no init scripts, no JVM libs.
- **One active Lakeflow pipeline per type** → do not use Declarative Pipelines/DLT.
  Everything is a plain Job with explicit DQ code.
- Max 5 concurrent job tasks → the task graph stays narrow.
- Python and SQL only, no Scala.
- Quota exhaustion shuts compute down for the rest of the day → jobs must fail fast
  and must not retry blindly.

---

## 4. Layered structure

```
1sde-edgar-04-pipelines/
├── AGENTS.md
├── pyproject.toml
├── databricks.yml            # Asset Bundle: job + task wiring
├── src/pipelines/
│   ├── __init__.py
│   ├── config.py             # L0: settings from env/SSM/widgets
│   ├── session.py            # L0: SparkSession accessor (local vs DBR)
│   ├── framework/
│   │   ├── autoloader.py     # L1: read_landing_stream
│   │   ├── dq.py             # L1: apply_dq -> (passed, quarantined, metrics)
│   │   ├── merge.py          # L1: merge_scd1, merge_scd2
│   │   ├── preflight.py      # L1: table-existence gate
│   │   └── metrics.py        # L1: emit job metrics
│   ├── bronze/
│   │   ├── filing_index.py           # L2
│   │   ├── company_submissions.py    # L2
│   │   └── company_concept.py        # L2
│   ├── silver/
│   │   ├── filing.py         # L3
│   │   ├── company.py        # L3 (SCD-2)
│   │   └── financial_fact.py # L3
│   ├── gold/
│   │   ├── financials_current.py     # L4
│   │   ├── restatement_event.py      # L4  <- the differentiator
│   │   ├── filing_activity_daily.py  # L4
│   │   └── company_profile.py        # L4
│   ├── export/
│   │   └── serving.py        # L5: gold -> Parquet -> S3 + manifest
│   └── entrypoints/          # L6: thin, one per job task
├── tests/
│   ├── conftest.py           # local SparkSession + Delta fixture
│   └── fixtures/
└── .github/workflows/ci.yml
```

**Layer rule:** L1 framework knows nothing about EDGAR. L2–L5 contain domain logic and
call down into L1. Entrypoints contain no logic.

---

## 5. Non-negotiable rules for the agent

1. **Never `CREATE TABLE`.** Liquibase owns DDL. Preflight asserts existence and fails
   with the missing changeset id.
2. **Bronze is append-only.** No `UPDATE`, no `DELETE`, no dedup beyond Auto Loader's
   file-level checkpoint. Bronze is what you replay from.
3. **Every silver write is a `MERGE` on the business key.** Never `overwrite`, never
   `append` into silver. Re-running a batch must be a no-op.
4. **`_first_seen_ts` is set on insert and never updated.** `_last_seen_ts` updates
   every merge. Getting this backwards destroys the "when did we first see this"
   question, which is half of why the table exists.
5. **Sort array columns before hashing `_hash_diff`.** Source array ordering is not
   stable; unsorted hashing generates a spurious new SCD-2 version every single day
   and your dimension explodes. Put this sentence in the code comment — it is the most
   commonly re-introduced bug in this repo.
6. **Restatement comparison uses relative tolerance, never `!=`.**
   `abs(a-b) > greatest(abs(a)*1e-6, 1e-6)`. Filers report identical figures at
   different `decimals` scales; equality comparison makes the table pure noise.
7. **Restatement comparison is scoped to identical
   `(unit, period_start, period_end, period_type)`.** Comparing a Q4 duration against
   an FY duration is not a restatement, it is a bug.
8. **DQ severities are three, not two:** `reject` (quarantine the row), `warn` (metric,
   keep the row), `reject_batch` (fail the job). SCD-2 invariants are `reject_batch` —
   one bad row means the dimension is structurally broken and every downstream join
   fans out.
9. **`apply_dq` emits metrics even when zero rows fail.** A silent zero is
   indistinguishable from a check that never ran.
10. **Auto Loader options are fixed:** `cloudFiles.format=json`,
    `cloudFiles.schemaLocation=<per-stream>`, `cloudFiles.schemaEvolutionMode=rescue`,
    directory-listing mode. File-notification mode needs cloud event configuration
    that is unavailable here.
11. **`_rescued_data` non-null is a WARN, not a silent pass.** It is how you find out
    the source changed shape. Emit the count; a non-zero value sets job status to WARN.
12. **No `display()`, no `dbutils` outside entrypoints.** Everything testable must run
    in a local SparkSession.
13. **Every job entrypoint logs a structured summary** — rows in, rows out, rows
    quarantined, duration — as one line.

---

## 6. Features to generate

### F-1 · `framework/preflight.py`
```python
def assert_tables_exist(spark, tables: Sequence[str]) -> None: ...
```
Query `system.information_schema.tables` if available, else
`SHOW TABLES IN <schema>`. On failure raise `MissingTableError` naming the table and
the changeset (`020-silver.yaml`).

**Acceptance:** missing table → error message contains both the FQN and the changelog
filename.

### F-2 · `framework/autoloader.py`
```python
def read_landing_stream(spark, stream: Stream, mode: Literal["s3","volume"],
                        checkpoint_root: str) -> DataFrame: ...
```
Path from `names.landing_path` prefix. Adds `_metadata.file_path` as `_source_file`.

**Acceptance:** same file processed twice adds zero rows (checkpoint honored).

### F-3 · `framework/dq.py`
```python
def apply_dq(df: DataFrame, checks: Sequence[DQCheck],
             run_id: str) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Returns (passed, quarantined, metrics). Raises DQBatchFailure on reject_batch."""
```
Quarantined rows carry `_dq_failure_reason`, `_dq_check_name`, `_dq_run_id`,
`_quarantined_at`.

**Acceptance:** a `warn` check never removes rows; a `reject` check moves exactly the
failing rows; a `reject_batch` check raises; metrics dict has one key per check even
when all pass.

### F-4 · `framework/merge.py`
```python
def merge_scd1(spark, source, target_table, keys, update_cols=None) -> MergeStats: ...
def merge_scd2(spark, source, target_table, natural_key, tracked_cols,
               logical_date) -> MergeStats: ...
```

**SCD-2 test suite — all four are required:**
- (a) no change → 0 new rows
- (b) tracked column changed → old row closed (`valid_to = logical_date - 1`,
  `is_current = false`), new row inserted
- (c) **array column reordered, same members → 0 new rows** (rule 5)
- (d) run twice → identical result

### F-5 · Bronze (×3)
Per `02-data-contracts.md` §2. Six metadata columns on every table. Payload preserved:
`filing_index` gets passthrough columns; `company_submissions` and `company_concept`
keep `payload_json` as a single STRING — the documents are deeply nested and their
shape changes; exploding at bronze couples you to a shape you do not control.

**Acceptance:** re-processing a landing file adds zero rows. `bronze_rescued_row_count`
emitted per stream.

### F-6 · `silver/filing.py`
Per §3.1. Normalizes accession, pads CIK, derives `is_amendment` and `base_form_type`.

**Acceptance**
- 🔴 Idempotency: run twice → identical row count **and** identical `_first_seen_ts`.
- Malformed accession lands in `filing_quarantine`, not `filing`.
- `base_form_type` correct for `10-K`, `10-K/A`, `8-K`, `S-1/A`, and lowercase input.

### F-7 · `silver/company.py`
Per §3.2. Parses `payload_json`. SCD-2 via F-4.

**Acceptance:** F-4's four tests re-run against real fixture payloads. The
"exactly one `is_current` per cik" check is `reject_batch` — assert the job raises.

### F-8 · `silver/financial_fact.py`
Per §3.3. Explodes the XBRL `units` map. **Preserves `accession_number` at the grain.**

**Acceptance:** 🔴 a fixture where the same `(cik, concept, period)` is reported by two
accessions produces **2 rows, not 1**. If this fails, restatement detection is
impossible and there is no point continuing. Instant facts (`period_start` null) must
not fail the `period_end >= period_start` check.

### F-9 · `gold/restatement_event.py` — the differentiator
Per §4.2. Self-join `silver.financial_fact` within identical
`(cik, concept_canonical, unit, period_start, period_end, period_type)`, ordered by
`filed_date`, comparing consecutive assertions.

```sql
WHERE abs(later.value - earlier.value)
      > greatest(abs(earlier.value) * 1e-6, 1e-6)
```

`materiality_band`: `immaterial` <1%, `notable` 1–5%, `material` >5%. These are a
**product heuristic, not an accounting standard** — say so in the docstring and carry
it through to the UI.

**Acceptance**
- Restatement fixture (10-K then 10-K/A with a changed value) → exactly one row,
  correct `delta_pct` and `days_to_restatement`.
- 🔴 Rounding-only fixture (same value, different `decimals` scale) → **zero rows**.
- Different-`unit` fixture → zero rows.
- `delta_pct` is null, not an exception, when `original_value = 0`.

### F-10 · Remaining gold + export
`financials_current`, `filing_activity_daily`, `company_profile` per §4.
Export writes one Parquet per gold table to `s3://<serving>/v1/{table}/data.parquet`
plus `_manifest.json` per §5.

**Acceptance:** `manifest.gold_max_filed_date` equals `max(filed_date)` in
`silver.filing`. Export is idempotent (overwrite, not append).

### F-11 · `databricks.yml` (Asset Bundle)
Job matching repo 2's definition. Wheel referenced by pinned version. Task graph per
repo 2 F-6. `max_concurrent_runs: 1` — overlapping runs on a Free Edition quota is how
you lose a day.

---

## 7. Testing requirements

| Requirement | Threshold |
|---|---|
| Local Spark tests | required; `conftest.py` builds a SparkSession with Delta on tmpdir |
| Coverage (non-Spark modules) | ≥ 85% |
| Idempotency tests | F-6 and F-4(d), both required |
| Restatement false-positive tests | F-9, all three, required |
| Databricks in unit tests | zero — everything runs locally |

Mark Spark tests `@pytest.mark.spark` so the fast suite stays fast.

---

## 8. CI — `.github/workflows/ci.yml`

```
on: pull_request -> ruff, mypy (non-Spark), pytest -m "not spark",
                    pytest -m spark, contract-compat check
on: push main    -> above + build wheel + databricks bundle validate
                    + upload wheel + bundle deploy (dev target)
```
**Never `bundle run` in CI.** Deploying a definition is safe; triggering a run burns
Free Edition quota on every merge.

**Contract-compat check:** assert every table/column this repo reads or writes exists
in the pinned contracts version's schemas. Failing must block merge.

---

## 9. EXECUTION — what you do manually

### 9.1 Create the repo
```bash
gh repo create Dark417/1sde-edgar-04-pipelines \
  --private --add-readme --gitignore Python --license mit --clone
cd 1sde-edgar-04-pipelines
mkdir -p docs && cp ../design/00-design-doc.md ../design/02-data-contracts.md docs/
```

### 9.2 Confirm prerequisites before generating 🔴
```sql
-- in a Databricks notebook
SHOW TABLES IN edgar.bronze;
SHOW TABLES IN edgar.silver;
SHOW TABLES IN edgar.gold;
SELECT * FROM edgar.default.DATABASECHANGELOG ORDER BY dateexecuted DESC LIMIT 5;
```
All tables present and `DATABASECHANGELOG` populated. If not, go back to repo 1 §9.6.
Generating pipeline code against tables that do not exist wastes a full cycle.

```python
dbutils.fs.ls("/Volumes/edgar/landing/edgar/filing_index/")   # repo 3 output present?
```

### 9.3 Install the CLI and bundle tooling
```bash
brew install databricks       # or: pip install databricks-cli
databricks configure --token  # host + PAT
databricks bundle validate
```

### 9.4 Run bronze interactively — do not schedule 🔴
Attach the wheel or use Databricks Repos (GitHub app password) and run the bronze
entrypoint in a notebook.

**Check by hand:**
```sql
SELECT count(*), count(_rescued_data) FROM edgar.bronze.filing_index_raw;
```
`_rescued_data` must be null for every row on the first run. If it is not, your
contract is already wrong — fix repo 1 before touching silver.

### 9.5 Run silver, then run it again 🔴
This is the single most important manual check in the project.
```sql
SELECT count(*) FROM edgar.silver.filing;
-- re-run the silver entrypoint
SELECT count(*) FROM edgar.silver.filing;      -- must be IDENTICAL
SELECT count(DISTINCT _first_seen_ts) FROM edgar.silver.filing;  -- must not grow
```
If the count grows, your MERGE key is wrong. Stop and fix it. Everything downstream is
built on this being true.

### 9.6 Verify quarantine is working
```sql
SELECT _dq_check_name, count(*) FROM edgar.silver.filing_quarantine GROUP BY 1;
```
Zero rows on clean data is expected. Zero rows *forever* means the checks are not
wired up — deliberately inject a malformed accession into a landing file and confirm
it lands in quarantine.

### 9.7 Verify the restatement feature by hand 🔴
Pick a company you know amended a filing. Confirm:
```sql
SELECT accession_number, filed_date, value
FROM edgar.silver.financial_fact
WHERE cik = '<cik>' AND concept_canonical = 'revenue_total'
  AND period_end = '<date>'
ORDER BY filed_date;
```
Two rows with different values, then check `gold.restatement_event` caught it. Then
find a company with *no* amendment and confirm it produces zero rows. A restatement
table that flags everything is worse than no table.

### 9.8 Run the export and check the manifest
```bash
aws s3 ls s3://<serving-bucket>/v1/ --recursive
aws s3 cp s3://<serving-bucket>/v1/_manifest.json - | jq
```

### 9.9 Deploy the bundle, then tell repo 2 to enable the schedule
```bash
databricks bundle deploy -t dev
```
Only now go back to repo 2 §9.9 and set `schedule_enabled=true`.

### 9.10 Watch the quota
Databricks UI → usage. If you are near the daily limit, stop. Compute shutdown for the
rest of the day costs you an evening.

---

## 10. Published outputs — what repo 5 consumes

| Output | Form | Consumed by |
|---|---|---|
| `s3://<serving>/v1/{table}/data.parquet` | Parquet, overwritten daily | 5 (DuckDB reads) |
| `s3://<serving>/v1/_manifest.json` | JSON | 5 (`/health` freshness) |
| Gold column names/types | per contracts §4 | 5 (API response models) |

**Contract with repo 5:** repo 5 reads Parquet from S3 and **never** connects to
Databricks. A `databricks` import in repo 5 is a design failure (design doc §5.4) —
Free Edition compute shuts down on quota exhaustion, and the demo link must survive
that.

---

## 11. Definition of done

- [ ] `ruff`, `mypy`, both pytest suites green
- [ ] Preflight fails clearly when a table is missing
- [ ] Bronze re-processing adds zero rows
- [ ] 🔴 Silver run twice → identical count and identical `_first_seen_ts`
- [ ] 🔴 SCD-2 array-reorder test → zero new versions
- [ ] 🔴 Same period from two accessions → 2 rows in `financial_fact`
- [ ] 🔴 Rounding-only fixture → zero restatement events
- [ ] Real restatement verified by hand against a known 10-K/A
- [ ] Quarantine verified with an injected bad row
- [ ] Export + manifest correct
- [ ] Bundle deployed; schedule enabled only after all of the above

---

## 12. References

1. Auto Loader schema evolution and rescued data — https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema
2. Delta Lake MERGE — https://docs.delta.io/latest/delta-update.html
3. Databricks Asset Bundles — https://docs.databricks.com/aws/en/dev-tools/bundles/
4. Free Edition limitations (why plain Jobs, not DLT) — https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
5. SEC XBRL `companyconcept` response shape — https://www.sec.gov/edgar/sec-api-documentation
6. `databricks/delta-live-tables-notebooks` — readable medallion reference — https://github.com/databricks/delta-live-tables-notebooks
7. Kleppmann, *DDIA* ch. 11 — the bitemporal framing behind `financial_fact`
