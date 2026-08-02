# 00 — Design doc (edgar-lakehouse)

> **Provenance:** reconstructed in repo 4. See `docs/README.md`. Repo 1 owns the
> authoritative copy once published.

## 1. What this is

A medallion lakehouse over SEC EDGAR filings and XBRL financial facts, built on
Databricks Free Edition (AWS, `us-east-2`), split across five repos with
one-directional dependencies.

| # | Repo | Role |
|---|---|---|
| 1 | `…-01-contracts` | Liquibase DDL + `edgar_lakehouse_contracts` wheel + schema-drift test |
| 2 | `…-02-infra` | Terraform: AWS + Databricks workspace objects + SSM interface |
| 3 | `…-03-ingest` | EDGAR → S3 (system of record) + Volume (transport) |
| 4 | `…-04-pipelines` | bronze → silver → gold → Parquet serving export |
| 5 | `…-05-serving` | FastAPI + DuckDB over the Parquet export |

The flagship user-facing feature is **restatement detection**: automatically surfacing
where a company later asserted a different value for a period it had already reported.

## 2. Data flow

```
SEC EDGAR ──repo 3──▶ s3://…/landing/    (system of record, immutable)
                       └─mirror─▶ /Volumes/edgar/landing/edgar/  (transport)
                                    │
                                    ▼  Auto Loader, rescue mode
                              edgar.bronze.*        append-only, replayable
                                    │  typing, normalization, DQ
                                    ▼
                              edgar.silver.*        MERGE on business key, SCD-2, quarantine
                                    │  aggregation, self-join
                                    ▼
                              edgar.gold.*          marts incl. restatement_event
                                    │  overwrite
                                    ▼
                        s3://<serving>/v1/{table}/data.parquet + _manifest.json
                                    │
                                    ▼  repo 5 (DuckDB) — never touches Databricks
```

## 3. Layer responsibilities

**Landing** is bytes as received, wrapped in an envelope (contracts §1). Never edited,
never deleted. Everything downstream is reproducible from it.

**Bronze** is landing, parsed into columns, with six metadata columns and nothing
else — append-only, no dedup beyond the file checkpoint, no `UPDATE`, no `DELETE`.
Bronze is what you replay from; a bronze you have edited is a bronze you cannot trust
as a replay source.

**Silver** is typed, normalized, deduped, quality-checked, and written **only** by
`MERGE` on a business key. `silver.financial_fact` is bitemporal: one row per
*assertion*, not per period. That distinction is what makes restatement detection
possible at all (see §6).

**Gold** is query-shaped marts, rebuilt from silver each run. Gold is disposable —
if a gold table is wrong, you fix the transform and rebuild; you never patch it.

**Serving export** writes each gold table to a single Parquet object plus a manifest,
because repo 5 must keep working when Databricks compute is quota-shut-down.

## 4. Platform constraints

### 4.1 Databricks Free Edition — these are design inputs, not annoyances

| Constraint | Consequence for this design |
|---|---|
| Serverless compute only | No cluster config, no init scripts, no JVM libraries. Pure Python + SQL. |
| **One active Lakeflow pipeline per type** | **No Declarative Pipelines / DLT.** Everything is a plain Job with explicit DQ code — `framework/dq.py` exists because DLT expectations are unavailable. |
| Max 5 concurrent job tasks | The task graph stays narrow: bronze fan-out is 3, silver is 3, gold is 4 but sequenced. |
| Python and SQL only | No Scala. |
| Daily quota exhaustion shuts compute down for the rest of the day | Jobs **fail fast** and **do not retry blindly**. `max_concurrent_runs: 1`. CI never runs a job. |
| No account-level API | Everything is workspace-scoped. |

The quota rule is the sharpest one: a job that retries three times on a real error
costs the rest of the day's compute, and the demo goes dark until tomorrow.

### 4.2 Why plain Jobs rather than DLT

DLT would give expectations, lineage and auto-scaling for free. Free Edition's
one-pipeline-per-type limit makes it unusable for a three-layer medallion. The cost is
that DQ, quarantine and metrics are hand-written here — which is what `framework/`
is.

## 5. Cross-repo contracts

### 5.1 Dependencies
Repos depend only on repo 1's published wheel (`==`, never `>=`, never `main`) and
repo 2's SSM parameters. Never on each other's source.

### 5.2 One owner per object
Tables → Liquibase (repo 1). Catalog/schemas/volume/jobs → Terraform (repo 2).
Landing objects → repo 3. Delta rows → repo 4. Parquet export → repo 4, read by
repo 5.

**This repo never issues `CREATE TABLE`.** A missing table is a failed migration, and
the correct response is a loud failure naming the changeset — not a silent
`CREATE TABLE IF NOT EXISTS` that forks the schema away from `DATABASECHANGELOG` and
destroys the migration audit trail.

### 5.3 Configuration
`env var → SSM → fail with a message naming the missing key`. No hardcoded ARNs,
hosts, buckets or paths outside repos 1–2. There is no fourth resolution step and no
default for anything environment-specific: a default bucket name is how a dev run
writes into the production prefix.

### 5.4 Why repo 5 reads Parquet and not Databricks
Free Edition compute shuts down on quota exhaustion. If the public demo talked to
Databricks, the demo would go dark whenever the quota did. Repo 5 reads Parquet from
S3 with DuckDB and has no `databricks` import at all; that import appearing in repo 5
is a design failure, not a shortcut.

## 6. The bitemporal core

XBRL facts are asserted, not stated. The same `(cik, concept, period)` is reported
repeatedly — in the original 10-K, in a 10-K/A, in the comparative column of the next
year's 10-K. Those assertions can disagree.

`silver.financial_fact` therefore keys on **`(cik, taxonomy, concept_tag, unit,
period_start, period_end, period_type, accession_number)`**. Collapsing
`accession_number` out of the grain — the "obvious" dedup — destroys exactly the
information restatement detection needs, and it does so silently: the table still
looks right.

Two time axes:
* *valid time* — `period_start` / `period_end`: when the fact was true.
* *transaction time* — `filed_date` / `accession_number`: when it was asserted.

(Kleppmann, *DDIA* ch. 11, is the framing.)

## 7. Restatement detection

Within one `(cik, concept_canonical, unit, period_start, period_end, period_type)`,
order assertions by `filed_date` and compare consecutive pairs.

Two rules keep the table from being noise:

1. **Never `!=`.** Filers report the same figure at different `decimals` scales;
   equality comparison flags every one of them. Comparison is a tolerance:
   `abs(later - earlier) > greatest(abs(earlier) * 1e-6, 1e-6)`.
   *(See ADR-002 — a fixed relative tolerance is necessary but not sufficient, and
   real EDGAR data proves it.)*
2. **Scope to identical `(unit, period_start, period_end, period_type)`.** Comparing a
   Q4 duration to an FY duration is not a restatement, it is a bug.

`materiality_band` (`immaterial` <1%, `notable` 1–5%, `material` >5%) is a **product
heuristic, not an accounting standard**. That sentence belongs in the docstring, in
this doc, and in the UI.

## 8. Correctness properties

### 8.1 Idempotency
Every repo has a run-it-twice test. Here the two that matter:

* **Silver run twice → identical row count *and* identical `_first_seen_ts` values.**
  If this fails, nothing downstream is trustworthy. `_first_seen_ts` is written on
  INSERT and never appears in an UPDATE set; `_last_seen_ts` updates on every merge.
* **A rounding-only difference produces zero restatement events.** If this fails,
  `gold.restatement_event` is noise and the flagship feature is worthless.

Determinism follows the same rule: batch ids, run ids and hashes derive from logical
dates and sorted inputs — never wall clock, never `hash()`, never dict order.

### 8.2 Schema evolution
Auto Loader runs with `schemaEvolutionMode=rescue` and a per-stream `schemaLocation`.
Unknown columns land in `_rescued_data` rather than being dropped.

**A non-null `_rescued_data` is a WARN, never a silent pass.** It is the only signal
we get that the SEC changed a payload shape. The count is emitted per stream and a
non-zero value moves the job status to WARN.

Deep documents (`submissions`, `companyconcept`) are kept as a single opaque
`payload_json` STRING in bronze. Exploding them at bronze would couple bronze to a
nested shape nobody here controls; the explode belongs in silver, where a shape change
is a transform fix rather than a table migration.

### 8.3 Data quality
Three severities, not two:

| Severity | Effect |
|---|---|
| `reject` | Failing rows move to the quarantine table; the batch continues. |
| `warn` | Nothing is removed. A metric is emitted; job status becomes WARN. |
| `reject_batch` | The batch is abandoned. Reserved for structural invariants. |

SCD-2 invariants are `reject_batch`: two `is_current` rows for one natural key fans
out every downstream join and doubles every aggregate, so one bad row means the
dimension is structurally broken.

**`apply_dq` emits a metric for every check, including checks that failed nothing.** A
missing key and a zero look identical on a dashboard and mean opposite things:
"nothing failed" versus "the check never ran".

## 9. Observability

Every job entrypoint emits exactly one structured JSON summary line: rows in, rows
out, rows quarantined, duration, status, and every metric any stage recorded. One
line, because that is what survives a log aggregator.

No `display()`, and no `dbutils` outside `entrypoints/` — anything touching `dbutils`
cannot run in the local test suite, and the local suite is the only place the two
tests in §8.1 can run without burning quota.

## 10. Build order

1 → 2 → 3 → 4 → 5, with one documented backward edge: repo 2 creates the catalog and
schemas before repo 1's `liquibase update` can run.
