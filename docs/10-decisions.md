# 10 — Decisions (ADRs)

Decisions this repo had to make that `AGENTS.md` does not settle, or where following
`AGENTS.md` literally would not have met its own acceptance criteria. Each records
what was decided, why, and what it would take to reverse.

---

## ADR-001 — The contracts package is mirrored in-repo, behind one import site

**Status:** accepted, temporary by construction.

**Context.** `AGENTS.md` §2 requires schemas, the DQ registry, names and concepts to
come from `edgar_lakehouse_contracts==<version>`, repo 1's published wheel. That wheel
does not exist yet. Repo 4 cannot be written, tested or reviewed without those
definitions.

**Options considered.**
1. Stub the imports and leave the modules empty. Rejected: `AGENTS.global.md` rule 10
   forbids `TODO` placeholders, and an empty schema makes every test vacuous.
2. Vendor a copy of repo 1's source. Rejected: repo 1 has no source to vendor yet, and
   a vendored copy invites edits that silently fork.
3. Write an in-repo **mirror** with an automated drift check against the wheel.
   Accepted.

**Decision.** `src/pipelines/contracts/` holds the definitions repo 4 needs, written to
be replaced. `pipelines.contracts.provenance()` reports `"mirror"` or `"published"`.
`pipelines.contracts.verify_against_published()` diffs the mirror against the wheel
whenever the wheel is importable and returns every discrepancy; `tests/
test_contract_compat.py` fails on any. Nothing else in `src/pipelines` imports the
mirror modules directly — every consumer goes through the package, so the swap is one
import site.

The check is **one-directional**: every table and column *this repo touches* must exist
in the wheel with the same type. Columns the wheel has and we do not are fine — repo 1
serves other consumers.

**Reversal.** Install the wheel, replace the mirror modules with re-exports, run the
compat test. No call site changes.

---

## ADR-002 — Restatement comparison uses a precision-aware floor, not a bare relative tolerance

**Status:** accepted. **This deviates from the literal text of `AGENTS.md` rule 6 and
should be reviewed.**

**Context.** `AGENTS.md` rule 6 and F-9 fix the comparison as:

```sql
abs(later.value - earlier.value) > greatest(abs(earlier.value) * 1e-6, 1e-6)
```

and F-9's acceptance requires that a *rounding-only* difference produces **zero**
rows. The stated reason is exactly right: "filers report identical figures at
different `decimals` scales; equality comparison makes the table pure noise."

**What the real data shows.** Two problems, both found against live EDGAR:

1. **`decimals` is not in the payload.** The `companyconcept` API returns
   `start, end, val, accn, fy, fp, form, filed, frame` and nothing else. There is no
   `decimals` field to compare scales with. Verified against
   `data.sec.gov/api/xbrl/companyconcept/...` on 2026-08-01.

2. **`1e-6` is too tight for the rounding it is meant to absorb.** Dream Finders Homes
   (CIK `0001825088`), FY2020 `NetIncomeLoss`:

   | Accession | Filed | Value |
   |---|---|---|
   | `0001140361-22-009752` | 2022-03-16 | `79,093,455` |
   | `0001825088-23-000011` | 2023-03-02 | `79,093,000` |

   That is one figure rounded to the nearest thousand. Relative difference
   `455 / 79,093,455 = 5.75e-6` — **5.75× the tolerance.** The literal rule flags it.
   Rounding an *n*-digit figure to the nearest 10³ moves it by up to `500`, so any
   value below `5 × 10⁸` reported at both scales exceeds a `1e-6` relative tolerance.
   The same filer's `Assets` and `Revenues` for the same period are the identical
   pattern and happen to fall *under* the threshold only because they are ten times
   larger.

   Left as-is, `gold.restatement_event` fills with exactly the noise rule 6 exists to
   prevent, and F-9's own acceptance test fails on real data.

**Decision.** Keep the rule-6 expression as the floor and add one more term:

```
threshold = greatest(
    abs(earlier) * rel_tol,             -- 1e-6, rule 6
    abs_tol,                            -- 1e-6, rule 6
    scale_multiplier * reporting_scale  -- new; only when decimals_aware
)
```

`reporting_scale` is the coarser of the two values' reported precisions:

* when `decimals` is present (a future ingest path from raw XBRL instances), it is
  `10^(-min(decimals_earlier, decimals_later))` — the literal reading of rule 6's
  intent;
* when `decimals` is absent, it is **inferred from the values' trailing zeros**, capped
  at `abs(value) * max_scale_fraction`.

Two constants, both fixed by what the real data does rather than by taste:

**`scale_multiplier = 1.0`, not `0.5`.** Half a unit is what *rounding* can move a
value by. Filers also **truncate**. The same company reported `44,694,524` and later
`44,694,000` — correct rounding to thousands would have given `44,694,000`… no:
`44,695,000`. The filer cut the value rather than rounding it, landing 524 away. A
half-unit floor catches the rounders and flags every truncator.

**`max_scale_fraction = 1e-3`** (four significant digits). Without a cap, a value that
happens to be round — `2,000,000` — would carry a ±2,000,000 tolerance and every
restatement of it would vanish. 1e-3 is the coarsest real reporting in the sample:
`1,034,000` stated to the nearest thousand is exactly 1e-3.

Against the case above: inferred scales are `1` and `1,000`, floor is `1,000`, observed
difference is `455` → **not a restatement.** Against a genuine 1%+ restatement the
floor is three orders of magnitude below the difference and changes nothing. On the
committed sample this drops 9 scale artifacts and keeps all of Apple's real 2008–2009
revenue-recognition restatements.

**What it costs.** With `decimals` absent, a difference below 0.1% of the value cannot
be distinguished from the same figure re-stated at a coarser scale, and is not flagged.
That is an order of magnitude below the `immaterial` band's own 1% floor, so nothing a
user would call a restatement falls in the gap — but it is a real limit, and
`test_the_documented_blind_spot_is_asserted_not_assumed` keeps it visible in the suite
rather than discoverable in production. It disappears the day a source supplies
`decimals`; the same test asserts that.

`decimals_aware` is a parameter (`RestatementTolerance`), **default `True`**. Setting
it `False` reproduces rule 6's literal expression, and that path has its own test, so
the spec-as-written stays executable and reviewable.

**Reversal.** Set `decimals_aware=False`. Expect false positives on any figure below
~`5 × 10⁸` that a filer re-reported at a coarser scale.

**Escalation.** Per `AGENTS.global.md` ("when a repo file and the authoritative docs
disagree, stop and report the conflict"), this is reported rather than resolved
silently: rule 6's *expression* and F-9's *acceptance criterion* cannot both hold on
real EDGAR data. The implementation satisfies the acceptance criterion, which is the
one that describes the observable behavior users care about.

---

## ADR-003 — A local batch landing reader alongside Auto Loader

**Status:** accepted.

**Context.** `AGENTS.md` §7 requires the whole suite — including the two tests that
decide the project — to run in a local `SparkSession` with **zero** Databricks. Auto
Loader (`cloudFiles`) is a Databricks-only source and does not exist in OSS Spark.

**Decision.** `framework/autoloader.py` carries two readers behind one contract.
`read_landing_stream` is the production path with the fixed options from rule 10.
`read_landing_batch` is the local path: it lists landing files, skips any already in a
JSON ledger under the checkpoint root, and reproduces `schemaEvolutionMode=rescue` by
routing unknown columns into `_rescued_data` instead of dropping them. `EDGAR_INGEST_MODE`
selects between them.

The ledger is committed **after** the write succeeds, so a crash mid-write replays the
file rather than losing it. At-least-once into an append-only bronze is the right
trade: bronze is what you replay from, and the silver MERGE is what makes the end state
idempotent.

**Known difference:** Auto Loader's rescue captures unknown columns *and* values that
do not fit the tracked type. The batch reader captures unknown columns only. Both make
`_rescued_data` non-null on a payload shape change, which is what rule 11 is about.

---

## ADR-004 — Local DDL harness stands in for Liquibase, outside the package

**Status:** accepted.

**Context.** Rule 1: this repo never issues `CREATE TABLE`. But the local test suite
needs tables to exist, and repo 1's Liquibase cannot run against a laptop's Delta
warehouse.

**Decision.** `tools/local_ddl.py` renders `CREATE TABLE` from the same `TableSpec`
objects preflight validates against, and lives **outside `src/pipelines`**. It is
imported only by `tests/conftest.py` and `tools/run_local_pipeline.py`. Nothing that
ships in the wheel can create a table, and a CI grep enforces it.

This preserves the rule's actual purpose — pipeline code must never paper over a
missing migration — while letting the harness create the fixture it is testing against.

---

## ADR-005 — Spark 4.0 / Delta 4.0 for local tests

**Status:** accepted.

**Context.** `AGENTS.md` §3 says PySpark and Delta versions come from the DBR
serverless runtime. Local test containers here ship JDK 21 only; Spark 3.5 supports
JDK 17 and is not supported on 21.

**Decision.** Pin `pyspark==4.0.1` / `delta-spark==4.0.0` in the `dev` extra. These are
**dev dependencies only** — the wheel declares no PySpark dependency at all, because
installing PySpark into a Databricks job shadows the runtime build.

**Reversal.** When the serverless runtime's Spark version is confirmed, pin the dev
extra to match and provide a matching JDK.

## ADR-006 — The Databricks job is owned by this repo's bundle, not by repo 2

**Status:** accepted here, **not yet actioned in repo 2.**

**Context.** Two repos currently create a Databricks job for the same pipeline:

* repo 2, `modules/databricks/main.tf` → `databricks_job.daily`
* repo 4, `databricks.yml` → `edgar-medallion-${bundle.target}`

That is a direct breach of global law 2 (one owner per object; "jobs: Terraform (repo 2)").
The two designs were never reconciled — repo 2 also publishes
`/edgar-lakehouse/dbx/job_id` described as "consumed by repo 4 to update the job's wheel
version", which is the *other* model, where repo 2 owns the job and repo 4 only bumps a
pin. Nobody noticed because neither has ever been applied. On first apply they would not
conflict or error; they would simply both exist, and two jobs would run the same pipeline
against the same tables.

**Decision.** The job belongs to **repo 4's Databricks Asset Bundle**.

The job definition is not stable infrastructure — it is the task graph, the entrypoints,
and the wheel version, all of which change whenever this repo's code changes. Owning it
here keeps the job version-locked to the code it runs, so a task rename is one PR rather
than two in dependency order. It also makes `databricks bundle run` available for manual
and CI runs, which is the idiomatic Databricks path.

Repo 2 keeps everything whose lifecycle is genuinely slower: catalog, schemas, volumes,
grants, and the SSM interface.

**Consequences — repo 2 must change, and this repo cannot do it (law 11):**

1. remove `databricks_job.daily` from `modules/databricks`;
2. remove the `/edgar-lakehouse/dbx/job_id` parameter, which has no consumer under this
   model;
3. keep `databricks_grants` — the bundle deploys a job, it does not grant itself access.

Until that lands, applying repo 2 and deploying this bundle produces two jobs. Repo 2
has an open PR at the time of writing, so this is recorded rather than patched across the
boundary.

**Also unreconciled, same root cause.** Two of the three SSM keys this repo reads are not
published by repo 2:

| repo 4 reads | repo 2 publishes |
|---|---|
| `/edgar-lakehouse/dbx/landing_volume` | `/edgar-lakehouse/dbx/volume_path` |
| `/edgar-lakehouse/dbx/checkpoint_root` | *nothing* |
| `/edgar-lakehouse/s3/serving_bucket` | matches |

Invisible today because config resolves `env var → SSM → fail` and the env vars shadow
SSM in every path currently exercised. It would surface the first time the scheduled job
runs without them.

**Reversal.** If job definitions ever need to be reviewed through the same approval path
as IAM and buckets, move ownership back to Terraform and reduce this repo to publishing a
wheel plus a version pin.
