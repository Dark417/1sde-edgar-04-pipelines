# 05 — Layer signature, versioning, index columns and views

Status: **design agreed, implementation in progress** · Author: dark417 · 2026-08-01

This document covers the second build of repo 4. Read [00-design-doc.md](00-design-doc.md)
first for the medallion shape and [02-data-contracts.md](02-data-contracts.md) for the
table-by-table contract.

---

## 1. Why this exists: the contract break

Repo 4's first build was written against an envelope and a set of table schemas that
**no other repo produces**. This was found on 2026-08-01 by comparing repo 1's published
contract against repo 4's committed fixtures, and confirmed against the live workspace.

### 1.1 The envelope disagreed on every field

| repo 3 wrote | repo 4's bronze read |
| --- | --- |
| `_stream` | `stream` |
| `_logical_date` | `logical_date` |
| `_batch_id` | `batch_id` |
| `_fetched_at` | `fetched_at` |
| `_source_url` | `request_url` |
| `_schema_version` | `envelope_version` |
| `payload` (nested object) | `payload_json` (string) |
| — | `source_system`, `resource_id`, `http_status`, `content_sha256` |

Zero of eleven names matched. The landing path disagreed too (`dt=` vs `logical_date=`),
as did the filename and the `batch_id` format.

**The failure mode is what makes this serious.** Auto Loader does not raise on unknown
fields; it routes them into `_rescued_data`. Bronze would have ingested every file
"successfully" and produced a table of all-NULL rows. The break would have surfaced in
silver, or in a demo.

### 1.2 All thirteen tables disagreed with the deployed DDL

Repo 1's Liquibase changelogs are the tables that actually exist in the workspace. Every
one of the thirteen drifted from what repo 4's code writes — `name` vs `company_name`,
`fy`/`fp` vs `fiscal_year`/`fiscal_period`, `_batch_id` vs `_ingest_batch_id`, and three
quarantine tables built on an entirely different model (repo 1 mirrored the source
columns; repo 4 uses a generic `record_json` envelope).

### 1.3 Why the contract gate did not catch it

```
edgar_lakehouse_contracts installed: False
provenance: mirror
discrepancies: []
```

`verify_against_published()` returns `[]` when the wheel is absent, and repo 4 never
declared the wheel as a dependency. The `contract-compat` CI job was green because it was
comparing the mirror against nothing. **A gate that cannot fail is not a gate** — this is
the root cause, and it is fixed first (F-1) so the rest cannot regress.

### 1.4 Resolution

Repo 4's definitions are richer and are what its passing tests exercise, so they are
promoted to the contract rather than discarded:

- repo 1's `LandingEnvelope` becomes the eleven-field shape, with `content_sha256`
  derived at construction — **done**, repo 1 green.
- repo 1's `landing_path` uses `logical_date=` — **done**.
- repo 3 emits it, including a new `resource_id` per stream and `accession_number`
  extracted from the index `file_name` — **done**, repo 3 green.
- repo 1's Liquibase DDL is rewritten to match repo 4's specs (F-2).

All thirteen tables were verified empty before deciding this, so the DDL is replaced by
drop-and-recreate rather than a column-by-column migration:

```
bronze.company_concept_raw 0   silver.company        0   gold.company_profile       0
bronze.company_submissions_raw 0   silver.filing     0   gold.financials_current    0
bronze.filing_index_raw    0   silver.financial_fact 0   gold.filing_activity_daily 0
                                                         gold.restatement_event     0
```

---

## 2. The three-layer signature

The medallion layers are currently a naming convention. This build makes the layer
**manifest in the schema**: every table carries the audit columns of its layer and
nothing else, so a table's layer is readable from its columns alone and a table that
drifts out of its layer's shape fails a test rather than a review.

`TableSpec.layer` already exists. The signature makes it enforceable.

### 2.1 Bronze — "what arrived, and from where"

Append-only. Never modified, never deduped, never typed.

| column | type | meaning |
| --- | --- | --- |
| `_ingest_batch_id` | STRING | repo 3's deterministic batch id |
| `_ingest_ts` | TIMESTAMP | when *this pipeline* wrote the row |
| `_source_file` | STRING | landing file the row came from |
| `_source_system` | STRING | `sec_edgar` |
| `_envelope_version` | STRING | read, not assumed — an unknown version fails loudly |
| `_content_sha256` | STRING | **new**: integrity, and re-fetch detection without diffing |
| `_rescued_data` | STRING | Auto Loader's catch-all; **non-null is a DQ warn** (F-3) |

`_rescued_data` being silently ignored is precisely how §1.1 would have gone unnoticed.
It becomes a monitored signal.

### 2.2 Silver — "the current truth, and how it changed"

Typed, deduped, MERGEd, quarantined on failure.

| column | type | meaning |
| --- | --- | --- |
| `<entity>_sk` | STRING | **new**: deterministic surrogate key (§4) |
| `version_number` | INT | **new**: 1-based, increments per natural key |
| `valid_from` | DATE | inclusive |
| `valid_to` | DATE | exclusive; NULL = current |
| `is_current` | BOOLEAN | exactly one true per natural key — a `reject_batch` invariant |
| `_hash_diff` | STRING | sha256 over tracked columns; drives change detection |
| `_first_seen_ts` | TIMESTAMP | never in an UPDATE set — this is what makes re-runs idempotent |
| `_last_seen_ts` | TIMESTAMP | refreshed on every sighting |
| `_ingest_batch_id`, `_source_file` | STRING | lineage back to bronze |

### 2.3 Gold — "a published answer, and which inputs produced it"

Rebuilt, not merged. Every gold table is a pure function of silver at a point in time.

| column | type | meaning |
| --- | --- | --- |
| `_generated_at` | TIMESTAMP | when this answer was computed |
| `_run_id` | STRING | the `JobRun` that produced it |
| `_source_version` | BIGINT | **new**: Delta version of the silver input |

`_source_version` is the reproducibility hook: it turns "gold looked wrong last Tuesday"
into `SELECT * FROM silver.company VERSION AS OF <n>`.

### 2.4 Enforcement

`tests/test_layer_signature.py` asserts, for every table in `TABLES`:

- it carries **all** of its layer's signature columns, with the declared types;
- it carries **no** signature column belonging to another layer (no `valid_from` in gold);
- gold tables carry no `_hash_diff` and silver tables carry no `_rescued_data`.

---

## 3. Versioning — keep the old version

### 3.1 Dimensions: SCD Type 2

`silver.company` already merges SCD-2. This build extends it and adds the same treatment
to `silver.filing`.

A new version opens when any **tracked** column changes. Tracked columns are declared per
table, not inferred — inferring means a new upstream field silently rewrites history.

```
cik        version_number  valid_from   valid_to     is_current  company_name
0000320193 1               2026-07-29   2026-07-31   false       APPLE COMPUTER INC
0000320193 2               2026-07-31   NULL         true        Apple Inc.
```

Invariants, enforced as `reject_batch` (a violation fans out every downstream join, so a
single bad row is fatal, and the merge rolls back):

1. exactly one `is_current` row per natural key;
2. no two versions of one key have overlapping `[valid_from, valid_to)`;
3. `version_number` is dense and 1-based per key;
4. the `is_current` row is the one with the highest `version_number`.

`silver.filing` gets SCD-2 on `form_type`, `company_name` and `primary_doc_url`. Filings
are amended, and an amendment that silently overwrites the original destroys the
restatement story that `gold.restatement_event` is built on.

### 3.2 Facts: append-with-supersession

`silver.financial_fact` is not a dimension — a restatement is a new assertion about the
same period, not a correction to a row. Both are kept:

| column | meaning |
| --- | --- |
| `assertion_version` | 1-based per `(cik, concept_canonical, period_end, unit)` |
| `superseded_by_accession` | NULL for the current assertion |
| `is_current_assertion` | exactly one true per fact key |

This is what lets `gold.restatement_event` be a straight join rather than a reconstruction
from Delta history.

### 3.3 Delta history is retained, not relied upon

SCD-2 answers "what did we believe about this company over time" — a business question.
Delta time travel answers "what did this table physically contain at version 12" — an
operational one. They are not substitutes.

Retention is set explicitly so the operational answer survives long enough to be useful:

```sql
ALTER TABLE edgar.silver.company SET TBLPROPERTIES (
  delta.logRetentionDuration            = 'interval 90 days',
  delta.deletedFileRetentionDuration    = 'interval 30 days'
);
```

`docs/06-time-travel-runbook.md` documents `DESCRIBE HISTORY` and `VERSION AS OF`.

---

## 4. Index columns

Databricks Free Edition is serverless-only, where `ZORDER` is not available and liquid
clustering is the supported mechanism. Both index features below are therefore chosen for
what the runtime actually supports.

### 4.1 Surrogate keys

Deterministic, not generated. `sha2(concat_ws('|', <natural key parts>), 256)`:

| table | column | derived from |
| --- | --- | --- |
| `silver.company` | `company_sk` | `cik` |
| `silver.filing` | `filing_sk` | `accession_number` |
| `silver.financial_fact` | `fact_sk` | `cik｜concept_canonical｜period_end｜unit｜accession_number` |

Deterministic because an identity column would produce different keys on a re-run,
breaking both the idempotency guarantee and any downstream join that cached one. The
`'|'` delimiter is explicit so `('ab','c')` and `('a','bc')` cannot collide.

For SCD-2 tables the surrogate key identifies the **entity**, not the version; the version
is `(<entity>_sk, version_number)`. This keeps joins from gold simple: join on `_sk`, filter
`is_current`.

### 4.2 Liquid clustering

Chosen from the predicates gold and repo 5 actually issue, not from cardinality alone:

| table | `CLUSTER BY` | why |
| --- | --- | --- |
| `bronze.*_raw` | `logical_date` | every read is a date-bounded incremental slice |
| `silver.company` | `cik` | point lookups and the SCD-2 merge condition |
| `silver.filing` | `filed_date, cik` | gold aggregates by date; repo 5 filters by company |
| `silver.financial_fact` | `cik, period_end` | the shape of every financial query |
| `gold.financials_current` | `cik` | repo 5's primary access path |

At ~750k silver rows clustering changes little measurably; it is declared because the
*layout decision* is part of the contract and because the cost of adding it later, once
tables are large, is a full rewrite.

---

## 5. Views

Views are DDL, so they are **owned by Liquibase in repo 1** — repo 4 never issues
`CREATE VIEW`, and the existing `no CREATE TABLE` repo guard is extended to cover it.

| view | purpose |
| --- | --- |
| `gold.v_company_current` | current SCD-2 row per company; hides `is_current` from consumers |
| `gold.v_filing_current` | current version per filing |
| `gold.v_financials_latest` | current assertion per fact key, restatements excluded |
| `gold.v_restatement_history` | original vs restated assertion, side by side |
| `gold.v_company_timeline` | **the versioning demo**: every SCD-2 version per company, with the changed columns |
| `gold.v_pipeline_health` | last run per layer, row counts, DQ failures, quarantine depth |

`v_company_timeline` exists to make the versioning visible in one query during a demo,
rather than requiring a hand-written `valid_to IS NULL` filter.

---

## 6. Features

| id | feature | acceptance |
| --- | --- | --- |
| F-1 | Contract gate actually fires | wheel is a declared dependency; `provenance() == "published"` in CI; a deliberate mirror edit fails the build |
| F-2 | DDL regenerated from specs | Liquibase YAML is generated from `TABLES`; a test fails if the checked-in YAML and the specs disagree |
| F-3 | Bronze signature + `_rescued_data` monitoring | non-null `_rescued_data` emits a `warn` DQ metric |
| F-4 | Silver SCD-2 on `company` and `filing` | all four invariants enforced; violation rolls back |
| F-5 | `version_number` dense and 1-based | invariant test over a three-version fixture |
| F-6 | Fact assertion versioning | restatement produces two rows; exactly one `is_current_assertion` |
| F-7 | Deterministic surrogate keys | same input twice → identical `_sk`; documented delimiter |
| F-8 | Liquid clustering declared | `DESCRIBE DETAIL` reports the declared clustering columns |
| F-9 | Six gold views | each returns the expected shape against the local harness |
| F-10 | Gold `_source_version` | equals the Delta version of the silver input at build time |
| F-11 | Layer-signature test | every table matches exactly its layer's audit columns |
| F-12 | End-to-end local run | landing → bronze → silver → gold → views, on the committed fixtures, twice, byte-identical |

## 7. Tasks

1. **T-1** — repo 4 declares `edgar-lakehouse-contracts`; make `verify_against_published`
   compare envelope fields as well as tables. *(F-1)*
2. **T-2** — repo 1: add `models.py` + `schemas.py` as the canonical specs; add
   `tools/gen_changelog.py`; regenerate `010-bronze/020-silver/040-gold`; add
   `050-views.yaml` and `060-clustering.yaml`. *(F-2, F-8, F-9)*
3. **T-3** — repo 4 mirror updated to match, with the new signature/versioning columns. *(F-3…F-7)*
4. **T-4** — `framework/scd2.py`: extend `merge_scd2` with `version_number` and the four
   invariants; add `merge_assertions` for facts. *(F-4, F-5, F-6)*
5. **T-5** — `framework/keys.py`: `surrogate_key()` helper; wire into the three silver builds. *(F-7)*
6. **T-6** — gold builds record `_source_version`. *(F-10)*
7. **T-7** — tests: layer signature, SCD-2 invariants, version density, key determinism,
   view shapes, end-to-end twice. *(F-11, F-12)*
8. **T-8** — docs: this file, `06-time-travel-runbook.md`, update `02-data-contracts.md`
   and `10-decisions.md` with ADR-002 (envelope promotion) and ADR-003 (SCD-2 vs time travel).

## 8. Test plan

| level | what | runs where |
| --- | --- | --- |
| contract | envelope field names/types; mirror vs wheel | no Spark, every PR |
| signature | layer audit columns per table | no Spark, every PR |
| unit | key determinism, hash-diff, version arithmetic | no Spark |
| merge | SCD-2 open/close, invariant violation → rollback, assertion supersession | local Spark + Delta |
| view | each view's columns and row shape | local Spark + Delta |
| e2e | fixtures → all four layers, run twice, compare | local Spark + Delta |

The Spark tiers need a JVM and do not run on this machine (no Java); CI covers them.
Everything else runs locally.

## 9. Out of scope

- Backfill or replay tooling — repo 3 owns replay.
- Streaming. Auto Loader runs in `availableNow` batch mode; Free Edition has no DLT.
- Repo 5's API. This build stops at the views it will read.
