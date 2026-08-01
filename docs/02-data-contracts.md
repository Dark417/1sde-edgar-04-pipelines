# 02 — Data contracts

> **Provenance:** reconstructed in repo 4. See `docs/README.md`. Repo 1 owns the
> authoritative copy once published. The machine-readable form of everything below is
> `src/pipelines/contracts/` — if this document and that package disagree, the package
> is what runs, and the disagreement is a bug in one of them.

Project-wide invariants that apply to every table here:

* **`cik` is a `STRING`, zero-padded to 10.** Never an int, in any repo, at any layer
  after bronze. Bronze keeps it raw because bronze keeps everything raw.
* **`accession_number` is `##########-##-######`** — 10 digits, 2 digits, 6 digits.
* Timestamps are UTC. Dates are dates, not timestamps.
* Money is `DECIMAL(38,6)`. Never `DOUBLE`: XBRL values reach 10^12 and binary
  floating point silently loses cents at that magnitude, which manufactures
  restatements that never happened.

---

## 1. Landing envelope

Repo 3 never writes a bare API response. Every landing record is one line of
newline-delimited JSON in this shape:

```json
{
  "envelope_version": "1",
  "source_system":    "sec_edgar",
  "stream":           "company_concept",
  "resource_id":      "0000320193/us-gaap/Revenues",
  "logical_date":     "2026-07-31",
  "batch_id":         "company_concept-2026-07-31",
  "fetched_at":       "2026-08-01T07:54:10Z",
  "request_url":      "https://data.sec.gov/api/xbrl/companyconcept/CIK0000320193/us-gaap/Revenues.json",
  "http_status":      200,
  "content_sha256":   "…",
  "payload_json":     "{…}"
}
```

| Field | Type | Notes |
|---|---|---|
| `envelope_version` | STRING | Only `"1"` exists. Bronze **reads** it and fails on an unknown value rather than assuming. |
| `source_system` | STRING | Always `sec_edgar`. |
| `stream` | STRING | One of `filing_index`, `company_submissions`, `company_concept`. |
| `resource_id` | STRING | Identifies the fetched resource within the stream. |
| `logical_date` | STRING | `YYYY-MM-DD`. The business date, not the fetch date. |
| `batch_id` | STRING | `<stream>-<logical_date>`. Deterministic — derived, never generated. |
| `fetched_at` | STRING | ISO-8601 UTC instant of the HTTP response. |
| `request_url` | STRING | The exact URL. Reproducing a row must not require guessing. |
| `http_status` | INT | |
| `content_sha256` | STRING | Over `payload_json` as written. |
| `payload_json` | STRING | The response body, **as a JSON string**, not a nested object. |

`payload_json` is a string on purpose. The three streams disagree about payload shape,
and two of them are deeply nested documents whose shape the SEC changes without
notice. A nested object would force one union schema across all three and make every
shape change a table migration.

### 1.1 Landing layout

```
<root>/<stream>/logical_date=YYYY-MM-DD/part-#####.json
```

Identical for `s3://`, `/Volumes/…` and a local directory, so the same code reads all
three. Partition value equals the envelope's own `logical_date` — a backfill that
pulls three index days lands three partitions.

---

## 2. Bronze

Append-only. No `UPDATE`, no `DELETE`, no dedup beyond the file checkpoint.

### 2.1 The six metadata columns — on every bronze table

| Column | Type | Notes |
|---|---|---|
| `_ingest_batch_id` | STRING | From the envelope. Deterministic. |
| `_ingest_ts` | TIMESTAMP | When the row was appended. |
| `_source_file` | STRING | `_metadata.file_path` of the landing object. |
| `_source_system` | STRING | `sec_edgar`. |
| `_envelope_version` | STRING | The version the row was parsed as. |
| `_rescued_data` | STRING | Auto Loader rescued columns. **Non-null ⇒ WARN.** |

`_rescued_data` non-null is never a silent pass. `bronze_rescued_row_count` is emitted
per stream, and a non-zero value moves job status to WARN.

### 2.2 `bronze.filing_index_raw` — passthrough

Columns: `logical_date DATE`, `resource_id`, `fetched_at TIMESTAMP`, `form_type`,
`company_name`, `cik`, `date_filed`, `accession_number`, `file_name` + the six.

`cik` and `date_filed` are **raw text here** — untyped, unpadded. Typing happens in
silver so that a value that fails to parse can be quarantined with its original bytes
visible.

### 2.3 `bronze.company_submissions_raw` — opaque payload

Columns: `logical_date DATE`, `resource_id`, `fetched_at`, `cik`,
`payload_json STRING` + the six.

### 2.4 `bronze.company_concept_raw` — opaque payload

Columns: `logical_date DATE`, `resource_id`, `fetched_at`, `cik`, `taxonomy`, `tag`,
`payload_json STRING` + the six.

**Acceptance:** re-processing a landing file adds zero rows.

---

## 3. Silver

Every write is a `MERGE` on the business key. Never `overwrite`, never `append`.

Lineage columns on every silver table:

| Column | Type | Notes |
|---|---|---|
| `_first_seen_ts` | TIMESTAMP | Written on INSERT. **Never updated.** |
| `_last_seen_ts` | TIMESTAMP | Written on INSERT and on every matching MERGE. |
| `_ingest_batch_id` | STRING | Batch that last touched the row. |
| `_source_file` | STRING | Landing object the row was last derived from. |

### 3.0 Quarantine tables

One shape for all of them, so DQ execution stays domain-agnostic:

`_dq_record_id`, `_dq_run_id`, `_dq_check_name`, `_dq_failure_reason`,
`_quarantined_at`, `_source_table`, `_source_file`, `_ingest_batch_id`, `record_json`.

`record_json` is the rejected record verbatim. A quarantine row you cannot replay is a
log line that costs storage.

`_dq_record_id` = `sha2(source_table | check | record_json)` — **deliberately not over
`run_id`.** It is the business key quarantine is MERGEd on, so replaying a batch
refreshes the row rather than appending a second copy. Without it, quarantine would be
the one place in silver where "run it twice" changes the answer, and every "how bad is
the data" number would double on replay.

### 3.1 `silver.filing` — business key `accession_number`

| Column | Type | Derivation |
|---|---|---|
| `accession_number` | STRING NOT NULL | Normalized to `##########-##-######`; an 18-digit bare form is hyphenated, anything else is quarantined. |
| `cik` | STRING NOT NULL | Zero-padded to 10. |
| `company_name` | STRING | Trimmed. |
| `form_type` | STRING NOT NULL | Upper-cased, trimmed. |
| `base_form_type` | STRING NOT NULL | `form_type` with a trailing `/A` removed. `10-K/A → 10-K`, `S-1/A → S-1`, `10-K → 10-K`. |
| `is_amendment` | BOOLEAN NOT NULL | `form_type` ends in `/A`. |
| `filed_date` | DATE NOT NULL | Parsed from `YYYYMMDD` or `YYYY-MM-DD`. |
| `primary_doc_url` | STRING | `https://www.sec.gov/Archives/<file_name>`. |
| `logical_date` | DATE NOT NULL | Landing partition last seen in. |

Lowercase input is accepted and upper-cased; `base_form_type` must be correct for
`10-K`, `10-K/A`, `8-K`, `S-1/A` and lowercase variants.

**DQ**

| Check | Severity |
|---|---|
| `filing_accession_format` | reject |
| `filing_cik_zero_padded` | reject |
| `filing_form_type_present` | reject |
| `filing_filed_date_present` | reject |
| `filing_filed_date_not_after_logical_date` | reject |
| `filing_company_name_present` | warn |

**Acceptance**
* 🔴 Run twice → identical row count **and** identical `_first_seen_ts`.
* A malformed accession lands in `filing_quarantine`, not `filing`.

### 3.2 `silver.company` — SCD-2, natural key `cik`

Parsed from `company_submissions_raw.payload_json`.

Tracked columns (a change in any of these opens a new version): `company_name`, `sic`,
`sic_description`, `ein`, `entity_type`, `state_of_incorporation`, `fiscal_year_end`,
`tickers ARRAY<STRING>`, `exchanges ARRAY<STRING>`, `former_names ARRAY<STRING>`.

SCD-2 columns: `valid_from DATE`, `valid_to DATE` (null while current),
`is_current BOOLEAN`, `_hash_diff STRING`.

> **Sort array columns before hashing `_hash_diff`.** Source array ordering is not
> stable; unsorted hashing generates a spurious new version every single day and the
> dimension explodes. This is the most commonly re-introduced bug in the repo.

Grain is one version per `cik` per `logical_date`. A second change within the same
logical date updates that day's version in place, because closing it would produce
`valid_to = valid_from - 1` — a negative-length interval that cannot be point-in-time
queried.

**DQ**

| Check | Severity |
|---|---|
| `company_cik_zero_padded` | reject |
| `company_valid_from_present` | reject |
| `company_name_present` | warn |
| `company_exactly_one_current` | **reject_batch** |
| `company_no_overlapping_versions` | **reject_batch** |

**Acceptance** — all four SCD-2 cases:
(a) no change → 0 new rows; (b) tracked column changed → old row closed
(`valid_to = logical_date - 1`, `is_current = false`) and a new row inserted;
(c) 🔴 array column reordered with the same members → **0 new rows**;
(d) run twice → identical result.

### 3.3 `silver.financial_fact` — the bitemporal table

Business key: **`(cik, taxonomy, concept_tag, unit, period_start, period_end,
period_type, accession_number)`**.

Derived by exploding `companyconcept.units`, which is a map of unit → array of fact
objects.

| Column | Type | Derivation |
|---|---|---|
| `cik` | STRING NOT NULL | Zero-padded to 10. |
| `taxonomy`, `concept_tag` | STRING NOT NULL | As reported. |
| `concept_canonical` | STRING | Mapped; **null when unmapped, and the row is kept**. |
| `unit` | STRING NOT NULL | Map key: `USD`, `shares`, `USD/shares`. |
| `period_start` | DATE | `start`. **Null for instant facts.** |
| `period_end` | DATE NOT NULL | `end`. |
| `period_type` | STRING NOT NULL | `instant` when `start` is null, else `duration`. |
| `accession_number` | STRING NOT NULL | `accn`. **Part of the grain — do not collapse.** |
| `value` | DECIMAL(38,6) | `val`. |
| `decimals` | INT | XBRL `decimals`; negative means rounded to `10^-decimals`. **Always null from this source — see below.** |
| `fiscal_year`, `fiscal_period` | INT, STRING | `fy`, `fp`. See the warning below. |
| `form_type` | STRING | `form`. |
| `filed_date` | DATE NOT NULL | `filed`. |
| `frame` | STRING | `frame`, when present. |

Unmapped tags are kept with a null `concept_canonical` and counted as a WARN.
Discarding them would make adding a concept later require a full bronze replay.

> **`decimals` is not in the payload.** The `companyconcept` response carries exactly
> `start`, `end`, `val`, `accn`, `fy`, `fp`, `form`, `filed`, `frame` — verified
> against the live API. There is no `decimals`. The column stays in the contract
> because the raw XBRL instance documents do carry it and a future ingest path can
> populate it, but every row sourced from `companyconcept` has `decimals = NULL`.
> ADR-002 explains what that costs the restatement comparison and how it is handled.

> **`fy` / `fp` describe the filing, not the fact.** They are the fiscal-year and
> fiscal-period *focus of the document the fact appeared in*. A FY2020 figure restated
> in the FY2022 10-K carries `fy = 2022`. They are stored for traceability and are
> **never** used for period identity — period identity is `(period_start, period_end,
> period_type)` and nothing else. Grouping by `fy`/`fp` would split one period into
> two groups and hide every restatement.

**DQ**

| Check | Severity |
|---|---|
| `fact_cik_zero_padded`, `fact_accession_format`, `fact_unit_present`, `fact_value_present`, `fact_period_end_present`, `fact_period_type_valid` | reject |
| `fact_period_order` (`period_start IS NULL OR period_end >= period_start`) | reject |
| `fact_concept_mapped` | warn |
| `fact_grain_unique` | **reject_batch** |

The null branch of `fact_period_order` is load-bearing, not defensive: instant facts
have no `period_start` and must pass.

**Acceptance**
* 🔴 The same `(cik, concept, period)` reported by two accessions produces **2 rows,
  not 1**. If this fails, restatement detection is impossible.
* Instant facts do not fail `fact_period_order`.

---

## 4. Gold

Rebuilt from silver each run. `_generated_at` and `_run_id` on every table.

### 4.1 `gold.financials_current`

The winning assertion per `(cik, concept_canonical, unit, period_start, period_end,
period_type)`: latest `filed_date`, ties broken by the greater `accession_number` so
the choice is deterministic. Carries `assertion_count` and `was_restated`.

### 4.2 `gold.restatement_event` — the differentiator

Self-join `silver.financial_fact` within identical
`(cik, concept_canonical, unit, period_start, period_end, period_type)`, ordered by
`filed_date`, comparing **consecutive** assertions.

```sql
WHERE abs(later.value - earlier.value)
      > greatest(abs(earlier.value) * 1e-6, 1e-6)
```

* `delta_abs` = `restated_value - original_value`
* `delta_pct` = `delta_abs / abs(original_value)` — **null, not an exception, when
  `original_value = 0`**
* `days_to_restatement` = `datediff(restated_filed_date, original_filed_date)`
* `restatement_id` = sha2 over the grain plus both accession numbers — deterministic,
  so a re-run merges rather than duplicates
* `materiality_band`: `immaterial` <1%, `notable` 1–5%, `material` >5%

`materiality_band` is a **product heuristic, not an accounting standard.** That
sentence carries through to the docstring and to the UI.

> **See ADR-002 in `10-decisions.md`.** A fixed `1e-6` relative tolerance is necessary
> but not sufficient, and real EDGAR data proves it: Dream Finders Homes (CIK
> 0001825088) reported FY2020 `NetIncomeLoss` as `79,093,455` in accession
> `0001140361-22-009752` and as `79,093,000` in `0001825088-23-000011`. That is the
> same figure rounded to the nearest thousand — a relative difference of `5.8e-6`,
> almost six times the tolerance, and the literal expression above flags it as a
> restatement. The implementation therefore adds a **precision-aware floor** to the
> same comparison: one reporting unit at the coarser of the two values' inferred
> precisions, capped at 0.1% of the value. The literal expression is retained and
> tested as `decimals_aware=False`.

**Acceptance**
* Restatement fixture (10-K then 10-K/A with a changed value) → exactly one row, with
  correct `delta_pct` and `days_to_restatement`.
* 🔴 Rounding-only fixture (same value, different `decimals` scale) → **zero rows**.
* Different-`unit` fixture → zero rows.
* `delta_pct` null, not an exception, when `original_value = 0`.

### 4.3 `gold.filing_activity_daily`

Per `(filed_date, base_form_type)`: `filing_count`, `amendment_count`,
`distinct_cik_count`.

### 4.4 `gold.company_profile`

Current `silver.company` attributes plus `filing_count`, `first_filed_date`,
`last_filed_date`, `restatement_count`.

---

## 5. Serving export

One Parquet **object** (not a directory of parts) per gold table:

```
s3://<serving>/v1/{table}/data.parquet
s3://<serving>/v1/_manifest.json
```

Overwrite, never append — the export is a projection of gold, and an appended export
diverges from it on the first re-run.

```json
{
  "manifest_version": "1",
  "generated_at": "2026-08-01T08:00:00Z",
  "run_id": "export-2026-07-31-…",
  "logical_date": "2026-07-31",
  "gold_max_filed_date": "2026-07-31",
  "tables": [
    {"name": "financials_current", "path": "v1/financials_current/data.parquet",
     "row_count": 1234, "bytes": 45678, "sha256": "…"}
  ]
}
```

**`gold_max_filed_date` equals `max(filed_date)` in `silver.filing`.** Repo 5's
`/health` endpoint reports freshness from it, so it must describe the source data, not
the export time.

Table order in `tables` is fixed, so identical data produces an identical manifest.

**Acceptance:** manifest freshness matches silver; export is idempotent.

---

## 6. Concept mapping

`concept_canonical` values and their source tags (lower `preference` wins when a filer
reports several):

| Canonical | Tags (preference order) |
|---|---|
| `revenue_total` | `RevenueFromContractWithCustomerExcludingAssessedTax`, `…IncludingAssessedTax`, `Revenues`, `SalesRevenueNet` |
| `net_income` | `NetIncomeLoss`, `ProfitLoss` |
| `operating_income` | `OperatingIncomeLoss` |
| `gross_profit` | `GrossProfit` |
| `assets_total` | `Assets` |
| `liabilities_total` | `Liabilities` |
| `equity_total` | `StockholdersEquity`, `…IncludingPortionAttributableToNoncontrollingInterest` |
| `cash_and_equivalents` | `CashAndCashEquivalentsAtCarryingValue` |
| `eps_basic` / `eps_diluted` | `EarningsPerShareBasic` / `EarningsPerShareDiluted` |
| `shares_outstanding` | `dei:EntityCommonStockSharesOutstanding` |

The tie-break is explicit and total so that the winning tag does not depend on join
order.
