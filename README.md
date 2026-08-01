# 1sde-databricks-edgar-04-pipelines

Repo 4 of 5 in the **edgar-lakehouse** project: the medallion transform.
`landing → bronze → silver → gold → Parquet serving export`, as Databricks Jobs on
Free Edition.

The flagship feature is **restatement detection** — automatically surfacing where a
company later asserted a different value for a period it had already reported.

## Layout

```
src/pipelines/
  config.py            L0  settings: env var -> SSM -> fail naming the key
  session.py           L0  SparkSession accessor (local or Databricks)
  contracts/           L0  schemas, DQ registry, names, concepts   (mirror -- ADR-001)
  framework/           L1  generic: autoloader, dq, merge, preflight, metrics, delta_ops
  bronze/              L2  landing -> bronze, append only
  silver/              L3  typing, dedup, MERGE, SCD-2, DQ, quarantine
  gold/                L4  marts incl. restatement_event
  export/              L5  gold -> Parquet -> S3 + manifest
  entrypoints/         L6  one thin wrapper per job task
tools/                     dev harness: fetch test data, local DDL, local pipeline run
data/landing/              ~1.3 MB of real EDGAR responses, committed as test data
docs/                      design doc, data contracts, local harness, ADRs
```

**Layer rule:** L1 knows nothing about EDGAR. L2–L5 hold domain logic and call down.
Entrypoints contain no logic.

## Run it locally

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
python tools/run_local_pipeline.py --landing data/landing --warehouse .local/warehouse
```

That runs the whole medallion against the committed EDGAR subset — no Databricks, no
network. See **[docs/03-local-test-harness.md](docs/03-local-test-harness.md)** for
what it proves, how to refresh the data, and every way the harness differs from the
workspace.

```bash
pytest -m "not spark"    # fast suite
pytest -m spark          # local SparkSession + Delta
ruff check . && mypy src/pipelines
```

## The rules that matter most

Full list in `AGENTS.md` §5. The four that break things silently:

1. **This repo never issues `CREATE TABLE`.** Liquibase (repo 1) owns DDL. A missing
   table fails preflight with the name of the changeset that creates it. Enforced by
   `tests/test_repo_guards.py`.
2. **`_first_seen_ts` is set on INSERT and never updated.** Reversing it with
   `_last_seen_ts` turns "when did we first see this" into "when did the job last run",
   and nothing fails.
3. **Sort array columns before hashing `_hash_diff`.** Source array ordering is not
   stable; unsorted hashing opens a new SCD-2 version every day and the dimension
   explodes.
4. **Restatement comparison uses a tolerance, never `!=`.** Filers re-report the same
   figure at a coarser scale; equality makes the table pure noise.

## Read the docs before changing behavior

| Doc | What is in it |
|---|---|
| [`docs/00-design-doc.md`](docs/00-design-doc.md) | Why the shape is the shape; Free Edition constraints |
| [`docs/02-data-contracts.md`](docs/02-data-contracts.md) | Every table, column, DQ check and acceptance criterion |
| [`docs/03-local-test-harness.md`](docs/03-local-test-harness.md) | The local path, the test data, the differences |
| [`docs/10-decisions.md`](docs/10-decisions.md) | ADRs, including where the implementation deviates from `AGENTS.md` and why |
| [`docs/README.md`](docs/README.md) | **Provenance:** which docs are reconstructed rather than copied from repo 1 |

## Status

Built and verified locally end to end. Not yet run against a Databricks workspace —
`AGENTS.md` §9.2 onward (workspace prerequisites, the manual re-run checks, bundle
deploy) is still outstanding, and repos 1–3 have to land first.
