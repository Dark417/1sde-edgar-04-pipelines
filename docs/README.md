# docs/ — provenance notice

> **Read this before treating anything in this directory as authoritative.**

`AGENTS.md` §0 names `docs/00-design-doc.md` and `docs/02-data-contracts.md` as the
authoritative specification for this repo, and `AGENTS.global.md` says both are
**copied from repo 1**, which is their source of truth.

Repo 1 (`1sde-databricks-edgar-01-contracts`) has not published them yet, and repo 4
cannot be built or tested without them. So the copies here were **reconstructed in this
repo** from `AGENTS.md` §§1–11 plus the observed shape of the live SEC EDGAR API. They
are complete and self-consistent, and the code and tests in this repo are written
against them — but they are a *reconstruction*, not a copy.

| File | Status |
|---|---|
| `00-design-doc.md` | Reconstructed here. Replace with repo 1's copy when published. |
| `02-data-contracts.md` | Reconstructed here. Replace with repo 1's copy when published. |
| `03-local-test-harness.md` | Written here. Owned by this repo; not a repo 1 document. |
| `10-decisions.md` | Written here. ADRs for choices this repo had to make. |

**When repo 1 publishes:** replace `00-` and `02-` with repo 1's copies, install
`edgar_lakehouse_contracts==<version>`, and run
`pytest tests/test_contract_compat.py`. That test diffs
`src/pipelines/contracts/` (the in-repo mirror) against the published wheel and fails
on any drift — see ADR-001 in `10-decisions.md`. Per `AGENTS.global.md`, if the
published docs disagree with `AGENTS.md`, **stop and report the conflict** rather than
picking a side.
