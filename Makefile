.PHONY: install lint test test-fast test-spark local-run fetch-data verify-workspace build deploy clean

VENV ?= .venv
PY   ?= $(VENV)/bin/python
LANDING   ?= data/landing
WAREHOUSE ?= .local/warehouse
TARGET    ?= dev

install:
	python -m venv $(VENV)
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -e ".[dev]"

lint:
	$(VENV)/bin/ruff check .
	$(VENV)/bin/mypy src/pipelines

test-fast:
	$(PY) -m pytest -m "not spark" -q

test-spark:
	$(PY) -m pytest -m spark -q

test:
	$(PY) -m pytest -q

# Needs network and EDGAR_USER_AGENT. The SEC requires a declared User-Agent and
# blocks by IP; the fetcher rate-limits itself to stay inside the fair-access policy.
fetch-data:
	$(PY) tools/fetch_test_data.py --out $(LANDING) --max-index-rows 120 \
		--inject-bad-accession --inject-rescue

local-run:
	$(PY) tools/run_local_pipeline.py --landing $(LANDING) --warehouse $(WAREHOUSE)

# --- Databricks. See docs/04-databricks-deploy.md.
# Needs DATABRICKS_HOST and DATABRICKS_TOKEN (copy .env.example to .env).

# Read-only prerequisite check. Run this BEFORE deploy: it is far cheaper to find a
# missing migration here than during a job run on a Free Edition quota.
verify-workspace:
	$(PY) tools/dbx_verify.py

build:
	$(PY) -m pip install -q build
	$(PY) -m build --wheel

# Uploads the wheel and the job definition. Does NOT trigger a run -- `bundle run`
# burns quota, and an exhausted quota shuts compute down for the rest of the day.
deploy: verify-workspace build
	databricks bundle validate -t $(TARGET)
	databricks bundle deploy -t $(TARGET)

clean:
	rm -rf .local .pytest_cache .mypy_cache .ruff_cache dist build
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
