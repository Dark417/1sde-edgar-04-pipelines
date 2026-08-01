.PHONY: install lint test test-fast test-spark local-run fetch-data clean

VENV ?= .venv
PY   ?= $(VENV)/bin/python
LANDING   ?= data/landing
WAREHOUSE ?= .local/warehouse

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

clean:
	rm -rf .local .pytest_cache .mypy_cache .ruff_cache dist build
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
