# Usage: make <target>. All targets use the project virtualenv in .venv.
PYTHON ?= python3.11
VENV   := .venv
PY     := $(VENV)/bin/python
export PYTHONPATH := src

ROWS ?= 1000000
SEED ?= 42

.DEFAULT_GOAL := help
.PHONY: help setup sample generate-large clean

help:                       ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | expand -t 24

setup:                      ## create venv and install pinned dependencies
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

sample:                     ## (re)generate the small committed sample dataset (150 rows)
	$(PY) -m ecom_pipeline.data_generator --rows 150 --seed $(SEED) --out data/sample

generate-large:             ## generate a large dataset: make generate-large ROWS=1000000
	$(PY) -m ecom_pipeline.data_generator --rows $(ROWS) --seed $(SEED) --days 31 \
		--out data/generated/rows_$(ROWS)

clean:                      ## remove generated data and pipeline outputs
	rm -rf data/generated/rows_* data/output spark-warehouse .pytest_cache metastore_db derby.log

.PHONY: run-sample run-sample-baseline run-large
run-sample:                 ## run the pipeline on the committed sample
	$(PY) -m ecom_pipeline.pipeline --config conf/sample.json

run-sample-baseline:        ## same input, baseline (unoptimized) job, separate output dir
	$(PY) -m ecom_pipeline.pipeline --config conf/sample.json --mode baseline \
		--output-dir data/output/sample_baseline/orders --quarantine-dir data/output/sample_baseline/quarantine \
		--metrics-dir data/output/sample_baseline/run_metrics

run-large:                  ## run on data/generated/rows_1000000 (run `make generate-large` first)
	$(PY) -m ecom_pipeline.pipeline --config conf/large.json

.PHONY: test test-fast
test:                       ## full pytest suite (~2 min, local Spark)
	$(PY) -m pytest

test-fast:                  ## generator tests only (no Spark)
	$(PY) -m pytest tests/test_data_generator.py

.PHONY: benchmark benchmark-quick
benchmark:                  ## baseline vs optimized on the 1M dataset -> docs/performance_report.md
	@test -d data/generated/rows_$(ROWS)/input || $(MAKE) generate-large ROWS=$(ROWS)
	$(PY) -m ecom_pipeline.benchmark --input-dir data/generated/rows_$(ROWS)/input --iterations 3

benchmark-quick:            ## smoke-run the benchmark on the sample (numbers are meaningless)
	$(PY) -m ecom_pipeline.benchmark --input-dir data/sample/input --iterations 1 --warmup 0 \
		--report data/output/benchmark_quick.md --results-json data/output/benchmark_quick.json

.PHONY: reconcile reconcile-large
reconcile:                  ## fresh sample run + reconciliation report -> docs/reconciliation_report.md
	rm -rf data/output/sample data/output/sample_staging
	$(PY) -m ecom_pipeline.pipeline --config conf/sample.json
	$(PY) -m ecom_pipeline.reconcile --config conf/sample.json --expected-dir data/sample/expected

reconcile-large:            ## reconcile the 1M run (after `make run-large`)
	$(PY) -m ecom_pipeline.reconcile --config conf/large.json \
		--expected-dir data/generated/rows_$(ROWS)/expected \
		--report data/output/reconciliation_large.md --results-json data/output/reconciliation_large.json
