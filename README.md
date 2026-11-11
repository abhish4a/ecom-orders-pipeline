# E-commerce Orders Pipeline (PySpark 3.5, local)

A batch pipeline that ingests e-commerce **order events** (CSV), validates them against
**customers** and **products** dimensions, quarantines bad records with a reason, de-duplicates
to one row per order (latest event wins), and publishes an **idempotent, partitioned Parquet**
fact table with lineage and run metrics. It also includes a reproducible data generator, 63
pytest tests, a baseline vs optimized benchmark with measured results, and an independent
reconciliation report.

```
CSV order events ─► ingest (schema check) ─► validate (rules + RI) ─► transform (dedup) ─► publish (merge by key)
                                                   └─► quarantine (reason, source_file, run_id, raw record)
                                                                         run_metrics JSON ◄─┘
```

Details: [architecture](docs/architecture.md) · [data dictionary](docs/data_dictionary.md) ·
[design & limitations](docs/design.md) · [operations: retry/restart/backfill](docs/operations.md) ·
[performance report](docs/performance_report.md) · [reconciliation report](docs/reconciliation_report.md) ·
[requirements checklist](docs/requirements_checklist.md)

## 1. Setup from a clean environment

Prerequisites:
- **Python 3.10–3.12.** PySpark 3.5 does not support 3.13 yet.
- **Java 17** (8, 11 or 17 are supported; this project was also run on Java 21).
- **make.**

No cloud services or secrets are needed.

```bash
git clone <this repo> ecom-orders-pipeline && cd ecom-orders-pipeline
```
```bash
make setup PYTHON=python3.11      # creates .venv and installs pyspark==3.5.3, pytest==8.3.3
```

Without `make`:

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
```
```bash
export PYTHONPATH=src             # all commands below assume this when not using make
```

## 2. Run

| What | Command | Output |
|---|---|---|
| List all targets | `make` | |
| Regenerate the committed sample (150 rows, seed 42) | `make sample` | `data/sample/` |
| **Sample run** | `make run-sample` | `data/output/sample/{orders,quarantine,run_metrics}` |
| Sample run, baseline job | `make run-sample-baseline` | `data/output/sample_baseline/` |
| Generate a large dataset | `make generate-large ROWS=1000000` | `data/generated/rows_1000000/` (≈118 MB, ≈30 s) |
| **Large run** | `make run-large` | `data/output/large/` (≈40 s on 4 cores) |
| **Benchmark** baseline vs optimized | `make benchmark` | `docs/performance_report.md` (≈12 min) |
| **Tests** | `make test` | 63 tests, ≈2 min |
| Reconciliation (fresh sample run + report) | `make reconcile` | `docs/reconciliation_report.md` |
| Reconcile the large run | `make reconcile-large` | `data/output/reconciliation_large.md` |

Pipeline CLI (every config value can be overridden):

```bash
python -m ecom_pipeline.pipeline --config conf/sample.json \
    [--mode optimized|baseline] [--orders-pattern 'orders/orders_batch_001.csv'] \
    [--input-dir ...] [--output-dir ...] [--quarantine-dir ...] [--metrics-dir ...] \
    [--shuffle-partitions 8] [--run-id my_run] [--fail-at-step validate|stage|publish]
```

Exit code 0 = SUCCESS, 1 = FAILED. Metrics are written in both cases.

Generator CLI:

```bash
python -m ecom_pipeline.data_generator --rows 150 --seed 42 --out data/sample
```
```bash
python -m ecom_pipeline.data_generator --rows 1000000 --seed 42 --days 31 --out data/generated/rows_1000000
```

Inspect results:

```bash
cat data/output/sample/run_metrics/run_metrics.jsonl
```
```bash
.venv/bin/python -c "from pyspark.sql import SparkSession as S; s=S.builder.getOrCreate(); \
s.read.parquet('data/output/sample/quarantine').groupBy('reject_reason').count().show(truncate=False)"
```

Sample SQL: [`docs/sample_queries.sql`](docs/sample_queries.sql). The results are included in the
reconciliation report.

## 3. Results at a glance (measured, see the reports)

| | |
|---|---|
| Sample: 150 input rows | 123 output + 21 rejected + 6 deduped = 150 ✅ |
| 1M rows: 1,000,000 input | 940,330 output + 19,690 rejected + 39,980 deduped ✅ |
| Baseline vs optimized (1M rows, 4 cores, median of 3) | 111.65 s → 23.29 s (**4.79x**), shuffle 657 MB → 241 MB, 3,598 → 31 files, identical output |
| Rerun same input / file-by-file vs all-at-once / crash + retry | identical target, no duplicates (tests) |

## 4. Project layout

```
conf/                    sample.json, large.json (paths + parameters, no secrets)
src/ecom_pipeline/
  data_generator.py      reproducible data + expected-outcome labels (stdlib only)
  config.py              PipelineConfig (JSON + CLI overrides)
  spark_session.py       SparkSession + baseline/optimized settings
  schemas.py             raw / dimension / target / quarantine contracts
  ingest.py              file listing, per-file header check, string read
  validate.py            parsing, rules, referential integrity
  transform.py           pruning, dedup (window vs group-by), target shaping
  publish.py             merge-by-key, staging + swap, quarantine
  metrics.py             run metrics JSON + JSONL history
  pipeline.py            orchestration + CLI
  benchmark.py           baseline vs optimized benchmark → docs/performance_report.md
  reconcile.py           source vs target reconciliation → docs/reconciliation_report.md
tests/                   63 pytest tests (see docs/requirements_checklist.md)
data/sample/input/       committed sample: customers, products, 3 order files (incl. schema v2)
data/sample/expected/    labels: expected rejects/dedups, counts, daily totals (NOT input)
data/generated/          large generated datasets (git-ignored)
docs/                    architecture, data dictionary, design, operations, reports, SQL
```

## 5. Assumptions

- Each order CSV row is an **event** of an order. `order_id` is the business key. The newest
  `event_ts` is the order's current state. `order_ts` never changes for an order.
- All timestamps are ISO-8601 with an offset (`Z` or `+hh:mm`). They are stored and partitioned in **UTC**.
- Refunds arrive as status `REFUNDED` with a **negative** `order_amount`. Negative amounts on any
  other status are errors. Zero amounts (100% promotions) are valid.
- An order dated before the customer's `signup_date` is a data error (quarantined).
- Customers and products are small, full snapshots delivered with each run, and are trusted.
- Only one pipeline run writes to a given output directory at a time.
- Business limits: quantity 1–1000, |amount| and unit price ≤ 1,000,000.00 (configurable).

## 6. Naming conventions

- **snake_case** for files, modules, columns, config keys and Make targets.
- **Timestamps**: ISO-8601 UTC strings `YYYY-MM-DDTHH:MM:SSZ` in JSON/CSV, Spark `timestamp` in Parquet.
  Dates are `YYYY-MM-DD`.
- **IDs**:
  - `order_sk = sha256(order_id)`
  - generator `event_id = "E" + sha256(order_id|status|event_ts)[:20]`
  - `run_id = <UTC start YYYYMMDDTHHMMSSZ>_<8 hex>` (sortable)
- **Lineage** on every output row: `run_id`, `source_file` (file base name), plus `processed_at`
  (target) or `rejected_at` (quarantine).
- **Reason codes**: `UPPER_SNAKE`, with optional `:<column>` suffix, e.g. `MISSING_REQUIRED:order_id`.
- **Paths**: `data/output/<dataset>/{orders,quarantine,run_metrics}`, partitions `order_date=YYYY-MM-DD`
  and `source_file=<name>.csv`.
- **Money** is always `decimal(12,2)`, never float.
