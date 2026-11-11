# Requirements checklist

Each requirement mapped to the file(s) that satisfy it and the evidence (test or report).
Test paths are relative to `tests/`.

## Fixed decisions

| Requirement | Where |
|---|---|
| E-commerce domain: orders fact, customers + products dims | `src/ecom_pipeline/data_generator.py`, `docs/data_dictionary.md` |
| Python 3.10+, PySpark 3.5 local, pytest, no cloud/secrets | `requirements.txt`, `src/ecom_pipeline/spark_session.py`, `conf/*.json` |
| Parquet output partitioned by `order_date` | `publish.py::merge_into_target`, `test_schema_and_empty.py::test_target_schema_contract` |
| Quarantine with reject_reason, source_file, run_id, raw record | `publish.py::finalize_rejects`, `schemas.py::REJECT_SCHEMA`, `test_schema_and_empty.py::test_renamed_column_quarantines_whole_file` |
| snake_case, ISO-8601 timestamps, deterministic IDs | `README.md` §6, `transform.py::build_target` (`order_sk=sha256`), `test_pipeline.py::test_happy_path` |
| run_id + source_file on every output row | `transform.py::build_target`, `publish.py::finalize_rejects`, reconciliation check "run_id / source_file never null" |
| Fixed random seed | `data_generator.py` (`--seed`, default 42), `test_data_generator.py::test_same_seed_is_byte_identical` |

## Deliverables

| # | Deliverable | Where | Evidence |
|---|---|---|---|
| 1 | Project tree, README, requirements, Makefile | `src/ tests/ data/sample/ data/generated/ docs/ README.md requirements.txt Makefile` | `make` lists targets |
| 2 | Sample (~50–200 rows) + scalable generator (`--rows`, `--seed`) | `data_generator.py`, `data/sample/` (150 rows) | 1M rows in ≈28 s |
| 2 | Normal rows, boundary values | `data_generator.py::apply_boundary` (qty 1/1000, amount 0 / 1,000,000, window start/end second, signup-day order) | `test_validation.py::test_rule[*_boundary, amount_zero_promo, on_signup_day_ok]` |
| 2 | Duplicate keys / events | generator: exact redeliveries + status updates | `test_pipeline.py::test_duplicates_latest_event_wins` |
| 2 | Missing required fields, malformed values, negative amounts | `data_generator.py::DEFECTS` (16 kinds, one per row) | `test_data_generator.py::test_every_edge_case_is_present` |
| 2 | Late / out-of-order events | generator moves old-date events + stale events into the last batch | `test_pipeline.py::test_out_of_order_and_late_data_across_runs` |
| 2 | Invalid customer_id references | defect `unknown_customer` (and `unknown_product`) | `test_validation.py::test_rule[unknown_customer]` |
| 2 | Changed-schema file | `orders_batch_00N_schema_v2.csv` (renamed column + new column) | `test_schema_and_empty.py::test_renamed_column_quarantines_whole_file` |
| 2 | Labels in a SEPARATE file | `data/sample/expected/expected_outcomes.csv`, `expected_summary.json`, `expected_daily_totals.csv` | `test_data_generator.py::test_labels_are_never_inside_input_files` |
| 3 | Modules ingest / validate / transform / publish / metrics | `src/ecom_pipeline/{ingest,validate,transform,publish,metrics}.py` | `docs/architecture.md` |
| 3 | Configurable paths/params (CLI or config) | `config.py`, `conf/sample.json`, `conf/large.json`, `pipeline.py::parse_args` | README §2 |
| 4 | Schema check | `ingest.py::check_file` | `test_schema_and_empty.py` (renamed, extra, re-ordered columns) |
| 4 | Required fields | `validate.py::_rules` (`MISSING_REQUIRED:*`) | `test_validation.py::test_rule[null_*]` |
| 4 | Type / range rules | `validate.py::parse_orders`, `_rules` | `test_validation.py::test_rule[qty_*, amount_*, price_*, *_date]` |
| 4 | PK uniqueness | dedup in `transform.py` + merge in `publish.py` | `reconcile.py` check "order_id unique", SQL "Primary key check" |
| 4 | Referential integrity vs customers/products | `validate.py::join_dimensions` | `test_validation.py::test_rule[unknown_*]`, `::test_broadcast_and_shuffle_join_agree` |
| 4 | Dedup rule stated (latest `event_ts` wins, why) | docstring of `transform.py`, `docs/design.md` | `test_pipeline.py::test_duplicates_latest_event_wins`, `::test_tie_on_event_ts_later_file_wins` |
| 4 | Explicit quarantine path | `quarantine_dir` in `config.py` / `conf/*.json` | `data/output/<dataset>/quarantine/` |
| 5 | Idempotency (merge-by-key + dynamic partition overwrite) | `publish.py` | `test_idempotency.py::test_rerun_same_input_creates_no_duplicates`, `::test_incremental_file_by_file_equals_full_run` |
| 5 | Retry / restart / backfill documented | `docs/operations.md` | `test_idempotency.py::test_partial_failure_then_retry[*]`, `::test_backfill_corrected_redelivery_replaces_row` |
| 6 | Logging | `logging` in every module, `pipeline.py::main` | console output of `make run-sample` |
| 6 | run_metrics JSON with run_id, start/end, counts, status, failure_reason | `metrics.py::RunMetrics`, `<metrics_dir>/<run_id>.json` + `run_metrics.jsonl` | `test_schema_and_empty.py::test_no_input_files`, `test_idempotency.py::test_partial_failure_then_retry` |
| 7 | Baseline: shuffle join, default partitions, multiple wide transforms | `spark_session.py::apply_mode`, `transform.py::dedup_latest_groupby`, `select_typed(prune=False)` | plan table in `docs/performance_report.md` |
| 7 | Optimized: broadcast, sized shuffle partitions, repartition by write key, early pruning, fewer shuffles | `validate.py::join_dimensions(broadcast=True)`, `transform.py::dedup_latest_window`, `publish.py` | same |
| 7 | Benchmark N runs, wall-clock, shuffle read/write, row-count equality | `benchmark.py`, `make benchmark` | `docs/performance_report.md`, `docs/benchmark_results.json` (measured: 4.79x, −63% shuffle) |
| 8 | Tests: happy path | `test_pipeline.py::test_happy_path` | |
| 8 | empty input | `test_schema_and_empty.py::test_no_input_files`, `::test_header_only_and_zero_byte_files` | |
| 8 | malformed records | `test_validation.py::test_rule[...]`, `::test_malformed_csv_lines_are_quarantined` | |
| 8 | duplicates | `test_pipeline.py::test_duplicates_latest_event_wins` | |
| 8 | nulls | `test_validation.py::test_rule[null_*, blank_*]` | |
| 8 | unexpected schema change | `test_schema_and_empty.py::test_renamed_column_quarantines_whole_file` | |
| 8 | invalid reference key | `test_validation.py::test_rule[unknown_customer, unknown_product]` | |
| 8 | late / out-of-order data | `test_pipeline.py::test_out_of_order_and_late_data_across_runs` | |
| 8 | rerun idempotency | `test_idempotency.py::test_rerun_same_input_creates_no_duplicates` | |
| 8 | partial failure + retry | `test_idempotency.py::test_partial_failure_then_retry[validate,stage,publish]` | |
| 8 | domain edge: refunds / negative totals | `test_pipeline.py::test_refunds_reduce_daily_total`, `test_validation.py::test_rule[refund_*]` | |
| 8 | domain edge: order before customer signup | `test_pipeline.py::test_order_before_signup_is_quarantined`, `test_validation.py::test_rule[before_signup, offset_shifts_date]` | |
| 9 | Reconciliation input = output + rejects + deduped | `reconcile.py`, `make reconcile` | `docs/reconciliation_report.md`, `test_reconcile.py` |
| 9 | Key aggregates (sum of order_amount by day) source vs target | `reconcile.py::source_side` (independent Python recomputation) | daily table in `docs/reconciliation_report.md` |
| 9 | Sample SQL queries | `docs/sample_queries.sql` | results in `docs/reconciliation_report.md` |
| 10 | Data dictionary | `docs/data_dictionary.md` | |
| 10 | Architecture / flow diagram (Mermaid + ASCII) | `docs/architecture.md` | |
| 10 | Design / trade-offs, known limitations, next steps | `docs/design.md` | |
| 11 | README: clean setup, run commands, assumptions, naming | `README.md` | |
