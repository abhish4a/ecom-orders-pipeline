# Operations: idempotency, retry, restart, backfill

## How a run writes data

```
validate ──► quarantine write (overwrite partitions = this run's source_file values)
         └─► merge-by-key ──► staging/<run_id>  ──► swap into target (dynamic partition overwrite)
                                                      ──► delete staging ──► metrics SUCCESS
```

* **Target** `orders/` is partitioned by `order_date`. A run only rewrites the
  `order_date` partitions that contain rows from its batch. For those partitions it
  reads the existing rows, unions the batch, and keeps **one row per `order_id`**:
  latest `event_ts`, then incoming-over-existing, then `source_file` DESC, then `event_id` DESC.
* **Quarantine** `quarantine/` is partitioned by `source_file`; a run overwrites only
  the `source_file` partitions it processed.
* **Metrics** `run_metrics/<run_id>.json` is written as `RUNNING` at start and
  rewritten as `SUCCESS`/`FAILED` at the end (atomic rename). Every finished run is
  also appended to `run_metrics/run_metrics.jsonl`.

## Idempotency guarantee

Re-running the pipeline on input that has already been processed gives the same target
rows (same `order_id`s, same winning events, same amounts). Only the lineage columns
`run_id` / `processed_at` change, to the latest run. Nothing is appended twice because
every write is a partition **overwrite** of a fully recomputed partition, never an
append. The same holds for the quarantine (keyed by `source_file`).

Processing files one at a time (incremental) produces the same final target as processing
them all in one run. This is checked in `tests/test_idempotency.py`.

## Retry (a run failed)

1. Look at `run_metrics/<run_id>.json`: `status = FAILED`, `failure_reason` says why.
2. Fix the cause (bad config, missing file, out of disk, ...).
3. Re-run **the same command**. No manual cleanup is needed:
   * If the failure happened before the swap, the target was never touched. Staging
     for the failed run may be left under `<output_dir>_staging/<run_id>` for debugging
     and is safe to delete.
   * If the failure happened after the swap (e.g. while writing metrics), the target
     already holds the result; re-running re-merges identical rows (no duplicates).
   * The quarantine is overwritten per `source_file`, so the retry replaces the rejects
     written by the failed attempt.

Failure points can be simulated with `--fail-at-step validate|stage|publish`.

## Restart (the machine / Spark died mid-write)

* Crash **during staging write**: the target is unchanged. Re-run.
* Crash **during the swap** (dynamic partition overwrite): Spark replaces partitions
  one directory at a time, so on a local filesystem a hard crash in this short window
  can leave some partitions new and some old. Every partition is still internally
  consistent and de-duplicated, and re-running the same input converges to the
  correct state, because the merge is by key. Truly atomic multi-partition commits need a
  table format such as Delta Lake or Iceberg (see `docs/design.md`, next steps).

## Backfill

* **Late or historical files**: drop them in the input folder (or point
  `--orders-pattern` at them) and run. Only the `order_date` partitions they touch are
  rewritten. Older events never overwrite newer ones (latest `event_ts` wins).
* **Corrected re-delivery** of a file: same `event_ts` plus the incoming-wins tie-break
  means the corrected rows replace the old ones. To force a correction with an older
  `event_ts`, delete the affected `order_date=` partitions first, then re-run every
  file that contains those dates.
* **Full rebuild**: delete `orders/`, `quarantine/` and re-run over all files
  (`--orders-pattern 'orders/*.csv'`). The result is identical to the incremental history.

## Assumptions / limits

* Single writer: only one pipeline run writes a given `output_dir` at a time.
* `order_ts` (therefore `order_date`) is immutable per `order_id`. If a source changed
  it, the order could exist in two partitions. This is listed in known limitations.
* Dimensions (customers, products) are reloaded in full every run (small snapshots).
