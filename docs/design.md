# Design notes, trade-offs, limitations, next steps

## Key decisions and trade-offs

| Decision | Why | Trade-off / alternative |
|---|---|---|
| Read every source column as **string**, then parse with regex + explicit casts | Malformed values (e.g. `12.3.4`, `abc`, `2026-02-30`) become visible reject reasons instead of silent NULLs or job failures | Slightly more code than `inferSchema`. Inference was rejected because it changes types between files |
| Schema check **per file from its header**, read by column name | A renamed/missing column quarantines only that file with a clear `SCHEMA_MISMATCH`, and other files still load. Re-ordered or extra columns are tolerated | Additive columns are dropped silently (logged). A schema registry would be stricter |
| Reject = **all** failing rules, joined in a fixed order | Operators see every problem at once, and tests can assert exact strings | Longer reason strings |
| **One row per `order_id`, latest `event_ts` wins** | `event_ts` is the source's time of the state change, so late or out-of-order files cannot roll an order back | Requires trustworthy producer clocks. Ties broken by later file, then `event_id`, for determinism |
| Refunds are valid **negative** amounts on `REFUNDED`. Negative on other statuses is rejected | Keeps net revenue correct (`sum(order_amount)` nets refunds off) while catching sign errors | Partial refunds as separate line items are not modelled |
| Orders before customer `signup_date` are **rejected** | Signals an RI/timing defect in source systems | Guest checkouts followed by signup would need a different rule (flag instead of reject) |
| Merge-by-key over **touched partitions** + staging + dynamic partition overwrite | Idempotent reruns, correct incremental/late data, target untouched if the run fails before the swap | Swap of several partitions is not atomic on a plain filesystem (see limitations) |
| Quarantine partitioned by `source_file` (overwrite) | Re-processing a file replaces its rejects, so there are no duplicates | History of earlier rejects of the same file is not kept (metrics history is) |
| `run_id` / `processed_at` = **last** writer | Lineage answers "which run produced this row" | Use the metrics history for a full audit trail |
| Deterministic `order_sk = sha256(order_id)` | Same key in every environment and rerun, no sequence/state needed | 64-char string key (larger than an integer) |
| Broadcast small dimensions, window dedup, early pruning, sized shuffle partitions | Measured 4.8x faster and 63% less shuffle on 1M rows (`docs/performance_report.md`) | Broadcast assumes dimensions stay small (MBs). AQE handles it if they grow |
| JSON config + CLI overrides, no secrets | Simple, diff-able, works locally | No env-specific profiles beyond separate files |
| Metrics as JSON files (not a table) | Zero infrastructure, readable with `jq` or `spark.read.json` | No concurrency control: single writer assumed |

## Known limitations

1. **Non-atomic multi-partition commit.** A hard crash *during* the final dynamic-overwrite
   swap can leave some `order_date` partitions new and some old. Every partition stays
   internally consistent and a rerun repairs it, but readers may briefly see a mixed state.
2. **Single writer.** Two concurrent runs on the same `output_dir` can overwrite each other's partitions.
3. **`order_ts` assumed immutable per `order_id`.** If a source changes it across events,
   the order could end up in two `order_date` partitions (the merge only looks at touched partitions).
4. **Dimensions are trusted snapshots**: not validated beyond key dedup, and not
   historised (no SCD2). Enrichment uses the *current* country/category.
5. **Timestamp format is strict** (`yyyy-MM-dd'T'HH:mm:ssXXX`). Fractional seconds or
   other ISO variants are rejected as `INVALID_TIMESTAMP`.
6. **Currency** is carried but not validated against a reference list or converted.
   Sums mix currencies if the source ever sends more than one.
7. **Corrections with an older `event_ts`** cannot override a newer row through a normal
   run. They need the partition to be deleted and the date reprocessed (see `operations.md`).
8. **Local mode only.** File listing uses Python `glob` on the local filesystem, and the
   staging cleanup uses `shutil`. These need `fs` abstractions for HDFS/S3.
9. Quarantine keeps only the latest rejects per file. The full history is in run metrics counts only.

## Next steps

1. Move target and quarantine to **Delta Lake or Iceberg**: atomic commits, `MERGE INTO`,
   time travel, concurrent writers. This removes limitations 1–3 and the staging step.
2. **Orchestrate** with Airflow/Dagster: one task per stage, retries with backoff, alert on
   `status=FAILED` or `reconciled=false`.
3. Publish metrics to a **table + dashboard** (reject-rate trend, late-data volume, duration).
4. **Schema registry / data contracts** for the order feed, with explicit versioning instead
   of header sniffing.
5. **SCD2 customers/products** and point-in-time joins on `order_ts`.
6. Add an `order_items` fact for multi-line orders and partial refunds.
7. Configurable rule severity (reject vs warn), e.g. `ORDER_BEFORE_SIGNUP` as a warning.
8. CI: run `make test` and `make reconcile` on every push, plus `make benchmark` nightly with
   regression thresholds.
