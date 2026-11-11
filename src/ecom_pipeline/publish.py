"""Publish: idempotent merge-by-key into the target + quarantine write.

Target (Parquet, partitioned by order_date)
-------------------------------------------
1. Find the order_date partitions touched by this batch.
2. Read the EXISTING target rows of only those partitions (partition pruning). If none of
   them exist yet (first load), step 3 is skipped -- the batch is already unique per key.
3. Union existing + incoming and keep one row per order_id with the same rule as the
   in-batch dedup (latest event_ts wins). On a full tie the INCOMING row wins, so
   re-processing identical input yields identical data with refreshed lineage, and a
   corrected re-delivery (backfill) replaces the old row.
4. Write the merged partitions to a STAGING directory (the target is untouched so far).
5. Copy staging -> target with dynamic partition overwrite: only the touched
   partitions are replaced; all other partitions are left alone.
6. Delete staging.

Why it is idempotent: the output of steps 2-3 depends only on (existing rows, incoming
rows), and re-running with the same incoming rows produces the same set of order_ids
with the same winning events -- never a second copy.

Assumption: order_ts (hence order_date) never changes for an order_id. See docs/operations.md.

Quarantine (Parquet, partitioned by source_file)
------------------------------------------------
Dynamic partition overwrite by source_file: re-processing a file replaces that file's
rejects instead of appending duplicates.
"""
from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ecom_pipeline.schemas import REJECT_COLUMNS, TARGET_COLUMNS, TARGET_SCHEMA

log = logging.getLogger(__name__)


class InjectedFailure(RuntimeError):
    """Raised by the fail_at_step test hook to simulate a crash."""


@dataclass
class MergeStats:
    partitions_written: int = 0
    output_count: int = 0        # incoming rows that are now in the target
    inserted_count: int = 0      # ...for order_ids not previously in the target
    updated_count: int = 0       # ...that replaced an existing row
    target_rows_in_partitions: int = 0


def finalize_rejects(rejects: DataFrame, run_id: str, rejected_at: datetime) -> DataFrame:
    return (rejects.withColumn("run_id", F.lit(run_id))
            .withColumn("rejected_at", F.lit(rejected_at).cast("timestamp"))
            .select(*REJECT_COLUMNS))


def target_exists(path: str) -> bool:
    p = Path(path)
    return p.is_dir() and any(p.glob("order_date=*"))


def read_existing(spark: SparkSession, path: str, dates: list[Any]) -> DataFrame:
    if not dates or not target_exists(path):
        return spark.createDataFrame([], TARGET_SCHEMA)
    return spark.read.schema(TARGET_SCHEMA).parquet(path).filter(F.col("order_date").isin(dates))


def merge_frames(existing: DataFrame, incoming: DataFrame) -> DataFrame:
    """One row per order_id. Adds _is_new (row came from this batch) and _had_existing."""
    both = (existing.select(*TARGET_COLUMNS).withColumn("_is_new", F.lit(0))
            .unionByName(incoming.select(*TARGET_COLUMNS).withColumn("_is_new", F.lit(1))))
    by_key = Window.partitionBy("order_id")
    ranked = Window.partitionBy("order_id").orderBy(
        F.col("event_ts").desc(), F.col("_is_new").desc(),
        F.col("source_file").desc(), F.col("event_id").desc())
    return (both.withColumn("_had_existing", F.min("_is_new").over(by_key) == 0)
            .withColumn("_rn", F.row_number().over(ranked))
            .filter(F.col("_rn") == 1).drop("_rn"))


def merge_into_target(
    spark: SparkSession, incoming: DataFrame, target_path: str, staging_path: str,
    repartition_by_key: bool, fail_at_step: str | None = None,
) -> MergeStats:
    stats = MergeStats()
    dates = sorted(r["order_date"] for r in incoming.select("order_date").distinct().collect())
    if not dates:
        log.info("no valid rows in batch -> target untouched")
        return stats
    stats.partitions_written = len(dates)

    existing_dates = [d for d in dates if (Path(target_path) / f"order_date={d}").is_dir()]
    if existing_dates:
        merged = merge_frames(read_existing(spark, target_path, existing_dates), incoming)
    else:
        # First load of these dates: the batch is already unique per order_id, so the merge
        # window (a full shuffle) would change nothing -- skip it.
        merged = (incoming.select(*TARGET_COLUMNS).withColumn("_is_new", F.lit(1))
                  .withColumn("_had_existing", F.lit(False)))
    if repartition_by_key:  # one file per order_date partition, no small-file explosion
        merged = merged.repartition("order_date")

    # Stage first: if anything fails before the swap the target is still the previous version.
    shutil.rmtree(staging_path, ignore_errors=True)
    merged.write.mode("overwrite").partitionBy("order_date").parquet(staging_path)
    staged = spark.read.parquet(staging_path)
    agg = staged.agg(
        F.count("*").alias("total"),
        F.sum(F.col("_is_new")).alias("new"),
        F.sum(F.when((F.col("_is_new") == 1) & F.col("_had_existing"), 1).otherwise(0)).alias("upd"),
    ).first()
    stats.target_rows_in_partitions = int(agg["total"])
    stats.output_count = int(agg["new"] or 0)
    stats.updated_count = int(agg["upd"] or 0)
    stats.inserted_count = stats.output_count - stats.updated_count
    log.info("staged %d partition(s) at %s", len(dates), staging_path)
    if fail_at_step == "stage":
        raise InjectedFailure("injected failure after staging, before publish")

    # Swap: dynamic overwrite replaces only the partitions present in `staged`. No repartition
    # needed: staging already holds the final file layout, so files are copied 1:1.
    final = staged.select(*TARGET_COLUMNS)
    final.write.mode("overwrite").partitionBy("order_date").parquet(target_path)
    shutil.rmtree(staging_path, ignore_errors=True)
    log.info("published %d partition(s) to %s", len(dates), target_path)
    return stats


def write_quarantine(df: DataFrame, path: str) -> None:
    df.repartition("source_file").write.mode("overwrite").partitionBy("source_file").parquet(path)
    log.info("quarantine written: %s", path)
