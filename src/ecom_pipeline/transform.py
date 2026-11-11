"""Transform: dedup by business key and shape the curated fact table.

Dedup rule (applies within a batch and, in publish, against the existing target):
    For each order_id keep exactly one record -- the one with the LATEST event_ts.
    Why: event_ts is when the source system produced the state change, so the latest
    event is the current state of the order regardless of the order files arrived in
    (late / out-of-order delivery cannot roll an order back).
    Ties (same event_ts) are broken by source_file DESC (later delivery wins), then
    event_id DESC, so the result is fully deterministic. Exact redeliveries have the
    same content, so whichever copy wins the output is identical.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecom_pipeline.schemas import REFUND_STATUS, TARGET_COLUMNS

# Typed columns that survive into the target (raw strings and raw_record are pruned).
VALID_COLUMNS: list[str] = [
    "event_id", "order_id", "customer_id", "product_id", "quantity_p", "unit_price_p",
    "order_amount_p", "currency", "order_status", "order_ts_p", "event_ts_p",
    "customer_country", "product_category", "source_file",
]
# Winner ordering for dedup (column names; Columns are built lazily inside functions).
WINNER_ORDER = ("event_ts", "source_file", "event_id")


def select_typed(valid: DataFrame, prune: bool) -> DataFrame:
    """Rename typed *_p columns to their final names.

    prune=True  (optimized): drop everything not needed downstream BEFORE the dedup shuffle,
                so the large raw strings / raw_record JSON are never shuffled.
    prune=False (baseline):  carry the raw columns along; they are only dropped at the end.
    """
    typed = [F.col(c).alias(c[:-2]) if c.endswith("_p") else F.col(c) for c in VALID_COLUMNS]
    if prune:
        return valid.select(*typed)
    raw_cols = [F.col(c).alias(f"raw_{c}")
                for c in ("quantity", "unit_price", "order_amount", "order_ts", "event_ts")]
    return valid.select(*typed, *raw_cols, "raw_record", "customer_signup_date")


def dedup_latest_window(valid: DataFrame) -> DataFrame:
    """Optimized dedup: one shuffle (hash partition by order_id) + row_number."""
    w = Window.partitionBy("order_id").orderBy(*[F.col(c).desc() for c in WINNER_ORDER])
    return (valid.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1).drop("_rn"))


def dedup_latest_groupby(valid: DataFrame) -> DataFrame:
    """Baseline dedup: aggregate the winning key, join it back, then drop exact duplicates.

    Same result as the window version, but needs three shuffles (groupBy, join, dropDuplicates).
    """
    winner = (valid.groupBy("order_id")
              .agg(F.max(F.struct(*WINNER_ORDER)).alias("_w"))
              .select(F.col("order_id").alias("_w_order_id"),
                      F.col("_w.event_ts").alias("_w_event_ts"),
                      F.col("_w.source_file").alias("_w_source_file"),
                      F.col("_w.event_id").alias("_w_event_id")))
    joined = valid.join(
        winner,
        (valid["order_id"] == winner["_w_order_id"])
        & (valid["event_ts"] == winner["_w_event_ts"])
        & (valid["source_file"] == winner["_w_source_file"])
        & (valid["event_id"] == winner["_w_event_id"]),
        "inner",
    ).drop("_w_order_id", "_w_event_ts", "_w_source_file", "_w_event_id")
    return joined.dropDuplicates(["order_id"])


def dedup_latest(valid: DataFrame, mode: str) -> DataFrame:
    return dedup_latest_groupby(valid) if mode == "baseline" else dedup_latest_window(valid)


def build_target(deduped: DataFrame, run_id: str, processed_at: datetime) -> DataFrame:
    """Add deterministic surrogate key, derived columns and lineage; fix column order."""
    return (
        deduped.withColumn("order_sk", F.sha2(F.col("order_id"), 256))
        .withColumn("is_refund", F.col("order_status") == F.lit(REFUND_STATUS))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("processed_at", F.lit(processed_at).cast("timestamp"))
        .select(*TARGET_COLUMNS)
    )
