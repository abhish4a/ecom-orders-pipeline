"""End-to-end behaviour: happy path, duplicates, late / out-of-order data, domain cases."""
from __future__ import annotations

import csv
import hashlib
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecom_pipeline.pipeline import run_pipeline
from tests.helpers import (
    SAMPLE_DIR, make_cfg, order, quarantine, target_by_order, target_rows, write_csv,
)


def test_happy_path(spark: SparkSession, workdir: Path) -> None:
    write_csv(workdir / "input" / "orders" / "b1.csv", [
        order(event_id="E1", order_id="O1"),
        order(event_id="E2", order_id="O2", product_id="P2", unit_price="25.00", order_amount="50.00",
              order_ts="2026-01-03T08:00:00Z", event_ts="2026-01-03T08:00:00Z"),
    ])
    cfg = make_cfg(workdir)
    m = run_pipeline(spark, cfg)

    assert m.status == "SUCCESS" and m.reconciled
    assert (m.input_count, m.output_count, m.rejected_count, m.duplicate_count) == (2, 2, 0, 0)
    rows = target_by_order(spark, cfg)
    o1 = rows["O1"]
    assert o1["order_sk"] == hashlib.sha256(b"O1").hexdigest()          # deterministic id
    assert o1["run_id"] == m.run_id and o1["source_file"] == "b1.csv"    # lineage on every row
    assert o1["customer_country"] == "US" and rows["O2"]["product_category"] == "books"
    assert str(o1["order_date"]) == "2026-01-02" and o1["is_refund"] is False


def test_committed_sample_matches_expected_labels(spark: SparkSession, tmp_path: Path) -> None:
    """The generator's independent labels are the oracle for the whole pipeline."""
    cfg = make_cfg(tmp_path, input_dir=SAMPLE_DIR / "input")
    m = run_pipeline(spark, cfg)

    with (SAMPLE_DIR / "expected" / "expected_outcomes.csv").open() as fh:
        labels = list(csv.DictReader(fh))
    expected_rejects = {(r["source_file"], r["event_id"], r["expected_reason"])
                        for r in labels if r["expected_outcome"] == "rejected"}
    actual_rejects = {(r["source_file"], r["event_id"], r["reject_reason"])
                      for r in quarantine(spark, cfg)}
    assert actual_rejects == expected_rejects
    deduped = sum(1 for r in labels if r["expected_outcome"] == "deduped")
    assert m.duplicate_count + m.stale_count == deduped
    assert m.input_count == m.output_count + m.rejected_count + deduped

    with (SAMPLE_DIR / "expected" / "expected_daily_totals.csv").open() as fh:
        expected_totals = {r["order_date"]: (int(r["order_count"]), Decimal(r["total_order_amount"]))
                           for r in csv.DictReader(fh)}
    actual = spark.read.parquet(cfg.output_dir).groupBy("order_date").agg(
        F.count("*").alias("n"), F.sum("order_amount").alias("amt")).collect()
    assert {str(r["order_date"]): (r["n"], r["amt"]) for r in actual} == expected_totals


def test_duplicates_latest_event_wins(spark: SparkSession, workdir: Path) -> None:
    write_csv(workdir / "input" / "orders" / "b1.csv", [
        order(event_id="E1", order_status="PLACED", event_ts="2026-01-02T10:00:00Z"),
        order(event_id="E1", order_status="PLACED", event_ts="2026-01-02T10:00:00Z"),  # exact redelivery
        order(event_id="E3", order_status="DELIVERED", event_ts="2026-01-04T09:00:00Z"),
        order(event_id="E2", order_status="SHIPPED", event_ts="2026-01-03T09:00:00Z"),  # older than E3
    ])
    cfg = make_cfg(workdir)
    m = run_pipeline(spark, cfg)
    assert (m.input_count, m.output_count, m.duplicate_count) == (4, 1, 3)
    row = target_by_order(spark, cfg)["O1"]
    assert (row["event_id"], row["order_status"]) == ("E3", "DELIVERED")


def test_tie_on_event_ts_later_file_wins(spark: SparkSession, workdir: Path) -> None:
    ts = "2026-01-02T10:00:00Z"
    write_csv(workdir / "input" / "orders" / "b1.csv", [order(event_id="EA", order_amount="20.00", event_ts=ts)])
    write_csv(workdir / "input" / "orders" / "b2.csv", [order(event_id="EB", order_amount="18.00", event_ts=ts)])
    cfg = make_cfg(workdir)
    run_pipeline(spark, cfg)
    row = target_by_order(spark, cfg)["O1"]
    assert (row["source_file"], str(row["order_amount"])) == ("b2.csv", "18.00")


def test_out_of_order_and_late_data_across_runs(spark: SparkSession, workdir: Path) -> None:
    orders = workdir / "input" / "orders"
    # Run 1: O1 already refunded; O2 on 2026-01-02.
    write_csv(orders / "b1.csv", [
        order(event_id="E1R", order_status="REFUNDED", order_amount="-20.00", event_ts="2026-01-05T00:00:00Z"),
        order(event_id="E2", order_id="O2"),
    ])
    cfg1 = make_cfg(workdir, orders_pattern="orders/b1.csv")
    run_pipeline(spark, cfg1)
    # Run 2 (arrives later): stale DELIVERED event for O1 + a late order O3 for an OLD date.
    write_csv(orders / "b2.csv", [
        order(event_id="E1D", order_status="DELIVERED", event_ts="2026-01-04T00:00:00Z"),
        order(event_id="E3", order_id="O3", order_ts="2026-01-02T23:00:00Z", event_ts="2026-01-02T23:00:00Z"),
    ])
    m2 = run_pipeline(spark, make_cfg(workdir, orders_pattern="orders/b2.csv"))

    assert (m2.output_count, m2.stale_count, m2.inserted_count) == (1, 1, 1) and m2.reconciled
    rows = target_by_order(spark, cfg1)
    assert rows["O1"]["order_status"] == "REFUNDED"           # older event did not roll it back
    assert set(rows) == {"O1", "O2", "O3"}                    # old partition kept O1/O2, gained O3
    assert rows["O2"]["run_id"] != m2.run_id                  # untouched row keeps its lineage


def test_refunds_reduce_daily_total(spark: SparkSession, workdir: Path) -> None:
    """Domain edge case: refunds are valid negative totals and net off in daily sums."""
    write_csv(workdir / "input" / "orders" / "b1.csv", [
        order(event_id="E1", order_id="O1", order_amount="20.00"),
        order(event_id="E2", order_id="O2", order_status="REFUNDED", order_amount="-20.00"),
        order(event_id="E3", order_id="O3", order_amount="-5.00"),  # negative but not a refund
    ])
    cfg = make_cfg(workdir)
    m = run_pipeline(spark, cfg)
    assert (m.output_count, m.rejected_count) == (2, 1)
    total = spark.read.parquet(cfg.output_dir).agg(F.sum("order_amount")).first()[0]
    assert total == Decimal("0.00")
    assert target_by_order(spark, cfg)["O2"]["is_refund"] is True


def test_order_before_signup_is_quarantined(spark: SparkSession, workdir: Path) -> None:
    """Domain edge case: C2 signs up on 2026-01-10; an order on 01-09 is rejected."""
    write_csv(workdir / "input" / "orders" / "b1.csv", [
        order(event_id="E1", customer_id="C2", order_ts="2026-01-09T12:00:00Z"),
        order(event_id="E2", order_id="O2", customer_id="C2", order_ts="2026-01-10T12:00:00Z"),
    ])
    cfg = make_cfg(workdir)
    m = run_pipeline(spark, cfg)
    assert (m.output_count, m.reject_reason_counts) == (1, {"ORDER_BEFORE_SIGNUP": 1})
    assert list(target_by_order(spark, cfg)) == ["O2"]


def test_baseline_and_optimized_produce_same_output(spark: SparkSession, tmp_path: Path) -> None:
    opt = make_cfg(tmp_path / "opt", input_dir=SAMPLE_DIR / "input", mode="optimized")
    base = make_cfg(tmp_path / "base", input_dir=SAMPLE_DIR / "input", mode="baseline")
    m_opt, m_base = run_pipeline(spark, opt), run_pipeline(spark, base)
    assert (m_base.output_count, m_base.rejected_count) == (m_opt.output_count, m_opt.rejected_count)
    assert target_rows(spark, base) == target_rows(spark, opt)
