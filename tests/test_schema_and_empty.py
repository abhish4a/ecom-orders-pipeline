"""Unexpected schema changes and empty inputs, end to end through run_pipeline."""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession

from ecom_pipeline.pipeline import run_pipeline
from ecom_pipeline.schemas import ORDER_COLUMNS, TARGET_COLUMNS
from tests.helpers import make_cfg, order, quarantine, target_by_order, write_csv


def test_renamed_column_quarantines_whole_file(spark: SparkSession, workdir: Path) -> None:
    orders = workdir / "input" / "orders"
    write_csv(orders / "good.csv", [order()])
    v2_cols = [("total_amount" if c == "order_amount" else c) for c in ORDER_COLUMNS] + ["coupon_code"]
    v2_rows = [dict(order(event_id=f"V{i}", order_id=f"OV{i}"), total_amount="20.00", coupon_code="X")
               for i in range(3)]
    write_csv(orders / "v2.csv", v2_rows, v2_cols)

    m = run_pipeline(spark, make_cfg(workdir))

    assert m.status == "SUCCESS" and m.reconciled
    assert (m.input_count, m.output_count, m.rejected_count) == (4, 1, 3)
    rejects = quarantine(spark, make_cfg(workdir))
    assert {r["reject_reason"] for r in rejects} == {"SCHEMA_MISMATCH"}
    assert all("missing columns: order_amount" in r["reject_detail"] for r in rejects)
    assert all(r["source_file"] == "v2.csv" and '"total_amount":"20.00"' in r["raw_record"]
               for r in rejects)
    assert list(target_by_order(spark, make_cfg(workdir))) == ["O1"]


def test_extra_column_and_reordered_columns_are_accepted(spark: SparkSession, workdir: Path) -> None:
    """Additive change (new column) is tolerated; columns are matched by name, not position."""
    cols = list(reversed(ORDER_COLUMNS)) + ["coupon_code"]
    write_csv(workdir / "input" / "orders" / "new_col.csv", [dict(order(), coupon_code="SPRING")], cols)

    m = run_pipeline(spark, make_cfg(workdir))

    assert (m.output_count, m.rejected_count) == (1, 0)
    row = target_by_order(spark, make_cfg(workdir))["O1"]
    assert row["customer_id"] == "C1" and str(row["order_amount"]) == "20.00"


def test_no_input_files(spark: SparkSession, workdir: Path) -> None:
    cfg = make_cfg(workdir)
    m = run_pipeline(spark, cfg)
    assert m.status == "SUCCESS" and m.reconciled
    assert (m.input_count, m.output_count, m.rejected_count, m.partitions_written) == (0, 0, 0, 0)
    assert not Path(cfg.output_dir).exists()  # nothing to publish -> target untouched
    saved = json.loads((Path(cfg.metrics_dir) / f"{m.run_id}.json").read_text())
    assert saved["status"] == "SUCCESS" and saved["input_files"] == []


def test_header_only_and_zero_byte_files(spark: SparkSession, workdir: Path) -> None:
    orders = workdir / "input" / "orders"
    write_csv(orders / "header_only.csv", [])
    (orders / "zero_bytes.csv").write_text("")
    m = run_pipeline(spark, make_cfg(workdir))
    assert m.status == "SUCCESS"
    assert (m.input_count, m.output_count) == (0, 0)
    assert sorted(m.input_files) == ["header_only.csv", "zero_bytes.csv"]


def test_target_schema_contract(spark: SparkSession, workdir: Path) -> None:
    write_csv(workdir / "input" / "orders" / "a.csv", [order()])
    cfg = make_cfg(workdir)
    run_pipeline(spark, cfg)
    df = spark.read.parquet(cfg.output_dir)
    assert sorted(df.columns) == sorted(TARGET_COLUMNS)
    types = dict(df.dtypes)
    assert types["order_amount"] == "decimal(12,2)" and types["order_ts"] == "timestamp"
    assert types["order_date"] == "date" and types["quantity"] == "int"
    assert (Path(cfg.output_dir) / "order_date=2026-01-02").is_dir()
