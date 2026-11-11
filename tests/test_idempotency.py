"""Idempotency, incremental == full, partial failure + retry, backfill correction."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ecom_pipeline.metrics import read_history
from ecom_pipeline.pipeline import run_pipeline
from ecom_pipeline.publish import InjectedFailure
from tests.helpers import SAMPLE_DIR, make_cfg, order, quarantine, target_by_order, target_rows, write_csv

SAMPLE_FILES = ["orders_batch_001.csv", "orders_batch_002.csv", "orders_batch_003_schema_v2.csv"]


@pytest.fixture(scope="module")
def full_run(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> list[tuple]:
    """Reference result: every sample file processed in a single run."""
    cfg = make_cfg(tmp_path_factory.mktemp("full"), input_dir=SAMPLE_DIR / "input")
    run_pipeline(spark, cfg)
    return target_rows(spark, cfg)


def test_rerun_same_input_creates_no_duplicates(spark: SparkSession, tmp_path: Path,
                                                full_run: list[tuple]) -> None:
    cfg = make_cfg(tmp_path, input_dir=SAMPLE_DIR / "input")
    m1 = run_pipeline(spark, cfg)
    q1 = len(quarantine(spark, cfg))
    m2 = run_pipeline(spark, cfg)

    assert target_rows(spark, cfg) == full_run                 # same rows, no extra copies
    assert len(quarantine(spark, cfg)) == q1 == m1.rejected_count
    assert m2.output_count == m1.output_count and m2.inserted_count == 0
    assert m2.updated_count == m2.output_count                 # every row re-merged onto itself
    assert {r["run_id"] for r in spark.read.parquet(cfg.output_dir).collect()} == {m2.run_id}
    assert [h["status"] for h in read_history(cfg.metrics_dir)] == ["SUCCESS", "SUCCESS"]


def test_incremental_file_by_file_equals_full_run(spark: SparkSession, tmp_path: Path,
                                                  full_run: list[tuple]) -> None:
    for name in SAMPLE_FILES:
        m = run_pipeline(spark, make_cfg(tmp_path, input_dir=SAMPLE_DIR / "input",
                                         orders_pattern=f"orders/{name}"))
        assert m.reconciled
    assert target_rows(spark, make_cfg(tmp_path, input_dir=SAMPLE_DIR / "input")) == full_run


@pytest.mark.parametrize("step", ["validate", "stage", "publish"])
def test_partial_failure_then_retry(spark: SparkSession, tmp_path: Path, full_run: list[tuple],
                                    step: str) -> None:
    # Pre-existing state: batch 1 already published.
    run_pipeline(spark, make_cfg(tmp_path, input_dir=SAMPLE_DIR / "input",
                                 orders_pattern="orders/orders_batch_001.csv"))
    cfg = make_cfg(tmp_path, input_dir=SAMPLE_DIR / "input")
    before = target_rows(spark, cfg)

    with pytest.raises(InjectedFailure):
        run_pipeline(spark, make_cfg(tmp_path, input_dir=SAMPLE_DIR / "input", fail_at_step=step))

    failed = read_history(cfg.metrics_dir)[-1]
    assert failed["status"] == "FAILED" and "InjectedFailure" in failed["failure_reason"]
    saved = json.loads((Path(cfg.metrics_dir) / f"{failed['run_id']}.json").read_text())
    assert saved["status"] == "FAILED" and saved["end_time"] is not None
    if step in ("validate", "stage"):
        assert target_rows(spark, cfg) == before               # crash before swap: target untouched

    retry = run_pipeline(spark, cfg)                            # same command, no manual cleanup
    assert retry.status == "SUCCESS" and retry.reconciled
    assert target_rows(spark, cfg) == full_run
    assert len(quarantine(spark, cfg)) == retry.rejected_count  # no duplicated rejects


def test_backfill_corrected_redelivery_replaces_row(spark: SparkSession, workdir: Path) -> None:
    orders = workdir / "input" / "orders"
    write_csv(orders / "day.csv", [order(order_amount="20.00"), order(event_id="E2", order_id="O2")])
    cfg = make_cfg(workdir)
    run_pipeline(spark, cfg)
    # Source re-sends the file with a corrected amount (same events, same event_ts).
    write_csv(orders / "day.csv", [order(order_amount="19.50"), order(event_id="E2", order_id="O2")])
    m = run_pipeline(spark, cfg)
    rows = target_by_order(spark, cfg)
    assert len(rows) == 2 and str(rows["O1"]["order_amount"]) == "19.50"
    assert m.updated_count == 2 and m.inserted_count == 0
