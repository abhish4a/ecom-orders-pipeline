"""Reconciliation passes on a correct run and catches a tampered target."""
from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from ecom_pipeline.pipeline import run_pipeline
from ecom_pipeline.reconcile import reconcile, write_report
from tests.helpers import SAMPLE_DIR, make_cfg


def test_reconciliation_passes_then_detects_lost_partition(spark: SparkSession, tmp_path: Path) -> None:
    cfg = make_cfg(tmp_path, input_dir=SAMPLE_DIR / "input")
    run_pipeline(spark, cfg)

    res = reconcile(spark, cfg, SAMPLE_DIR / "expected")
    assert all(res["checks"].values()), res["checks"]
    c = res["counts"]
    assert c["input_rows"] == c["output_rows"] + c["rejected_rows"] + c["deduped_rows"] == 150
    write_report(res, tmp_path / "report.md")
    assert "PASS" in (tmp_path / "report.md").read_text()

    # Simulate data loss: drop one day of the target -> counts and daily sums must fail.
    shutil.rmtree(Path(cfg.output_dir) / "order_date=2026-01-03")
    bad = reconcile(spark, cfg, SAMPLE_DIR / "expected")
    assert not bad["checks"]["input == output + rejected + deduped"]
    assert not bad["checks"]["daily count + sum(order_amount): source == target"]
