"""Pipeline entry point: ingest -> validate -> transform -> publish, with run metrics.

Usage:
    python -m ecom_pipeline.pipeline --config conf/sample.json
    python -m ecom_pipeline.pipeline --config conf/sample.json --orders-pattern 'orders/orders_batch_001.csv'
    python -m ecom_pipeline.pipeline --config conf/sample.json --mode baseline --run-id my_run

Exit code is 0 on SUCCESS and 1 on FAILED (metrics are written in both cases).
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecom_pipeline import ingest, publish, transform, validate
from ecom_pipeline.config import PipelineConfig, load_config
from ecom_pipeline.metrics import RunMetrics, utc_iso, write_metrics
from ecom_pipeline.publish import InjectedFailure
from ecom_pipeline.spark_session import apply_mode, build_spark

log = logging.getLogger("ecom_pipeline")


def new_run_id(now: datetime | None = None) -> str:
    """Sortable, unique run id, e.g. 20260111T101500Z_1a2b3c4d."""
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"


def _execute(spark: SparkSession, cfg: PipelineConfig, m: RunMetrics, started: datetime) -> None:
    """The actual work. Fills `m` as it goes so a failure still reports partial counts."""
    apply_mode(spark, cfg.mode, cfg.shuffle_partitions)

    # ---- ingest ------------------------------------------------------------
    files = ingest.list_order_files(cfg.input_dir, cfg.orders_pattern)
    m.input_files = [Path(f).name for f in files]
    log.info("found %d order file(s): %s", len(files), m.input_files)
    ing = ingest.read_orders(spark, files)
    customers = ingest.read_customers(spark, str(Path(cfg.input_dir) / cfg.customers_file))
    products = ingest.read_products(spark, str(Path(cfg.input_dir) / cfg.products_file))

    # ---- validate ----------------------------------------------------------
    checked = validate.validate_orders(ing.raw, customers, products, cfg,
                                       broadcast=cfg.mode == "optimized")
    checked = checked.persist(StorageLevel.MEMORY_AND_DISK)  # reused by 3 consumers below
    rejects = publish.finalize_rejects(
        validate.to_reject_records(checked.filter(~F.col("is_valid"))).unionByName(ing.schema_rejects),
        m.run_id, started,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    m.input_count = checked.count() + ing.schema_rejects.count()
    m.reject_reason_counts = {r["reject_reason"]: int(r["count"]) for r in sorted(
        rejects.groupBy("reject_reason").count().collect(), key=lambda r: r["reject_reason"])}
    m.rejected_count = sum(m.reject_reason_counts.values())
    m.valid_count = m.input_count - m.rejected_count
    log.info("validated: input=%d valid=%d rejected=%d", m.input_count, m.valid_count, m.rejected_count)
    if cfg.fail_at_step == "validate":
        raise InjectedFailure("injected failure after validate")

    # ---- transform ---------------------------------------------------------
    typed = transform.select_typed(checked.filter(F.col("is_valid")), prune=cfg.mode == "optimized")
    batch = transform.build_target(transform.dedup_latest(typed, cfg.mode), m.run_id, started)
    batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
    batch_unique = batch.count()
    m.duplicate_count = m.valid_count - batch_unique

    # ---- publish -------------------------------------------------------------
    publish.write_quarantine(rejects, cfg.quarantine_dir)
    staging = str(Path(cfg.staging_root) / m.run_id)
    stats = publish.merge_into_target(spark, batch, cfg.output_dir, staging,
                                      repartition_by_key=cfg.mode == "optimized",
                                      fail_at_step=cfg.fail_at_step)
    m.output_count = stats.output_count
    m.inserted_count = stats.inserted_count
    m.updated_count = stats.updated_count
    m.partitions_written = stats.partitions_written
    m.stale_count = batch_unique - stats.output_count
    for df in (batch, rejects, checked):
        df.unpersist()
    if cfg.fail_at_step == "publish":
        raise InjectedFailure("injected failure after publish, before run was marked SUCCESS")


def run_pipeline(spark: SparkSession, cfg: PipelineConfig) -> RunMetrics:
    """Run once and always write metrics. Re-raises on failure after recording it."""
    started = datetime.now(timezone.utc).replace(microsecond=0)
    m = RunMetrics(run_id=cfg.run_id or new_run_id(started), mode=cfg.mode,
                   start_time=utc_iso(started), config=cfg.to_dict())
    write_metrics(m, cfg.metrics_dir, append_history=False)   # status RUNNING
    log.info("run %s started (mode=%s)", m.run_id, cfg.mode)
    try:
        _execute(spark, cfg, m, started)
    except Exception as exc:  # noqa: BLE001 - record any failure, then propagate
        m.finish("FAILED", f"{type(exc).__name__}: {exc}"[:1000])
        write_metrics(m, cfg.metrics_dir, append_history=True)
        log.exception("run %s FAILED", m.run_id)
        raise
    m.finish("SUCCESS")
    write_metrics(m, cfg.metrics_dir, append_history=True)
    log.info("run %s SUCCESS in %.1fs: input=%d output=%d rejected=%d duplicate=%d stale=%d "
             "reconciled=%s", m.run_id, m.duration_seconds or 0, m.input_count, m.output_count,
             m.rejected_count, m.duplicate_count, m.stale_count, m.reconciled)
    return m


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="E-commerce orders batch pipeline")
    p.add_argument("--config", help="JSON config file (see conf/)")
    p.add_argument("--input-dir")
    p.add_argument("--output-dir")
    p.add_argument("--quarantine-dir")
    p.add_argument("--metrics-dir")
    p.add_argument("--staging-dir")
    p.add_argument("--orders-pattern", help="glob relative to input-dir, e.g. 'orders/orders_batch_001.csv'")
    p.add_argument("--mode", choices=["optimized", "baseline"])
    p.add_argument("--shuffle-partitions", type=int)
    p.add_argument("--run-id")
    p.add_argument("--fail-at-step", choices=["validate", "stage", "publish"],
                   help="testing only: simulate a crash after this step")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    logging.basicConfig(level=a.log_level, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    cfg = load_config(
        a.config, input_dir=a.input_dir, output_dir=a.output_dir, quarantine_dir=a.quarantine_dir,
        metrics_dir=a.metrics_dir, staging_dir=a.staging_dir, orders_pattern=a.orders_pattern,
        mode=a.mode, shuffle_partitions=a.shuffle_partitions, run_id=a.run_id,
        fail_at_step=a.fail_at_step,
    )
    spark = build_spark()
    try:
        run_pipeline(spark, cfg)
        return 0
    except Exception:  # noqa: BLE001 - already logged + recorded in metrics
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
