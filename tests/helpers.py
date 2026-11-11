"""Small builders shared by tests: tiny CSV inputs and readers for pipeline outputs."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from ecom_pipeline.config import PipelineConfig, load_config
from ecom_pipeline.schemas import ORDER_COLUMNS

SAMPLE_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"

# C1 signed up long ago; C2 signs up on 2026-01-10 (for ORDER_BEFORE_SIGNUP cases).
CUSTOMERS = [
    {"customer_id": "C1", "customer_name": "Ava Smith", "email": "a@example.com",
     "country": "US", "signup_date": "2025-01-01"},
    {"customer_id": "C2", "customer_name": "Liam Kim", "email": "l@example.com",
     "country": "GB", "signup_date": "2026-01-10"},
]
PRODUCTS = [
    {"product_id": "P1", "product_name": "Mug", "category": "home", "list_price": "10.00"},
    {"product_id": "P2", "product_name": "Book", "category": "books", "list_price": "25.00"},
]


def order(**overrides: Any) -> dict[str, Any]:
    """A valid order event; override any field (None = empty CSV cell)."""
    row: dict[str, Any] = {
        "event_id": "E1", "order_id": "O1", "customer_id": "C1", "product_id": "P1",
        "quantity": "2", "unit_price": "10.00", "order_amount": "20.00", "currency": "USD",
        "order_status": "PLACED", "order_ts": "2026-01-02T10:00:00Z",
        "event_ts": "2026-01-02T10:05:00Z",
    }
    row.update(overrides)
    return row


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = columns or ORDER_COLUMNS
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: ("" if r.get(c) is None else r[c]) for c in cols})
    return path


def make_input(root: Path) -> Path:
    """Create <root>/input with the test dimensions and an empty orders/ folder."""
    inp = root / "input"
    write_csv(inp / "customers.csv", CUSTOMERS, list(CUSTOMERS[0]))
    write_csv(inp / "products.csv", PRODUCTS, list(PRODUCTS[0]))
    (inp / "orders").mkdir(parents=True, exist_ok=True)
    return inp


def make_cfg(root: Path, input_dir: Path | None = None, **overrides: Any) -> PipelineConfig:
    out = root / "out"
    base: dict[str, Any] = dict(
        input_dir=str(input_dir or root / "input"),
        output_dir=str(out / "orders"),
        quarantine_dir=str(out / "quarantine"),
        metrics_dir=str(out / "run_metrics"),
        shuffle_partitions=2,
    )
    base.update(overrides)
    return load_config(None, **base)


def target_rows(spark: SparkSession, cfg: PipelineConfig) -> list[tuple]:
    """Target content without lineage columns that legitimately change per run."""
    if not Path(cfg.output_dir).exists():
        return []
    df = spark.read.parquet(cfg.output_dir).drop("run_id", "processed_at")
    cols = sorted(df.columns)
    return sorted(tuple(r[c] for c in cols) for r in df.select(*cols).collect())


def target_by_order(spark: SparkSession, cfg: PipelineConfig) -> dict[str, Any]:
    return {r["order_id"]: r for r in spark.read.parquet(cfg.output_dir).collect()}


def quarantine(spark: SparkSession, cfg: PipelineConfig) -> list[Any]:
    if not Path(cfg.quarantine_dir).exists():
        return []
    return spark.read.parquet(cfg.quarantine_dir).collect()
