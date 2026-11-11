"""Pipeline configuration: JSON file + CLI overrides, no secrets."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from typing import Any

MODES = ("optimized", "baseline")
FAIL_STEPS = ("validate", "stage", "publish")


@dataclass(frozen=True)
class PipelineConfig:
    # Input layout: <input_dir>/customers.csv, products.csv, orders/*.csv
    input_dir: str = "data/sample/input"
    orders_pattern: str = "orders/*.csv"
    customers_file: str = "customers.csv"
    products_file: str = "products.csv"
    # Outputs
    output_dir: str = "data/output/sample/orders"
    quarantine_dir: str = "data/output/sample/quarantine"
    metrics_dir: str = "data/output/sample/run_metrics"
    staging_dir: str | None = None     # default: <output_dir>_staging (single-writer assumption)
    # Execution
    mode: str = "optimized"            # "optimized" or "baseline" (see docs/performance_report.md)
    shuffle_partitions: int = 8        # used by optimized mode only; baseline keeps Spark default 200
    run_id: str | None = None          # generated when not supplied
    # Business rules
    max_quantity: int = 1000
    max_amount: str = "1000000.00"     # inclusive bound on abs(order_amount) and unit_price
    # Test hook: raise right after this step to simulate a crash ("validate", "stage", "publish")
    fail_at_step: str | None = None

    def validate(self) -> "PipelineConfig":
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {self.mode!r}")
        if self.shuffle_partitions < 1:
            raise ValueError("shuffle_partitions must be >= 1")
        if self.fail_at_step is not None and self.fail_at_step not in FAIL_STEPS:
            raise ValueError(f"fail_at_step must be one of {FAIL_STEPS}")
        return self

    @property
    def staging_root(self) -> str:
        return self.staging_dir or f"{self.output_dir.rstrip('/')}_staging"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_config(path: str | Path | None = None, **overrides: Any) -> PipelineConfig:
    """Load a JSON config file (optional) and apply non-None keyword overrides."""
    cfg = PipelineConfig()
    known = {f.name for f in fields(PipelineConfig)}
    if path:
        data = json.loads(Path(path).read_text())
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        cfg = replace(cfg, **data)
    cfg = replace(cfg, **{k: v for k, v in overrides.items() if v is not None and k in known})
    return cfg.validate()
