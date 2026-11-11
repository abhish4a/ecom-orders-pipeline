"""Run metrics: one JSON document per run + an append-only JSONL history.

    <metrics_dir>/<run_id>.json        latest state of that run (RUNNING -> SUCCESS | FAILED)
    <metrics_dir>/run_metrics.jsonl    one line per finished run (query with spark.read.json)

Counts reconcile per run:
    input_count = output_count + rejected_count + duplicate_count + stale_count
where duplicate_count = valid rows that lost the in-batch dedup and stale_count = batch
winners that lost to a newer row already in the target.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class RunMetrics:
    run_id: str
    mode: str
    start_time: str
    job_name: str = "ecom_orders_pipeline"
    status: str = "RUNNING"            # RUNNING | SUCCESS | FAILED
    end_time: str | None = None
    duration_seconds: float | None = None
    input_files: list[str] = field(default_factory=list)
    input_count: int = 0
    rejected_count: int = 0
    valid_count: int = 0
    duplicate_count: int = 0           # lost in-batch dedup
    stale_count: int = 0               # lost to a newer row already in the target
    output_count: int = 0              # rows of this batch now in the target
    inserted_count: int = 0
    updated_count: int = 0
    partitions_written: int = 0
    reject_reason_counts: dict[str, int] = field(default_factory=dict)
    reconciled: bool | None = None     # input == output + rejected + duplicate + stale
    failure_reason: str | None = None
    config: dict[str, Any] = field(default_factory=dict)

    def finish(self, status: str, failure_reason: str | None = None) -> None:
        end = datetime.now(timezone.utc)
        self.status = status
        self.failure_reason = failure_reason
        self.end_time = utc_iso(end)
        start = datetime.strptime(self.start_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        self.duration_seconds = round((end - start).total_seconds(), 3)
        if status == "SUCCESS":
            self.reconciled = self.input_count == (
                self.output_count + self.rejected_count + self.duplicate_count + self.stale_count)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_metrics(m: RunMetrics, metrics_dir: str, append_history: bool) -> Path:
    """Atomically write <run_id>.json; append to the JSONL history when the run is final."""
    d = Path(metrics_dir)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{m.run_id}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(m.to_dict(), indent=2, default=str) + "\n")
    os.replace(tmp, path)  # atomic rename: readers never see a half-written file
    if append_history:
        with (d / "run_metrics.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(m.to_dict(), default=str) + "\n")
    return path


def read_history(metrics_dir: str) -> list[dict[str, Any]]:
    p = Path(metrics_dir) / "run_metrics.jsonl"
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]
