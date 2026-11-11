"""Benchmark the baseline vs optimized job on the same dataset.

    python -m ecom_pipeline.benchmark --input-dir data/generated/rows_1000000/input --iterations 3

For every measured iteration both variants run once (order alternates each iteration to
avoid favouring one of them with warm OS caches). Each run writes to a fresh output
folder, so the merge step reads no existing data and both variants do identical work.

Collected per run:
  * wall-clock seconds of run_pipeline (ingest -> publish)
  * shuffle read / write bytes, #stages, #tasks -- from Spark's monitoring REST API
    (/api/v1/applications/<id>/stages), summed over the stages that ran during that run
  * number of Parquet files written to the target (small-file indicator)
  * row count + an order-independent content checksum (must be equal across variants)

Results: docs/benchmark_results.json and docs/performance_report.md.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import re
import shutil
import statistics
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecom_pipeline import ingest, transform, validate
from ecom_pipeline.config import PipelineConfig, load_config
from ecom_pipeline.pipeline import run_pipeline
from ecom_pipeline.spark_session import apply_mode, build_spark

log = logging.getLogger("ecom_pipeline.benchmark")
MODES = ("baseline", "optimized")
PLAN_OPERATORS = ("FileScan", "Exchange", "SortMergeJoin", "BroadcastHashJoin", "BroadcastExchange",
                  "Aggregate", "Window", "WindowGroupLimit")


# ---------------------------------------------------------------- Spark REST metrics
def _get_json(url: str) -> Any:
    # Bypass any HTTP(S)_PROXY environment variables: the Spark UI is local.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=30) as resp:
        return json.loads(resp.read())


def stage_metrics(spark: SparkSession) -> dict[int, dict[str, int]]:
    """All completed stage attempts so far, keyed by stageId (attempts summed)."""
    sc = spark.sparkContext
    stages = _get_json(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages?status=complete")
    out: dict[int, dict[str, int]] = {}
    for s in stages:
        acc = out.setdefault(s["stageId"], {"shuffle_read": 0, "shuffle_write": 0, "tasks": 0})
        acc["shuffle_read"] += int(s.get("shuffleReadBytes", 0))
        acc["shuffle_write"] += int(s.get("shuffleWriteBytes", 0))
        acc["tasks"] += int(s.get("numTasks", 0))
    return out


def wait_for_listener(spark: SparkSession, timeout_s: float = 30.0) -> None:
    """The UI is fed by an async listener; wait until no stage is reported active."""
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages?status=active"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if not _get_json(url):
            time.sleep(1.0)  # one more beat for the last 'stage completed' events
            return
        time.sleep(0.5)


# ---------------------------------------------------------------- plan inspection
def plan_summary(spark: SparkSession, cfg: PipelineConfig) -> dict[str, int]:
    """Count key physical operators in the plan of the validate+dedup DataFrame."""
    apply_mode(spark, cfg.mode, cfg.shuffle_partitions)
    files = ingest.list_order_files(cfg.input_dir, cfg.orders_pattern)
    ing = ingest.read_orders(spark, files)
    checked = validate.validate_orders(
        ing.raw,
        ingest.read_customers(spark, str(Path(cfg.input_dir) / cfg.customers_file)),
        ingest.read_products(spark, str(Path(cfg.input_dir) / cfg.products_file)),
        cfg, broadcast=cfg.mode == "optimized")
    typed = transform.select_typed(checked.filter(F.col("is_valid")), prune=cfg.mode == "optimized")
    df = transform.build_target(transform.dedup_latest(typed, cfg.mode), "plan", datetime.now(timezone.utc))
    plan = df._jdf.queryExecution().executedPlan().toString()  # noqa: SLF001 - plan text only
    counts = {op: 0 for op in PLAN_OPERATORS}
    for line in plan.splitlines():
        # "   :  +- *(9) SortMergeJoin [..]" -> "SortMergeJoin"
        name = re.sub(r"^[\s:+\-]*(\*\(\d+\)\s*)?", "", line).split(" ")[0].split("(")[0]
        if name in ("HashAggregate", "SortAggregate", "ObjectHashAggregate"):
            name = "Aggregate"
        if name in counts:
            counts[name] += 1
    return counts


# ---------------------------------------------------------------- one measured run
def target_fingerprint(spark: SparkSession, path: str) -> dict[str, Any]:
    df = spark.read.parquet(path).drop("run_id", "processed_at")
    cols = sorted(df.columns)
    row = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("checksum"),
        F.sum("order_amount").alias("total_amount"),
    ).first()
    return {"rows": row["rows"], "distinct_orders": row["distinct_orders"],
            "checksum": str(row["checksum"]), "total_amount": str(row["total_amount"])}


def run_once(spark: SparkSession, base: PipelineConfig, mode: str, work: Path, tag: str) -> dict[str, Any]:
    out = work / f"{mode}_{tag}"
    shutil.rmtree(out, ignore_errors=True)
    cfg = load_config(None, **{**base.to_dict(), "mode": mode,
                               "output_dir": str(out / "orders"),
                               "quarantine_dir": str(out / "quarantine"),
                               "metrics_dir": str(out / "run_metrics"),
                               "staging_dir": None, "run_id": None})
    before = set(stage_metrics(spark))
    t0 = time.perf_counter()
    m = run_pipeline(spark, cfg)
    wall = time.perf_counter() - t0
    wait_for_listener(spark)
    new = {k: v for k, v in stage_metrics(spark).items() if k not in before}
    fp = target_fingerprint(spark, cfg.output_dir)
    files = len(list(Path(cfg.output_dir).rglob("*.parquet")))
    shutil.rmtree(out, ignore_errors=True)  # keep disk usage flat
    return {
        "mode": mode, "tag": tag, "wall_seconds": round(wall, 2),
        "shuffle_read_bytes": sum(v["shuffle_read"] for v in new.values()),
        "shuffle_write_bytes": sum(v["shuffle_write"] for v in new.values()),
        "stages": len(new), "tasks": sum(v["tasks"] for v in new.values()),
        "target_files": files, "input_count": m.input_count, "output_count": m.output_count,
        "rejected_count": m.rejected_count, "fingerprint": fp,
    }


# ---------------------------------------------------------------- report
def _mb(b: float) -> str:
    return f"{b / 1024 / 1024:,.1f} MB"


def summarize(runs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for mode in MODES:
        rs = [r for r in runs if r["mode"] == mode and r["tag"] != "warmup"]
        walls = [r["wall_seconds"] for r in rs]
        out[mode] = {
            "runs": len(rs),
            "wall_median": round(statistics.median(walls), 2),
            "wall_mean": round(statistics.mean(walls), 2),
            "wall_min": min(walls), "wall_max": max(walls),
            "shuffle_read_bytes": int(statistics.median(r["shuffle_read_bytes"] for r in rs)),
            "shuffle_write_bytes": int(statistics.median(r["shuffle_write_bytes"] for r in rs)),
            "stages": int(statistics.median(r["stages"] for r in rs)),
            "tasks": int(statistics.median(r["tasks"] for r in rs)),
            "target_files": int(statistics.median(r["target_files"] for r in rs)),
        }
    return out


def write_report(path: Path, meta: dict[str, Any], runs: list[dict[str, Any]],
                 summary: dict[str, dict[str, Any]], plans: dict[str, dict[str, int]],
                 equal: bool) -> None:
    b, o = summary["baseline"], summary["optimized"]
    speedup = b["wall_median"] / o["wall_median"] if o["wall_median"] else float("nan")
    fp = runs[-1]["fingerprint"]
    lines = [
        "# Performance report: baseline vs optimized",
        "",
        f"Generated by `make benchmark` on {meta['generated_at']}. All numbers below were measured on",
        "the machine described here. Nothing is estimated. Re-run the benchmark to reproduce them on yours.",
        "",
        "## Environment and dataset",
        "",
        "| Item | Value |",
        "|---|---|",
        f"| CPU cores (Spark `local[*]`) | {meta['cpu_count']} |",
        f"| Driver memory | {meta['driver_memory']} |",
        f"| Spark / Python / Java | {meta['spark_version']} / {meta['python']} / {meta['java']} |",
        f"| Input | `{meta['input_dir']}`: {meta['input_files']} order files, {_mb(meta['input_bytes'])} CSV |",
        f"| Input rows | {runs[0]['input_count']:,} (rejected {runs[0]['rejected_count']:,}, "
        f"written {runs[0]['output_count']:,}) |",
        f"| Iterations | {meta['iterations']} measured per variant (+{meta['warmup']} warm-up each, "
        "discarded), order alternated |",
        "",
        "## Results (median of measured runs)",
        "",
        "| Metric | Baseline | Optimized | Change |",
        "|---|---:|---:|---:|",
        f"| Wall-clock (s) | {b['wall_median']} | {o['wall_median']} | **{speedup:.2f}x faster** |",
        f"| Wall-clock min / max (s) | {b['wall_min']} / {b['wall_max']} | {o['wall_min']} / {o['wall_max']} | |",
        f"| Shuffle write | {_mb(b['shuffle_write_bytes'])} | {_mb(o['shuffle_write_bytes'])} | "
        f"{_pct(b['shuffle_write_bytes'], o['shuffle_write_bytes'])} |",
        f"| Shuffle read | {_mb(b['shuffle_read_bytes'])} | {_mb(o['shuffle_read_bytes'])} | "
        f"{_pct(b['shuffle_read_bytes'], o['shuffle_read_bytes'])} |",
        f"| Stages | {b['stages']} | {o['stages']} | |",
        f"| Tasks | {b['tasks']:,} | {o['tasks']:,} | {_pct(b['tasks'], o['tasks'])} |",
        f"| Parquet files in target | {b['target_files']:,} | {o['target_files']:,} | |",
        "",
        "### Output equality",
        "",
        f"* Row count, distinct `order_id`, `sum(order_amount)` and a content checksum "
        f"(sum of `xxhash64` over all non-lineage columns) are **{'identical' if equal else 'DIFFERENT'}** "
        "for every run of both variants.",
        f"* rows = {fp['rows']:,}, distinct orders = {fp['distinct_orders']:,}, "
        f"total amount = {fp['total_amount']}, checksum = {fp['checksum']}",
        "",
        "### Physical plan of validate + dedup (operator counts)",
        "",
        "Counted from `executedPlan` of the validate -> dedup -> target DataFrame *without* the cache "
        "the pipeline adds. In the baseline, the `groupBy` + join-back dedup is a self-join, so "
        "the plan contains the CSV scan and both dimension joins **twice**. In the real run the "
        "validated rows are cached, so the second branch reads from memory but still pays the extra "
        "shuffles. `Exchange` = one shuffle. `Aggregate` includes the dimension `dropDuplicates`.",
        "",
        "| Operator | Baseline | Optimized |",
        "|---|---:|---:|",
        *[f"| `{op}` | {plans['baseline'][op]} | {plans['optimized'][op]} |" for op in plans["baseline"]],
        "",
        "### All runs",
        "",
        "| Variant | Run | Wall (s) | Shuffle write | Shuffle read | Stages | Tasks | Files |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
        *[f"| {r['mode']} | {r['tag']} | {r['wall_seconds']} | {_mb(r['shuffle_write_bytes'])} | "
          f"{_mb(r['shuffle_read_bytes'])} | {r['stages']} | {r['tasks']:,} | {r['target_files']:,} |"
          for r in runs],
        "",
        "## What differs between the two jobs",
        "",
        "| Technique | Baseline | Optimized |",
        "|---|---|---|",
        "| Dimension joins (RI check + enrichment) | sort-merge join: the 1M-row fact is shuffled "
        "and sorted for **each** of the 2 joins | `broadcast()` hint: customers/products are copied "
        "to every task and the fact is not shuffled |",
        "| `spark.sql.shuffle.partitions` | 200 (default), AQE off | "
        f"{meta['optimized_shuffle_partitions']}, AQE on (coalesces small partitions) |",
        "| Column pruning | raw string columns + `raw_record` JSON carried through dedup | only the "
        "14 typed columns needed downstream are selected **before** the dedup shuffle |",
        "| Dedup | `groupBy(max(struct))` + join back + `dropDuplicates`: 3 wide steps | one "
        "`row_number()` window: 1 shuffle |",
        "| Write | no repartition: every shuffle partition writes a file per `order_date` | "
        "`repartition(order_date)`: one file per partition directory |",
        "",
        "## Why the optimized job is faster",
        "",
        "1. **Broadcast joins remove the biggest shuffles.** The dimensions are tiny (thousands of "
        "rows), but a sort-merge join has to hash-partition and sort the *fact* table on the join key. "
        "Broadcasting the dimensions turns both joins into map-side hash lookups.",
        "2. **Fewer bytes per shuffle.** Pruning to typed columns before the dedup means the "
        "`raw_record` JSON and raw strings (the widest columns) are never written to shuffle "
        "files. That shows up directly in the shuffle write/read rows above.",
        "3. **Fewer wide transformations.** The window-based dedup needs one exchange instead of three.",
        "4. **Right-sized partitions.** On a 4-core laptop, 200 shuffle partitions mean hundreds of "
        "tiny tasks per stage (scheduling overhead) and, at write time, hundreds of small Parquet "
        "files. Sized partitions plus AQE coalescing and `repartition(order_date)` give one file per "
        "day partition, which is also faster for downstream readers.",
        "",
        "Stage counts are similar because both variants share the cache, count and write steps, "
        "and AQE adds its own stages. The difference is in the work *inside* the stages: tasks and "
        "shuffled bytes. The publish step (merge-by-key, staging, swap) is the same code in both "
        "variants and is not part of the comparison.",
        "",
        "Caveats: local mode on a single machine. Shuffle data never crosses a network here, so on a "
        "real cluster the gap from avoiding shuffles is usually *larger*. Timings vary a few seconds "
        "between runs (see min/max), which is why medians over several runs are reported.",
        "",
    ]
    path.write_text("\n".join(lines))


def _pct(before: float, after: float) -> str:
    if not before:
        return ""
    return f"{(after - before) / before * 100:+.0f}%"


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Benchmark baseline vs optimized pipeline")
    p.add_argument("--input-dir", default="data/generated/rows_1000000/input")
    p.add_argument("--iterations", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--shuffle-partitions", type=int, default=8)
    p.add_argument("--driver-memory", default="4g")
    p.add_argument("--work-dir", default="data/output/benchmark")
    p.add_argument("--report", default="docs/performance_report.md")
    p.add_argument("--results-json", default="docs/benchmark_results.json")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    logging.getLogger("ecom_pipeline").setLevel(logging.WARNING)
    log.setLevel(logging.INFO)

    if not Path(a.input_dir).is_dir():
        raise SystemExit(f"{a.input_dir} not found -- run `make generate-large` first")
    spark = build_spark(app_name="ecom-benchmark", ui_enabled=True, driver_memory=a.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")
    base = load_config(None, input_dir=a.input_dir, shuffle_partitions=a.shuffle_partitions)
    work = Path(a.work_dir)
    order_files = ingest.list_order_files(a.input_dir, base.orders_pattern)
    meta = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cpu_count": os.cpu_count(), "driver_memory": a.driver_memory,
        "spark_version": pyspark.__version__, "python": platform.python_version(),
        "java": spark.sparkContext._jvm.System.getProperty("java.version"),  # noqa: SLF001
        "input_dir": a.input_dir, "input_files": len(order_files),
        "input_bytes": sum(Path(f).stat().st_size for f in order_files),
        "iterations": a.iterations, "warmup": a.warmup,
        "optimized_shuffle_partitions": a.shuffle_partitions,
    }
    try:
        plans = {m: plan_summary(spark, load_config(None, **{**base.to_dict(), "mode": m})) for m in MODES}
        runs: list[dict[str, Any]] = []
        schedule = [(m, "warmup") for _ in range(a.warmup) for m in MODES]
        for i in range(a.iterations):
            order = MODES if i % 2 == 0 else tuple(reversed(MODES))
            schedule += [(m, f"run{i + 1}") for m in order]
        for mode, tag in schedule:
            r = run_once(spark, base, mode, work, tag)
            log.info("%-9s %-7s wall=%6.2fs shuffle_w=%s stages=%d tasks=%d files=%d rows=%d",
                     mode, tag, r["wall_seconds"], _mb(r["shuffle_write_bytes"]), r["stages"],
                     r["tasks"], r["target_files"], r["fingerprint"]["rows"])
            runs.append(r)
    finally:
        spark.stop()

    equal = len({json.dumps(r["fingerprint"], sort_keys=True) for r in runs}) == 1
    summary = summarize(runs)
    Path(a.results_json).write_text(json.dumps(
        {"meta": meta, "summary": summary, "plans": plans, "outputs_equal": equal, "runs": runs},
        indent=2) + "\n")
    write_report(Path(a.report), meta, runs, summary, plans, equal)
    log.info("summary: %s | outputs_equal=%s", json.dumps(summary), equal)
    log.info("report written to %s", a.report)
    if not equal:
        raise SystemExit("baseline and optimized outputs differ!")


if __name__ == "__main__":
    main()
