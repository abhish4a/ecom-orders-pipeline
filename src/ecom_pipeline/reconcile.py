"""Reconciliation: prove the target + quarantine account for every source row.

    python -m ecom_pipeline.reconcile --config conf/sample.json [--expected-dir data/sample/expected]

The SOURCE side is recomputed in plain Python (csv + Decimal) and does not reuse any
pipeline code, so it is an independent check:

  * input rows        = rows in the order CSV files
  * rejected          = rows in the quarantine (matched back to source rows by source_file+event_id)
  * deduped           = valid source rows - distinct valid order_ids
  * output            = rows in the target
  * CHECK             input == output + rejected + deduped
  * CHECK             sum(order_amount) and count per order_date: source (dedup recomputed in
                      Python, latest event_ts wins) == target [== generator expectation if given]
  * CHECK             order_id unique, lineage columns never null, partition matches order_ts

Writes a Markdown report (default docs/reconciliation_report.md) plus JSON, and runs the
sample SQL queries from docs/sample_queries.sql against the outputs.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecom_pipeline.config import PipelineConfig, load_config
from ecom_pipeline.ingest import list_order_files
from ecom_pipeline.metrics import read_history
from ecom_pipeline.spark_session import build_spark

SQL_FILE = Path("docs/sample_queries.sql")


@dataclass
class SourceSide:
    rows_per_file: dict[str, int] = field(default_factory=dict)
    input_rows: int = 0
    valid_rows: int = 0
    daily: dict[str, tuple[int, Decimal]] = field(default_factory=dict)
    distinct_valid_orders: int = 0


def source_side(files: list[str], rejected_keys: Counter) -> SourceSide:
    """Independent recomputation from the raw CSV files."""
    s = SourceSide()
    remaining = Counter(rejected_keys)
    winners: dict[str, tuple[str, str, str, str, str]] = {}   # order_id -> (event_ts, file, event_id, order_ts, amount)
    for path in files:
        name = Path(path).name
        with open(path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        s.rows_per_file[name] = len(rows)
        s.input_rows += len(rows)
        for r in rows:
            key = (name, (r.get("event_id") or "").strip() or None)
            if remaining[key] > 0:          # this source row is accounted for by the quarantine
                remaining[key] -= 1
                continue
            s.valid_rows += 1
            cand = (r["event_ts"].strip(), name, r["event_id"].strip(),
                    r["order_ts"].strip(), r["order_amount"].strip())
            oid = r["order_id"].strip()
            # Same rule as the pipeline, written independently: latest event_ts, then file, then id.
            # (Timestamps are fixed-format ISO strings in UTC 'Z', so string order == time order.)
            if oid not in winners or cand[:3] > winners[oid][:3]:
                winners[oid] = cand
    s.distinct_valid_orders = len(winners)
    for _, _, _, order_ts, amount in winners.values():
        day = order_ts[:10]
        n, total = s.daily.get(day, (0, Decimal("0")))
        s.daily[day] = (n + 1, total + Decimal(amount))
    unmatched = +remaining
    if unmatched:
        raise ValueError(f"quarantine rows not found in source: {dict(unmatched)}")
    return s


def load_expected(expected_dir: Path | None) -> dict[str, tuple[int, Decimal]] | None:
    if not expected_dir or not (expected_dir / "expected_daily_totals.csv").exists():
        return None
    with (expected_dir / "expected_daily_totals.csv").open() as fh:
        return {r["order_date"]: (int(r["order_count"]), Decimal(r["total_order_amount"]))
                for r in csv.DictReader(fh)}


def split_sql(text: str) -> list[tuple[str, str]]:
    """Split docs/sample_queries.sql into (title, sql) using '-- name:' markers."""
    out: list[tuple[str, str]] = []
    for block in re.split(r"^-- name: ", text, flags=re.M)[1:]:
        title, _, body = block.partition("\n")
        out.append((title.strip(), body.strip().rstrip(";")))
    return out


def _md_table(rows: list[dict[str, Any]], limit: int = 15) -> list[str]:
    if not rows:
        return ["_(no rows)_"]
    cols = list(rows[0])
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows[:limit]]
    if len(rows) > limit:
        lines.append(f"\n_{len(rows) - limit} more rows not shown_")
    return lines


def reconcile(spark: SparkSession, cfg: PipelineConfig, expected_dir: Path | None) -> dict[str, Any]:
    files = list_order_files(cfg.input_dir, cfg.orders_pattern)
    target = spark.read.parquet(cfg.output_dir)
    quarantine = spark.read.parquet(cfg.quarantine_dir)
    q_rows = quarantine.select("source_file", "event_id").collect()
    src = source_side(files, Counter((r["source_file"], r["event_id"]) for r in q_rows))

    output_rows = target.count()
    rejected = len(q_rows)
    deduped = src.valid_rows - src.distinct_valid_orders
    tgt_daily = {str(r["order_date"]): (r["n"], r["amt"]) for r in target.groupBy("order_date")
                 .agg(F.count("*").alias("n"), F.sum("order_amount").alias("amt")).collect()}
    expected = load_expected(expected_dir)
    integrity = target.agg(
        (F.count("*") - F.countDistinct("order_id")).alias("duplicate_order_ids"),
        F.sum(F.when(F.col("run_id").isNull() | F.col("source_file").isNull(), 1).otherwise(0))
        .alias("rows_missing_lineage"),
        F.sum(F.when(F.to_date("order_ts") != F.col("order_date"), 1).otherwise(0))
        .alias("rows_in_wrong_partition"),
    ).first().asDict()
    reasons = {r["reject_reason"]: r["count"] for r in
               quarantine.groupBy("reject_reason").count().orderBy(F.desc("count"), "reject_reason").collect()}
    rejects_per_file = {r["source_file"]: r["count"] for r in quarantine.groupBy("source_file").count().collect()}

    days = sorted(set(src.daily) | set(tgt_daily) | set(expected or {}))
    daily_rows = []
    for d in days:
        s_n, s_amt = src.daily.get(d, (0, Decimal("0")))
        t_n, t_amt = tgt_daily.get(d, (0, Decimal("0")))
        row = {"order_date": d, "source_orders": s_n, "target_orders": t_n,
               "source_sum_order_amount": f"{s_amt:.2f}", "target_sum_order_amount": f"{t_amt:.2f}",
               "diff": f"{t_amt - s_amt:.2f}"}
        if expected is not None:
            e_n, e_amt = expected.get(d, (0, Decimal("0")))
            row["expected_sum_order_amount"] = f"{e_amt:.2f}"
        row["match"] = "✅" if (s_n, s_amt) == (t_n, t_amt) and (
            expected is None or expected.get(d, (0, Decimal("0"))) == (t_n, t_amt)) else "❌"
        daily_rows.append(row)

    checks = {
        "input == output + rejected + deduped": src.input_rows == output_rows + rejected + deduped,
        "output == distinct valid source orders": output_rows == src.distinct_valid_orders,
        "daily count + sum(order_amount): source == target": all(r["match"] == "✅" for r in daily_rows),
        "order_id unique in target": integrity["duplicate_order_ids"] == 0,
        "run_id / source_file never null": integrity["rows_missing_lineage"] == 0,
        "order_date partition == date(order_ts)": integrity["rows_in_wrong_partition"] == 0,
    }
    if expected is not None:
        summary = json.loads((expected_dir / "expected_summary.json").read_text())  # type: ignore[operator]
        checks["counts == generator expectation"] = (
            (output_rows, rejected, deduped)
            == (summary["expected_output_rows"], summary["expected_rejected"], summary["expected_deduped"]))

    # Sample SQL queries against temp views.
    target.createOrReplaceTempView("orders")
    quarantine.createOrReplaceTempView("quarantine")
    queries = []
    if SQL_FILE.exists():
        for title, sql in split_sql(SQL_FILE.read_text()):
            rows = [r.asDict() for r in spark.sql(sql).collect()]
            queries.append({"title": title, "sql": sql, "rows": rows})

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "config": {k: cfg.to_dict()[k] for k in ("input_dir", "output_dir", "quarantine_dir", "metrics_dir")},
        "runs": [{k: h.get(k) for k in ("run_id", "status", "input_count", "output_count", "rejected_count",
                                         "duplicate_count", "stale_count", "reconciled", "failure_reason")}
                 for h in read_history(cfg.metrics_dir)],
        "counts": {"input_rows": src.input_rows, "output_rows": output_rows, "rejected_rows": rejected,
                   "deduped_rows": deduped, "valid_source_rows": src.valid_rows},
        "per_file": [{"source_file": f, "input_rows": n, "rejected_rows": rejects_per_file.get(f, 0)}
                     for f, n in src.rows_per_file.items()],
        "reject_reasons": reasons,
        "integrity": integrity,
        "daily": daily_rows,
        "checks": checks,
        "queries": queries,
    }


def write_report(res: dict[str, Any], path: Path) -> None:
    c = res["counts"]
    ok = all(res["checks"].values())
    lines = [
        "# Reconciliation report",
        "",
        f"Generated by `make reconcile` on {res['generated_at']} against "
        f"`{res['config']['output_dir']}` (input `{res['config']['input_dir']}`).",
        "",
        f"**Overall: {'PASS ✅' if ok else 'FAIL ❌'}**",
        "",
        "## Checks",
        "",
        "| Check | Result |",
        "|---|---|",
        *[f"| {k} | {'✅ pass' if v else '❌ FAIL'} |" for k, v in res["checks"].items()],
        "",
        "## Row counts: input = output + rejects + deduped",
        "",
        "| Input rows (CSV) | Output rows (target) | Rejected (quarantine) | Deduped | "
        "Output + Rejected + Deduped |",
        "|---:|---:|---:|---:|---:|",
        f"| {c['input_rows']:,} | {c['output_rows']:,} | {c['rejected_rows']:,} | {c['deduped_rows']:,} | "
        f"{c['output_rows'] + c['rejected_rows'] + c['deduped_rows']:,} |",
        "",
        "`deduped` = valid source rows that are not the winning (latest `event_ts`) event of their "
        "`order_id`: exact redeliveries plus superseded status updates, including out-of-order "
        "stale events.",
        "",
        "### Per source file",
        "",
        *_md_table(res["per_file"], limit=50),
        "",
        "### Reject reasons",
        "",
        *_md_table([{"reject_reason": k, "rows": v} for k, v in res["reject_reasons"].items()], limit=50),
        "",
        "## Key aggregate: sum(order_amount) by order_date, source vs target",
        "",
        "Source = raw CSV rows minus quarantined rows, deduplicated in plain Python (independent "
        "of the Spark code)."
        + (" Expected = generator's oracle (`expected_daily_totals.csv`)." if res["daily"] and
           "expected_sum_order_amount" in res["daily"][0] else ""),
        "",
        *_md_table(res["daily"], limit=60),
        "",
        "## Integrity",
        "",
        *_md_table([res["integrity"]]),
        "",
        "## Run history (run_metrics.jsonl)",
        "",
        *_md_table(res["runs"], limit=20),
        "",
        "## Sample SQL queries",
        "",
        "Run against temp views `orders` (target) and `quarantine`. Source: `docs/sample_queries.sql`.",
        "",
    ]
    for q in res["queries"]:
        lines += [f"### {q['title']}", "", "```sql", q["sql"], "```", "", *_md_table(q["rows"], limit=10), ""]
    path.write_text("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Reconcile pipeline outputs against source files")
    p.add_argument("--config", default="conf/sample.json")
    p.add_argument("--expected-dir", type=Path, help="generator expected/ folder (optional oracle)")
    p.add_argument("--report", type=Path, default=Path("docs/reconciliation_report.md"))
    p.add_argument("--results-json", type=Path, default=Path("docs/reconciliation_results.json"))
    a = p.parse_args(argv)
    cfg = load_config(a.config)
    spark = build_spark(app_name="ecom-reconcile")
    try:
        res = reconcile(spark, cfg, a.expected_dir)
    finally:
        spark.stop()
    write_report(res, a.report)
    a.results_json.write_text(json.dumps(res, indent=2, default=str) + "\n")
    for k, v in res["checks"].items():
        print(f"{'PASS' if v else 'FAIL'}  {k}")
    print(f"report: {a.report}")
    return 0 if all(res["checks"].values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
