"""Reproducible synthetic data generator for the e-commerce orders pipeline.

Pure standard library (no Spark needed). The same code path builds both the
small committed sample (``--rows 150``) and large benchmark datasets
(``--rows 1000000``); the seed makes every run byte-for-byte reproducible.

What it writes under ``--out``::

    input/customers.csv                 dimension (clean)
    input/products.csv                  dimension (clean)
    input/orders/orders_batch_001.csv   order events, arrival order
    input/orders/orders_batch_00N.csv   ... last batch also holds late / out-of-order events
    input/orders/orders_batch_00M_schema_v2.csv   changed schema (renamed + extra column)
    expected/expected_outcomes.csv      one row per record that must NOT reach the target
    expected/expected_summary.json      expected counts (input = output + rejected + deduped)
    expected/expected_daily_totals.csv  expected sum(order_amount) per order_date in the target

Labels live only under ``expected/`` -- never inside the input files.

Every order *event* is a row. An order can have several events (status
updates); the pipeline must keep the one with the latest ``event_ts``.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

ORDER_COLUMNS_V1: list[str] = [
    "event_id", "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "order_amount", "currency", "order_status", "order_ts", "event_ts",
]
# Schema drift: order_amount renamed to total_amount and a new coupon_code column.
ORDER_COLUMNS_V2: list[str] = [
    "event_id", "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "total_amount", "currency", "order_status", "order_ts", "event_ts", "coupon_code",
]
CUSTOMER_COLUMNS: list[str] = ["customer_id", "customer_name", "email", "country", "signup_date"]
PRODUCT_COLUMNS: list[str] = ["product_id", "product_name", "category", "list_price"]

# Defect kind -> reject_reason code the pipeline is expected to emit.
# Each defective row carries exactly ONE defect so the expected reason is unambiguous.
DEFECTS: dict[str, str] = {
    "missing_order_id": "MISSING_REQUIRED:order_id",
    "missing_customer_id": "MISSING_REQUIRED:customer_id",
    "missing_order_ts": "MISSING_REQUIRED:order_ts",
    "missing_event_ts": "MISSING_REQUIRED:event_ts",
    "impossible_date": "INVALID_TIMESTAMP:order_ts",
    "garbage_date": "INVALID_TIMESTAMP:order_ts",
    "non_numeric_quantity": "INVALID_TYPE:quantity",
    "malformed_amount": "INVALID_TYPE:order_amount",
    "quantity_zero": "OUT_OF_RANGE:quantity",
    "quantity_too_large": "OUT_OF_RANGE:quantity",
    "negative_amount": "NEGATIVE_AMOUNT",
    "positive_refund": "POSITIVE_REFUND",
    "invalid_status": "INVALID_STATUS",
    "unknown_customer": "UNKNOWN_CUSTOMER",
    "unknown_product": "UNKNOWN_PRODUCT",
    "order_before_signup": "ORDER_BEFORE_SIGNUP",
}
SCHEMA_MISMATCH_REASON = "SCHEMA_MISMATCH"
SUPERSEDED_REASON = "SUPERSEDED_BY_NEWER_EVENT"
EXACT_DUP_REASON = "EXACT_DUPLICATE"

# Valid boundary values the pipeline must ACCEPT.
BOUNDARY_KINDS: list[str] = [
    "qty_min", "qty_max", "amount_zero", "amount_max", "ts_window_start", "ts_window_end",
]
MAX_QUANTITY = 1000
MAX_AMOUNT_CENTS = 100_000_000  # 1,000,000.00

FIRST_NAMES = ["Ava", "Liam", "Noah", "Emma", "Mia", "Arjun", "Sofia", "Kenji", "Lena", "Omar"]
LAST_NAMES = ["Smith", "Patel", "Garcia", "Mueller", "Tanaka", "Rossi", "Kim", "Silva", "Brown"]
COUNTRIES = ["US", "GB", "DE", "IN", "FR", "CA", "AU", "JP"]
CATEGORIES = ["electronics", "books", "home", "toys", "fashion", "grocery", "sports"]
NEXT_STATUS = {"PLACED": "SHIPPED", "SHIPPED": "DELIVERED"}


@dataclass
class Event:
    """One source row plus generator-only bookkeeping (never written to input files)."""

    row: dict[str, str | None]
    arrival_ts: datetime          # used to sort events into arrival batches
    true_order_id: str
    label: tuple[str, str] | None = None   # (expected_outcome, expected_reason)
    source_file: str = ""


@dataclass
class Dataset:
    customers: list[dict[str, str]]
    products: list[dict[str, str]]
    files: dict[str, tuple[list[str], list[Event]]] = field(default_factory=dict)
    final_rows: dict[str, dict[str, str | None]] = field(default_factory=dict)


def iso_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_cents(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    c = abs(cents)
    return f"{sign}{c // 100}.{c % 100:02d}"


def make_event_id(order_id: str, status: str, event_ts: str) -> str:
    """Deterministic event id: hash of the event's business content."""
    return "E" + hashlib.sha256(f"{order_id}|{status}|{event_ts}".encode()).hexdigest()[:20]


def random_date(rng: random.Random, lo: date, hi: date) -> date:
    return lo + timedelta(days=rng.randint(0, (hi - lo).days))


def build_dimensions(
    rng: random.Random, rows: int, start: date, end: date
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[str], list[str]]:
    """Return customers, products, ids usable for valid orders, and late-signup customer ids."""
    n_customers = max(50, min(rows // 200, 20_000))
    n_products = max(20, min(rows // 2_000, 1_000))
    customers: list[dict[str, str]] = []
    for i in range(n_customers):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        # Customer 0 signs up exactly on the window start (boundary: same-day order is valid).
        signup = start if i == 0 else random_date(rng, date(2024, 1, 1), date(2025, 12, 31))
        customers.append({
            "customer_id": f"C{i:06d}",
            "customer_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "country": rng.choice(COUNTRIES),
            "signup_date": signup.isoformat(),
        })
    # Two customers who sign up AFTER the order window -> used for ORDER_BEFORE_SIGNUP.
    late_ids: list[str] = []
    for j in range(2):
        cid = f"C{n_customers + j:06d}"
        late_ids.append(cid)
        customers.append({
            "customer_id": cid, "customer_name": "Late Signup", "email": f"late{j}@example.com",
            "country": "US", "signup_date": (end + timedelta(days=30)).isoformat(),
        })
    products = [
        {
            "product_id": f"P{i:05d}",
            "product_name": f"Product {i}",
            "category": rng.choice(CATEGORIES),
            "list_price": fmt_cents(rng.randint(199, 49_999)),
        }
        for i in range(n_products)
    ]
    valid_customer_ids = [c["customer_id"] for c in customers[:n_customers]]
    return customers, products, valid_customer_ids, late_ids


def base_row(
    order_id: str, customer_id: str, product: dict[str, str], qty: int,
    status: str, order_ts: datetime, event_ts: datetime,
) -> dict[str, str | None]:
    price_cents = int(product["list_price"].replace(".", ""))
    return {
        "event_id": make_event_id(order_id, status, iso_ts(event_ts)),
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product["product_id"],
        "quantity": str(qty),
        "unit_price": product["list_price"],
        "order_amount": fmt_cents(qty * price_cents),
        "currency": "USD",
        "order_status": status,
        "order_ts": iso_ts(order_ts),
        "event_ts": iso_ts(event_ts),
    }


def apply_boundary(kind: str, row: dict[str, str | None], start: date, end: date,
                   boundary_customer: str) -> None:
    """Mutate a valid row into a valid-but-extreme row."""
    if kind == "qty_min":
        row["quantity"] = "1"
        row["order_amount"] = row["unit_price"]
    elif kind == "qty_max":
        row["quantity"] = str(MAX_QUANTITY)
        row["order_amount"] = fmt_cents(MAX_QUANTITY * int(str(row["unit_price"]).replace(".", "")))
    elif kind == "amount_zero":  # e.g. 100% promotion
        row["order_amount"] = "0.00"
    elif kind == "amount_max":
        row["quantity"] = "1"
        row["unit_price"] = row["order_amount"] = fmt_cents(MAX_AMOUNT_CENTS)
    elif kind == "ts_window_start":  # first second of window, on customer's signup day
        row["customer_id"] = boundary_customer
        row["order_ts"] = row["event_ts"] = iso_ts(datetime.combine(start, time(0, 0, 0), timezone.utc))
    elif kind == "ts_window_end":  # last second of window
        ts = iso_ts(datetime.combine(end, time(23, 59, 59), timezone.utc))
        row["order_ts"] = row["event_ts"] = ts
    # event_id must stay a function of the (possibly changed) business content
    row["event_id"] = make_event_id(str(row["order_id"]), str(row["order_status"]), str(row["event_ts"]))


def apply_defect(kind: str, row: dict[str, str | None], idx: int, start: date,
                 late_customer: str) -> None:
    """Inject exactly one defect. event_id is left untouched so labels can reference it."""
    if kind == "missing_order_id":
        row["order_id"] = None
    elif kind == "missing_customer_id":
        row["customer_id"] = None
    elif kind == "missing_order_ts":
        row["order_ts"] = None
    elif kind == "missing_event_ts":
        row["event_ts"] = None
    elif kind == "impossible_date":
        row["order_ts"] = f"{start.year}-02-30T10:15:00Z"
    elif kind == "garbage_date":
        row["order_ts"] = "not-a-date"
    elif kind == "non_numeric_quantity":
        row["quantity"] = "abc"
    elif kind == "malformed_amount":
        row["order_amount"] = "12.3.4"
    elif kind == "quantity_zero":
        row["quantity"] = "0"
    elif kind == "quantity_too_large":
        row["quantity"] = str(MAX_QUANTITY + 1)
    elif kind == "negative_amount":
        row["order_status"] = "PLACED"
        row["order_amount"] = "-" + str(row["order_amount"]).lstrip("-")
        if row["order_amount"] == "-0.00":
            row["order_amount"] = "-1.00"
    elif kind == "positive_refund":
        row["order_status"] = "REFUNDED"
        if row["order_amount"] == "0.00":
            row["order_amount"] = "1.00"
    elif kind == "invalid_status":
        row["order_status"] = "SHIPPPED"
    elif kind == "unknown_customer":
        row["customer_id"] = f"CX{idx:07d}"
    elif kind == "unknown_product":
        row["product_id"] = f"PX{idx:06d}"
    elif kind == "order_before_signup":
        row["customer_id"] = late_customer
    else:  # pragma: no cover - guarded by DEFECTS keys
        raise ValueError(f"unknown defect kind {kind}")


def build_dataset(
    rows: int, seed: int, start: date, days: int, rows_per_file: int,
    defect_rate: float, update_rate: float, dup_rate: float, late_rate: float,
) -> Dataset:
    if rows < 100:
        raise ValueError("--rows must be >= 100 so every edge case fits")
    rng = random.Random(seed)
    end = start + timedelta(days=days - 1)
    customers, products, valid_cids, late_cids = build_dimensions(rng, rows, start, end)
    ds = Dataset(customers=customers, products=products)

    # ---- row budget -------------------------------------------------------
    n_v2 = max(5, min(rows // 1_000, 500))
    n_v1 = rows - n_v2
    n_dups = max(2, round(n_v1 * dup_rate))
    n_updates = max(4, round(n_v1 * update_rate))
    n_base = n_v1 - n_dups - n_updates
    n_defects = max(len(DEFECTS), round(n_base * defect_rate))
    n_late = max(3, round(n_base * late_rate))
    n_ooo = max(2, round(n_updates * 0.25))      # stale event arrives after the newer one
    n_refunds = max(2, round(n_updates * 0.3))
    needed = len(BOUNDARY_KINDS) + n_defects + n_updates + n_dups + n_late
    if needed > n_base:
        raise ValueError("rates too high for the requested --rows")

    # Assign each base order exactly one role so edge cases never overlap.
    idx = list(range(n_base))
    rng.shuffle(idx)
    cut = 0

    def take(n: int) -> list[int]:
        nonlocal cut
        part = idx[cut:cut + n]
        cut += n
        return part

    boundary_idx = take(len(BOUNDARY_KINDS))
    defect_idx = take(n_defects)
    update_idx = take(n_updates)
    dup_idx = take(n_dups)
    late_idx = set(take(n_late))

    window_s = days * 86_400
    base_events: list[Event] = []
    for i in range(n_base):
        oid = f"O{i:09d}"
        if i in late_idx:  # late data: old order date (first 2 days) arriving in the last batch
            order_ts = datetime.combine(start, time(), timezone.utc) + timedelta(
                seconds=rng.randrange(min(days, 2) * 86_400))
        else:
            order_ts = datetime.combine(start, time(), timezone.utc) + timedelta(
                seconds=rng.randrange(window_s))
        event_ts = order_ts + timedelta(seconds=rng.randint(0, 300))
        status = rng.choices(["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"], [60, 20, 15, 5])[0]
        qty = rng.choices([1, 2, 3, 4, 5], [50, 25, 12, 8, 5])[0]
        row = base_row(oid, rng.choice(valid_cids), rng.choice(products), qty, status, order_ts, event_ts)
        base_events.append(Event(row=row, arrival_ts=event_ts, true_order_id=oid))

    for kind, i in zip(BOUNDARY_KINDS, boundary_idx):
        apply_boundary(kind, base_events[i].row, start, end, valid_cids[0])

    for k, i in enumerate(defect_idx):
        kinds = list(DEFECTS)
        kind = kinds[k] if k < len(kinds) else rng.choice(kinds)  # every kind at least once
        apply_defect(kind, base_events[i].row, i, start, rng.choice(late_cids))
        base_events[i].label = ("rejected", DEFECTS[kind])

    # ---- status updates (duplicate business key, newer event wins) --------
    update_events: list[Event] = []
    stale_to_last: list[Event] = []
    for k, i in enumerate(update_idx):
        base = base_events[i]
        r = base.row
        if k < n_refunds:   # DELIVERED -> REFUNDED with a negative total (valid domain case)
            r["order_status"], new_status = "DELIVERED", "REFUNDED"
            new_amount = "-" + str(r["order_amount"]).lstrip("-")
            if new_amount == "-0.00":
                new_amount = "0.00"
        else:
            if r["order_status"] not in NEXT_STATUS:
                r["order_status"] = "PLACED"
            new_status, new_amount = NEXT_STATUS[str(r["order_status"])], str(r["order_amount"])
        r["event_id"] = make_event_id(str(r["order_id"]), str(r["order_status"]), str(r["event_ts"]))
        new_event_ts = base.arrival_ts + timedelta(seconds=rng.randint(3_600, 72 * 3_600))
        new_row = dict(r)
        new_row.update({
            "order_status": new_status, "order_amount": new_amount,
            "event_ts": iso_ts(new_event_ts),
            "event_id": make_event_id(str(r["order_id"]), new_status, iso_ts(new_event_ts)),
        })
        update_events.append(Event(row=new_row, arrival_ts=new_event_ts, true_order_id=base.true_order_id))
        base.label = ("deduped", SUPERSEDED_REASON)
        if k < n_ooo:   # out-of-order: the OLDER event arrives in the last batch
            stale_to_last.append(base)

    # ---- exact duplicate redeliveries ---------------------------------------
    dup_events = [
        Event(row=dict(base_events[i].row), arrival_ts=base_events[i].arrival_ts,
              true_order_id=base_events[i].true_order_id, label=("deduped", EXACT_DUP_REASON))
        for i in dup_idx
    ]
    dup_source = {id(d): base_events[i] for d, i in zip(dup_events, dup_idx)}

    # ---- assemble arrival batches -------------------------------------------
    late_events = [base_events[i] for i in sorted(late_idx)]
    held_back = {id(e) for e in late_events + stale_to_last}
    regular = [e for e in base_events if id(e) not in held_back] + update_events
    regular.sort(key=lambda e: (e.arrival_ts, str(e.row["event_id"])))
    n_files = max(2, math.ceil(len(regular) / rows_per_file))
    chunk = math.ceil(len(regular) / n_files)
    batches: list[list[Event]] = [regular[j * chunk:(j + 1) * chunk] for j in range(n_files)]
    batches[-1].extend(late_events + stale_to_last)
    names = [f"orders_batch_{j + 1:03d}.csv" for j in range(n_files)]
    for name, batch in zip(names, batches):
        for e in batch:
            e.source_file = name
    file_index = {name: j for j, name in enumerate(names)}
    for d in dup_events:  # redelivered in the same or a later batch, at a random position
        j = rng.randint(file_index[dup_source[id(d)].source_file], n_files - 1)
        batches[j].insert(rng.randint(0, len(batches[j])), d)
        d.source_file = names[j]
    for name, batch in zip(names, batches):
        ds.files[name] = (ORDER_COLUMNS_V1, batch)

    # ---- changed-schema file: otherwise-valid new orders, all rejected ----------
    v2_name = f"orders_batch_{n_files + 1:03d}_schema_v2.csv"
    v2_events: list[Event] = []
    for j in range(n_v2):
        oid = f"O{n_base + j:09d}"
        order_ts = datetime.combine(end, time(12), timezone.utc) + timedelta(seconds=j)
        row = base_row(oid, rng.choice(valid_cids), rng.choice(products), 1, "PLACED", order_ts, order_ts)
        row["total_amount"] = row.pop("order_amount")
        row["coupon_code"] = rng.choice(["", "WELCOME10", "SPRING5"]) or None
        v2_events.append(Event(row=row, arrival_ts=order_ts, true_order_id=oid,
                               label=("rejected", SCHEMA_MISMATCH_REASON), source_file=v2_name))
    ds.files[v2_name] = (ORDER_COLUMNS_V2, v2_events)

    # ---- expected final target state ----------------------------------------
    for e in base_events + update_events:
        if e.label is None:
            ds.final_rows[e.true_order_id] = e.row
    return ds


def write_csv(path: Path, columns: list[str], rows: list[dict[str, str | None]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for r in rows:
            writer.writerow({c: ("" if r.get(c) is None else r[c]) for c in columns})


def write_dataset(ds: Dataset, out: Path, seed: int, rows: int) -> dict[str, object]:
    orders_dir = out / "input" / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    for old in orders_dir.glob("orders_batch_*.csv"):  # never mix batches from an older run
        old.unlink()
    write_csv(out / "input" / "customers.csv", CUSTOMER_COLUMNS, ds.customers)
    write_csv(out / "input" / "products.csv", PRODUCT_COLUMNS, ds.products)

    labels: list[dict[str, str | None]] = []
    file_counts: dict[str, int] = {}
    for name, (cols, events) in ds.files.items():
        write_csv(orders_dir / name, cols, [e.row for e in events])
        file_counts[name] = len(events)
        for e in events:
            if e.label:
                labels.append({
                    "source_file": name, "event_id": e.row["event_id"], "order_id": e.row["order_id"],
                    "expected_outcome": e.label[0], "expected_reason": e.label[1],
                })
    write_csv(out / "expected" / "expected_outcomes.csv",
              ["source_file", "event_id", "order_id", "expected_outcome", "expected_reason"], labels)

    daily: dict[str, list[int]] = {}
    for r in ds.final_rows.values():
        d = str(r["order_ts"])[:10]
        acc = daily.setdefault(d, [0, 0])
        acc[0] += 1
        acc[1] += int(str(r["order_amount"]).replace(".", ""))
    write_csv(out / "expected" / "expected_daily_totals.csv",
              ["order_date", "order_count", "total_order_amount"],
              [{"order_date": d, "order_count": str(c), "total_order_amount": fmt_cents(s)}
               for d, (c, s) in sorted(daily.items())])

    reasons: dict[str, int] = {}
    dedup_reasons: dict[str, int] = {}
    for lab in labels:
        target = reasons if lab["expected_outcome"] == "rejected" else dedup_reasons
        target[str(lab["expected_reason"])] = target.get(str(lab["expected_reason"]), 0) + 1
    input_rows = sum(file_counts.values())
    summary: dict[str, object] = {
        "seed": seed,
        "rows_requested": rows,
        "input_rows": input_rows,
        "files": file_counts,
        "expected_rejected": sum(reasons.values()),
        "expected_deduped": sum(dedup_reasons.values()),
        "expected_output_rows": len(ds.final_rows),
        "reject_reason_counts": dict(sorted(reasons.items())),
        "dedup_reason_counts": dict(sorted(dedup_reasons.items())),
        "customers": len(ds.customers),
        "products": len(ds.products),
    }
    # The core reconciliation invariant must hold for the generated data itself.
    assert input_rows == summary["expected_output_rows"] + summary["expected_rejected"] \
        + summary["expected_deduped"], "generator invariant violated"
    (out / "expected" / "expected_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate reproducible e-commerce order data.")
    p.add_argument("--rows", type=int, default=150, help="total order rows across all files")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", type=Path, default=Path("data/sample"))
    p.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 1, 1))
    p.add_argument("--days", type=int, default=5, help="length of the order_date window")
    p.add_argument("--rows-per-file", type=int, default=None,
                   help="rows per arrival batch (default: max(100, rows // 5))")
    p.add_argument("--defect-rate", type=float, default=0.02)
    p.add_argument("--update-rate", type=float, default=0.03)
    p.add_argument("--dup-rate", type=float, default=0.01)
    p.add_argument("--late-rate", type=float, default=0.02)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    a = parse_args(argv)
    rows_per_file = a.rows_per_file or max(100, a.rows // 5)
    ds = build_dataset(a.rows, a.seed, a.start_date, a.days, rows_per_file,
                       a.defect_rate, a.update_rate, a.dup_rate, a.late_rate)
    summary = write_dataset(ds, a.out, a.seed, a.rows)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
