"""Row-level validation rules: nulls, malformed values, ranges, domain rules, references.

All cases are written to ONE input file and validated in a single Spark job (fast);
each case is then asserted separately via parametrize.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from ecom_pipeline import ingest, validate
from ecom_pipeline.config import PipelineConfig
from tests.helpers import make_input, order, write_csv

# case_id -> (field overrides, expected reject reasons in reporting order; [] = valid)
CASES: dict[str, tuple[dict[str, Any], list[str]]] = {
    "happy_path": ({}, []),
    # --- nulls / blanks ---
    "null_order_id": ({"order_id": None}, ["MISSING_REQUIRED:order_id"]),
    "null_customer_id": ({"customer_id": None}, ["MISSING_REQUIRED:customer_id"]),
    "blank_whitespace_product": ({"product_id": "   "}, ["MISSING_REQUIRED:product_id"]),
    "null_event_ts": ({"event_ts": None}, ["MISSING_REQUIRED:event_ts"]),
    "null_currency": ({"currency": None}, ["MISSING_REQUIRED:currency"]),
    # --- malformed values ---
    "qty_not_number": ({"quantity": "abc"}, ["INVALID_TYPE:quantity"]),
    "qty_decimal": ({"quantity": "1.5"}, ["INVALID_TYPE:quantity"]),
    "qty_overflow": ({"quantity": "99999999999"}, ["INVALID_TYPE:quantity"]),
    "amount_two_dots": ({"order_amount": "12.3.4"}, ["INVALID_TYPE:order_amount"]),
    "price_three_decimals": ({"unit_price": "10.001"}, ["INVALID_TYPE:unit_price"]),
    "impossible_date": ({"order_ts": "2026-02-30T10:00:00Z"}, ["INVALID_TIMESTAMP:order_ts"]),
    "garbage_date": ({"order_ts": "not-a-date"}, ["INVALID_TIMESTAMP:order_ts"]),
    "non_iso_event_ts": ({"event_ts": "2026/01/02 10:00"}, ["INVALID_TIMESTAMP:event_ts"]),
    # --- ranges & boundaries ---
    "qty_zero": ({"quantity": "0"}, ["OUT_OF_RANGE:quantity"]),
    "qty_1001": ({"quantity": "1001"}, ["OUT_OF_RANGE:quantity"]),
    "qty_min_boundary": ({"quantity": "1", "order_amount": "10.00"}, []),
    "qty_max_boundary": ({"quantity": "1000", "order_amount": "10000.00"}, []),
    "price_zero": ({"unit_price": "0.00"}, ["OUT_OF_RANGE:unit_price"]),
    "amount_zero_promo": ({"order_amount": "0.00"}, []),
    "amount_max_boundary": ({"order_amount": "1000000.00"}, []),
    "amount_over_max": ({"order_amount": "1000000.01"}, ["OUT_OF_RANGE:order_amount"]),
    # --- domain: refunds / negative totals ---
    "negative_amount_placed": ({"order_amount": "-20.00"}, ["NEGATIVE_AMOUNT"]),
    "refund_negative_ok": ({"order_status": "REFUNDED", "order_amount": "-20.00"}, []),
    "refund_zero_ok": ({"order_status": "REFUNDED", "order_amount": "0.00"}, []),
    "refund_positive": ({"order_status": "REFUNDED", "order_amount": "20.00"}, ["POSITIVE_REFUND"]),
    "status_typo": ({"order_status": "SHIPPPED"}, ["INVALID_STATUS"]),
    "status_lowercase_ok": ({"order_status": "shipped"}, []),
    # --- referential integrity ---
    "unknown_customer": ({"customer_id": "C999"}, ["UNKNOWN_CUSTOMER"]),
    "unknown_product": ({"product_id": "P999"}, ["UNKNOWN_PRODUCT"]),
    # --- domain: order before customer signup (C2 signs up 2026-01-10) ---
    "before_signup": ({"customer_id": "C2", "order_ts": "2026-01-09T23:59:59Z"}, ["ORDER_BEFORE_SIGNUP"]),
    "on_signup_day_ok": ({"customer_id": "C2", "order_ts": "2026-01-10T00:00:00Z"}, []),
    # +02:00 offset -> 2026-01-09T23:30Z in UTC -> before signup
    "offset_shifts_date": ({"customer_id": "C2", "order_ts": "2026-01-10T01:30:00+02:00"},
                           ["ORDER_BEFORE_SIGNUP"]),
    # --- several problems on one row: all reported, fixed order ---
    "multiple_defects": ({"customer_id": None, "quantity": "abc", "order_status": "X"},
                         ["MISSING_REQUIRED:customer_id", "INVALID_TYPE:quantity", "INVALID_STATUS"]),
}


@pytest.fixture(scope="module")
def results(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> dict[str, list[str]]:
    root = tmp_path_factory.mktemp("validation")
    inp = make_input(root)
    rows = [order(**overrides, event_id=case) for case, (overrides, _) in CASES.items()]
    write_csv(inp / "orders" / "cases.csv", rows)
    ing = ingest.read_orders(spark, ingest.list_order_files(str(inp), "orders/*.csv"))
    checked = validate.validate_orders(
        ing.raw,
        ingest.read_customers(spark, str(inp / "customers.csv")),
        ingest.read_products(spark, str(inp / "products.csv")),
        PipelineConfig(), broadcast=True,
    )
    return {r["event_id"]: list(r["reject_reasons"]) for r in checked.collect()}


@pytest.mark.parametrize("case", list(CASES))
def test_rule(results: dict[str, list[str]], case: str) -> None:
    assert results[case] == CASES[case][1]


def test_malformed_csv_lines_are_quarantined(spark: SparkSession, tmp_path: Path) -> None:
    inp = make_input(tmp_path)
    good = ",".join(str(v) for v in order().values())
    (inp / "orders" / "broken.csv").write_text(
        ",".join(order()) + "\n" + good + "\n"
        + good + ",EXTRA_FIELD\n"              # too many fields
        + "E9,O9,C1\n"                          # too few fields
    )
    ing = ingest.read_orders(spark, ingest.list_order_files(str(inp), "orders/*.csv"))
    checked = validate.validate_orders(
        ing.raw, ingest.read_customers(spark, str(inp / "customers.csv")),
        ingest.read_products(spark, str(inp / "products.csv")), PipelineConfig(), broadcast=False)
    reasons = sorted(";".join(r["reject_reasons"]) for r in checked.collect())
    assert reasons == ["", "MALFORMED_ROW", "MALFORMED_ROW"]


def test_broadcast_and_shuffle_join_agree(spark: SparkSession, results: dict[str, list[str]],
                                          tmp_path: Path) -> None:
    """Baseline (shuffle) and optimized (broadcast) RI checks must give the same verdicts."""
    inp = make_input(tmp_path)
    write_csv(inp / "orders" / "cases.csv",
              [order(**o, event_id=c) for c, (o, _) in CASES.items()])
    ing = ingest.read_orders(spark, ingest.list_order_files(str(inp), "orders/*.csv"))
    checked = validate.validate_orders(
        ing.raw, ingest.read_customers(spark, str(inp / "customers.csv")),
        ingest.read_products(spark, str(inp / "products.csv")), PipelineConfig(), broadcast=False)
    assert {r["event_id"]: list(r["reject_reasons"]) for r in checked.collect()} == results
