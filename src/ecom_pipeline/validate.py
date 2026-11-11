"""Validation: required fields, types, ranges, domain rules, referential integrity.

Every rule produces a reason code. A row is valid only if it has no reason codes.
All reasons are kept (joined with ';' in a fixed order) so a row with several
problems reports all of them.

Reason codes
------------
MALFORMED_ROW                  CSV line could not be parsed (wrong field count)
MISSING_REQUIRED:<col>         required field is null/blank
INVALID_TYPE:<col>             quantity / unit_price / order_amount not a number (or overflow)
INVALID_TIMESTAMP:<col>        order_ts / event_ts not ISO-8601 or impossible date
OUT_OF_RANGE:<col>             quantity not in [1, max_quantity]; unit_price not in (0, max_amount];
                               abs(order_amount) > max_amount
NEGATIVE_AMOUNT                order_amount < 0 on a non-refund status
POSITIVE_REFUND                order_amount > 0 on status REFUNDED
INVALID_STATUS                 order_status not in the allowed set
UNKNOWN_CUSTOMER               customer_id not in customers dimension
UNKNOWN_PRODUCT                product_id not in products dimension
ORDER_BEFORE_SIGNUP            order date (UTC) earlier than the customer's signup_date
SCHEMA_MISMATCH                (set in ingest) file header misses required columns
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ecom_pipeline.config import PipelineConfig
from ecom_pipeline.schemas import (
    CORRUPT_COL, MONEY, ORDER_COLUMNS, REFUND_STATUS, REQUIRED_FIELDS, TS_FORMAT, VALID_STATUSES,
)

INT_PATTERN = r"^-?[0-9]+$"
DEC_PATTERN = r"^-?[0-9]+(\.[0-9]{1,2})?$"


def _clean(c: str) -> Column:
    """Trim whitespace; blank strings count as missing."""
    t = F.trim(F.col(c))
    return F.when(t == "", F.lit(None)).otherwise(t).alias(c)


def parse_orders(raw: DataFrame) -> DataFrame:
    """Add typed columns (suffix _p). Unparseable values become NULL in the _p column."""
    df = raw.select(*[_clean(c) for c in ORDER_COLUMNS], "source_file", CORRUPT_COL, "raw_record")
    return (
        df.withColumn("quantity_p",
                      F.when(F.col("quantity").rlike(INT_PATTERN), F.col("quantity").cast("int")))
        .withColumn("unit_price_p",
                    F.when(F.col("unit_price").rlike(DEC_PATTERN), F.col("unit_price").cast(MONEY)))
        .withColumn("order_amount_p",
                    F.when(F.col("order_amount").rlike(DEC_PATTERN), F.col("order_amount").cast(MONEY)))
        .withColumn("order_ts_p", F.to_timestamp("order_ts", TS_FORMAT))
        .withColumn("event_ts_p", F.to_timestamp("event_ts", TS_FORMAT))
        .withColumn("order_status", F.upper(F.col("order_status")))
        .withColumn("currency", F.upper(F.col("currency")))
    )


def join_dimensions(parsed: DataFrame, customers: DataFrame, products: DataFrame,
                    broadcast: bool) -> DataFrame:
    """Left-join both dimensions (attributes + a 'found' flag used for RI checks).

    broadcast=True ships the small dimensions to every executor (no shuffle of the fact);
    broadcast=False leaves the decision to Spark, which in baseline mode means a sort-merge
    join that shuffles the large fact table twice.
    """
    cust = customers.select(
        F.col("customer_id").alias("_dim_customer_id"),
        F.col("signup_date").alias("customer_signup_date"),
        F.col("country").alias("customer_country"),
        F.lit(True).alias("_customer_found"),
    )
    prod = products.select(
        F.col("product_id").alias("_dim_product_id"),
        F.col("category").alias("product_category"),
        F.lit(True).alias("_product_found"),
    )
    if broadcast:
        cust, prod = F.broadcast(cust), F.broadcast(prod)
    return (
        parsed.join(cust, parsed["customer_id"] == cust["_dim_customer_id"], "left")
        .join(prod, F.col("product_id") == prod["_dim_product_id"], "left")
        .drop("_dim_customer_id", "_dim_product_id")
    )


def _rules(cfg: PipelineConfig) -> list[tuple[str, Column]]:
    """(reason_code, condition) pairs in reporting order. Conditions are null-safe."""
    max_amt = F.lit(cfg.max_amount).cast(MONEY)
    rules: list[tuple[str, Column]] = [(f"MISSING_REQUIRED:{c}", F.col(c).isNull()) for c in REQUIRED_FIELDS]
    for raw_c in ("quantity", "unit_price", "order_amount"):
        rules.append((f"INVALID_TYPE:{raw_c}", F.col(raw_c).isNotNull() & F.col(f"{raw_c}_p").isNull()))
    for ts_c in ("order_ts", "event_ts"):
        rules.append((f"INVALID_TIMESTAMP:{ts_c}", F.col(ts_c).isNotNull() & F.col(f"{ts_c}_p").isNull()))
    q, price, amt, status = (F.col("quantity_p"), F.col("unit_price_p"),
                             F.col("order_amount_p"), F.col("order_status"))
    is_refund = status == F.lit(REFUND_STATUS)
    rules += [
        ("OUT_OF_RANGE:quantity", (q < 1) | (q > cfg.max_quantity)),
        ("OUT_OF_RANGE:unit_price", (price <= 0) | (price > max_amt)),
        ("OUT_OF_RANGE:order_amount", F.abs(amt) > max_amt),
        ("NEGATIVE_AMOUNT", (amt < 0) & ~is_refund),
        ("POSITIVE_REFUND", (amt > 0) & is_refund),
        ("INVALID_STATUS", status.isNotNull() & ~status.isin(*VALID_STATUSES)),
        ("UNKNOWN_CUSTOMER", F.col("customer_id").isNotNull() & F.col("_customer_found").isNull()),
        ("UNKNOWN_PRODUCT", F.col("product_id").isNotNull() & F.col("_product_found").isNull()),
        ("ORDER_BEFORE_SIGNUP", F.to_date("order_ts_p") < F.col("customer_signup_date")),
    ]
    # NULL comparisons evaluate to NULL; coalesce makes every rule a strict boolean.
    return [(code, F.coalesce(cond, F.lit(False))) for code, cond in rules]


def validate_orders(raw: DataFrame, customers: DataFrame, products: DataFrame,
                    cfg: PipelineConfig, broadcast: bool) -> DataFrame:
    """Return every input row with `reject_reasons` (array<string>) and `is_valid`."""
    checked = join_dimensions(parse_orders(raw), customers, products, broadcast)
    reasons = F.filter(F.array(*[F.when(cond, F.lit(code)) for code, cond in _rules(cfg)]),
                       lambda x: x.isNotNull())
    # A line Spark could not split correctly is reported only as MALFORMED_ROW.
    reasons = F.when(F.col(CORRUPT_COL).isNotNull(), F.array(F.lit("MALFORMED_ROW"))).otherwise(reasons)
    return (checked.withColumn("reject_reasons", reasons)
            .withColumn("is_valid", F.size("reject_reasons") == 0))


def to_reject_records(invalid: DataFrame) -> DataFrame:
    """Project invalid rows to the quarantine shape (run-level columns are added in publish)."""
    return invalid.select(
        "event_id", "order_id", "source_file",
        F.array_join("reject_reasons", ";").alias("reject_reason"),
        F.lit(None).cast("string").alias("reject_detail"),
        "raw_record",
    )
