"""Column contracts shared by every pipeline step (single source of truth)."""
from __future__ import annotations

from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, StringType, StructField, StructType,
    TimestampType,
)

# ---- source (raw CSV) ------------------------------------------------------
# Every column is read as STRING first so malformed values can be detected and
# quarantined instead of silently becoming null.
ORDER_COLUMNS: list[str] = [
    "event_id", "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "order_amount", "currency", "order_status", "order_ts", "event_ts",
]
# All source columns are mandatory and must be non-null.
REQUIRED_FIELDS: list[str] = list(ORDER_COLUMNS)
CORRUPT_COL = "_corrupt_record"

RAW_ORDERS_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in ORDER_COLUMNS]
    + [
        StructField("source_file", StringType(), True),
        StructField(CORRUPT_COL, StringType(), True),
        StructField("raw_record", StringType(), True),
    ]
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("list_price", DecimalType(12, 2), True),
])

VALID_STATUSES: tuple[str, ...] = ("PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED")
REFUND_STATUS = "REFUNDED"
MONEY = DecimalType(12, 2)
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"   # ISO-8601 with offset, e.g. 2026-01-01T10:00:00Z

# ---- target (curated fact) -------------------------------------------------
TARGET_SCHEMA = StructType([
    StructField("order_sk", StringType(), False),          # sha256(order_id)
    StructField("order_id", StringType(), False),
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", MONEY, False),
    StructField("order_amount", MONEY, False),
    StructField("currency", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("is_refund", BooleanType(), False),
    StructField("order_ts", TimestampType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("customer_country", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("source_file", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("processed_at", TimestampType(), False),
    StructField("order_date", DateType(), False),           # partition column
])
TARGET_COLUMNS: list[str] = [f.name for f in TARGET_SCHEMA.fields]

# ---- quarantine ------------------------------------------------------------
REJECT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("rejected_at", TimestampType(), False),
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("reject_reason", StringType(), False),     # ';'-joined reason codes
    StructField("reject_detail", StringType(), True),
    StructField("raw_record", StringType(), True),         # JSON of the raw source row
    StructField("source_file", StringType(), False),       # partition column
])
REJECT_COLUMNS: list[str] = [f.name for f in REJECT_SCHEMA.fields]
