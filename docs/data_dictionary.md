# Data dictionary

Conventions: snake_case names; timestamps are ISO-8601 in UTC (`2026-01-02T10:05:00Z`) and
stored as Spark `timestamp` (UTC session time zone); money is `decimal(12,2)` (never float);
deterministic IDs are `sha256` hex of the business key.

## Sources (CSV, header row, UTF-8)

### `orders/*.csv`: order events (fact source)

One row per order **event** (creation or status change). An order can appear many times.
All columns are read as strings first, then validated and typed.

| Column | Raw type | Target type | Required | Rules |
|---|---|---|---|---|
| `event_id` | string | string | yes | producer event id. Exact redeliveries share it |
| `order_id` | string | string | yes | **business key** of the order |
| `customer_id` | string | string | yes | must exist in `customers.csv` |
| `product_id` | string | string | yes | must exist in `products.csv` |
| `quantity` | string | int | yes | integer, 1..1000 |
| `unit_price` | string | decimal(12,2) | yes | max 2 decimals, > 0 and ≤ 1,000,000.00 |
| `order_amount` | string | decimal(12,2) | yes | max 2 decimals, \|x\| ≤ 1,000,000.00; ≥ 0 unless `REFUNDED`; ≤ 0 if `REFUNDED` |
| `currency` | string | string | yes | upper-cased (sample data is USD only) |
| `order_status` | string | string | yes | one of PLACED, SHIPPED, DELIVERED, CANCELLED, REFUNDED (case-insensitive) |
| `order_ts` | string | timestamp | yes | `yyyy-MM-dd'T'HH:mm:ssXXX`; date (UTC) ≥ customer `signup_date` |
| `event_ts` | string | timestamp | yes | same format; decides which event wins |

Primary key (after dedup): `order_id`. Dedup winner: latest `event_ts`, then `source_file` DESC,
then `event_id` DESC.

### `customers.csv`: dimension

| Column | Type | Nullable | Key |
|---|---|---|---|
| `customer_id` | string | no | PK |
| `customer_name` | string | yes | |
| `email` | string | yes | |
| `country` | string | yes | ISO-3166 alpha-2 |
| `signup_date` | date | yes | |

### `products.csv`: dimension

| Column | Type | Nullable | Key |
|---|---|---|---|
| `product_id` | string | no | PK |
| `product_name` | string | yes | |
| `category` | string | yes | |
| `list_price` | decimal(12,2) | yes | |

## Target: `orders/` (Parquet, partitioned by `order_date`)

Grain: **one row per `order_id`**, holding the latest valid event.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `order_sk` | string | no | `sha256(order_id)`, deterministic surrogate key |
| `order_id` | string | no | **PK** (business key) |
| `event_id` | string | no | event that produced this row |
| `customer_id` | string | no | FK → customers |
| `product_id` | string | no | FK → products |
| `quantity` | int | no | |
| `unit_price` | decimal(12,2) | no | |
| `order_amount` | decimal(12,2) | no | negative for refunds |
| `currency` | string | no | |
| `order_status` | string | no | upper case |
| `is_refund` | boolean | no | `order_status = 'REFUNDED'` |
| `order_ts` | timestamp | no | UTC |
| `event_ts` | timestamp | no | UTC |
| `customer_country` | string | yes | from customers |
| `product_category` | string | yes | from products |
| `source_file` | string | no | file base name the winning event came from |
| `run_id` | string | no | run that last wrote the row, `YYYYMMDDTHHMMSSZ_<8 hex>` |
| `processed_at` | timestamp | no | start time of that run (UTC) |
| `order_date` | date | no | **partition column**, `to_date(order_ts)` in UTC |

## Quarantine: `quarantine/` (Parquet, partitioned by `source_file`)

Grain: one row per rejected source row.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `run_id` | string | no | run that rejected it |
| `rejected_at` | timestamp | no | run start time (UTC) |
| `event_id` | string | yes | from the raw row (null if missing) |
| `order_id` | string | yes | from the raw row (null if missing) |
| `reject_reason` | string | no | `;`-joined reason codes, fixed order (see below) |
| `reject_detail` | string | yes | extra context (e.g. missing columns for `SCHEMA_MISMATCH`) |
| `raw_record` | string | yes | the raw source row as JSON (nulls kept), or the raw line if unparseable |
| `source_file` | string | no | **partition column**, file base name |

Reason codes: `MALFORMED_ROW`, `SCHEMA_MISMATCH`, `MISSING_REQUIRED:<col>`, `INVALID_TYPE:<col>`,
`INVALID_TIMESTAMP:<col>`, `OUT_OF_RANGE:<col>`, `NEGATIVE_AMOUNT`, `POSITIVE_REFUND`,
`INVALID_STATUS`, `UNKNOWN_CUSTOMER`, `UNKNOWN_PRODUCT`, `ORDER_BEFORE_SIGNUP`.

## Run metrics: `run_metrics/` (JSON)

`<run_id>.json` (latest state of a run) and `run_metrics.jsonl` (one line per finished run).

| Field | Type | Description |
|---|---|---|
| `run_id` | string | unique, sortable |
| `job_name`, `mode` | string | `ecom_orders_pipeline`; `optimized` or `baseline` |
| `status` | string | RUNNING, SUCCESS, FAILED |
| `start_time`, `end_time` | string | ISO-8601 UTC |
| `duration_seconds` | float | |
| `input_files` | list[string] | files processed |
| `input_count` | int | source rows read |
| `rejected_count` | int | rows quarantined |
| `valid_count` | int | input − rejected |
| `duplicate_count` | int | valid rows that lost the in-batch dedup |
| `stale_count` | int | batch winners that lost to a newer row already in the target |
| `output_count` | int | rows from this batch now in the target |
| `inserted_count` / `updated_count` | int | split of `output_count` (new key vs replaced row) |
| `partitions_written` | int | `order_date` partitions rewritten |
| `reject_reason_counts` | map | reason → rows |
| `reconciled` | bool | `input = output + rejected + duplicate + stale` |
| `failure_reason` | string | `ExceptionType: message` when FAILED |
| `config` | map | effective configuration |
