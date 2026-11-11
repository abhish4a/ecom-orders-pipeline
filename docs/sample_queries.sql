-- Sample analytical / operational queries.
-- `make reconcile` runs every query below against temp views:
--   orders      = curated target (Parquet, partitioned by order_date)
--   quarantine  = rejected records
-- Outside the reconcile script, register the views first, e.g. in pyspark:
--   spark.read.parquet("data/output/sample/orders").createOrReplaceTempView("orders")
--   spark.read.parquet("data/output/sample/quarantine").createOrReplaceTempView("quarantine")

-- name: Daily net revenue (refunds net off)
SELECT order_date,
       COUNT(*)                                          AS orders,
       SUM(order_amount)                                 AS net_amount,
       SUM(CASE WHEN is_refund THEN order_amount END)    AS refunded_amount
FROM orders
GROUP BY order_date
ORDER BY order_date;

-- name: Order status mix
SELECT order_status, COUNT(*) AS orders, SUM(order_amount) AS amount
FROM orders
GROUP BY order_status
ORDER BY orders DESC;

-- name: Revenue by customer country and product category (top 10)
SELECT customer_country, product_category, COUNT(*) AS orders, SUM(order_amount) AS amount
FROM orders
GROUP BY customer_country, product_category
ORDER BY amount DESC
LIMIT 10;

-- name: Data quality: rejects by reason and file
SELECT source_file, reject_reason, COUNT(*) AS rejected_rows
FROM quarantine
GROUP BY source_file, reject_reason
ORDER BY source_file, rejected_rows DESC, reject_reason;

-- name: Lineage: rows per run and source file
SELECT run_id, source_file, COUNT(*) AS rows_in_target, MIN(processed_at) AS processed_at
FROM orders
GROUP BY run_id, source_file
ORDER BY run_id, source_file;

-- name: Primary key check (must return no rows)
SELECT order_id, COUNT(*) AS copies
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;
