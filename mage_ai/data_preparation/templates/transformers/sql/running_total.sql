-- Adds a running total per key in time order: change customer_id to your key,
-- created_at to the order and amount to the column to sum. Window functions as
-- here work in PostgreSQL, DuckDB, BigQuery, Snowflake, MySQL 8 and
-- ClickHouse.
SELECT
    *,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY created_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_amount
FROM {{ df_1 }}
