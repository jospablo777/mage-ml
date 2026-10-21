-- Keeps the latest row of each key: change id to your key column and
-- updated_at to the column whose largest value is the latest. Rows without a
-- key are dropped. DISTINCT ON works in PostgreSQL and DuckDB; in BigQuery
-- and Snowflake, use QUALIFY ROW_NUMBER() OVER (PARTITION BY id
-- ORDER BY updated_at DESC) = 1 instead.
SELECT DISTINCT ON (id) *
FROM {{ df_1 }}
WHERE id IS NOT NULL
ORDER BY id, updated_at DESC
