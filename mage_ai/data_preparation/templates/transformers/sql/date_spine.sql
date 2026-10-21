-- One row per day from the first day of the data to the last, with zeros on
-- days without rows, so charts and features see every day. Change created_at
-- to your date or timestamp column and amount to the column to total.
-- generate_series as here works in PostgreSQL and DuckDB.
WITH totals AS (
    SELECT
        CAST(created_at AS DATE) AS day,
        SUM(amount) AS amount,
        COUNT(*) AS row_count
    FROM {{ df_1 }}
    GROUP BY 1
),

days AS (
    SELECT CAST(spine.day AS DATE) AS day
    FROM generate_series(
        CAST((SELECT MIN(day) FROM totals) AS TIMESTAMP),
        CAST((SELECT MAX(day) FROM totals) AS TIMESTAMP),
        INTERVAL '1 day'
    ) AS spine (day)
)

SELECT
    days.day,
    COALESCE(totals.amount, 0) AS amount,
    COALESCE(totals.row_count, 0) AS row_count
FROM days
LEFT JOIN totals ON days.day = totals.day
ORDER BY days.day
