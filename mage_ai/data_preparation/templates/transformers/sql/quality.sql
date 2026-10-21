-- A data quality report of the upstream rows: how many rows, how many have no
-- id, how many repeat an id and how many have a negative amount. Change the
-- columns to yours, and add a block test or a monitor that fails on a count
-- above 0.
SELECT
    COUNT(*) AS row_count,
    COUNT(*) - COUNT(id) AS missing_ids,
    COUNT(id) - COUNT(DISTINCT id) AS repeated_ids,
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amounts
FROM {{ df_1 }}
