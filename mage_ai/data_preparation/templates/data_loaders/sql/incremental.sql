-- Loads the rows changed since the run's execution date, so each run of a
-- trigger loads its own interval: change your_table to your table and
-- updated_at to the column that records changes.
SELECT *
FROM your_table
WHERE updated_at >= '{{ execution_date }}'
