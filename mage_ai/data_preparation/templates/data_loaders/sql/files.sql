-- Reads files with DuckDB, without loading them first: Parquet with
-- read_parquet, CSV with read_csv and JSON with read_json, local or on S3
-- (s3://bucket/path/*.parquet with the httpfs extension), globs allowed.
-- DuckDB only: choose a DuckDB connection for this block.
SELECT *
FROM read_parquet('data/*.parquet')
