# Trino with pandas and Polars

Problems found by `integration_tests/trino/` in Mage's Trino client (`mage_ai/io/trino.py`),
the Trino SQL block and the Trino destination of `mage_integrations`, against Trino 483.
The tests run in three catalogs: `memory`, `iceberg` (tables in MinIO, catalog in
PostgreSQL) and `delta` (Delta Lake in MinIO with a file metastore). Each entry gives the
effect, the fix or the behavior to know about, and the test.

## Export

New tables took their column types from `infer_dtypes`, which gave BIGINT, DOUBLE,
BOOLEAN, TIMESTAMP or VARCHAR. Measured before the change:

| Frame column | Stored as | Effect |
| --- | --- | --- |
| timestamp | `TIMESTAMP`, which is `timestamp(3)` | rounded to milliseconds |
| uint64 above 2**63 | `BIGINT` | failed: invalid numeric literal |
| date, Decimal, UUID, bytes | `VARCHAR` | failed: the inserted values had their own types |

New columns are now `TINYINT`, `SMALLINT`, `INTEGER` or `BIGINT` by width, `DECIMAL(20, 0)`
for uint64, `REAL`, `DOUBLE`, `DECIMAL(p, s)` sized from the values, `DATE`,
`TIMESTAMP(6)`, `TIMESTAMP(6) WITH TIME ZONE` for zoned columns, `TIME(6)`, `UUID` and
`VARBINARY`. Python integers wider than 64 bits are `DECIMAL(n, 0)` up to 38 digits.
Durations are `BIGINT` microseconds, and lists and dicts are JSON text in `VARCHAR`, since
Iceberg and Delta Lake have no JSON type. `data_type_properties.timestamp_precision` still
sets the timestamp precision. The catalog's connector is read from
`system.metadata.catalogs`; for Delta Lake, which has no `UUID` or `TIME` type, these
are text, and zoned timestamps are `TIMESTAMP(3) WITH TIME ZONE`, the connector's limit.
Iceberg stores `TINYINT` and `SMALLINT` as `INTEGER`.
Test: `test_column_types_hold_every_value`, `test_special_values`,
`test_nanoseconds_round_to_microseconds`.

Rows were sent with `executemany`, one query per row: 20,000 rows were 20,000 queries
and, in Iceberg, 20,000 commits. Running that test against the old client exhausted
Trino's 1 GB heap. Rows now go in `INSERT ... VALUES` statements of up to 500,000
characters (`Trino.QUERY_MAX_LENGTH`; Trino's `query.max-length` defaults to 1,000,000),
with typed literals: `DECIMAL '…'`, `TIMESTAMP '… +05:30'`, `X'…'`, `UUID '…'`.
Test: `test_rows_go_in_few_statements`.

Other changes:

- Inserts named no columns, so a frame in another column order than the table mixed
  values. Columns are matched by name, and values are written as the existing table's
  column types. Test: `test_appends_match_columns_by_name`.
- A replace into the memory connector failed, since it cannot `DELETE`. It falls back to
  `TRUNCATE TABLE`. Test: `test_write_policies`.
- A replace from a query into an existing table, without `drop_table_on_replace`, deleted
  the rows and then ran `CREATE TABLE AS`, which failed. It runs `INSERT INTO`. Test:
  `test_query_string_into_an_existing_table`.
- `table_exists` used `SHOW TABLES LIKE`, where `_` matches any character, so `axb`
  matched a table named `a_b`. It reads `information_schema.columns` by exact name.
  Test: `test_table_exists_matches_exact_names`.
- Table names are quoted.

## Load

`load()` and `execute_queries()` caught `TrinoUserError`, printed it, ran the query twice
more and returned `None` or `[]`, so a typo in a query reached the next block as a
missing frame. Errors raise. The trino client already retries HTTP 502, 503 and 504.
Test: `test_failed_queries_raise`.

Loads ran `SELECT * FROM (query) AS subquery LIMIT n`. Trino drops an `ORDER BY` in a
subquery, so `load('… ORDER BY id')` returned the rows in any order, in every mode. The
query runs as written, and the rows after `limit` are not fetched: the cursor is closed,
which cancels the query. Test: `test_loads_keep_the_order_and_the_limit`.

`load()` without options uses pandas `read_sql`: integer columns with a NULL become
float64. `load(nullable_integers=True)` keeps them as nullable Int64, and Trino SQL blocks
load their result with it. `load(exact_types=True)` returns pyarrow-backed columns and
`load(polars=True)` a Polars frame, built from the Trino column types:

| Trino | Arrow |
| --- | --- |
| `tinyint` … `bigint` | int8 … int64 |
| `real`, `double` | float32, float64 |
| `decimal(p, s)` | decimal128(p, s) |
| `varchar`, `char`, `json`, `uuid` | string |
| `varbinary` | binary |
| `date`, `time(p)` | date32, time64[us] |
| `timestamp(p)` | timestamp[us] |
| `timestamp(p) with time zone` | timestamp[us, UTC] |
| `interval day to second` | duration[us] |
| `array`, `map`, `row` | Arrow's list, map or struct when it can build one, else JSON text |

Test: `test_load_modes`, `test_sql_block`.

## Trino destination of mage_integrations

Run with Singer messages in each catalog. Measured before the change:

- `string_parse_func` replaced every apostrophe with a double quote after the value was
  escaped, so `it's` was stored as `it""s`, and JSON with an apostrophe was no longer
  JSON.
- Strings were `VARCHAR(255)` and date-times `VARCHAR(52)`. Values were inserted with
  `CAST('…' AS VARCHAR(255))`, which cut longer text at 255 characters without an error.
- In the memory catalog and others that are not Iceberg or Delta Lake, objects and arrays
  are `JSON` columns. Objects were inserted with `CAST('…' AS JSON)`, which stores a JSON
  string, arrays were cast to their item type, which the column rejected, and an
  apostrophe in an array ended the SQL string.
- Empty arrays were written as NULL.

Strings and date-times are now `VARCHAR`, objects and arrays go in as `JSON '…'` literals
with apostrophes escaped, and empty arrays stay empty. Date-times stay text, as before.
Tables created before keep their column types. Upserts use `MERGE` in Iceberg and Delta
Lake; the memory connector cannot merge, so rows are appended.
Test: `test_values_arrive_unchanged`, `test_date_times`, `test_appends_and_new_columns`,
`test_upserts`.

## Trino behavior to know about

- The memory connector fails to read a column added with `ALTER TABLE ADD COLUMN` after
  the table had rows, unless the query reads every column: `SELECT new_column` raises
  `Index out of bounds`, `SELECT *` works.
- Trino rounds timestamps to the column's precision on insert; nanoseconds round to
  microseconds.
- Iceberg's JDBC catalog needs its tables created before Trino uses it; the test setup
  creates them (`integration_tests/services/trino/setup`).
