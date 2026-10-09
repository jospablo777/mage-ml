# DuckDB with pandas and Polars

Problems found by `integration_tests/duckdb/` in Mage's DuckDB client
(`mage_ai/io/duckdb.py`), with DuckDB 1.5, pandas 3.0 and Polars 2.0. Each entry gives
the effect, the fix or the behavior to know about, and the test.

## Export

The exporter created new tables from Mage's own type mapping and inserted with
`INSERT INTO table SELECT * FROM df`. Measured before the rewrite:

| Frame column | Stored as | Effect |
| --- | --- | --- |
| float64 `0.1234567` | `DECIMAL(18,3)` | `0.123`; `1e300` failed |
| bool | `VARCHAR` | the text `true` |
| zoned timestamp, 12:00 New York | `TIMESTAMP` | `11:00` on a machine in UTC-6: shifted by the local zone, zone dropped |
| timedelta with NaT | `BIGINT` | nanoseconds, and NaT as -9223372036854775808 |
| bytes | `VARBINARY(255)` | failed: DuckDB has no such type |
| dict, list | `VARCHAR` | the Python repr, `{'a': 1}`, not JSON |
| Polars integers with NULL | `DECIMAL(18,3)` | failed after the frame went through pandas floats |
| `name`, `date`, `text`, any reserved word | | failed: the table had the prefixed name, the insert the plain one |
| frame columns in another order than the table | | values went to the wrong columns, or failed to cast |

The exporter now hands DuckDB the frame as Arrow. New tables get the frame's types,
rows are inserted with `INSERT ... BY NAME`, and the export runs in one transaction.
Dicts and nested values in pandas object columns are stored as JSON, UUID objects as
UUID, Polars 128-bit integers as HUGEINT and UHUGEINT. For an existing table, each frame
column matches the prefixed, the plain or the cleaned name, as in the PostgreSQL client.
Round trips from `exact_types=True` and `polars=True` loads leave every column of the
36-column dataset unchanged, into new and existing tables.

Test: `test_round_trip`, `test_created_types`, `test_numpy_backed_pandas_frames`,
`test_object_columns`, `test_columns_are_matched_by_name_not_position`.

### Conflicts

- With `unique_conflict_method='UPDATE'`, DuckDB keeps the first of several rows that
  share a key, without an error. The exporter raises ValueError before writing.
  Test: `test_update_with_duplicate_keys_raises_and_writes_nothing`.
- `ON CONFLICT DO UPDATE` keeps only one of several rows whose key is NULL, while a
  plain insert and `DO NOTHING` keep them all. A NULL key never conflicts, so the
  exporter inserts those rows without `ON CONFLICT`.
  Test: `test_null_keys_are_not_duplicates`.
- New tables get the unique constraint of `unique_constraints`.
- A failed export, including the `DELETE` of `if_exists='replace'`, leaves the table as
  it was. Test: `test_a_failed_export_leaves_the_table_unchanged`.

### Types DuckDB cannot convert directly

- DuckDB has no cast between `TIMESTAMP_S`, `TIMESTAMP_MS` and `TIMESTAMP_NS`. Polars has
  no seconds unit, so a `TIMESTAMP_S` column loads as milliseconds; appending it back to
  the `TIMESTAMP_S` column failed. The exporter casts through `TIMESTAMP`.
- pyarrow cannot receive Polars `Int128` columns; they go as text and are cast in DuckDB.

## Load

`load()` without options still goes through `pandas.read_sql` and returns the same types
as before: integer columns with a NULL become float, decimals become float, and
nanosecond timestamps are rounded by DuckDB's Python conversion, so
`1969-12-31 23:59:59.999999999` becomes `1970-01-01 00:00:00`. 1,000,000 rows take 4.3 s.

`load(exact_types=True)` returns pyarrow-backed pandas columns and `load(polars=True)` a
Polars frame, both from DuckDB's Arrow result in 0.04 s for the same rows. They keep
integer widths, integers with NULL, decimals, nanoseconds and nested types. Equal values
in another representation: UUID as text, MAP as key and value pairs in pandas.

Test: `test_values_match_duckdb`, `test_nanoseconds_are_kept`,
`test_the_default_load_rounds_nanoseconds_to_the_next_second`.

### Values DuckDB's Arrow export changes

- UHUGEINT is exported as `decimal128(38, 0)`, and values above 2**127 wrap around: the
  largest arrives as `-1`. Polars rounds HUGEINT values of 39 digits to 38. The client
  reads these columns as text and rebuilds exact integers, `Int128` and `UInt128` in
  Polars. Test: `test_128_bit_integers_are_exact`.
- TIMESTAMPTZ is exported in the session time zone. The client returns UTC.
  Test: `test_zoned_timestamps_are_in_utc`.
- Polars cannot import Arrow intervals, neither through Mage nor through DuckDB's own
  `.pl()`. Intervals without months become nanosecond durations; intervals with months a
  struct of months, days and nanoseconds, which the exporter turns back into INTERVAL.
- `fetch_arrow_table()` is deprecated in DuckDB 1.5; the client uses `to_arrow_table()`.

## Files

- Mage opens DuckDB files for writing. A second process cannot open a file while a
  connection holds it, so blocks running in other processes on the same file fail until
  it closes. Clients in one process share the database.
  Test: `test_another_process_cannot_open_a_file_in_use`.
- `read_csv` reads a quoted empty string as NULL unless `allow_quoted_nulls = false`.
  Test: `test_reading_parquet_csv_and_json_files`.

## SQL blocks

A DuckDB SQL block exports each upstream output to a table with the client's exporter,
so upstream Polars and pandas frames keep their types, and the block's query result is
stored in its own table. The result reaches the next block through the default load.
Test: `test_sql_block_on_a_polars_upstream`.
