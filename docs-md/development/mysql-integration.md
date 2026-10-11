# MySQL with pandas and Polars

Problems found by `integration_tests/mysql/` in Mage's MySQL client (`mage_ai/io/mysql.py`),
against MySQL 8.4 with pandas 3.0 and Polars 2.0. The test server runs with its time zone
at `-06:00`, so values that go through the session time zone show it. Each entry gives the
effect, the fix or the behavior to know about, and the test.

## Export

New tables took their column types from a mapping that lost values. Measured before the
change:

| Frame column | Stored as | Effect |
| --- | --- | --- |
| float `0.25` | `DECIMAL`, which MySQL reads as `DECIMAL(10,0)` | `0`; `1e300` failed |
| `Decimal('12345.6789')` | `DECIMAL(10,0)` | `12346` |
| bool | `CHAR(52)` | the text `True` |
| timestamp | `TIMESTAMP` | microseconds dropped; values after 2038 failed |
| zoned timestamp | `TIMESTAMP`, sent as text with an offset | converted to the session time zone |
| text | `TEXT` | values above 64 KB failed |
| bytes | `VARBINARY(255)` | values above 255 bytes failed |
| list of scalars | `TEXT` | failed: the driver cannot send a list |
| NaT in a timedelta column | `BIGINT` | `-9223372036854775808` |
| text `'"quoted"'` | | stored as `quoted`: surrounding double quotes were removed |

New columns are now `DOUBLE`, `DECIMAL(p, s)` sized from the values, `BOOLEAN`,
`DATETIME(6)`, `LONGTEXT`, `LONGBLOB` and `JSON` for dicts and lists. Zoned timestamps
are stored in `DATETIME(6)` in UTC. Integers outside `BIGINT` are `BIGINT UNSIGNED` or,
for 128-bit values, `DECIMAL(39, 0)`. Durations within `TIME`'s range of 838 hours are
`TIME(6)`; longer ones are integer nanoseconds, with NULL for NaT.

Test: `test_created_types`, `test_round_trip`, `test_text_and_bytes_keep_their_values`,
`test_zoned_timestamps_are_stored_in_utc`, `test_durations`.

The NaT conversion is shared by every SQL client that uses `BaseSQL.clean`, PostgreSQL
included: `timedelta_to_nanoseconds` cast NaT to the int64 minimum.

### Polars frames

The SQL clients other than PostgreSQL and DuckDB converted Polars frames with a plain
`to_pandas()`. Integer columns with a null became float64, which rounds values above
2**53, dates became datetimes, and `Int128` columns failed the conversion. Polars frames
now become pandas' nullable integers and booleans, dates stay dates, and 128-bit integers
are Python ints. This applies to MSSQL, Trino, Redshift, BigQuery and the other clients
that share `to_pandas_frame` or `BaseSQL.export`. Test: `test_polars_frames`,
`test_polars_pipeline`.

### Names

- Table and schema names were not quoted, so `CREATE TABLE long`, a reserved word, and
  names with a dash or a space failed. Test: `test_table_names_are_quoted`.
- `ON DUPLICATE KEY UPDATE` did not quote column names, so reserved words such as
  `order` failed. A unique key on a text column failed, since MySQL cannot index `TEXT`;
  text key columns are `VARCHAR(255)`.
  Test: `test_upsert_on_a_text_key_with_reserved_column_names`.
- Mage prefixes reserved column names with an underscore. Appending to a table created
  outside Mage, with the plain names, failed. Columns of an existing table are matched by
  the prefixed, the plain or the cleaned name. Test: `test_append_matches_columns_by_name`.

## Load

`load()` without options uses pandas `read_sql`: integer columns with a NULL become
float64, so `9007199254740993` becomes `9007199254740992.0`; `DECIMAL` becomes float, and
`TIMESTAMP` is naive, in the session time zone. Test:
`test_default_load_turns_integers_with_nulls_into_floats`,
`test_timestamps_are_in_utc_whatever_the_session_time_zone`.

`load(exact_types=True)` returns pyarrow-backed pandas columns and `load(polars=True)` a
Polars frame, built from the MySQL column types:

| MySQL | Arrow |
| --- | --- |
| integer types | int64; `BIGINT UNSIGNED` uint64; `YEAR` int16; `BIT` uint64 |
| `FLOAT`, `DOUBLE` | float64 |
| `DECIMAL` | decimal sized from the values; the driver reports no precision |
| `DATE`, `DATETIME`, `TIME` | date32, timestamp[us], duration[us] |
| `TIMESTAMP` | timestamp[us, UTC]; the query runs with the session at UTC |
| text, `ENUM`, `JSON` | string |
| `BLOB`, `BINARY` | binary |
| `SET` | list of strings, sorted |

Polars decimals hold 38 digits and Polars panicked on wider Arrow decimals, so
`DECIMAL(65,30)` reaches Polars as its exact text. Test:
`test_exact_types_values_match_mysql`, `test_polars_values_match_mysql`,
`test_wide_decimals_reach_polars_as_exact_text`.

### Transactions

A load left its transaction open. MySQL's default isolation, REPEATABLE READ, kept that
transaction's snapshot, so the next load on the same client missed rows other sessions
had committed, and the metadata lock on the tables it read made other sessions' `ALTER`
and `DROP` wait. A load that starts a transaction commits it. Test:
`test_each_load_sees_committed_rows`, `test_a_load_does_not_block_other_sessions`.
