# PostgreSQL data integrity with pandas and Polars

Problems found while testing Mage's PostgreSQL client (`mage_ai/io/postgres.py`) and
block output storage against a real server, with pandas 3.0 and Polars 2.0. Each entry
gives what happened, the cause, the fix and the test that covers it. Tests live in
`integration_tests/postgres/` unless another path is given; see
`integration_tests/README.md` to run them.

Versions: PostgreSQL 16, psycopg2 2.9, pandas 3.0.6, Polars 2.0.0, pyarrow 25.0.1.

## Export

The old write path rendered a frame with `DataFrame.to_csv` for COPY, and bound Python
values for `INSERT ... ON CONFLICT`. Column types for new tables came from
`get_type`, which looked at the values of the current batch. The new path renders every
value in PostgreSQL's text input format for its target column type
(`mage_ai/io/postgres_types.py`), uses COPY text format, and casts each INSERT value to
its column type.

### Empty strings became NULL on COPY

COPY used CSV with `NULL ''` and `FORCE_NULL` on every column, so an empty string and a
missing value were written as the same empty field and both stored as NULL.

Fix: COPY text format, where NULL is `\N`. Tests: `test_text_values`,
`test_nulls_round_trip_through_the_copy_path`.

### NaN became NULL

`to_csv(na_rep='')` wrote NaN as an empty field, and the INSERT path replaced NaN with
None before binding. A float column holding both NaN and NULL lost the difference on
either path.

Fix: NaN is written as `NaN` when the frame keeps NaN apart from NULL, which Polars
columns and pandas object columns do. In pandas `float64` columns NaN is the missing
value marker, and pandas 3 `Float64` turns NaN into NA, so in those NaN is written as
NULL. An exact load returns a float column holding both as object for this reason.
Tests: `test_float_values`, `test_nulls_round_trip`.

### bytes were stored as their Python repr

`to_csv` writes `b'\x00\xff'` for a bytes value, so a `bytea` column received the
characters of the repr. Columns typed from bytes were created as `bytea`, which reads
that text in escape format and stores different bytes.

Fix: bytea is written as `\x` hex. Tests: `test_bytes_values`, `test_polars_binary`.

### Arrays with brackets, quotes or backslashes were corrupted

Lists were dumped as JSON and every `[` and `]` in the text was replaced by `{` and
`}`, including brackets inside string elements. JSON escapes are not array literal
escapes, so quotes and backslashes in elements also changed.

Fix: array literals with every element quoted and escaped, and `NULL` for missing
elements. Tests: `test_array_values`, and `test_array_text` in
`mage_ai/tests/io/create_table/test_postgresql.py`.

### Integer column width followed the first batch

`get_type` chose `smallint` or `integer` from the minimum and maximum of the values
being written. A table created from a batch of small keys rejected larger keys on a later
append.

Fix: the width follows the dtype: `int16` to smallint, `int32` to integer, `int64` and
`Int64` to bigint, `uint64` to `numeric(20, 0)`. Tests:
`test_large_keys_do_not_get_a_smallint_column`, `test_uint64_creates_a_numeric_column`.

### Decimal, date, time, interval and UUID columns got lossy types

Decimal columns were created as `double precision`, timedelta as `bigint`
nanoseconds, and other object columns as `JSONB`.

Fix: new tables get `numeric`, `date`, `time`, `interval`, `bytea`, `uuid`, `jsonb` and
array columns from the values in object columns. Tables created before keep their
types. Tests: `test_created_table_column_types`, `test_dates_times_and_intervals`,
`test_uuid_objects_create_a_uuid_column`, `test_timedelta_column_exports_as_interval`.

### Unknown conflict methods were treated as IGNORE

Any `unique_conflict_method` other than `UPDATE`, including a typo, produced
`ON CONFLICT DO NOTHING`.

Fix: the method is case insensitive, and an unknown value raises ValueError before
anything is written. Tests: `test_an_unknown_method_raises_before_writing` and the
spelling cases in `test_export_conflicts.py`.

### UPDATE with duplicate keys depended on the page size

PostgreSQL rejects `ON CONFLICT DO UPDATE` when one statement touches the same row
twice. Duplicate keys inside one export failed when they landed in the same INSERT page
and silently kept the last one when they did not.

Fix: with UPDATE, duplicate keys raise ValueError before anything is written. The
message gives the count and examples. Rows with a NULL key never conflict and are not
counted. Tests: `test_update_with_duplicate_keys_raises_and_writes_nothing`,
`test_rows_with_a_null_key_never_count_as_duplicates`.

IGNORE keeps the first row of each key at any page size and does not raise. The
streaming sink depends on that behavior. Test:
`test_ignore_with_duplicate_keys_keeps_the_first_at_any_page_size`.

### A failed statement left the client unusable

After a failed export, load or execute the connection stayed in an aborted transaction,
and every later statement failed with `current transaction is aborted`.

Fix: each of them rolls back on error. Tests: `test_client_is_usable_after_a_failed_export`,
`test_client_is_usable_after_a_failed_query`, `test_failed_replace_keeps_the_old_rows`,
`test_a_bad_row_in_a_late_chunk_writes_nothing`.

### A load held locks until the client closed

`load` opened a transaction and left it open. ALTER, DROP and TRUNCATE in other
sessions waited on its locks, which hung the test suite on its first run.

Fix: a load that starts a transaction commits it. Each test connection also sets
`lock_timeout`, so a leaked transaction fails a test after 15 seconds. Test: every test
that drops its schema after loading.

### Reserved column names did not match existing tables

With `allow_reserved_words=False`, the default, Mage prefixes names on its list of 825
reserved words (among them `name`, `date`, `value`, `type`, `status`, `data`) with an
underscore. Exporting into a table created outside Mage looked for `_name` and failed.
A unique constraint on such a column was passed to CREATE TABLE without the prefix, so
creating the table failed.

Fix: for an existing table, each frame column matches the first of the prefixed name,
the original name and the cleaned name that the table has. Keys use the same name as
their column. Tests: `test_export_names.py`.

### Smaller fixes

- A missing unique index for ON CONFLICT raises an error that names the columns
  (`test_missing_unique_index_raises_a_clear_error`).
- A frame column missing from an existing table raises an error that names it
  (`test_a_column_missing_from_the_table_is_named_in_the_error`).
- `close` raised AttributeError when the client failed during `__init__`.

## Load

### The default load changes values

`load()` without options returns what `pandas.read_sql` style conversion gives, and is
unchanged for compatibility:

- integer columns with a NULL become float64, so bigint values above 2^53 lose
  precision, and 9223372036854775807 becomes 2^63, which no bigint column accepts on
  the way back (`test_default_load_cannot_write_the_largest_bigint`);
- numeric becomes float64;
- NaN and NULL in float columns both become NaN.

Tests: `test_default_load_converts_integers_with_nulls_and_numerics_to_float` and the
`pandas_default` cases in `test_export_roundtrip.py`, which pin exactly these four
losses.

### Exact loads

`load(exact_types=True)` and `load(polars=True)` build each column from the PostgreSQL
column type. Both return every value of the 32 column dataset unchanged, and write it
back unchanged through COPY and through ON CONFLICT
(`test_round_trip_into_an_existing_table`, `test_round_trip_into_a_table_mage_creates`).

| PostgreSQL | pandas exact | Polars |
| --- | --- | --- |
| smallint, integer, bigint | Int64 | Int16, Int32, Int64 |
| numeric(p, s) | Decimal objects | Decimal(p, s), up to 38 digits |
| numeric, no scale | Decimal objects | String when a value has more than 38 digits or is NaN |
| real, double precision | float64, or Float64 or object to keep NaN apart from NULL | Float32, Float64 |
| text, varchar, char, uuid, enum | str | String |
| bytea | bytes objects | Binary |
| date, time | date and time objects | Date, Time |
| timetz | time objects with tzinfo | String in ISO format |
| timestamp, timestamptz | datetime64[us], datetime64[us, UTC] | Datetime(us), Datetime(us, UTC) |
| interval | timedelta64[us] | Duration(us) |
| json, jsonb | dicts and lists | String with the JSON text |
| arrays | lists | List of the element type |

Polars has no time with offset or arbitrary precision decimal type, so those columns
load as text. Mage writes them back to their original column types when the table
exists.

## Block outputs

A pandas block output is written to Parquet and read by the next block
(`mage_ai/data_preparation/models/variable.py`). Values from an exact load changed on
that trip. Tests: `test_pipeline_round_trip` runs pipelines through Mage's executor,
and `test_exact_values_survive_the_variable_round_trip` in
`mage_ai/tests/data_preparation/models/test_variable.py` checks the storage alone.

- An object column of Decimals with mixed magnitudes failed to write. Arrow needs one
  precision per decimal column and allows at most 76 digits.
- `astype(bytes)` produced fixed width numpy bytes, which drop trailing zero bytes.
- Times with an offset lost it, because Parquet times have no time zone.
- A float object column holding NaN and NULL stored both as null.
- NULL came back as NaN in dict, list and Decimal columns.
- Decimals, dates, datetimes, times, timedeltas, bytes and UUIDs inside dicts and lists
  came back as floats or strings.
- A dict column went through Polars first, which turned the dicts into structs and added
  the keys of every other row to each value.

Fix: these columns are stored as JSON text with a type tag for values JSON cannot hold,
and restored with their types. Values written before the tags read as before.

## Values PostgreSQL changes

These come from PostgreSQL and are covered so a change would show up:

- numeric values with more digits than the column scale are rounded
  (`test_decimals_beyond_the_column_scale_are_rounded_by_postgres`);
- timestamps keep microseconds, so pandas nanoseconds are rounded
  (`test_nanoseconds_round_to_microseconds`);
- naive timestamps written into `timestamptz` are read in the session time zone
  (`test_naive_and_aware_timestamps_with_a_session_timezone`);
- a fractional float written into an integer column fails, an integral one is stored
  as an integer (`test_fractional_float_into_an_integer_column_fails`,
  `test_integral_floats_fill_integer_columns`).
