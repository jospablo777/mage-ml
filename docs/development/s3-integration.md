# S3 with pandas and Polars

Problems found by `integration_tests/s3/` in Mage's S3 code, against MinIO
RELEASE.2025-09-07T16-13-09Z, with pandas 3.0, Polars 2.0 and pyarrow 25. Each entry
gives the effect, the fix or the behavior to know about, and the test.

MinIO no longer publishes Docker images, so `integration_tests/services/minio/` builds
the release from its source.

## Block output storage

`S3Storage` stores block outputs when the project's `remote_variables_dir` is an `s3://`
path. S3 has no directories: a path is a key prefix, and a prefix without a trailing
slash also matches siblings.

| Operation | Before | Now |
| --- | --- | --- |
| `remove_dir('.../output_1')` | also deleted `output_10/` to `output_19/` | deletes `output_1/` only |
| `remove('.../data.json')` | also deleted `data.json.bak` | deletes the key and what is under `data.json/` |
| `path_exists('.../output_')` | true when `output_1/` exists | matches whole names |
| removing a missing path | `MalformedXML` error | does nothing |
| `listdir`, `remove_dir` on 2,500 keys | listed and deleted 1,000 | pages through every key; deletes in batches of 1,000 |
| `read_async` (S3 and GCS) | returned None, so the file cache read nothing | returns the text |
| `download_file` | used a boto3 resource with default settings, ignoring the client's endpoint and credentials | uses the client |

GCS `remove_dir` had the same sibling match and is fixed the same way; there is no GCS
service in the suite to test it.

Test: `test_storage.py`.

A LazyFrame block output on S3 used to be collected in memory and uploaded, and the next
block got a DataFrame. Polars now streams it to S3 with `sink_parquet`, and the next
block gets a `scan_parquet` LazyFrame, which reads only the columns and row groups its
query needs. Storages expose `polars_location(path)`: the URI and storage options
Polars needs, built from the S3 client's settings, or None for storages Polars does not
reach. Test: `test_block_outputs_in_s3`.

## Mage's S3 client

`load()` and the other file clients take two new options, `exact_types=True` for
pyarrow-backed pandas columns and `polars=True` for a Polars frame, for CSV, JSON and
Parquet. Measured on a 29-column frame of 2,005 rows:

| Load | Integers with nulls | Decimals | Lists, structs | Exact round trip |
| --- | --- | --- | --- | --- |
| default (pandas readers) | float64; 2**53 + 1 becomes 2**53 | object | NumPy arrays | no |
| `exact_types=True` | int64 | decimal128 | Arrow lists, structs | yes; an Enum comes back as a dictionary |
| `polars=True` | Int64 | Decimal | List, Struct, Array | yes |

pandas Parquet loads now go through `read_pandas_parquet`, which reads list and struct
columns that pandas wrote from `pd.ArrowDtype` columns; `pd.read_parquet` failed on
them.

Test: `test_polars_parquet_round_trip`, `test_exact_types_load_of_a_polars_file`,
`test_default_load_turns_integers_with_nulls_into_floats`,
`test_arrow_backed_pandas_round_trip`.

### pandas CSV parser

- pandas 3.0's C parser reads -2**63 as missing when the column has another missing
  value. With `dtype_backend='numpy_nullable'` the value is lost as well.
  Test: `test_pandas_csv_parser_loses_the_int64_minimum`.
- A null in a one-column CSV is an empty line, which pandas skips by default
  (`skip_blank_lines=True`), so the row is lost.
  Test: `test_pandas_csv_parser_drops_null_rows_of_one_column_files`.

Both happen only in the default load; `exact_types` and `polars` read CSV with Polars.
Polars reads CSV and JSON with `infer_schema_length=None`, since its default of 100 rows
fails on a later row that does not fit.

### JSON

Polars writes JSON as an array of rows and pandas as `{column: {index: value}}`. The
Polars reader takes either, and newline-delimited JSON. pandas JSON is read from the
parsed columns: `pd.read_json` turned integers with nulls into floats. pandas writes the
integers inside nested values as floats. Test: `test_json_from_polars_and_from_pandas`.

### Fixed-size lists with nulls

pyarrow up to 25.0.1 cannot read a Parquet fixed-size list column that holds a null:
"Expected all lists to be of size=2 but index 1 had size=0"
([apache/arrow#35692](https://github.com/apache/arrow/issues/35692)). pyarrow 24 and
older refused to write such columns; pyarrow 25 and Polars write them, and Polars,
DuckDB and the pyarrow 26 nightly read them. Embeddings stored as a Polars `Array` with
null rows hit it through `pd.read_parquet`.

`read_parquet_table` in `base_storage.py` reads files with a fixed-size list through
Polars and casts them to the Arrow schema stored in the file, which keeps the pandas
index and types. Block outputs and `exact_types` loads use it. It applies only below
pyarrow 26; upgrade to 26 when it is released and remove it.
Test: `test_fixed_size_lists_with_nulls_load_into_pandas`.

### Export

- pandas wrote the default index to CSV and Excel files, which came back as a column
  named `Unnamed: 0`. A default range index is left out; a named or other index is still
  written. Test: `test_csv_export_leaves_out_the_default_index`.
- pandas Parquet exports coerced every timestamp column, first to milliseconds, which
  dropped microseconds, and then to microseconds with truncation allowed, which dropped
  nanoseconds. Frames with nanosecond columns are written in microseconds for Spark,
  Athena and Hive, and a value with nanoseconds raises; `coerce_timestamps=None` writes
  nanoseconds. Other frames keep their units.
  Test: `test_nanoseconds_raise_unless_written_as_nanoseconds`.
- HDF5 loads and exports shared the directory `.tmp` under the working directory, which
  the first call to finish emptied and removed. Each call now gets its own temporary
  directory.
- Parquet does not keep the width of dictionary values: a `large_string` dictionary
  comes back as `string`.

### pandas NaN and NA

pandas 3 treats NaN and NA as the same missing value in pyarrow-backed and nullable
float columns, unless `future.distinguish_nan_and_na` is set. A NaN that Polars wrote
loads as NaN, and the first pandas arithmetic on it returns NA. Test:
`test_block_outputs_in_s3`.

## Data integration connectors

The Amazon S3 source and destination of `mage_integrations`. Test:
`test_integrations.py`.

Source:

- Parquet was read with `pd.read_parquet`, which turned integer columns with nulls into
  floats, and failed on fixed-size lists with nulls. CSV was read with pandas' C parser,
  with the losses above, and integer columns were discovered as `number`. Both formats
  are read with Polars into pyarrow-backed pandas.
- Lists and structs were discovered as `string`. They are `array` and `object`.
- `records_from_frame` converted pyarrow-backed lists to NumPy arrays, which turned
  integers into floats and nulls into NaN. pyarrow-backed columns are converted with
  `to_pylist`, and float NaN becomes None, since NaN is not JSON. The API source uses the
  same function.

Destination:

- Each batch was written to a file named after the second it was written in, so batches
  written within the same second replaced each other and their rows were lost. Names
  carry microseconds and a random suffix.
- Records were built into a frame with `pd.DataFrame`, which turned integer columns with
  nulls into floats. Columns the schema types as integer are nullable integers.
- CSV files held nested values as Python reprs, `{'a': None}`. They hold JSON.

The GCS destination had the same code and is fixed the same way. The GCS and Azure
sources read files with the same Polars reader as the S3 source; CSV in another encoding,
which the GCS source detects, is decoded to UTF-8 first. Neither has a service in the
suite, so they are covered by unit tests of the reader.

Other connectors built frames from records with pandas, with the same float conversion:
the Snowflake and BigQuery destinations, the sample data sources send to the UI, and the
API source's discovery, which typed integer columns with nulls as `number`. They use
`frame_from_records`, which also infers integer columns when the schema gives no type.

### Delta Lake

The Delta Lake destinations (S3, Azure and the shared base) failed to import since
deltalake was upgraded to 0.20 (upstream #5541, November 2024): the writer copied from an
older deltalake imported names that no longer exist. Before that, the S3 destination's
table URI passed a list to `posixpath.join`, which raised TypeError on every export
since September 2023. Both are broken on upstream master.

The destination now writes with deltalake's `write_deltalake`. Measured against MinIO
with the old type handling replaced:

- Records went through a pandas frame, so integers with nulls became floats, and every
  column holding a null was turned into text with `''` for null. The table schema was
  overwritten on each batch, so a column's type depended on the batch. Columns now take
  their Arrow type from the stream schema; arrays and objects are JSON text.
- In overwrite mode each batch replaced the table, so a sync kept only its last batch.
  The first write of a sync replaces the table, and later batches append.
- Partitioned overwrite rewrote the Delta log after each commit to keep partitions the
  batch did not touch. It uses a `predicate` on the batch's partitions, once per
  partition and sync.
- New columns are merged into the table schema.
- Without a Delta log, objects under the table path were removed by a prefix without a
  trailing slash, so a table named `orders` also removed `orders_archive`, and only the
  first 1,000 objects were listed.
- The S3 destination takes `aws_endpoint`, for S3-compatible storage.

Test: `test_delta_lake.py`.
