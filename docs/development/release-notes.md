# Release notes

## v0.9.79-ml.5 (unreleased)

Changes since `v0.9.79-ml.4`. The fork runs on Python 3.12 with pandas 3.0, Polars 2.0,
pyarrow 25 and NumPy 2. Most changes fix values that Mage changed or lost without an
error; the per-service findings are in `docs/development/*-integration.md` and
`postgres-data-integrity.md`.

### Behavior changes to check before upgrading

Each item says what changed, which pipelines it affects, and what to do.

#### Block outputs

- **A missing upstream output raises.** When an upstream block's output file or
  directory is missing, the next block fails with `Failed to read ...`. It used to
  receive `{}` and run on it. Pipelines that relied on running after a deleted or
  partial output now fail at that block.
- **Outputs are written atomically.** On local storage, a block output is written to a
  hidden staging directory (`.output_0.<id>.staging`) and swapped in when complete. A
  killed process can leave such a directory behind; it is ignored and safe to delete.
- **LazyFrame outputs are data.** A block that returns a Polars LazyFrame has the result
  streamed to Parquet; the next block receives `pl.scan_parquet` of it, on local storage
  and on S3. The query plan used to be pickled and run again by the next block, against
  whatever its sources held then.
- **The notebook preview of a Polars output holds a sample.** Every row was converted to
  JSON for it. A Duration column failed the preview, and so did bytes that are not UTF-8,
  in Polars and pandas outputs; durations show as ISO 8601 and bytes as hex.
- **pandas and Polars dtypes are kept** across blocks: nullable integers, categoricals,
  non-string column labels, MultiIndex columns, decimals, UUIDs, bytes and values inside
  dicts and lists. Outputs written by earlier versions read as before.

#### File exports (local, S3, GCS, Azure)

- **pandas CSV and Excel exports leave out the default index.** The default range index
  used to be written and came back as a column named `Unnamed: 0`. A named or other
  index is still written. Readers that expected the extra first column need updating.
- **Parquet exports of pandas frames:** frames with a nanosecond column are written in
  microseconds, and a value with nanoseconds raises `would lose data`. Pass
  `coerce_timestamps=None` to write nanoseconds. Other frames keep their timestamp units;
  the exporter used to coerce every frame to milliseconds.
- **JSON exports of pandas frames** write ISO dates with microseconds.

#### Data integration connectors

- **Records carry Python values.** Sources that read files (S3, GCS, Azure) and the API
  source read through Polars into pyarrow-backed pandas. Records hold Python `datetime`,
  `date` and `Decimal` values where they held pandas `Timestamp`, integers stay integers,
  and NaN becomes `None`.
- **Discovery types change:** integer columns with nulls are `integer` (they were
  `number`), and lists and structs are `array` and `object` (they were `string`).
  Re-run discovery to pick up the types; existing catalogs keep working.
- **S3 and GCS destinations name batch files** `YYYYMMDD-HHMMSS-ffffff-<id>.<ext>`.
  Names had one-second resolution, and batches written within the same second replaced
  each other. Consumers that parse the file name need the new pattern. CSV files hold
  nested values as JSON.
- **Delta Lake destinations work again.** They failed to import with deltalake 0.20.
  Column types come from the stream schema; arrays and objects are JSON text. In overwrite
  mode the first write of a sync replaces the table and later batches append; a
  partitioned table replaces only the partitions it writes. Tables written before keep
  their old column types; the first write merges new columns.
- **Singer destinations validate records against the whole schema:** a column missing
  from the schema raises a validation error.
- **Sources read the command line only when run as programs**, through `main()`. A source
  built in code read the arguments of the program that built it.

#### Databases

- **MySQL new tables** get `DOUBLE`, sized `DECIMAL`, `BOOLEAN`, `DATETIME(6)`,
  `LONGTEXT`, `LONGBLOB`, `JSON` and `TIME(6)`. Floats used to be `DECIMAL(10,0)`, which
  rounded them to integers. Zoned timestamps are stored in UTC. Text values keep
  surrounding double quotes, which were removed. Table and column names are quoted.
  Tables created before keep their types.
- **PostgreSQL new tables** follow the frame's dtypes (interval, bytea, jsonb, numeric,
  arrays, integer widths). An unknown `unique_conflict_method` raises; it was treated as
  IGNORE. With UPDATE, duplicate keys in one export raise before anything is written.
- **DuckDB exports** create tables from the frame's types, insert by column name, and run
  in one transaction. With UPDATE, duplicate keys raise; DuckDB kept one silently.
- **Loads commit the transaction they start** (PostgreSQL, MySQL), so a client no longer
  blocks other sessions' `ALTER` or `DROP`, and a second load sees newly committed rows.
- **Existing tables match columns by name:** Mage's prefixed name for reserved words
  (`_name`), the plain name, or the cleaned name. Appends to tables created outside Mage
  used to fail.
- **Column names that clean to the same name** (`Total Sales`, `total_sales`) raise
  `ValueError`. One column used to overwrite the other.
- **Missing durations (NaT) are NULL** in every SQL client; they were written as
  `-9223372036854775808`.
- **Polars frames keep integers with nulls, dates and 128-bit integers** when exported
  through the pandas-based SQL clients (MSSQL, Trino, Redshift, BigQuery and others).

#### MongoDB

- **Exports store Decimal, date, timedelta, NumPy arrays, sets and UUIDs**, which raised.
  `unique_constraints` with `unique_conflict_method` upserts, and `if_exists='replace'`
  swaps the collection in one rename. `load(exact_types=True)` and `load(polars=True)`
  keep integers and decimals. Install the driver with `mage-ml[mongodb]`.
- **The MongoDB source emits dates in UTC.** It read BSON dates as local time, shifting
  them and datetime bookmarks by the host's offset. Discovered types change for embedded
  documents (`object`), arrays (`array`), Decimal128 (`number`) and mixed fields (a union);
  re-run discovery.
- **The MongoDB destination works again** and upserts on every key property; records
  whose `_id` is not an ObjectId are kept.

#### Runtime

- **Block runs execute with spawn and forkserver.** With Redis configured, the job queue's
  worker pool failed to start on macOS and Windows, and would on Linux from Python 3.14,
  so block runs stayed queued. Races in the queue that dropped new jobs, or left them
  waiting with no worker pool, are fixed. See `scheduler-soak.md`.
- **Redis keys carry a namespace** from the metadata database URL, so deployments that
  share a Redis server keep their jobs and locks apart. Stop the old scheduler before the
  new one starts: during an upgrade the two versions do not see each other's locks.
- **A Rust panic in Polars or pyarrow fails the block run** and its retries apply. The run
  used to stay running.
- **Event, metric and cache timestamps** come from `time.time()`. On hosts outside UTC
  they were off by the UTC offset.
- **Passwords** are truncated to 72 bytes before hashing, as bcrypt 4 did, so existing
  hashes keep verifying with bcrypt 5.
- **Publishing** the image and the distributions runs on manual dispatch only.
- **The scheduler's memory check works outside containers on every platform.** It ran
  Linux's `free` command, which macOS lacks, so it printed an error on every heartbeat
  and never checked memory there; on Linux, `free -t` counted swap in the total. It reads
  the machine's memory with psutil.

#### R blocks

- **R blocks run without Docker, with R 4.6 and an rv environment** that `mage r init`
  creates in `<project>/r`. They used to run only in the Docker image, with whatever
  packages its R library held. See `r-blocks.md`.
- **Data frames cross as Arrow** and keep 64-bit integers, lists, dates, time zones,
  durations and times. They used to cross as CSV: integers above 2**53, lists, dates and
  time zones were lost, and every column came back as text or float.
- **Pipeline variables cross as JSON.** They used to be pasted into the R code, so a
  quote in a value broke the block. `global_vars` is now a list.
- **R errors fail the block with R's message and the block's lines**; the output used to
  be lost. Blocks mark their function with `#* @transformer` and the other annotations,
  and their tests with `#* @test`; blocks that define `load_data()`, `transform()` or
  `export_data()` still run.
- The Docker image installs R 4.6 from CRAN and rv 0.20, in place of Debian's R with
  pacman and renv.

#### SQL blocks

- **SQL blocks keep integers** on PostgreSQL, MySQL and DuckDB, in standard and raw SQL
  blocks. pandas read_sql turned integer columns with NULLs into float64, so
  `9223372036854775807` became `9.223372036854776e18`. Integer columns are nullable Int64,
  or UInt64 for unsigned ones; the other columns keep read_sql's types, so numeric and
  DECIMAL are float and MySQL TIMESTAMP is naive in the session time zone, as before.
- **The tables Mage creates for `{{ df_1 }}` keep the frame's column names.** Names on the
  reserved word list, such as `date`, `name` and `text`, were prefixed with an underscore,
  so `SELECT date FROM {{ df_1 }}` failed. SQL blocks that used `_date` for a frame's
  `date` column need `date`.
- **Polars Int128 and UInt128 columns are written to PostgreSQL as numeric(39, 0)**. They
  were written as text.
- **Polars dates and date-times after the year 9999 export to PostgreSQL.** The export
  built Python datetimes, which end at 9999, and failed with a Rust panic. Polars renders
  them, and nanoseconds are rounded to microseconds as for pandas columns; they were
  truncated.

### New

- R blocks with the `mageml` R package: annotations for block functions and tests,
  pipeline variables, and `read_sql`, `write_table` and `db_connect` for the databases of
  `io_config.yaml`. `mage r init`, `mage r sync` and `mage r status` manage the R
  environment.
- Extra: `pointblank`, for data validation in `@test` functions.
- `load(exact_types=True)` and `load(polars=True)` on the PostgreSQL, MySQL and DuckDB
  clients and on the S3 and other file clients. They keep integers with nulls, decimals,
  unsigned and 128-bit integers, nested values and zoned timestamps. The default load is
  unchanged.
- S3 block output storage streams LazyFrames with Polars and pages through any number of
  keys.
- The S3 client and the Delta Lake S3 destination take an endpoint, for S3-compatible
  storage such as MinIO.
- Extras: `mlflow` (mlflow-skinny 3.17 and skops) and `duckdb`.
- Integration tests against real services, run with `make -C integration_tests ci`:
  PostgreSQL, MySQL, MongoDB, Redis, a REST API service, Feast, MLflow, DuckDB, S3
  (MinIO), and R blocks with R 4.6 and rv.
  `make -C integration_tests test-soak` runs the scheduler with many concurrent
  pipelines.

### Upstream issues and workarounds

- **pyarrow 25 and older cannot read a Parquet fixed-size list column that holds a null**
  ([apache/arrow#35692](https://github.com/apache/arrow/issues/35692)). Mage reads such
  files through Polars. The fix ships in pyarrow 26; the workaround switches off there.
- **pandas 3.0's CSV parser** reads `-2**63` as missing in a column with other missing
  values, and drops null rows of one-column files. Loads with `exact_types` or `polars`
  read CSV with Polars.
- **pandas 3** treats NaN and NA as one missing value in pyarrow-backed and nullable float
  columns unless `future.distinguish_nan_and_na` is set; the first arithmetic turns NaN
  into NA.
- **RPostgres** writes dates before the year 1000 without leading zeros, which PostgreSQL
  reads as years after 2000, and truncates the microseconds of date-times.
  `mageml::write_table` formats them itself. R's arrow package truncates the microseconds
  of date-times too; Mage rounds them.
- **Polars 2** wraps Python datetimes outside the years 1677 to 2262 when it builds a
  nanosecond Series from them: the year 1 becomes 1754. Build such columns in
  microseconds.
- **rv 0.20** parses `dev_dependencies` in `rproject.toml` but installs none, so the
  integration tests' R environment lists testthat, lintr and roxygen2 as dependencies.
- **Feast 0.66** rounds Int64 features above 2**53 in pushes with a NULL in the column,
  and keeps the last write over the latest event. See `feast-integration.md`.
