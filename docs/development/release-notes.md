# Release notes

## v0.9.79-ml.5 (unreleased)

Changes since `v0.9.79-ml.4`. The fork runs on Python 3.12 with pandas 3.0, Polars 2.0,
pyarrow 25 and NumPy 2. Most changes fix values that Mage changed or lost without an
error; the per-service findings are in `docs/development/*-integration.md` and
`postgres-data-integrity.md`.

### Behavior changes to check before upgrading

Each item says what changed, which pipelines it affects, and what to do.

#### Block outputs

- **Block runs have attempts.** Each claim of a block run by a worker increments
  `block_run.attempt` (a database migration adds the column), and the worker's COMPLETED
  and FAILED writes apply only while the block run is RUNNING in its attempt. A worker
  the scheduler gave up on (crash reset, timeout, cancel, retry) no longer overwrites the
  newer state; its result is discarded with a warning. Retrying blocks stops their
  running jobs first. See `docs/development/durable-execution.md`.
- **Scheduler restarts are not block crashes.** A block run interrupted when the
  scheduler running it stopped runs again and counts as an interruption
  (`MAGE_BLOCK_RUN_MAX_INTERRUPTIONS`, default 10). It counted as a crash, so three
  deploys during a long block failed it as running out of memory.
- **Set, frozenset and tuple columns keep their type.** A pandas output with such a
  column came back with numpy arrays in it; the values are now stored as tagged JSON and
  read back as sets, frozensets and tuples. The MySQL exporter writes sets as sorted JSON
  arrays, as it writes lists.
- **A missing upstream output raises.** When an upstream block's output file or
  directory is missing, the next block fails with `Failed to read ...`. It used to
  receive `{}` and run on it. Pipelines that relied on running after a deleted or
  partial output now fail at that block.
- **Outputs are written atomically.** On local storage, a block output is written to a
  hidden staging directory (`.output_0.<id>.staging`) and swapped in when complete. A
  killed process can leave such a directory behind; it is ignored and safe to delete.
  Two processes writing the same output no longer fail with `Directory not empty`; the
  last writer wins.
- **LazyFrame outputs are data.** A block that returns a Polars LazyFrame has the result
  streamed to Parquet; the next block receives `pl.scan_parquet` of it, on local storage
  and on S3. The query plan used to be pickled and run again by the next block, against
  whatever its sources held then.
- **The notebook preview of a Polars output holds a sample.** Every row was converted to
  JSON for it. A Duration column failed the preview, and so did bytes that are not UTF-8,
  in Polars and pandas outputs; durations show as ISO 8601 and bytes as hex.
- **Sparse matrices inside list and dict outputs read back.** They were stored as a dense
  table, and reading the output failed with `scipy.sparse does not support dtype
  object`. They are stored as npz; outputs written the old way read as before.
- **GeoDataFrames pass to the next block as GeoDataFrames.** Type inference took them for
  pandas frames, so they were stored as plain Parquet and came back without their
  geometry type and coordinate reference system. They are stored as GeoParquet, on local,
  S3 and GCS storage; the old shapefile writer, which cut column names to 10 characters,
  is gone. The notebook shows them as tables with WKT geometry; the output failed.
- **NumPy arrays and lists of objects pass to the next block.** They were stored as their
  description, and the next block received a dict such as `{'module': 'numpy', 'name':
  'ndarray', ...}`. They are pickled; generators and objects that cannot be pickled are
  stored as their description, with a warning.
- **A block's outputs reach the next block in order.** They were read in text order, so
  a block that returned 11 or more outputs passed `output_10` before `output_2`.
- **Pipelines that run in one process store the error of a failed block.** The error was
  left out because the exception is not JSON, so the run page and failure notifications
  had none.
- **`cache_block_output_in_memory` drops each output once its downstream blocks ran.**
  Every output stayed in memory until the run ended.
- **Outputs that are not stored leave no files.** With `cache_block_output_in_memory`,
  each block still wrote shape files into an `output_0` with no data.
- **Blocks without tests no longer read their output back** after writing it. The whole
  output was loaded again for tests that did not exist.
- **With `cache_block_output_in_memory`, an upstream output that is not in the cache is
  read from storage.** In a resumed run, the blocks that completed before were missing
  from the cache, and their downstream blocks received no input instead of an error.
- **Block tests run on the output with `cache_block_output_in_memory`.** They received the
  result dict's key, `'output'`, and every block with a `@test` function failed.
- The async write of list and dict outputs that hold objects, such as frames or models,
  raised `AttributeError`: it called a misspelled method.
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

#### ClickHouse

- **Tables Mage creates use MergeTree.** They used the Memory engine, which keeps data in
  RAM only, so a restart of ClickHouse emptied every exported table and every SQL block's
  table. Tables created before keep their engine.
- **Column types hold the values.** Nullable Int64, int32 and uint64 columns became
  String, and uint64 values failed to insert; dates, decimals and UUIDs failed; datetimes
  became DateTime64(3), which dropped their microseconds. Columns get their integer width,
  Date32, Decimal(P, S), UUID, DateTime64 at the column's precision, in UTC for zoned
  columns, and Int128 for wider Python integers. Durations are stored in microseconds, and
  lists and dicts as JSON text.
- Column and table names are quoted, so names with spaces or keywords work.
- **The ClickHouse destination of mage_integrations writes data.** It failed on its first
  SCHEMA message: SQLAlchemy 2 removed `MetaData(bind=...)`. Its column types made
  integers Int32, which wrapped 2**53 + 1 around to 1, numbers Float32 and date-times
  DateTime without microseconds, and no column was Nullable, so NULL became 0, `''`,
  false or 1970-01-01. Columns are Int64, Float64, Bool, DateTime64(6, 'UTC'), Date32 or
  String, Nullable unless the schema rules out null, and date-times keep microseconds. A
  second sync into a table failed on its date-time columns.

#### Kafka

- **The Kafka source and sink work with Kafka 4.** `api_version` defaulted to 0.10.2, so
  every request timed out against Kafka 4, which removed the protocol versions older
  than 2.1. It defaults to the broker's version; a set `api_version` is kept.
- **The sink waits until Kafka has the messages.** `batch_write` returned with messages
  in the producer's buffer, and the source then committed their offsets, so a crash lost
  them; failed sends, such as a message over the size limit, went unnoticed. It flushes
  and raises for a failed send. Dates and decimals in messages are written as JSON.
- **The source commits offsets in single-message mode.** It never did, so a consumer
  group that restarted skipped or read again what came while it was down.

#### Streaming pipelines

- **Messages are copied only for blocks that share them.** Every transformer and sink got
  a deep copy of each batch, which cost about 4 ms per 1000 messages per block, more
  than most transformers. When a block has several downstream blocks, all but the last
  get a copy; the last one, and the only one in a chain, gets the messages in a list of
  its own. A block that changes its messages in place still changes no other block's.
- **Execute pipeline works on macOS.** The notebook runs a pipeline in a process started
  with spawn, macOS's default. That process imported the scheduler, which started a
  multiprocessing Manager while the process was starting, and it died. The scheduler
  creates its job manager on first use. This affected every pipeline type, and would
  affect Linux from Python 3.14, which no longer forks by default.
- **Streaming source and sink blocks check their connection when run alone.** Their YAML
  ran as Python and failed with a NameError; the notebook hid the Run button. The block
  renders its config, connects, and closes; the button reads "Check connection".
- **Database sinks keep integers.** The Postgres sink and the generic sink (MySQL,
  ClickHouse, DuckDB, MSSQL, BigQuery) made an integer field with a missing value
  float64, so 2**53 + 1 became 9007199254740992.0. The Postgres sink also wrote messages
  with the `{"data", "metadata"}` format as two columns.
- **ClickHouse exports write any value into a String column as text.** A column created
  from a first batch whose values were all missing is String, and later integers failed
  to insert.
- **The Kafka templates no longer set `api_version: 0.10.2`**, which fails against Kafka 4.
- **The RabbitMQ source acks messages.** It never acked them unless the transformer did,
  so a restarted pipeline read every message again. A message is acked when the
  transformer returns, unless the transformer acked, nacked or rejected it with
  `kwargs['channel']`; a failed transformer leaves it in the queue. Also:
  - `prefetch_count` (default 100): the broker sent the whole queue, which the source
    held in memory.
  - `batch_size` and `batch_timeout`: the transformer gets lists of messages, so sinks
    write in batches. The default is one message, as before.
  - Inactivity timeouts no longer reach the transformer as messages of None values.
  - The template set the consume options outside `consume_config`, where they were
    ignored.
- **The RabbitMQ sink waits for the broker's confirms**, so a message no queue takes
  raises; it was lost. Messages are persistent and JSON-encode dates, decimals and UUIDs,
  which failed.
- **RabbitMQ credentials are quoted and not printed.** A password with `@`, `/` or `:`
  failed to connect, and the source and sink printed the URL with the password.
- `encode_complex` writes UUIDs, bytes and `datetime.timedelta`, which simplejson
  reported as circular references; this affected the Kafka sink too.
- **The NATS JetStream source acks after the transformer.** It acked each message before
  the transformer ran, so a failed transformer lost its batch. A failed transformer's
  messages are nacked and delivered again. A message that is not JSON reaches the
  transformer as text; it failed every fetch and came back each time. The push consumer
  no longer stops the first time no message arrives within the timeout. A failed
  connection raises; it was printed and the source failed later with an AttributeError.
  The pull consumer connected twice. New setting: `ack_wait`.
- **A NATS JetStream sink**, which waits for JetStream to store each message.
- **A PostgreSQL streaming source** (`connector_type: postgres`), which reads the changes
  of tables with logical replication: typed rows with the operation, table, log position
  and commit time. It confirms a transaction to the server once the transformer has
  handled all of its changes and saves the position in the streaming checkpoint, so a
  restart loses nothing. It creates the slot and publication when missing, reads
  unchanged TOASTed values from the table, and turns a changed primary key into a delete
  and an update. Checking its block in the notebook creates no slot.
- **The ActiveMQ source acks after the transformer.** The transformer ran on the STOMP
  receiver thread, which swallowed its errors, and the subscription acked messages on
  delivery, so a failed transformer lost its messages while the pipeline seemed to run.
  A failed transformer stops the pipeline and its messages go back to the queue; a lost
  connection stops the pipeline too. New settings: `batch_size` and `batch_timeout`. The
  docs listed settings the source does not take.
- **The ActiveMQ sink sends persistent messages and waits for the broker's receipt.**
  Messages were not persistent, a rejected message was lost, and dates failed to encode.
- **The MongoDB change stream source resumes after a restart.** Each start watched from
  that moment, so changes made while the pipeline was stopped were lost. The resume
  token of the last handled change is saved in the streaming checkpoint, for its
  database and collection. Every error, from the transformer too, was printed and the
  pipeline stopped as if it had finished; errors raise. `operation_time` failed, since it
  was passed to watch under a name it does not take; it takes seconds since the epoch.
  Without a collection the source watched nothing; it watches the database.
- `encode_complex` writes ObjectIds and Decimal128 as text and BSON timestamps as
  `{"t", "i"}`, so change documents reach JSON sinks.

#### Trino

- **Exports keep their values.** Columns were BIGINT, DOUBLE, BOOLEAN, TIMESTAMP or
  VARCHAR. TIMESTAMP is `timestamp(3)` in Trino, so timestamps lost their microseconds;
  uint64 values failed, and dates, decimals, UUIDs and bytes failed, since their columns
  were VARCHAR. Columns get their integer width, DECIMAL(20, 0) for uint64,
  DECIMAL(P, S), DATE, TIMESTAMP(6), with time zone for zoned columns, TIME(6), UUID
  and VARBINARY. Durations are stored in microseconds, and lists and dicts as JSON
  text. In Delta Lake, which has no UUID or TIME type, these are text, and zoned
  timestamps keep milliseconds. `data_type_properties.timestamp_precision` still sets
  the precision.
- **Rows go in batched INSERT statements.** Each row was its own query, and in Iceberg
  its own commit; 20,000 rows exhausted a 1 GB Trino heap.
- **Appends match columns by name** and write values as the table's column types. Rows
  were inserted by position.
- **Replace works in the memory connector**, which cannot DELETE, by truncating the
  table. A replace from a query into a kept table ran CREATE TABLE AS and failed.
- **Failed queries raise.** `load` and `execute_queries` printed the error, ran the query
  twice more and returned None or an empty list.
- **Loads keep the query's order.** They ran `SELECT * FROM (query) LIMIT n`, and Trino
  drops an ORDER BY in a subquery, so rows came in any order.
- `table_exists` matches exact names; `SHOW TABLES LIKE` read `_` as any character.
- `load(exact_types=True)`, `load(polars=True)` and `load(nullable_integers=True)`, as for
  MySQL. Trino SQL blocks keep integer columns with NULLs as integers.
- **The Trino destination of mage_integrations keeps text.** Apostrophes were replaced
  with double quotes, so `it's` was stored as `it""s`; text was cut at 255 characters
  without an error; empty arrays became NULL. In the memory connector, objects were
  stored as JSON strings, arrays failed, and an apostrophe in an array failed the sync.
  Date-times stay text. Tables created before keep their column types.

#### PostgreSQL change data capture (LOG_BASED replication)

- **The first change after the initial sync is read.** The initial sync bookmarked the
  slot's confirmed LSN, where the next change can start, and the next run skips changes
  at or before the bookmark. On an idle server that change was lost. The bookmark is
  the byte before.
- **Changes reach the destination with their types.** Log values were text, so ids
  became strings and arrays failed the destination; they were matched to the columns of
  information_schema, in no set order. Values are matched by the names in the log and
  converted with PostgreSQL's type casters.
- **No change is lost or read twice.** The first sync bookmarked the server's position at
  its end, so changes made while the table was read were never read. Each change was
  confirmed to the server when read, so a failed destination lost it. The change at the
  bookmark was read again. A sync took its end at the WAL write position, before
  transactions committed with synchronous_commit off, and stopped before them.
- Only the table of the configured schema is read; a table of the same name in another
  schema leaked in.
- Unchanged large (TOASTed) values are read from the table; NULL was written over them.
- An update of the primary key marks the old key's row as deleted.
- From PostgreSQL 14, a sync stops once it has read the log; it waited
  `logical_poll_total_seconds`, 60 by default, after the last change.
- **The PostgreSQL destination keeps the last record of each key in a batch**, which
  failed with "ON CONFLICT DO UPDATE command cannot affect row a second time", and a
  delete only marks the row as deleted; its NULLs were written over the row's values.
  The Trino destination's MERGE keeps the last record of each key too.

#### PostgreSQL and MySQL sources and destinations of mage_integrations

- **The PostgreSQL source syncs bytea, timetz, interval and array columns.** It failed on
  bytea, timetz and interval values, which its JSON writer could not encode. It
  discovered arrays as their element type and interval as an integer, so the destination
  failed writing them. Arrays are discovered with their item type, interval as a string
  and json and jsonb as objects. Binary values are written as `\x` hex text.
- **The PostgreSQL destination writes arrays and JSON.** NULL in an array was written as
  `None`, text with commas or quotes broke array literals, JSON values were written
  unencoded, and an apostrophe in JSON ended the SQL string.
- **The MySQL source syncs SET and binary columns.** SET values failed to encode, and
  non-UTF-8 bytes failed the log writer. SET values are written comma-separated.
  Sessions use UTC, so TIMESTAMP values no longer shift with the server's time zone.
  Unsigned columns are discovered with `minimum: 0`, and incremental syncs on BIGINT
  UNSIGNED keys compare above 2**63.
- **The MySQL destination keeps values.** Integers were cast to UNSIGNED, so every
  negative value failed; strings were CHAR(255), which cut them at 255 characters and
  dropped trailing spaces; booleans and date-times were stored as CHAR(52); integers
  were INT, which failed above 2**31; backslashes in text were read as escapes. New
  columns are BIGINT, BIGINT UNSIGNED for unsigned source columns, BOOLEAN, DOUBLE, JSON,
  DATETIME(6) and LONGTEXT, or VARCHAR(255) for keys. Tables created before keep their
  types. The Doris destination, which used the same functions, keeps its previous types.
- Limits of the Singer format that remain: decimals become double precision, NaN and
  Infinity become NULL, dates become date-times at midnight UTC, and JSON null becomes
  NULL.

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

- **Stopping the server stops the scheduler and every block run process.** SIGTERM, or
  killing the server, left the scheduler, its worker pool, block workers and
  multiprocessing managers running. A scheduler left behind kept running pipelines next
  to the scheduler of the next server, so block runs ran twice and one copy failed. The
  server now stops the scheduler on SIGTERM, the scheduler stops its block runs, and
  each of these processes exits when its parent is gone. Cancelling a block run kills
  the processes the block started, such as R, along with it.
- **Listing pipeline runs works after a pipeline is deleted.** With
  `include_pipeline_type`, the list failed with an error once any run's pipeline was
  gone; such runs have no type.
- **A block run whose process keeps dying fails after 3 crashes**
  (`MAGE_BLOCK_RUN_MAX_CRASHES`). Crashed block runs were run again with no limit, so a
  block that ran out of memory restarted forever. The crash count is in the block run's
  metrics.
- **Job keys in Redis are deleted when the job ends.** They were never deleted, so a block
  run that last ran on a replica that is still alive counted as running there, and
  running it again from another replica left it queued.
- **Block and pipeline timeouts fire on machines that are not on UTC.** SQLite returns the
  stored UTC times without a zone, which were read as local time, so a run west of UTC
  looked hours younger and never timed out.
- **Retried block runs start clean.** "Retry blocks" and "Retry incomplete block runs"
  reset only the status: the old `started_at` made a block with a timeout time out at
  once, and the earlier error and crash count carried over. "Retry blocks" also restarts
  the pipeline run's timeout.
- **Block run metrics changed in place are saved.** The error of a failed block run and
  the metrics merged when a block completes were set on the stored dict and SQLAlchemy
  saw no change, so they were lost.
- **API errors are returned.** The error handler called `asyncio.run` inside the server's
  event loop, which raised, so a failing request got an empty response.
- **Errors reported to Sentry leave out local variables**, which held the DataFrames of
  block code.
- **Each job starts one worker process.** The worker pool started workers while the queue
  was not empty, and a worker took its job only after it started, seconds later with
  spawn (macOS, Linux from Python 3.14). Every block run started up to 20 processes,
  the queue's concurrency, that each loaded Mage (about 370 MB) and exited. The pool
  takes each job from the queue and starts one worker for it.
- **Retry configs merge as documented: project, pipeline, then block.** The pipeline's
  `retry_config` was ignored for batch blocks, and a null field replaced the project's
  value; `retries: null` failed the block with a TypeError. Blank and null fields now
  inherit.
- **Pipelines that run in one process get the trigger's `allow_blocks_to_fail`.** The
  process stopped at the first failed block, and the scheduler started another one for
  the remaining branches.
- **Global hooks read the outputs of their pipeline's run.** They raised TypeError
  (`get_outputs` takes no `sample` argument).
- **Dashboard charts over the last runs of a block show each run's output.** Every run
  read the notebook's output.
- **Downstream block runs start when their upstream finishes.** They waited for the next
  scheduler run, up to `SCHEDULER_TRIGGER_INTERVAL` (10 seconds). The scheduler runs
  again when a job finishes, at most once a second. A trigger run of 5 blocks on 3
  million rows took 56 seconds and takes 33.
- **Block runs execute with spawn and forkserver.** With Redis configured, the job queue's
  worker pool failed to start on macOS and Windows, and would on Linux from Python 3.14,
  so block runs stayed queued. Races in the queue that dropped new jobs, or left them
  waiting with no worker pool, are fixed. See `scheduler-soak.md`.
- **A crashed scheduler's block runs run again within 30 seconds.** Another scheduler
  process treated them as running while the crashed one's liveness key lived, 300
  seconds. The key lives 30 seconds, set with `MAGE_QUEUE_LIVENESS_SECONDS`, and the
  worker pool renews it every second; a stopped scheduler deletes it.
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
- **The notebook warned that the kernel had restarted when it had not.** The warning
  compared the kernel's process id with the previous one, which is unset on the page's
  first usage response, so running a block right after opening the editor showed it and
  cleared the running-block indicators.
- **A server stopped with SIGTERM while Python ran a finalizer kept running.** The signal
  handler raised SystemExit, which Python ignores inside `__del__`; a zmq context collected
  at that moment left the server and its scheduler up. The event loop now handles SIGTERM.
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
- **R blocks run from the editor keep their annotations.** The notebook removed every
  line starting with `#` from the code it sent to the kernel, including the block's code,
  so a block whose function was not named `load_data`, `transform` or `export_data`
  failed with "The block has no function". Only Python comment lines are removed now;
  lines in strings stay, which also keeps Rust attributes such as `#[derive(...)]`.
- **An R block stops when its run is interrupted or the process running it is gone.**
  Interrupting an R block in the notebook, or killing the kernel or worker, left R
  computing. Rscript now runs under a small supervisor that stops it, with the processes
  it started, once its parent is gone; this adds about 15 ms to a block run.
- The Docker image installs R 4.6 from CRAN and rv 0.20, in place of Debian's R with
  pacman and renv.
- **R environments install on Linux arm64 and other platforms without binaries.** The
  Docker image lacked the libraries that source builds need, and the tidyverse's `fs`
  failed without libuv; arrow built without S3. On Debian and Ubuntu, `mage r setup`
  prints the `apt-get install` command for the missing libraries.
- **New R environments hold DBI, RPostgres, RMariaDB, duckdb, RSQLite and httr2** with
  the tidyverse, for the R block templates; the first `mage r init` installs more. On
  macOS, RMariaDB comes from CRAN, since Posit Package Manager has no binary for R 4.6.
- **The block menus list SQL and R first.** R is a submenu with the base block and the R
  templates; the "R block" item of the Python list is gone. Submenus that would cross the
  right edge of the editor open to the left.
- Block templates read from a file end with a newline.
- The SQLite exporter template creates the database's directory; SQLite does not, and
  the export failed with `unable to open database file`.

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

- **Pipeline environments.** A pipeline can give its Python blocks their own packages:
  name a requirements file in the pipeline's settings (or `environment:` in its
  metadata.yaml) and its data loaders, transformers, data exporters and custom blocks run
  in a virtual environment uv builds from it, in the notebook and in triggered runs. The
  environment is built once and reused; pandas, pyarrow, Polars and NumPy stay at Mage's
  versions unless the file pins them. `mage export service` installs the environment's
  packages in the image. See `docs/guides/pipelines/pipeline-environments.mdx`.
- **Pipeline services run R and SQL blocks.** `mage export service` exports R blocks with
  the project's rv environment (the image installs R 4.6 and the packages rv.lock pins)
  and SQL blocks on PostgreSQL, MySQL, DuckDB and ClickHouse, which run as in a Mage pipeline run. Rust blocks are
  compiled into the image, and secrets can come from AWS Secrets Manager and Parameter
  Store. See `docs/production/pipeline-services.mdx`.
- **Light theme.** Settings > Preferences > Light theme, or Light theme in the user menu,
  switches this browser to light colors; dark stays the default. The choice is a cookie,
  so the static export starts in it without a dark flash. The code editors use GitHub's
  light colors, code blocks Prism's One Light, and ColumnAtlas and the charts follow the
  theme. `mage-console --light` does the same for the terminal console.
- **ColumnAtlas**, the block output explorer, replaces the table of pandas and Polars
  outputs in the notebook and on block runs. It reads the whole stored output, not a
  sample: column profiles in the headers (distribution and missing share), sort and
  filter on the server, a full window with per-column summaries, keyboard navigation and
  a cell inspector. GeoDataFrame outputs open too, with geometry as WKT. The engine is a
  Rust extension on Polars (`rust/column_atlas`), run
  in worker processes with deadlines and memory limits; 128 rows of a 2-million-row
  output arrive in 4 to 20 ms. Other outputs and outputs on S3 or GCS show the plain
  table. Printed text and errors show in full, as before. `MAGE_COLUMN_ATLAS=0` turns it off. See `docs/design/column-atlas.mdx` and
  `column-atlas.md`. Building Mage from source now needs Rust (`rustup`); the Docker
  image has it in the build stage only. Hovering a mini chart shows its numbers at once:
  a histogram bar its range, row count and share; a category its name, count and share;
  the completeness bar and missing marks their counts. All are computed over the whole
  output.
- **Block fusion** (`block_fusion: chains`, or "Run chains of blocks together" in the
  pipeline settings): blocks that form a chain run as one stage, in one process, and each
  block receives the previous block's output from memory. Every block keeps its block run,
  status, logs, retries, timeout and stored output; branches still run in parallel. A
  5-block chain on 3 million rows ran in 12 to 17 seconds instead of 29. See
  `docs/design/data-pipeline-management.mdx` and `block-fusion.md`.
- **R custom blocks and more R and Rust templates.** R blocks can be custom blocks
  (`#* @custom`, `custom()` or a function named `custom`), with a template, and the custom
  block menu offers R and Rust. New Rust templates: an API loader and exporter (JSON over
  HTTP, in batches), a Parquet or CSV file exporter, and model features per entity (lags,
  changes, running means, z-scores, ranks). New Rust projects get `ureq` for HTTP.
- **Pipeline services: `mage export service PROJECT PIPELINE...`** turns pipelines into a
  Docker image that runs them without Mage: an HTTP API to start runs (with
  `?wait=SECONDS` to get the outcome in the same call), schedules, a SQLite run history,
  logs per block, Prometheus metrics, retries, timeouts and cancellation. The runtime is
  Rust (`mage_ai/pipeline_services/service`); Python blocks run in long-lived worker
  processes, and tables pass between blocks as Arrow IPC files. The export pins only the
  packages the blocks' imports load (18 for a pandas and PostgreSQL pipeline), lists the
  environment variables the code reads, and takes secrets from the environment, `NAME_FILE`
  or a directory of secret files, redacting them from logs. `mage-service run PIPELINE`
  runs once and exits, for cron jobs and Kubernetes Jobs. `MAGE_SERVICE_SECRETS` fetches
  secrets at start-up from a provider, `[NAME=]provider:reference[#field]`; the first
  providers are IBM Cloud Secrets Manager (API key or Code Engine trusted profile) and files.
  Each export includes deployment files for IBM Cloud Code Engine, a Tekton pipeline for IBM
  Cloud Continuous Delivery and a Kubernetes manifest. Blocks that load a model with
  `load_model('fraud', uri='models:/fraud@champion')` run the same code in Mage, where the
  model comes from MLflow, and in the service, where the export has embedded the model with
  its version, run, params, metrics and requirements (`GET /v1/models`). Guide:
  `docs/production/pipeline-services.mdx`; design: `docs/design/pipeline-services.md`.
- **Faster block imports**: `mage_ai.io.postgres` no longer imports SciPy, paramiko and
  sshtunnel unless an SSH tunnel or a sparse matrix is used (1.1 s to 0.7 s), and
  `io_config.yaml` loads secrets support only when it reads a secret.
- **`mage verify-fusion`** runs a pipeline block by block and fused, compares every
  block's stored outputs and names the first block whose output differs. Floats that differ
  only by rounding are reported as close. See "Verify fusion before turning it on" in
  `docs/design/data-pipeline-management.mdx`.
- **Rust blocks**: data loaders, transformers, data exporters and custom blocks written in
  Rust. A block is one function named for its type; its parameters are the upstream
  tables (`LazyFrame` or `DataFrame`), pipeline variables or JSON values, and its return
  value is its output; `test_*` functions test it. Blocks build with Mage's `mage` crate
  (Polars, anyhow, serde) in a Cargo workspace in `<project>/rust`, where projects add
  crates. A block rebuilds in about a second after an edit and runs a cached binary when
  unchanged. Compile errors are marked in the editor as you type and shown in the output
  at the block's own lines; panics report their location. Upstream tables stored as
  Parquet reach the block by path, without passing through Python. Outputs are Mage
  outputs: ColumnAtlas, downstream Python and R blocks, retries and pipeline runs work as
  for any block. `mage rust init`, `mage rust build` and `mage rust status` manage the
  workspace. Measured through Mage's block execution: per-row logic that has no
  vectorized form ran in 0.12 s on 1 million rows, against 0.75 s with pandas and 0.68 s
  with Python Polars; a Polars aggregation of 5 million rows ran in 0.11 s, against
  0.08 s in Python Polars, which uses the same engine. Conditional Rust blocks are not
  supported yet. Rust blocks need Rust (`rustup`).
- **Playground** (`make playground`): Mage built from this repository, PostgreSQL 17 with
  a seeded shop (1.5 million order lines, a million web events, generated with Faker),
  and pipelines in pandas, Polars with block fusion, R (dplyr, tidyr, lubridate,
  tibble), Python with R, and geopandas, which export to the same database.
  `make playground-check` runs them all. See `playground/README.md`.
- R blocks with the `mageml` R package: annotations for block functions and tests,
  pipeline variables, and `read_sql`, `write_table` and `db_connect` for the databases of
  `io_config.yaml`. `mage r init`, `mage r sync` and `mage r status` manage the R
  environment.
- Extra: `pointblank`, for data validation in `@test` functions.
- R block templates in the block menus: loaders and exporters for local files, S3,
  APIs, PostgreSQL, MySQL, DuckDB and SQLite, and transformers that clean, aggregate,
  join and reshape.
- mageml `read_file`, `write_file`, `read_s3`, `write_s3` and `s3_filesystem`: CSV, TSV,
  Parquet, Feather, JSON, NDJSON and RDS files, locally and on S3 with the `AWS_*`
  settings of an `io_config.yaml` profile.
- R versions with rig: Mage runs the Rscript of the R version the rv environment names,
  from `PATH` or from the versions rig installed. `mage r setup` prints what is missing
  and how to install it; `mage r init --install-r` installs the R version with rig.
- `mage-ml[r]` extra. R and rv are not Python packages; it documents the setup that
  `mage r setup` checks.
- `load(exact_types=True)` and `load(polars=True)` on the PostgreSQL, MySQL and DuckDB
  clients and on the S3 and other file clients. They keep integers with nulls, decimals,
  unsigned and 128-bit integers, nested values and zoned timestamps. The default load is
  unchanged.
- S3 block output storage streams LazyFrames with Polars and pages through any number of
  keys.
- The S3 client and the Delta Lake S3 destination take an endpoint, for S3-compatible
  storage such as MinIO.
- Extras: `mlflow` (mlflow-skinny 3.17 and skops), `duckdb`, `trino` and `geo`
  (geopandas 1.2, for spatial and spatiotemporal data; in the Docker image).
- Integration tests against real services, run with `make -C integration_tests ci`:
  PostgreSQL, MySQL, MongoDB, ClickHouse, Kafka, RabbitMQ, NATS, ActiveMQ, Trino (memory,
  Iceberg and Delta Lake), Redis, a REST API service, Feast, MLflow, DuckDB, S3 (MinIO), streaming
  pipelines run from triggers and from the notebook, and R blocks with R 4.6 and rv.
  `make -C integration_tests test-soak` runs the scheduler with many concurrent
  pipelines.

### CI

- A Rust job runs `cargo fmt`, clippy and the engine tests; the frontend unit tests run
  with `yarn test:unit`. The publish workflow builds the extension's wheels for
  manylinux, macOS and Windows.
- The Docker image workflow builds the image and pushes it nowhere. It used to log in
  to the GitHub container registry and push when run by hand.

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
