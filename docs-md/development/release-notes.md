# Release notes

## Unreleased

### PostgreSQL

- Exports send rows in PostgreSQL's binary `COPY` format, encoded in Rust
  (`column-atlas-native` 0.2.0, from the Rust blocks' client), when every column encodes to
  its table column's type; COPY text otherwise. An export of 1 million rows of seven
  columns took about 1 second instead of 7, with the same values and types, in the same
  transaction. `MAGE_POSTGRES_BINARY_COPY=off` keeps COPY text. See
  [PostgreSQL](../integrations/databases/PostgreSQL.md#export-a-dataframe).
- `load(..., polars=True)` reads through binary `COPY` on a connection of the Rust client,
  kept for the next loads, with the column types and values of the psycopg2 load: 1 million
  rows in 0.9 seconds instead of 2.0. Queries with bound parameters, a transaction open on
  the client's connection, or connection settings the Rust client lacks go through psycopg2.
  `MAGE_POSTGRES_BINARY_LOAD=off` turns it off.

### Rust blocks

- `mage::storage` (feature `storage`) reads and writes Parquet and CSV on S3 and
  S3-compatible stores, Google Cloud Storage and Azure Blob Storage, with credentials
  from the block's io_config profile. Scans are lazy, so filters and column selections
  reach the files. See [Rust blocks](../guides/blocks/rust-blocks.md#object-storage).

### Stream services

- After a Kafka broker's controlled shutdown, librdkafka could hand a message over twice,
  and the stream wrote it twice. The source now skips an offset it already read under the
  same partition assignment, and logs it.

### Trino

- Trino SQL blocks run in [pipeline services](../production/pipeline-services.md), as in a
  Mage pipeline run.
- An empty upstream frame was skipped, and the SQL block read the rows of the table an
  earlier run left. It is now written as a table with no rows. BigQuery and Snowflake still
  skip it, and no longer fail on an empty Polars frame.
- Two Trino SQL blocks on the same profile copied the upstream block's table through pandas
  into a new table before each query; the query now reads the table itself.
- `unique_upstream_table_name` added its suffix to the tables of SQL upstream blocks too,
  which were never written under that name, and every run left its tables behind. Only the
  tables written for the run get the suffix, it now includes a random part (it was the time
  in seconds, which two runs started in the same second shared), and the tables are dropped
  when the block ends.

### Command line runs

- `mage run` records its run when it ends: a pipeline run of the pipeline's "Command
  line" trigger, with its block runs (status, times, output digests) and its run record,
  so `mage run-diff` and `mage reproduce` work on command line runs. It is recorded with
  its final status, so a server's scheduler on the same database never runs it again.
  `--no-record` skips it.

### Logs

- A block run's log is rotated at 20 MB, and the logs page and API read only the newest
  segment: whatever a block printed before the last rotation was hidden (a block printing
  300,000 lines showed 57,408), and output past 220 MB was deleted. Logs now keep every
  segment; each log item carries its newest 5,000 lines with `first_line` and
  `total_lines`, the page shows the lines before them as a row that loads them, and search
  covers every segment. The page also stopped downloading every log whole every five
  seconds (22 MB each poll for that block).

### Scheduler

- A scheduler pass read each pipeline's files about five times and the project's
  metadata.yaml four times for each running pipeline run; it now reads them once per pass.
  With 50 runs in flight a pass took 0.18 seconds instead of 0.22.

- `MAGE_JOB_ENGINE=on` runs the scheduler's jobs in warm Python workers of the Rust run
  engine (`mageml-engine` 0.2.0, `mage-service jobs`) instead of a new process per block
  or stage. A job starts without starting Python or importing MageML: a fused run of a
  three-block pipeline took 0.12 seconds instead of 1.0, the same run block by block 0.3
  instead of 2.4, and the 1 million row pipelines of the benchmark (pandas, Polars, R,
  Rust) a third of their time. Runs keep everything they record (block runs, logs, outputs,
  run records, tests, retries); a worker drops the project's modules after each job and is
  replaced when a job leaves threads or memory behind. Without the engine, jobs run in
  processes as before. See [run engine](run-engine.md).
- Runs spend less time collecting garbage. A stage collects garbage after each block to free
  its inputs, and each collection also scanned all of MageML: 33 ms per block in a warm
  worker. Job workers and `mage run` now freeze what they imported before the first block
  (`gc.freeze`): a fused three-block run on the run engine went from 0.25 to 0.12 seconds,
  and `mage run` of the same pipeline from 0.95 to 0.77.
- A run read the project's `metadata.yaml` as a Jinja template about a dozen times and
  compiled it each time, about a sixth of a short run on the run engine. Templates are now
  compiled once per text and still rendered on each read, so environment variables are
  current.

## MageML 0.2.0 (2026-10-19)

Changes since 0.1.1. The main addition is stream services: streaming pipelines exported as
services that run in the Rust engine, with Python, Rust or R blocks
([Stream services](../production/stream-services.md)).

### Behavior changes to check before upgrading

- **Stream runs from the editor and `mage service run` no longer move production's
  position.** Kafka reads with `<group>-dev-<user>`, PostgreSQL change capture with
  `<slot>_dev_<user>`, NATS with `<name>-dev-<user>`; the editor asks before a run writes to
  real sinks. Triggers and deployed services are unchanged.
- **Services live in `services/<name>/service.yaml`.** `pipeline_services.yaml` is still
  read; a saved service moves to its folder, and `mage service migrate` moves them all. A
  build writes `services/<name>/service.lock`, which pins the manifest, files,
  requirements, model versions and engine.
- **Integers too large for 64 bits are refused** in run variables and manifest values: the
  run API answers 400 and the exporter stops, naming the value. They lost their last digits
  without an error.
- **Service blocks start from an empty environment.** The service's API tokens, secret
  references and IBM Cloud key no longer reach user code; a block that read one of them
  must get it as a pipeline variable or secret. Requests come on descriptor 4 and stdin is
  `/dev/null`.
- **No service token is printed.** On 127.0.0.1 a made token goes to a private file;
  elsewhere the service needs `MAGE_SERVICE_TOKEN`.
- **The Elasticsearch sink writes each message as the document**, with `_id` as the
  document id. It wrapped messages as `{"_doc": message, "id": ...}` and never set the id;
  `_source: _doc` keeps the old shape for an existing index.
- **Pipeline `metadata.yaml` no longer stores block statuses or `variables_dir`**; statuses
  are kept beside the pipeline's outputs on each machine.
- **A block downstream of several children of one dynamic block pairs their items**: with
  `B` and `C` both downstream of dynamic `A`, `D` runs once per item with `B:i` and `C:i`. It
  ran once per combination, n * n times (upstream #5980). Dynamic upstream blocks from
  different dynamic blocks still make every combination.
- **A callback block attached to no block is a pipeline callback**: it runs once when a run
  completes or fails. Such blocks ran nothing before.
- **SQL loaders mask string literals in the queries they log** (`'…'`), so values written in
  a query (emails, names, tokens) stay out of run logs; `MAGE_LOG_QUERY_VALUES=on` shows them.
- **Kafka `offset` in services** applies while the consumer group has committed nothing,
  then the committed position (MageML seeks at every start).
- **`mage service deploy` runs the service tests first** (each stream sample through the
  blocks a sink follows) and stops when one fails; `--skip-tests "<reason>"` deploys anyway
  and records the reason on the deployment.

### Stream services

- Sources: Kafka, Azure Event Hubs (Kafka endpoint), IBM Event Streams credentials, webhooks
  (answered once the sinks confirm, `Idempotency-Key`), RabbitMQ, NATS JetStream, IBM MQ,
  ActiveMQ, Amazon SQS, Google Cloud Pub/Sub, MongoDB change streams, and PostgreSQL changes
  by logical replication.
- Sinks: Kafka, NATS JetStream, RabbitMQ, IBM MQ, ActiveMQ, Google Cloud Pub/Sub, MongoDB,
  PostgreSQL (COPY, upserts), ClickHouse (deduplicated inserts), Elasticsearch and OpenSearch
  (bulk API, `_id` replaces documents, SigV4 for AWS), InfluxDB (line protocol), object storage (S3, GCS,
  Azure Blob, disk; Parquet or JSON lines), HTTP and Feast.
- At least once everywhere: a source's position moves only after every sink took the batch.
  Dead letters take the messages a block fails on, found by splitting the batch.
- Message formats: JSON values kept exact, Kafka `RAW_VALUE` as bytes, Avro through a schema
  registry, `include_metadata`, and MageML's message shapes for each source.
- Blocks: Python, Rust (a resident program) and R (a resident R session), mixed in one
  chain; a Python block annotated `pl.DataFrame` gets each batch as a table.
- Throughput on a laptop, Kafka to Kafka through one block: a Rust block about a million
  messages a second, a Python block 586,000, a Polars DataFrame block 651,000.
- Stream images hold only what their blocks import (a Kafka stream image: 485 MB to 185 MB);
  logs rotate by size; `mage-console` shows each stream's state, lag and rates, and pauses
  it; the Kubernetes deployment carries Prometheus scrape annotations.

### Stream development in the editor

- Stream examples: `mage examples stream orders|iot|payments|clickstream|cdc|webhooks|multilang` writes a runnable stream with a
  sample, and `mage examples generate` sends synthetic messages to its source.
- The Compile panel of a streaming pipeline shows how it runs on the engine (runs of one
  language, boundaries, sinks, batches, dead letters) and the problems that would stop it.
- The **Dead letters** tab reads a Kafka or NATS JetStream dead-letter sink with each
  message's error, opens the messages as a sample to reproduce the failure, and replays them
  to the source's topic or subject once fixed, after its name is typed.
- New streaming pipelines can start from a template per source (Kafka, RabbitMQ, NATS,
  webhook, PostgreSQL changes) with a transformer, a printing sink and a sample.
- While a stream runs on the local engine, the dependency graph shows each block's numbers.
- The Run tab's tap shows the last messages at the source or at any block of a running
  stream (development runs only: `MAGE_SERVICE_TAP=on`, `GET /v1/streams/<pipeline>/tap`).
- A stream service's `/v1/streams/<pipeline>` reports each block's messages in and out,
  batches, errors and median and 95th percentile time per batch; the Run tab shows them.
- The **Run** tab starts a streaming pipeline on the local engine with development positions
  and the sinks it writes named first; it shows state, rates, lag, totals and the engine's
  log, and pauses, resumes and stops the stream.
- Samples can be captured from NATS JetStream sources too, with an ephemeral consumer that
  acknowledges nothing.
- Captured samples redact secret fields: names such as `password`, `access_token` or
  `card_number`, and the fields a source lists in `secret_fields`.
- Run on sample works for Rust and R transformers too, and shows what the block changed:
  fields added, removed and changed, with how many messages each.
- Check connection on RabbitMQ and NATS JetStream sources reports the queue's or stream's
  messages and consumers, and where the NATS durable consumer stands.
- Check connection on a Kafka source reports partitions, retained messages, retention and the
  consumer group's lag, or what a group with no committed offsets would read first.
- A Samples tab captures the newest or first messages of a Kafka source without a consumer
  group (no position moves), or takes pasted ones, and Cmd+Enter runs a stream transformer and the blocks before it on a sample, showing input
  and output side by side, each block's time and output. Rust stream blocks run the same
  way, and in Execute pipeline.
- Tutorials: [Kafka to PostgreSQL](../guides/streaming/tutorials/kafka-to-postgres.md) and
  [PostgreSQL changes to Kafka](../guides/streaming/tutorials/postgres-changes-to-kafka.md), from
  the first sample to a service.
- The playground has three stream pipelines (`orders_stream`, `clickstream_stream`,
  `cdc_stream`): `make playground-streams` starts Kafka, builds the stream engine and sends
  synthetic traffic, so the Samples, Run and Dead letters tabs work on live data.

### Services

- `mage service deploy`, `status`, `diff` and `rollback` for IBM Cloud Code Engine through
  its API: content-named Secrets, a wait for the new revision with the app patched back on
  failure, and rollback from the previous deployment the app records.
- `mage service deploy`, `status`, `diff` and `rollback` for Kubernetes targets: images
  pushed by lock digest, Secrets named by content so a rollout undo restores the variables,
  rollout waits with automatic undo, and status from the pod template's annotations.
- `mage service deploy`, `status`, `diff` and `rollback` for Docker targets: labeled
  containers, health waits with automatic rollback, drift from the repository, and the
  previous deployment kept for rollback. A deploy with uncommitted changes to the service's
  inputs stops unless `--allow-uncommitted "<reason>"` records why.
- `mage service deploy --from staging` promotes the image another environment runs, without
  building; `mage service ci --promote prod=staging` does it in CI.
- `service.lock` records the columns each data contract of a service declares; a rebuild
  that would break their readers stops unless `--allow-breaking "<reason>"` records why.
- `mage service test` runs each stream sample of a service through the blocks a sink
  follows, as Run on sample does; `deploy` runs it first, and `check --tests` (in the
  pull request check of `mage service ci`) too.
- `mage service ci` writes a GitHub Actions workflow: pull requests check the services they
  change and their locks; rules deploy each environment on a branch or tag, building nothing
  when the deployment already runs the lock (`mage service deploy --if-changed`).
- A deploy keeps the running deployment's API token, so deploys from CI, which has no
  token file, no longer replace it.
- `mage service run --reload` starts the service again when a file it reads changes.
- `mage service call` runs a service's pipeline and prints each block's status and the result.
- `mage service runs` and `mage service logs` list a service's runs and print a run's log
  or the engine's own log, for a local run or a deployment.
- `mage service check --affected` checks the services that the changes since a Git ref touch
  (their pipelines, imported code, stream settings and `io_config.yaml`), for CI.
- `mage service run` runs a service on this machine's engine without Docker, with the
  environment's variables and `.env.<env>` secrets, a private API token and state kept across
  restarts. The engine ships as the `mageml-engine` platform wheel.
- `mage service new, list, check, build, export, migrate`; the pipeline settings page lists
  the services that run the pipeline.
- Secrets at start-up from AWS Secrets Manager and Parameter Store, Azure Key Vault, Google
  Secret Manager and IBM Cloud Secrets Manager; `aws_secret_var`, `azure_secret_var`,
  `gcp_secret_var` and `ibm_secret_var` in `io_config.yaml` export as those sources.

### IBM Cloud

- IBM Cloud Object Storage (IAM tokens or HMAC keys), MLflow artifacts on COS (`cos://`),
  and the standalone `mlflow-ibm-cos` package for MLflow servers.
- IBM Db2: io class, templates and SQL blocks (the `db2` extra), values exact.
- IBM MQ sources and sinks in streams; IBM Event Streams from its service credentials.

### Blocks

- `exclusive: true` in a block's configuration runs its stage alone in `mage run`, for blocks
  that change process-wide settings such as environment variables.
- Pipeline callbacks: a callback block attached to no block runs once when a run completes or
  fails, with its status and failed blocks (upstream #5438). `@callback('success', 'failure')`
  runs for both statuses; it ran for the first only, and stacked `@callback` decorators work.
- A block that returns a matplotlib figure, a PIL image or PNG or JPEG bytes shows the image
  in the editor (upstream #6073).
- A block can be disabled in its settings (`configuration.disabled`): runs skip it and its
  downstream blocks, as a false condition does (upstream #5470).
- R blocks read MageML secrets with `get_secret_value("name")`; only the secrets the code
  names are passed to R (upstream #4265).
- Block functions can be `async def`; they ran nothing and returned the coroutine
  (upstream #5164).

### Triggers

- The pipeline runs page deletes the selected runs that are not waiting or running
  (upstream #4516).
- The triggers page searches triggers by name or pipeline (upstream #5697).
- The pipeline runs page filters runs by time range (last hour, 24 hours, 7 or 30 days)
  (upstream #6062). `start_timestamp` and `end_timestamp` in the API are read as UTC; they
  were read as the server's local time.
- Cancelling a pipeline run cancels the runs its blocks started with `trigger_pipeline` that
  are still waiting or running; they ran on (upstream #5641).
- API triggers honor **Skip run if previous run still in progress**: a call while a run is
  queued or running answers with a cancelled run (upstream PR 5961).

### Observability

- `/metrics` (with `ENABLE_PROMETHEUS`) exposes pipeline and block runs by status, the last
  run's duration and the time of the last success, read from the database at each scrape
  (upstream #5196).

### Pipeline logs

- `logging_config.loggers` sends libraries' log records, such as urllib3 at DEBUG, to the log
  of the block that logs them (upstream #5766).
- A search box on the logs page, and `text` in the logs API, return only the log lines that
  contain the text, over up to 200 log files (upstream #6105).

### Security

- `column-atlas-native` 0.1.2 and the stream engine are built with rustls 0.23.46; 0.23.45
  had four advisories (RUSTSEC-2026-0346 to 0349). CI audits every Rust lock file with
  cargo-audit.

### Fixes

- Kafka streams: after a graceful restart of a one-broker cluster with a slow shutdown, a
  stream stood still for minutes or did not go on at all: the broker answered metadata
  with no brokers, librdkafka forgot the broker, and it does not bootstrap again without
  one. The source and the sink connect to `bootstrap_server` again after 30 seconds
  without an answer.
- The overview's run counts and charts refresh every minute, as its failed-run lists do;
  they changed only after a browser refresh or a change of period tab.
- `mage examples stream <name> <project>` failed, or wrote the block code into the
  current folder's project, for a project other than the current folder.
- A development run's files under `.mage/dev/` were not kept out of Git; the run adds the
  line to the project's `.gitignore`, as the service commands do.
- Kafka streams: a position committed while the broker was down was lost without a word,
  and a partition whose last batch it was kept its old committed offset while the stream
  ran (lag stayed up; a crash read the batch again). The engine commits the positions
  again every 5 seconds.
- Kafka streams: giving up partitions made a commit that librdkafka waits on without end,
  even for partitions the group had already taken away, so a coordinator that did not
  answer held the stream. Lost partitions are now given up at once, and a commit before
  giving partitions up, or at the stop, waits 10 seconds at most.
- Kafka streams: a partition paused when the group took it away came back paused and was
  never read again.
- Streams stood still after a broker outage long enough for the sink's writes to fail
  after the partitions went to another owner: the writer waited for a retry that never
  comes. Writes of dropped batches are now skipped.
- Stream errors (a source reconnecting, a block failing, a commit failing) go to the log,
  once and every 30 seconds while they repeat; only the status had them.
- Kafka streams: a paused partition's messages held back the other partitions' messages
  read after them.
- Kafka sources write librdkafka's warnings and errors to the service log; they went
  nowhere. `librdkafka: {debug: ...}` adds its debug lines.
- Restarted streams: sinks named objects and deduplication keys by position, and only
  Kafka keeps positions across a restart, so the object storage sink overwrote the last
  run's objects, and ClickHouse tokens or NATS message ids could drop new messages. Sinks
  now name the stream's run unless the source's positions survive restarts.
- Kafka backpressure stalled streams (48,000 to 412,000 messages a second after the fix);
  partitions are handed over only after their batches finish.
- The kernel restart, switch and interrupt no longer block the server: they run off its
  event loop with a time limit, and a replaced kernel client is closed.
- The first export on a fresh server is fast (secret clients made on first use).
- A page opened at `localhost` on a port other than 6789 talks to its own server. It sent
  every call to port 6789, so with two servers it edited the other project.
- Two sinks of one stream writing to the same place (a NATS stream, a ClickHouse table, a
  queue, a bucket) after different blocks made the same deduplication keys or object names,
  and the second sink's messages were dropped; each sink keys its messages by its own name.
- A NATS stream exports as a service with `connector_type: nats`, the name Mage runs; only
  `nats_js` was accepted.
- Exported services no longer require packages that a `.pth` file loads when Python starts
  (such as `ibm-db`, which has no Linux arm64 wheel and broke image builds there).
- `mage service check --affected` finds a new service whose folder is not yet in Git.
- Deploy API tokens in `.mage/deploy/` are kept out of Git.
- `mage service run` checks the box with the engine before serving, so a stale engine says
  to update itself, and in a source checkout it uses the newer of cargo's builds. It names a
  port in use before building anything.
- Building a service box takes half a second instead of 18 in a source checkout: copying
  MageML's Python no longer walks the Rust build folder and the frontend.
- SIGTERM or SIGHUP to `mage service run` stops the engine; it was left serving, holding the
  port and the run history.
- Object columns with missing values keep their values between blocks: ints came back as
  floats (integers past 2**53 lost digits, also in Rust blocks reading the file), and None
  as NaN or NaT, in a column of another dtype (upstream #5913).
- `FileIO.load(file_directories=...)` reads each file in its own format (it used the first
  file's), in name order, skips hidden and `_` files such as `_SUCCESS`, and concatenates
  Polars frames with Polars (upstream #6160).
- A dynamic block that returns no items creates no children. The scheduler waited 2 minutes
  for items, then ran one child without input, which failed the run (upstream #6045).
- API triggers accept a JSON list or scalar body, as webhooks such as HubSpot's send, as
  `kwargs['body']`; it failed with a 500 (upstream #5414). A body that is not JSON gets a 400
  with the reason, refused API requests no longer return the server's traceback, and the
  trigger token is compared in constant time.
- Single sign-on (OIDC, Active Directory, Google, GitHub Enterprise, GitLab, Bitbucket) and
  the update check use `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY`, and give up after 30
  seconds instead of 5 minutes (upstream #5748).
- The example pipeline writes `titanic_clean.csv` into the project instead of the folder the
  server started in (upstream #6137).
- Monitor statistics count only the project's runs (projects sharing a database saw each
  other's), block runs filtered by trigger are counted once instead of once per pipeline
  run, a block's runs in one pipeline are summed instead of showing 1, and the timing prints
  in the server log are gone (upstream #6036).
- A block output holding an object of the user's own class shows as the object's text; it
  raised "Circular reference detected" (upstream #5850). Storage still refuses values it
  cannot encode.
- MySQL and Snowflake SQL blocks with query preprocessing off run every statement of the
  block and return the last one's rows. MySQL failed with "Commands out of sync" and
  Snowflake with "Actual statement count did not match" (upstream #5695, #5696).
- An active schedule trigger saved with an empty cron stopped every scheduler pass, so no
  other trigger ran (upstream #5769). An empty cron is now no schedule, and a trigger that
  fails to schedule is logged while the others run.
- Callback blocks can be renamed in the editor; the new name was dropped (upstream #6110).
- A running block no longer holds a database transaction open: reads before the block
  left one open for the whole run, on PostgreSQL an idle-in-transaction connection holding
  locks (upstream #6030, #5408, #4167).
- With Redis, the job keys of a replica killed while jobs ran are deleted by the other
  schedulers within 5 minutes; they stayed in Redis for good (upstream #6102).
- Block timeouts apply to pipelines that run in one process: the block past its timeout
  fails and the run's process is stopped. They applied only to block-by-block runs
  (upstream #5133).
- ECS executor: a task that outlives the wait timeout is stopped instead of running on
  unseen (a retry ran the block twice), and a container that never started or a task ECS
  did not place reports ECS's reason (upstream #4655).
- Kubernetes executor: a block whose pod failed and was retried within the Job's
  `backoffLimit` no longer fails at the first failed pod, and a failed Job says why its pods
  stopped, such as `OOMKilled (exit code 137)` (upstream #6177, #6086).
- The InfluxDB stream sink writes each batch before the source acknowledges it: it batched
  in the background, so acknowledged messages could be lost and rejected writes were only
  logged. Messages without a time no longer overwrite each other, and the documented
  `metadata['time']` is read (only `timestamp` was).
- The server answers 5 seconds sooner after starting: it waited a fixed 5 seconds on its
  event loop for the scheduler process to make the database tables. It now migrates the
  database itself before starting the scheduler (0.01 s when already up to date).

## MageML 0.1.1 (2026-10-13)

A security release; the GitHub release text is in `releases/v0.1.1.md`.

### Security

- `/api/browser_items` read, wrote, moved and deleted any path the server could reach, for
  viewers too (reads) and editors (writes). Every path, move and rename targets included,
  must now be in the project, with symlinks resolved.
- `ensure_file_is_in_project` no longer skips the check when the project platform is on.
- The Kubernetes client's response logger stays at INFO, and `ApiException` prints its
  status, reason and the paths of rejected fields, never headers, bodies or values.
- An existing Git SSH private key is set to mode 600, and `add_host_to_known_hosts` returns
  False and adds nothing when `ssh-keyscan` fails.

### Fixes

- The play and pause button on the Pipelines page sends only the status; it wrote the
  defaults of block fusion, retries, environment, concurrency and other settings.
- The scheduler resets a crashed block run only while it still has the status it read; a
  block completed in between ran a second time.
- File browser renames: two debugging `raise` statements failed them, a rename by name
  moved the file into the working folder, and a file's saved versions now follow it.
- Integration CI groups start the services their tests use (Redis for the soak tests;
  MySQL, MinIO and the HTTP API for the R templates).

## MageML 0.1.0 (2026-10-13)

The first release under MageML's own versions, which start at 0.1.0. Earlier builds of this
fork were versioned after Mage (`0.9.79+ml.1` to `0.9.79+ml.4`); MageML is based on Mage
0.9.79. Changes since `0.9.79+ml.4`. MageML runs on Python 3.12 only (3.11 is no longer
supported; support for 3.14 is planned) with pandas 3.0, Polars 2.0,
pyarrow 26 and NumPy 2. Most changes fix values that Mage changed or lost without an
error; the per-service findings are in `docs/development/*-integration.md` and
`postgres-data-integrity.md`.

### Behavior changes to check before upgrading

Each item says what changed, which pipelines it affects, and what to do.

#### SQL blocks

- **Raw SQL statements are split by a lexer.** Semicolons inside dollar-quoted bodies
  (`DO $$ ... $$`, function definitions), `E''` strings, quoted names and comments no
  longer split a statement; such scripts failed with a syntax error.
- **Raw SQL that creates or fills a table passes all its rows downstream**, within
  `limit_in_pipeline_run`. It passed 1000 rows, so a downstream Python block silently
  worked on part of the table.
- **An empty upstream frame becomes an empty table.** It was skipped, and the SQL block
  read the rows an earlier run had left in the table.
- **Upstream frames keep their column names** in the tables SQL blocks read. They were
  cleaned (`MixedCase` became `mixedcase`, `with space` became `with_space`).
- **Append writes PostgreSQL columns by name.** `INSERT INTO table <query>` matched
  columns by position, so a query with another column order wrote values into the wrong
  columns. Values are cast to the column types (uuid and enum values read as text no
  longer fail).
- **PostgreSQL table names that need quotes work**, such as `with space`, and replacing a
  table configured as `MyTable` finds `mytable`.
- New `exact_numbers` setting for PostgreSQL SQL blocks (`numeric` values as `Decimal`)
  and DuckDB SQL blocks (exact decimals in pyarrow-backed columns).
- **Raw SQL recognizes `CREATE OR REPLACE TABLE`, `CREATE TEMP TABLE` and `CREATE TABLE
  ... AS` without parentheses** as creating a table, and passes that table on; it passed
  the statement's status.
- **MySQL and ClickHouse appends write columns by name** too, and an empty ClickHouse
  result keeps its columns (clickhouse-connect returned none).
- **DuckDB appends write columns by name** (`INSERT ... BY NAME`), and a DuckDB SQL block
  closes its connection when it fails, which kept the database file from other
  connections of the process.

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

- **The API source reads `.xlsx` files again**: `openpyxl`, which it came with through a
  removed package, is declared by mage-integrations.
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

- **PostgreSQL profiles without `POSTGRES_SCHEMA` use the connection's current schema.**
  A SQL block reading `{{ df_1 }}` with such a profile failed from its second run on: the
  upstream table was not found to replace, then not found by its name.
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

#### Privacy

- **Usage statistics are removed.** Mage sent events about projects, pipelines, blocks,
  runs and errors to the upstream Mage project (api.mage.ai) when `help_improve_mage` was
  on, the default for new projects. MageML has no such module: the setting, its toggle in
  Settings and its API field are gone, and a `help_improve_mage` key left in a project's
  metadata.yaml is ignored. The demo app's Google Analytics is removed too.
- **The docs no longer send visits to upstream Mage's Google Analytics**, and their
  header and footer link to MageML's repository instead of Mage's chat and social accounts.
- **No sign-in goes through Mage's servers.** GitHub sign-in in the Git settings and
  Active Directory sign-in used Mage's own OAuth applications, which sent the authorization
  code, and so the token, through api.mage.ai. Git sync works with a personal access token,
  an SSH key, or your own OAuth application (for GitHub, `GHE_HOSTNAME=https://github.com`,
  `GHE_CLIENT_ID` and `GHE_CLIENT_SECRET`); Active Directory needs your own Azure
  application (`ACTIVE_DIRECTORY_CLIENT_ID` and `ACTIVE_DIRECTORY_CLIENT_SECRET`). The
  sign-in screen with a MageML user and password was always local and is unchanged.
- **The update check reads MageML's GitHub releases** (`jospablo777/mage-ml`,
  `MAGE_UPDATE_CHECK_REPOSITORY`), not PyPI, where the name is not MageML's.
  `MAGE_UPDATE_CHECK_ENABLED=0` turns it off.

#### Security

- **Kernel outputs and terminals go only to signed-in WebSocket clients** when user
  authentication is on. Any connection got every user's block outputs, and any connection
  to the terminal socket started a shell.
- **The event, event matcher and block analysis routes need sign-in** when authentication
  is on.
- **Error messages no longer change after a failed sign-in.** The sign-in check edited
  Mage's shared "Record not found." error, so after one wrong password every later missing
  record in the process reported "Email/username and/or password invalid.". The shared
  errors are read-only now.

#### Production runs

- **`POST /api/runs/<token>` works.** It failed on every call under SQLAlchemy 2, and an
  unknown token answered with a database error instead of a 404.
- **A trigger's `envs` holds for after-run triggers**: a trigger set to `prod` no longer
  starts runs on a dev server.
- **In a fused, compiled chain of Rust or R blocks, the blocks after a failed block are
  cancelled**, as when blocks run one by one.
- **Production runs fuse chains of blocks by default.** Triggers, schedules, backfills, the
  API and `mage run` run each chain of blocks in one process; block by block is for the
  editor. Turn it off per pipeline with `block_fusion: 'off'` ("Run chains of blocks
  together" in the settings), or for the server with `MAGE_BLOCK_FUSION_DEFAULT=off`.
- **Consecutive Rust blocks compile into one program, and consecutive R blocks share one
  R session.** The blocks before the one whose output is needed store a plan instead of
  their output; reading it runs the chain. Pipelines that read an intermediate block's
  output from storage, outside Mage, should set `keep_block_outputs: true` ("Keep every
  block's output"). `MAGE_COMPILED_CHAINS=off` turns compiled chains off.

#### Runtime

- **The kernel cleanup no longer freezes the server** or stops kernels in use.
- **`.cbm` files load as the CatBoost class they were saved from**, so a classifier has
  `predict_proba`; the generic class returned raw scores.
- **`mage verify-fusion` runs again a block whose process died.**
- **pandas categoricals with missing values pass to Rust blocks.**
- **`HIDE_ENV_VAR_VALUES` can be turned off.**
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

- **The R environment is checked once** until `rproject.toml`, `rv.lock`, the library or
  the executables change; a package from a URL or git no longer costs 10 to 40 seconds on
  every R block run.
- **Processes an R block starts see the project's packages** (`parallel`, `future`,
  `callr` workers): the runner exports the library paths.
- **An R block's log names the columns R changes**: uint64 above 2^63 - 1 becomes text,
  and timestamps lose their nanoseconds.
- **Without an R environment, R's own library is checked** for the packages MageML
  exchanges data with (arrow, bit64, jsonlite, tibble), and a missing one is named with
  the command that installs it. New R environments include tidymodels.
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

- **Package management with uv and rv.** `pyproject.toml` is the single source of
  MageML's dependencies and `uv.lock` pins every package, so each install resolves the
  same versions; `setup.py`, `poetry.lock` and the hand-kept `requirements.txt` are gone
  (`make requirements` generates one from the lock when a tool needs it). Install with
  `uv pip install mage-ml`, develop with `uv sync`. Pipeline environments are virtual
  environments uv builds from a requirements file. R blocks use an rv project
  (`rproject.toml`, `rv.lock`) that `mage r init` creates, and rig installs the R version
  it names. See `docs/guides/blocks/r-setup.mdx` and
  `docs/guides/pipelines/pipeline-environments.mdx`.
- **Services in the app.** **Export as service** in the pipelines list and the editor's
  File menu exports one or more pipelines with a live preview and a progress bar. The
  **Services** page in the left menu keeps services in `pipeline_services.yaml`, checks
  each one as you edit (blocks, project files, packages, schedules, loops), lists the
  secrets and variables the container needs (every `io_config.yaml` variable is a secret;
  those of the profiles the blocks use are required), says how to keep run history, logs
  and metrics, and builds and pushes the image for `linux/amd64` and `linux/arm64` to
  Docker Hub, IBM Cloud Container Registry, GHCR, Amazon ECR, Google Artifact Registry,
  Azure Container Registry or a local registry. The export is a folder you can keep as a
  repository, with a GitHub Actions workflow that builds and pushes it. The CLI does the
  same: `mage export service . --service <name> --platform linux/amd64,linux/arm64 --push`.
- **After-run triggers.** A pipeline starts when a run of another pipeline completes,
  fails or ends either way, in MageML and in exported services. The run gets
  `upstream_pipeline`, `upstream_pipeline_run_id` and `upstream_status` as variables;
  loops are refused when the trigger is saved and at export.
- **Models in exported services.** A service names its models and the MLflow server they
  come from: a registered model by alias (`champion`) or version, or any URI MLflow can
  download. The Services page resolves each alias while you edit, with the version, its
  tags and its Feast feature service; the export embeds each model and reports the version
  it embedded or why it could not. `load_model(name, flavor='native')` returns the
  framework's own model (a CatBoost classifier with `predict_proba`), model files logged as
  they are, such as `.cbm`, load in their own format, and `feature_service(name)` reads the
  `feast.feature_service` tag of Feast's MLflow integration. Registry errors name what is
  wrong (a wrong name, a missing alias or version, refused credentials) and an unreachable
  registry is reported in seconds instead of minutes.
- **Conditional blocks in exported services.** A block whose Python conditions return
  False is marked `condition_failed`, so are the blocks after it, and the run completes,
  as in MageML. The Services page lists the blocks guarded by conditions.
- **Exports find more of what blocks read**: SQL files a helper reads
  (`read_sql('sql/orders.sql')`), `FEAST_URL` for `FeastClient`, packages MageML's modules
  for `.cbm`). MLflow goes into an image only when a model needs it, and the export's zip
  streams its files.
- **R and Rust setup is explained in the editor.** Creating an R or Rust block on a machine
  where that language is not set up shows what is missing, the commands that fix it and
  the setup guide; the block is still created. `GET /api/language_setups/<r|rust>` does the
  check. A `rust` extra joins the `r` extra; both name the setup and install no packages.
- **Docker build and push failures say why**: Docker not running, no disk space, a builder
  that builds one platform, a step for another processor without QEMU, a base image that
  cannot be pulled, a registry that refuses the push or cannot be reached.
- **Pipeline shapes.** A guide to the seven shapes of a pipeline (Line, Parallel lines,
  Diamond, Broadcast, Merge, Lattice, Thicket), what fuses in each, best practices and
  anti-patterns, with diagrams; the compilation study behind it
  (`docs/development/compilation-study.md`); and Diamond, Broadcast and Merge pipelines in
  the playground.
- **Clearer errors.** A 404 names what was not found ("Pipeline run 42 was not found.")
  instead of "Record not found.", requests in the app that showed nothing when they failed
  show the error, and invalid cron expressions, missing Google Sheets and blocks stopped by
  a monitor say what to do.
- **Faster Rust pipelines.** A cached Rust block's process starts in about 10 ms: Mage
  no longer hashes the SDK before every block, no longer makes macOS check each binary
  again, and adding a block no longer rebuilds the others. Every job process imports
  3.5 s less (dbt, scikit-learn and others load where used). The 5-block benchmark chain
  runs block by block in about 11 s instead of 20 s. `MAGE_RUST_TIMINGS=1` also prints
  the phases inside a block. See `docs/development/rust-pipeline-optimization.md`.
- **Exported services build their Rust blocks into one binary.** Each block's binary held
  its own copy of Polars (about 75 MB): five Rust blocks were 361 MB of the image, now
  73 MB, and a run loads the code once.
- **Rust blocks read and write PostgreSQL.** `mage::postgres` streams binary `COPY` both
  ways with typed columns, keeps `numeric` values exact, handles nested arrays, and
  connects with the block's io_config profile. TLS modes are libpq's (`prefer` by
  default, `require`, `verify-ca`, `verify-full`, with a root certificate file for
  private CAs). Appends cast by column name; a write
  is one transaction. New PostgreSQL loader and exporter templates. Exported services
  pass the profile to Rust blocks, with variables and secrets filled in at run time.
- **Monitoring block outputs.** A monitor (`monitors/<name>.yaml`) watches a block's
  output across pipeline runs against a baseline run. It checks drift of each column
  (Jensen–Shannon divergence over the baseline's bins or frequent values), missing
  values, row counts and the freshness of a timestamp column. The whole output is
  profiled in Rust. Results show on the run page and in `mage monitor-status`, and a
  failed monitor sends the pipeline's notifications or fails the block run. Monitors
  also check model performance when the output holds labels (accuracy, F1, ROC AUC,
  RMSE and others, against bounds and the baseline), offer the population stability
  index, per-column thresholds, rolling baselines and notifications on change only.
  Evidently and NannyML can run as engines of a monitor (presets with their tests;
  label-free performance estimation and multivariate drift), in an environment Mage
  builds for them, with their HTML reports on the run page. See
  `docs/guides/pipelines/monitoring.mdx`.
- **Resources and shared limits.** A block can declare the memory, CPUs, GPUs and shared
  limits it needs (block settings > Resources), and the project declares what exists
  (`resources` in its metadata.yaml: limits such as `warehouse: 4`, a memory budget and
  GPU devices). A triggered block run starts only when what it needs is free. A waiting
  run shows why on the pipeline run's page, a request that can never fit fails at once,
  and a run gets its own GPU devices and thread counts. See
  `docs/guides/pipelines/resources.mdx`.
- **Model release checks.** A policy in `releases/<model>.yaml` lists metric rules (min,
  max, at most this much worse than the champion). Each version a block registers in
  MLflow is checked against it and decided pass, hold or fail. A passing version is
  promoted by moving an MLflow alias (`champion`), at once or after **Approve promotion**
  on the run page. A stale approval is refused, and every promotion and rollback is
  logged. CLI: `mage release-status`, `release-promote`, `release-rollback`. See
  `docs/guides/pipelines/model-releases.mdx`.
- **MLflow runs linked to Mage runs.** MLflow runs that a block starts during a pipeline
  run are tagged with the project, pipeline, pipeline run, block, block run and code
  digest (`mage.*` tags), and the block run records them: experiment, status, latest
  metrics and registered model versions. The run page lists them with a link to MLflow,
  and comparing two runs shows which metrics changed. Blocks use MLflow as before;
  `MAGE_TRACK_EXPERIMENTS=0` turns it off. See `docs/guides/pipelines/experiments.mdx`.
- **Faster pandas outputs.** pandas block outputs whose columns are numbers, booleans,
  strings, timestamps, dates or decimals are written by the Polars Parquet writer, which
  encodes columns in parallel: a 2-million-row output writes in 0.03 s instead of 0.16 s,
  and the block run takes a third of the time. The files are zstd-compressed (30%
  smaller) and read back as the same types and values; frames with other column types
  (categoricals, lists) are written by pyarrow as before. See
  `docs/development/efficient-data-exchange.md`.
- **Run records and reproducing runs.** Every pipeline run records its code (each source
  file's SHA-256, with one copy kept per distinct set of files), Python, Mage and package
  versions, variable names and git commit; each block run records its outputs' SHA-256.
  A run's page shows the record, compares it with another run (changed files with diffs,
  package versions, variables, which outputs differ), and **Reproduce** runs the pipeline
  again with the run's own code, variables and execution date, then compares every
  block's outputs. CLI: `mage run-diff` and `mage reproduce`. `MAGE_RUN_RECORDS=0` turns
  recording off. See `docs/guides/pipelines/run-records.mdx`.
- **Data contracts.** A YAML file in the project's `contracts` folder declares what a
  block's output must look like: columns, types, missing values, ranges, allowed values,
  patterns, unique keys and row counts. Choose it in the block's settings, and every run
  checks the whole stored output with the block's tests, in a Rust engine (10 million rows
  in about 0.3 s). A broken contract fails the block (or only warns), so downstream
  blocks and exporters don't run, and the output lists each broken rule with exact counts,
  example rows and values. **Draft a contract from the output** in block settings, or
  `mage contract-draft`, writes one from a stored output; `mage contract-check` checks a
  stored output in CI. See `docs/guides/blocks/data-contracts.mdx`.
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
  table. Printed text and errors show in full, as before. `MAGE_COLUMN_ATLAS=0` turns it off. See `docs/guides/blocks/column-atlas.mdx` and
  `column-atlas.md`. Building Mage from source now needs Rust (`rustup`); the Docker
  image has it in the build stage only. Hovering a mini chart shows its numbers at once:
  a histogram bar its range, row count and share; a category its name, count and share;
  the completeness bar and missing marks their counts. All are computed over the whole
  output.
- **Block fusion** (on by default; "Run chains of blocks together" in the pipeline
  settings): blocks that form a chain run as one stage, in one process, and each
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
  `load_model('churn', uri='models:/churn@champion')` run the same code in Mage, where the
  model comes from MLflow, and in the service, where the export has embedded the model with
  its version, run, params, metrics and requirements (`GET /v1/models`). Guide:
  `docs/production/pipeline-services.mdx`; design: `docs/design/pipeline-services.md`.
- **Faster block imports**: `mage_ai.io.postgres` no longer imports SciPy, paramiko and
  sshtunnel unless an SSH tunnel or a sparse matrix is used (1.1 s to 0.7 s), and
  `io_config.yaml` loads secrets support only when it reads a secret.
- **Pipeline compilation**: MageML checks Python and R blocks, prepares the R environment
  and builds the Rust chains and blocks ahead of a pipeline's runs: after a save, when a
  trigger is turned on, at server start for pipelines with active triggers, and from the
  **Compile** button in the editor and the pipelines list, with progress, each step's error
  and when the pipeline last compiled. The panel shows where a pipeline switches language,
  since single-language pipelines run faster. API: `/api/pipeline_compilations`.
- **Diagrams and a tutorial in the docs**: diagrams with the logos of the languages and
  tools (compiled chains, Rust and R blocks, pipeline services, Feast), drawn by
  `scripts/docs/diagrams.py` in light and dark, and the tutorial "A pipeline in Python,
  Rust and R" (`docs/getting-started/polyglot-pipeline.mdx`).
- **The documentation on GitHub**: `docs-md/` holds every page as Markdown GitHub renders,
  with images and light and dark diagrams, generated from `docs/` by
  `scripts/docs/export_markdown.py`; a test fails while the two differ.
- **Pipeline documentation**: each pipeline can have a README in Markdown (`README.md`)
  or Quarto (`README.qmd`) in its folder, with images beside it. Open it from the document
  button on each row of the pipelines list or from **Documentation** in the pipeline's
  navigation; edit it beside a live preview. Quarto documents render with the quarto
  command when installed, without running code. Guide:
  `docs/guides/pipelines/documentation.mdx`.
- **Feast client for blocks** (`mage_ai.feature_stores.feast.FeastClient`): reads online
  features into pandas, Polars or Arrow with integers exact, and pushes, writes and
  materializes through a stock Feast feature server's REST API. It sends large integers
  next to NULLs so Feast keeps them exact, writes rows in event-time order so the latest
  event wins, refuses timestamps Feast would misread, and retries only transient failures.
  Rust blocks have the same client in `mage::feast` (the SDK's `feast` feature).
  Guide: `docs/guides/blocks/feast-features.mdx`.
- **MageML branding**: the header, sign-in, 404 page and browser tab show MageML with
  its mark and favicon, and product copy says MageML. Routes, settings, identifiers and the
  `mage_ai` package keep their names. The header shows `MageML <version>`, linked to the
  releases, with the Mage version it is based on.
- **The Mage Pro promotion is removed**: Live help and Try Pro in the header, Deploy (Pro)
  in the side navigation, AI Code (Pro) in the add-block menu and the live chat links.
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

### Removed

- **Python 3.11.** MageML runs on Python 3.12 only; support for 3.14 is planned.
- **Mage Pro promotion** in the app and the docs, and **74 docs pages** no page linked:
  Mage Pro, Mage Agent and Pro architecture pages, migrations to Mage Pro, the Pro API and
  Mage's community pages.
- **Unused code**: 17 Python modules nothing imported (among them an unused copy of the
  pipeline scheduler, a websockets package, the Spark SQL block module, a client that
  synced pipelines to backend.mage.ai) and 103 frontend modules no page reached, about
  14,600 lines; and legacy files: `example.ipynb`, the GitBook `SUMMARY.md`, the
  `mage_dev` sample project, the `sample_datasets` module and obsolete scripts.
- **AI assistant features**: the OpenAI and Hugging Face clients, pipelines and code
  generated with AI, AI documentation and comments for blocks, the project's OpenAI key
  and the `ai` extra. MageML has no AI integration; RAG pipelines stay. **Mage Pro docs**: 36 pages
  about features of Mage Pro.

### CI

- A Rust job runs `cargo fmt`, clippy and the engine tests; the frontend unit tests run
  with `yarn test:unit`. The publish workflow builds the extension's wheels for
  manylinux, macOS and Windows.
- The Docker image workflow builds the image and pushes it nowhere. It used to log in
  to the GitHub container registry and push when run by hand.

### Upstream issues and workarounds

- **pyarrow 25 and older cannot read a Parquet fixed-size list column that holds a null**
  ([apache/arrow#35692](https://github.com/apache/arrow/issues/35692)). MageML read such
  files through Polars; it now requires pyarrow 26, which fixed the reader.
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
