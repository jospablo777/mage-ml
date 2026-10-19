# MageML playground

MageML built from this repository, a PostgreSQL database with a seeded shop, and a project
(`shop_lab`) with pipelines that use pandas, Polars, R (dplyr, tidyr, lubridate, tibble),
Python and R together, and geopandas. Exporters write to the same database, in the
`analytics` schema.

## Start

From the repository root:

```bash
make playground
```

It builds the images, starts everything and waits until MageML answers at
<http://localhost:6789>. The first build compiles MageML's Rust extension and installs the
R packages; on arm64 (Apple silicon) R packages without a binary compile from source,
Arrow's C++ library among them, which takes a while once. The compiled packages stay in
a build cache, so later builds, also after changes to MageML, reuse them.

`seed` fills the database on the first start (about 1.5 million rows, a minute or two)
and does nothing on later starts.

Sign in with `admin@admin.com` and password `admin`; user authentication is on, as in a
deployment.

| Command | |
| --- | --- |
| `make playground` | Build, start and wait for MageML |
| `make playground-check` | Run every pipeline once and list the exported tables |
| `make playground-logs` | Follow the logs of the seed and MageML |
| `make playground-ps` | State of each service |
| `make playground-psql` | psql on the playground database |
| `make playground-stop`, `make playground-start` | Stop and start again, keeping everything |
| `make playground-restart` | Restart MageML only |
| `make playground-down` | Remove the containers; the database and MageML's data stay |
| `make playground-reset` | Delete the database and MageML's data (asks first) |
| `make playground-reseed` | Fill the shop schema again |
| `make playground-service` | Export the live pipelines as a service and start it |
| `make playground-console` | The service's terminal dashboard |
| `make playground-service-stop` | Stop the service |
| `make playground-streams` | Start Kafka, the stream engine and synthetic traffic for the stream pipelines |
| `make playground-streams-stop` | Stop Kafka and the traffic |

The targets wrap `docker compose -f playground/compose.yaml`; use it directly for
anything else.

| Setting | Default | |
| --- | --- | --- |
| `PLAYGROUND_MAGE_PORT` | `6789` | MageML |
| `PLAYGROUND_POSTGRES_PORT` | `55432` | PostgreSQL on the host (user `mage`, password `mage`, database `playground`) |
| `PLAYGROUND_KAFKA_PORT` | `59092` | Kafka on the host, with `make playground-streams` |
| `SEED_SCALE` | `1` | Multiplies the row counts; `0.1` for a quick start |

## The data

Schema `shop`:

| Table | Rows | |
| --- | --- | --- |
| `customers` | 50,000 | Names, emails, cities in Costa Rica and abroad, sign-up times, lifetime value (numeric), locations, some nulls |
| `products` | 2,000 | Prices and costs (numeric), tags (text[]), attributes (jsonb) |
| `stores` | 120 | Locations |
| `orders` | 400,000 | Status, channel, discount, shipping cost, delivery date, rating (often null) |
| `order_items` | about 1.2 million | Quantity and price of each line |
| `web_events` | 1,000,000 | Sessions (uuid), event types, pages, devices, durations, scroll depth with NaN, properties (jsonb) |

The values are random but the same on every seed.

## Pipelines

| Pipeline | Shows |
| --- | --- |
| `pandas_sales` | pandas with exact PostgreSQL types; two branches that run in parallel; block tests |
| `polars_customer_360` | Polars on a million events; the loader, sessions and engagement blocks run as one compiled chain |
| `r_dplyr_cohorts` | R blocks: `read_sql`, dplyr, lubridate, tidyr `pivot_wider`, tibble, `write_table`, `#* @test` |
| `polyglot_churn` | pandas loads, R scores customers with dplyr, Polars summarizes, Python exports the R output |
| `geo_store_coverage` | geopandas: GeoDataFrames pass between blocks with their CRS; nearest store with `sjoin_nearest` |
| `explore_web_events` | A million-row output and a wide one (about 60 columns), to explore |
| `rust_session_scores` | Rust blocks: sessions from a million events with Polars expressions, then a per-row engagement score on every core with Rayon; block tests in Rust |
| `rust_postgres_margins` | All Rust: reads 1.2 million order lines from PostgreSQL with binary COPY, computes exact decimal margins per day and category and customer value tiers in two parallel branches, writes both back with binary COPY |
| `shape_diamond_store_scorecard` | A Diamond: pandas loads order lines, Polars and R compute revenue and margin per store side by side, Rust joins the branches; in `analytics.store_scorecard` |
| `shape_broadcast_web_events` | A Broadcast: a million web events read once feed Polars, R and Rust summaries that run side by side |
| `shape_merge_customer_signals` | A Merge: customers (pandas), order stats (Rust, binary COPY) and web stats (Polars) load side by side and one Rust block joins them; in `analytics.customer_signals` |
| `live_traffic` | New web events every minute, more or fewer as the hour goes on, appended to `analytics.live_events`; keeps six hours |
| `live_minute_kpis` | Polars: events, purchases, conversion and revenue per minute of the last hour, in `analytics.live_minute_kpis`; starts after each `live_traffic` run |
| `live_device_revenue` | All Rust: revenue per device over the last 15 minutes, appended to `analytics.live_device_revenue` after each `live_traffic` run |
| `orders_stream` | Streaming: Kafka `orders` parsed and validated into `orders_clean`; a broken order (about one in fifty) goes to `orders_dead_letters` with its error |
| `clickstream_stream` | Streaming: Kafka `clicks` with session ids upserted into `public.click_events` by `event_id`, so a replayed batch writes no row twice |
| `cdc_stream` | Streaming: changes of `public.shop_orders` by logical replication, as events in Kafka `order_changes` |

The `shape_` pipelines are the topology families of the
[compilation study](../docs/development/compilation-study.md): which blocks fuse into one
stage and which run side by side follows from the shape.

Run a pipeline from its page (**Run @once**), or open it in the editor and run blocks one
by one. Exported tables are in `analytics`:

```bash
docker compose exec postgres psql -U mage -d playground -c '\dt analytics.*'
```

`make playground-check` runs every pipeline once in the container (`mage run`) and lists
the exported tables with their row counts.

## What to look at

- **ColumnAtlas**: run a block in the editor; its output opens in the explorer. Header
  profiles, sort (click a column name, Shift-click for more keys), filters, **Expand**
  for the full window and per-column summaries. `explore_web_events` has a million
  rows; scroll to the end, filter `event_type = purchase`, sort by `duration_ms`.
- **Block fusion**: run `polars_customer_360` from a trigger; each block keeps its own
  block run, logs and output, and the chain runs in one process.
- **R and polyglot**: in `polyglot_churn`, the R block receives the pandas frame as a
  tibble and its factor reaches Polars as a category.
- **Rust**: open `rust_session_scores` in the editor. A compile error is marked in the
  editor as you type and shown in the output at the block's line. The first run of a
  block compiles it (a few seconds, the image has the dependencies built); later runs of
  unchanged code start at once. The Cargo workspace is `shop_lab/rust`.
  `rust_postgres_margins` has no Python at all: `mage::postgres` reads and writes the
  database with the `default` profile of `io_config.yaml`, and the exported decimals
  match PostgreSQL's own sums to the cent.
- **Types**: decimals, zoned timestamps, nullable integers, uuid, jsonb, text[] and NaN
  are all in the data; check them in the outputs and in the exported tables.
- **A live service**: `make playground-service` exports the service `shop-live`, saved in
  `pipeline_services.yaml` (open **Services** in the left menu to see it and its checks), and
  starts it next to the playground. `live_traffic` runs every minute, and each of its runs
  starts `live_minute_kpis` and `live_device_revenue` (after-run triggers), so the
  `analytics` tables grow and the dashboard moves. `make playground-console` opens the
  service's terminal dashboard (`q` quits), and its HTTP API is at http://localhost:8081
  with the token `playground`. The minute trigger carries `envs: [prod]`, so MageML's own
  scheduler, which runs as dev, leaves it to the service. `make playground-service-stop`
  stops it.
- **Pipeline services**: export a pipeline as a Docker service that runs without MageML,
  and run it once against the playground database. From the repository root:

  ```bash
  mage export service playground/shop_lab rust_session_scores --out /tmp/rust-scores
  docker build -t rust-scores /tmp/rust-scores
  docker run --rm --network mage-playground_default \
    -e PGHOST=postgres -e PGPORT=5432 -e PGUSER=mage -e PGPASSWORD=mage \
    -e PGDATABASE=playground rust-scores run rust_session_scores
  ```

  It writes `analytics.rust_session_scores`, the same rows as the pipeline run in MageML.
  Without `run`, the service starts its HTTP API; see
  [Pipeline services](../docs-md/production/pipeline-services.md).

## Streams

The three stream pipelines need a broker, the stream engine and messages:

```bash
make playground-streams
```

It starts Kafka, builds the engine from this repository the first time (several minutes:
Rust, librdkafka and OpenSSL compile from source; later builds reuse a cache), and starts
`traffic`, which sends clicks and orders to Kafka (10 a second each) and inserts order
rows into `public.shop_orders` (1 a second) for as long as it runs. MageML restarts with
`KAFKA_BOOTSTRAP` and the engine set.

Open `orders_stream` in the editor:

- **Samples** (right panel): the `example` sample came with the pipeline; **Capture**
  reads the newest messages of `orders` without joining its consumer group.
- **Run on sample** (play button on `parse_orders`): the block runs on the sample at once,
  with its output and the difference from its input.
- **Compile** (header): how the stream runs on the engine, its sinks and dead letters.
- **Run** tab: starts the stream on the engine with a development consumer group
  (`orders-clean-dev-<user>`), so production's position never moves; it asks before writing
  to the real sinks. It shows throughput, lag and per-block numbers, and the graph shows
  live counts on each block.
- **Dead letters** tab: the broken orders with their errors; open them as a sample to see
  the failure with Run on sample, fix the block, and replay them to `orders`.

`clickstream_stream` writes to the playground database (`select * from click_events`);
`cdc_stream` reads with its own replication slot: a development run uses
`shop_orders_cdc_dev_<user>` (`shop_orders_cdc_dev_admin` for the default user). Drop it
when done, since a slot holds WAL: `make playground-psql`, then
`select pg_drop_replication_slot('shop_orders_cdc_dev_admin');`.

Each one exports as a service as it is: `mage service new orders-svc --pipelines
orders_stream`, then `mage service test orders-svc` runs its sample through it. The
[stream examples](../docs-md/guides/streaming/tutorials/stream-examples.md) list the
others (`mage examples streams`).

## Reset

`make playground-reset` deletes the database and MageML's data; the next
`make playground` seeds again. `make playground-reseed` refills only the shop schema.
`SEED_SCALE` applies when the database is seeded: `make playground-reseed SEED_SCALE=0.1`.

The project files are in `shop_lab/` on the host: blocks you edit in MageML are saved
there.

## Troubleshooting

- **The build runs out of disk space** (`not enough free space` from apt): the image
  needs about 15 GB of build space. Free Docker build cache with `docker builder prune`.
  `make playground` builds with the builder of the current Docker context, the one
  `docker build` uses; `PLAYGROUND_BUILDER=<name>` picks another.
- **Port in use**: `make playground PLAYGROUND_MAGE_PORT=6800` (or
  `PLAYGROUND_POSTGRES_PORT`).
