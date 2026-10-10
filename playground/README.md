# Mage playground

Mage built from this repository, a PostgreSQL database with a seeded shop, and a project
(`shop_lab`) with pipelines that use pandas, Polars, R (dplyr, tidyr, lubridate, tibble),
Python and R together, and geopandas. Exporters write to the same database, in the
`analytics` schema.

## Start

From the repository root:

```bash
make playground
```

It builds the images, starts everything and waits until Mage answers at
<http://localhost:6789>. The first build compiles Mage's Rust extension and installs the
R packages; on arm64 (Apple silicon) R packages without a binary compile from source,
Arrow's C++ library among them, which takes a while once. The compiled packages stay in
a build cache, so later builds, also after changes to Mage, reuse them.

`seed` fills the database on the first start (about 1.5 million rows, a minute or two)
and does nothing on later starts.

Sign in with `admin@admin.com` and password `admin`; user authentication is on, as in a
deployment.

| Command | |
| --- | --- |
| `make playground` | Build, start and wait for Mage |
| `make playground-check` | Run every pipeline once and list the exported tables |
| `make playground-logs` | Follow the logs of the seed and Mage |
| `make playground-ps` | State of each service |
| `make playground-psql` | psql on the playground database |
| `make playground-stop`, `make playground-start` | Stop and start again, keeping everything |
| `make playground-restart` | Restart Mage only |
| `make playground-down` | Remove the containers; the database and Mage's data stay |
| `make playground-reset` | Delete the database and Mage's data (asks first) |
| `make playground-reseed` | Fill the shop schema again |

The targets wrap `docker compose -f playground/compose.yaml`; use it directly for
anything else.

| Setting | Default | |
| --- | --- | --- |
| `PLAYGROUND_MAGE_PORT` | `6789` | Mage |
| `PLAYGROUND_POSTGRES_PORT` | `55432` | PostgreSQL on the host (user `mage`, password `mage`, database `playground`) |
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
| `polars_customer_360` | Polars on a million events; block fusion (`block_fusion: chains`): the loader, sessions and engagement blocks run as one stage |
| `r_dplyr_cohorts` | R blocks: `read_sql`, dplyr, lubridate, tidyr `pivot_wider`, tibble, `write_table`, `#* @test` |
| `polyglot_churn` | pandas loads, R scores customers with dplyr, Polars summarizes, Python exports the R output |
| `geo_store_coverage` | geopandas: GeoDataFrames pass between blocks with their CRS; nearest store with `sjoin_nearest` |
| `explore_web_events` | A million-row output and a wide one (about 60 columns), to explore |
| `rust_session_scores` | Rust blocks: sessions from a million events with Polars expressions, then a per-row engagement score on every core with Rayon; block tests in Rust |

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
- **Types**: decimals, zoned timestamps, nullable integers, uuid, jsonb, text[] and NaN
  are all in the data; check them in the outputs and in the exported tables.

## Reset

`make playground-reset` deletes the database and Mage's data; the next
`make playground` seeds again. `make playground-reseed` refills only the shop schema.
`SEED_SCALE` applies when the database is seeded: `make playground-reseed SEED_SCALE=0.1`.

The project files are in `shop_lab/` on the host: blocks you edit in Mage are saved
there.

## Troubleshooting

- **The build runs out of disk space** (`not enough free space` from apt): the image
  needs about 15 GB of build space. Free Docker build cache with `docker builder prune`.
  `make playground` builds with the builder of the current Docker context, the one
  `docker build` uses; `PLAYGROUND_BUILDER=<name>` picks another.
- **Port in use**: `make playground PLAYGROUND_MAGE_PORT=6800` (or
  `PLAYGROUND_POSTGRES_PORT`).
