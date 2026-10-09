# Integration tests

Tests that run Mage against real services. The unit tests under `mage_ai/tests` mock the
drivers; these tests check what reaches the database and what comes back.

## Running

Requirements: Docker with Compose v2, and the project environment with the `postgres`,
`mlflow`, `duckdb` and `s3` extras
(`uv sync --group dev --extra postgres --extra mlflow --extra duckdb --extra s3`).

```bash
make -C integration_tests test             # start the services, run every test
make -C integration_tests test-postgres    # PostgreSQL only
make -C integration_tests test-redis       # Redis only
make -C integration_tests test-api         # REST API only
make -C integration_tests test-feast       # Feast only
make -C integration_tests test-mlflow      # MLflow only
make -C integration_tests test-duckdb      # DuckDB only, no service needed
make -C integration_tests test-s3          # S3 (MinIO) only
make -C integration_tests test PYTEST_ARGS='-n 4 -k conflicts'
make -C integration_tests down             # stop the services and drop their data
```

The services stay up between runs. PostgreSQL and MinIO keep their data in memory, so `down`
leaves nothing behind. Ports default to 15432, 16379, 18000, 16566, 15000 and 19000; set `MAGE_TEST_POSTGRES_PORT`,
`MAGE_TEST_REDIS_PORT`, `MAGE_TEST_API_PORT`, `MAGE_TEST_FEAST_PORT`,
`MAGE_TEST_MLFLOW_PORT` or `MAGE_TEST_S3_PORT` to change them. `make up` rebuilds the
service images when their files change. To use another interpreter, pass `PYTHON`, for
example `PYTHON=.venv/bin/python`.

Without the `MAGE_TEST_*` variables the tests skip, so `pytest integration_tests` in a
plain checkout passes without Docker. `make ci` runs the same suite and fails when any
test skips. CI runs `make ci` in the `integration` job of `build_and_test.yml`.

## Layout

| Path | Contents |
| --- | --- |
| `compose.yaml` | PostgreSQL 16, Redis 7, the test API, Feast, MLflow and MinIO, with health checks |
| `conftest.py` | Connection settings and fixtures. Every test gets its own PostgreSQL schema, dropped afterwards |
| `data/postgres_dataset.py` | Source table with 32 column types: hand-written edge rows plus seeded Faker rows in 8 locales. Also the SQL comparison used by every test |
| `postgres/` | Load, export, duplicate handling, column names, values, round trips and pipelines |
| `redis/` | The scheduler's distributed lock |
| `services/api/` | A FastAPI service that serves and accepts data frames as JSON, NDJSON, CSV, Parquet and Arrow, exchanges images, and returns every kind of failure |
| `api/` | Pulling and pushing frames with pandas and Polars, failure handling, images, and Mage pipelines that call the API |
| `services/feast/` | Feast 0.66 as a microservice, with PostgreSQL as registry, offline store and online store. Feast requires pandas below 3, so it runs in its own container. Adds `/get-historical-features` and `/registry` to Feast's feature server |
| `feast/` | Online reads, pushes, writes, materialization and point-in-time retrieval, and Mage pipelines that pull features and write them back |
| `services/mlflow/` | MLflow 3.17 tracking server with PostgreSQL as backend store and proxied artifacts, seeded with an experiment, two runs, artifacts of several types, and a model registered as a cloudpickle version and a skops version with aliases |
| `mlflow/` | Experiments, runs, metric histories, registry and aliases, artifact listings and downloads over HTTP and with the client, models loaded both ways, and a Mage pipeline that logs predictions back |
| `data/duckdb_dataset.py` | DuckDB source table with one column per DuckDB type, limits and special values, and the SQL comparison for DuckDB |
| `duckdb/` | Loads in each mode, exports to new and existing tables, conflicts, names, database files and locking, reading Parquet, CSV and JSON, and Mage pipelines with Python and SQL blocks |
| `services/minio/` | MinIO RELEASE.2025-09-07T16-13-09Z built from source, since MinIO no longer publishes images |
| `data/s3_dataset.py` | Polars frame with one column per type Parquet stores, limits and special values, and a frame comparison |
| `s3/` | Mage's S3 client in each format with pandas, pyarrow-backed pandas and Polars, block output storage on S3, a pipeline whose block outputs live in S3, and the S3 source and destination of `mage_integrations` |
| `mage_runner.py` | Runs the pipelines in `project/` through Mage's trigger, scheduler and executor |
| `project/` | Mage project with the pipelines the `postgres/`, `api/`, `feast/`, `mlflow/`, `duckdb/` and `s3/` tests run |

## How tables are compared

`postgres_dataset.mismatches` compares two tables row by row in SQL with
`IS DISTINCT FROM`, after casting the target column to the source type. A failure names
each column with differences and shows up to five rows, with both values. Comparing in
the database avoids a second conversion through Python, which could hide the same loss
the test looks for. `test_comparison_reports_a_change_in_every_column` checks that the
comparison detects a change in every column type.

## Adding a test

Use the `schema` fixture for tables, and `source_table` for the shared dataset. When a
test finds a bug, fix it in Mage and keep the test. Record the bug in
`docs/development/postgres-data-integrity.md`.
