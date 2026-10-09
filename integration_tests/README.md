# Integration tests

Tests that run Mage against real services. The unit tests under `mage_ai/tests` mock the
drivers; these tests check what reaches the database and what comes back.

## Running

Requirements: Docker with Compose v2, and the project environment with the `postgres`
extra (`uv sync --group dev --extra postgres`).

```bash
make -C integration_tests test             # start the services, run every test
make -C integration_tests test-postgres    # PostgreSQL only
make -C integration_tests test-redis       # Redis only
make -C integration_tests test-api         # REST API only
make -C integration_tests test PYTEST_ARGS='-n 4 -k conflicts'
make -C integration_tests down             # stop the services and drop their data
```

The services stay up between runs. PostgreSQL keeps its data in memory, so `down` leaves
nothing behind. Ports default to 15432, 16379 and 18000; set `MAGE_TEST_POSTGRES_PORT`,
`MAGE_TEST_REDIS_PORT` or `MAGE_TEST_API_PORT` to change them. `make up` rebuilds the
service images when their files change. To use another interpreter, pass `PYTHON`, for
example `PYTHON=.venv/bin/python`.

Without the `MAGE_TEST_*` variables the tests skip, so `pytest integration_tests` in a
plain checkout passes without Docker. `make ci` runs the same suite and fails when any
test skips. CI runs `make ci` in the `integration` job of `build_and_test.yml`.

## Layout

| Path | Contents |
| --- | --- |
| `compose.yaml` | PostgreSQL 16, Redis 7 and the test API, with health checks |
| `conftest.py` | Connection settings and fixtures. Every test gets its own PostgreSQL schema, dropped afterwards |
| `data/postgres_dataset.py` | Source table with 32 column types: hand-written edge rows plus seeded Faker rows in 8 locales. Also the SQL comparison used by every test |
| `postgres/` | Load, export, duplicate handling, column names, values, round trips and pipelines |
| `redis/` | The scheduler's distributed lock |
| `services/api/` | A FastAPI service that serves and accepts data frames as JSON, NDJSON, CSV, Parquet and Arrow, exchanges images, and returns every kind of failure |
| `api/` | Pulling and pushing frames with pandas and Polars, failure handling, images, and Mage pipelines that call the API |
| `mage_runner.py` | Runs the pipelines in `project/` through Mage's trigger, scheduler and executor |
| `project/` | Mage project with the pipelines that `postgres/test_pipelines.py` runs |

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
