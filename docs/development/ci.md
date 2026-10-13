# CI and tests

MageML's CI runs on GitHub Actions in the fork's own repository. This guide covers every
test suite, how to write and run tests, every CI workflow and job, the failures CI has had
with their causes, and how to run the same checks on your machine before pushing.

## Test suites

| Suite | Where | Runner | Needs | Runs in CI |
| --- | --- | --- | --- | --- |
| Unit tests | `mage_ai/tests`, `mage_integrations/mage_integrations/tests`, `vendor/singer-python/tests` (about 6,400) | pytest | Python only; Rust and R tests skip without `cargo` or `Rscript` | test_backend |
| Integration tests | `integration_tests/<service>` (about 930) | pytest with xdist | The Docker services of `integration_tests/compose.yaml`, R with rv, Rust | Integration tests |
| Scheduler soak tests | `integration_tests/soak` | pytest | PostgreSQL, Redis | Integration tests |
| mageml R package | `mage_ai/data_preparation/models/block/r/mageml/tests`, run by `integration_tests/r/test_mageml_package.py` (testthat, lintr, roxygen, `R CMD check`) | pytest | R and the rv environment | Integration tests |
| Rust | ColumnAtlas (`rust/column_atlas`), the block SDK, the pipeline service crates | cargo test | Rust | Rust extension |
| Frontend unit tests | `mage_ai/frontend/tests/unit/*.unit.mjs` | `node --test` | Node | test_backend |
| Browser tests | `mage_ai/frontend/tests/*.spec.ts` (12 specs) | Playwright | A running MageML server | test_backend |
| Storybook tests | `mage_ai/frontend` stories | Playwright | The Storybook build | test_backend |
| Compilation study | `integration_tests/topologies` | pytest | Small grid by default; full grid with `MAGE_STUDY_FULL=1` | The small grid, in Integration tests |
| Benchmarks | `integration_tests/benchmark` (fusion and Rust pipelines) | pytest | Rust; `make -C integration_tests test-benchmark` keeps results under `integration_tests/benchmark/results` | Both, as smoke tests that also compare outputs, in Integration tests |

## Writing tests

### Rules

- **Every change has a test** that fails without it.
- **Write only under temporary folders.** The run fails when a test leaves anything in the
  working directory, which is the checkout (`mage_ai/tests/conftest.py`).
- **No waiting without a deadline.** Poll with a deadline and fail with what you waited for.
  In CI, a test that runs over 900 s (integration) fails with its stack.
- **No shared state.** Never change module constants in place (`ApiError` error kinds are
  read-only: start from `dict(ApiError.RESOURCE_NOT_FOUND)`); undo environment changes with
  `patch.dict(os.environ)` or `monkeypatch`.
- **Processes start with spawn**, through `mage_ai.shared.processes.PROCESS_CONTEXT`, as Mage
  starts them.
- **A skip says why** and what to install. In CI an integration test may not skip.
- **No real credentials.** Tests use fake values; services get theirs from `compose.yaml`.
- **Name tests by behavior:** `test_a_wrong_password_says_so`, not `test_session_2`.

### Unit test helpers (`mage_ai/tests`)

| Helper | Use |
| --- | --- |
| `base_test.TestCase` | A temporary Mage project for the test class, removed afterwards |
| `base_test.DBTestCase`, `AsyncDBTestCase` | The same with the metadata database (`test.db`, `test<worker>.db` with xdist), migrated and dropped |
| `factory.py` | `create_user`, pipeline runs, schedules and other records |
| `shared/tracking_utils.use_temporary_mlflow(test, experiments)` | An MLflow store in a temporary folder for one test |
| `conftest.py` | `ENV=test_mage`, the temporary MLflow store for the session, the leftover-files check |

Contract tests guard the repository itself: `test_dependency_contracts.py`,
`test_stack_contracts.py`, `test_deployment_contracts.py`, `test_docs_markdown.py` (the
`docs-md/` export matches `docs/`; regenerate it with `scripts/docs/export_markdown.py`) and
`test_test_cleanup.py` (the cleanup rules above).

### Integration test helpers (`integration_tests`)

| Helper | Use |
| --- | --- |
| `mage_runner.mage_project` | A temporary copy of `integration_tests/project` with its own metadata database |
| `mage_runner.execute_pipeline` | Runs a pipeline to its end and returns the run |
| `streaming_runner.streaming_pipeline`, `wait_until` | Runs a streaming pipeline the two ways Mage does, until the test stops it |
| `exported_services.py` | Exports a pipeline as a service and runs it |
| Per-service fixtures in each folder's `conftest.py` | Connections, schemas and tables that are removed afterwards |

Tests that compile Rust blocks belong to the xdist group `rust_builds`
(`integration_tests/conftest.py`): list a new module there. See
[integration_tests/README.md](https://github.com/jospablo777/mage-ml/blob/master/integration_tests/README.md) for how tables are compared.

### Browser test helpers (`mage_ai/frontend/tests`)

| Helper | Use |
| --- | --- |
| `base.ts` `test` | Signs in as `admin@admin.com` before each test and fails it on page errors |
| `api.ts` `MageApi` | Creates and deletes pipelines and blocks, writes files, runs triggers, restarts the kernel |

Tests that run blocks restart the kernel in `beforeEach`.

## Environment variables

| Variable | Effect |
| --- | --- |
| `ENV=test_mage` | Unit test mode: SQLite `test.db` in the working directory. Set by `conftest.py` and for the CI jobs; the browser tests unset it |
| `MAGE_TEST_<SERVICE>_*` | Where each integration service listens; the Makefile sets them from `compose.yaml`'s ports |
| `MAGE_TEST_R_PROJECT_DIR` | The rv environment the R tests use (`integration_tests/r_env`, set by the Makefile) |
| `MAGE_TEST_SOAK_*` | Length, pipelines and fusion of the soak tests |
| `MAGE_STUDY_FULL=1` | The compilation study's full grid, its machine guards and the model experiments |
| `MAGE_STUDY_GPU=1` | The CUDA scorers and the GPU readings; without it no test uses a GPU |
| `MAGE_STUDY_RESULTS_DIR`, `MAGE_STUDY_REPEATS`, `MAGE_STUDY_*` | Where the study writes results and its limits ([compilation-study.md](compilation-study.md)) |
| `MAGE_TEST_SERVER_URL` | A running server for the browser tests instead of one Playwright starts |

## Workflows

| Workflow | When | What it checks |
| --- | --- | --- |
| `build_and_test.yml` | Push to `master` and pull requests that change `mage_ai/`, `mage_integrations/`, `vendor/`, `runtimes/`, `rust/`, `integration_tests/`, `pyproject.toml`, `uv.lock` or the workflow; manual | Lint, Rust, packaging, integration tests, unit tests and the browser tests |
| `dependency_audit.yml` | Dependency changes; Mondays 06:20 UTC; manual | Known vulnerabilities in the Python and JavaScript dependencies |
| `codeql-analysis.yml` | Tuesdays 16:24 UTC; manual | CodeQL code scanning |
| `trivy.yml` | Tuesdays 01:35 UTC; manual | Trivy scan of the image and the repository |
| `lint_docker_image.yml` | Pull requests that change the Dockerfile; manual | Dockerfile lint |
| `publish_docker_image.yml` | Manual only | Builds the Docker image; pushes nowhere |
| `release.yml` | Manual only | Builds and qualifies the wheels, then publishes after approval in the `release` environment |

Changes to `docs/` alone start no workflow.

## Jobs of `build_and_test.yml`

| Job | Steps | Time | Run it locally |
| --- | --- | --- | --- |
| Lint changed files | pre-commit (ruff, isort, YAML, JSON, test names) on the changed files | 1 min | `uv run pre-commit run --files <files>` |
| Rust extension | `cargo fmt --check`, `clippy -D warnings` and `cargo test` for ColumnAtlas, the block SDK and the pipeline service | 2 min | the same cargo commands in `rust/column_atlas`, `mage_ai/data_preparation/models/block/rust/sdk` and `mage_ai/pipeline_services/service` |
| Packaging | `uv lock --check`, `uv build --all-packages` | 8 min | the same commands |
| Integration and MageML pipeline groups | Seven parallel jobs, each with only its services: see [Integration groups](#integration-groups) | 15 to 30 min each (90 min limit) | `make -C integration_tests ci-tests TESTS=... SERVICES=...`, below |
| test_backend | Git tests, the unit tests, frontend unit tests, the frontend build, the Playwright tests, Storybook | 35 to 45 min (90 min limit) | see [Unit tests](#unit-tests) and [Browser tests](#browser-tests) |

## Integration groups

The integration tests run as seven jobs in parallel. "Integration" groups test MageML with
external tools; "MageML" groups test MageML's own pipelines. Each job starts only its
services, and fails when one of its tests is skipped.

| Job | Tests | Services | R | Rust warm-up |
| --- | --- | --- | --- | --- |
| Integration: SQL databases | `postgres`, `mysql`, `clickhouse`, `trino`, `duckdb` | PostgreSQL, MySQL, ClickHouse, Trino (with MinIO) | No | Yes |
| Integration: document stores and caches | `mongodb`, `redis` | MongoDB, Redis | No | No |
| Integration: streaming and storage | `kafka`, `rabbitmq`, `nats`, `activemq`, `streaming`, `s3` | The brokers, MinIO, and the sinks the streaming tests write to | No | No |
| Integration: ML and HTTP services | `feast`, `mlflow`, `api` | Feast (with PostgreSQL and Redis), MLflow, the test HTTP API | No | No |
| MageML: compiled and polyglot pipelines | `chains`, `tutorials`, `benchmark`, `r` | PostgreSQL, MySQL, MinIO, the test HTTP API | Yes | Yes |
| MageML: pipeline shape study | `topologies` | PostgreSQL, for the warm-up | Yes | Yes |
| MageML: scheduler soak | `soak`, plain and with block fusion | PostgreSQL, Redis | No | No |

A new test folder goes into one group in `.github/workflows/build_and_test.yml`, with the
services it needs. The Rust groups share one build cache (the repository's caches are
limited to 10 GB), saved by the pipelines group.

## Failures and their causes

Each row is a failure CI has had, what it looked like, and the rule that prevents it.

### Unit tests

| Symptom | Cause | Rule |
| --- | --- | --- |
| `Tests left files in the working directory ...: mlflow.db, test` while every test passed | A test wrote into the checkout. MLflow writes `./mlflow.db` when no tracking URI is set, and `set_tracking_uri(None)` also removes `MLFLOW_TRACKING_URI`; a test built variables under the relative path `test` | Tests write only under temporary folders. The test session points MLflow at a temporary store (`mage_ai/tests/conftest.py`); use `use_temporary_mlflow` for a store of your own |
| `.DS_Store` in the leftover list on macOS | Finder writes it | Ignored by the check |
| A test passes alone and fails in the full run | State left by an earlier test: a shared constant changed in place, a module global, an active MLflow experiment | Never change shared constants: `ApiError` error kinds are read-only, start from `dict(ApiError.RESOURCE_NOT_FOUND)` |
| Service tests fail with `No module named numpy` | Code the exported service runs imported a package its bare Python does not have | Import optional packages where they are used |

### Integration tests

| Symptom | Cause | Rule |
| --- | --- | --- |
| The job stops at its 2 hour limit; the output stalls at some percentage | Tests waiting for something that never comes. With the 900 s per-test limit and `-v`, each such test fails with its name and stack | Never wait without a deadline in a test |
| Topologies tests time out, pandas-only ones included | The compilation study's guards waited for an idle machine (8 GB free, low load, cool CPU) before each test and each run; a CI runner is never idle | The guards (`machine_guard`, `provenance.wait_quiet`) wait only with `MAGE_STUDY_FULL=1` |
| An exported service's Rust build times out | The service compiles all its blocks into one bundle whose crate features differ from single blocks, so cargo built Polars again, cold | The warm-up builds that bundle too |
| Tests that compile Rust time out together | They share one cargo target directory, which cargo locks while it builds; four workers queued behind one cold Polars build | They run in the xdist group `rust_builds` on one worker (`integration_tests/conftest.py`, `--dist loadgroup`); CI builds Polars and the SDK once in the "Build Polars and the Rust block SDK" step (`make -C integration_tests warm-rust`: Mage's blocks and an exported service's bundle) and caches the target directory |
| `make ci` fails after the tests with `skipped: ...` | `check_no_skips.py` fails any skipped test: in CI every service is configured | A test that needs a package must have it in the `dev` group (CatBoost for `integration_tests/feast`); tests that cannot run on CI are not collected by default (GPU tests) |
| "Start the integration services" fails, a container exited | A service did not boot. RabbitMQ stopped with `eacces` on its Erlang cookie: the healthcheck ran as root before the server wrote the cookie, and created it readable by root only | RabbitMQ runs as its own user (`user: rabbitmq`), so its healthcheck does too; the "Service logs" step prints each failing service's log |
| A test fails on some runs only, comparing the order of a service's answer | The test assumed an order the service does not promise (Feast's feature order differs between runs) | Assert what the contract promises: map by names, compare sets |
| A streaming test hangs on Linux only | The helper forked the pipeline process; a process forked after its parent ran Polars can wait forever on its first Polars call | Start processes with `mage_ai.shared.processes.PROCESS_CONTEXT` (spawn), as Mage does |
| GPU tests skip or fail | No GPU on runners | GPU tests run only with `MAGE_STUDY_GPU=1` |

### Browser tests (Playwright)

| Symptom | Cause | Rule |
| --- | --- | --- |
| A block run in the editor never shows its output; later tests in the same server fail the same way | On CI the notebook kernel sometimes stopped running code after a few runs (cause still open) | Tests that run blocks restart the kernel first (`api.restartKernel()` in `beforeEach`) |
| "Email/username and/or password invalid." not shown | The sign-in error was replaced by a generic not-found message after a shared error dict was changed in place | Fixed; see the unit test rule above |
| The e2e server behaves unlike a real install | `ENV=test_mage`, set for the whole job, puts the server and its kernel in the unit tests' mode | The Playwright steps set `ENV: ""` |

### Lint, Rust and packaging

| Symptom | Cause | Rule |
| --- | --- | --- |
| Lint fails on a file you did not touch | pre-commit runs on the files changed in the push | Run pre-commit on your files before pushing |
| `uv lock --check` fails | `pyproject.toml` changed without `uv lock` | Run `uv lock` and commit `uv.lock` |
| Clippy fails | Warnings are errors (`-D warnings`) | Run clippy with the same flags |
| A Rust test fails only under load | A race, such as two threads loading the same ONNX model | Fix the race; the tests run threaded |

### Release workflow

| Symptom | Cause | Rule |
| --- | --- | --- |
| The contract job fails | The tag and the `pyproject.toml` version differ | Tag `v<version>` exactly |
| Publishing is refused | The `github-pages` environment allows only `master` | Run the workflow from `master` |
| Publishing waits | The `release` environment needs a reviewer's approval | Approve the run in GitHub |

## Run the checks locally

### Unit tests

```bash
uv sync --locked --group dev
uv run pytest mage_ai/tests mage_integrations/mage_integrations/tests vendor/singer-python/tests
```

The run fails if a test left files in the working directory, as in CI.

### Integration tests

```bash
make -C integration_tests up          # the Docker services
make -C integration_tests warm-rust   # optional: build Polars and the Rust SDK once
make -C integration_tests ci PYTEST_ARGS='-n 4 --dist loadgroup -v --timeout=900'
make -C integration_tests down        # stops the services and removes their volumes

# One CI group, as its job runs it:
make -C integration_tests up SERVICES="mongodb redis"
make -C integration_tests ci-tests TESTS="integration_tests/mongodb integration_tests/redis" \
  REPORT=results-documents.xml PYTEST_ARGS='-n 4 --dist loadgroup -v --timeout=900'
```

`make -C integration_tests test-<service>` runs one service's tests, for example
`test-rabbitmq`. The services need several GB of memory; stop other containers first if
Docker's memory is small.

### Browser tests

Run them against a scratch server, never against a project you care about: the tests
create and delete pipelines.

```bash
cd /tmp && mage init test_project && REQUIRE_USER_AUTHENTICATION=1 mage start test_project --port 6797
cd mage_ai/frontend && MAGE_TEST_SERVER_URL=http://127.0.0.1:6797 \
  npx playwright test -c playwright.config.ci.ts
```

### The whole workflow

[`act`](https://github.com/nektos/act) runs a GitHub workflow in local containers. It needs
much memory with the integration services; use it for changes to the workflow files.

## When CI fails

1. Open the failed job and read the first error, not the last one.
2. Integration tests print each test with its result (`-v`); a timeout shows the test and
   its stack at the end of the log.
3. The Playwright report is an artifact of the run (`playwright-report-Linux-3.12`), with a
   trace and screenshots for each failed test.
4. Reproduce with the local commands above, fix, and add a test that fails without the fix.
5. Add the failure to this page when it is new.
