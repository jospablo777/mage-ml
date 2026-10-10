# Pipeline services: pipelines exported as standalone Docker services

Status: phase 1 implemented (Python and pandas/Polars pipelines, secrets providers, models,
IBM Cloud and Kubernetes deploy files, the console); Rust blocks are compiled into the
image; R blocks run with Mage's R runner and the locked rv environment; SQL blocks run on
PostgreSQL (runtime/sql.py follows Mage's PostgreSQL path). Other SQL databases are next. User guide: `docs/production/pipeline-services.mdx`.

## Goal

`mage export` turns one pipeline, or a group of pipelines, into a Docker image that runs
without Mage: no web server, no scheduler process, no metadata database, no notebook. The
image starts in under a second, runs its pipelines on their schedules and on HTTP
requests, keeps a durable run history, and serves metrics and a live terminal console.

The users are data and ML engineers who built a pipeline in Mage and need it in
production next to other services: a scoring job, a feature refresh, an export to a
warehouse. What they need, in order:

1. Export, build and run in minutes, with one command each.
2. The pipeline behaves as it did in Mage: same code, same outputs, same tests, same
   retries.
3. They can see what it does: runs, failures with the block and error, logs, durations.
4. They can call it: trigger a run with variables and get the outcome.
5. It is small and fast, and it fails loudly and safely.

## Review of the proposal

What to keep:

- The Ratatui console: its layout, palette, pixel counters, Braille charts, stale and
  disconnected states, read-only default, reviewed commands with idempotency keys, and the
  terminal text filter. It is good; it needs real data, a runs view and a logs view.
- The security defaults of the API: separate read and control tokens, loopback HTTP only,
  HTTPS for remote consoles, bounded request bodies, no shell or code evaluation.
- Capturing triggers from both `triggers.yaml` and the database, with environment filters,
  and refusing to guess when they disagree.
- A compatibility report that lists every block and setting as supported or rejected, so
  nothing silently disappears.
- One image per service, a non-root user, a read-only root filesystem, no compiler or
  console in the runtime image.

What does not hold up:

- **Nothing executes.** The starter has a planner over a hand-written manifest and a
  service that simulates counters. Running Mage blocks without Mage, the part that makes
  the feature exist, is deferred behind milestones (M00 to M15, E00 to E09) and an
  "office" qualification process that this project does not have.
- **The manifest is not Mage's model.** Runtime profiles with artifact URIs and fixture
  digests, kernel contracts and effect classes do not exist in a Mage project. The manifest
  has to be produced from the project by Mage, with real file digests, block order,
  upstream argument order, retry settings and triggers.
- **Rust kernels fused into one binary** assume a block API Mage does not have. Rust
  blocks already build into one binary each with Mage's `mage` crate and a job protocol;
  the export compiles those at image build time and runs them the same way.
- **The first release is overscoped.** Seven deployment targets, lease and fencing tokens,
  outboxes, effect reconciliation and ownership transfer are real concerns at fleet scale,
  but a user who cannot yet run one exported pipeline gets nothing from them. Docker and
  Compose first; Kubernetes manifests next; provider adapters after that.
- **Exporting schedules as inactive by default** protects against the same schedule
  running in Mage and in the service, but a service that silently does nothing is the
  worse failure. The image carries the schedules and runs them when
  `MAGE_SERVICE_SCHEDULES=on`; the console, the logs and `/v1/snapshot` say plainly when
  schedules are off.
- **Full ColumnAtlas inspection inside every service** is a large surface for a
  production job. Each run keeps its block outputs for a retention period; the API returns
  a sample and the schema, and the files can be opened in Mage.

## Design

```
mage export service PROJECT PIPELINE [PIPELINE...] --out DIR [--build --tag IMAGE]
        |
        v
  bundle/                       Docker image
    service.json  ---------->   /usr/local/bin/mage-service   (Rust: API, scheduler,
    project/  (blocks, utils,                                   ledger, executor)
               io_config.yaml)  /opt/mage-service/python       (block worker + the
    rust/     (block crates)                                    block-facing mage_ai)
    Dockerfile, compose.yaml    /opt/mage-service/bin/*        (Rust block binaries)
    README.md, report.txt       Rscript + runner.R             (when R blocks exist)
```

### Export (Python, inside Mage)

Only Mage can read the effective configuration, so capture runs in Mage:

- the selected pipelines, their blocks in order, upstream argument order, block types,
  languages, retry and timeout settings, block tests, conditionals and callbacks;
- block files and the project's shared code (`utils/`, custom modules), `io_config.yaml`,
  `requirements.txt`, the R and Rust environments when used;
- triggers from `triggers.yaml` and the database, with their environment filters;
- pipeline variables, with secrets kept as references (`env_var`, `mage_secret_var`).

It writes `service.json` (versioned), a compatibility report, and the build context. A
block or setting the runtime does not support stops the export with the block named;
nothing runs as a no-op. Capture reads only: it never syncs triggers or writes to the
project.

### Runtime (Rust, `mage-service`)

- **API** (axum): `POST /v1/pipelines/{id}/runs` with variables and an idempotency key,
  `GET /v1/runs`, `GET /v1/runs/{id}` with each block's status, duration and error,
  `GET /v1/runs/{id}/logs`, `GET /v1/snapshot` for the console, `GET /metrics`
  (Prometheus), `GET /healthz`, `POST /v1/control` for pause, resume and concurrency.
- **Ledger** (SQLite, WAL): runs, block runs, idempotency keys and schedule occurrences.
  A run is committed before the request is acknowledged. At startup, runs left running by
  a killed process are marked failed with that reason, or retried when the pipeline allows
  it. One schedule owner: the ledger holds a lease, so two replicas on one volume do not
  both fire.
- **Scheduler**: cron expressions and Mage's named intervals with the trigger's timezone,
  start time and `skip_if_previous_running`; each occurrence has a stable key, so a restart
  does not fire it twice.
- **Executor**: walks the block graph, runs independent branches in parallel within the
  pipeline's concurrency, applies block retries and timeouts, runs block tests, stops
  downstream blocks after a failure as Mage does. Outputs pass between blocks as Arrow IPC
  files in the run's directory; a chain of Python blocks runs in one worker and passes
  frames in memory, as block fusion does.
- **Workers**: Python blocks run in long-lived worker processes (no import cost per
  block), each block in a fresh namespace with Mage's decorators and `kwargs`. Rust blocks
  run their prebuilt binaries with the existing job protocol. R blocks run with Mage's R
  runner. Every worker runs in its own process group and is killed with it on timeout,
  cancellation or shutdown.
- **Shutdown**: SIGTERM stops admission, lets running blocks finish up to a deadline,
  then kills them and records the runs as interrupted.

### Console (Rust, `mage-console`)

The starter's console, on the real snapshot: pipelines with schedule state, next run and
last outcome; run history with block statuses; the failing block's error and log tail;
durations and throughput from the ledger. Read-only by default; with a control token it
can pause, resume, change concurrency and trigger a run.

## Phases

1. Export and runtime for Python, Polars and pandas pipelines: API, ledger, scheduler,
   executor, logs, metrics, Docker image. Console on real data.
2. Rust blocks, R blocks, SQL blocks through the Python worker, groups of pipelines with
   shared and separate concurrency.
3. Kubernetes manifests, signed and digest-pinned images, outputs on object storage.

Each phase ships with tests that run real pipelines through the exported service and
compare their outputs with the same pipeline run by Mage.
