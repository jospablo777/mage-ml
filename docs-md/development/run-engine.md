# Rust run engine for triggered runs

Status: steps 1 to 3 done (2026-10-20); `MAGE_JOB_ENGINE=on` turns it on.

## Where a triggered run spends its time

Measured on a three-block Python pipeline (1,000 rows, each block 20 to 40 ms), through
the scheduler loop (`fusion_verify.run_in_process`, as the pipeline benchmark runs it),
median of four runs on an Apple M-series laptop:

| | Whole run | Start to first block | Blocks | Last block to run completed |
| --- | --- | --- | --- | --- |
| Fused (the default) | 1.0 s | 0.7 s | 0.09 s | 0.17 s |
| Block by block | 2.4 s | 0.7 s, then 0.8 s per block | 0.08 s | 0.1 s |

About 90% of a fused run is not the blocks. The largest part is the job process: every
block or stage job starts a new Python with `spawn` (`PROCESS_CONTEXT`, all platforms),
which imports Mage before the block starts. Forking from a loaded process was tried and
rejected: pandas and Polars start threads at import, and forked children crashed.

A server adds waits the measurement above does not include:

- the worker pool takes queued jobs once a second (`poll_job_and_execute` sleeps 1 s);
- the scheduler runs every 10 seconds (`SCHEDULER_TRIGGER_INTERVAL`), earlier when a job
  finishes (`LoopTimeTrigger.wait`, at least 1 s apart).

## Design

The scheduler keeps deciding what runs and keeps every record (block runs, logs, stored
outputs, run records, tests, contracts, monitors, callbacks). What changes is where a job
runs: in a warm Python worker held by the Rust engine, not in a new process.

- `mage-service jobs`: the engine binary (the `mageml-engine` wheel, already used by
  stream and pipeline services) runs as a child of the scheduler process. It reads job
  requests on stdin and writes job events on stdout, one JSON object per line, and exits
  with its parent. It holds a pool of Python job workers, already started and with Mage
  imported.
- A job worker (`mage_ai/orchestration/queue/engine_worker.py`) runs one job at a time:
  the target and its arguments arrive pickled, as the process queue passes them, and the
  worker calls them as a process queue worker does (database session, Sentry, job
  aliases). After each job it drops the project's own modules, so edited code is read
  again; it is replaced after a number of jobs or when its memory grows, and killed with
  its process group when the job is killed or times out.
- `EngineQueue` (`mage_ai/orchestration/queue/engine_queue.py`) implements the queue the
  job manager uses (enqueue, has_job, kill_job, jobs_finished), with the same Redis keys
  as the process queue, so replicas and crash detection behave the same. A job starts as
  soon as it is enqueued and a worker is free.
- Without the engine binary, or with `MAGE_JOB_ENGINE=off`, jobs run in the process
  queue as before.

Not chosen as the first step: the engine running the blocks itself, as it does in an
exported service. Every Mage run feature above lives in `BlockExecutor` and would have to
be written again in the engine, while the blocks are about 10% of the run. It stays an
option for pipelines that use none of those features, measured against warm workers.

## Measured

The pipeline benchmark (`integration_tests/benchmark/pipelines`, `--modes fused warm
service --repeats 3`), 2026-10-20, Apple M-series laptop with 24 GB, median seconds and
peak memory of the processes involved, from a git worktree pinned at c07b4c6f6 (after the
garbage collection and template changes below). Every run's output matched the pandas
reference.

| Pipeline | Rows | Fused, processes | Fused, run engine | Exported service |
| --- | --- | --- | --- | --- |
| pandas | 1,000,000 | 1.25 s, 893 MB | 0.40 s, 1,315 MB | 0.47 s, 395 MB |
| Polars | 1,000,000 | 1.11 s, 775 MB | 0.30 s, 1,146 MB | 0.24 s, 256 MB |
| R | 1,000,000 | 2.15 s, 943 MB | 1.42 s, 1,526 MB | 1.64 s, 659 MB |
| Rust | 1,000,000 | 1.24 s, 651 MB | 0.42 s, 986 MB | 0.24 s, 207 MB |
| pandas | 3,000,000 | 1.75 s, 1,440 MB | 0.93 s, 1,776 MB | 0.69 s, 862 MB |
| Polars | 3,000,000 | 1.44 s, 1,366 MB | 0.61 s, 1,601 MB | 0.24 s, 794 MB |
| R | 3,000,000 | 2.93 s, 1,355 MB | 2.17 s, 1,738 MB | 2.29 s, 1,193 MB |
| Rust | 3,000,000 | 1.27 s, 685 MB | 0.48 s, 1,269 MB | 0.24 s, 417 MB |

On the run engine a run takes about a third of the time at 1 million rows. pandas and R
runs were faster than the exported service here because the benchmark runs the service
with `mage-service run`, a new process per run that starts its Python or R worker; a
service that serves runs keeps its workers too. The memory column counts the engine and
its idle warm worker, about 450 MB that a server holds whether runs happen or not
(`MAGE_JOBS_WARM` sets how many). Workers started for a burst of jobs end after
`MAGE_JOBS_IDLE_SECONDS` (60) without a job.

The gap left to an exported service for Polars and Rust is MageML's work around the
blocks: a profile of a warm run shows about 56 SQL statements and 14 commits per run and
writing each output as Parquet.

## Garbage collection after each block

A profile of warm runs in one process showed 60% of the time in `gc.collect()`: a stage
collects garbage after each block to free the block's inputs (`fusion.release_block`), and
a full collection scanned every object of MageML, 33 ms per block. `freeze_imports`
(`mage_ai/shared/processes.py`) moves what is loaded before the first job out of the
collector's reach (`gc.freeze`), so a collection scans only what jobs made. Engine workers
freeze after their imports, process queue workers before their job, and `mage run` before
the pipeline; a notebook or the server never freezes.

| Three-block pipeline | Before | After |
| --- | --- | --- |
| Fused run on the run engine | 0.23 to 0.30 s | 0.12 to 0.13 s |
| `mage run` | 0.95 s | 0.77 s |

## Plan

1. `mage-service jobs` and the job worker, with unit tests in Rust and Python.
2. `EngineQueue`, opt-in with `MAGE_JOB_ENGINE=on`; the scheduler tests and the soak
   test pass with it.
3. Measure with the pipeline benchmark: the `warm` mode, above.
4. Default on where the binary is installed: to decide. The scheduler soak tests (12
   pipelines with retries, fan-out, a failing pipeline, overlapping runs, a worker killed
   in the middle of a block) pass on it, block by block and fused.
