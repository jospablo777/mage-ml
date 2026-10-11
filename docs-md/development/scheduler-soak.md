# Scheduler soak test

`integration_tests/soak/` runs Mage's scheduler as production does: `run_scheduler()` in
its own process, with a PostgreSQL metadata database, Redis for locks and job tracking,
and the process job queue. The project has chain pipelines (a Polars loader, a LazyFrame
transformer and an exporter), a pipeline whose loader fails on its first attempt and has
retries, a pipeline that always fails, and a dynamic fan-out pipeline with a block run
limit of 2. Every pipeline has a trigger that runs once at start and one that runs every
minute. The scheduler loop runs every second.

While the triggers are active, the test samples Mage's tables. It then deactivates the
triggers, waits for every run to finish, and checks that:

- every run reached a final status: completed, or failed for the failing pipeline;
- each trigger produced one run per execution date, the minute trigger once per minute;
- each completed run wrote its result once, with the values of its own frame;
- the flaky loader ran twice per run and the run completed;
- the fan-out pipeline never had more than 2 block runs running at once;
- runs overlapped, and the scheduler process stayed up.

`make -C integration_tests test-soak` runs it for 150 seconds with 12 chain pipelines;
`MAGE_TEST_SOAK_SECONDS` and `MAGE_TEST_SOAK_PIPELINES` change both. It also passed for
600 seconds with 20 chain pipelines.

## What it found

On macOS, with Redis configured, no block run ever executed: pipeline runs stayed
running until the test gave up. Four problems in the process job queue
(`mage_ai/orchestration/queue/process_queue.py`) and one in Redis key names:

1. **The worker pool did not start where processes start with spawn or forkserver.** It
   received the Redis client as an argument, and those arguments are pickled; a Redis
   client holds locks that cannot be pickled. That is the default on macOS and Windows,
   and on Linux from Python 3.14. The pool now receives the Redis URL and connects
   itself.
2. **New jobs were deleted before they ran.** `clean_up_jobs` runs right after the
   scheduler enqueues, and treated a queued job as gone when `Queue.empty()` returned
   True. `Queue.put` hands the job to a feeder thread, so the queue looks empty right
   after it. The worker then found no status for the job and skipped it (upstream it
   raised `KeyError`), and the scheduler enqueued it again on the next loop, without end.
   A queued job now counts as present for 60 seconds while the queue looks empty; a job
   lost after that is enqueued again.
3. **A worker could take a job before its status existed**, since `enqueue` set the
   status after putting the job. The status is set first, and a worker skips a job whose
   status is missing.
4. **A job could wait with no worker pool.** The pool exits when it finds the queue
   empty, which can happen right after a put; the job then waited until another enqueue
   started a pool. The pool waits 5 empty checks before exiting, and `clean_up_jobs`
   starts a pool when jobs are waiting.
5. **Redis keys were shared between deployments.** Job keys were named by id alone, such
   as `block_run_1`, and lock keys by schedule id. Two deployments that share a Redis
   server, or a deployment whose metadata database was recreated, took each other's keys:
   jobs were skipped as running elsewhere. Keys now start with a hash of the metadata
   database URL. During an upgrade, a scheduler on the old version and one on the new
   version do not see each other's locks.

Problems 2 to 4 are races that exist on Linux as well; the one-second loop and slower
process start on macOS made them happen on every run.

## Related fix

Singer sources parsed the command line in their constructor, so a source built in code
inside another program, such as pytest with `-p`, read that program's arguments as
Singer options. Sources run as programs through `main()`, which now parses the command
line and passes it.
