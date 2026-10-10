# Durable execution: block run attempts

Status: block run fencing and interruption accounting are implemented (`BlockRun.attempt`, `fusion.claim_block_run`,
`BlockRun.update_if_attempt`) and tested, including a worker killed in a real scheduler
run and a scheduler killed and restarted
(`integration_tests/soak/test_worker_crash.py`). The remaining gaps are listed below.

## Review of the starter

`do_not_commit/top_10_next_features/01_durable_execution` proposes a new job queue: a
Rust state machine with fenced leases, a PostgreSQL queue schema, an outbox, effect
reconciliation and a Recovery page. Measured against Mage's scheduler:

- **The queue exists.** Mage creates block runs, enqueues one job per block run (or per
  stage with block fusion) and detects lost workers on every tick through the job manager
  (`has_block_run_job`: the worker's pid, or its Redis liveness key). A second queue would
  duplicate run, trigger, backfill and retry semantics that Mage already has.
- **The missing piece was fencing.** Workers write their own final status, and no write
  checked whether the worker still owned the block run. A worker the scheduler had given
  up on (crash reset, timeout, cancel, manual retry, a Redis liveness lapse on another
  replica) could still write COMPLETED or FAILED over what happened since.
- **Exactly-once effects are not achievable by the scheduler.** The starter's
  "needs reconciliation" state requires every exporter to report when it starts an
  external effect, which no Mage block does. Re-running an exporter after its process died
  stays at-least-once; destinations need idempotent writes (keys, upserts, replace
  policies).

So the implementation adds attempts to block runs instead of a new queue.

## Attempts

- `block_run.attempt` (migration `a7c31e9f0d42`) is incremented by every claim of the
  block run: `run_block` (as the scheduler enqueues it) and each block of a fused stage.
  A claim is one guarded UPDATE: INITIAL or QUEUED to RUNNING, only while the pipeline
  run is RUNNING, so two workers cannot both start the same block run, and a queued job
  does not start a block run that was cancelled.
- The worker keeps the attempt it claimed. Its COMPLETED and FAILED writes
  (`on_block_complete*`, `on_block_failure`, `fusion.complete_and_claim`) apply only while
  the block run is RUNNING in that attempt. Otherwise the result is discarded with a
  warning in the log, and a fused stage stops.
- Scheduler writes stay authoritative: a crash reset (back to INITIAL), a timeout
  (FAILED) or a cancel (CANCELLED) changes the status, so the superseded worker's write
  matches nothing.
- Retrying blocks stops their running jobs before resetting them.
- Each queued block run records the scheduler process that launched it
  (`metrics.launched_by`, the queue's `HOST_<host>_PID_<pid>`). A lost job counts as a
  crash (`MAGE_BLOCK_RUN_MAX_CRASHES`, default 3) only if this scheduler launched it; a
  block interrupted when its scheduler stopped (a restart, a deploy, a lost replica) runs
  again and counts as an interruption (`MAGE_BLOCK_RUN_MAX_INTERRUPTIONS`, default 10).

## Remaining gaps

1. **Interrupted blocks start over.** A block interrupted by a scheduler restart runs
   again from the beginning; long blocks need their own checkpoints.
2. **Stale outputs.** Outputs are stored per partition and block without an attempt, so a
   superseded worker can still overwrite the files of a newer attempt before its status
   write is rejected.
3. **Locks are not leases.** The scheduler's per-run Redis lock (10 s) is neither renewed
   nor released, so a long tick allows a second replica in; without `REDIS_URL` replicas
   share no job ownership at all.
4. **PID reuse.** A reused pid keeps a dead worker's block run RUNNING until its timeout.
5. **Integration pipelines and Kubernetes.** Stream jobs and Kubernetes block runs write
   status on other paths that are not fenced yet.
