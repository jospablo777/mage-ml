# Resource-aware scheduling

Status: implemented in `mage_ai/orchestration/resources.py`, with admission in
`PipelineScheduler.__queue` and the environment applied in `run_block`. Tested in
`mage_ai/tests/orchestration/test_resources.py`: decisions, and admission through the
scheduler across pipelines, with GPUs and impossible requests. The user guide is
`docs/guides/pipelines/resources.mdx`.

## Review of the starter

`do_not_commit/top_10_next_features/08_resource_scheduling` is a Rust admission kernel:
CPU, memory, exclusive GPU devices and tenant-wide concurrency keys, reserved atomically
under one mutex, with best-fit worker selection, reservation guards and machine-readable
waiting reasons. It has no durable state, scheduler loop or process supervision. Measured
against Mage:

- **Mage has one kind of worker.** A scheduler's process queue runs jobs as child
  processes on its host, and replicas share work through the database and Redis. There's
  no pool of heterogeneous workers to fit jobs onto, so worker selection and capability
  matching don't apply.
- **The durable state exists.** The starter's design asks for reservations reconstructed
  from durable job attempts. In Mage the queued and running block runs are those
  attempts. Recording what each holds on the block run makes the accounting durable
  without a separate reservation store, and the existing crash detection (feature 01)
  releases what crashed runs held.
- **What Mage lacked:** limits shared across pipelines (it had per-pipeline block and
  pipeline run limits), memory admission, GPU device assignment and waiting reasons.

So the implementation is a Python admission step in the scheduler that reads durable
state. A Rust kernel would only replace a few comparisons.

## Design

- **Request** (`configuration.resources`): memory, cpu, gpu and uses, validated strictly.
  Unknown settings are errors.
- **Policy** (`resources` in the project's `metadata.yaml`, read directly because
  `RepoConfig` drops unknown keys): limits, a memory budget (default 80% of host RAM) and
  GPU devices.
- **Admission** happens when a block run would be queued. Under a distributed lock (Redis
  when configured), the scheduler loads the queued and running block runs, sums what
  their `metrics.resources` hold, and decides. Shared limits are scoped by project
  folder name. Memory and GPUs count only block runs with the same `launched_by` (the
  scheduler process). Admitted runs record `resources` in the same update that marks them
  QUEUED. Waiting runs get `metrics.waiting`, written only when it changes. Impossible
  requests fail the block run.
- **Environment.** `run_block` sets the thread variables and `CUDA_VISIBLE_DEVICES` from
  what the block run holds, in the job's own process.
- **Fusion.** Blocks with resources don't fuse: a stage claims later blocks without
  passing through admission.

## Remaining gaps

1. **No enforcement.** Declared memory is accounting, not a cgroup limit. Containers and
   Kubernetes enforce.
2. **Thread pools already initialized** in the worker process keep their size.
3. **Priorities and fairness**: waiting block runs are admitted in scheduler order, with
   no priorities and no protection against a large request waiting behind small ones.
4. **Kubernetes and ECS executors** ignore memory and GPU accounting on their own
   nodes; shared limits still apply to them.
5. **Placement preview** ("would this fit now?") is not offered.
