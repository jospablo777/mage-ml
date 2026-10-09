# Block fusion: blocks that run together without losing their boundaries

Status: phase 1 is implemented (`mage_ai/orchestration/fusion.py`, `run_stage` in the
scheduler) and tested; "verify fusion" and the benchmark test are not built yet. User docs:
`docs/design/data-pipeline-management.mdx`. Measurements are from a 5-block pandas chain
on 3 million rows (load, clean, enrich, aggregate, export), run from an API trigger
through `mage start` on macOS.

## The cost of passing data between blocks

| Mode | Run time | Blocks' own time |
|---|---|---|
| Process per block, outputs through storage (default) | 33 s (56 s before 0bf18cba4) | 6 s |
| `run_pipeline_in_one_process` | 14.5 s | 1.1 s |
| One process with `cache_block_output_in_memory` | 12.6 s | 0.4 s |
| Block fusion (`block_fusion: chains`) | 12.3 to 16.5 s | 1.5 s |

Where the default mode spends its time and memory:

1. **A process per block run.** Each one starts with spawn on macOS (and on Linux from
   Python 3.14), imports Mage for about 3 s and holds about 370 MB before the block's
   data. With 20 concurrent block runs that is 7 GB of imports.
2. **Writing every output**, with Python-level type checks, Parquet, a sample file and
   several JSON files.
3. **Reading every input back** in the next process, with the reverse conversions.

A single-block pipeline avoids all of this, and loses what blocks are for: code you can
read, test and rerun one step at a time, and an output to inspect after each step.

## The idea

Keep blocks as they are in the editor, the code, the tests, the logs and the run
history. Change only how a pipeline run executes them: blocks that form a chain run as
one **stage**, in one process, and the next block receives the previous output from
memory. Each block keeps its own block run, status, logs, retries and stored output.

A chain is a path where each block has one downstream block and that block has one
upstream block:

```
load -> clean -> enrich -> aggregate -+-> export_a        stage 1: load .. aggregate
                                      +-> export_b        stages 2 and 3: one block each
```

Chains, and not larger groups, because:

- **No output has two readers in memory**, so a block that changes its input in place
  (`df['x'] = ...`, `inplace=True`) changes no other block's input. pandas
  copy-on-write does not protect two readers of the same object.
- **Memory holds one link of the chain**, not every output of the run, as the
  in-memory cache does today.
- **Branches still run in parallel**, as separate stages in separate processes.

The stored outputs stay exactly what they are now, written before the next block starts.
So every reader of stored outputs keeps working unchanged: the run page, CSV downloads,
`get_variable`, global data products, remote blocks, SQL blocks, conditionals,
callbacks, Great Expectations, retries and resumes. A stage saves the process start of
every block after the first, and reading the input back. On the benchmark that is 33 s
against about 14.5 s.

## Developing and debugging stay block by block

- **The notebook is unchanged.** Fusion applies to pipeline runs: triggers, backfills
  and the API. The Run button still runs one block, reads its upstream outputs from
  storage and stores its own output.
- **The plan is computed, not edited, so it cannot go stale.** The scheduler builds each
  stage when its first block becomes ready, from the pipeline as saved then
  (`fusion_plan` and `stage_block_runs` share one rule). The pipeline settings show the
  stages of the saved pipeline. Outlining them in the dependency graph is not built
  yet.
- **A failure names its block.** Each block keeps its block run, status, log file and
  error. Log lines carry a `stage` tag next to the block's tags.
- **Verify fusion (not built yet).** An action that runs the pipeline once fused and
  once block by block, compares every stored output, and names the first block whose
  output differs. Until then, the type matrix in
  `mage_ai/tests/orchestration/test_fusion_types.py` and the flow tests hold the rule.

## Which blocks join a chain

A block joins its upstream's stage only when the upstream has no other downstream block,
the block has no other upstream block, and neither is excluded below. Each rule comes
from a reader or a runtime that needs a separate process or data in storage.

| Excluded | Why |
|---|---|
| Dynamic blocks, their children, reduce | Block runs created at run time; read the parent from storage |
| Replicated blocks; block runs with metrics overrides (`upstream_blocks`, controller, original) | Not one block run per block |
| Data integration blocks | Their own controller and stream block runs |
| Blocks whose executor, resolved through `ExecutorFactory`, is not the local `BlockExecutor` | They run elsewhere; the resolution covers the block, the pipeline and `DEFAULT_EXECUTOR_TYPE` |
| PySpark pipelines | Outputs become Spark frames only when stored |
| Sensors and global data product blocks | They can wait for hours and would hold the stage |
| dbt blocks | Their own upstream tables and outputs |
| Blocks with `configuration.variables` (chunks, batch settings, input types) or with reduce or global data product upstreams | Their reads come from storage with settings memory does not apply |
| Hook block runs | No block, `metrics.hook` |
| Pipelines with `MEMORY_MANAGER_V2` | Parts and readers on disk |
| Pipelines with `run_pipeline_in_one_process` | That setting already decides the process; it wins |
| Blocks with `fusion: false` | The block runs alone: it starts a stage and ends it |
| Streaming and integration pipelines | Their own executors |

Fusion is a pipeline setting, `block_fusion: chains`, off by default in the first
release. `fusion: false` on a block isolates it on both sides, for blocks that change
process state or that should not share a process.

## Which values pass in memory

The next member receives the previous member's output from memory only when storage
would give it the identical value. Everything else is read from storage, as now:

- **In memory:** a single output that is a pandas DataFrame with no object columns and
  no object index, or a Polars DataFrame.
- **From storage:** everything else. That includes:
  - more than one output;
  - a Polars LazyFrame, so its plan runs once and the next block gets `scan_parquet`
    of the result;
  - NumPy arrays and pyarrow Tables;
  - JSON values (dict keys and tuples change on the way);
  - models, custom objects and GeoDataFrames.

pandas object columns are excluded because storage casts strings to the `str` dtype, so
`None` becomes `NaN` and `dtype == object` checks change. Generators are detected by the
output's type when it is passed, since they are known only after the block runs.

A CI test matrix holds this list honest. For each type, it compares the in-memory value
with the value read back, with strict dtype, index and column checks.

## Scheduling: members are claimed one at a time

- **One job per stage**, under the first block run's job id, `block_run_<id>`. The
  scheduler enqueues a stage when its first block run is executable.
- **The other block runs stay INITIAL.** In one transaction, the stage marks block run k
  COMPLETED and claims k+1 as RUNNING, only if k+1 is still INITIAL and the pipeline run
  is still RUNNING. If no row changes, the stage stops. So:
  - the scheduler never sees a block run it could start on its own: one is INITIAL until
    the stage claims it, and its upstream is not COMPLETED before that claim;
  - at most one block run of a stage is RUNNING or QUEUED, so `block_run_limit` counts
    the stage once;
  - cancelling the run makes the next claim fail, and the stage stops.
- **Each claimed block run is an alias of the stage job.** Before claiming a block run,
  the stage registers `block_run_<id>` as an alias of its job in the process queue's job
  table and in Redis (`register_job_alias`). Every existing path that checks or kills a
  block run's job finds the stage process under that id: crash detection, block
  timeouts, cancellation. Older scheduler replicas read the same keys, so a rolling
  deploy needs no gate. When the job ends, its id and aliases are marked completed and
  their Redis keys deleted.
- **Failure.** When a block fails, it is marked FAILED and the stage stops. The block runs
  after it were never claimed and stay INITIAL; the scheduler handles them as today:
  UPSTREAM_FAILED when failures are allowed, CANCELLED when the run fails.
- **Crashes.** When the stage process dies, crash detection finds the RUNNING block run
  without a live job, resets it to INITIAL and counts the crash in its metrics. A block
  run with a crash runs alone, outside any stage; after 3 crashes it is FAILED with "the
  process died while this block ran". This cap applies to every block run: crash restarts
  were unbounded, so a block that ran out of memory restarted forever.
- **Timeouts and cancellation** kill the job found under the block run's id, the stage
  for a member. CANCELLED is written before the kill, as now.

## Running each member

The stage runs each member through `BlockExecutor`, as `PipelineExecutor` does now, so
retries, callbacks, conditionals, statuses and logs behave as they do today.

- **Variables.** Each member gets its own deep copy of the run's variables, as a
  separate process would. `enrich_global_vars` changes the dict it gets; shared, it
  passed remote block outputs, `context` and `retry` to the next member.
- **Code.** Each member's code is read from its file when it starts, as now.
- **Tests** get a shallow copy of the output, so a test that changes it does not change
  the next member's input. The executor passes the outputs to the tests rather than
  reading them back.
- **Retries.** An attempt after the first drops the upstream from memory and reads it
  from storage, so it does not see what the failed attempt changed. Retry delays still
  hold the stage's memory while they wait.
- **Process state.** Between members the stage restores what is cheap to restore:
  environment variables, the working directory, `sys.path`, pandas options, warning
  filters, NumPy error settings and logging configuration. A member that leaves a
  non-daemon thread running ends the stage.
- **Memory.** After each member the stage:
  - clears the block's `exec` globals, `test_functions` and `global_vars`, which hold
    its inputs in a reference cycle;
  - drops the traceback of a caught error;
  - runs `gc.collect()` and returns pyarrow's unused memory.

  The process keeps freed memory, so a stage's RSS is the largest of its members. A
  stage that passes an RSS threshold ends after the current member, and the scheduler
  starts the next stage from storage.
- **Errors reported to Sentry** leave out local variables, which hold DataFrames.

## Later, separately

- **Cheaper writes, for every mode.** Intermediate outputs do not need the analysis
  files and pandas type passes they get today. This saves the write cost without the
  races of writing in a background thread, which this proposal dropped.
- **Polars lazy chains, opt-in.** A chain of blocks that return LazyFrames could pass the
  plan, so Polars optimizes the chain as one query and runs it once at the end of the
  stage. It changes what a run means: the run page could show only the schema, a sample
  would run the plan again, and errors would surface in the last block. It needs its own
  design.
- **R stages.** Each R block starts its own `Rscript`, loads the tidyverse and arrow
  (1–2 s) and exchanges Arrow IPC files with Python. A stage of R blocks would keep one
  R session alive for the chain and pass data frames inside R, with the same claims,
  crash handling and isolation as Python stages. It comes after Python stages work
  in production, with what they teach.
- **Polyglot pipelines.** The cost is at each boundary between languages. Options to
  assess, with stability first:
  - Arrow IPC through shared memory or a pipe between Python and a resident R
    session: no files, near zero-copy, and the two runtimes stay in separate
    processes, so a crash in one does not take the other down.
  - rpy2 (R inside Python) or reticulate (Python inside R): one process, and each
    side reads the other's memory. Faster at the boundary, but a crash or memory leak
    in either runtime takes down both, they share the GIL and signal handlers, and
    package versions of both must agree in one process. To be measured against the
    separate-process option, not assumed faster or safer.
  - DuckDB in-process for SQL blocks, so a SQL block queries an upstream DataFrame
    directly instead of loading it into a database.

  Separate processes are the default to beat: the shared-process options are adopted
  only where measurements show a large gain and tests show they stay stable.
- **Cheaper processes at stage boundaries.** Block processes could start from a
  forkserver that imported Mage once. The Redis client and the database engine created
  at import must go first, and macOS needs its own tests.

## Known limit

An upstream that stored nothing gives the next block no input, which is also what a
block that returned `None` gives. Telling them apart needs a marker for "returned
nothing"; until then a missing output is not an error.

## Tests

- **Chain detection**, with every exclusion above.
- **Flow tests**, in the style of `mage_ai/tests/orchestration/test_flow_execution.py`:
  - statuses after success, failure with and without `allow_blocks_to_fail`, condition
    failure and cancellation;
  - a cancelled run stops at the next claim;
  - a stage killed in its second member resumes there, reading its input from storage;
  - a member that crashes 3 times is FAILED;
  - retries see an unchanged input after a failed attempt changes it in place;
  - a block timeout fails only that member;
  - `block_run_limit` counts a stage once;
  - outputs equal those of the default mode for every type.
- **The soak test with fusion on:** many concurrent pipelines, killed workers, no lost or
  duplicated block runs.
- **The benchmark above (not automated yet).** The numbers in the table come from
  `mage start` runs; an integration test that fails when fused runs stop being faster is
  still to be written.
- **Faults with real processes, run by hand against `mage start`:** a stage killed with
  SIGKILL mid-block resumed and completed; a run cancelled mid-stage killed the stage and
  cancelled the rest; a block that passed its timeout failed and the rest were cancelled.
