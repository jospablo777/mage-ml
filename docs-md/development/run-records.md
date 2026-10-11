# Run records

Status: implemented (`mage_ai/orchestration/run_records.py`) and tested
(`mage_ai/tests/orchestration/test_run_records.py`). The tests cover a triggered run's
manifest and output digests, comparing two runs, and reproducing a run after its code
changed. API tests: `test_run_record_resource.py`. The user guide is
`docs/guides/pipelines/run-records.mdx`.

## Review of the starter

`do_not_commit/top_10_next_features/04_reproducible_runs` is a Rust crate with manifest
identities over canonical JSON, artifact verification, split-leakage checks and an
eligibility report. Its documents plan a content-addressed artifact namespace, immutable
source snapshots, a lineage graph, retention, and a Reproduce preflight in the UI.
Measured against Mage:

- **Mage had none of the inputs recorded.** A pipeline run stores its variables and
  execution date; nothing records the code, packages or outputs. That's the gap, and it
  doesn't need a new store: the variables storage already holds per-run outputs.
- **Bytes-level verification of datasets** belongs to the storage layer. The run
  record hashes the stored outputs Mage itself writes, which is what a comparison needs.
- **Immutable source snapshots** depend on each source (table versions, object
  versions). A Mage run reads live databases; recording a query doesn't make the
  data reproducible. The guide states this, and the comparison shows when the data
  changed.
- **Split-leakage checks** are specific to training pipelines, and are better expressed
  as data contract rules (feature 03) than as a run gate. Not implemented.
- **The Rust crate is unnecessary.** Recording is hashing files, which `hashlib` does at
  the speed of OpenSSL, and writing JSON. The work is integration with the scheduler and
  storage.

Kept from the starter: a separate record per start (a retry never replaces the first
record), content addressing for deduplication, verification of restored bytes against
their digests, and a reproduction that creates a new linked run instead of rewriting
the old one.

## Design

- **Capture** happens in `PipelineScheduler.start`, after the run is marked RUNNING, for
  every run except fusion verification runs. A failure is logged and doesn't stop the
  run. Files are found by walking the pipeline folder, the block files and the project
  (code suffixes only, skipping dot folders, `pipelines/`, virtualenvs and build
  folders), capped at 5,000 files, 1 MiB each and 64 MiB in total.
- **Storage**: `<variables dir>/.run_records/{runs,code,environments}` through the
  variable manager's storage (tested on local disk; S3 and GCS go through the same
  storage interface, untested). Code and
  environment documents are written once per digest. `pipeline_run.metrics.run_record`
  points to the latest manifest.
- **Output digests**: `BlockExecutor` computes them before `on_complete` and passes them
  as `metrics.outputs`. They hash the data files of each output variable and skip
  derived files (samples, statistics), on local storage only.
- **Variables** in records and comparisons leave out `execution_partition`. Mage adds it
  to every run, and passing it to a reproduction would make the new run write into the
  original's outputs.
- **Reproduce**: the parent restores the snapshot into
  `<tmp>/<project name>/`, checking each file against its digest. It starts
  `mage reproduce-run` there with `MAGE_DATA_DIR` set so the copy resolves the same
  variables folder (secrets keys, outputs and the default SQLite database live there),
  and with the database URL made absolute. The child puts the restored project first on
  `sys.path`, creates a run on an inactive trigger with the original's variables and
  execution date, and runs it in process with the fusion verifier's job runner. The
  parent compares outputs with `fusion_verify.compare_runs`. The new run's own record
  must have the original's code digest (`code_restored`).

## Remaining gaps

1. **Packages are not restored.** A reproduction lists package differences; restoring
   them would need a uv environment built from the recorded versions (feature 02 builds
   only from a requirements file).
2. **Output digests are local-only.** Remote storage records no output digests, so
   comparisons show those outputs as not recorded.
3. **Variables folders named unlike the project** (or remote ones) can't be reproduced
   into, because the copy must resolve the same folder.
4. **Retention**: records are never deleted. Code and environment documents are shared,
   so deleting them needs reference counting.
5. **Project platform** (multi-project) pipelines are not recorded by its scheduler.
