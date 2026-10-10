# Experiments and the model registry

Status: MLflow runs started by blocks are tagged with the Mage run and recorded on the
block run (`mage_ai/orchestration/experiments.py`, `mlflow_context.py`). Tested in
`mage_ai/tests/orchestration/test_experiments.py` against a SQLite MLflow store with a
registered model. The user guide is `docs/guides/pipelines/experiments.mdx`.

## Review of the starter

`do_not_commit/top_10_next_features/06_experiment_registry` is a Rust crate that prepares
MLflow REST requests to create model versions, with an outbox (prepared, dispatching,
unknown), reconciliation of ambiguous registrations, bounded metric batches, and a
`Models > Experiments / Registry` UI. Measured against Mage:

- **Blocks already talk to MLflow.** An ML engineer's training block calls
  `mlflow.start_run`, `log_metric` and `log_model(..., registered_model_name=...)`
  directly. Putting a Rust service in between would duplicate the MLflow client and
  force blocks to change.
- **The missing piece is the link.** Nothing told MLflow which Mage run produced a run
  or a model version, and nothing in Mage showed the MLflow runs of a pipeline run.
  MLflow has a supported extension point for exactly this: run context providers add
  tags to every run started in a context.
- **Registration outcomes belong to the registry.** Model versions are created by the
  block's own MLflow call, so Mage has no ambiguous request to reconcile. It reads what
  the block registered (`search_model_versions(run_id=...)`).

So the implementation tags and records; it doesn't proxy.

## Design

- **Context.** `BlockExecutor` wraps the block's execution in
  `experiments.block_context(...)`. That sets a context variable and the
  `MAGE_TRACKING_TAGS` environment variable (for threads and child processes), holding
  the project, pipeline, pipeline run, block, block run, execution partition and the run
  record's code digest.
- **Provider.** `MlflowRunContext` returns those tags while a context is active. It
  reaches MLflow two ways: the package entry point (`mlflow.run_context_provider` in
  `pyproject.toml`) and an import hook that registers it when
  `mlflow.tracking.context.registry` loads. The hook is for installs without the entry
  point, such as an editable install made before it existed, and for blocks that import
  MLflow themselves. Registration is idempotent.
- **Recording.** In `_output_record`, when `mlflow` is in `sys.modules`, the executor
  searches all experiments for `tags.mage.block_run_id = <id>` (up to 50 runs) and stores
  their summaries, with up to 100 metrics each, in `block_run.metrics['mlflow']`. The
  tracking URI is stored without its password.
- **Comparison.** `run_records.compare` adds, per block, the metrics that differ between
  the two pipeline runs. With several MLflow runs in a block, metric names are prefixed
  with the run name.

## Remaining gaps

1. **Pipeline environments.** Blocks that run in a pipeline environment use the
   environment's MLflow in another process. The tags variable reaches it, but the
   provider isn't registered there, and the parent doesn't search.
2. **No registry page.** Mage shows each run's registered versions; browsing models
   across runs happens in MLflow, where the `mage.*` tags link back.
3. **Other trackers** (Weights & Biases, Neptune) aren't linked.
4. **Metrics history.** Only each metric's latest value is recorded.
