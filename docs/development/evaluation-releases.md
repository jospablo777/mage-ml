# Evaluation and releases

Status: implemented in `mage_ai/orchestration/releases.py`, with the check in
`BlockExecutor`, the `model_releases` API, a section on the pipeline run page and the
`release-*` CLI commands. Tests: `mage_ai/tests/orchestration/test_releases.py` (rule
semantics, policy errors, and automatic and manual promotion through real pipeline runs
with an MLflow registry) and `test_model_release_resource.py`. The user guide is
`docs/guides/pipelines/model-releases.mdx`.

## Review of the starter

`do_not_commit/top_10_next_features/07_evaluation_releases` is a Rust crate with evidence
records (model, dataset, evaluator and environment digests; observed and eligible sample
counts), policies of up to 128 metric rules, pass, hold and fail decisions, and a release
state machine (approved, prepared, dispatched, unknown, active, rollback) that changes
only on a deployment receipt. Measured against Mage:

- **The release target exists: MLflow aliases.** MLflow's registry moves an alias
  (`champion`) between versions atomically, and consumers load
  `models:/name@champion`. Mage's own services embed models at export. A deployment
  provider with dispatch and reconciliation states would be needed only for pushing to
  serving infrastructure, which Mage doesn't own.
- **The decision rules carry over.** Missing or non-finite metrics hold, and a known
  failure wins over a hold. Regressions compare with the champion, and approvals bind
  the champion they were evaluated against, so a stale approval can't promote. All of
  these are kept.
- **Evidence identity comes from earlier features.** The version's MLflow run carries the
  Mage run's tags (feature 06), including the run record's code digest (feature 04). Data
  contracts (feature 03) check the evaluation data before the training block runs.
- **Sample counts and coverage** are expressed as ordinary metric rules (`eval_rows` with
  a `min`). The block logs the counts it evaluated on.

Point-estimate gates only, as in the starter: no confidence intervals or significance
tests.

## Design

- **Trigger.** After a block executes, `BlockExecutor` records its MLflow runs (feature
  06). For each registered version whose model has a policy, it evaluates the version's
  run metrics against the policy and the champion's run metrics. With automatic
  approval it promotes passing versions. The evaluations go to
  `block_run.metrics['releases']` and the block run's logs. A failed check with
  `fail_block: true` raises in the executor, so the block run fails through the normal
  failure path, and the retry policy doesn't apply to it.
- **Promotion** reads the current alias holder, compares it with the expected champion,
  sets the alias and appends to the release log. Two promotions racing between the read
  and the write can both pass the check; MLflow has no conditional alias update.
- **Rollback** finds the log entry that made the current champion and promotes its
  `previous` version through the same check.
- **Log** is JSON lines in `<variables dir>/.releases/<model>.jsonl`.

## Remaining gaps

1. **Race between promotions.** The champion check and the alias update aren't one
   atomic step in MLflow.
2. **Statistical gates** (confidence intervals, significance) and per-slice rules with
   their own denominators are not implemented.
3. **Approvals are not separated from authorship.** Any editor can approve, including
   the author of the change.
4. **Serving targets** other than MLflow aliases (pushing a new image, a canary) are left
   to deployment tooling.
