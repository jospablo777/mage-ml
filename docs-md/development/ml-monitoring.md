# Monitoring

Status: implemented. The profile engine is `rust/column_atlas/src/profile.rs` (crate
tests). The monitors are `mage_ai/orchestration/monitors.py`, checked in
`BlockExecutor._output_record` and shown on the run page and by `mage monitor-status`.
Tests: `mage_ai/tests/orchestration/test_monitors.py`, which covers the divergence and
check semantics, and runs through the scheduler with a Slack webhook served by the test.
The user guide is `docs/guides/pipelines/monitoring.mdx`.

## Review of the starter

`do_not_commit/top_10_next_features/09_ml_monitoring` is a Rust crate for streaming
prediction monitoring: event-time windows with lateness and deduplication, histograms
with fixed edges, Jensen–Shannon divergence, partition-watermark freshness, and an alert
ledger with cooldowns. Measured against Mage:

- **Mage runs batches.** The unit of work is a block run whose whole output is stored.
  Windows, lateness and event deduplication serve a stream of prediction events, which
  Mage pipelines don't produce. The natural monitor compares one run's output with a
  baseline run's.
- **Kept:** histograms with the baseline's edges plus underflow and overflow bins;
  missing values counted apart from the distribution; Jensen–Shannon divergence with
  natural logs and no pseudocounts; hold when data is insufficient, never pass.
- **Alerts** go through Mage's notification settings (Slack, email, Teams and so on)
  instead of a separate ledger. A failed monitor alerts once per run.
- **Delayed labels and performance estimation** belong to the training pipeline. Feature
  07's release checks gate models on measured metrics.

## Design

- **Profile (Rust).** One Polars pass computes rows, missing counts (null and NaN),
  quantiles and temporal ranges. A second pass counts every numeric bin. Text and
  boolean columns get one group-by each for their most frequent values.
- **Baseline.** Either the previous run's saved record or a given run's. Its numeric
  edges are passed to the current profile, so both are binned the same way. A run with
  no baseline holds its drift checks and becomes the baseline of the next run.
- **Records** are written per monitor and pipeline run through the variable manager's
  storage. The block run keeps the record without its profile.
- **Failure.** A monitor that can't run (a missing column, an output that isn't a
  DataFrame) records `error` and logs a warning; it doesn't fail the block.

## Evidently and NannyML

Both resolve and run with this fork's pins (pandas 3.0.6, NumPy 2.5, pyarrow 26, Python
3.12), checked in a scratch environment: Evidently 0.7.23 presets with tests, NannyML
0.13.1 CBPE, univariate and data reconstruction drift. They are heavy (Plotly, many
pinned packages), so they never enter Mage's environment: `monitor_engines.py` is a
standalone script run with `python -I` by an environment `environments.environment_for`
builds. `-I` matters: run from `mage_ai/orchestration`, the script's folder would put
Mage's `queue` package in front of the standard library's. Results come back as checks,
with a severity per engine, and HTML reports listed in the run's record. The
`monitor_reports` API serves only reports a record lists.

## Remaining gaps

1. **No streaming or event windows**, as above.
2. **Seasonality** by run count only: `lag: 7` compares a daily pipeline's run with the
   one seven runs back. Calendar selectors ("the same weekday", skipped or extra runs
   accounted for) are not implemented.
3. **Alerts** can be limited to changes (`notify: on_change`), but there is no
   acknowledgement or silencing with an expiry.
4. **Segments** cover one column's most frequent values (`segments`), with the
   missing-value and drift checks only; rows, freshness and performance are checked on the
   whole output. NannyML covers multivariate drift.
5. **Engines on remote storage**: Evidently and NannyML read the stored outputs from
   local disk.
