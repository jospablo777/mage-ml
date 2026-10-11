# Monitoring

Use the Overview page to monitor aggregate pipeline run health across your
project. The monitoring panels show recent run status, failing runs, active
runs, and trigger activity for the selected time range.

<img src="https://mage-ai.github.io/assets/monitoring.jpg" alt="Monitoring" />

## Metrics

Here are the following metrics that are monitored for each pipeline and block:

- Pipeline runs completed successfully
- Pipeline runs failed
- Block runs completed successfully
- Block runs failed
- How long each block ran for

The pipeline's **Monitor** page charts pipeline runs, block runs and block runtime, filtered by trigger.

To check the data a block produces, use [data contracts](../guides/blocks/data-contracts.md)
(rules checked on every row of a block's output) and
[monitors](../guides/pipelines/monitoring.md) (drift, missing values, row counts and freshness
of a block's output across runs).
