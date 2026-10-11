# Data contracts

Status: implemented and tested. The engine is `rust/column_atlas/src/contract.rs` (crate
tests). The Mage side is `mage_ai/data_preparation/contracts.py`, the check in
`Block.run_tests`, the `data_contracts` API, the block settings section, and the CLI
`contract-draft` and `contract-check`. Python tests: `test_contracts.py`, which covers
the notebook, a triggered run where a broken contract stops the exporter, the CLI and
error messages, and `test_data_contract_resource.py`. The user guide is
`docs/guides/blocks/data-contracts.mdx`.

## Review of the starter

`do_not_commit/top_10_next_features/03_data_contracts` is a Rust crate that validates JSON
record batches against a contract, with enforcement modes (off, warn, fail, sample),
semantic versions with a compatibility function, canonical hashes and bounded
diagnostics. Its documents plan a registry service, receipts, Arrow kernels, a
`Data > Contracts` page and a staged rollout. Measured against Mage:

- **Records are the wrong input.** Block outputs are stored as Parquet. Checking JSON
  records would convert every cell. The starter itself names typed Arrow kernels as the
  production path, and it doesn't have them.
- **Mage already has the right engine.** ColumnAtlas runs Polars in Rust over the stored
  Parquet. A contract compiles to Polars expressions there, and one lazy query counts
  every rule over the whole file, with exact dataset-wide uniqueness. The starter only
  checked within a batch and reported `dataset_uniqueness_verified: false`.
- **Mage already has the gate.** Block tests run after the output is stored, and a
  failing test fails the block run, so downstream blocks never start. A contract is a
  declarative test, so it runs in `run_tests`. That covers the notebook, triggers,
  retries, block fusion, and R, SQL and Rust blocks without a new execution path.
- **A registry service is unnecessary.** Contracts are files in the project, versioned and
  reviewed with git. Publication, canonical hashes and receipts duplicate what git
  and the stored report give.
- **Sampling was dropped.** A full check of 10 million rows takes about 0.3 s, so a
  sampled mode, and its wording about partial coverage, isn't needed.
- **Values in diagnostics.** The starter left values out of reports. Here a report lists
  up to 10 distinct violating values, shortened to 80 characters. Without them a failure
  takes a second step to understand. The people who see the report can already see the
  output.

Kept from the starter: missing versus null (`required` versus `nullable`), exact counts
with bounded examples, conservative versioning (a block pins a major version), and strict
parsing: unknown fields are errors, so a typo such as `nulable` isn't silently ignored.

## Design

- **Contract file.** `contracts/<name>.yaml`, holding columns (a mapping, or a list),
  `unique`, `extra_columns`, `min_rows`, `max_rows`, and the informational `version`,
  `owner` and `description`. Python loads the YAML, turns the column mapping into a list
  and YAML dates into ISO text, and the Rust parser checks it (`check_contract`).
- **Binding.** `block.configuration.contract`, either a name or
  `{name, enforcement, output, version}`.
- **Check.** `contracts.check_output` finds the stored Parquet file of the output, the
  same file ColumnAtlas reads. Remote storage and in-memory outputs are written to a
  temporary Parquet file. The Rust `validate_contract` returns the report as JSON. The
  report is saved under `<pipeline variables>/.contract_reports/<partition>/<block>.json`,
  outside the block's variable folder, where Mage would list it as an output.
- **Engine.** Each rule compiles to a boolean expression that is true for a violating row.
  The type check runs on the schema, and a column with the wrong type skips its value
  rules. NaN counts as missing for `nullable` and is excluded from ranges, because Polars
  orders NaN above every number. One `select` sums every rule. Each broken rule then reads
  its first row positions and its distinct values.

## Remaining gaps

1. **Pipeline services** don't check contracts. The service image would need the Rust
   extension (a Polars build) or a Python Polars port of the rules.
2. **Dynamic blocks** pass the child's uuid to the check, but no test covers them yet,
   and the block settings show only the parent's notebook report.
3. **No compatibility check in CI** between two versions of a contract. A block's version
   pin catches a major bump at run time.
4. **No contracts page.** Contracts are edited as files; the block settings pick one and
   draft one.
5. **Nested values** (list and struct contents) have type rules only.
