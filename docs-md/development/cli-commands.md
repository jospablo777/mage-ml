# CLI Commands Reference

*Reference for the MageML command-line interface.*

MageML provides a command-line interface (CLI), the `mage` command, for managing projects, running pipelines, and performing administrative tasks. The CLI is built with [Typer](https://typer.tiangolo.com/).

In a uv project, run the commands with `uv run mage ...` from the project folder; after `uv tool install` or with an activated environment, run `mage ...`. In a source checkout, use `uv run --no-sync mage ...` from the repository folder.

## Quick Reference

| Command | Description |
|---------|-------------|
| [`init`](#init-initialize-a-project) | Initialize a new MageML project at the specified path |
| [`start`](#start-start-the-server-and-ui) | Start the MageML server and web UI |
| [`run`](#run-run-a-pipeline) | Execute a pipeline or a specific block within a pipeline |
| [`clean-cached-variables`](#clean-cached-variables) | Delete stored block outputs older than the retention period or `--older-than` |
| [`clean-old-logs`](#clean-old-logs) | Delete pipeline run logs older than the logging retention period |
| [`create-spark-cluster`](#create-spark-cluster) | Create an EMR cluster from the project's EMR config |
| [`contract-draft`](#contract-draft) | Draft a data contract from a block's stored output |
| [`contract-check`](#contract-check) | Check a block's stored output against its data contract |
| [`contract-compat`](#contract-compat) | Compare a contract with its version at a git revision |
| [`run-diff`](#run-diff) | Compare what two runs of a pipeline ran with |
| [`reproduce`](#reproduce) | Run a pipeline run again with its recorded code and compare the outputs |
| [`release-status`](#release-status) | Show a model's release policy, alias holder and release log |
| [`release-promote`](#release-promote) | Evaluate a model version and move the release alias to it |
| [`release-rollback`](#release-rollback) | Give the release alias back to the previous version |
| [`monitor-status`](#monitor-status) | Show a monitor's checks for the latest pipeline runs |
| [`monitor-silence`](#monitor-silence) | Silence a monitor's notifications for a while |
| [`verify-fusion`](#verify-fusion) | Run a pipeline block by block and fused, and compare the outputs |
| [`r setup`, `r init`, `r sync`, `r status`](#r-blocks) | Manage the rv environment that R blocks run in |
| [`rust init`, `rust build`, `rust status`](#rust-blocks) | Manage the Cargo workspace that Rust blocks build in |
| [`export service`](#export-service) | Export pipelines as a standalone Docker service |

### Getting Help

To see all available commands:
```bash
mage --help
```

To get help for a specific command:
```bash
mage <command> --help
```

---

## Project Management

#### `init` - Initialize a project

Initialize a new MageML project at the specified path.

**Usage:**
```bash
mage init <project_path> [OPTIONS]
```

**Parameters:**
- `project_path` (required): Path of the MageML project to be created
- `--project-type` (optional): Type of project to create. Options: `main`, `sub`, or `standalone` (default: `standalone`)
- `--cluster-type` (optional): Type of instance to create for workspace management
- `--project-uuid` (optional): Project UUID for the new project

**Example:**
```bash
mage init my_project --project-type standalone
```

---

#### `start` - Start the server and UI

Start the MageML server and web UI.

**Usage:**
```bash
mage start [project_path] [OPTIONS]
```

**Parameters:**
- `project_path` (optional): Path of the MageML project to be loaded (default: current directory)
- `--host` (optional): Specify the host (default: `localhost`)
- `--port` (optional): Specify the port (default: `6789`)
- `--manage-instance` (optional): Enable manage instance (default: `0`)
- `--dbt-docs-instance` (optional): Enable dbt docs instance (default: `0`)
- `--instance-type` (optional): `server_and_scheduler`, `web_server` or `scheduler` (default: `server_and_scheduler`). The `INSTANCE_TYPE` environment variable overrides it.
- `--project-type` (optional): Create project of this type if it does not exist. Options: `main`, `sub`, or `standalone` (default: `standalone`)
- `--cluster-type` (optional): Type of instance to create for workspace management
- `--project-uuid` (optional): Set project UUID for the repo that is being started

**Example:**
```bash
mage start . --host 0.0.0.0 --port 6789
```

---

## Pipeline Execution

#### `run` - Run a pipeline

Execute a pipeline or a specific block within a pipeline.

**Usage:**
```bash
mage run <project_path> <pipeline_uuid> [OPTIONS]
```

**Parameters:**
- `project_path` (required): Path of the MageML project that contains the pipeline
- `pipeline_uuid` (required): UUID of the pipeline to be run
- `--test` (optional): Run the blocks' tests (default: `False`). Data contracts are checked either way; a broken contract stops the run with exit code 1
- `--block-uuid` (optional): UUID of the block to be run (if not specified, runs entire pipeline)
- `--execution-partition` (optional): Where the run keeps its block outputs, so runs at the same time do not share them
- `--executor-type` (optional): Executor type to use
- `--callback-url` (optional): Callback URL for block execution
- `--block-run-id` (optional): Block run ID
- `--pipeline-run-id` (optional): Pipeline run ID
- `--runtime-vars` (optional): Runtime variables in JSON format. These will overwrite the pipeline global variables
- `--var` (optional, repeatable): One runtime variable as `key=value`; values that read as JSON keep their type (`2`, `true`, `[1, 2]`), others are text. Overrides `--runtime-vars`
- `--skip-sensors` (optional): Specify if the sensors should be skipped (default: `False`)
- `--template-runtime-configuration` (optional): Runtime configuration of data integration block runs (JSON format)
- `--clean-after-success` (optional): Remove the outputs the run stored once it succeeds; a failed run keeps them for debugging
- `--record/--no-record` (default: record): When the run ends, record it as a pipeline run of the pipeline's "Command line" trigger, with its block runs and [run record](../guides/pipelines/run-records.md), so `mage run-diff` and `mage reproduce` can use it
- `--max-parallel` (optional): Most stages that run at once (default: the pipeline's `concurrency_config.block_run_limit`, else no limit)

With block fusion (the default), `mage run` runs the pipeline in stages: each chain of blocks
runs as one stage, and a stage starts as soon as the blocks it waits for are done. Stages
run in threads of one process, so blocks that change process-wide settings (environment
variables, an MLflow tracking URI) or need every core should not run beside others: set
`exclusive: true` in such a block's `configuration`, and its stage starts when no other
stage runs and none starts beside it; `--max-parallel 1` or the pipeline's
`block_run_limit` runs every stage alone. No new stage starts while the
process holds more memory than `MAGE_STAGE_MAX_MEMORY_SHARE` (0.5) of the machine and
another stage runs. When a stage fails, no other stage starts; the run waits for the stages
already running and fails with the first error, noting the others.

The run ends with each block's time and tests, such as `2/2 passed`, or `2 not run` with a
reminder to pass `--test`: data contracts are always checked, block tests only with
`--test`.

**Examples:**
```bash
# Run entire pipeline
mage run /path/to/project my_pipeline_uuid

# Run specific block
mage run /path/to/project my_pipeline_uuid --block-uuid my_block_uuid

# Run with runtime variables
mage run /path/to/project my_pipeline_uuid --runtime-vars '{"key": "value"}'
mage run /path/to/project my_pipeline_uuid --var date=2026-01-31 --var limit=1000

# Run with tests
mage run /path/to/project my_pipeline_uuid --test

# One stage at a time, outputs under their own partition
mage run /path/to/project my_pipeline_uuid --max-parallel 1 --execution-partition run_42
```

---

## Maintenance Commands

#### `clean-cached-variables`

Delete stored block outputs older than `variables_retention_period` in the project's
`metadata.yaml`, or `--older-than`. See [Variable retention](../design/data-pipeline-management.md#variable-retention).
It covers scheduled runs' partitions and the outputs of editor and `mage run` runs (per
block, or per `--execution-partition`), whose age is their newest file's. Block outputs
hold the data blocks read, personal data included; they stay under `MAGE_DATA_DIR`
until removed.

**Usage:**
```bash
mage clean-cached-variables <project_path> [OPTIONS]
```

**Parameters:**
- `project_path` (required): Path of the MageML project to clean variables
- `--pipeline-uuid` (optional): UUID of the pipeline to clean (if not specified, cleans all pipelines)
- `--older-than` (optional): Remove outputs older than this, such as `12h`, `7d` or `2w`; `0d` removes all (default: the retention period)
- `--dry-run` (optional): List what would be removed, with sizes, and remove nothing

Without a retention period or `--older-than`, nothing is removed and the command says how
much the outputs hold. It exits with 1 when a folder could not be removed.

**Example:**
```bash
mage clean-cached-variables /path/to/project --older-than 7d --dry-run
```

---

#### `clean-old-logs`

Delete pipeline run logs older than `retention_period` in the project's `logging_config`.
See [Set log retention period](../observability/logging.md#set-log-retention-period).

**Usage:**
```bash
mage clean-old-logs <project_path> [OPTIONS]
```

**Parameters:**
- `project_path` (required): Path of the MageML project to clean old logs
- `--pipeline-uuid` (optional): UUID of the pipeline to clean (if not specified, cleans all pipelines)

**Example:**
```bash
mage clean-old-logs /path/to/project
```

---

#### `create-spark-cluster`

Create an EMR cluster from the EMR config of the project.

**Usage:**
```bash
mage create-spark-cluster <project_path>
```

---

## Data contracts

See [Data contracts](../guides/blocks/data-contracts.md).

#### `contract-draft`

Draft a data contract from a block's stored output (run the block first): its columns, types and which columns have no missing values.

**Usage:**
```bash
mage contract-draft <project_path> <pipeline_uuid> <block_uuid> [OPTIONS]
```

**Parameters:**
- `--name` (optional): Contract name; the block UUID by default
- `--write` (optional): Save it as `contracts/<name>.yaml` instead of printing it
- `--force` (optional): Replace an existing contract file

#### `contract-check`

Check a block's stored output against its data contract. Exits with 1 when the output breaks it, 2 when the check cannot run.

**Usage:**
```bash
mage contract-check <project_path> <pipeline_uuid> <block_uuid> [--partition <execution_partition>]
```

#### `contract-compat`

Compare a contract with its version at a git revision: each change, marked breaking when it weakens what readers of the data rely on, and whether the version number fits. Exits with 1 when a breaking change keeps the major version or a change keeps the version, 2 when the comparison cannot run.

**Usage:**
```bash
mage contract-compat <project_path> <name> [--base <git_revision>]
```

**Options:**
- `--base` (optional): The revision to compare with (default: `HEAD`, the last commit); `origin/master` in a pull request's CI

---

## Run records

See [Run records](../guides/pipelines/run-records.md).

#### `run-diff`

Compare what two runs of a pipeline ran with: code (with a diff of each changed file), Python, MageML and package versions, variables, and which blocks' outputs differ.

**Usage:**
```bash
mage run-diff <project_path> <pipeline_uuid> <first_run_id> <second_run_id> [--json]
```

#### `reproduce`

Run a pipeline run again with the code it ran with (restored from its run record), its variables and its execution date, then compare every block's outputs with the original run's. The pipeline runs in full, so exporters write again.

**Usage:**
```bash
mage reproduce <project_path> <pipeline_uuid> <run_id> [OPTIONS]
```

**Parameters:**
- `--yes`, `-y` (optional): Run without asking first
- `--report` (optional): Also write the result as JSON to this path
- `--keep` (optional): Keep the restored copy of the project

---

## Model releases

See [Model releases](../guides/pipelines/model-releases.md).

#### `release-status`

Show a model's release policy, the version holding its alias and its release log. With `--version`, evaluate that version against the policy and the current champion.

```bash
mage release-status <project_path> <model> [--version <version>]
```

#### `release-promote`

Evaluate a version against the model's release policy and, if it passes, move the policy's alias to it. `--force` promotes even if the check does not pass.

```bash
mage release-promote <project_path> <model> <version> [--force]
```

#### `release-rollback`

Give the model's alias back to the version that held it before the last promotion.

```bash
mage release-rollback <project_path> <model>
```

---

## Monitors

#### `monitor-status`

Show a monitor's checks for the latest pipeline runs it watched. See [Monitoring](../guides/pipelines/monitoring.md).

```bash
mage monitor-status <project_path> <monitor> [--runs 10]
```

---

#### `monitor-silence`

Silence a monitor's notifications for a while, a known failure being worked on. Its checks still run and `fail_block` still fails the block.

**Usage:**
```bash
mage monitor-silence <project_path> <monitor> --hours 6 [--reason "..."]
mage monitor-silence <project_path> <monitor> --clear
```

## Block fusion

#### `verify-fusion`

Run a pipeline block by block, then with block fusion, and compare every block's stored outputs. Names the first block whose output differs. Both runs execute the whole pipeline, so exporters write twice. See [Verify fusion](../design/data-pipeline-management.md#verify-fusion).

**Usage:**
```bash
mage verify-fusion <project_path> <pipeline_uuid> [OPTIONS]
```

**Parameters:**
- `--runtime-vars` (optional): Runtime variables in JSON format
- `--yes`, `-y` (optional): Run without asking first
- `--report` (optional): Also write the result as JSON to this path
- `--exact` (optional): Count floats that differ only by rounding as a difference

---

## R blocks

Manage the rv environment that R blocks run in. The project path defaults to the current directory. See [R blocks](../guides/blocks/r-blocks.md).

- `mage r setup [project_path] [--r-version 4.6]`: check the tools R blocks need (R, rv and rig) and print how to install the missing ones.
- `mage r init [project_path] [--package <name>] [--r-version 4.6] [--install-r]`: create the project's R environment in `<project>/r` with rv and install its packages (the tidyverse by default).
- `mage r sync [project_path]`: install the packages of the R environment, as `rv.lock` pins them.
- `mage r status [project_path]`: check that R blocks can run.

## Rust blocks

Manage the Cargo workspace that Rust blocks build in. The project path defaults to the current directory. See [Rust blocks](../guides/blocks/rust-blocks.md).

- `mage rust init [project_path]`: create the project's Rust environment in `<project>/rust`.
- `mage rust build [project_path]`: build every Rust block of the project, so runs start without compiling. Use it when deploying.
- `mage rust status [project_path]`: check that Rust blocks can build (cargo, rustc, the workspace and its crates).

---

## Pipeline services

#### `export service`

Export pipelines as a standalone Docker service: an HTTP API, schedules, run history and logs, without MageML. Writes a build context with a Dockerfile, `compose.yaml` and a README. See [Pipeline services](../production/pipeline-services.md).

**Usage:**
```bash
mage export service <project_path> <pipeline_uuid>... --out <directory> [OPTIONS]
```

**Parameters:**
- `--out`, `-o` (required): Directory for the Docker build context
- `--name` (optional): Service name; defaults to the pipeline
- `--max-runs` (optional): Runs of each pipeline at once; defaults to the pipeline setting or 1
- `--build` (optional): Also build the Docker image
- `--tag` (optional): Image tag for `--build`; defaults to the name
- `--force` (optional): Replace a non-empty output directory
- `--model`, `-m` (optional): An MLflow model to embed, `NAME=URI`; repeat for more

The folder holds `pyproject.toml` and `uv.lock` (the image installs exactly the lock) and
`manifest.json` (versions, lock and model digests, and the SHA-256 of every file).

#### `export compare`

What changed between two exports of a service, for a change review: MageML, Python and uv,
the packages of `uv.lock`, the models and the files.

**Usage:**
```bash
mage export compare <old_folder> <new_folder>
```

```text
Python packages (1):
  ~ pandas 3.0.5 -> 3.0.6
Files (1):
  ~ shop/transformers/clean_orders.py
```

---

## Common Use Cases

### Initializing a New Project
```bash
mage init my_data_project --project-type standalone
mage start my_data_project
```

### Running a Pipeline with Variables
```bash
mage run /path/to/project my_pipeline_uuid \
  --runtime-vars '{"date": "2024-01-01", "environment": "production"}'
```

### Cleaning Up Old Data
```bash
# Delete variables older than variables_retention_period in metadata.yaml
mage clean-cached-variables /path/to/project

# Delete logs older than logging_config.retention_period
mage clean-old-logs /path/to/project
```

---

## Environment Variables

- `INSTANCE_TYPE`: instance type override for the `start` command

---

## Additional Resources

- [MageML CLI source](https://github.com/jospablo777/mage-ml/blob/master/mage_ai/cli/main.py)
- [Project Setup Guide](project/setup.md) - Learn how to set up a MageML project

---

## Notes

- All paths can be absolute or relative to the current working directory
- JSON parameters should be properly escaped when used in shell commands
- Some commands may require database connections to be properly configured
