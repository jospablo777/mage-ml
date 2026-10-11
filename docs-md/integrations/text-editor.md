# Using an external IDE

*Develop pipelines in your own IDE by editing the project files.*

## Create a new pipeline

Each pipeline is represented by a YAML file in a folder named `pipelines/` under the
project directory.

For example, if your project is named `demo_project` and your pipeline is named `etl_demo` then
you’ll have a folder structure that looks like this:

```
demo_project/
|   pipelines/
|   |   etl_demo/
|   |   |   __init__.py
|   |   |   metadata.yaml
```

Create a new folder in the `demo_project/pipelines/` directory. Name this new folder after the name
of your pipeline.

Add 2 files in this new folder:

1. `__init__.py`
1. `metadata.yaml`

In the `metadata.yaml` file, add the following content:

```yaml
blocks: []
name: etl_demo
type: python
uuid: etl_demo
```

> [!NOTE]
> Change `etl_demo` to whatever name you’re using for your new pipeline.

## Sample pipeline metadata content

This sample pipeline `metadata.yaml` will produce the following block dependencies:

<img src="https://mage-ai.github.io/assets/data-pipeline-overview.png" alt="Sample pipeline" />

```yaml
blocks:
- downstream_blocks:
    - select_columns
  executor_type: local_python
  language: python
  name: load_data_from_file
  type: data_loader
  upstream_blocks: []
  uuid: load_data_from_file
- downstream_blocks:
    - export_to_file
  executor_type: local_python
  language: python
  name: select_columns
  type: transformer
  upstream_blocks:
    - load_data_from_file
  uuid: select_columns
- downstream_blocks: []
  executor_type: local_python
  language: python
  name: export_to_file
  type: data_exporter
  upstream_blocks:
    - select_columns
  uuid: export_to_file
name: etl_demo
type: python
uuid: etl_demo
```

## `metadata.yaml` sections

### Pipeline attributes

**`blocks`** (array of objects)

An array of blocks that are in the pipeline.

**`name`** (string)

Unique name of the pipeline.

**`type`** (string enum)

The type of pipeline. Currently available options are:
- `databricks`
- `integration`
- `pyspark`
- `python` (most common)
- `streaming`

**`uuid`** (string)

Unique identifier of the pipeline. This UUID must be unique across all pipelines.

**`description`** (string)

Optional description of what the pipeline does.

**`executor_type`** (string enum)

Pipeline level executor type. Supported values:
- `ecs`
- `gcp_cloud_run`
- `azure_container_instance`
- `k8s`
- `local_python` (most common)
- `pyspark`

**`block_fusion`** (string)

Set to `'off'` to run production runs block by block instead of running chains of blocks
in one process. See [block fusion](../design/data-pipeline-management.md#block-fusion).

**`keep_block_outputs`** (boolean)

If true, every block of a compiled chain computes and stores its own output. See
[compiled chains](../design/data-pipeline-management.md#compiled-chains-of-rust-and-r-blocks).

**`environment`** (object)

The pipeline's own Python environment. See
[pipeline environments](../guides/pipelines/pipeline-environments.md).

**`executor_count`** (integer)

Number of concurrent executors to run the pipeline. Used in streaming pipeline.

**`executor_config`** (object)

Optional configuration specific to the selected executor type.
Refer to the following documentation for executor-specific options:

- [`ecs`](../production/executors/aws.md)
- [`gcp_cloud_run`](../production/executors/gcp.md)
- [`azure_container_instance`](../production/executors/azure.md)
- [`k8s`](../production/executors/k8.md)

**`spark_config`** (object)

Spark-specific configuration for PySpark pipelines. Mirrors the keys you
would normally pass to `SparkConf` (e.g., `spark_master`, `executor_env`,
`spark_jars`).

**`retry_config`** (object)

Retry configuration at the pipeline level.
See [documentation](../orchestration/pipeline-runs/retrying-block-runs.md#3-pipeline-level-retry-config) for details.

**`notification_config`** (object)

Configuration for pipeline notification messages (e.g., on failure or success).
See [documentation](../observability/alerting/alerting-slack.md) for details.

**`concurrency_config`** (object)

Concurrency settings for block execution within the pipeline.
See [documentation](../design/data-pipeline-management.md#pipeline-level-concurrency) for details.
- `block_run_limit`: Maximum number of blocks that can run in parallel.
- `pipeline_run_limit`
- `pipeline_run_limit_all_triggers`
- `on_pipeline_run_limit_reached`

**`cache_block_output_in_memory`** (boolean)

Whether to cache block output in memory during execution.

**`run_pipeline_in_one_process`** (boolean)

If true, runs all blocks in a single process or k8s pod.

**`callbacks`** (array of objects)

Blocks that run after your main graph finishes (e.g., notifications, cleanup).
Same shape as items in `blocks`.

**`conditionals`** (array of objects)

Conditional blocks that can short-circuit or branch execution. Same shape as
items in `blocks`.

**`extensions`** (array of objects)

Extension blocks (e.g., Great Expectations). Same shape as items in `blocks`.

**`widgets`** (array of objects)

Chart or reporting blocks shown in the pipeline UI. Same shape as items in
`blocks`.

**`settings`** (object)

Pipeline-level settings; currently supports `triggers.save_in_code_automatically`
to control whether new/updated triggers are persisted to YAML.

**`variables`** (object)

Key/value pairs available to the pipeline for templating. Useful for
environment-specific values that are not secrets.

**`tags`** (array of strings)

Free-form labels to organize and search for pipelines. Each tag name can contain up to 70
characters.

### Block attributes

**`downstream_blocks`** (array of strings)

An array of block UUIDs that depend on this current block.
These downstream blocks will have access to this current block’s data output.

**`executor_type`** (string enum)

The method for running this block of code. Currently available options are:
- `ecs`
- `gcp_cloud_run`
- `azure_container_instance`
- `k8s`
- `local_python` (most common)
- `pyspark`

**`executor_config`** (object)

Optional configuration specific to the selected executor type.
Refer to the following documentation for executor-specific options:

- [`ecs`](../production/executors/aws.md)
- [`gcp_cloud_run`](../production/executors/gcp.md)
- [`azure_container_instance`](../production/executors/azure.md)
- [`k8s`](../production/executors/k8.md)

**`language`** (string enum)

Programming language used by the block. Supported values:
- `markdown`
- `python` (most common)
- `r` (see [R blocks](../guides/blocks/r-blocks.md))
- `rust` (see [Rust blocks](../guides/blocks/rust-blocks.md))
- `sql`
- `yaml`

**`name`** (string)

Unique name of the block.

**`type`** (string enum)

The type of block. Currently available options are:
- `chart`
- `custom` (most common)
- `data_exporter`
- `data_loader`
- `dbt`
- `scratchpad`
- `sensor`
- `transformer`

The type of block will determine which folder it needs to be in. For example, if the block `type`
is `data_loader`, then the file must be in the `[project_name]/data_loaders/` folder. It can be
nested in any number of subfolders.

**`upstream_blocks`** (array of strings)

An array of block UUIDs that this current block depends on.
These upstream blocks will pass its data output to this current block.

**`uuid`** (string)

Unique identifier of the block. This UUID must be unique within the current pipeline. The UUID
corresponds to the name of the file for this block.

For example, if the UUID is `load_data` and the `language` is `python`, then the file name
will be `load_data.py`.

**`color`** (string enum)

Optional color used to visually organize blocks in the UI: `blue`, `grey`, `pink`,
`purple`, `teal` or `yellow`.

**`configuration`** (object)

Block-specific configuration. For integrations or dbt blocks this mirrors the
configuration visible in the block settings panel. `fusion: false` makes the block run
alone, outside [block fusion](../design/data-pipeline-management.md#block-fusion).

**`retry_config`** (object)

Retry configuration at the block level.
See [documentation](../orchestration/pipeline-runs/retrying-block-runs.md#2-block-level-retry-config) for details.

**`tags`** (array of strings)

Tags MageML derives from the block's settings, such as `dynamic`, `dynamic_child`,
`reduce_output` and `replica`. They are written by MageML, not set by hand.

**`timeout`** (integer)

Maximum execution time for the block in seconds.
