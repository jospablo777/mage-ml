# Overview

*The Blocks API is used to create, update, and delete blocks objects from a pipeline.*

### Block Object

**`all_upstream_blocks_executed`** (boolean)

Whether or not all upstream blocks have been successfully executed.

**`color`** (string)

The color of the block in the notebook UI.

**`configuration`** (object)

Miscellaneous configuration settings for the block.

**`data_provider`** (string)

Database or data warehouse for the SQL block to connect to.

**`data_provider_database`** (string)

Database name to use when saving the output of the SQL block.

**`data_provider_profile`** (string)

Profile target for the dbt block.

**`data_provider_schema`** (string)

Schema name to use when saving the output of the SQL block.

**`data_provider_table`** (string)

Table name to use when saving the output of the SQL block.

**`export_write_policy`** (string)

Whether to `replace` the existing table of the SQL block output, `append`, or
raise an error and `fail`.

**`use_raw_sql`** (string)

Toggle writing raw SQL in the block. Read more [here](../../../guides/blocks/sql-blocks.md#using-raw-sql).

**`content`** (string)

Block file contents.

**`downstream_blocks`** (array of strings)

The block UUIDs that depend on this block.

**`executor_config`** (object)

Used when configuring a Kubernetes executor. See the [Kubernetes config](../../../production/configuring-production-settings/compute-resource.md#block-executor) page for more details

**`executor_type`** (string)

The type of executor to use for the block:
`ecs`, `gcp_cloud_run`, `azure_container_instance`, `k8s`, `local_python`, `local_python_force`, `pyspark`

See the [block executor](../../../production/configuring-production-settings/compute-resource.md#block-executor) docs.

**`has_callback`** (boolean)

**`language`** (string)

`python`, `r`, `rust`, `sql`, `yaml`, `markdown`

**`metadata`** (object)

**`name`** (string, required)

Human readable name of block.

**`status`** (string)

Status of block: `executed`, `failed`, `not_executed`, `updated`

**`type`** (string, required)

Type of block:
`callback`, `chart`, `conditional`, `custom`, `data_exporter`, `data_loader`, `dbt`, `extension`, `global_data_product`, `markdown`, `scratchpad`, `sensor`, `transformer`

**`upstream_blocks`** (string)

The block UUIDs that this block depends on.

**`uuid`** (string, required)

Unique identifier for the block.

**Response**

```json
{
    "block": {
        "all_upstream_blocks_executed": true,
        "color": null,
        "configuration": {},
        "content": "some_long_block_content_string",
        "downstream_blocks": [],
        "executor_config": null,
        "executor_type": "local_python",
        "has_callback": false,
        "name": "solitary silence",
        "language": "python",
        "retry_config": null,
        "status": "not_executed",
        "type": "data_loader",
        "upstream_blocks": [],
        "uuid": "solitary_silence",
        "content": "some_long_block_content_string",
        "outputs": []
    }
}
```
