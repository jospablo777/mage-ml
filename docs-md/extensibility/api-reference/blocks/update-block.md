# Update block

`PUT /api/pipelines/:pipeline_uuid/blocks/:block_uuid`

**`name`** (string, required)

Human readable block name. This is used to generate the UUID, e.g. `restless dust`. Names will be slugified to a safe-uuid.

**`type`** (string, required)

Type of block:
`callback`, `chart`, `conditional`, `custom`, `data_exporter`, `data_loader`, `dbt`, `extension`, `global_data_product`, `markdown`, `scratchpad`, `sensor`, `transformer`

**`bookmark_values`** ([string])

Data integration bookmark values as a list of strings.

**`callback_blocks`** ([string])

Array of [callback](../../../design/blocks/callbacks.md) block objects. Same shape as `blocks`.

**`color`** (string)

Block color displayed by the frontend for `custom` blocks. All other block types have predefined colors.

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

The content of block formatted as a string. Use `\n` for newlines. Omitting will create an empty block.

**`destination_table`** (string)

The destination table for a data integration pipeline.

**`downstream_blocks`** ([string])

Array of blocks downstream from the created block.

**`executor_config`** (object)

Used when configuring a Kubernetes executor. See the [Kubernetes config](../../../production/configuring-production-settings/compute-resource.md#block-executor) page for more details

**`executor_type`** (string)

The executor that runs the block: `ecs`, `gcp_cloud_run`, `azure_container_instance`, `k8s`, `local_python`, `local_python_force`, `pyspark`. See the [block executor](../../../production/configuring-production-settings/compute-resource.md#block-executor) docs.

**`extension_uuid`** (string)

UUID of extensions, currently Great Expectations is the only extension supported.

**`language`** (string)

The block language:
`python`, `sql`, `r`, `rust`, `yaml`, `markdown`

**`pipelines`** ([string])

Array of strings containing the pipeline uuids for which the block is a member.

**`retry_config`** (object)

The configuration for block retry logic. See the [pipeline runs](../../../orchestration/pipeline-runs/retrying-block-runs.md) docs for retry config examples.

**`tap_stream_id`** (string)

The block tap stream ID for data integration pipelines.

**`upstream_blocks`** ([string])

An array of strings representing the uuids of upstream blocks (the blocks on which the new block depends). For example, `["a", "b"]` will create a block that depends on blocks `a` and `b`.

**`uuid`** (string)

The unique id of the block. This is generated from `name`, so it's not necessary to populate this field.

**Request**

```bash
curl --location --request PUT 'http://localhost:6789/api/pipelines/broken_wave/blocks/solitary_silence' \
--header 'Cookie: oauth_token=long_token_string' \
--header 'Content-Type: text/plain' \
--header 'X-API-KEY: zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
--data '{
 "block": {
   "color": "teal"
 }
}'
```

**Response**

```json
{
    "block": {
        "all_upstream_blocks_executed": true,
        "color": "teal",
        "configuration": {},
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
        "content": "some_long_content_string"
    }
}
```
