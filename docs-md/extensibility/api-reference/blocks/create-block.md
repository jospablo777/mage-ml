# Create block

`POST /api/pipelines/:pipeline_uuid/blocks`

Create a new block in one or more pipelines.

**`name`** (string, required)

Human readable block name. This is used to generate the UUID, e.g. `restless dust`. Names will be slugified to a safe-uuid.

**`type`** (string, required)

Type of block:
`callback`, `chart`, `conditional`, `custom`, `data_exporter`, `data_loader`, `dbt`, `extension`, `global_data_product`, `markdown`, `scratchpad`, `sensor`, `transformer`

**`color`** (string)

Block color displayed by the frontend for `custom` blocks. All other block types have predefined colors.

**`config`** (object)

An optional parameter specifying a block configuration file For example, `{data_source: "s3"}` will create an s3 data loader.

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

**`extension_uuid`** (string)

UUID of extensions, currently Great Expectations is the only extension supported.

**`language`** (string)

The block language:
`python`, `sql`, `r`, `rust`, `yaml`, `markdown`

**`pipelines`** ([string])

Array of strings containing the pipeline uuids for which the block is a member.

**`priority`** (integer)

The 0-indexed position of the block in the pipeline, e.g. 0 is the first block location.

**`upstream_blocks`** ([string])

An array of strings representing the uuids of upstream blocks (the blocks on which the new block depends). For example, `["a", "b"]` will create a block that depends on blocks `a` and `b`.

**`uuid`** (string)

The unique id of the block. This is generated from `name`, so it's not necessary to populate this field.

**Request**

```bash
curl 'http://localhost:6789/api/pipelines/broken_wave/blocks?api_key=zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
-H 'Accept: application/json, text/plain, */*' \
-H 'Authorization: Bearer 018Cc8ydDUa6YWoLad2oMdw0EKdWWJuyg85D02GN1ww' \
-H 'Content-Type: application/json' \
--data-raw '
  {
  "block": {
      "name": "silent fire",
      "priority": 2,
      "configuration": {
          "data_source": null
      },
      "language": "python",
      "type": "data_exporter"
  },
  "api_key": "zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2"
}
' \
--compressed
```

**Response**

```json
{
    "block": {
        "content": null,
        "name": "silent fire",
        "priority": 2,
        "config": {
            "data_source": null
        },
        "language": "python",
        "type": "data_loader",
        "configuration": {},
        "upstream_blocks": []
    },
}
```
