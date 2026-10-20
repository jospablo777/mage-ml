# Trino SQL blocks

*Execute SQL commands in Trino.*

<img src="https://dbdb.io/media/logos/trino.png" alt="Trino" />

## Credentials

Open the file named `io_config.yaml` at the root of your MageML project and enter the following:

```yaml
version: 0.1.1
default:
  trino:
    catalog: postgresql                       # Change this to the catalog of your choice
    host: 127.0.0.1
    http_headers:
      X-Something: 'mage=power'
    http_scheme: http
    password: mage1337                        # Optional
    port: 8080
    schema: core_data
    session_properties:                       # Optional
      acc01.optimize_locality_enabled: false
      optimize_hash_generation: true
    source: trino-cli                         # Optional
    user: admin
    verify: /path/to/your/ca.crt              # Optional
    data_type_properties:
      timestamp_precision: 3                              # Optional. Set TIMESTAMP(n) precision
    overwrite_types: path_to_types_json # Optional
```

> [!NOTE]
> Change the values according to your Trino settings.

## How to use

1. Trino must be running on a remote server or on your local workstation.
Read these [instructions](https://github.com/jospablo777/mage-ml/blob/master/mage_integrations/mage_integrations/destinations/trino/README.md#run-trino-locally)
to run Trino locally.

1. Create a new pipeline or edit an existing pipeline.

1. Add a data loader or transformer SQL block.

1. In the block’s header, click the dropdown menu labeled <b>Connection</b> and select the option
labeled <b>Trino</b>.

1. In the block’s header, click the dropdown menu labeled <b>Profile</b> and select the option
labeled <b>default</b>.

> [!NOTE]
> If the name of your profile in the `io_config.yaml` file is different,
> then select the profile name that matches your settings.

1. You can optionally check the box labeled <b>Use raw SQL</b>.
If you do, read more about [how to use SQL blocks with raw SQL](../../guides/blocks/sql-blocks.md#using-raw-sql).

1. You can optionally overwrite your table columns types by adding a JSON to `overwrite_types` config, the format needs to be `{"column_name":"VARCHAR"}`

1. Write your SQL statements in the SQL block.

1. Once you’re finished writing your SQL statements,
click the play button in the top right corner of the block to execute the SQL block and preview
the results.

## Upstream tables

A query reads an upstream block's output as `{{ df_1 }}`, `{{ df_2 }}` and so on:

- The output of a Python or R block is written to a table in the block's catalog and schema
  first, replacing the table of the last run. An empty frame is written as a table with its
  columns and no rows.
- The table of an upstream Trino SQL block on the same profile (catalog, host, port and
  schema) is read as it is.
- With `unique_upstream_table_name: true` in the block's configuration, the tables written
  for a run have a suffix of that run, so two runs at the same time do not write the same
  table, and they are dropped when the block ends.

Trino SQL blocks also run in [pipeline services](../../production/pipeline-services.md).
