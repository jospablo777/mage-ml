# Trino

## Basic config

```yaml
connector_type: trino
profile: default    # profile in the io_config.yaml
config:
  table_name: table_name
```

## Optional fields

These keys go under `config`.

```yaml
config:
  table_name: table_name
  schema_name: schema_name    # Override the schema name.
```

## In an exported service

[Exported services](../../../production/stream-services.md) do not run this connector yet: exporting a pipeline that uses it stops and says so. It runs in MageML as described above.
