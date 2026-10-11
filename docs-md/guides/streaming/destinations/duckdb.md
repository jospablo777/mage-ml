# DuckDB

## Basic config

```yaml
connector_type: duckdb
profile: default    # profile in the io_config.yaml
config:
  table_name: table_name
```

## Optional fields

These keys go under `config`.

```yaml
config:
  table_name: table_name
  schema_name: schema_name    # Override the schema name. Default schema is "main"
  if_exists: append           # Default for the streaming sink
```
