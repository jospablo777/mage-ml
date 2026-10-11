# Snowflake

## Basic config

```yaml
connector_type: snowflake
profile: default    # profile in the io_config.yaml
config:
  table_name: table_name
```

## Optional fields

These keys go under `config`.

```yaml
config:
  table_name: table_name
  database: database  # Override the database
  schema: schema      # Override the schema
```
