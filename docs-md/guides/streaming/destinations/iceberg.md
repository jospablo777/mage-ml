# Apache Iceberg

A streaming pipeline can append its messages to an Apache Iceberg table through a catalog.
In the editor the sink writes with pyiceberg (the `streaming` extra); an exported service
writes with MageML's engine, through a REST catalog, and can keep each message exactly once
from Kafka. See [stream services](../../../production/stream-services.md#apache-iceberg) for how
services write and commit.

## Config

```yaml
connector_type: iceberg
catalog:
  type: rest
  uri: "http://localhost:8181"
  warehouse: "lake"
  token: "{{ env_var('ICEBERG_TOKEN') }}"
table: "analytics.events"
create_table: true
columns:
  event_time: timestamptz
partition_by:
  - day(event_time)
```

| Setting | Meaning |
| --- | --- |
| `catalog` | The catalog's properties as Iceberg names them: `type` (`rest`), `uri`, `warehouse`, `token` or `credential`, and storage properties such as `s3.endpoint` |
| `table` | The table with its namespace |
| `create_table` | Create the table from the first messages when it does not exist |
| `columns` | Types of columns when the table is created: `long`, `int`, `double`, `float`, `string`, `boolean`, `date`, `timestamp`, `timestamptz`, `decimal(10, 2)` |
| `partition_by` | The new table's partitions: columns, `year`, `month`, `day` or `hour` of a time column, `bucket(16, id)`, `truncate(4, name)` |

## Messages

Each message is a JSON object, or `{"data": {...}, "metadata": {...}}` with the object in
`data`. Fields the table does not have are left out and columns a message does not have
are null. A list or an object in a text column is written as its JSON text.

## Try it locally

```bash
uv pip install "mage-ml[streaming]"
docker run -p 8181:8181 apache/iceberg-rest-fixture:1.10.1
```

The fixture keeps its tables in the container; point `catalog.uri` at
`http://localhost:8181`.
