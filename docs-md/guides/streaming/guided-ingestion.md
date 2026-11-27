# Copy a database and keep it in step

*Guided ingestion: a PostgreSQL database's tables copied into another and kept in sync by change capture.*

The **Ingestion** page, in the left navigation, sets up a sync from a PostgreSQL database to
another: the tables you choose are copied, then every insert, update and delete follows
within seconds. It checks both servers, shows a sample, says what the sync will do, and
creates a streaming pipeline you can read, run from its Run tab and export as a service.

## Steps

| Step | What it checks or asks |
| --- | --- |
| Source | `wal_level` is `logical`, the user may replicate (`REPLICATION`), a replication slot is free; then each table with its primary key and size |
| Tables | Choose them; a table without a primary key cannot be chosen (rows upsert and delete by it). **Sample** shows its first rows |
| Destination | The same server or another; the schema, and a prefix for the copies' names; whether a deleted row is deleted or kept with `_mage_deleted_at` |
| Review | What the sync will do, in sentences: copy, keys, deletes, new columns, restarts |
| Create | The pipeline; the passwords become project secrets |

## What the pipeline is

A change capture source and a PostgreSQL sink in mirror mode:

```yaml
# source
connector_type: postgres
host: db.example.com
database: shop
user: replicator
password: "{{ mage_secret_var('sync_shop_source_password') }}"
replication_slot: mageml_sync_shop
publication_name: mageml_sync_shop
tables: [public.orders, public.customers]
snapshot: initial
```

```yaml
# copy
connector_type: postgres
host: warehouse.example.com
database: analytics
username: loader
password: "{{ mage_secret_var('sync_shop_destination_password') }}"
schema: raw
table: "copy_{_mage_table}"
mirror:
  key: {orders: [id], customers: [id]}
  deletes: delete
```

- `snapshot: initial` copies the tables' rows, then streams their changes from the
  replication slot made before the copy, so nothing is missed.
- The slot moves on only after the destination committed a batch: after a stop, the sync
  continues from the last change it wrote, and a change it repeats writes the same row.
- A column added in the source is added to the copy with its next change. `TRUNCATE` is not
  replicated.

See [stream services](../../production/stream-services.md#postgresql-changes) for the source and
the sink's mirror mode in detail.
