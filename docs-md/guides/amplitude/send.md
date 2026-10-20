# Send data to Amplitude

*Send orders from PostgreSQL to Amplitude as events and churn scores as user properties, with a data integration pipeline, a Python block or a stream, each event once.*

Product analytics often lacks what lives in the database: orders paid on the server,
refunds, a customer's plan, a model's churn score. You send them to Amplitude, to chart and
segment on them next to the product's events. It takes about fifteen minutes.

| You send | Use | When |
| --- | --- | --- |
| Rows of tables, on a schedule | [Data integration pipeline](#1-start-postgresql-and-a-local-amplitude) | Orders, subscriptions, scores computed daily |
| A frame a block computed | [Python block](#from-a-python-block) | Model outputs, features, one-off backfills |
| Each event as it happens | [Stream](#from-a-stream) | Server-side events, webhooks of payment providers |

All three use the project's **API key** only (Amplitude > Settings > Projects > your
project > General), and its region, `US` or `EU`. Each event gets an `insert_id`, which
Amplitude uses to drop an event it took in the last 7 days, so sending again adds nothing.

## Before you start

- MageML: `uv pip install "mage-ml[integrations]"`, and a project (`mage init demo`).
- Docker, for PostgreSQL.

## 1. Start PostgreSQL and a local Amplitude

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=mage -p 5432:5432 postgres:17
mage examples amplitude-server
```

The second command keeps running: leave it in its own terminal. It takes events as
Amplitude does and prints each request it takes. With a real project, skip it, and use
your API key and region in step 4.

## 2. Create the tables

```bash
docker exec -i postgres psql -U postgres <<'SQL'
CREATE TABLE orders (
  order_id integer PRIMARY KEY,
  customer_id text NOT NULL,
  ordered_at timestamptz NOT NULL,
  amount numeric(10, 2),
  status text
);
CREATE TABLE customer_scores (
  customer_id text PRIMARY KEY,
  churn_risk double precision,
  segment text
);
INSERT INTO orders VALUES
  (1, 'customer-1', now() - interval '3 hours', 19.50, 'paid'),
  (2, 'customer-2', now() - interval '2 hours', 35.00, 'paid'),
  (3, 'customer-1', now() - interval '1 hour', 9.50, 'paid');
INSERT INTO customer_scores VALUES
  ('customer-1', 0.82, 'at risk'),
  ('customer-2', 0.05, NULL);
SQL
```

## 3. Create the pipeline and pick the source

```bash
cd demo
export AMPLITUDE_API_KEY=test-api-key PGPASSWORD=mage
mage start .
```

**New** > **Data integration**, name it `postgres_to_amplitude`. Under **Source**, select
**PostgreSQL**:

```yaml
database: postgres
host: localhost
port: 5432
schema: public
username: postgres
password: "{{ env_var('PGPASSWORD') }}"
```

**View and select streams**: select `orders` and `customer_scores`. Each comes keyed by its
primary key: `order_id` and `customer_id` are checked under **Key prop**. Keep them; the
destination makes each order's `insert_id` from its key.

## 4. Set up Amplitude as the destination

Under **Destination**, select **Amplitude**. Its documentation opens next to the settings.
Replace them with:

```yaml
api_key: "{{ env_var('AMPLITUDE_API_KEY') }}"
region: US
host: http://127.0.0.1:18765
streams:
  orders:
    event_type: Order Completed
    time: ordered_at
    user_id: customer_id
  customer_scores:
    user_properties: true
    user_id: customer_id
```

- `orders` rows become `Order Completed` events: `customer_id` is the user, `ordered_at` the
  event's time, and the other columns (`order_id`, `amount`, `status`) its properties.
- `customer_scores` rows set user properties: `churn_risk` and `segment` on each customer.
- `host` points at the local Amplitude; delete it for a real project, and set `region: EU`
  if the project stores its data in the EU.

Press **Test connection**. It checks the API key and the region without sending an event.

## 5. Run it

Press **Run@once** (or add an hourly trigger under **Triggers**). The local Amplitude's
terminal shows what it took:

```
/batch took 3 Order Completed.
/batch took 2 user property updates.
```

`customer-2`'s empty `segment` was left out: an empty value does not erase a property.

## 6. Run it again

Change an order and run once more:

```bash
docker exec postgres psql -U postgres -c "UPDATE orders SET status = 'refunded' WHERE order_id = 2"
```

```
/batch took nothing new; 3 already taken (same insert_id).
/batch took 2 user property updates.
```

The streams are full-table: every run sends every row. Each order keeps the `insert_id` of
its `order_id`, so Amplitude keeps one event per order, the first one sent; the scores are
set again to the same values. To send a refund as its own event, give refunds their own
table or view with an `event_type` of `Order Refunded`.

For large tables, set the stream's replication to **Incremental** with a bookmark such as
`updated_at`: each run then sends only the rows changed since the last one.

## From a Python block

A data exporter sends a frame with the same rules. Put the key in `io_config.yaml`
(`AMPLITUDE_API_KEY`, `AMPLITUDE_REGION`; for the local Amplitude,
`AMPLITUDE_HOST: http://127.0.0.1:18765`):

```python
from os import path

from mage_ai.io.amplitude import Amplitude
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path

@data_exporter
def send_scores(scores, *args, **kwargs):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    amplitude = Amplitude.with_config(ConfigFileLoader(config_path, 'default'))
    # customer_id, churn_risk, segment
    sent = amplitude.set_user_properties(scores, columns={'user_id': 'customer_id'})
    print(f'{sent} customers updated in Amplitude')
```

| Method | Sends |
| --- | --- |
| `send_events(rows, event_type=None, key_columns=None, columns=None)` | Each row as an event. `key_columns` make the `insert_id`; `columns` names the row's columns for `user_id`, `device_id`, `time`, `insert_id`. |
| `set_user_properties(rows, columns=None)` | Each row's columns as its user's properties. |
| `export(rows, user_properties=False, ...)` | One of the two. |

`rows` is a pandas or Polars frame, or a list of dicts. Sending needs no secret key.

## From a stream

A stream's sink sends each batch to Amplitude as the blocks made it, from the engine or an
exported service, within seconds. In a streaming pipeline, add a data exporter from the
**Amplitude** template:

```yaml
connector_type: amplitude
api_key: "{{ env_var('AMPLITUDE_API_KEY') }}"
region: US
event_type: Order Completed
columns:
  user_id: customer_id
  time: ordered_at
key_columns: [order_id]
```

`user_properties: true` sets user properties instead. Running the block checks the API key.
For the local Amplitude, add `url: http://127.0.0.1:18765` and `allow_private: true`. The
service answers its sender only after Amplitude took the events; a replay after a crash
sends the same `insert_id`s, so Amplitude keeps each event once. See
[Stream services](../../production/stream-services.md#amplitude-sink).

## Clean up

```bash
docker rm -f postgres
```

Stop the local Amplitude with Ctrl+C.

## Next

- [Amplitude destination](../../data-integrations/destinations/amplitude.md): every setting.
- [Events in a warehouse](warehouse.md): the other direction.
