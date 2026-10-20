# Amplitude

*Every way to bring Amplitude data into MageML: tables kept current, frames in Python blocks, and events as they happen.*

MageML reads Amplitude four ways. Each has a tutorial you can follow without an Amplitude
account, on a local server that answers as Amplitude does.

| You want | Use | Freshness | Tutorial |
| --- | --- | --- | --- |
| Every event, daily metric and the tracking plan in a database or warehouse, kept current | Data integration pipeline | Hourly, about 2 hours behind | [Events in a warehouse](warehouse.md) |
| Events or metrics as a frame in a block: analysis, features, training | Python block | Same | [Features in Python blocks](python-blocks.md) |
| Each event as it happens: real-time features, alerts, scoring | Stream from the Webhook destination | Seconds | [Events as they happen](webhook-stream.md) |
| Events through Kafka, when Amplitude already writes to Confluent | Stream from a Kafka topic | Seconds | [Events through Kafka](kafka.md) |

The first two read Amplitude's APIs with the project's **API key** and **secret key**
(Amplitude > Settings > Projects > your project > General) and its **region**, `US` or `EU`.
The streams receive what Amplitude sends from a destination (Data > Destinations), which
the Plus, Growth and Enterprise plans have.

## Try it without an Amplitude account

```bash
mage examples amplitude-server
```

The command runs a local Amplitude on `http://127.0.0.1:18765` with a music app's events of
the last 7 days: sign-ups, plays, skips, carts, purchases and subscriptions. It answers the
Export, Dashboard REST, Taxonomy, Annotations and Cohorts APIs, refuses wrong keys as
Amplitude does, and prints the settings to use:

```yaml
api_key: test-api-key
secret_key: test-secret-key
host: http://127.0.0.1:18765
```

For the streams, `mage examples amplitude-webhook` posts events as the Webhook destination
does, and `mage examples amplitude-kafka` writes them to a topic as the Confluent destination
does.

## What MageML takes care of

- **No hour is missed.** Amplitude finishes an hour of events about two hours after it ends.
  Syncs continue from the last exported hour up to the newest finished one, whatever the
  schedule, and save their position after each request.
- **No row is repeated.** Amplitude can send an event again; rows are upserted by `uuid`,
  and a stream writes a retried event once.
- **Errors say what to do.** Wrong keys or the wrong region stop with that message; rate
  limits and server errors are retried as Amplitude asks.
- **Large projects stream.** Exports go to disk and are read an hour at a time, never held
  in memory.

## Reference

- [Amplitude source](../../data-integrations/sources/amplitude.md): every setting and stream.
- `mage_ai.io.amplitude.Amplitude`: the client of Python blocks.
- [Webhook sources](../../production/stream-services.md#webhooks) of stream services.
