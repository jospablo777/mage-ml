# Overview

Streaming pipelines in MageML process data as it arrives, rather than in batches at scheduled intervals. Use them for real-time analytics, monitoring, and other cases where data must be processed right away.

## Introduction

Each streaming pipeline has three components:
- Source: The data stream source to consume message from.
- Transformer: Write python code to transform the message before writing to destination.
- Sink (destination): The data stream destination to write message to.

Check out this [tutorial](tutorials/streaming-pipeline.md#create-a-new-pipeline) to set up an example streaming pipeline.

You can build a streaming pipeline with one source, multiple transformers, and multiple sinks. Here is an example streaming
pipeline:
![Complex streaming pipeline](https://media.graphassets.com/4ZWHyLpgTESu0rB1sXnP)

## Supported sources
- [ActiveMQ](sources/activemq.md)
- [Amazon SQS](sources/amazon-sqs.md)
- [Azure Event Hub](sources/azure-event-hub.md)
- [Google Cloud PubSub](sources/google-cloud-pubsub.md)
- [InfluxDB](sources/influxdb.md)
- [Kafka](sources/kafka.md)
- [Kinesis](sources/kinesis.md)
- [MongoDB (change streams)](sources/mongodb.md)
- [NATS JetStream](sources/nats.md)
- [PostgreSQL (logical replication)](sources/postgres.md)
- [RabbitMQ](sources/rabbitmq.md)

## Supported sinks (destinations)

- [ActiveMQ](destinations/activemq.md)
- [Amazon S3](destinations/amazon-s3.md)
- [Azure Data Lake](destinations/azure_data_lake.md)
- [BigQuery](destinations/bigquery.md)
- [ClickHouse](destinations/clickhouse.md)
- [DuckDB](destinations/duckdb.md)
- [Dummy](destinations/dummy.md)
- [Elasticsearch](destinations/elasticsearch.md)
- [Google Cloud PubSub](destinations/google-cloud-pubsub.md)
- [Google Cloud Storage](destinations/google-cloud-storage.md)
- [InfluxDB](destinations/influxdb.md)
- [Kafka](destinations/kafka.md)
- [Kinesis](destinations/kinesis.md)
- [MongoDB](destinations/mongodb.md)
- [Microsoft SQL Server](destinations/mssql.md)
- [MySQL](destinations/mysql.md)
- [NATS JetStream](destinations/nats.md)
- [Opensearch](destinations/opensearch.md)
- [Oracle Database](destinations/oracledb.md)
- [Postgres](destinations/postgres.md)
- [RabbitMQ](destinations/rabbitmq.md)
- [Redshift](destinations/redshift.md)
- [Snowflake](destinations/snowflake.md)
- [Trino](destinations/trino.md)

## Start from a template

**New > Streaming** asks where the pipeline starts from: Kafka, RabbitMQ, NATS JetStream, a
webhook (HTTP) or PostgreSQL changes, or empty. A template adds the source, with its
connection settings read from the environment (`KAFKA_BOOTSTRAP`, `PGHOST` and so on), a
Python transformer, a sink that prints, and a sample named `example`, so the transformer runs
on the sample with the first ⌘↵. Fill in the variables to run the stream, and replace the
printing sink with a real one when the output looks right.

## Develop blocks on sample messages

Write and test a transformer on real messages without starting the stream.

1. Open the **Samples** tab in the right panel. **Capture** reads the newest (or the first)
   messages from a Kafka source without joining its consumer group, or from a NATS JetStream
   source with an ephemeral consumer that acknowledges nothing, so a running stream keeps its
   position. Queue sources (RabbitMQ, SQS, ActiveMQ, Pub/Sub, IBM MQ) take messages away from
   other consumers when read: write or paste their samples. **Keep samples out of Git** is on by default, since captured messages can hold
   real data. You can also paste messages, as JSON lines or a JSON array, or edit a sample.
2. In a transformer (Python, Rust or R), press **⌘↵** (Ctrl+Enter), or the play button **Run on
   sample**. The blocks from the source down to it run on the chosen sample, with the same
   workers an exported service uses. Below the block you see what it changed (fields added,
   removed and changed, with how many messages each, or how many messages it dropped), the
   input and output messages side by side, each block's time and what the blocks printed.
3. When a block fails, the error and its traceback show in full, and the first message that
   fails on its own is marked in the input.

A capture replaces secret fields with `<redacted>` before the sample is written: fields whose
name or last part says so (`password`, `access_token`, `api_key`, `card_number`, `ssn` and the
like; `token_count` is kept), and the fields the source lists:

```yaml
connector_type: kafka
topic: orders
consumer_group: scoring
secret_fields: [customer.email, customer.phone]   # dotted paths, or a field name anywhere
```

The capture says which fields it redacted. `secret_fields` is read by capture only; the
pipeline and the exported service read the messages as they are.

Samples are JSON lines files in `pipelines/<pipeline>/samples/`. A block's top-level code, such
as loading a model, runs again only when its file changes, so repeat runs are fast.

## Test pipeline execution
After finishing configuring the streaming pipeline, you can click the button `Execute pipeline` to test streaming pipeline execution.

A run from the editor is a development run, and it never moves production's position:

- A Kafka source reads with the consumer group `<group>-dev-<user>`, a PostgreSQL change
  source with the slot `<slot>_dev_<user>` (drop it when you are done: a slot keeps the
  server's log until it is read), and a NATS source with the consumer `<name>-dev-<user>`.
  Python checkpoints (Kinesis, MongoDB) go to a folder of their own.
- Queues (RabbitMQ, SQS, ActiveMQ, Pub/Sub, IBM MQ) have no position to fork: what the run
  acknowledges leaves the queue, and the run says so. Point the source at a development
  queue to keep production's messages.
- The sinks are real, so the editor names them and asks before the run starts; the server
  refuses a run whose sinks were not confirmed.

`mage service run` reads with the same development positions. Triggers and deployed services
use the configured ones.

## How the stream runs

The **Compile** panel of a streaming pipeline shows how the engine runs it: the source, the
runs of blocks in one language (each run handles a batch in one process: one call to the
Python worker, or one Rust program or R session kept running), the language boundaries where
messages cross as JSON, the sinks, the batch size, the dead-letter sink, and the positions a
development run reads with. Problems that would stop the stream, such as a connector the
engine does not run or a missing setting, show there before Start.

## Run on the local engine

The **Run** tab of a streaming pipeline starts it on the local engine, the one exported
services run on, without a trigger: **Start on the local engine** names the sinks the run
writes and asks first, then the panel shows the state, messages read and written per second
and the lag over the last minute, the totals (batches, retries, dead-lettered messages,
alerts), each block's messages in and out, median and 95th percentile time per batch and
errors, and the engine's log. **Tap** shows the last 50 messages at the source or at any
block, refreshed every second, with secret fields redacted; only development runs keep
them. While the stream runs, the dependency graph shows each block's
messages in and out and p95 time on its node. The run refuses to start when a variable the
blocks need is not set, and names it. **Pause** stops reading and lets the work in flight
finish; **Resume** and **Stop** do what they say.

**Check connections** asks the engine whether the source and each sink answer, with the
settings a development run uses, before you start it. The check only reads: it never joins a
consumer group, takes a message or writes.

| Connector | What the check reports |
| --- | --- |
| Kafka | The brokers reached; each topic's partitions, messages kept and retention; the consumer group's lag, or that it has no position yet and where it would start. A sink's missing topic is a warning: the brokers may create it |
| PostgreSQL sink | The table and its columns, or that the first batch makes it; whether the user can insert (and update, for upserts) or make the table |
| PostgreSQL changes | `wal_level`, the user's right to replicate, the slot (its plugin, how far behind the server, whether another connection holds it) and the publication's tables |
| RabbitMQ | The queue's ready messages and consumers, or the sink's exchange |
| NATS JetStream | The stream's subjects and messages, and the source's consumer: messages not delivered and not acknowledged |
| HTTP source, stdout | Nothing to reach |

Other connectors say they have no check yet; their start reports what fails. A failed check
names what to fix: the password the server refused, the topic that does not exist, the
`wal_level` to set.

The run reads with development positions (a `<group>-dev-<user>` consumer group, see above),
from a build of the pipeline in `.mage/dev/<pipeline>/`, with the server's environment. The
engine needs the connectors the pipeline uses: the `mageml-engine` wheel has them all. It
stops with the server.

## Dead letters

With a dead-letter sink (a data exporter with `dead_letter: true`), a message a block fails on
goes there with its error instead of stopping the stream. The **Dead letters** tab reads a
Kafka or NATS JetStream dead-letter sink without moving anything (Kafka without a consumer
group; NATS with a consumer that acknowledges nothing and is deleted after the read): the
newest messages, newest first, each with its error, its position (partition and offset, or
stream sequence) and time, secret fields redacted as in samples.

- **Open as sample** saves them as a sample: Run on sample shows the failure again, and the
  fix can be checked on the very messages that failed.
- **Replay to** (the source's topic or subject) writes them back to the source's Kafka topic or NATS subject as they
  were received, once the block is fixed. It is a write to production, so you type its name
  to confirm. A NATS source subject with wildcards cannot be replayed to.

## Event time and late messages

A message often says when its event happened: an order's `created_at`, a reading's `at`.
With `event_time` on the source, an exported service and the local engine judge each
message by it:

```yaml
connector_type: kafka
topic: readings
consumer_group: readings
event_time:
  field: at              # a field of the message (a.b for a nested one); without it, the
                         # source's own timestamp
  format: epoch_s        # epoch_ms (the default), epoch_s or rfc3339
  lateness_seconds: 30   # how far behind the latest event an event may still arrive
  on_late: dead_letter   # or pass
```

Each partition has a watermark: the latest event time it has seen, less
`lateness_seconds`. A message older than its partition's watermark is late.

- With `on_late: dead_letter` (the default), late messages, and messages without an event
  time, leave the batch before the blocks and every sink and go to the dead-letter sink,
  with the reason: "event time 2026-10-21T10:00:05Z is before partition 0's watermark
  2026-10-21T10:00:10Z". The stream needs a dead-letter sink.
- With `on_late: pass` they stay in the stream and are counted.

The Run tab counts late messages, `/v1/streams/<pipeline>` shows each partition's
watermark, and `/metrics` has `mage_service_stream_late_total` and
`mage_service_stream_watermark_seconds`. A watermark lives with its partition on the
instance that reads it, and starts again when a partition moves or the service restarts.
MageML's own streaming takes messages as they come and ignores `event_time`.

## Windows

A transformer written in YAML with `window:` aggregates the messages of a window of event
time, per key, without code. The engine computes it, and emits one message per key and
window when the partition's watermark passes the window's end:

```yaml
window:
  key: card_id          # optional: windows per value of this field
  type: tumbling        # tumbling, hopping or session
  size_seconds: 60      # tumbling and hopping
  slide_seconds: 10     # hopping: a new window every 10 seconds
  gap_seconds: 300      # session: a key's events closer than this are one window
  aggregate:
    payments: count
    total: {sum: amount}
    largest: {max: amount}
    smallest: {min: amount}
    mean: {avg: amount}
    shops: {distinct: shop_id}
```

```json
{"key": "c-17", "window_start": "2026-10-21T10:00:00.000Z",
 "window_end": "2026-10-21T10:01:00.000Z", "payments": 3, "total": 61.5, "shops": 2}
```

Event time comes from the source's `event_time` (above), or the source's timestamp. A window
closes once; a message for a window that already closed is counted late.

The windows are saved with the positions they hold: before the source commits a position,
each partition's open windows as of it are written to
`<data>/state/<pipeline>/<block>/`. A restarted service resumes them from the same point,
and with Kafka or PostgreSQL changes it skips the messages its saved windows already hold.
The files are on the instance's data volume: a partition that moves to another instance
starts its windows empty there. Windows run on the engine, in exported services and in
the Run tab, not in MageML's own streaming.

## Keyed state

A stream block in Python, Rust or R can keep a value per key between batches: a count per
card, the last reading of a device, a running feature. Name the field of the source's
messages that keys it in the block's configuration (`state_key: card` in the pipeline's
`metadata.yaml`; a dotted path such as `card.id` reaches into objects).

A Python block gets `state` in its keyword arguments:

```python
@transformer
def velocity(messages, *args, **kwargs):
    state = kwargs['state']
    out = []
    for m in messages:
        state[m['card']] = state.get(m['card'], 0) + 1
        out.append(dict(m, payments_today=state[m['card']]))
    return out
```

A Rust block takes `State` after its messages; `key_of` reads a message's key as the
engine does:

```rust
use mage::prelude::*;

fn transform(messages: Vec<Value>, state: State) -> Result<Vec<Value>> {
    messages
        .into_iter()
        .map(|mut m| {
            let key = state.key_of(&m);
            let seen = state.get::<u64>(&key)?.unwrap_or(0) + 1;
            state.set(&key, seen)?;
            m["payments_today"] = json!(seen);
            Ok(m)
        })
        .collect()
}
```

An R block calls `state_get()`, `state_set()`, `state_remove()` and `state_keys()`:

```r
#* @transformer
velocity <- function(messages, ...) {
  messages$payments_today <- vapply(messages$card, function(card) {
    seen <- state_get(card, default = 0) + 1
    state_set(card, seen)
    seen
  }, numeric(1))
  messages
}
```

- The state holds the values of the keys of the batch's messages; set, change or delete
  them. Keys are text (a number's key is its JSON text, such as `7`) and values are JSON.
- A message that fails on its own (and goes to the dead-letter sink) leaves the state as it
  was.
- In services and the Run tab, the engine keeps the state and saves it with the positions
  it reflects, as windows are; it comes back after a restart, and with Kafka or PostgreSQL
  changes the messages it already holds are not run again. A block sends back only the
  keys it changed.
- In the editor, a sample run starts from empty state, and **Execute pipeline** keeps the
  state in memory for the run, without saving it.

### Timers

A block with `state_key` can also set a timer per key: a time in milliseconds when
something should happen if nothing else did, such as an alert when a device goes silent.
Timers follow event time when the stream has `time` (above), else the time batches arrive.
When a batch moves its partition's watermark to a timer or past it, the block runs once
with that key among the fired ones and no messages, before the batch's messages; the timer
is removed unless the block sets it again.

```python
@transformer
def watch(messages, *args, **kwargs):
    state, timers = kwargs['state'], kwargs['timers']
    # Keys whose timer fired: no reading for 60 s of event time.
    out = [dict(device=key, silent_since=state[key]) for key in kwargs['fired']]
    for m in messages:
        state[m['device']] = m['at']
        timers[m['device']] = m['at'] + 60_000
        out.append(m)
    return out
```

| | Python | Rust | R |
| --- | --- | --- | --- |
| Set or move a key's timer | `timers[key] = at` | `timers.set(&key, at)?` | `timer_set(key, at)` |
| Read, remove | `timers.get(key)`, `del timers[key]` | `timers.get(&key)`, `timers.remove(&key)` | `timer_get(key)`, `timer_remove(key)` |
| Keys that fired, with their times | `kwargs['fired']` | `timers.fired()` | `timers_fired()` |
| The time timers follow | `kwargs['watermark']` | `timers.watermark()` | `watermark()` |

A Rust block takes `Timers` after its messages, next to `State`. Timers are saved with the
state and its positions, so one that was due before a restart fires after it. They fire
with the batches of their partition: a timer fires when a message of its partition moves
the time, not while the partition receives nothing. In the editor, **Execute pipeline**
fires timers with the next batch, by the clock.

## Run pipeline in production
Create the trigger in [triggers page](../../design/data-pipeline-management.md#create-trigger) to run streaming pipelines in production.

### Executor count
If you want to run multiple executors at the same time to scale the streaming pipeline execution, you can set the `executor_count` variable
in the pipeline's metadata.yaml file. Here is an example:
```yaml
blocks:
- ...
- ...
executor_count: 10
name: test_streaming_pipeline
type: streaming
uuid: test_streaming_pipeline
```

### Executor type
When running MageML on a Kubernetes cluster, you can also configure streaming pipeline to be run on separate k8s pods by setting `executor_type`
field in the pipeline's metadata.yaml to `k8s`.

Example config:
```yaml
blocks:
- ...
- ...
executor_type: k8s
name: test_streaming_pipeline
type: streaming
uuid: test_streaming_pipeline
```

## Contributing guide

Follow this [doc](../../contributing/backend/streaming/sources-and-destinations.md) to add a new source or destination (sink) to MageML streaming pipelines.
