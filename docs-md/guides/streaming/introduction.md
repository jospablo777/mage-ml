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
errors, and the engine's log. **Tap** shows the last 50 messages at the source or at a block whose output the stream
writes, refreshed every second, with secret fields redacted; only development runs keep
them. While the stream runs, the dependency graph shows each block's
messages in and out and p95 time on its node. The run refuses to start when a variable the
blocks need is not set, and names it. **Pause** stops reading and lets the work in flight
finish; **Resume** and **Stop** do what they say.

The run reads with development positions (a `<group>-dev-<user>` consumer group, see above),
from a build of the pipeline in `.mage/dev/<pipeline>/`, with the server's environment. The
engine needs the connectors the pipeline uses: the `mageml-engine` wheel has them all. It
stops with the server.

## Dead letters

With a dead-letter sink (a data exporter with `dead_letter: true`), a message a block fails on
goes there with its error instead of stopping the stream. The **Dead letters** tab reads a
Kafka dead-letter sink without a consumer group: the newest messages, newest first, each with
its error, partition, offset and time (secret fields redacted, as in samples).

- **Open as sample** saves them as a sample: Run on sample shows the failure again, and the
  fix can be checked on the very messages that failed.
- **Replay to <topic>** writes them back to the source's topic as they were received, once the
  block is fixed. It is a write to production, so you type the topic's name to confirm.

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
