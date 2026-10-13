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

## Develop blocks on sample messages

Write and test a transformer on real messages without starting the stream.

1. Open the **Samples** tab in the right panel. **Capture** reads the newest (or the first)
   messages from a Kafka source without joining its consumer group, so a running stream keeps
   its position. **Keep samples out of Git** is on by default, since captured messages can hold
   real data. You can also paste messages, as JSON lines or a JSON array, or edit a sample.
2. In a transformer, press **⌘↵** (Ctrl+Enter), or the play button **Run on sample**. The blocks
   from the source down to it run on the chosen sample, with the same Python worker an exported
   service uses. Below the block you see the input and output messages side by side, each
   block's time and what the blocks printed.
3. When a block fails, the error and its traceback show in full, and the first message that
   fails on its own is marked in the input.

Samples are JSON lines files in `pipelines/<pipeline>/samples/`. A block's top-level code, such
as loading a model, runs again only when its file changes, so repeat runs are fast.

## Test pipeline execution
After finishing configuring the streaming pipeline, you can click the button `Execute pipeline` to test streaming pipeline execution.

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
