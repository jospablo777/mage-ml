# Amplitude events through Kafka

*Read the events Amplitude's Confluent Kafka REST destination writes to a topic with a MageML stream, Zstandard compression included.*

Amplitude's **Confluent Kafka REST** destination writes each event to a topic in Confluent
Cloud or Confluent Platform, keyed by `user_id`. When your data already flows through Kafka,
a MageML stream reads that topic like any other. It takes about ten minutes.

## Before you start

- MageML and the stream engine: `uv pip install "mage-ml[streaming,services]"`.
- Docker, for a local Kafka.

## 1. Start Kafka and write Amplitude events

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:4.1.0
export KAFKA_BOOTSTRAP=localhost:9092
mage examples amplitude-kafka --topic amplitude-events --count 500 --compression zstd
```

The command writes events as the Confluent destination does: the event as JSON, keyed by
`user_id` (`device_id` for visitors without one). `--compression zstd` compresses the
batches with Zstandard; the stream reads them as they are.

## 2. Create the stream

```bash
mage init demo
mage start demo
```

**New** > **Streaming**, name it `amplitude_kafka`, start from **Kafka**, and set the source:

```yaml
connector_type: kafka
bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
topic: amplitude-events
consumer_group: amplitude-features
auto_offset_reset: earliest
batch_size: 500
```

Replace the transformer's code with the one of the **Amplitude (webhook)** template: each
message is the same event Amplitude would post to a webhook.

## 3. Run it

**Run** tab > **Start on the local engine**. The run reads with a development consumer
group, so `amplitude-features` keeps its position; the tab shows 500 messages in, and the
printing sink shows one flat row per event.

## 4. Connect Amplitude

In Confluent, create the topic, and an API key with write access to it. In Amplitude:
**Data** > **Destinations** > **Confluent Kafka REST**:

1. **Endpoint**: the REST endpoint with its version: `https://<cluster>.confluent.cloud:443/kafka/v3`
   for Confluent Cloud, `https://<rest-proxy>/v3` for a REST Proxy.
2. **Cluster ID** and **Topic**: `amplitude-events`.
3. **API key** and **secret**: sent as HTTP Basic auth on each request.
4. **Key**: `user_id` (Amplitude falls back to `device_id`).

Then point the stream's `bootstrap_server` at Confluent, with `security_protocol: SASL_SSL`
and the `sasl_config` of a Confluent API key that can read the topic (see
[Kafka sources](../../production/stream-services.md)).

Amplitude produces one request per event, under 100 a second per connection; for more,
use the Webhook destination with several service instances.

## Writing Zstandard

A Kafka sink compresses with lz4. To write Zstandard, which made Amplitude's own topics
smaller for more CPU, set it in the sink:

```yaml
librdkafka:
  compression.type: zstd
```

## Clean up

```bash
docker rm -f kafka
```
