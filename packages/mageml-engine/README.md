# mageml-engine

The MageML service engine, `mage-service`, as a platform wheel: it runs pipelines and
streams exported from MageML, with the standard connectors (Kafka, RabbitMQ, NATS JetStream,
IBM MQ, ActiveMQ, Amazon SQS, Google Cloud Pub/Sub, MongoDB, PostgreSQL, ClickHouse, object
storage, Elasticsearch, OpenSearch and InfluxDB).

```bash
uv pip install "mage-ml[engine]"
mage service run my-service
```

Wheels are built for Linux x86_64 and aarch64 (glibc 2.28 or later) and macOS on Apple
silicon. Elsewhere, build the engine with `cargo build --release -p mage-service --features
full` and set `MAGE_ENGINE_PATH` to the binary.
