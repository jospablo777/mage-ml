//! Starts the manifest's streams: builds each one's source, sinks and transform session
//! from its settings and runs it until the service stops.
//!
//! Connector settings hold secrets as `$parts` environment references; they are filled in
//! here, in the engine, and never passed to block processes (see `environment`).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mage_connector_local::StdoutSink;
use mage_service_core::io_config;
use mage_service_core::manifest::{Block, Connector, ConnectorKind, Language, Manifest, Pipeline};
use mage_stream_core::{PoisonPolicy, RetryPolicy, RunnerConfig, SinkId, TrackerConfig};
use mage_stream_ports::{ConnectorError, Sink, Source, TransformSession};
use serde_json::Value;
use tokio::task::JoinHandle;

use crate::log::{self, RunLog};
use crate::service::Service;
use crate::stream::{self, SharedStatus, StreamParts, StreamState, StreamStatus};
use crate::stream_python::{MessageShape, PythonStreamSession};

/// The environment variables connector settings read. Blocks do not get them unless a block
/// reads them too (the manifest lists it).
pub fn connector_variables(manifest: &Manifest) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for pipeline in &manifest.pipelines {
        let Some(stream) = &pipeline.stream else {
            continue;
        };
        let connectors = std::iter::once(&stream.source)
            .chain(stream.sinks.iter())
            .chain(stream.errors.dead_letter.iter());
        for connector in connectors {
            names.extend(io_config::variables(&Value::Object(
                connector.config.clone(),
            )));
        }
    }
    let read_by_blocks: BTreeSet<&str> = manifest
        .environment
        .iter()
        .filter(|v| !v.used_by.is_empty())
        .map(|v| v.name.as_str())
        .collect();
    names.retain(|n| !read_by_blocks.contains(n.as_str()));
    names
}

fn settings(service: &Service, connector: &Connector) -> Value {
    let lookup = |name: &str| {
        service
            .extra_env
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .or_else(|| std::env::var(name).ok())
    };
    io_config::render(&Value::Object(connector.config.clone()), &lookup)
}

fn source(
    service: &Service,
    pipeline: &Pipeline,
    connector: &Connector,
) -> Result<Box<dyn Source>, ConnectorError> {
    let _config = settings(service, connector);
    match connector.kind {
        #[cfg(feature = "kafka")]
        ConnectorKind::Kafka => {
            let starts = from_savepoint(service, pipeline)?;
            Ok(Box::new(kafka_source(pipeline, _config, starts)?))
        }
        #[cfg(feature = "rabbitmq")]
        ConnectorKind::Rabbitmq => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            Ok(Box::new(mage_connector_rabbitmq::RabbitSource::new(
                &_config,
                mage_connector_rabbitmq::BatchLimits {
                    max_records: batch.max_records,
                    max_bytes: batch.max_bytes,
                    max_wait: Duration::from_millis(batch.max_wait_ms),
                },
            )?))
        }
        #[cfg(feature = "mongodb")]
        ConnectorKind::Mongodb => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            Ok(Box::new(mage_connector_mongodb::MongoSource::new(
                &_config,
                mage_connector_mongodb::BatchLimits {
                    max_records: batch.max_records,
                    max_bytes: batch.max_bytes,
                    max_wait: Duration::from_millis(batch.max_wait_ms),
                },
                &pipeline.uuid,
            )?))
        }
        #[cfg(feature = "activemq")]
        ConnectorKind::Activemq => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            Ok(Box::new(mage_connector_activemq::ActiveMqSource::new(
                &_config,
                mage_connector_activemq::BatchLimits {
                    max_records: batch.max_records,
                    max_bytes: batch.max_bytes,
                    max_wait: Duration::from_millis(batch.max_wait_ms),
                },
            )?))
        }
        #[cfg(feature = "pubsub")]
        ConnectorKind::Pubsub => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            Ok(Box::new(mage_connector_pubsub::PubSubSource::new(
                &_config,
                mage_connector_pubsub::BatchLimits {
                    max_records: batch.max_records,
                    max_bytes: batch.max_bytes,
                    max_wait: Duration::from_millis(batch.max_wait_ms),
                },
            )?))
        }
        #[cfg(feature = "sqs")]
        ConnectorKind::Sqs => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            Ok(Box::new(mage_connector_sqs::SqsSource::new(
                &_config,
                mage_connector_sqs::BatchLimits {
                    max_records: batch.max_records,
                    max_bytes: batch.max_bytes,
                    max_wait: Duration::from_millis(batch.max_wait_ms),
                },
            )?))
        }
        #[cfg(feature = "postgres")]
        ConnectorKind::Postgres => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            Ok(Box::new(mage_connector_postgres::cdc::PgCdcSource::new(
                &_config,
                mage_connector_postgres::cdc::BatchLimits {
                    max_records: batch.max_records,
                    max_bytes: batch.max_bytes,
                    max_wait: Duration::from_millis(batch.max_wait_ms),
                },
            )?))
        }
        #[cfg(feature = "nats")]
        ConnectorKind::Nats => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            Ok(Box::new(mage_connector_nats::NatsSource::new(
                &_config,
                mage_connector_nats::BatchLimits {
                    max_records: batch.max_records,
                    max_bytes: batch.max_bytes,
                    max_wait: Duration::from_millis(batch.max_wait_ms),
                },
            )?))
        }
        #[cfg(feature = "ibm-mq")]
        ConnectorKind::IbmMq => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            Ok(Box::new(mage_connector_ibmmq::MqSource::new(
                &_config,
                mage_connector_ibmmq::BatchLimits {
                    max_records: batch.max_records,
                    max_bytes: batch.max_bytes,
                    max_wait: Duration::from_millis(batch.max_wait_ms),
                },
            )?))
        }
        ConnectorKind::Http => {
            let stream = pipeline.stream.as_ref().expect("a stream");
            let seconds = |key: &str, default: u64| {
                _config
                    .get(key)
                    .and_then(Value::as_u64)
                    .map_or(Duration::from_secs(default), Duration::from_secs)
            };
            let config = mage_connector_http::HttpSourceConfig {
                answer_within: seconds("answer_within_seconds", 30),
                dedupe_window: seconds("dedupe_window_seconds", 24 * 3600),
                ..mage_connector_http::HttpSourceConfig::default()
            };
            let signature = match _config.get("signature") {
                None | Some(Value::Null) => None,
                Some(settings) => {
                    let text = |key: &str| settings.get(key).and_then(Value::as_str);
                    let secret = text("secret").filter(|s| !s.is_empty()).ok_or_else(|| {
                        ConnectorError::fatal(
                            "the webhook's signature needs a secret (an environment variable)",
                        )
                    })?;
                    Some(mage_connector_http::signature::SignatureCheck::new(
                        text("header").unwrap_or("X-Hub-Signature-256"),
                        text("prefix").unwrap_or("sha256="),
                        secret.as_bytes(),
                    ))
                }
            };
            let (source, handle) = mage_connector_http::HttpSource::new(config);
            service
                .ingest
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(
                    pipeline.uuid.clone(),
                    Arc::new(HttpIngest {
                        handle,
                        signature,
                        max_bytes: stream.batch.max_bytes,
                        key_field: _config
                            .get("idempotency_key_field")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|f| !f.is_empty())
                            .map(str::to_string),
                    }),
                );
            Ok(Box::new(source))
        }
        kind => Err(ConnectorError::fatal(format!(
            "this service cannot read a {} source: {}",
            kind.as_str(),
            unavailable(kind)
        ))),
    }
}

/// What the API needs to hand a webhook request to its stream.
pub struct HttpIngest {
    pub handle: mage_connector_http::IngestHandle,
    pub signature: Option<mage_connector_http::signature::SignatureCheck>,
    /// The largest request body accepted, the stream's batch size in bytes.
    pub max_bytes: usize,
    /// For requests without an `Idempotency-Key`: the message field that identifies them
    /// (`idempotency_key_field`, `uuid` for Amplitude).
    pub key_field: Option<String>,
}

/// With `MAGE_SERVICE_INSTANCE_ID` (a stable name per replica, such as a StatefulSet pod's),
/// the consumer is a static group member (I-39): a restart within the session timeout keeps
/// its partitions instead of moving every partition of the group twice.
#[cfg(feature = "kafka")]
#[cfg(feature = "kafka")]
fn kafka_source(
    pipeline: &Pipeline,
    config: Value,
    starts: Option<Arc<dyn mage_stream_ports::StartPositions>>,
) -> Result<mage_connector_kafka::KafkaSource, ConnectorError> {
    let stream = pipeline.stream.as_ref().expect("a stream");
    let batch = &stream.batch;
    let limits = mage_connector_kafka::BatchLimits {
        max_records: batch.max_records,
        max_bytes: batch.max_bytes,
        max_wait: Duration::from_millis(batch.max_wait_ms),
    };
    mage_connector_kafka::KafkaSource::with_start_positions(
        &static_membership(config, &pipeline.uuid),
        limits,
        in_flight_bytes(stream),
        starts,
    )
}

/// With MAGE_SERVICE_START_FROM_SAVEPOINT, the stream's state as the savepoint kept it, and
/// where its partitions start (savepoints.rs); None without it, or once it was restored.
fn from_savepoint(
    service: &Service,
    pipeline: &Pipeline,
) -> Result<Option<Arc<dyn mage_stream_ports::StartPositions>>, ConnectorError> {
    let Some(name) = std::env::var(crate::savepoints::START_FROM)
        .ok()
        .filter(|n| !n.trim().is_empty())
    else {
        return Ok(None);
    };
    crate::savepoints::restore(&service.state_files(&pipeline.uuid), name.trim())
        .map_err(|e| ConnectorError::fatal(format!("{}: {e}", crate::savepoints::START_FROM)))
}

#[cfg(feature = "kafka")]
fn static_membership(mut config: Value, pipeline: &str) -> Value {
    let Ok(instance) = std::env::var("MAGE_SERVICE_INSTANCE_ID") else {
        return config;
    };
    let instance = instance.trim();
    if instance.is_empty() {
        return config;
    }
    if let Some(settings) = config.as_object_mut() {
        let extra = settings
            .entry("librdkafka")
            .or_insert_with(|| Value::Object(Default::default()));
        if let Some(extra) = extra.as_object_mut() {
            extra
                .entry("group.instance.id")
                .or_insert_with(|| Value::String(format!("{instance}-{pipeline}")));
        }
    }
    config
}

/// `scope` names the stream for the sinks' object names and deduplication keys (see
/// [`sink_scope`]).
fn sink(
    service: &Service,
    scope: &str,
    connector: &Connector,
) -> Result<Arc<dyn Sink>, ConnectorError> {
    let _config = settings(service, connector);
    match connector.kind {
        ConnectorKind::Stdout => Ok(Arc::new(StdoutSink)),
        ConnectorKind::Http => Ok(Arc::new(mage_connector_http::HttpSink::new(
            &_config, scope,
        )?)),
        ConnectorKind::Feast => Ok(Arc::new(mage_connector_http::FeastSink::new(
            &_config, scope,
        )?)),
        ConnectorKind::Amplitude => Ok(Arc::new(mage_connector_http::AmplitudeSink::new(
            &_config, scope,
        )?)),
        #[cfg(feature = "kafka")]
        ConnectorKind::Kafka => Ok(Arc::new(mage_connector_kafka::KafkaSink::new(&_config)?)),
        #[cfg(feature = "rabbitmq")]
        ConnectorKind::Rabbitmq => Ok(Arc::new(mage_connector_rabbitmq::RabbitSink::new(
            &_config, scope,
        )?)),
        #[cfg(feature = "clickhouse")]
        ConnectorKind::Clickhouse => Ok(Arc::new(mage_connector_clickhouse::ClickHouseSink::new(
            &_config, scope,
        )?)),
        #[cfg(feature = "postgres")]
        ConnectorKind::Postgres => Ok(Arc::new(mage_connector_postgres::PgSink::new(&_config)?)),
        #[cfg(feature = "object-store")]
        ConnectorKind::ObjectStorage => Ok(Arc::new(mage_connector_objectstore::ObjectSink::new(
            &_config, scope,
        )?)),
        #[cfg(feature = "mongodb")]
        ConnectorKind::Mongodb => Ok(Arc::new(mage_connector_mongodb::MongoSink::new(&_config)?)),
        #[cfg(feature = "pubsub")]
        ConnectorKind::Pubsub => Ok(Arc::new(mage_connector_pubsub::PubSubSink::new(&_config)?)),
        #[cfg(feature = "search")]
        ConnectorKind::Search => Ok(Arc::new(mage_connector_search::SearchSink::new(&_config)?)),
        #[cfg(feature = "influxdb")]
        ConnectorKind::Influxdb => Ok(Arc::new(mage_connector_influxdb::InfluxDbSink::new(
            &_config,
        )?)),
        #[cfg(feature = "iceberg")]
        ConnectorKind::Iceberg => Ok(Arc::new(mage_connector_iceberg::IcebergSink::new(
            &_config, scope,
        )?)),
        #[cfg(feature = "activemq")]
        ConnectorKind::Activemq => Ok(Arc::new(mage_connector_activemq::ActiveMqSink::new(
            &_config,
        )?)),
        #[cfg(feature = "nats")]
        ConnectorKind::Nats => Ok(Arc::new(mage_connector_nats::NatsSink::new(
            &_config, scope,
        )?)),
        #[cfg(feature = "ibm-mq")]
        ConnectorKind::IbmMq => Ok(Arc::new(mage_connector_ibmmq::MqSink::new(
            &_config, scope,
        )?)),
        // Kafka without the kafka feature; with it, every kind has its own arm.
        #[allow(unreachable_patterns)]
        kind => Err(ConnectorError::fatal(format!(
            "this service cannot write a {} sink: {}",
            kind.as_str(),
            unavailable(kind)
        ))),
    }
}

fn unavailable(kind: ConnectorKind) -> &'static str {
    match kind {
        ConnectorKind::Kafka => "it was built without Kafka; build it with the kafka feature",
        ConnectorKind::Rabbitmq => {
            "it was built without RabbitMQ; build it with the rabbitmq feature"
        }
        ConnectorKind::IbmMq => "it was built without IBM MQ; build it with the ibm-mq feature",
        ConnectorKind::Nats => "it was built without NATS; build it with the nats feature",
        ConnectorKind::Mongodb => "it was built without MongoDB; build it with the mongodb feature",
        ConnectorKind::ObjectStorage => {
            "it was built without object storage; build it with the object-store feature"
        }
        ConnectorKind::Postgres => {
            "it was built without PostgreSQL; build it with the postgres feature"
        }
        ConnectorKind::Clickhouse => {
            "it was built without ClickHouse; build it with the clickhouse feature"
        }
        ConnectorKind::Sqs => "it was built without SQS; build it with the sqs feature",
        ConnectorKind::Pubsub => "it was built without Pub/Sub; build it with the pubsub feature",
        ConnectorKind::Activemq => {
            "it was built without ActiveMQ; build it with the activemq feature"
        }
        ConnectorKind::Search => {
            "it was built without Elasticsearch and OpenSearch; build it with the search feature"
        }
        ConnectorKind::Influxdb => {
            "it was built without InfluxDB; build it with the influxdb feature"
        }
        ConnectorKind::Iceberg => "it was built without Iceberg; build it with the iceberg feature",
        ConnectorKind::Http => "HTTP is built in",
        ConnectorKind::Stdout | ConnectorKind::Feast | ConnectorKind::Amplitude => {
            "it is a sink only"
        }
    }
}

/// One connector's check, for `mage-service check-connections` and the editor.
pub struct Checked {
    pub pipeline: String,
    /// `source`, `sink 1`, `dead letters`.
    pub role: String,
    pub kind: &'static str,
    pub report: mage_stream_ports::check::ConnectionReport,
}

/// Checks the source, sinks and dead-letter sink of each stream (or of `only`), with
/// settings filled from `lookup` as the stream fills them. Checks only read.
pub async fn check_connections(
    manifest: &Manifest,
    only: Option<&str>,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Vec<Checked> {
    let mut checked = Vec::new();
    for pipeline in &manifest.pipelines {
        let Some(stream) = &pipeline.stream else {
            continue;
        };
        if only.is_some_and(|name| name != pipeline.uuid) {
            continue;
        }
        let mut connectors = vec![("source".to_string(), &stream.source, true)];
        for (index, sink) in stream.sinks.iter().enumerate() {
            connectors.push((format!("sink {}", index + 1), sink, false));
        }
        if let Some(sink) = &stream.errors.dead_letter {
            connectors.push(("dead letters".to_string(), sink, false));
        }
        for (role, connector, source) in connectors {
            let config = io_config::render(&Value::Object(connector.config.clone()), lookup);
            checked.push(Checked {
                pipeline: pipeline.uuid.clone(),
                role,
                kind: connector.kind.as_str(),
                report: check_connector(connector.kind, &config, source).await,
            });
        }
    }
    checked
}

async fn check_connector(
    kind: ConnectorKind,
    config: &Value,
    source: bool,
) -> mage_stream_ports::check::ConnectionReport {
    use mage_stream_ports::check::ConnectionReport;
    match kind {
        ConnectorKind::Stdout => {
            ConnectionReport::passed("Writes to the service's output; nothing to reach.")
        }
        ConnectorKind::Http if source => ConnectionReport::passed(
            "Receives messages at POST /v1/streams/{pipeline}/messages; nothing to reach.",
        ),
        ConnectorKind::Postgres if source => {
            mage_connector_postgres::cdc::check_source(config).await
        }
        ConnectorKind::Postgres => mage_connector_postgres::check_sink(config).await,
        #[cfg(feature = "kafka")]
        ConnectorKind::Kafka => mage_connector_kafka::check::check(config, source).await,
        #[cfg(feature = "rabbitmq")]
        ConnectorKind::Rabbitmq => mage_connector_rabbitmq::check(config, source).await,
        #[cfg(feature = "nats")]
        ConnectorKind::Nats => mage_connector_nats::check(config, source).await,
        #[cfg(feature = "iceberg")]
        ConnectorKind::Iceberg => match mage_connector_iceberg::check(config).await {
            Ok(found) => ConnectionReport::passed(format!("Reached the catalog: {found}.")),
            Err(error) => ConnectionReport::failed(error.message),
        },
        #[allow(unreachable_patterns)]
        ConnectorKind::Kafka | ConnectorKind::Rabbitmq | ConnectorKind::Nats => {
            ConnectionReport::failed(format!(
                "This service cannot connect: {}.",
                unavailable(kind)
            ))
        }
        other => ConnectionReport::not_checked(format!(
            "{} has no connection check yet; the stream's start reports what fails.",
            other.as_str()
        )),
    }
}

fn in_flight_bytes(stream: &mage_service_core::manifest::Stream) -> u64 {
    (stream.batch.max_bytes as u64).saturating_mul(4)
}

fn runner_config(stream: &mage_service_core::manifest::Stream) -> RunnerConfig {
    let sinks: Vec<SinkId> = (0..stream.sinks.len()).map(|i| SinkId(i as u16)).collect();
    let errors = &stream.errors;
    let delay = Duration::from_secs_f64(errors.delay_seconds);
    let max_delay = Duration::from_secs_f64(errors.max_delay_seconds);
    RunnerConfig {
        tracker: TrackerConfig {
            required_sinks: sinks.clone(),
            max_in_flight_bytes: in_flight_bytes(stream),
            max_in_flight_batches: 8,
        },
        sinks,
        transform_retry: RetryPolicy {
            retries: errors.retries,
            delay,
            max_delay,
        },
        sink_retry: RetryPolicy {
            retries: errors.retries,
            delay,
            max_delay,
        },
        poison: if errors.dead_letter.is_some() {
            PoisonPolicy::DeadLetter
        } else {
            PoisonPolicy::Stop
        },
        has_dead_letter: errors.dead_letter.is_some(),
        alert_after: 5,
        exactly_once: stream.delivery == mage_service_core::manifest::Delivery::ExactlyOnce,
    }
}

/// The name sinks give the stream in object names and deduplication keys, with positions:
/// the pipeline's when its source's positions survive a restart (Kafka), otherwise the
/// pipeline's and this run's. A source that numbers its messages from 1 again at each start
/// would otherwise make a sink overwrite the objects of the last run, or drop new messages
/// as ones it already wrote (NATS message ids, ClickHouse insert tokens); a batch retried
/// within a run keeps its keys.
fn sink_scope(pipeline: &str, durable_positions: bool) -> String {
    if durable_positions {
        pipeline.to_string()
    } else {
        let run = uuid::Uuid::new_v4().simple().to_string();
        format!("{pipeline}~{}", &run[..16])
    }
}

/// A sink's own scope: two sinks writing to one place (a NATS stream, a ClickHouse table, a
/// bucket) after different blocks share the source's positions, so keys made from the stream
/// alone collided and the second sink's messages were dropped as duplicates.
fn own_scope(scope: &str, connector: &Connector) -> String {
    format!("{scope}.{}", connector.name)
}

fn parts(service: &Arc<Service>, pipeline: &Pipeline, log: RunLog) -> Result<StreamParts, String> {
    let stream = pipeline.stream.as_ref().expect("a stream");
    if stream.delivery == mage_service_core::manifest::Delivery::ExactlyOnce {
        return exactly_once_parts(service, pipeline, log);
    }
    let source = source(service, pipeline, &stream.source).map_err(|e| e.message)?;
    let scope = sink_scope(&pipeline.uuid, source.capabilities().durable_positions);
    let mut sinks = Vec::new();
    for connector in &stream.sinks {
        sinks.push((
            sink(service, &own_scope(&scope, connector), connector).map_err(|e| e.message)?,
            connector.after.clone(),
        ));
    }
    let dead_letter = match &stream.errors.dead_letter {
        Some(connector) => {
            Some(sink(service, &own_scope(&scope, connector), connector).map_err(|e| e.message)?)
        }
        None => None,
    };
    // Message data in memory, for the editor's tap: development runs only.
    let tap = std::env::var("MAGE_SERVICE_TAP").is_ok_and(|v| v == "on" || v == "1");
    let transform = transform_session(service, pipeline, log, tap)?;
    Ok(StreamParts {
        name: pipeline.uuid.clone(),
        source,
        sinks,
        dead_letter,
        transform,
        config: runner_config(stream),
        shutdown_deadline: Duration::from_secs(20),
        pause: None,
        tap,
        time: stream.time.clone().map(crate::stream_time::Lateness::new),
        transaction: None,
    })
}

/// Exactly-once delivery: a Kafka source whose sinks, dead letters included, write through
/// one transaction that commits the source's positions with them, a Kafka one (I-36) or a
/// PostgreSQL one (EXACTLY_ONCE_POSTGRES.md).
#[cfg(feature = "kafka")]
fn exactly_once_parts(
    service: &Arc<Service>,
    pipeline: &Pipeline,
    log: RunLog,
) -> Result<StreamParts, String> {
    use mage_service_core::manifest::ExactlyOnceTarget;

    let stream = pipeline.stream.as_ref().expect("a stream");
    match stream.exactly_once_target()? {
        ExactlyOnceTarget::Kafka => kafka_to_kafka_parts(service, pipeline, log),
        ExactlyOnceTarget::Postgres => kafka_to_postgres_parts(service, pipeline, log),
        #[cfg(feature = "iceberg")]
        ExactlyOnceTarget::Iceberg => kafka_to_iceberg_parts(service, pipeline, log),
        #[cfg(not(feature = "iceberg"))]
        ExactlyOnceTarget::Iceberg => Err(format!(
            "delivery: exactly_once into Iceberg: this service {}.",
            unavailable(ConnectorKind::Iceberg)
        )),
    }
}

/// Kafka sinks on the source's cluster write through one Kafka transaction that commits the
/// source's offsets (I-36).
#[cfg(feature = "kafka")]
fn kafka_to_kafka_parts(
    service: &Arc<Service>,
    pipeline: &Pipeline,
    log: RunLog,
) -> Result<StreamParts, String> {
    let stream = pipeline.stream.as_ref().expect("a stream");
    let connectors: Vec<&Connector> = std::iter::once(&stream.source)
        .chain(&stream.sinks)
        .chain(stream.errors.dead_letter.as_ref())
        .collect();
    let cluster =
        mage_connector_kafka::cluster(&settings(service, &stream.source)).map_err(|e| e.message)?;
    for connector in &connectors[1..] {
        let other =
            mage_connector_kafka::cluster(&settings(service, connector)).map_err(|e| e.message)?;
        if other != cluster {
            return Err(format!(
                "delivery: exactly_once commits the source's offsets with the sinks' writes, \
                 so they must be on the source's cluster ({cluster}); {} writes to {other}.",
                connector.name
            ));
        }
    }
    let tap = std::env::var("MAGE_SERVICE_TAP").is_ok_and(|v| v == "on" || v == "1");
    let transform = transform_session(service, pipeline, log, tap)?;
    let starts = from_savepoint(service, pipeline).map_err(|e| e.message)?;
    let source =
        kafka_source(pipeline, settings(service, &stream.source), starts).map_err(|e| e.message)?;
    // Stable per instance: a restart fences the producer it replaces.
    let instance = std::env::var("MAGE_SERVICE_INSTANCE_ID")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "single".to_string());
    let transaction = source
        .transaction(
            &settings(service, connectors[1]),
            &format!("mageml-{}-{}", pipeline.uuid, instance.trim()),
        )
        .map_err(|e| e.message)?;
    let mut sinks = Vec::new();
    for connector in &stream.sinks {
        let sink = transaction
            .sink(&settings(service, connector))
            .map_err(|e| e.message)?;
        sinks.push((Arc::new(sink) as Arc<dyn Sink>, connector.after.clone()));
    }
    let dead_letter = match &stream.errors.dead_letter {
        Some(connector) => Some(Arc::new(
            transaction
                .sink(&settings(service, connector))
                .map_err(|e| e.message)?,
        ) as Arc<dyn Sink>),
        None => None,
    };
    Ok(StreamParts {
        name: pipeline.uuid.clone(),
        source: Box::new(source),
        sinks,
        dead_letter,
        transform,
        config: runner_config(stream),
        shutdown_deadline: Duration::from_secs(20),
        pause: None,
        tap,
        time: stream.time.clone().map(crate::stream_time::Lateness::new),
        transaction: Some(Arc::new(transaction)),
    })
}

/// PostgreSQL sinks on one database write through one PostgreSQL transaction that keeps the
/// source's positions in `mageml_stream_positions`; the source starts each partition it is
/// assigned where that table says (EXACTLY_ONCE_POSTGRES.md).
#[cfg(feature = "kafka")]
fn kafka_to_postgres_parts(
    service: &Arc<Service>,
    pipeline: &Pipeline,
    log: RunLog,
) -> Result<StreamParts, String> {
    let stream = pipeline.stream.as_ref().expect("a stream");
    let outputs: Vec<&Connector> = stream
        .sinks
        .iter()
        .chain(stream.errors.dead_letter.as_ref())
        .collect();
    let transaction = mage_connector_postgres::transaction::PgTransaction::new(
        &settings(service, outputs[0]),
        &pipeline.uuid,
    )
    .map_err(|e| e.message)?;
    for connector in &outputs[1..] {
        if !transaction
            .same_database(&settings(service, connector))
            .map_err(|e| e.message)?
        {
            return Err(format!(
                "delivery: exactly_once commits the rows of every sink in one PostgreSQL \
                 transaction, so they must write to one database as one user; {} writes \
                 elsewhere than {}.",
                connector.name, outputs[0].name
            ));
        }
    }
    if std::env::var(crate::savepoints::START_FROM).is_ok_and(|n| !n.trim().is_empty()) {
        return Err(format!(
            "{} with exactly-once delivery into PostgreSQL: the partitions start where \
             mageml_stream_positions says, so a savepoint's offsets would not apply; start \
             without it.",
            crate::savepoints::START_FROM
        ));
    }
    let tap = std::env::var("MAGE_SERVICE_TAP").is_ok_and(|v| v == "on" || v == "1");
    let transform = transform_session(service, pipeline, log, tap)?;
    let batch = &stream.batch;
    let source = mage_connector_kafka::KafkaSource::with_start_positions(
        &static_membership(settings(service, &stream.source), &pipeline.uuid),
        mage_connector_kafka::BatchLimits {
            max_records: batch.max_records,
            max_bytes: batch.max_bytes,
            max_wait: Duration::from_millis(batch.max_wait_ms),
        },
        in_flight_bytes(stream),
        Some(transaction.clone() as Arc<dyn mage_stream_ports::StartPositions>),
    )
    .map_err(|e| e.message)?;
    transaction.name_positions_with(source.position_names());
    let mut sinks = Vec::new();
    for connector in &stream.sinks {
        let sink = transaction
            .sink(&settings(service, connector))
            .map_err(|e| e.message)?;
        sinks.push((Arc::new(sink) as Arc<dyn Sink>, connector.after.clone()));
    }
    let dead_letter = match &stream.errors.dead_letter {
        Some(connector) => Some(Arc::new(
            transaction
                .sink(&settings(service, connector))
                .map_err(|e| e.message)?,
        ) as Arc<dyn Sink>),
        None => None,
    };
    Ok(StreamParts {
        name: pipeline.uuid.clone(),
        source: Box::new(source),
        sinks,
        dead_letter,
        transform,
        config: runner_config(stream),
        shutdown_deadline: Duration::from_secs(20),
        pause: None,
        tap,
        time: stream.time.clone().map(crate::stream_time::Lateness::new),
        transaction: Some(transaction as Arc<dyn mage_stream_ports::Transaction>),
    })
}

/// One Iceberg sink whose commits keep the source's positions as table properties; the
/// source starts each partition it is assigned where the table says (ADR-0042).
#[cfg(all(feature = "kafka", feature = "iceberg"))]
fn kafka_to_iceberg_parts(
    service: &Arc<Service>,
    pipeline: &Pipeline,
    log: RunLog,
) -> Result<StreamParts, String> {
    let stream = pipeline.stream.as_ref().expect("a stream");
    let transaction = mage_connector_iceberg::transaction::IcebergTransaction::new(
        &settings(service, &stream.sinks[0]),
        &pipeline.uuid,
    )
    .map_err(|e| e.message)?;
    if std::env::var(crate::savepoints::START_FROM).is_ok_and(|n| !n.trim().is_empty()) {
        return Err(format!(
            "{} with exactly-once delivery into Iceberg: the partitions start where the \
             table's properties say, so a savepoint's offsets would not apply; start without it.",
            crate::savepoints::START_FROM
        ));
    }
    let tap = std::env::var("MAGE_SERVICE_TAP").is_ok_and(|v| v == "on" || v == "1");
    let transform = transform_session(service, pipeline, log, tap)?;
    let batch = &stream.batch;
    let source = mage_connector_kafka::KafkaSource::with_start_positions(
        &static_membership(settings(service, &stream.source), &pipeline.uuid),
        mage_connector_kafka::BatchLimits {
            max_records: batch.max_records,
            max_bytes: batch.max_bytes,
            max_wait: Duration::from_millis(batch.max_wait_ms),
        },
        in_flight_bytes(stream),
        Some(transaction.clone() as Arc<dyn mage_stream_ports::StartPositions>),
    )
    .map_err(|e| e.message)?;
    transaction.name_positions_with(source.position_names());
    let sink = Arc::new(transaction.sink()) as Arc<dyn Sink>;
    Ok(StreamParts {
        name: pipeline.uuid.clone(),
        source: Box::new(source),
        sinks: vec![(sink, stream.sinks[0].after.clone())],
        dead_letter: None,
        transform,
        config: runner_config(stream),
        shutdown_deadline: Duration::from_secs(20),
        pause: None,
        tap,
        time: stream.time.clone().map(crate::stream_time::Lateness::new),
        transaction: Some(transaction as Arc<dyn mage_stream_ports::Transaction>),
    })
}

#[cfg(not(feature = "kafka"))]
fn exactly_once_parts(
    _service: &Arc<Service>,
    _pipeline: &Pipeline,
    _log: RunLog,
) -> Result<StreamParts, String> {
    Err("delivery: exactly_once needs the engine built with Kafka (the kafka feature).".into())
}

/// The session that runs a stream's blocks: one Python or Rust session when they are all in
/// one language; for a chain that mixes them, one session per run of blocks in a language,
/// each taking the messages of the run before it. With `tap` (development runs), every
/// block's output comes back for the editor's tap, not only those a sink follows: a block in
/// the middle of a run was not tapped (I-186).
fn transform_session(
    service: &Arc<Service>,
    pipeline: &Pipeline,
    log: RunLog,
    tap: bool,
) -> Result<Option<Box<dyn TransformSession>>, String> {
    let stream = pipeline.stream.as_ref().expect("a stream");
    if pipeline.blocks.is_empty() {
        return Ok(None);
    }
    let order: Vec<Block> = pipeline
        .topological_order()
        .ok_or("the stream's blocks have a cycle")?
        .into_iter()
        .cloned()
        .collect();
    if let Some(block) = order.iter().find(|b| {
        !matches!(
            b.language,
            Language::Python
                | Language::Rust
                | Language::R
                | Language::Sql
                | Language::Window
                | Language::Lookup
        )
    }) {
        return Err(format!(
            "stream block {} is in {:?}; stream blocks run in Python, Rust, R or SQL, or are \
             windows or lookups",
            block.uuid, block.language
        ));
    }
    let wanted: BTreeSet<String> = if tap {
        order.iter().map(|b| b.uuid.clone()).collect()
    } else {
        stream
            .sinks
            .iter()
            .filter_map(|s| s.after.clone())
            .collect()
    };
    let dead_letters = stream.errors.dead_letter.is_some();
    let source_shape = source_shape(&stream.source);
    let regions = regions(&order);
    let session = |blocks: &[Block], outputs: Vec<String>, first: bool, log: RunLog| {
        region_session(
            service,
            pipeline,
            blocks,
            outputs,
            first && dead_letters,
            if first {
                source_shape
            } else {
                MessageShape::Values
            },
            log,
        )
    };
    if regions.len() == 1 {
        return session(&order, wanted.into_iter().collect(), true, log).map(Some);
    }
    // Mixed languages or workers: one chain, each block reading the one before.
    for (position, block) in order.iter().enumerate() {
        let expected: Vec<&str> = order[..position]
            .last()
            .map(|b| vec![b.uuid.as_str()])
            .unwrap_or_default();
        if block
            .upstream
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != expected
        {
            return Err(format!(
                "stream block {} does not read the block before it; a stream that mixes \
                 languages or Python workers runs its blocks as one chain",
                block.uuid
            ));
        }
    }
    let mut chained = Vec::with_capacity(regions.len());
    for (index, mut blocks) in regions.into_iter().enumerate() {
        let last = blocks.last().expect("a block").uuid.clone();
        let mut outputs: Vec<String> = blocks
            .iter()
            .filter(|b| wanted.contains(&b.uuid))
            .map(|b| b.uuid.clone())
            .collect();
        if !outputs.contains(&last) {
            outputs.push(last.clone());
        }
        // The run's first block takes the run's input, not a block of another session.
        blocks[0].upstream.clear();
        chained.push((session(&blocks, outputs, index == 0, log.clone())?, last));
    }
    let broadcast = service
        .broadcast(&pipeline.uuid)
        .map_err(|e| format!("its broadcast values: {e}"))?;
    Ok(Some(Box::new(crate::stream_chain::ChainedSession::new(
        chained,
        wanted,
        Some(broadcast),
    ))))
}

/// The stream's blocks in runs of one session each: each window and lookup block is a
/// session of its own, Python blocks split where their Python worker changes (each worker is a pool of its
/// own interpreter), and R and Rust blocks where their environment does (each environment
/// is a program of its own).
fn regions(order: &[Block]) -> Vec<Vec<Block>> {
    let mut regions: Vec<Vec<Block>> = Vec::new();
    for block in order.iter().cloned() {
        match regions.last_mut() {
            Some(region)
                if region[0].language == block.language
                    && !matches!(
                        block.language,
                        Language::Window | Language::Lookup | Language::Sql
                    )
                    && region[0].python_worker == block.python_worker
                    && region[0].language_environment() == block.language_environment() =>
            {
                region.push(block)
            }
            _ => regions.push(vec![block]),
        }
    }
    regions
}

/// How the source's messages reach the first blocks, as Mage hands them over.
fn source_shape(source: &Connector) -> MessageShape {
    let raw = source
        .config
        .get("serde_config")
        .and_then(|serde| serde.get("serialization_method"))
        .and_then(Value::as_str)
        .is_some_and(|method| method.eq_ignore_ascii_case("RAW_VALUE"));
    if source.kind == ConnectorKind::Rabbitmq {
        MessageShape::RabbitmqPayload
    } else if (raw && source.kind == ConnectorKind::Sqs) || source.kind == ConnectorKind::Activemq {
        // Mage's SQS source hands RAW_VALUE bodies over as text, its ActiveMQ source every
        // body.
        MessageShape::RawText
    } else if source.config.contains_key("event_hubs_connection_string") {
        MessageShape::EventHubsData
    } else if raw {
        MessageShape::RawBytes
    } else {
        MessageShape::Values
    }
}

fn region_session(
    service: &Arc<Service>,
    pipeline: &Pipeline,
    blocks: &[Block],
    outputs: Vec<String>,
    isolate: bool,
    shape: MessageShape,
    log: RunLog,
) -> Result<Box<dyn TransformSession>, String> {
    let cancel = service.stream_stop.child_token();
    // Where blocks with keyed state keep it, and whether positions stay the same across
    // restarts (offsets and log positions do; queue tags do not).
    let state_dir = service.state_files(&pipeline.uuid);
    let stable_positions = matches!(
        pipeline.stream.as_ref().map(|s| s.source.kind),
        Some(ConnectorKind::Kafka | ConnectorKind::Postgres)
    );
    if blocks[0].language == Language::Window {
        let block = &blocks[0];
        let spec: mage_stream_core::window::WindowSpec =
            serde_json::from_value(block.window.clone().unwrap_or(Value::Null))
                .map_err(|e| format!("window block {}: {e}", block.uuid))?;
        let stream = pipeline.stream.as_ref().expect("a stream");
        // Offsets and log positions stay the same across restarts; queue tags do not.
        let stable = matches!(
            stream.source.kind,
            ConnectorKind::Kafka | ConnectorKind::Postgres
        );
        let dir = state_dir.child(&crate::executor::sanitize(&block.uuid));
        return Ok(Box::new(
            crate::stream_window::WindowSession::new(
                &block.uuid,
                spec,
                stream.time.clone(),
                dir,
                stable,
            )
            .map_err(|e| format!("window block {}: {e}", block.uuid))?
            .exactly_once(crate::stream_python::exactly_once(pipeline)),
        ));
    }
    if blocks[0].language == Language::Lookup {
        let block = &blocks[0];
        let named = |e: String| format!("lookup block {}: {e}", block.uuid);
        let mut spec: crate::stream_lookup::LookupSpec =
            serde_json::from_value(block.lookup.clone().unwrap_or(Value::Null))
                .map_err(|e| named(e.to_string()))?;
        // The connection's env_var references, filled in as a connector's are.
        let lookup = |name: &str| {
            service
                .extra_env
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .or_else(|| std::env::var(name).ok())
        };
        spec.connection = io_config::render(&spec.connection, &lookup);
        let fetch = match &spec.history {
            Some(table) => mage_connector_postgres::lookup::PgLookup::history(
                &spec.connection,
                table,
                &spec.match_column,
            ),
            None => mage_connector_postgres::lookup::PgLookup::new(
                &spec.connection,
                &spec.query,
                &spec.match_column,
            ),
        }
        .map_err(|e| named(e.message))?;
        return Ok(Box::new(
            crate::stream_lookup::LookupSession::new(&block.uuid, spec, Box::new(fetch), Some(log))
                .map_err(named)?,
        ));
    }
    let register = |views: Vec<(String, String, crate::keyed_state::StateView)>| {
        let mut registry = service
            .stream_state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        for (block, field, view) in views {
            registry.insert((pipeline.uuid.clone(), block), (field, view));
        }
    };
    let broadcast = service
        .broadcast(&pipeline.uuid)
        .map_err(|e| format!("its broadcast values: {e}"))?;
    if blocks[0].language == Language::Sql {
        let block = &blocks[0];
        let file = service.project_dir().join(&block.file);
        let text = std::fs::read_to_string(&file)
            .map_err(|e| format!("SQL block {}: reading {}: {e}", block.uuid, file.display()))?;
        #[cfg(feature = "sql")]
        return Ok(Box::new(
            crate::stream_sql::SqlSession::new(
                &block.uuid,
                &text,
                broadcast,
                crate::stream_sql::columns_of(&block.configuration)
                    .map_err(|e| format!("SQL block {}: {e}", block.uuid))?,
            )
            .map_err(|e| format!("SQL block {}: {e}", block.uuid))?,
        ));
        #[cfg(not(feature = "sql"))]
        return Err(format!(
            "SQL block {}: this engine is built without SQL stream blocks (the sql feature); \
             an export with a SQL stream block builds it with them ({} characters of SQL)",
            block.uuid,
            text.len()
        ));
    }
    if matches!(blocks[0].language, Language::Rust | Language::R) {
        let session = crate::stream_rust::RustStreamSession::new(
            service.clone(),
            pipeline,
            blocks,
            crate::stream_rust::RustSessionOptions {
                outputs,
                log,
                cancel,
                isolate,
                pool: service.python.clone(),
                state_dir,
                stable_positions,
                time: pipeline.stream.as_ref().and_then(|s| s.time.clone()),
                broadcast,
            },
        )?;
        register(session.state_views());
        return Ok(Box::new(session));
    }
    let pool =
        service
            .python_for(&blocks[0])
            .cloned()
            .ok_or_else(|| match &blocks[0].python_worker {
                Some(alias) => format!("the service has no Python worker {alias}"),
                None => "the stream has Python blocks but the service has no Python worker".into(),
            })?;
    let session = PythonStreamSession::new(
        pool,
        &service.project_dir(),
        pipeline,
        blocks,
        crate::stream_python::SessionOptions {
            outputs,
            log,
            cancel,
            // Messages that fail on their own go to the dead-letter sink when there is one.
            isolate,
            shape,
            state_dir,
            stable_positions,
            time: pipeline.stream.as_ref().and_then(|s| s.time.clone()),
            broadcast,
        },
    );
    register(session.state_views());
    Ok(Box::new(session))
}

/// Starts every stream of the manifest; a stream that cannot start is reported as failed with
/// the reason, and the others run.
pub fn start(service: &Arc<Service>) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    for pipeline in service
        .manifest
        .pipelines
        .iter()
        .filter(|p| p.stream.is_some())
    {
        let status: SharedStatus = Arc::new(Mutex::new(StreamStatus::default()));
        service
            .streams
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(pipeline.uuid.clone(), status.clone());
        let log_path = service
            .data_dir
            .join("streams")
            .join(crate::executor::sanitize(&pipeline.uuid))
            .join("stream.log");
        let (max_bytes, keep) = crate::log::stream_log_limits();
        let log = match RunLog::rotating(&log_path, &pipeline.uuid, max_bytes, keep) {
            Ok(log) => log,
            Err(error) => {
                fail(
                    &status,
                    &pipeline.uuid,
                    &format!("cannot write its log: {error}"),
                );
                continue;
            }
        };
        match parts(service, pipeline, log) {
            Ok(mut parts) => {
                // A stream paused before the service stopped starts paused.
                let paused = service
                    .ledger
                    .settings(&pipeline.uuid, pipeline.max_concurrent_runs)
                    .map(|s| s.paused)
                    .unwrap_or(false);
                let (switch, receiver) = tokio::sync::watch::channel(paused);
                service
                    .stream_pause
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(pipeline.uuid.clone(), switch);
                parts.pause = Some(receiver);
                log::info(&format!("Stream {} starts.", pipeline.uuid));
                if let Ok(broadcast) = service.broadcast(&pipeline.uuid) {
                    handles.push(tokio::spawn(follow_broadcast(
                        pipeline.uuid.clone(),
                        broadcast,
                        service.stream_stop.clone(),
                    )));
                }
                let stop = service.stream_stop.clone();
                handles.push(tokio::spawn(stream::run(parts, status, stop)));
            }
            Err(reason) => fail(&status, &pipeline.uuid, &reason),
        }
    }
    handles
}

/// Reads a stream's broadcast values again every second, so a value set through another
/// instance (on a shared state store) takes effect here; a value set through this one does
/// at once. A store that cannot be read is logged once, and the last values stay.
async fn follow_broadcast(
    pipeline: String,
    broadcast: Arc<crate::broadcast::Broadcast>,
    stop: tokio_util::sync::CancellationToken,
) {
    let mut failing = false;
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }
        let reading = broadcast.clone();
        match tokio::task::spawn_blocking(move || reading.refresh()).await {
            Ok(Ok(changed)) => {
                if failing {
                    log::info(&format!(
                        "Stream {pipeline}: its broadcast values read again."
                    ));
                    failing = false;
                }
                if let Some(snapshot) = changed {
                    log::info(&format!(
                        "Stream {pipeline}: broadcast values now {}.",
                        match snapshot.label() {
                            label if label.is_empty() => "none".to_string(),
                            label => label,
                        }
                    ));
                }
            }
            Ok(Err(error)) if !failing => {
                log::warn(&format!(
                    "Stream {pipeline}: cannot read its broadcast values, so it keeps the \
                     last ones: {error}"
                ));
                failing = true;
            }
            _ => {}
        }
    }
}

fn fail(status: &SharedStatus, pipeline: &str, reason: &str) {
    log::error(&format!("Stream {pipeline} cannot start: {reason}"));
    if let Ok(mut s) = status.lock() {
        s.state = StreamState::Failed;
        s.last_error = Some(reason.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connector(kind: ConnectorKind, config: Value) -> Connector {
        Connector {
            name: "source".into(),
            kind,
            after: None,
            config: config.as_object().cloned().unwrap_or_default(),
        }
    }

    #[test]
    fn stream_blocks_split_into_sessions_by_language_worker_and_environment() {
        let block = |uuid: &str, language: &str, configuration: Value| -> Block {
            serde_json::from_value(serde_json::json!({
                "uuid": uuid, "type": "transformer", "language": language,
                "file": format!("{uuid}.x"), "sha256": "0", "configuration": configuration,
            }))
            .unwrap()
        };
        let order = vec![
            block("a", "r", serde_json::json!({})),
            block("b", "r", serde_json::json!({"r_environment": "default"})),
            block("c", "r", serde_json::json!({"r_environment": "models"})),
            block("d", "r", serde_json::json!({"r_environment": "models"})),
            block("e", "python", serde_json::json!({})),
        ];
        let uuids: Vec<Vec<String>> = regions(&order)
            .iter()
            .map(|r| r.iter().map(|b| b.uuid.clone()).collect())
            .collect();
        assert_eq!(uuids, vec![vec!["a", "b"], vec!["c", "d"], vec!["e"]]);
    }

    #[test]
    fn each_sink_keys_its_messages_by_its_own_name() {
        // Two sinks writing to one NATS stream after different blocks made the same message
        // ids, and JetStream dropped the second sink's messages (I-189).
        let (mut clean, mut alerts) = (
            connector(ConnectorKind::Stdout, serde_json::json!({})),
            connector(ConnectorKind::Stdout, serde_json::json!({})),
        );
        clean.name = "readings_clean".into();
        alerts.name = "readings_alerts".into();
        assert_eq!(own_scope("iot", &clean), "iot.readings_clean");
        assert_ne!(own_scope("iot", &clean), own_scope("iot", &alerts));
    }

    #[test]
    fn sinks_name_a_run_unless_positions_survive_a_restart() {
        // Positions numbered from 1 at each start made a sink overwrite the last run's
        // objects and drop new messages as duplicates (I-101).
        assert_eq!(sink_scope("orders", true), "orders");
        let (a, b) = (sink_scope("orders", false), sink_scope("orders", false));
        assert!(a.starts_with("orders~") && b.starts_with("orders~"));
        assert_ne!(a, b);
    }

    #[test]
    fn json_objects_keep_their_keys_sorted_whatever_the_features() {
        // A dependency that turns on serde_json's preserve_order changes every JSON object
        // of the engine, and only in builds with that dependency (bson 2 did, I-97).
        let value = serde_json::json!({"b": 1, "a": 2});
        assert_eq!(value.to_string(), r#"{"a":2,"b":1}"#);
    }

    #[test]
    fn a_source_without_serde_settings_hands_over_values() {
        // Indexing the settings map with a missing key panicked at start (I-89).
        let kafka = connector(ConnectorKind::Kafka, serde_json::json!({"topic": "t"}));
        assert_eq!(source_shape(&kafka), MessageShape::Values);
        let raw = connector(
            ConnectorKind::Kafka,
            serde_json::json!({"serde_config": {"serialization_method": "raw_value"}}),
        );
        assert_eq!(source_shape(&raw), MessageShape::RawBytes);
        let rabbit = connector(ConnectorKind::Rabbitmq, serde_json::json!({}));
        assert_eq!(source_shape(&rabbit), MessageShape::RabbitmqPayload);
        let hubs = connector(
            ConnectorKind::Kafka,
            serde_json::json!({"event_hubs_connection_string": {"$parts": ["x"]}}),
        );
        assert_eq!(source_shape(&hubs), MessageShape::EventHubsData);
        let sqs = connector(
            ConnectorKind::Sqs,
            serde_json::json!({"serde_config": {"serialization_method": "RAW_VALUE"}}),
        );
        assert_eq!(source_shape(&sqs), MessageShape::RawText);
        let activemq = connector(ConnectorKind::Activemq, serde_json::json!({}));
        assert_eq!(source_shape(&activemq), MessageShape::RawText);
    }
}
