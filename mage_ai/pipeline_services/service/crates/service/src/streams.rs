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
use mage_service_core::manifest::{Connector, ConnectorKind, Language, Manifest, Pipeline};
use mage_stream_core::{PoisonPolicy, RetryPolicy, RunnerConfig, SinkId, TrackerConfig};
use mage_stream_ports::{ConnectorError, Sink, Source, TransformSession};
use serde_json::Value;
use tokio::task::JoinHandle;

use crate::log::{self, RunLog};
use crate::service::Service;
use crate::stream::{self, SharedStatus, StreamParts, StreamState, StreamStatus};
use crate::stream_python::PythonStreamSession;

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
            let stream = pipeline.stream.as_ref().expect("a stream");
            let batch = &stream.batch;
            let limits = mage_connector_kafka::BatchLimits {
                max_records: batch.max_records,
                max_bytes: batch.max_bytes,
                max_wait: Duration::from_millis(batch.max_wait_ms),
            };
            Ok(Box::new(mage_connector_kafka::KafkaSource::new(
                &static_membership(_config, &pipeline.uuid),
                limits,
                in_flight_bytes(stream),
            )?))
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
}

/// With `MAGE_SERVICE_INSTANCE_ID` (a stable name per replica, such as a StatefulSet pod's),
/// the consumer is a static group member (I-39): a restart within the session timeout keeps
/// its partitions instead of moving every partition of the group twice.
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

fn sink(
    service: &Service,
    pipeline: &Pipeline,
    connector: &Connector,
) -> Result<Arc<dyn Sink>, ConnectorError> {
    let _config = settings(service, connector);
    match connector.kind {
        ConnectorKind::Stdout => Ok(Arc::new(StdoutSink)),
        ConnectorKind::Http => Ok(Arc::new(mage_connector_http::HttpSink::new(
            &_config,
            &pipeline.uuid,
        )?)),
        ConnectorKind::Feast => Ok(Arc::new(mage_connector_http::FeastSink::new(
            &_config,
            &pipeline.uuid,
        )?)),
        #[cfg(feature = "kafka")]
        ConnectorKind::Kafka => Ok(Arc::new(mage_connector_kafka::KafkaSink::new(&_config)?)),
        #[cfg(feature = "rabbitmq")]
        ConnectorKind::Rabbitmq => Ok(Arc::new(mage_connector_rabbitmq::RabbitSink::new(
            &_config,
            &pipeline.uuid,
        )?)),
        #[cfg(feature = "ibm-mq")]
        ConnectorKind::IbmMq => Ok(Arc::new(mage_connector_ibmmq::MqSink::new(
            &_config,
            &pipeline.uuid,
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
        ConnectorKind::Http => "HTTP is built in",
        ConnectorKind::Stdout | ConnectorKind::Feast => "it is a sink only",
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
    }
}

fn parts(service: &Arc<Service>, pipeline: &Pipeline, log: RunLog) -> Result<StreamParts, String> {
    let stream = pipeline.stream.as_ref().expect("a stream");
    let source = source(service, pipeline, &stream.source).map_err(|e| e.message)?;
    let mut sinks = Vec::new();
    for connector in &stream.sinks {
        sinks.push((
            sink(service, pipeline, connector).map_err(|e| e.message)?,
            connector.after.clone(),
        ));
    }
    let dead_letter = match &stream.errors.dead_letter {
        Some(connector) => Some(sink(service, pipeline, connector).map_err(|e| e.message)?),
        None => None,
    };
    let transform: Option<Box<dyn TransformSession>> = if pipeline.blocks.is_empty() {
        None
    } else {
        if pipeline
            .blocks
            .iter()
            .any(|b| b.language != Language::Python)
        {
            return Err(
                "this service runs Python stream blocks; Rust stream blocks come in a later \
                 step of streaming"
                    .into(),
            );
        }
        let pool = service
            .python
            .clone()
            .ok_or("the stream has Python blocks but the service has no Python worker")?;
        let order: Vec<_> = pipeline
            .topological_order()
            .ok_or("the stream's blocks have a cycle")?
            .into_iter()
            .cloned()
            .collect();
        let outputs: Vec<String> = stream
            .sinks
            .iter()
            .filter_map(|s| s.after.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Some(Box::new(PythonStreamSession::new(
            pool,
            &service.project_dir(),
            pipeline,
            &order,
            crate::stream_python::SessionOptions {
                outputs,
                log,
                cancel: service.stream_stop.child_token(),
                // Messages that fail on their own go to the dead-letter sink when there is one.
                isolate: stream.errors.dead_letter.is_some(),
                shape: if stream.source.kind == ConnectorKind::Rabbitmq {
                    crate::stream_python::MessageShape::RabbitmqPayload
                } else {
                    crate::stream_python::MessageShape::Values
                },
            },
        )))
    };
    Ok(StreamParts {
        name: pipeline.uuid.clone(),
        source,
        sinks,
        dead_letter,
        transform,
        config: runner_config(stream),
        shutdown_deadline: Duration::from_secs(20),
        pause: None,
    })
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
                let stop = service.stream_stop.clone();
                handles.push(tokio::spawn(stream::run(parts, status, stop)));
            }
            Err(reason) => fail(&status, &pipeline.uuid, &reason),
        }
    }
    handles
}

fn fail(status: &SharedStatus, pipeline: &str, reason: &str) {
    log::error(&format!("Stream {pipeline} cannot start: {reason}"));
    if let Ok(mut s) = status.lock() {
        s.state = StreamState::Failed;
        s.last_error = Some(reason.to_string());
    }
}
