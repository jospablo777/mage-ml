//! RabbitMQ source and sink for streams, through lapin (pure Rust AMQP 0-9-1).
//!
//! Source: one consumer with manual acknowledgement. The broker sends at most `prefetch_count`
//! messages it has not had acknowledged, which bounds what the stream holds; a delivery is
//! acknowledged only when the stream committed past it, cumulatively up to its delivery tag.
//! A delivery tag is valid on its channel only, so each connection is an assignment of its own
//! epoch: when the connection drops, its unacknowledged messages go back to the queue, work in
//! flight is refused at commit, and the broker delivers the messages again (to this consumer
//! or another one) after the reconnection.
//!
//! Sink: messages published persistent, `mandatory`, with publisher confirms awaited before
//! the batch is confirmed; a message no queue took, or that the broker refused, fails the
//! write. Each message carries `message_id = <stream>:<partition>:<position>` for consumers
//! that deduplicate.
//!
//! Settings follow Mage's RabbitMQ YAML: `connection_host`, `connection_port`, `queue_name`,
//! `username`, `password`, `amqp_url_virtual_host` (URL-encoded, `%2f` by default),
//! `url_protocol` (`amqp` or `amqps`), `prefetch_count`; the sink also takes `exchange` and
//! `routing_key` (the queue by default).

use std::time::{Duration, Instant};

use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions,
};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, Consumer};
use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome,
    Confirmation as BatchConfirmation, ConnectorError, Envelope, EnvelopeBuilder, Header,
    PartitionId, Position, RecordBatch, Sink, SinkCapabilities, Source, SourceCapabilities,
    SourceEvent, read_envelope,
};
use serde_json::Value;

const PARTITION: PartitionId = PartitionId(0);

/// How a source groups messages into batches.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_wait: Duration,
}

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Percent-encodes a user name or password for the URL.
fn encode(part: &str) -> String {
    part.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The connection URL, and the same without its credentials for messages.
fn connection_url(config: &Value) -> Result<(String, String), ConnectorError> {
    let host = text(config, "connection_host")
        .ok_or_else(|| ConnectorError::fatal("RabbitMQ settings need connection_host."))?;
    let port = text(config, "connection_port").unwrap_or_else(|| "5672".into());
    let protocol = text(config, "url_protocol").unwrap_or_else(|| "amqp".into());
    if protocol != "amqp" && protocol != "amqps" {
        return Err(ConnectorError::fatal(
            "RabbitMQ's url_protocol is amqp or amqps.",
        ));
    }
    let user = text(config, "username").unwrap_or_else(|| "guest".into());
    let password = text(config, "password").unwrap_or_else(|| "guest".into());
    let vhost = text(config, "amqp_url_virtual_host").unwrap_or_else(|| "%2f".into());
    Ok((
        format!(
            "{protocol}://{}:{}@{host}:{port}/{vhost}",
            encode(&user),
            encode(&password)
        ),
        format!("{protocol}://{host}:{port}/{vhost}"),
    ))
}

fn queue_name(config: &Value) -> Result<String, ConnectorError> {
    text(config, "queue_name")
        .ok_or_else(|| ConnectorError::fatal("RabbitMQ settings need queue_name."))
}

fn amqp_error(label: &str, error: lapin::Error) -> ConnectorError {
    ConnectorError::retryable(format!("RabbitMQ {label}: {error}"))
}

fn header_bytes(value: &AMQPValue) -> Vec<u8> {
    match value {
        AMQPValue::LongString(s) => s.as_bytes().to_vec(),
        AMQPValue::ShortString(s) => s.as_str().as_bytes().to_vec(),
        other => format!("{other:?}").into_bytes(),
    }
}

/// The connection check of a source (`source`) or a sink: the broker, and the queue (its
/// messages and consumers) or the sink's exchange. Passive declarations only, so nothing is
/// made, taken or written.
pub async fn check(config: &Value, source: bool) -> mage_stream_ports::check::ConnectionReport {
    use lapin::options::{ExchangeDeclareOptions, QueueDeclareOptions};
    use mage_stream_ports::check::ConnectionReport;

    let (url, label) = match connection_url(config) {
        Ok(urls) => urls,
        Err(error) => return ConnectionReport::failed(error.to_string()),
    };
    let connecting = Connection::connect(&url, ConnectionProperties::default());
    let connection = match tokio::time::timeout(Duration::from_secs(10), connecting).await {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => {
            return ConnectionReport::failed(format!("Could not connect to {label}: {error}"));
        }
        Err(_) => {
            return ConnectionReport::failed(format!("{label} did not answer within 10 seconds."));
        }
    };
    let mut report = ConnectionReport::passed(format!("Connected to {label}."));
    let exchange = if source {
        String::new()
    } else {
        text(config, "exchange").unwrap_or_default()
    };
    let queue = text(config, "queue_name");
    if !exchange.is_empty() {
        let channel = match connection.create_channel().await {
            Ok(channel) => channel,
            Err(error) => return report.fail(format!("Opening a channel: {error}")),
        };
        let passive = ExchangeDeclareOptions {
            passive: true,
            ..Default::default()
        };
        report = match channel
            .exchange_declare(
                exchange.as_str().into(),
                lapin::ExchangeKind::Direct,
                passive,
                FieldTable::default(),
            )
            .await
        {
            Ok(()) => report.fact("exchange", &exchange),
            Err(error) => report.fail(format!("Exchange {exchange}: {error}")),
        };
    }
    // A source reads the queue; a sink on the default exchange publishes to it by name.
    let queue = if source || exchange.is_empty() {
        queue.or_else(|| text(config, "routing_key"))
    } else {
        None
    };
    if let Some(queue) = queue {
        let channel = match connection.create_channel().await {
            Ok(channel) => channel,
            Err(error) => return report.fail(format!("Opening a channel: {error}")),
        };
        let passive = QueueDeclareOptions {
            passive: true,
            ..Default::default()
        };
        report = match channel
            .queue_declare(queue.as_str().into(), passive, FieldTable::default())
            .await
        {
            Ok(declared) => report
                .fact("queue", &queue)
                .fact("messages ready", declared.message_count())
                .fact("consumers", declared.consumer_count()),
            Err(error) if error.to_string().contains("NOT_FOUND") => {
                report.fail(format!("Queue {queue} does not exist."))
            }
            Err(error) => report.fail(format!("Queue {queue}: {error}")),
        };
    } else if source {
        report = report.fail("The settings name no queue (queue_name).");
    }
    let _ = connection.close(0, "check done".into()).await;
    report
}

struct Live {
    connection: Connection,
    channel: Channel,
    consumer: Consumer,
}

struct Pending {
    builder: EnvelopeBuilder,
    first: u64,
    last: u64,
    started: Instant,
}

pub struct RabbitSource {
    url: String,
    label: String,
    queue: String,
    prefetch: u16,
    limits: BatchLimits,
    live: Option<Live>,
    epoch: u64,
    seq: u64,
    pending: Option<Pending>,
    paused: bool,
    /// The connection was lost: the stream hears a revoke before the next assignment.
    lost: bool,
}

impl RabbitSource {
    pub fn new(config: &Value, limits: BatchLimits) -> Result<RabbitSource, ConnectorError> {
        let (url, label) = connection_url(config)?;
        let prefetch = config
            .get("prefetch_count")
            .and_then(Value::as_u64)
            .unwrap_or((limits.max_records as u64).saturating_mul(2))
            .clamp(1, u16::MAX as u64) as u16;
        Ok(RabbitSource {
            url,
            label,
            queue: queue_name(config)?,
            prefetch,
            limits,
            live: None,
            epoch: 0,
            seq: 0,
            pending: None,
            paused: false,
            lost: false,
        })
    }

    async fn connect(&mut self) -> Result<SourceEvent, ConnectorError> {
        let connection = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| amqp_error(&format!("connecting to {}", self.label), e))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| amqp_error("opening a channel", e))?;
        channel
            .basic_qos(self.prefetch, BasicQosOptions::default())
            .await
            .map_err(|e| amqp_error("setting the prefetch", e))?;
        let consumer = channel
            .basic_consume(
                ShortString::from(self.queue.clone()),
                ShortString::from(""),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| amqp_error(&format!("consuming queue {}", self.queue), e))?;
        self.live = Some(Live {
            connection,
            channel,
            consumer,
        });
        self.epoch += 1;
        self.seq = 0;
        Ok(SourceEvent::Assigned {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
        })
    }

    fn take(&mut self) -> Result<Option<SourceEvent>, ConnectorError> {
        let Some(mut pending) = self.pending.take() else {
            return Ok(None);
        };
        let records = pending.builder.len() as u32;
        let bytes = pending.builder.bytes() as u64;
        let batch = pending
            .builder
            .finish()
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let meta = BatchMeta {
            id: BatchId {
                partition: PARTITION,
                epoch: AssignmentEpoch(self.epoch),
                seq: self.seq,
            },
            first: Position::Tag(pending.first),
            last: Position::Tag(pending.last),
            records,
            bytes,
        };
        self.seq += 1;
        Ok(Some(SourceEvent::Batch { batch, meta }))
    }

    fn full(&self) -> bool {
        self.pending.as_ref().is_some_and(|p| {
            p.builder.len() >= self.limits.max_records || p.builder.bytes() >= self.limits.max_bytes
        })
    }

    fn wait(&self) -> Duration {
        self.pending
            .as_ref()
            .map_or(self.limits.max_wait, |p| {
                (p.started + self.limits.max_wait).saturating_duration_since(Instant::now())
            })
            .max(Duration::from_millis(1))
    }

    fn lose(&mut self) -> SourceEvent {
        self.live = None;
        self.pending = None;
        self.lost = true;
        SourceEvent::Revoking {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
            within: Duration::from_millis(0),
        }
    }
}

impl Source for RabbitSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if self.live.is_none() {
                self.lost = false;
                return self.connect().await;
            }
            loop {
                if self.full() {
                    return Ok(self.take()?.expect("full"));
                }
                if self.paused {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
                }
                let wait = self.wait();
                let live = self.live.as_mut().expect("connected");
                match tokio::time::timeout(wait, live.consumer.next()).await {
                    Ok(Some(Ok(delivery))) => {
                        let tag = delivery.delivery_tag;
                        let headers = delivery
                            .properties
                            .headers()
                            .as_ref()
                            .map(|table| {
                                table
                                    .inner()
                                    .iter()
                                    .map(|(k, v)| Header {
                                        key: k.as_str().to_string(),
                                        value: header_bytes(v),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        let routing_key = delivery.routing_key.as_str();
                        let envelope = Envelope {
                            key: (!routing_key.is_empty()).then(|| routing_key.as_bytes().to_vec()),
                            value: delivery.data,
                            headers,
                            partition: PARTITION.0,
                            position: Some(tag as i64),
                            timestamp: delivery
                                .properties
                                .timestamp()
                                .map(|t| (t as i64).saturating_mul(1000)),
                        };
                        let pending = self.pending.get_or_insert_with(|| Pending {
                            builder: EnvelopeBuilder::new(),
                            first: tag,
                            last: tag,
                            started: Instant::now(),
                        });
                        pending.builder.push(&envelope);
                        pending.last = tag;
                    }
                    Ok(Some(Err(_))) | Ok(None) => return Ok(self.lose()),
                    Err(_) => return Ok(self.take()?.unwrap_or(SourceEvent::Idle)),
                }
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            for commit in commits {
                if commit.epoch != AssignmentEpoch(self.epoch) || self.live.is_none() {
                    return CommitOutcome::Stale;
                }
                let Position::Tag(tag) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "RabbitMQ acknowledges delivery tags",
                    ));
                };
                let channel = &self.live.as_ref().expect("checked").channel;
                if let Err(error) = channel
                    .basic_ack(tag, BasicAckOptions { multiple: true })
                    .await
                {
                    // The channel is gone: its messages return to the queue.
                    self.lose();
                    return CommitOutcome::Failed(amqp_error("acknowledging", error));
                }
            }
            CommitOutcome::Committed
        })
    }

    fn pause(&mut self, _partition: PartitionId) {
        // The broker sends no more than the prefetch while nothing is acknowledged.
        self.paused = true;
    }

    fn resume(&mut self, _partition: PartitionId) {
        self.paused = false;
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            replayable: true,
            ordered: false,
            ack_kind: AckKind::Message,
            max_instances: None,
            needs_keepalive: true,
            durable_positions: false,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Some(live) = self.live.take() {
                let _ = live.channel.close(200, "the stream stopped".into()).await;
                let _ = live
                    .connection
                    .close(200, "the stream stopped".into())
                    .await;
            }
        })
    }
}

pub struct RabbitSink {
    url: String,
    label: String,
    exchange: String,
    routing_key: String,
    stream: String,
    channel: tokio::sync::Mutex<Option<(Connection, Channel)>>,
}

impl RabbitSink {
    /// `stream`: the stream's name, part of each message's id.
    pub fn new(config: &Value, stream: &str) -> Result<RabbitSink, ConnectorError> {
        let (url, label) = connection_url(config)?;
        let queue = text(config, "queue_name");
        let routing_key = text(config, "routing_key").or(queue).ok_or_else(|| {
            ConnectorError::fatal("RabbitMQ sink settings need queue_name or routing_key.")
        })?;
        Ok(RabbitSink {
            url,
            label,
            exchange: text(config, "exchange").unwrap_or_default(),
            routing_key,
            stream: stream.to_string(),
            channel: tokio::sync::Mutex::new(None),
        })
    }

    async fn open(&self) -> Result<(Connection, Channel), ConnectorError> {
        let connection = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| amqp_error(&format!("connecting to {}", self.label), e))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| amqp_error("opening a channel", e))?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| amqp_error("turning publisher confirms on", e))?;
        Ok((connection, channel))
    }
}

impl RabbitSink {
    /// Publishes the batch's messages on the channel, in order, and returns their confirms.
    async fn publish(
        &self,
        batch: &RecordBatch,
        meta: &BatchMeta,
    ) -> Result<Vec<lapin::PublisherConfirm>, ConnectorError> {
        let messages = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let mut guard = self.channel.lock().await;
        if guard.as_ref().is_none_or(|(_, c)| !c.status().connected()) {
            *guard = Some(self.open().await?);
        }
        let channel = guard.as_ref().expect("opened").1.clone();
        let mut confirms = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let mut headers = FieldTable::default();
            for header in &message.headers {
                headers.insert(
                    ShortString::from(header.key.clone()),
                    AMQPValue::LongString(LongString::from(header.value.clone())),
                );
            }
            let properties = BasicProperties::default()
                .with_delivery_mode(2)
                .with_content_type(ShortString::from("application/json"))
                .with_message_id(ShortString::from(format!(
                    "{}:{}:{}",
                    self.stream,
                    meta.id.partition.0,
                    meta.row_key(message.position, index)
                )))
                .with_headers(headers);
            let confirm = channel
                .basic_publish(
                    ShortString::from(self.exchange.clone()),
                    ShortString::from(self.routing_key.clone()),
                    BasicPublishOptions {
                        mandatory: true,
                        ..BasicPublishOptions::default()
                    },
                    &message.value,
                    properties,
                )
                .await;
            match confirm {
                Ok(confirm) => confirms.push(confirm),
                Err(error) => {
                    *guard = None;
                    return Err(amqp_error("publishing", error));
                }
            }
        }
        Ok(confirms)
    }
}

/// Waits for every confirm of a batch; a returned message names the routing key.
async fn confirmed(
    confirms: Vec<lapin::PublisherConfirm>,
    routing_key: String,
    exchange: String,
) -> Result<(), ConnectorError> {
    for confirm in confirms {
        match confirm.await {
            Ok(Confirmation::Ack(None)) | Ok(Confirmation::NotRequested) => {}
            Ok(Confirmation::Ack(Some(_))) => {
                return Err(ConnectorError::fatal(format!(
                    "RabbitMQ: no queue is bound to routing key {routing_key} on exchange \
                     {exchange:?}; the message was returned"
                )));
            }
            Ok(Confirmation::Nack(_)) => {
                return Err(ConnectorError::retryable(
                    "RabbitMQ refused a message (nack)",
                ));
            }
            // A closed channel is opened again by the next publish.
            Err(error) => return Err(amqp_error("waiting for a confirm", error)),
        }
    }
    Ok(())
}

impl Sink for RabbitSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let confirms = self.publish(batch, meta).await?;
            confirmed(confirms, self.routing_key.clone(), self.exchange.clone()).await
        })
    }

    /// A channel keeps the order of what is published on it, so the next batch is published
    /// while this one's confirms come back: a batch waited for its last confirm before the
    /// next was published (I-86).
    fn queue<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<BatchConfirmation, ConnectorError>> {
        Box::pin(async move {
            let confirms = self.publish(batch, meta).await?;
            Ok(Box::pin(confirmed(
                confirms,
                self.routing_key.clone(),
                self.exchange.clone(),
            )) as BatchConfirmation)
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: false,
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_encoded_and_left_out_of_the_label() {
        let (url, label) = connection_url(&serde_json::json!({
            "connection_host": "rabbit", "connection_port": 5671, "url_protocol": "amqps",
            "username": "svc", "password": "p@ss/w:rd", "queue_name": "orders"
        }))
        .unwrap();
        assert_eq!(url, "amqps://svc:p%40ss%2Fw%3Ard@rabbit:5671/%2f");
        assert_eq!(label, "amqps://rabbit:5671/%2f");
        assert!(
            connection_url(&serde_json::json!({"connection_host": "h", "url_protocol": "http"}))
                .is_err()
        );
    }

    /// Against a broker (`MAGE_TEST_RABBITMQ`, host:port; integration_tests/compose.yaml
    /// serves localhost:15673 with its user, which the test uses).
    #[tokio::test]
    async fn a_check_reports_the_queue_or_what_is_missing() {
        use lapin::options::QueueDeclareOptions;
        use mage_stream_ports::check::CheckResult;

        let Ok(address) = std::env::var("MAGE_TEST_RABBITMQ") else {
            eprintln!("MAGE_TEST_RABBITMQ is not set: skipped");
            return;
        };
        let (host, port) = address.rsplit_once(':').unwrap();
        let queue = format!("check_{}", std::process::id());
        let settings = |extra: Value| {
            let mut base = serde_json::json!({"connection_host": host, "connection_port": port,
                                              "queue_name": queue, "username": "mage",
                                              "password": "p@ss/w:rd%"});
            for (key, value) in extra.as_object().unwrap() {
                base[key] = value.clone();
            }
            base
        };
        let missing = check(&settings(serde_json::json!({})), true).await;
        assert_eq!(missing.result, CheckResult::Failed, "{missing:?}");
        assert!(
            missing.problems[0].contains("does not exist"),
            "{missing:?}"
        );

        let (url, _) = connection_url(&settings(serde_json::json!({}))).unwrap();
        let connection = Connection::connect(&url, ConnectionProperties::default())
            .await
            .unwrap();
        let channel = connection.create_channel().await.unwrap();
        channel
            .queue_declare(
                queue.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        for body in [b"1", b"2"] {
            channel
                .basic_publish(
                    "".into(),
                    queue.as_str().into(),
                    BasicPublishOptions::default(),
                    body,
                    BasicProperties::default(),
                )
                .await
                .unwrap()
                .await
                .unwrap();
        }
        let report = check(&settings(serde_json::json!({})), true).await;
        assert_eq!(report.result, CheckResult::Passed, "{report:?}");
        assert!(
            report
                .facts
                .contains(&("messages ready".to_string(), "2".to_string())),
            "{report:?}"
        );

        let sink = check(
            &settings(serde_json::json!({"exchange": "no_such_exchange"})),
            false,
        )
        .await;
        assert_eq!(sink.result, CheckResult::Failed, "{sink:?}");
        assert!(sink.problems[0].contains("no_such_exchange"), "{sink:?}");
        let wrong = check(&settings(serde_json::json!({"password": "wrong"})), true).await;
        assert_eq!(wrong.result, CheckResult::Failed, "{wrong:?}");
        channel
            .queue_delete(queue.as_str().into(), Default::default())
            .await
            .unwrap();
    }
}
