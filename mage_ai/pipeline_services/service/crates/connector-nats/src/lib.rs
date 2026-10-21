//! NATS JetStream source and sink for streams, with MageML's NATS settings (`server_url`,
//! `stream_name`, `subject`, `consumer_name`, `ack_wait`, `user_credentials`,
//! `nkeys_seed_str`, `use_tls`, `ssl_config`).
//!
//! Source: a durable pull consumer, named `consumer_name` or the stream's name as in MageML,
//! on `stream_name`, made when it does not exist. Each message is acknowledged once every sink took
//! its batch; one not acknowledged in `ack_wait` is delivered again (at-least-once), and
//! several instances share the consumer's messages. A consumer the source makes allows ten
//! batches unacknowledged (JetStream's default of 1,000 is smaller than one large batch).
//!
//! Sink: a `stream_name` is made when it does not exist; without one, messages go to the
//! stream that already takes the subject. Each message is published with a `Nats-Msg-Id` made from the stream, partition and
//! position, so JetStream drops a message a replayed batch publishes again within its
//! duplicate window; the batch is confirmed when JetStream acknowledged all of it. As in
//! MageML, a message `{"data": ..., "metadata": {...}}` publishes its data with the metadata
//! as headers.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use async_nats::HeaderMap;
use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self, Context, stream};
use futures_util::{FutureExt, StreamExt};
use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, Confirmation,
    ConnectorError, Envelope, EnvelopeBuilder, Header, PartitionId, Position, RecordBatch, Sink,
    SinkCapabilities, Source, SourceCapabilities, SourceEvent, read_envelope,
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

fn nats_error(label: &str, error: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::retryable(format!("NATS {label}: {error}"))
}

/// The connection settings both directions share.
#[derive(Debug, Clone)]
struct Connection {
    url: String,
    /// The stream, made when it does not exist; without one, the sink publishes to the
    /// stream that already takes the subject, as MageML's does.
    stream: Option<String>,
    subject: String,
    credentials: Option<Credentials>,
    nkey: Option<String>,
    tls: Option<Tls>,
}

#[derive(Debug, Clone)]
enum Credentials {
    /// A .creds file.
    File(String),
    UserPassword(String, String),
}

#[derive(Debug, Clone, Default)]
struct Tls {
    cafile: Option<String>,
    certfile: Option<String>,
    keyfile: Option<String>,
}

impl Connection {
    fn new(config: &Value, role: &str) -> Result<Connection, ConnectorError> {
        let url = text(config, "server_url").ok_or_else(|| {
            ConnectorError::fatal(format!("NATS {role} settings need server_url."))
        })?;
        let subject = text(config, "subject")
            .ok_or_else(|| ConnectorError::fatal(format!("NATS {role} settings need subject.")))?;
        let stream = text(config, "stream_name");
        // MageML's user_credentials: a .creds file, or [user, password].
        let credentials = match config.get("user_credentials") {
            Some(Value::String(path)) if !path.is_empty() => Some(Credentials::File(path.clone())),
            Some(Value::Array(pair)) if pair.len() == 2 => Some(Credentials::UserPassword(
                pair[0].as_str().unwrap_or_default().to_string(),
                pair[1].as_str().unwrap_or_default().to_string(),
            )),
            _ => None,
        };
        let tls = config
            .get("use_tls")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            .then(|| {
                let ssl = config.get("ssl_config").cloned().unwrap_or(Value::Null);
                Tls {
                    cafile: text(&ssl, "cafile"),
                    certfile: text(&ssl, "certfile"),
                    keyfile: text(&ssl, "keyfile"),
                }
            });
        Ok(Connection {
            url,
            stream,
            subject,
            credentials,
            nkey: text(config, "nkeys_seed_str"),
            tls,
        })
    }

    async fn connect(&self) -> Result<(async_nats::Client, Context), ConnectorError> {
        let (client, context) = self.reach().await?;
        if let Some(name) = &self.stream {
            context
                .get_or_create_stream(stream::Config {
                    name: name.clone(),
                    subjects: vec![self.subject.clone()],
                    ..Default::default()
                })
                .await
                .map_err(|e| nats_error(&format!("opening stream {name}"), e))?;
        }
        Ok((client, context))
    }

    /// The connection, without making the stream.
    async fn reach(&self) -> Result<(async_nats::Client, Context), ConnectorError> {
        let mut options = async_nats::ConnectOptions::new()
            .name("mage-service")
            .connection_timeout(Duration::from_secs(10));
        match &self.credentials {
            Some(Credentials::File(path)) => {
                options = options
                    .credentials_file(path)
                    .await
                    .map_err(|e| ConnectorError::fatal(format!("NATS credentials {path}: {e}")))?;
            }
            Some(Credentials::UserPassword(user, password)) => {
                options = options.user_and_password(user.clone(), password.clone());
            }
            None => {}
        }
        if let Some(seed) = &self.nkey {
            options = options.nkey(seed.clone());
        }
        if let Some(tls) = &self.tls {
            options = options.require_tls(true);
            if let Some(ca) = &tls.cafile {
                options = options.add_root_certificates(ca.into());
            }
            if let (Some(cert), Some(key)) = (&tls.certfile, &tls.keyfile) {
                options = options.add_client_certificate(cert.into(), key.into());
            }
        }
        let client = options
            .connect(self.url.as_str())
            .await
            .map_err(|e| nats_error(&format!("connecting to {}", self.url), e))?;
        let context = jetstream::new(client.clone());
        Ok((client, context))
    }
}

/// The connection check of a source (`source`) or a sink: the server, the stream (its
/// messages and subjects) and the source's consumer (what it has not delivered or not had
/// acknowledged). Reads only: a missing stream or consumer is made by the stream's first
/// start, not by the check.
pub async fn check(config: &Value, source: bool) -> mage_stream_ports::check::ConnectionReport {
    use mage_stream_ports::check::ConnectionReport;

    let connection = match Connection::new(config, if source { "source" } else { "sink" }) {
        Ok(connection) => connection,
        Err(error) => return ConnectionReport::failed(error.to_string()),
    };
    let (_client, context) = match connection.reach().await {
        Ok(reached) => reached,
        Err(error) => return ConnectionReport::failed(format!("Could not connect: {error}")),
    };
    let mut report = ConnectionReport::passed(format!("Connected to {}.", connection.url));
    let Some(name) = connection.stream.clone() else {
        if source {
            return report.fail("The settings name no stream (stream_name).");
        }
        return match context.stream_by_subject(connection.subject.clone()).await {
            Ok(stream) => report
                .fact("stream", stream)
                .fact("subject", &connection.subject),
            Err(error) => report.fail(format!(
                "No stream takes subject {}: {error}",
                connection.subject
            )),
        };
    };
    let stream = match context.get_stream(&name).await {
        Ok(stream) => stream,
        Err(error) if error.to_string().contains("not found") => {
            return report.warn(format!(
                "Stream {name} does not exist; the stream's first start makes it."
            ));
        }
        Err(error) => return report.fail(format!("Stream {name}: {error}")),
    };
    let info = stream.cached_info();
    report = report
        .fact("stream", &name)
        .fact("subjects", info.config.subjects.join(", "))
        .fact("messages", info.state.messages);
    if !source {
        return report;
    }
    let consumer = text(config, "consumer_name").unwrap_or_else(|| name.clone());
    match stream.consumer_info(&consumer).await {
        Ok(info) => report
            .fact("consumer", &consumer)
            .fact("not delivered", info.num_pending)
            .fact("not acknowledged", info.num_ack_pending),
        Err(error) if error.to_string().contains("not found") => report.warn(format!(
            "Consumer {consumer} does not exist; the stream's first start makes it and reads \
             from the stream's first message."
        )),
        Err(error) => report.fail(format!("Consumer {consumer}: {error}")),
    }
}

struct Live {
    client: async_nats::Client,
    /// One pull that goes on for the connection's life: a pull per batch that expired while
    /// the server was still delivering lost those messages to an inbox nobody read, and
    /// they came again only after ack_wait (I-90).
    messages: pull::Stream,
}

struct Pending {
    builder: EnvelopeBuilder,
    first: u64,
    last: u64,
    started: Instant,
}

pub struct NatsSource {
    connection: Connection,
    stream: String,
    consumer: String,
    ack_wait: Option<Duration>,
    limits: BatchLimits,
    live: Option<Live>,
    epoch: u64,
    seq: u64,
    /// Each delivered message's position, counting deliveries (a redelivered message comes
    /// again with a new one), and its acknowledgement.
    delivered: u64,
    unacked: BTreeMap<u64, jetstream::message::Acker>,
    pending: Option<Pending>,
    paused: bool,
}

impl NatsSource {
    pub fn new(config: &Value, limits: BatchLimits) -> Result<NatsSource, ConnectorError> {
        let connection = Connection::new(config, "source")?;
        let stream = connection
            .stream
            .clone()
            .ok_or_else(|| ConnectorError::fatal("NATS source settings need stream_name."))?;
        let consumer = text(config, "consumer_name").unwrap_or_else(|| stream.clone());
        let ack_wait = config
            .get("ack_wait")
            .and_then(Value::as_f64)
            .filter(|s| *s > 0.0)
            .map(Duration::from_secs_f64);
        Ok(NatsSource {
            connection,
            stream,
            consumer,
            ack_wait,
            limits,
            live: None,
            epoch: 0,
            seq: 0,
            delivered: 0,
            unacked: BTreeMap::new(),
            pending: None,
            paused: false,
        })
    }

    async fn connect(&mut self) -> Result<SourceEvent, ConnectorError> {
        let (client, context) = self.connection.connect().await?;
        let stream = context
            .get_stream(&self.stream)
            .await
            .map_err(|e| nats_error("reading the stream", e))?;
        let mut consumer_config = pull::Config {
            durable_name: Some(self.consumer.clone()),
            filter_subject: self.connection.subject.clone(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            max_ack_pending: (self.limits.max_records as i64)
                .saturating_mul(10)
                .clamp(1_000, 1_000_000),
            ..Default::default()
        };
        if let Some(wait) = self.ack_wait {
            consumer_config.ack_wait = wait;
        }
        let consumer: jetstream::consumer::Consumer<pull::Config> = stream
            .get_or_create_consumer(&self.consumer, consumer_config)
            .await
            .map_err(|e| nats_error(&format!("opening consumer {}", self.consumer), e))?;
        let messages = consumer
            .stream()
            .max_messages_per_batch(self.limits.max_records.max(1))
            .messages()
            .await
            .map_err(|e| nats_error(&format!("reading consumer {}", self.consumer), e))?;
        self.live = Some(Live { client, messages });
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
            .max(Duration::from_millis(10))
    }

    /// The connection is gone: what was not acknowledged is delivered again.
    fn lose(&mut self) -> SourceEvent {
        self.live = None;
        self.pending = None;
        self.unacked.clear();
        SourceEvent::Revoking {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
            within: Duration::from_millis(0),
        }
    }

    fn add(&mut self, message: async_nats::Message, acker: jetstream::message::Acker) {
        self.delivered += 1;
        let position = self.delivered;
        let headers = message
            .headers
            .as_ref()
            .map(|headers| {
                headers
                    .iter()
                    .flat_map(|(key, values)| {
                        values.iter().map(move |value| Header {
                            key: key.to_string(),
                            value: value.as_str().as_bytes().to_vec(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let envelope = Envelope {
            key: Some(message.subject.as_bytes().to_vec()),
            value: message.payload.to_vec(),
            headers,
            partition: PARTITION.0,
            position: Some(position as i64),
            timestamp: None,
        };
        let pending = self.pending.get_or_insert_with(|| Pending {
            builder: EnvelopeBuilder::new(),
            first: position,
            last: position,
            started: Instant::now(),
        });
        pending.builder.push(&envelope);
        pending.last = position;
        self.unacked.insert(position, acker);
    }
}

impl Source for NatsSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if self.live.is_none() {
                return self.connect().await;
            }
            loop {
                if self.full() {
                    return Ok(self.take()?.expect("full"));
                }
                if self.paused {
                    // Nothing is fetched: JetStream keeps the messages.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
                }
                let wait = self.wait();
                let live = self.live.as_mut().expect("connected");
                match tokio::time::timeout(wait, live.messages.next()).await {
                    Ok(Some(Ok(message))) => {
                        let (message, acker) = message.split();
                        self.add(message, acker);
                        // What has already arrived, without a timer per message.
                        while !self.full() {
                            let live = self.live.as_mut().expect("connected");
                            match live.messages.next().now_or_never() {
                                Some(Some(Ok(message))) => {
                                    let (message, acker) = message.split();
                                    self.add(message, acker);
                                }
                                Some(Some(Err(_))) | Some(None) => return Ok(self.lose()),
                                None => break,
                            }
                        }
                        let waited = self
                            .pending
                            .as_ref()
                            .is_some_and(|p| p.started.elapsed() >= self.limits.max_wait);
                        if waited {
                            return Ok(self.take()?.expect("pending"));
                        }
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
                let Position::Tag(last) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "NATS acknowledges message positions",
                    ));
                };
                let later = self.unacked.split_off(&(last + 1));
                let done = std::mem::replace(&mut self.unacked, later);
                for (_, acker) in done {
                    if let Err(error) = acker.ack().await {
                        self.lose();
                        return CommitOutcome::Failed(nats_error("acknowledging", error));
                    }
                }
            }
            // The acknowledgements leave before the batch counts as committed.
            if let Some(live) = &self.live
                && let Err(error) = live.client.flush().await
            {
                self.lose();
                return CommitOutcome::Failed(nats_error("acknowledging", error));
            }
            CommitOutcome::Committed
        })
    }

    fn pause(&mut self, _partition: PartitionId) {
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
            needs_keepalive: false,
            durable_positions: false,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Some(live) = self.live.take() {
                let _ = live.client.flush().await;
            }
        })
    }
}

pub struct NatsSink {
    connection: Connection,
    stream_name: String,
    context: tokio::sync::Mutex<Option<Context>>,
}

impl NatsSink {
    /// `stream_name`: the stream this service's messages come from, for message ids.
    pub fn new(config: &Value, stream_name: &str) -> Result<NatsSink, ConnectorError> {
        Ok(NatsSink {
            connection: Connection::new(config, "sink")?,
            stream_name: stream_name.to_string(),
            context: tokio::sync::Mutex::new(None),
        })
    }

    async fn context(&self) -> Result<Context, ConnectorError> {
        let mut guard = self.context.lock().await;
        if let Some(context) = guard.as_ref() {
            return Ok(context.clone());
        }
        let (_, context) = self.connection.connect().await?;
        *guard = Some(context.clone());
        Ok(context)
    }

    /// Publishes the batch's messages in order and returns their acknowledgements.
    async fn publish(
        &self,
        batch: &RecordBatch,
        meta: &BatchMeta,
    ) -> Result<Vec<jetstream::context::PublishAckFuture>, ConnectorError> {
        let messages = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let context = self.context().await?;
        let mut acks = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let (payload, mut headers) = mage_message(&message.value);
            for header in &message.headers {
                headers.append(
                    header.key.as_str(),
                    String::from_utf8_lossy(&header.value).as_ref(),
                );
            }
            headers.insert(
                "Nats-Msg-Id",
                format!(
                    "{}:{}:{}",
                    self.stream_name,
                    meta.id.partition.0,
                    meta.row_key(message.position, index)
                )
                .as_str(),
            );
            match context
                .publish_with_headers(self.connection.subject.clone(), headers, payload)
                .await
            {
                Ok(ack) => acks.push(ack),
                Err(error) => {
                    *self.context.lock().await = None;
                    return Err(nats_error("publishing", error));
                }
            }
        }
        Ok(acks)
    }
}

/// MageML's sink publishes a `{"data", "metadata"}` message's data, with the metadata as
/// headers; other messages go as they are.
fn mage_message(value: &[u8]) -> (bytes::Bytes, HeaderMap) {
    let mut headers = HeaderMap::new();
    if let Ok(Value::Object(object)) = serde_json::from_slice::<Value>(value)
        && object.len() == 2
        && let (Some(data), Some(Value::Object(metadata))) =
            (object.get("data"), object.get("metadata"))
    {
        for (key, item) in metadata {
            let text = match item {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            headers.insert(key.as_str(), text.as_str());
        }
        let data = serde_json::to_vec(data).unwrap_or_default();
        return (data.into(), headers);
    }
    (bytes::Bytes::copy_from_slice(value), headers)
}

async fn acknowledged(
    acks: Vec<jetstream::context::PublishAckFuture>,
    subject: String,
) -> Result<(), ConnectorError> {
    for ack in acks {
        ack.await.map_err(|e| {
            ConnectorError::retryable(format!(
                "NATS: JetStream did not take a message on {subject}: {e}"
            ))
        })?;
    }
    Ok(())
}

impl Sink for NatsSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let acks = self.publish(batch, meta).await?;
            acknowledged(acks, self.connection.subject.clone()).await
        })
    }

    /// One connection keeps the order of what is published on it: the next batch is
    /// published while JetStream acknowledges this one.
    fn queue<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<Confirmation, ConnectorError>> {
        Box::pin(async move {
            let acks = self.publish(batch, meta).await?;
            Ok(Box::pin(acknowledged(acks, self.connection.subject.clone())) as Confirmation)
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: true,
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_follow_mageml() {
        let connection = Connection::new(
            &serde_json::json!({
                "server_url": "nats://localhost:4222",
                "subject": "orders.new",
                "user_credentials": ["reader", "secret"],
                "use_tls": true,
                "ssl_config": {"cafile": "/ca.pem"},
            }),
            "source",
        )
        .unwrap();
        assert_eq!(connection.stream, None);
        assert!(
            matches!(connection.credentials, Some(Credentials::UserPassword(ref u, _)) if u == "reader")
        );
        assert_eq!(connection.tls.unwrap().cafile.as_deref(), Some("/ca.pem"));
        assert!(Connection::new(&serde_json::json!({"subject": "a"}), "sink").is_err());
    }

    #[test]
    fn data_and_metadata_messages_publish_their_data_with_headers() {
        let (payload, headers) =
            mage_message(br#"{"data": {"n": 1}, "metadata": {"source": "web", "n": 2}}"#);
        assert_eq!(&payload[..], br#"{"n":1}"#);
        assert_eq!(headers.get("source").map(|v| v.as_str()), Some("web"));
        assert_eq!(headers.get("n").map(|v| v.as_str()), Some("2"));
        let (payload, headers) = mage_message(br#"{"data": 1}"#);
        assert_eq!(&payload[..], br#"{"data": 1}"#);
        assert!(headers.is_empty());
    }

    /// Against a server (`MAGE_TEST_NATS`, a URL; integration_tests/compose.yaml serves
    /// nats://localhost:14222).
    #[tokio::test]
    async fn a_check_reports_the_stream_and_the_consumer_without_making_them() {
        use mage_stream_ports::check::CheckResult;

        let Ok(url) = std::env::var("MAGE_TEST_NATS") else {
            eprintln!("MAGE_TEST_NATS is not set: skipped");
            return;
        };
        let name = format!("CHECK_{}", std::process::id());
        let settings = serde_json::json!({"server_url": url, "stream_name": name,
                                          "subject": format!("check.{}", std::process::id())});
        let report = check(&settings, true).await;
        assert_eq!(report.result, CheckResult::Warning, "{report:?}");
        let client = async_nats::connect(url.as_str()).await.unwrap();
        let context = jetstream::new(client);
        assert!(
            context.get_stream(&name).await.is_err(),
            "the check made the stream"
        );

        let connection = Connection::new(&settings, "source").unwrap();
        connection.connect().await.unwrap();
        context
            .publish(connection.subject.clone(), "1".into())
            .await
            .unwrap()
            .await
            .unwrap();
        let report = check(&settings, true).await;
        assert_eq!(report.result, CheckResult::Warning, "{report:?}");
        assert!(
            report
                .facts
                .contains(&("messages".to_string(), "1".to_string())),
            "{report:?}"
        );
        assert!(report.problems[0].contains("Consumer"), "{report:?}");
        assert_eq!(check(&settings, false).await.result, CheckResult::Passed);
        let nowhere = check(
            &serde_json::json!({"server_url": url, "subject": "no.stream.here"}),
            false,
        )
        .await;
        assert_eq!(nowhere.result, CheckResult::Failed, "{nowhere:?}");
        context.delete_stream(&name).await.unwrap();
    }
}
