//! Google Cloud Pub/Sub source and sink for streams, with MageML's settings.
//!
//! Source (`project_id`, `topic_id`, `subscription_id`, `pubsub_emulator_host`, and
//! `receivers`, pulls at once, 2 by default; `ack_deadline`, seconds, the subscription's by
//! default): the subscription is made on the topic when it does not exist. Each message
//! reaches the blocks as MageML hands it over, `{"data": <the body as text>, "metadata":
//! {"attributes": {...}}}`. A message is acknowledged only once every sink took its batch,
//! and while a batch runs its messages' ack deadlines are extended, so a slow batch is not
//! delivered twice; one not acknowledged comes back after its deadline. Several instances
//! share a subscription.
//!
//! Sink (`project_id`, `topic_id`, `pubsub_emulator_host`): each message is published as
//! MageML publishes it: an object's `data` (or the object) as JSON, a string as its text.
//! A batch goes in requests of up to 1,000 messages, and is confirmed once Pub/Sub took
//! every one; the next batch is published meanwhile.
//!
//! Credentials are the Google chain the GCS sink uses: `GOOGLE_APPLICATION_CREDENTIALS`,
//! application default credentials, or the metadata server (GKE, Cloud Run, GCE).

mod api;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine as _;
use futures_util::StreamExt;
use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, Confirmation,
    ConnectorError, Envelope, EnvelopeBuilder, PartitionId, Position, RecordBatch, Sink,
    SinkCapabilities, Source, SourceCapabilities, SourceEvent, read_envelope,
};
use serde_json::value::RawValue;
use serde_json::{Value, json};
use tokio::sync::mpsc;

pub use api::Api;

const PARTITION: PartitionId = PartitionId(0);
/// Messages a pull asks for, and the most a publish request carries.
const PER_CALL: usize = 1000;
/// Acknowledgement ids an acknowledge call carries (each up to a few hundred bytes).
const ACK_IDS_PER_CALL: usize = 500;
/// Publish requests stay under Pub/Sub's 10 MB.
const PUBLISH_BYTES: usize = 7 * 1024 * 1024;
const CALLS_AT_ONCE: usize = 8;

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

fn need(config: &Value, key: &str) -> Result<String, ConnectorError> {
    text(config, key).ok_or_else(|| ConnectorError::fatal(format!("Pub/Sub settings need {key}.")))
}

fn emulator(config: &Value) -> Option<String> {
    text(config, "pubsub_emulator_host").or_else(|| std::env::var("PUBSUB_EMULATOR_HOST").ok())
}

struct Received {
    value: Vec<u8>,
    ack_id: String,
    published_ms: Option<i64>,
}

struct Pending {
    builder: EnvelopeBuilder,
    first: u64,
    last: u64,
    started: Instant,
}

struct Connected {
    deadline: Duration,
    messages: mpsc::Receiver<Result<Vec<Received>, ConnectorError>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Connected {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub struct PubSubSource {
    subscription: String,
    topic: String,
    receivers: usize,
    deadline: Option<u64>,
    limits: BatchLimits,
    api: Arc<Api>,
    connected: Option<Connected>,
    epoch: u64,
    seq: u64,
    delivered: u64,
    /// The ack id of each delivered message not yet acknowledged, and when its deadline was
    /// last set.
    ack_ids: BTreeMap<u64, (String, Instant)>,
    pending: Option<Pending>,
    paused: Arc<AtomicBool>,
    last_extend: Instant,
}

impl PubSubSource {
    pub fn new(config: &Value, limits: BatchLimits) -> Result<PubSubSource, ConnectorError> {
        let project = need(config, "project_id")?;
        let subscription = need(config, "subscription_id")?;
        let topic = need(config, "topic_id")?;
        let number = |key: &str| -> Result<Option<u64>, ConnectorError> {
            text(config, key)
                .map(|v| {
                    v.parse::<f64>()
                        .ok()
                        .filter(|v| *v >= 0.0)
                        .map(|v| v as u64)
                        .ok_or_else(|| ConnectorError::fatal(format!("Pub/Sub {key} is a number.")))
                })
                .transpose()
        };
        Ok(PubSubSource {
            subscription: format!("projects/{project}/subscriptions/{subscription}"),
            topic: format!("projects/{project}/topics/{topic}"),
            receivers: number("receivers")?.unwrap_or(2).clamp(1, 32) as usize,
            deadline: number("ack_deadline")?,
            limits,
            api: Arc::new(Api::new(
                emulator(config).as_deref(),
                Duration::from_secs(90),
            )?),
            connected: None,
            epoch: 0,
            seq: 0,
            delivered: 0,
            ack_ids: BTreeMap::new(),
            pending: None,
            paused: Arc::new(AtomicBool::new(false)),
            last_extend: Instant::now(),
        })
    }

    async fn connect(&mut self) -> Result<SourceEvent, ConnectorError> {
        let made = self
            .api
            .call(
                "PUT",
                &self.subscription,
                Some(&json!({"topic": self.topic})),
            )
            .await;
        match made {
            Ok(_) => {}
            // MageML makes the subscription when it is missing.
            Err(error) if error.message.contains("ALREADY_EXISTS") => {}
            Err(error) => return Err(error),
        }
        let deadline = match self.deadline {
            Some(seconds) => seconds,
            None => self
                .api
                .call("GET", &self.subscription, None)
                .await?
                .get("ackDeadlineSeconds")
                .and_then(Value::as_u64)
                .unwrap_or(10),
        };
        let (sender, messages) = mpsc::channel(self.receivers * 2);
        let tasks = (0..self.receivers)
            .map(|_| {
                tokio::spawn(pull(
                    self.api.clone(),
                    format!("{}:pull", self.subscription),
                    sender.clone(),
                    self.paused.clone(),
                ))
            })
            .collect();
        self.connected = Some(Connected {
            deadline: Duration::from_secs(deadline.clamp(10, 600)),
            messages,
            tasks,
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
            first: Position::Offset(pending.first as i64),
            last: Position::Offset(pending.last as i64),
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

    fn add(&mut self, message: Received) {
        self.delivered += 1;
        let position = self.delivered;
        self.ack_ids
            .insert(position, (message.ack_id, Instant::now()));
        let envelope = Envelope {
            key: None,
            value: message.value,
            headers: Vec::new(),
            partition: PARTITION.0,
            position: Some(position as i64),
            timestamp: message.published_ms,
        };
        let pending = self.pending.get_or_insert_with(|| Pending {
            builder: EnvelopeBuilder::new(),
            first: position,
            last: position,
            started: Instant::now(),
        });
        pending.builder.push(&envelope);
        pending.last = position;
    }

    /// Extends the deadlines of the messages of running batches past half their deadline.
    async fn extend(&mut self) -> Result<(), ConnectorError> {
        let Some(connected) = &self.connected else {
            return Ok(());
        };
        if self.last_extend.elapsed() < Duration::from_secs(1) {
            return Ok(());
        }
        self.last_extend = Instant::now();
        let now = Instant::now();
        let half = connected.deadline / 2;
        let due: Vec<(u64, String)> = self
            .ack_ids
            .iter()
            .filter(|(_, (_, at))| now.duration_since(*at) >= half)
            .map(|(position, (id, _))| (*position, id.clone()))
            .collect();
        if due.is_empty() {
            return Ok(());
        }
        let seconds = connected.deadline.as_secs();
        ack_calls(
            self.api.clone(),
            format!("{}:modifyAckDeadline", self.subscription),
            due.iter().map(|(_, id)| id.clone()).collect(),
            Some(seconds),
        )
        .await?;
        for (position, _) in due {
            if let Some(entry) = self.ack_ids.get_mut(&position) {
                entry.1 = now;
            }
        }
        Ok(())
    }
}

/// One pull after another, until the source goes.
async fn pull(
    api: Arc<Api>,
    path: String,
    sender: mpsc::Sender<Result<Vec<Received>, ConnectorError>>,
    paused: Arc<AtomicBool>,
) {
    let mut backoff = Duration::from_millis(100);
    let request = json!({"maxMessages": PER_CALL});
    while !sender.is_closed() {
        if paused.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        match api.call("POST", &path, Some(&request)).await {
            Ok(answer) => {
                backoff = Duration::from_millis(100);
                let messages: Vec<Received> = answer
                    .get("receivedMessages")
                    .and_then(Value::as_array)
                    .map(|list| list.iter().filter_map(received).collect())
                    .unwrap_or_default();
                if !messages.is_empty() && sender.send(Ok(messages)).await.is_err() {
                    return;
                }
            }
            Err(error) => {
                if sender.send(Err(error)).await.is_err() {
                    return;
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }
    }
}

/// A pulled message as MageML hands it to blocks.
fn received(message: &Value) -> Option<Received> {
    let ack_id = message.get("ackId")?.as_str()?.to_string();
    let body = message.get("message")?;
    let data = body
        .get("data")
        .and_then(Value::as_str)
        .map(|d| base64::engine::general_purpose::STANDARD.decode(d))
        .transpose()
        .ok()?
        .unwrap_or_default();
    let attributes = body.get("attributes").cloned().unwrap_or_else(|| json!({}));
    let value = json!({
        "data": String::from_utf8_lossy(&data),
        "metadata": {"attributes": attributes},
    });
    Some(Received {
        value: serde_json::to_vec(&value).expect("JSON"),
        ack_id,
        published_ms: body
            .get("publishTime")
            .and_then(Value::as_str)
            .and_then(rfc3339_millis),
    })
}

/// Milliseconds since the epoch of an RFC 3339 UTC time, such as 2026-10-14T10:00:00.123Z.
fn rfc3339_millis(text: &str) -> Option<i64> {
    let (date, time) = text.trim_end_matches('Z').split_once('T')?;
    let mut parts = date.split('-').map(|p| p.parse::<i64>().ok());
    let (year, month, day) = (parts.next()??, parts.next()??, parts.next()??);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, "0"));
    let mut clock = clock.split(':').map(|p| p.parse::<i64>().ok());
    let (hour, minute, second) = (clock.next()??, clock.next()??, clock.next()??);
    let millis = format!("{fraction:0<3}")[..3].parse::<i64>().ok()?;
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 24 + hour) * 60 + minute) * 60_000 + second * 1000 + millis)
}

/// Acknowledges the ids, or with `deadline` extends them, several calls at once.
async fn ack_calls(
    api: Arc<Api>,
    path: String,
    ids: Vec<String>,
    deadline: Option<u64>,
) -> Result<(), ConnectorError> {
    let bodies: Vec<Value> = ids
        .chunks(ACK_IDS_PER_CALL)
        .map(|chunk| match deadline {
            Some(seconds) => json!({"ackIds": chunk, "ackDeadlineSeconds": seconds}),
            None => json!({"ackIds": chunk}),
        })
        .collect();
    let answers: Vec<Result<Value, ConnectorError>> =
        futures_util::stream::iter(bodies.into_iter().map(|body| {
            let (api, path) = (api.clone(), path.clone());
            async move { api.call("POST", &path, Some(&body)).await }
        }))
        .buffer_unordered(CALLS_AT_ONCE)
        .collect()
        .await;
    for answer in answers {
        answer?;
    }
    Ok(())
}

impl Source for PubSubSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if self.connected.is_none() {
                return self.connect().await;
            }
            loop {
                if self.full() {
                    return Ok(self.take()?.expect("full"));
                }
                self.extend().await?;
                if self.paused.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
                }
                let wait = self.wait();
                let connected = self.connected.as_mut().expect("connected");
                match tokio::time::timeout(wait, connected.messages.recv()).await {
                    Ok(Some(Ok(messages))) => {
                        for message in messages {
                            self.add(message);
                        }
                    }
                    Ok(Some(Err(error))) => return Err(error),
                    Ok(None) => {
                        self.connected = None;
                        self.pending = None;
                        self.ack_ids.clear();
                        return Ok(SourceEvent::Revoking {
                            partition: PARTITION,
                            epoch: AssignmentEpoch(self.epoch),
                            within: Duration::from_millis(0),
                        });
                    }
                    Err(_) => return Ok(self.take()?.unwrap_or(SourceEvent::Idle)),
                }
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            let mut done = Vec::new();
            for commit in commits {
                if commit.epoch != AssignmentEpoch(self.epoch) || self.connected.is_none() {
                    return CommitOutcome::Stale;
                }
                let Position::Offset(last) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "Pub/Sub acknowledges message positions",
                    ));
                };
                let later = self.ack_ids.split_off(&(last as u64 + 1));
                done.extend(
                    std::mem::replace(&mut self.ack_ids, later)
                        .into_values()
                        .map(|(id, _)| id),
                );
            }
            if done.is_empty() {
                return CommitOutcome::Committed;
            }
            let path = format!("{}:acknowledge", self.subscription);
            match ack_calls(self.api.clone(), path, done, None).await {
                Ok(()) => CommitOutcome::Committed,
                // Not acknowledged: the messages come back after their deadline.
                Err(error) => CommitOutcome::Failed(error),
            }
        })
    }

    fn pause(&mut self, _partition: PartitionId) {
        self.paused.store(true, Ordering::Relaxed);
    }

    fn resume(&mut self, _partition: PartitionId) {
        self.paused.store(false, Ordering::Relaxed);
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
            self.connected = None;
        })
    }
}

pub struct PubSubSink {
    path: String,
    api: Arc<Api>,
}

impl PubSubSink {
    pub fn new(config: &Value) -> Result<PubSubSink, ConnectorError> {
        let project = need(config, "project_id")?;
        let topic = need(config, "topic_id")?;
        Ok(PubSubSink {
            path: format!("projects/{project}/topics/{topic}:publish"),
            api: Arc::new(Api::new(
                emulator(config).as_deref(),
                Duration::from_secs(60),
            )?),
        })
    }

    /// Starts the batch's publish requests: their confirmation.
    fn publish(&self, batch: &RecordBatch) -> Result<Confirmation, ConnectorError> {
        let envelopes =
            read_envelope(batch).map_err(|e| ConnectorError::fatal(format!("Pub/Sub: {e}")))?;
        let mut requests = Vec::new();
        let mut messages = Vec::new();
        let mut bytes = 0;
        for envelope in &envelopes {
            let data = base64::engine::general_purpose::STANDARD.encode(payload(&envelope.value));
            if messages.len() == PER_CALL || bytes + data.len() > PUBLISH_BYTES {
                requests.push(json!({"messages": std::mem::take(&mut messages)}));
                bytes = 0;
            }
            bytes += data.len();
            messages.push(json!({"data": data}));
        }
        if !messages.is_empty() {
            requests.push(json!({"messages": messages}));
        }
        let tasks: Vec<_> = requests
            .into_iter()
            .map(|body| {
                let (api, path) = (self.api.clone(), self.path.clone());
                tokio::spawn(async move { api.call("POST", &path, Some(&body)).await })
            })
            .collect();
        Ok(Box::pin(async move {
            for task in tasks {
                task.await
                    .map_err(|e| ConnectorError::retryable(format!("Pub/Sub publish: {e}")))??;
            }
            Ok(())
        }))
    }
}

/// What MageML publishes for a message: an object's `data` (or the object) as JSON, a
/// string's text, anything else as its JSON.
fn payload(value: &[u8]) -> Vec<u8> {
    match value.iter().find(|b| !b.is_ascii_whitespace()) {
        Some(b'{') => match serde_json::from_slice::<HashMap<String, Box<RawValue>>>(value) {
            Ok(fields) => match fields.get("data") {
                Some(data) => data.get().as_bytes().to_vec(),
                None => value.to_vec(),
            },
            Err(_) => value.to_vec(),
        },
        Some(b'"') => match serde_json::from_slice::<String>(value) {
            Ok(text) => text.into_bytes(),
            Err(_) => value.to_vec(),
        },
        _ => value.to_vec(),
    }
}

impl Sink for PubSubSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move { self.publish(batch)?.await })
    }

    fn queue<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<Confirmation, ConnectorError>> {
        Box::pin(async move { self.publish(batch) })
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
    fn pulled_messages_become_what_mageml_hands_blocks() {
        let message = received(&json!({
            "ackId": "a1",
            "message": {"data": "eyJuIjogMX0=", "attributes": {"k": "v"},
                        "publishTime": "2026-10-14T10:00:00.123456Z"},
        }))
        .unwrap();
        assert_eq!(message.ack_id, "a1");
        let value: Value = serde_json::from_slice(&message.value).unwrap();
        assert_eq!(
            value,
            json!({"data": "{\"n\": 1}", "metadata": {"attributes": {"k": "v"}}})
        );
        assert_eq!(message.published_ms, Some(1_791_972_000_123));
        assert!(received(&json!({"message": {}})).is_none());
    }

    #[test]
    fn published_payloads_follow_mageml() {
        assert_eq!(payload(br#"{"data": {"n": 1}, "x": 2}"#), br#"{"n": 1}"#);
        assert_eq!(
            payload(br#"{"n": 12345678901234567890}"#),
            br#"{"n": 12345678901234567890}"#
        );
        assert_eq!(payload(br#"{"data": "text"}"#), br#""text""#);
        assert_eq!(payload(br#""plain""#), b"plain");
        assert_eq!(payload(b"[1, 2]"), b"[1, 2]");
    }

    #[test]
    fn times_read_as_milliseconds() {
        assert_eq!(rfc3339_millis("1970-01-01T00:00:01Z"), Some(1000));
        assert_eq!(
            rfc3339_millis("2000-03-01T00:00:00.5Z"),
            Some(951_868_800_500)
        );
        assert_eq!(rfc3339_millis("nope"), None);
    }

    #[test]
    fn settings_name_the_subscription_and_topic() {
        let limits = BatchLimits {
            max_records: 10,
            max_bytes: 1 << 20,
            max_wait: Duration::from_millis(100),
        };
        let source = PubSubSource::new(
            &json!({"project_id": "p", "topic_id": "t", "subscription_id": "s",
                    "pubsub_emulator_host": "localhost:8085", "receivers": 4}),
            limits,
        )
        .unwrap();
        assert_eq!(source.subscription, "projects/p/subscriptions/s");
        assert_eq!(source.topic, "projects/p/topics/t");
        assert_eq!(source.receivers, 4);
        assert!(PubSubSource::new(&json!({"project_id": "p"}), limits).is_err());
    }
}
