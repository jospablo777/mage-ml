//! ActiveMQ source and sink for streams, over STOMP, with MageML's settings.
//!
//! Source (`connection_host`, `connection_port`, `queue_name`, `username`, `password`, and
//! `prefetch`, messages the broker sends ahead, 1,000 by default): blocks get each body as
//! text, as in MageML. The subscription acknowledges cumulatively: once every sink took a
//! batch, one ACK of its last message acknowledges it and every message before it. Messages
//! not acknowledged when the service stops or dies go back to the queue.
//!
//! Sink (`connection_host`, `connection_port`, `queue_name`, `username`, `password`,
//! `receipt_timeout`): each message is sent persistent, as MageML sends it (an object's
//! `data`, or the object, as JSON, its `metadata` as headers), and a batch is confirmed by
//! the broker's receipt of its last frame, which STOMP sends after the frames before it. The
//! next batch is sent while one is confirmed.

mod stomp;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, Confirmation,
    ConnectorError, Envelope, EnvelopeBuilder, PartitionId, Position, RecordBatch, Sink,
    SinkCapabilities, Source, SourceCapabilities, SourceEvent, read_envelope,
};
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Mutex, oneshot};

const PARTITION: PartitionId = PartitionId(0);
const SUBSCRIPTION: &str = "mage";

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

fn settings(config: &Value) -> Result<(stomp::Settings, String), ConnectorError> {
    let need = |key: &str| {
        text(config, key)
            .ok_or_else(|| ConnectorError::fatal(format!("ActiveMQ settings need {key}.")))
    };
    let port = need("connection_port")?
        .parse::<u16>()
        .map_err(|_| ConnectorError::fatal("ActiveMQ connection_port is a number."))?;
    Ok((
        stomp::Settings {
            host: need("connection_host")?,
            port,
            // MageML's defaults.
            user: text(config, "username").unwrap_or_else(|| "admin".into()),
            password: text(config, "password").unwrap_or_else(|| "admin".into()),
        },
        format!("/queue/{}", need("queue_name")?),
    ))
}

struct Pending {
    builder: EnvelopeBuilder,
    first: u64,
    last: u64,
    started: Instant,
}

struct Subscribed {
    reader: stomp::Reader,
    writer: OwnedWriteHalf,
    /// STOMP 1.2 acknowledges a message's `ack` header, 1.1 its `message-id`.
    version_12: bool,
}

pub struct ActiveMqSource {
    settings: stomp::Settings,
    destination: String,
    prefetch: u64,
    limits: BatchLimits,
    session: Option<Subscribed>,
    epoch: u64,
    seq: u64,
    delivered: u64,
    /// The acknowledgement id of each delivered message not yet acknowledged.
    acks: BTreeMap<u64, String>,
    pending: Option<Pending>,
    paused: bool,
}

impl ActiveMqSource {
    pub fn new(config: &Value, limits: BatchLimits) -> Result<ActiveMqSource, ConnectorError> {
        let (settings, destination) = settings(config)?;
        let prefetch = match text(config, "prefetch") {
            Some(n) => n
                .parse::<u64>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| ConnectorError::fatal("ActiveMQ prefetch is a number."))?,
            None => 1000,
        };
        Ok(ActiveMqSource {
            settings,
            destination,
            prefetch,
            limits,
            session: None,
            epoch: 0,
            seq: 0,
            delivered: 0,
            acks: BTreeMap::new(),
            pending: None,
            paused: false,
        })
    }

    async fn connect(&mut self) -> Result<SourceEvent, ConnectorError> {
        let mut session = stomp::connect(&self.settings).await?;
        let prefetch = self.prefetch.to_string();
        let mut out = Vec::new();
        stomp::encode(
            "SUBSCRIBE",
            &[
                ("id", SUBSCRIPTION),
                ("destination", &self.destination),
                ("ack", "client"),
                ("activemq.prefetchSize", &prefetch),
            ],
            b"",
            &mut out,
        );
        session
            .writer
            .write_all(&out)
            .await
            .map_err(|e| ConnectorError::retryable(format!("ActiveMQ: {e}")))?;
        self.session = Some(Subscribed {
            reader: session.reader,
            writer: session.writer,
            version_12: session.version == "1.2",
        });
        self.acks.clear();
        self.epoch += 1;
        self.seq = 0;
        Ok(SourceEvent::Assigned {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
        })
    }

    fn lose(&mut self) -> SourceEvent {
        self.session = None;
        self.pending = None;
        self.acks.clear();
        SourceEvent::Revoking {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
            within: Duration::from_millis(0),
        }
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

    fn add(&mut self, frame: stomp::Frame, version_12: bool) {
        let id = if version_12 {
            frame.header("ack")
        } else {
            frame.header("message-id")
        };
        let Some(id) = id.map(str::to_string) else {
            return;
        };
        self.delivered += 1;
        let position = self.delivered;
        self.acks.insert(position, id);
        let envelope = Envelope {
            key: None,
            value: frame.body.to_vec(),
            headers: Vec::new(),
            partition: PARTITION.0,
            position: Some(position as i64),
            timestamp: frame.header("timestamp").and_then(|t| t.parse().ok()),
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
}

impl Source for ActiveMqSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if self.session.is_none() {
                return self.connect().await;
            }
            loop {
                if self.full() {
                    return Ok(self.take()?.expect("full"));
                }
                // Paused, the source reads nothing: the broker stops at the prefetch.
                if self.paused {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
                }
                let wait = self.wait();
                let session = self.session.as_mut().expect("connected");
                let version_12 = session.version_12;
                match tokio::time::timeout(wait, session.reader.next()).await {
                    Ok(Ok(frame)) => match frame.command.as_str() {
                        "MESSAGE" => self.add(frame, version_12),
                        "ERROR" => {
                            let error = frame.error_text();
                            self.lose();
                            return Err(ConnectorError::retryable(format!(
                                "ActiveMQ {}: {error}",
                                self.destination
                            )));
                        }
                        _ => {}
                    },
                    Ok(Err(_)) => return Ok(self.lose()),
                    Err(_) => return Ok(self.take()?.unwrap_or(SourceEvent::Idle)),
                }
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            let mut last_id = None;
            for commit in commits {
                if commit.epoch != AssignmentEpoch(self.epoch) || self.session.is_none() {
                    return CommitOutcome::Stale;
                }
                let Position::Offset(last) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "ActiveMQ acknowledges message positions",
                    ));
                };
                let later = self.acks.split_off(&(last as u64 + 1));
                let done = std::mem::replace(&mut self.acks, later);
                if let Some((_, id)) = done.into_iter().next_back() {
                    last_id = Some(id);
                }
            }
            let Some(id) = last_id else {
                return CommitOutcome::Committed;
            };
            let session = self.session.as_mut().expect("checked");
            let mut out = Vec::new();
            // A client-mode ACK acknowledges the message and every one before it.
            if session.version_12 {
                stomp::encode("ACK", &[("id", &id)], b"", &mut out);
            } else {
                stomp::encode(
                    "ACK",
                    &[("message-id", &id), ("subscription", SUBSCRIPTION)],
                    b"",
                    &mut out,
                );
            }
            match session.writer.write_all(&out).await {
                Ok(()) => CommitOutcome::Committed,
                Err(error) => {
                    self.session = None;
                    CommitOutcome::Failed(ConnectorError::retryable(format!("ActiveMQ: {error}")))
                }
            }
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
            ordered: true,
            ack_kind: AckKind::Message,
            // Competing consumers of one queue.
            max_instances: None,
            needs_keepalive: false,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Some(mut session) = self.session.take() {
                let mut out = Vec::new();
                stomp::encode("DISCONNECT", &[], b"", &mut out);
                let _ = session.writer.write_all(&out).await;
                let _ = session.writer.shutdown().await;
            }
        })
    }
}

type Waiting = Arc<std::sync::Mutex<HashMap<String, oneshot::Sender<Result<(), String>>>>>;

struct SinkConnection {
    writer: OwnedWriteHalf,
    waiting: Waiting,
    broken: Arc<AtomicBool>,
    reader: tokio::task::JoinHandle<()>,
}

impl Drop for SinkConnection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

pub struct ActiveMqSink {
    settings: stomp::Settings,
    destination: String,
    receipt_timeout: Duration,
    connection: Mutex<Option<SinkConnection>>,
    receipts: AtomicU64,
}

impl ActiveMqSink {
    pub fn new(config: &Value) -> Result<ActiveMqSink, ConnectorError> {
        let (settings, destination) = settings(config)?;
        let seconds = text(config, "receipt_timeout")
            .and_then(|t| t.parse::<f64>().ok())
            .unwrap_or(30.0);
        Ok(ActiveMqSink {
            settings,
            destination,
            receipt_timeout: Duration::from_secs_f64(seconds.max(1.0)),
            connection: Mutex::new(None),
            receipts: AtomicU64::new(0),
        })
    }

    async fn connect(&self) -> Result<SinkConnection, ConnectorError> {
        let session = stomp::connect(&self.settings).await?;
        let waiting: Waiting = Arc::default();
        let broken = Arc::new(AtomicBool::new(false));
        let reader = tokio::spawn(read_receipts(
            session.reader,
            waiting.clone(),
            broken.clone(),
        ));
        Ok(SinkConnection {
            writer: session.writer,
            waiting,
            broken,
            reader,
        })
    }

    /// Sends the batch and returns the wait for its receipt.
    async fn send(&self, batch: &RecordBatch) -> Result<Confirmation, ConnectorError> {
        let envelopes =
            read_envelope(batch).map_err(|e| ConnectorError::fatal(format!("ActiveMQ: {e}")))?;
        if envelopes.is_empty() {
            return Ok(Box::pin(async { Ok(()) }));
        }
        let receipt = format!("mage-{}", self.receipts.fetch_add(1, Ordering::Relaxed));
        let mut out = Vec::with_capacity(envelopes.iter().map(|e| e.value.len() + 128).sum());
        for (index, envelope) in envelopes.iter().enumerate() {
            let (body, metadata) = outgoing(&envelope.value);
            let mut headers: Vec<(&str, &str)> = vec![
                ("destination", &self.destination),
                ("persistent", "true"),
                ("content-type", "application/json"),
            ];
            headers.extend(metadata.iter().map(|(k, v)| (k.as_str(), v.as_str())));
            if index == envelopes.len() - 1 {
                headers.push(("receipt", &receipt));
            }
            stomp::encode("SEND", &headers, &body, &mut out);
        }
        let (sender, receiver) = oneshot::channel();
        let mut guard = self.connection.lock().await;
        if guard
            .as_ref()
            .is_none_or(|c| c.broken.load(Ordering::Relaxed))
        {
            *guard = Some(self.connect().await?);
        }
        let connection = guard.as_mut().expect("connected");
        connection
            .waiting
            .lock()
            .expect("lock")
            .insert(receipt.clone(), sender);
        if let Err(error) = connection.writer.write_all(&out).await {
            *guard = None;
            return Err(ConnectorError::retryable(format!("ActiveMQ: {error}")));
        }
        drop(guard);
        let timeout = self.receipt_timeout;
        let destination = self.destination.clone();
        Ok(Box::pin(async move {
            match tokio::time::timeout(timeout, receiver).await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(error))) => Err(ConnectorError::retryable(format!(
                    "ActiveMQ refused a message for {destination}: {error}"
                ))),
                Ok(Err(_)) => Err(ConnectorError::retryable(
                    "ActiveMQ closed the connection before confirming the messages",
                )),
                Err(_) => Err(ConnectorError::retryable(format!(
                    "ActiveMQ did not confirm the messages for {destination} within {} s",
                    timeout.as_secs()
                ))),
            }
        }))
    }
}

/// Settles receipts as the broker sends them; an error or a closed connection fails every
/// batch waiting.
async fn read_receipts(mut reader: stomp::Reader, waiting: Waiting, broken: Arc<AtomicBool>) {
    let failure = loop {
        match reader.next().await {
            Ok(frame) if frame.command == "RECEIPT" => {
                let id = frame.header("receipt-id").unwrap_or_default().to_string();
                if let Some(sender) = waiting.lock().expect("lock").remove(&id) {
                    let _ = sender.send(Ok(()));
                }
            }
            Ok(frame) if frame.command == "ERROR" => break frame.error_text(),
            Ok(_) => {}
            Err(error) => break error.message,
        }
    };
    broken.store(true, Ordering::Relaxed);
    for (_, sender) in waiting.lock().expect("lock").drain() {
        let _ = sender.send(Err(failure.clone()));
    }
}

/// What MageML sends for a message: an object's `data` (or the object) as JSON with its
/// `metadata` as headers; anything else as its JSON.
fn outgoing(value: &[u8]) -> (Vec<u8>, Vec<(String, String)>) {
    if value.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{') {
        return (value.to_vec(), Vec::new());
    }
    let Ok(fields) = serde_json::from_slice::<HashMap<String, Box<RawValue>>>(value) else {
        return (value.to_vec(), Vec::new());
    };
    let body = match fields.get("data") {
        Some(data) => data.get().as_bytes().to_vec(),
        None => value.to_vec(),
    };
    let metadata = fields
        .get("metadata")
        .and_then(|m| serde_json::from_str::<serde_json::Map<String, Value>>(m.get()).ok())
        .map(|map| {
            map.into_iter()
                .map(|(k, v)| match v {
                    Value::String(s) => (k, s),
                    other => (k, other.to_string()),
                })
                .collect()
        })
        .unwrap_or_default();
    (body, metadata)
}

impl Sink for ActiveMqSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move { self.send(batch).await?.await })
    }

    /// One connection keeps the order of what is sent on it: the next batch is sent while
    /// the broker confirms this one.
    fn queue<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<Confirmation, ConnectorError>> {
        Box::pin(async move { self.send(batch).await })
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
    fn outgoing_messages_follow_mageml() {
        let (body, headers) =
            outgoing(br#"{"data": {"n": 12345678901234567890}, "metadata": {"k": "v", "n": 1}}"#);
        assert_eq!(body, br#"{"n": 12345678901234567890}"#);
        let mut headers = headers;
        headers.sort();
        assert_eq!(
            headers,
            [
                ("k".to_string(), "v".to_string()),
                ("n".to_string(), "1".to_string())
            ]
        );
        assert_eq!(outgoing(br#"{"n": 1}"#).0, br#"{"n": 1}"#);
        assert_eq!(outgoing(br#""text""#).0, br#""text""#);
    }

    #[test]
    fn settings_follow_mageml() {
        let limits = BatchLimits {
            max_records: 10,
            max_bytes: 1 << 20,
            max_wait: Duration::from_millis(100),
        };
        let source = ActiveMqSource::new(
            &serde_json::json!({"connection_host": "mq", "connection_port": 61613,
                                "queue_name": "orders"}),
            limits,
        )
        .unwrap();
        assert_eq!(source.destination, "/queue/orders");
        assert_eq!(
            (source.settings.user.as_str(), source.prefetch),
            ("admin", 1000)
        );
        assert!(ActiveMqSource::new(&serde_json::json!({"queue_name": "q"}), limits).is_err());
    }
}
