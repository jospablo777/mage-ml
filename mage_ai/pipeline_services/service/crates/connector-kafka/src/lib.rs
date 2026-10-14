//! Kafka source and sink for streams, through librdkafka (ADR-0021).
//!
//! Source: a consumer group member with auto commit and auto offset store off, cooperative
//! sticky assignment, and offsets committed only for positions the runner acknowledged
//! (ADR-0029). Each assignment of a (topic, partition) gets a new epoch; a commit of an older
//! epoch is refused. Messages are grouped per partition into batches by count, bytes and
//! wait time.
//!
//! Sink: an idempotent producer with `acks=all`; a batch is confirmed when every message's
//! delivery report is.
//!
//! Settings follow Mage's Kafka source and sink YAML: `bootstrap_server`, `topic` or
//! `topics`, `consumer_group`, `auto_offset_reset`, `security_protocol`, `sasl_config`
//! (`mechanism`, `username`, `password`), `ssl_config` (`cafile`, `certfile`, `keyfile`,
//! `password`, `check_hostname`), and `librdkafka` for any other client property.

pub mod avro;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, Confirmation,
    ConnectorError, Envelope, EnvelopeBuilder, Header, PartitionId, Position, RecordBatch, Sink,
    SinkCapabilities, Source, SourceCapabilities, SourceEvent, read_envelope,
};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{Header as KafkaHeader, Headers, Message, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::Value;
use tokio::sync::mpsc;

/// How a source groups messages into batches.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_wait: Duration,
}

/// Azure Event Hubs through its Kafka endpoint: the namespace's host on port 9093, signed
/// in with SASL PLAIN as `$ConnectionString` and the connection string as the password.
/// The connection string is read at run time: in the manifest it is an environment
/// reference, so its namespace is not known before.
fn event_hubs(client: &mut ClientConfig, connection: &str) -> Result<(), ConnectorError> {
    let endpoint = connection
        .split(';')
        .find_map(|part| part.trim().strip_prefix("Endpoint="))
        .ok_or_else(|| {
            ConnectorError::fatal(
                "Azure Event Hubs: the connection string has no Endpoint=sb://<namespace>... part.",
            )
        })?;
    let host = endpoint
        .trim_start_matches("sb://")
        .trim_end_matches('/')
        .split('/')
        .next()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| ConnectorError::fatal("Azure Event Hubs: the Endpoint has no host."))?;
    client
        .set("bootstrap.servers", format!("{host}:9093"))
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "PLAIN")
        .set("sasl.username", "$ConnectionString")
        .set("sasl.password", connection);
    Ok(())
}

/// Mage's serde_config: JSON (and text) as they are, or Avro decoded with the schemas of a
/// schema registry.
fn message_decoder(config: &Value) -> Result<Option<avro::AvroDecoder>, ConnectorError> {
    let Some(serde) = config.get("serde_config") else {
        return Ok(None);
    };
    let method = text(serde, "serialization_method")
        .unwrap_or_else(|| "JSON".into())
        .to_uppercase();
    match method.as_str() {
        // RAW_VALUE: the bytes as they are; the stream hands them to blocks as bytes.
        "JSON" | "RAW_VALUE" => Ok(None),
        "AVRO" => Ok(Some(avro::AvroDecoder::new(avro::RegistryConfig {
            url: text(serde, "schema_registry_url").ok_or_else(|| {
                ConnectorError::fatal("Kafka source: AVRO messages need schema_registry_url.")
            })?,
            username: text(serde, "schema_registry_username"),
            password: text(serde, "schema_registry_password"),
        }))),
        other => Err(ConnectorError::fatal(format!(
            "Kafka source: {other} messages are not read by services; they read JSON, text, \
             raw bytes and Avro (with a schema registry)."
        ))),
    }
}

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The client properties shared by the source and the sink.
fn client_config(config: &Value) -> Result<ClientConfig, ConnectorError> {
    let mut client = ClientConfig::new();
    if let Some(connection) = text(config, "event_hubs_connection_string") {
        event_hubs(&mut client, &connection)?;
        return Ok(client);
    }
    let servers = text(config, "bootstrap_server")
        .or_else(|| text(config, "bootstrap_servers"))
        .ok_or_else(|| ConnectorError::fatal("Kafka settings need bootstrap_server."))?;
    client.set("bootstrap.servers", servers);
    if let Some(protocol) = text(config, "security_protocol") {
        client.set("security.protocol", protocol);
    }
    if let Some(sasl) = config.get("sasl_config") {
        if let Some(mechanism) = text(sasl, "mechanism") {
            client.set("sasl.mechanism", mechanism);
        }
        if let Some(username) = text(sasl, "username") {
            client.set("sasl.username", username);
        }
        if let Some(password) = text(sasl, "password") {
            client.set("sasl.password", password);
        }
    }
    if let Some(ssl) = config.get("ssl_config") {
        for (key, property) in [
            ("cafile", "ssl.ca.location"),
            ("certfile", "ssl.certificate.location"),
            ("keyfile", "ssl.key.location"),
            ("password", "ssl.key.password"),
        ] {
            if let Some(value) = text(ssl, key) {
                client.set(property, value);
            }
        }
        if let Some(check) = ssl.get("check_hostname").and_then(Value::as_bool) {
            client.set(
                "ssl.endpoint.identification.algorithm",
                if check { "https" } else { "none" },
            );
        }
    }
    if let Some(extra) = config.get("librdkafka").and_then(Value::as_object) {
        for (key, value) in extra {
            let value = match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            client.set(key, value);
        }
    }
    Ok(client)
}

fn connector_error(error: KafkaError) -> ConnectorError {
    let retryable = !matches!(
        error.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::MessageSizeTooLarge
                | RDKafkaErrorCode::InvalidRecord
                | RDKafkaErrorCode::TopicAuthorizationFailed
                | RDKafkaErrorCode::GroupAuthorizationFailed
                | RDKafkaErrorCode::SaslAuthenticationFailed
                | RDKafkaErrorCode::UnknownTopicOrPartition
                | RDKafkaErrorCode::InvalidConfig
        )
    );
    ConnectorError {
        message: format!("Kafka: {error}"),
        retryable,
    }
}

/// How long a revoke waits for the stream to finish the partition's batches in flight and
/// acknowledge them, before the partition goes to its next owner anyway. Well under the
/// group's `max.poll.interval.ms` (5 minutes), which the wait counts against.
const REVOKE_WITHIN: Duration = Duration::from_secs(20);
/// How often the poll thread updates the lag.
const LAG_EVERY: Duration = Duration::from_secs(5);
/// Chunks of messages between the poll thread and the stream; librdkafka's own queue, sized
/// from the stream's byte budget, holds the rest.
const CHANNEL: usize = 16;
/// Messages per chunk at most: the poll thread takes what librdkafka already holds.
const CHUNK: usize = 1024;
/// How long a pause only holds a partition's batches back before it becomes librdkafka's
/// pause (see [`KafkaSource::pause`]).
const HOLD: Duration = Duration::from_secs(5);

/// (topic, partition) to the stream's partition ids, and each one's current epoch.
#[derive(Debug, Default)]
struct Table {
    ids: HashMap<(String, i32), u32>,
    names: Vec<(String, i32)>,
    epochs: BTreeMap<u32, AssignmentEpoch>,
    next_epoch: u64,
    /// Revokes waiting for the stream: partition to the waiting rebalance callback.
    waiters: HashMap<u32, std::sync::mpsc::Sender<u32>>,
    /// Messages behind the end of each assigned partition.
    lag: BTreeMap<u32, i64>,
    /// The next offset to read, as last committed (asynchronously) per partition: committed
    /// again synchronously before a partition is given up and when the source closes.
    committed: BTreeMap<u32, i64>,
    /// An asynchronous commit's failure, reported by the next commit.
    commit_error: Option<String>,
}

impl Table {
    /// The committed offsets of the partitions, to commit synchronously.
    fn committed_list(&self, ids: impl Iterator<Item = u32>) -> TopicPartitionList {
        let mut list = TopicPartitionList::new();
        for id in ids {
            if let (Some(offset), Some((topic, partition))) =
                (self.committed.get(&id), self.names.get(id as usize))
            {
                let _ = list.add_partition_offset(topic, *partition, Offset::Offset(*offset));
            }
        }
        list
    }
}

impl Table {
    fn id(&mut self, topic: &str, partition: i32) -> u32 {
        let key = (topic.to_string(), partition);
        if let Some(id) = self.ids.get(&key) {
            return *id;
        }
        let id = self.names.len() as u32;
        self.ids.insert(key.clone(), id);
        self.names.push(key);
        id
    }

    fn assign(&mut self, id: u32) -> AssignmentEpoch {
        self.next_epoch += 1;
        let epoch = AssignmentEpoch(self.next_epoch);
        self.epochs.insert(id, epoch);
        epoch
    }
}

/// What the poll thread hands to the stream, in the order librdkafka delivered it: an
/// assignment change is never reordered with the messages around it.
enum Polled {
    /// Messages polled together: one handoff per chunk, not per message (I-84).
    Messages(Vec<Fetched>),
    Event(SourceEvent),
    Error(KafkaError),
}

/// A polled message and its value decoded for the blocks (Avro as JSON), or why it could
/// not be: then the message keeps its bytes and says why in a mage-decode-error header.
struct Fetched {
    message: rdkafka::message::OwnedMessage,
    value: Option<Vec<u8>>,
    error: Option<String>,
}

impl Fetched {
    fn new(
        message: rdkafka::message::OwnedMessage,
        decoder: &mut Option<avro::AvroDecoder>,
    ) -> Fetched {
        let (value, error) = match (decoder.as_mut(), message.payload()) {
            (Some(decoder), Some(payload)) => match decoder.decode(payload) {
                Ok(json) => (Some(json), None),
                Err(error) => (None, Some(format!("Avro: {error}"))),
            },
            _ => (None, None),
        };
        Fetched {
            message,
            value,
            error,
        }
    }
}

struct RebalanceContext {
    table: Arc<Mutex<Table>>,
    polled: mpsc::Sender<Polled>,
    stopped: Arc<AtomicBool>,
}

impl ClientContext for RebalanceContext {}

impl RebalanceContext {
    /// Whether a callback may wait: only on the poll thread while the source runs. librdkafka
    /// also calls the callbacks when the consumer closes, on whatever thread drops it, which
    /// can be one of the runtime's.
    fn may_wait(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed) && tokio::runtime::Handle::try_current().is_err()
    }

    /// Hands an item to the stream; false when the stream no longer reads.
    fn send(&self, item: Polled) -> bool {
        if self.may_wait() {
            self.polled.blocking_send(item).is_ok()
        } else {
            self.polled.try_send(item).is_ok()
        }
    }
}

impl ConsumerContext for RebalanceContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        if let Err(error) = result {
            let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
            table.commit_error = Some(error.to_string());
        }
    }

    /// Runs on the poll thread before partitions go. The partitions keep their epochs while
    /// the stream finishes their batches in flight and commits them (I-34), so the next owner
    /// starts after them; after REVOKE_WITHIN they go anyway, and what was not done is read
    /// again by the next owner.
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(list) = rebalance else {
            return;
        };
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut waiting = BTreeSet::new();
        let mut revoked = Vec::new();
        {
            let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
            for element in list.elements() {
                let id = table.id(element.topic(), element.partition());
                if let Some(epoch) = table.epochs.get(&id).copied() {
                    table.waiters.insert(id, done_tx.clone());
                    waiting.insert(id);
                    revoked.push((id, epoch));
                }
            }
        }
        for (id, epoch) in &revoked {
            let event = SourceEvent::Revoking {
                partition: PartitionId(*id),
                epoch: *epoch,
                within: REVOKE_WITHIN,
            };
            // The stream has stopped reading: nothing to wait for.
            if !self.send(Polled::Event(event)) {
                waiting.clear();
            }
        }
        if !self.may_wait() {
            waiting.clear();
        }
        let deadline = Instant::now() + REVOKE_WITHIN;
        while !waiting.is_empty() {
            let left = deadline.saturating_duration_since(Instant::now());
            match done_rx.recv_timeout(left) {
                Ok(id) => {
                    waiting.remove(&id);
                }
                Err(_) => break,
            }
        }
        // The stream's commits are asynchronous: the last ones go out before the partitions
        // do, so their next owner does not read them again.
        let list = {
            let table = self.table.lock().unwrap_or_else(|p| p.into_inner());
            table.committed_list(revoked.iter().map(|(id, _)| *id))
        };
        if list.count() > 0 {
            let _ = consumer.commit(&list, CommitMode::Sync);
        }
        let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        for (id, _) in revoked {
            table.waiters.remove(&id);
            table.epochs.remove(&id);
            table.lag.remove(&id);
            table.committed.remove(&id);
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(list) = rebalance {
            let mut assigned = Vec::new();
            {
                let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
                for element in list.elements() {
                    let id = table.id(element.topic(), element.partition());
                    assigned.push((id, table.assign(id)));
                }
            }
            for (id, epoch) in assigned {
                let _ = self.send(Polled::Event(SourceEvent::Assigned {
                    partition: PartitionId(id),
                    epoch,
                }));
            }
        }
    }
}

type KafkaConsumer = BaseConsumer<RebalanceContext>;

/// Polls the consumer until stopped. Rebalance callbacks run here, so a revoke can wait for
/// the stream without holding up the stream's own task.
fn poll_loop(
    consumer: Arc<KafkaConsumer>,
    table: Arc<Mutex<Table>>,
    stop: Arc<AtomicBool>,
    mut decoder: Option<avro::AvroDecoder>,
) {
    let polled = consumer.context().polled.clone();
    let mut lag_at = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        // One message woke the stream's task and parked it again each time: the two
        // threads mostly waited on each other (I-84). The messages librdkafka already
        // holds go together.
        let mut items = Vec::with_capacity(2);
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                // Decoded here, on the poll thread: a new schema's first read from the
                // registry waits here, not in the stream's loop.
                let mut chunk = Vec::with_capacity(CHUNK);
                chunk.push(Fetched::new(message.detach(), &mut decoder));
                while chunk.len() < CHUNK {
                    match consumer.poll(Duration::ZERO) {
                        Some(Ok(message)) => {
                            chunk.push(Fetched::new(message.detach(), &mut decoder))
                        }
                        Some(Err(error)) => {
                            items.push(Polled::Error(error));
                            break;
                        }
                        None => break,
                    }
                }
                items.insert(0, Polled::Messages(chunk));
            }
            Some(Err(error)) => items.push(Polled::Error(error)),
            None => {}
        }
        if items
            .into_iter()
            .any(|item| polled.blocking_send(item).is_err())
        {
            break;
        }
        if lag_at.elapsed() >= LAG_EVERY {
            lag_at = Instant::now();
            update_lag(&consumer, &table);
        }
    }
}

/// Lag per partition from the high watermarks librdkafka keeps from its fetches (no request
/// to the broker) and the consumer's position.
fn update_lag(consumer: &KafkaConsumer, table: &Mutex<Table>) {
    let Ok(positions) = consumer.position() else {
        return;
    };
    let mut lag = BTreeMap::new();
    let mut table = table.lock().unwrap_or_else(|p| p.into_inner());
    for element in positions.elements() {
        let Some(high) = cached_high_watermark(consumer, element.topic(), element.partition())
        else {
            continue;
        };
        let id = table.id(element.topic(), element.partition());
        if !table.epochs.contains_key(&id) || high < 0 {
            continue;
        }
        let position = match element.offset() {
            Offset::Offset(offset) => offset,
            // Nothing read yet: everything up to the end is behind.
            _ => continue,
        };
        lag.insert(id, (high - position).max(0));
    }
    table.lag = lag;
}

/// The partition's high watermark as librdkafka last saw it in a fetch. rdkafka has no
/// wrapper for it; `fetch_watermarks` asks the broker, which the poll thread must not wait on.
fn cached_high_watermark(consumer: &KafkaConsumer, topic: &str, partition: i32) -> Option<i64> {
    use rdkafka::bindings::{rd_kafka_get_watermark_offsets, rd_kafka_resp_err_t};

    let topic = std::ffi::CString::new(topic).ok()?;
    let (mut low, mut high) = (0i64, 0i64);
    // SAFETY: the client handle lives as long as `consumer`, the topic is a valid C string
    // for the call, and the function only reads librdkafka's cached offsets (thread safe).
    let code = unsafe {
        rd_kafka_get_watermark_offsets(
            consumer.client().native_ptr(),
            topic.as_ptr(),
            partition,
            &mut low,
            &mut high,
        )
    };
    (code == rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR).then_some(high)
}

struct Pending {
    builder: EnvelopeBuilder,
    epoch: AssignmentEpoch,
    first: i64,
    last: i64,
    started: Instant,
}

pub struct KafkaSource {
    consumer: Arc<KafkaConsumer>,
    table: Arc<Mutex<Table>>,
    polled: mpsc::Receiver<Polled>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Partitions being revoked: their messages still in the channel are not delivered.
    revoking: BTreeSet<u32>,
    pending: BTreeMap<u32, Pending>,
    seq: BTreeMap<u32, u64>,
    paused: BTreeSet<u32>,
    /// Messages of a chunk not yet in a batch: a batch filled before the chunk ended.
    carry: std::collections::VecDeque<Fetched>,
    /// Paused partitions only held back, since when; the others are paused in librdkafka.
    held: BTreeMap<u32, Instant>,
    stopped: BTreeSet<u32>,
    limits: BatchLimits,
}

impl KafkaSource {
    /// `byte_budget`: the stream's in-flight byte budget; librdkafka's own queue is sized
    /// from it so the client cannot buffer past what the stream allows.
    pub fn new(
        config: &Value,
        limits: BatchLimits,
        byte_budget: u64,
    ) -> Result<KafkaSource, ConnectorError> {
        let topics: Vec<String> = match (config.get("topics"), text(config, "topic")) {
            (Some(Value::Array(list)), _) if !list.is_empty() => list
                .iter()
                .filter_map(|t| t.as_str().map(String::from))
                .collect(),
            (_, Some(topic)) => vec![topic],
            _ => {
                return Err(ConnectorError::fatal(
                    "Kafka source settings need topic or topics.",
                ));
            }
        };
        let group = text(config, "consumer_group")
            .ok_or_else(|| ConnectorError::fatal("Kafka source settings need consumer_group."))?;
        let mut client = client_config(config)?;
        let queue_kb = (byte_budget / 1024).clamp(1024, 65536);
        client
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "false")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("queued.max.messages.kbytes", queue_kb.to_string())
            // The queue is kept small (the stream's byte budget), so it is often full; after
            // that librdkafka waits fetch.queue.backoff.ms (1 second by default) before it
            // fetches again, and a stream sat idle most of the time (I-78).
            .set("fetch.queue.backoff.ms", "20")
            .set(
                "auto.offset.reset",
                text(config, "auto_offset_reset").unwrap_or_else(|| "latest".into()),
            );
        let table = Arc::new(Mutex::new(Table::default()));
        let (tx, polled) = mpsc::channel(CHANNEL);
        let stop = Arc::new(AtomicBool::new(false));
        let consumer: KafkaConsumer = client
            .create_with_context(RebalanceContext {
                table: table.clone(),
                polled: tx,
                stopped: stop.clone(),
            })
            .map_err(connector_error)?;
        let decoder = message_decoder(config)?;
        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        consumer.subscribe(&names).map_err(connector_error)?;
        let consumer = Arc::new(consumer);
        let thread = std::thread::Builder::new()
            .name("kafka-source".into())
            .spawn({
                let (consumer, table, stop) = (consumer.clone(), table.clone(), stop.clone());
                move || poll_loop(consumer, table, stop, decoder)
            })
            .map_err(|e| ConnectorError::fatal(format!("Kafka: {e}")))?;
        Ok(KafkaSource {
            consumer,
            table,
            polled,
            stop,
            thread: Some(thread),
            revoking: BTreeSet::new(),
            pending: BTreeMap::new(),
            seq: BTreeMap::new(),
            paused: BTreeSet::new(),
            carry: std::collections::VecDeque::new(),
            held: BTreeMap::new(),
            stopped: BTreeSet::new(),
            limits,
        })
    }

    fn handle(&mut self, event: SourceEvent) -> SourceEvent {
        match &event {
            SourceEvent::Revoking { partition, .. } => {
                // Messages read but not yet in a batch stay with the partition: the next
                // owner reads them.
                self.pending.remove(&partition.0);
                self.paused.remove(&partition.0);
                self.held.remove(&partition.0);
                self.stopped.remove(&partition.0);
                self.revoking.insert(partition.0);
            }
            SourceEvent::Assigned { partition, .. } => {
                self.revoking.remove(&partition.0);
                self.seq.insert(partition.0, 0);
                // A new assignment fetches in librdkafka: a pause holds it again.
                self.stopped.remove(&partition.0);
                if self.paused.contains(&partition.0) {
                    self.held.insert(partition.0, Instant::now());
                }
            }
            _ => {}
        }
        event
    }

    fn take(&mut self, id: u32) -> Result<Option<SourceEvent>, ConnectorError> {
        let Some(mut pending) = self.pending.remove(&id) else {
            return Ok(None);
        };
        let bytes = pending.builder.bytes() as u64;
        let records = pending.builder.len() as u32;
        let batch = pending
            .builder
            .finish()
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let seq = self.seq.entry(id).or_insert(0);
        let meta = BatchMeta {
            id: BatchId {
                partition: PartitionId(id),
                epoch: pending.epoch,
                seq: *seq,
            },
            first: Position::Offset(pending.first),
            last: Position::Offset(pending.last),
            records,
            bytes,
        };
        *seq += 1;
        Ok(Some(SourceEvent::Batch { batch, meta }))
    }

    fn full(&self, pending: &Pending) -> bool {
        pending.builder.len() >= self.limits.max_records
            || pending.builder.bytes() >= self.limits.max_bytes
    }

    /// A batch of a partition not paused that is full, or (when `due`) the oldest one past
    /// its wait.
    fn ready(&mut self, due: bool) -> Result<Option<SourceEvent>, ConnectorError> {
        let full = self
            .pending
            .iter()
            .find(|(id, p)| !self.paused.contains(id) && self.full(p));
        if let Some((id, _)) = full {
            let id = *id;
            return self.take(id);
        }
        let now = Instant::now();
        let oldest = self
            .pending
            .iter()
            .filter(|(id, _)| !self.paused.contains(id))
            .min_by_key(|(_, p)| p.started)
            .filter(|(_, p)| due || now.duration_since(p.started) >= self.limits.max_wait)
            .map(|(id, _)| *id);
        match oldest {
            Some(id) => self.take(id),
            None => Ok(None),
        }
    }

    fn wait(&self) -> Duration {
        let now = Instant::now();
        self.pending
            .iter()
            .filter(|(id, _)| !self.paused.contains(id))
            .map(|(_, p)| (p.started + self.limits.max_wait).saturating_duration_since(now))
            .min()
            .unwrap_or(self.limits.max_wait)
            .max(Duration::from_millis(1))
    }

    fn tpl(&self, id: u32) -> Option<TopicPartitionList> {
        let table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        let (topic, partition) = table.names.get(id as usize)?.clone();
        let mut list = TopicPartitionList::new();
        list.add_partition(&topic, partition);
        Some(list)
    }

    /// Adds the message to its partition's batch; true when that batch is now full.
    fn add(&mut self, fetched: Fetched) -> bool {
        let Fetched {
            message,
            value,
            error,
        } = fetched;
        let (id, epoch) = {
            let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
            let id = table.id(message.topic(), message.partition());
            (id, table.epochs.get(&id).copied())
        };
        // A message of a partition no longer assigned, or being revoked, is not delivered;
        // the next owner reads it.
        let Some(epoch) = epoch else { return false };
        if self.revoking.contains(&id) {
            return false;
        }
        let mut headers: Vec<Header> = message
            .headers()
            .map(|h| {
                h.iter()
                    .map(|header| Header {
                        key: header.key.to_string(),
                        value: header.value.map(<[u8]>::to_vec).unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        if let Some(error) = error {
            headers.push(Header {
                key: "mage-decode-error".into(),
                value: error.into_bytes(),
            });
        }
        let offset = message.offset();
        let envelope = Envelope {
            key: message.key().map(<[u8]>::to_vec),
            value: value
                .unwrap_or_else(|| message.payload().map(<[u8]>::to_vec).unwrap_or_default()),
            headers,
            partition: id,
            position: Some(offset),
            timestamp: message.timestamp().to_millis(),
        };
        let pending = self.pending.entry(id).or_insert_with(|| Pending {
            builder: EnvelopeBuilder::new(),
            epoch,
            first: offset,
            last: offset,
            started: Instant::now(),
        });
        pending.builder.push(&envelope);
        pending.last = offset;
        pending.builder.len() >= self.limits.max_records
            || pending.builder.bytes() >= self.limits.max_bytes
    }
}

impl KafkaSource {
    /// Pauses in librdkafka the held partitions that need it: held for [`HOLD`] (the poll
    /// thread must go on polling, for rebalances and max.poll.interval.ms), or with a full
    /// batch waiting while other partitions are read.
    fn stop_held(&mut self) {
        let now = Instant::now();
        let due: Vec<u32> = self
            .held
            .iter()
            .filter(|(id, since)| {
                now.duration_since(**since) >= HOLD
                    || self.pending.get(id).is_some_and(|p| self.full(p))
            })
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            self.held.remove(&id);
            if self.stopped.insert(id)
                && let Some(list) = self.tpl(id)
            {
                let _ = self.consumer.pause(&list);
            }
        }
    }

    /// When every assigned partition is paused and one is only held: until when to stop
    /// reading the channel, so the poll thread and librdkafka's queue stop the fetching.
    fn holding_until(&self) -> Option<Instant> {
        let since = self.held.values().min()?;
        let table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        table
            .epochs
            .keys()
            .all(|id| self.paused.contains(id))
            .then(|| *since + HOLD)
    }
}

impl KafkaSource {
    /// Adds the messages of the last chunk not yet added, until a batch is full.
    fn take_carry(&mut self) -> Result<Option<SourceEvent>, ConnectorError> {
        while let Some(message) = self.carry.pop_front() {
            if self.add(message) {
                return self.ready(false);
            }
        }
        Ok(None)
    }
}

impl Source for KafkaSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            loop {
                if let Some(batch) = self.ready(false)? {
                    return Ok(batch);
                }
                // A chunk's messages before anything newer, so events keep their order.
                if let Some(batch) = self.take_carry()? {
                    return Ok(batch);
                }
                self.stop_held();
                if let Some(until) = self.holding_until() {
                    tokio::time::sleep_until(until.into()).await;
                    continue;
                }
                let wait = self.wait();
                match tokio::time::timeout(wait, self.polled.recv()).await {
                    Ok(Some(Polled::Messages(chunk))) => self.carry.extend(chunk),
                    Ok(Some(Polled::Event(event))) => return Ok(self.handle(event)),
                    Ok(Some(Polled::Error(error))) => return Err(connector_error(error)),
                    Ok(None) => return Ok(SourceEvent::End),
                    Err(_) => {
                        return Ok(self.ready(true)?.unwrap_or(SourceEvent::Idle));
                    }
                }
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            let mut list = TopicPartitionList::new();
            {
                let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(error) = table.commit_error.take() {
                    return CommitOutcome::Failed(ConnectorError::retryable(format!(
                        "Kafka: committing positions failed: {error}"
                    )));
                }
                let mut next = Vec::with_capacity(commits.len());
                for commit in commits {
                    let current = table.epochs.get(&commit.partition.0).copied();
                    let Position::Offset(last) = commit.position else {
                        return CommitOutcome::Failed(ConnectorError::fatal(
                            "Kafka acknowledges offsets",
                        ));
                    };
                    if current != Some(commit.epoch) {
                        return CommitOutcome::Stale;
                    }
                    let Some((topic, partition)) = table.names.get(commit.partition.0 as usize)
                    else {
                        return CommitOutcome::Stale;
                    };
                    // Kafka commits the next offset to read.
                    if let Err(error) =
                        list.add_partition_offset(topic, *partition, Offset::Offset(last + 1))
                    {
                        return CommitOutcome::Failed(connector_error(error));
                    }
                    next.push((commit.partition.0, last + 1));
                }
                table.committed.extend(next);
            }
            // Asynchronous, in order: a synchronous commit held the stream's loop for a
            // round trip to the coordinator on every batch (I-85). A failure comes back
            // through commit_callback; the positions are committed again synchronously
            // before a partition is given up and at close. Committed here means handed to
            // librdkafka: after a crash, a batch can be read again, as at-least-once allows.
            match self.consumer.commit(&list, CommitMode::Async) {
                Ok(()) => CommitOutcome::Committed,
                Err(error) => match error.rdkafka_error_code() {
                    Some(RDKafkaErrorCode::IllegalGeneration)
                    | Some(RDKafkaErrorCode::RebalanceInProgress)
                    | Some(RDKafkaErrorCode::UnknownMemberId) => CommitOutcome::Stale,
                    _ => CommitOutcome::Failed(connector_error(error)),
                },
            }
        })
    }

    /// Holds the partition's batches back. librdkafka's pause drops what it fetched, and
    /// it fetched again about 0.8 seconds after a resume: a stream under backpressure,
    /// paused and resumed every few batches, stood still most of the time (I-78). Held, the
    /// partition is not read while every partition is paused, so the bounded channel and
    /// librdkafka's queue stop the fetching; [`KafkaSource::stop_held`] pauses it in
    /// librdkafka when that cannot last.
    fn pause(&mut self, partition: PartitionId) {
        if self.paused.insert(partition.0) && !self.stopped.contains(&partition.0) {
            self.held.insert(partition.0, Instant::now());
        }
    }

    fn resume(&mut self, partition: PartitionId) {
        self.paused.remove(&partition.0);
        self.held.remove(&partition.0);
        if self.stopped.remove(&partition.0)
            && let Some(list) = self.tpl(partition.0)
        {
            let _ = self.consumer.resume(&list);
        }
    }

    fn revoked(&mut self, partition: PartitionId, _epoch: AssignmentEpoch) {
        let waiter = {
            let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
            table.waiters.remove(&partition.0)
        };
        if let Some(waiter) = waiter {
            let _ = waiter.send(partition.0);
        }
    }

    fn lag(&self) -> BTreeMap<u32, i64> {
        self.table
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lag
            .clone()
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            replayable: true,
            ordered: true,
            ack_kind: AckKind::Offset,
            max_instances: None,
            needs_keepalive: false,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.stop.store(true, Ordering::Relaxed);
            // A blocked send in the poll thread or a rebalance callback fails now.
            self.polled.close();
            // Unblock waiting revokes: the stream will not finish them.
            let waiters: Vec<_> = {
                let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
                table.waiters.drain().collect()
            };
            for (id, waiter) in waiters {
                let _ = waiter.send(id);
            }
            if let Some(thread) = self.thread.take() {
                let _ = tokio::task::spawn_blocking(move || thread.join()).await;
            }
            // The last asynchronous commits, made sure of before the consumer leaves.
            let list = {
                let table = self.table.lock().unwrap_or_else(|p| p.into_inner());
                table.committed_list(table.epochs.keys().copied())
            };
            let consumer = self.consumer.clone();
            let _ = tokio::task::spawn_blocking(move || {
                if list.count() > 0 {
                    let _ = consumer.commit(&list, CommitMode::Sync);
                }
                consumer.unsubscribe()
            })
            .await;
        })
    }
}

impl Drop for KafkaSource {
    fn drop(&mut self) {
        // Also when the stream ends without close: the poll thread must not outlive it.
        self.stop.store(true, Ordering::Relaxed);
        self.polled.close();
    }
}

pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    timeout: Duration,
}

impl KafkaSink {
    pub fn new(config: &Value) -> Result<KafkaSink, ConnectorError> {
        let topic = text(config, "topic")
            .ok_or_else(|| ConnectorError::fatal("Kafka sink settings need topic."))?;
        let mut client = client_config(config)?;
        client
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("linger.ms", "5")
            .set("compression.type", "lz4");
        let producer: FutureProducer = client.create().map_err(connector_error)?;
        Ok(KafkaSink {
            producer,
            topic,
            timeout: Duration::from_secs(30),
        })
    }
}

impl KafkaSink {
    /// Puts every message of the batch in the producer's queue, in order, and returns their
    /// deliveries: queuing each message only after the last was confirmed sent one message
    /// per round trip.
    async fn enqueue(&self, batch: &RecordBatch) -> Result<Vec<DeliveryFuture>, ConnectorError> {
        let messages = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let mut deliveries = Vec::with_capacity(messages.len());
        let started = std::time::Instant::now();
        for message in &messages {
            let mut headers = OwnedHeaders::new_with_capacity(message.headers.len());
            for h in &message.headers {
                headers = headers.insert(KafkaHeader {
                    key: &h.key,
                    value: Some(&h.value),
                });
            }
            let mut record = FutureRecord::to(&self.topic)
                .payload(&message.value)
                .headers(headers);
            if let Some(key) = &message.key {
                record = record.key(key);
            }
            loop {
                match self.producer.send_result(record) {
                    Ok(delivery) => {
                        deliveries.push(delivery);
                        break;
                    }
                    // The producer's queue is full: wait for deliveries to make room.
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned))
                        if started.elapsed() < self.timeout =>
                    {
                        record = returned;
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    Err((error, _)) => return Err(connector_error(error)),
                }
            }
        }
        Ok(deliveries)
    }
}

async fn delivered(deliveries: Vec<DeliveryFuture>) -> Result<(), ConnectorError> {
    for delivery in deliveries {
        match delivery.await {
            Ok(Ok(_)) => {}
            Ok(Err((error, _))) => return Err(connector_error(error)),
            Err(_) => {
                return Err(ConnectorError::retryable(
                    "Kafka: the producer stopped before confirming a message",
                ));
            }
        }
    }
    Ok(())
}

impl Sink for KafkaSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move { delivered(self.enqueue(batch).await?).await })
    }

    /// The producer keeps the order of what it was given (idempotence on), so the next
    /// batch is queued while this one's deliveries are confirmed: a batch waited for its
    /// last message's acknowledgement before the next was queued (I-85).
    fn queue<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<Confirmation, ConnectorError>> {
        Box::pin(async move {
            let deliveries = self.enqueue(batch).await?;
            Ok(Box::pin(delivered(deliveries)) as Confirmation)
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            use rdkafka::producer::Producer as _;
            let _ = self.producer.flush(Duration::from_secs(10));
        })
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
    fn event_hubs_connection_strings_become_the_kafka_endpoint() {
        let connection = "Endpoint=sb://shop.servicebus.windows.net/;SharedAccessKeyName=read;\
                          SharedAccessKey=abc=;EntityPath=orders";
        let client = client_config(&serde_json::json!({
            "event_hubs_connection_string": connection,
        }))
        .unwrap();
        assert_eq!(
            client.get("bootstrap.servers"),
            Some("shop.servicebus.windows.net:9093")
        );
        assert_eq!(client.get("sasl.username"), Some("$ConnectionString"));
        assert_eq!(client.get("sasl.password"), Some(connection));
        assert_eq!(client.get("security.protocol"), Some("SASL_SSL"));
        let error = client_config(&serde_json::json!({
            "event_hubs_connection_string": "SharedAccessKey=abc",
        }))
        .err()
        .unwrap();
        assert!(error.message.contains("Endpoint"), "{}", error.message);
    }
}
