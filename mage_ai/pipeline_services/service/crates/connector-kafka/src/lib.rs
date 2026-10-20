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
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header as KafkaHeader, Headers, Message, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer as _};
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

/// IBM Event Streams from its service credentials JSON (a string, as the environment holds
/// it, or an object): the brokers of `kafka_brokers_sasl`, SASL_SSL, and PLAIN sign-in as
/// `token` with the API key.
fn event_streams(client: &mut ClientConfig, credentials: &Value) -> Result<(), ConnectorError> {
    let parsed;
    let credentials = match credentials {
        Value::String(text) => {
            parsed = serde_json::from_str::<Value>(text).map_err(|e| {
                ConnectorError::fatal(format!(
                    "IBM Event Streams: event_streams_credentials is not the service \
                     credentials JSON: {e}"
                ))
            })?;
            &parsed
        }
        other => other,
    };
    let brokers: Vec<String> = match &credentials["kafka_brokers_sasl"] {
        Value::Array(list) => list
            .iter()
            .filter_map(|b| b.as_str().map(String::from))
            .collect(),
        Value::String(text) => text.split(',').map(String::from).collect(),
        _ => Vec::new(),
    };
    if brokers.is_empty() {
        return Err(ConnectorError::fatal(
            "IBM Event Streams: the credentials have no kafka_brokers_sasl.",
        ));
    }
    let password = text(credentials, "password")
        .or_else(|| text(credentials, "api_key"))
        .ok_or_else(|| {
            ConnectorError::fatal("IBM Event Streams: the credentials have no api_key.")
        })?;
    client
        .set("bootstrap.servers", brokers.join(","))
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "PLAIN")
        .set(
            "sasl.username",
            text(credentials, "user").unwrap_or_else(|| "token".into()),
        )
        .set("sasl.password", password);
    Ok(())
}

/// Assigns Mage's `offset.partitions` to the consumer. Each starts where the consumer group
/// committed, or where `offset` says when the group has nothing for it: Mage seeks at every
/// start, which in a service read everything again after each restart.
fn assign_from(
    consumer: &KafkaConsumer,
    table: &Mutex<Table>,
    offset: &Value,
) -> Result<Vec<(u32, AssignmentEpoch)>, ConnectorError> {
    let fatal = |message: &str| ConnectorError::fatal(format!("Kafka source offset: {message}"));
    let partitions: Vec<(String, i32)> = offset["partitions"]
        .as_array()
        .ok_or_else(|| fatal("it needs partitions, a list of {topic, partition}"))?
        .iter()
        .map(|p| {
            Ok((
                p["topic"]
                    .as_str()
                    .ok_or_else(|| fatal("a partition has no topic"))?
                    .to_string(),
                p["partition"]
                    .as_i64()
                    .ok_or_else(|| fatal("a partition has no number"))? as i32,
            ))
        })
        .collect::<Result<_, ConnectorError>>()?;
    let mut list = TopicPartitionList::new();
    for (topic, partition) in &partitions {
        list.add_partition(topic, *partition);
    }
    let committed = consumer
        .committed_offsets(list.clone(), Duration::from_secs(30))
        .map_err(connector_error)?;
    let kind = offset["type"].as_str().unwrap_or("beginning");
    let wanted = match kind {
        "beginning" => Offset::Beginning,
        "end" => Offset::End,
        "int" => Offset::Offset(
            offset["value"]
                .as_i64()
                .ok_or_else(|| fatal("type int needs value, the offset"))?,
        ),
        "timestamp" => Offset::Invalid,
        other => {
            return Err(fatal(&format!(
                "type {other} is not beginning, end, int or timestamp"
            )));
        }
    };
    let mut starts = TopicPartitionList::new();
    let mut by_time = TopicPartitionList::new();
    for element in committed.elements() {
        let offset = match element.offset() {
            Offset::Offset(n) if n >= 0 => Offset::Offset(n),
            _ if kind == "timestamp" => {
                let at = offset_value_ms(offset).ok_or_else(|| {
                    fatal("type timestamp needs value, milliseconds since the epoch")
                })?;
                let _ = by_time.add_partition_offset(
                    element.topic(),
                    element.partition(),
                    Offset::Offset(at),
                );
                continue;
            }
            _ => wanted,
        };
        starts
            .add_partition_offset(element.topic(), element.partition(), offset)
            .map_err(connector_error)?;
    }
    if by_time.count() > 0 {
        let found = consumer
            .offsets_for_times(by_time, Duration::from_secs(30))
            .map_err(connector_error)?;
        for element in found.elements() {
            let offset = match element.offset() {
                Offset::Offset(n) if n >= 0 => Offset::Offset(n),
                // No message at or after the time: from the end.
                _ => Offset::End,
            };
            starts
                .add_partition_offset(element.topic(), element.partition(), offset)
                .map_err(connector_error)?;
        }
    }
    consumer.assign(&starts).map_err(connector_error)?;
    let mut table = table.lock().unwrap_or_else(|p| p.into_inner());
    Ok(partitions
        .iter()
        .map(|(topic, partition)| {
            let id = table.id(topic, *partition);
            (id, table.assign(id))
        })
        .collect())
}

fn offset_value_ms(offset: &Value) -> Option<i64> {
    offset["value"].as_i64()
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

/// How long a client that knows no usable broker waits before it reads `bootstrap.servers`
/// again (librdkafka: 5 minutes). A broker in a controlled shutdown can answer metadata with
/// no brokers, and librdkafka then drops the brokers it knew: the client had none left, and
/// a stream stood still for 5 minutes after the broker was back (I-217).
const REBOOTSTRAP_AFTER: &str = "30000";

/// How often a client's broker watch checks that a broker answers.
const BROKER_CHECK_EVERY: Duration = Duration::from_secs(10);
/// How long a check waits for the broker's answer.
const BROKER_CHECK_WITHIN: Duration = Duration::from_secs(3);
/// Failed checks in a row after which the watch connects to `bootstrap.servers` again.
const BROKER_CHECKS_FAILED: u32 = 3;

/// The broker checks that failed in a row.
#[derive(Default)]
struct BrokerChecks {
    failed: u32,
}

impl BrokerChecks {
    /// Records a check; true when it is time to connect to `bootstrap.servers` again.
    fn checked(&mut self, answered: bool) -> bool {
        if answered {
            self.failed = 0;
            return false;
        }
        self.failed += 1;
        if self.failed < BROKER_CHECKS_FAILED {
            return false;
        }
        self.failed = 0;
        true
    }
}

/// Keeps a client able to reach the cluster. A one-broker cluster in a controlled shutdown
/// can answer metadata with no brokers, and librdkafka then drops the brokers it knew; its
/// own re-bootstrap needs a metadata request to time out, which a client with no broker
/// never sends. The producer held its messages until `message.timeout.ms` and the stream
/// stood still with them (I-217). Every BROKER_CHECK_EVERY this asks for the topic's
/// metadata, and after BROKER_CHECKS_FAILED failures in a row adds `bootstrap.servers`
/// again: librdkafka skips a broker it already knows by that address, so while the broker
/// is only down this changes nothing. The thread holds the client weakly and ends with it.
fn watch_brokers<T: Send + Sync + 'static>(
    side: &'static str,
    client: std::sync::Weak<T>,
    topic: String,
    servers: String,
    stop: Arc<AtomicBool>,
    answers: fn(&T, &str) -> bool,
    add: fn(&T, &std::ffi::CStr),
) -> Result<(), ConnectorError> {
    let Ok(servers) = std::ffi::CString::new(servers) else {
        return Ok(());
    };
    std::thread::Builder::new()
        .name(format!("kafka-{side}-brokers"))
        .spawn(move || {
            let mut checks = BrokerChecks::default();
            loop {
                let next = Instant::now() + BROKER_CHECK_EVERY;
                while Instant::now() < next {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(250));
                }
                let Some(client) = client.upgrade() else {
                    return;
                };
                if checks.checked(answers(&client, &topic)) {
                    write_log(
                        "warn",
                        side,
                        &format!(
                            "no broker answered for {} seconds; connecting to the bootstrap \
                             servers again",
                            (BROKER_CHECK_EVERY * BROKER_CHECKS_FAILED).as_secs()
                        ),
                    );
                    add(&client, &servers);
                }
            }
        })
        .map(|_| ())
        .map_err(|e| ConnectorError::fatal(format!("Kafka: {e}")))
}

/// Whether a broker answers a metadata request for the topic.
fn broker_answers<C: ClientContext>(client: &rdkafka::client::Client<C>, topic: &str) -> bool {
    client
        .fetch_metadata(Some(topic), BROKER_CHECK_WITHIN)
        .is_ok()
}

/// Adds the brokers of a `bootstrap.servers` list to the client.
fn add_brokers<C: ClientContext>(client: &rdkafka::client::Client<C>, servers: &std::ffi::CStr) {
    // SAFETY: the client handle is alive (borrowed), and librdkafka copies the list.
    unsafe {
        rdkafka::bindings::rd_kafka_brokers_add(client.native_ptr(), servers.as_ptr());
    }
}

/// The client properties shared by the source and the sink.
fn client_config(config: &Value) -> Result<ClientConfig, ConnectorError> {
    let mut client = ClientConfig::new();
    client.set(
        "metadata.recovery.rebootstrap.trigger.ms",
        REBOOTSTRAP_AFTER,
    );
    if let Some(connection) = text(config, "event_hubs_connection_string") {
        event_hubs(&mut client, &connection)?;
        return Ok(client);
    }
    if let Some(credentials) = config.get("event_streams_credentials") {
        event_streams(&mut client, credentials)?;
        apply_librdkafka(&mut client, config);
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
    apply_librdkafka(&mut client, config);
    Ok(client)
}

/// librdkafka properties set as they are (`librdkafka:` in the settings).
fn apply_librdkafka(client: &mut ClientConfig, config: &Value) {
    if let Some(extra) = config.get("librdkafka").and_then(Value::as_object) {
        for (key, value) in extra {
            let value = match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            client.set(key, value);
        }
    }
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
/// How long a commit made before giving partitions up waits for the coordinator's answer.
const COMMIT_WITHIN: Duration = Duration::from_secs(10);
/// How often the poll thread commits the positions again, as the Java client's auto commit
/// does. librdkafka reports the result of a manual asynchronous commit to no one, so a commit
/// lost while the coordinator was down stayed lost: a partition whose last batch it was kept
/// its old committed offset as long as the stream ran (I-195).
const RECOMMIT_EVERY: Duration = Duration::from_secs(5);
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
    /// again every [`RECOMMIT_EVERY`], and waited for (up to [`COMMIT_WITHIN`]) before a
    /// partition is given up and when the source closes.
    committed: BTreeMap<u32, i64>,
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

/// How the poll thread turns a message into the value blocks get.
struct Reading {
    decoder: Option<avro::AvroDecoder>,
    /// Mage's include_metadata: `{"data": <value>, "metadata": {key, partition, offset,
    /// time, topic}}`.
    include_metadata: bool,
}

impl Fetched {
    fn new(message: rdkafka::message::OwnedMessage, reading: &mut Reading) -> Fetched {
        let (mut value, error) = match (reading.decoder.as_mut(), message.payload()) {
            (Some(decoder), Some(payload)) => match decoder.decode(payload) {
                Ok(json) => (Some(json), None),
                Err(error) => (None, Some(format!("Avro: {error}"))),
            },
            _ => (None, None),
        };
        if reading.include_metadata && error.is_none() {
            let data = value
                .take()
                .unwrap_or_else(|| message.payload().unwrap_or_default().to_vec());
            value = Some(with_metadata(&message, &data));
        }
        Fetched {
            message,
            value,
            error,
        }
    }
}

/// The message as Mage's include_metadata hands it over: its value (JSON as it is, text as
/// a string) under `data`, and where it comes from under `metadata`. A key that is not
/// UTF-8 is its base64 text.
fn with_metadata(message: &rdkafka::message::OwnedMessage, data: &[u8]) -> Vec<u8> {
    use base64::Engine as _;

    let data: String = if serde_json::from_slice::<&serde_json::value::RawValue>(data).is_ok() {
        String::from_utf8_lossy(data).into_owned()
    } else {
        Value::String(String::from_utf8_lossy(data).into_owned()).to_string()
    };
    let key = match message.key() {
        None | Some([]) => Value::Null,
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => Value::String(text.to_string()),
            Err(_) => Value::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
        },
    };
    format!(
        r#"{{"data":{data},"metadata":{{"key":{key},"partition":{},"offset":{},"time":{},"topic":{}}}}}"#,
        message.partition(),
        message.offset(),
        message.timestamp().to_millis().unwrap_or(0),
        Value::String(message.topic().to_string()),
    )
    .into_bytes()
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
    /// Runs on the poll thread before partitions go. The partitions keep their epochs while
    /// the stream finishes their batches in flight and commits them (I-34), so the next owner
    /// starts after them; after REVOKE_WITHIN they go anyway, and what was not done is read
    /// again by the next owner.
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(list) = rebalance else {
            return;
        };
        // A lost assignment (the group moved on without this member, as after a broker
        // restart) is someone else's already: nothing to finish or commit for it.
        let lost = consumer.assignment_lost();
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
                within: if lost { Duration::ZERO } else { REVOKE_WITHIN },
            };
            // The stream has stopped reading: nothing to wait for.
            if !self.send(Polled::Event(event)) {
                waiting.clear();
            }
        }
        if lost || !self.may_wait() {
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
        if !lost && list.count() > 0 {
            let _ = commit_within(consumer, &list, COMMIT_WITHIN);
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

/// Writes a line of librdkafka's log as the service log writes its lines. `level` is
/// syslog's, as librdkafka gives it.
fn write_librdkafka_log(side: &str, level: i32, facility: &str, message: &str) {
    let name = match level {
        0..=3 => "error",
        4 => "warn",
        5 | 6 => "info",
        _ => "debug",
    };
    write_log(name, side, &format!("librdkafka {facility}: {message}"));
}

/// Writes a line of the connector's own, as the service log writes its lines.
fn write_log(level: &str, side: &str, message: &str) {
    let time = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    eprintln!("{time} {level:<5} Kafka {side}, {message}");
}

/// The sink's producer context: rdkafka serves the producer's log through it, and without
/// it librdkafka's warnings and errors went nowhere.
pub struct SinkContext;

impl ClientContext for SinkContext {
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, facility: &str, message: &str) {
        write_librdkafka_log("sink", level as i32, facility, message);
    }
}

/// librdkafka's own log, for the service log. rdkafka sends it to the consumer's queue,
/// whose poll ignores it, so librdkafka's warnings and errors went nowhere: the consumer's
/// log goes to a queue of its own, which the poll thread drains.
struct LogQueue {
    client: *mut rdkafka::bindings::rd_kafka_t,
    queue: *mut rdkafka::bindings::rd_kafka_queue_t,
}

// SAFETY: a librdkafka queue is thread safe; only the poll thread uses this handle.
unsafe impl Send for LogQueue {}

impl LogQueue {
    fn new(consumer: &KafkaConsumer) -> LogQueue {
        // SAFETY: the client handle lives as long as `consumer`, which outlives the queue.
        unsafe {
            let client = consumer.client().native_ptr();
            let queue = rdkafka::bindings::rd_kafka_queue_new(client);
            rdkafka::bindings::rd_kafka_set_log_queue(client, queue);
            LogQueue { client, queue }
        }
    }

    /// Writes what librdkafka logged since the last call.
    fn drain(&self) {
        use std::ffi::CStr;
        use std::os::raw::{c_char, c_int};

        // SAFETY: each event comes from this queue and is destroyed once read; the strings
        // it hands out live as long as the event.
        unsafe {
            loop {
                let event = rdkafka::bindings::rd_kafka_queue_poll(self.queue, 0);
                if event.is_null() {
                    return;
                }
                let (mut facility, mut message): (*const c_char, *const c_char) =
                    (std::ptr::null(), std::ptr::null());
                let mut level: c_int = 0;
                if rdkafka::bindings::rd_kafka_event_log(
                    event,
                    &mut facility,
                    &mut message,
                    &mut level,
                ) == 0
                {
                    write_librdkafka_log(
                        "source",
                        level,
                        &CStr::from_ptr(facility).to_string_lossy(),
                        &CStr::from_ptr(message).to_string_lossy(),
                    );
                }
                rdkafka::bindings::rd_kafka_event_destroy(event);
            }
        }
    }
}

impl Drop for LogQueue {
    fn drop(&mut self) {
        // SAFETY: the poll thread drops this before its consumer; the log goes back to the
        // main queue before the queue made by rd_kafka_queue_new is destroyed, once.
        unsafe {
            rdkafka::bindings::rd_kafka_set_log_queue(self.client, std::ptr::null_mut());
            rdkafka::bindings::rd_kafka_queue_destroy(self.queue);
        }
    }
}

/// Commits the positions and waits up to `within` for the coordinator's answer. librdkafka's
/// synchronous commit waits without end: in a revoke, a coordinator that did not answer
/// held the poll thread, and the stream with it (I-196).
fn commit_within(
    consumer: &KafkaConsumer,
    list: &TopicPartitionList,
    within: Duration,
) -> Result<(), String> {
    use rdkafka::bindings as rd;
    use rdkafka::bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR as NO_ERROR;

    // SAFETY: the client handle lives as long as `consumer` and the list as long as the
    // call (librdkafka copies it). The queue is ours: the commit's reply holds its own
    // reference, so destroying the queue before an answer comes is safe.
    unsafe {
        let rk = consumer.client().native_ptr();
        let queue = rd::rd_kafka_queue_new(rk);
        let result =
            match rd::rd_kafka_commit_queue(rk, list.ptr(), queue, None, std::ptr::null_mut()) {
                NO_ERROR => {
                    let millis = within.as_millis().min(i32::MAX as u128) as i32;
                    let event = rd::rd_kafka_queue_poll(queue, millis);
                    if event.is_null() {
                        Err(format!("no answer within {} seconds", within.as_secs()))
                    } else {
                        let error = rd::rd_kafka_event_error(event);
                        rd::rd_kafka_event_destroy(event);
                        if error == NO_ERROR {
                            Ok(())
                        } else {
                            Err(format!("{error:?}"))
                        }
                    }
                }
                error => Err(format!("{error:?}")),
            };
        rd::rd_kafka_queue_destroy(queue);
        result
    }
}

/// Polls the consumer until stopped. Rebalance callbacks run here, so a revoke can wait for
/// the stream without holding up the stream's own task.
fn poll_loop(
    consumer: Arc<KafkaConsumer>,
    table: Arc<Mutex<Table>>,
    stop: Arc<AtomicBool>,
    mut reading: Reading,
) {
    let polled = consumer.context().polled.clone();
    let log = LogQueue::new(&consumer);
    let mut lag_at = Instant::now();
    let mut recommit_at = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        log.drain();
        // One message woke the stream's task and parked it again each time: the two
        // threads mostly waited on each other (I-84). The messages librdkafka already
        // holds go together.
        let mut items = Vec::with_capacity(2);
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                // Decoded here, on the poll thread: a new schema's first read from the
                // registry waits here, not in the stream's loop.
                let mut chunk = Vec::with_capacity(CHUNK);
                chunk.push(Fetched::new(message.detach(), &mut reading));
                while chunk.len() < CHUNK {
                    match consumer.poll(Duration::ZERO) {
                        Some(Ok(message)) => {
                            chunk.push(Fetched::new(message.detach(), &mut reading))
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
        // On this thread, which also runs the revokes: a partition given up is out of the
        // table before its positions could be committed again.
        if recommit_at.elapsed() >= RECOMMIT_EVERY {
            recommit_at = Instant::now();
            let list = {
                let table = table.lock().unwrap_or_else(|p| p.into_inner());
                table.committed_list(table.epochs.keys().copied())
            };
            if list.count() > 0 {
                let _ = consumer.commit(&list, CommitMode::Async);
            }
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
    /// Assignments made at start (Mage's offset assigns partitions itself).
    initial: std::collections::VecDeque<SourceEvent>,
    seq: BTreeMap<u32, u64>,
    paused: BTreeSet<u32>,
    /// Messages polled but not yet in a batch, per partition: a paused partition's wait
    /// here without holding back the other partitions' (I-197).
    carry: BTreeMap<u32, std::collections::VecDeque<Fetched>>,
    /// Paused partitions only held back, since when; the others are paused in librdkafka.
    held: BTreeMap<u32, Instant>,
    stopped: BTreeSet<u32>,
    limits: BatchLimits,
    read: ReadPositions,
}

/// The last offset read from each partition under its assignment. librdkafka can hand a
/// message over twice after a broker's controlled shutdown (it fetched the partition again
/// from an earlier position), and the stream wrote it twice. Within one assignment the
/// source never seeks back, so an offset at or below the last one read is such a repeat;
/// after a new assignment, reading uncommitted messages again is at-least-once delivery.
#[derive(Debug, Default)]
struct ReadPositions(BTreeMap<u32, (AssignmentEpoch, i64)>);

impl ReadPositions {
    /// Records the offset; false when the partition's assignment read it already.
    fn first_read(&mut self, id: u32, epoch: AssignmentEpoch, offset: i64) -> bool {
        match self.0.get(&id) {
            Some((read_in, last)) if *read_in == epoch && offset <= *last => false,
            _ => {
                self.0.insert(id, (epoch, offset));
                true
            }
        }
    }
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
        let servers = client
            .get("bootstrap.servers")
            .unwrap_or_default()
            .to_string();
        let queue_kb = (byte_budget / 1024).clamp(1024, 65536);
        client
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "false")
            .set("partition.assignment.strategy", "cooperative-sticky")
            // librdkafka writes debug lines only for the contexts `librdkafka: {debug: ...}`
            // names; without a level set, rdkafka took it from a logger the engine does not
            // have, and librdkafka dropped even its errors.
            .set_log_level(rdkafka::config::RDKafkaLogLevel::Debug)
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
        let reading = Reading {
            decoder: message_decoder(config)?,
            include_metadata: config
                .get("include_metadata")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        let mut initial = std::collections::VecDeque::new();
        if let Some(offset) = config.get("offset").filter(|o| !o.is_null()) {
            // Mage's offset: the listed partitions, assigned without the group's rebalancing.
            for (id, epoch) in assign_from(&consumer, &table, offset)? {
                initial.push_back(SourceEvent::Assigned {
                    partition: PartitionId(id),
                    epoch,
                });
            }
        } else {
            let names: Vec<&str> = topics.iter().map(String::as_str).collect();
            consumer.subscribe(&names).map_err(connector_error)?;
        }
        let consumer = Arc::new(consumer);
        watch_brokers(
            "source",
            Arc::downgrade(&consumer),
            topics[0].clone(),
            servers,
            stop.clone(),
            |consumer: &KafkaConsumer, topic| broker_answers(consumer.client(), topic),
            |consumer: &KafkaConsumer, servers| add_brokers(consumer.client(), servers),
        )?;
        let thread = std::thread::Builder::new()
            .name("kafka-source".into())
            .spawn({
                let (consumer, table, stop) = (consumer.clone(), table.clone(), stop.clone());
                move || poll_loop(consumer, table, stop, reading)
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
            initial,
            seq: BTreeMap::new(),
            paused: BTreeSet::new(),
            carry: BTreeMap::new(),
            read: ReadPositions::default(),
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
                self.carry.remove(&partition.0);
                self.paused.remove(&partition.0);
                self.held.remove(&partition.0);
                self.stopped.remove(&partition.0);
                self.revoking.insert(partition.0);
            }
            SourceEvent::Assigned { partition, .. } => {
                // Messages polled before the assignment belong to an earlier one.
                self.carry.remove(&partition.0);
                self.revoking.remove(&partition.0);
                self.seq.insert(partition.0, 0);
                // librdkafka keeps a partition's pause across a revoke: one paused when the
                // group took it away came back paused, and was never read again (I-198). A
                // new assignment fetches; a pause holds it again.
                if let Some(list) = self.tpl(partition.0) {
                    let _ = self.consumer.resume(&list);
                }
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
        let offset = message.offset();
        if !self.read.first_read(id, epoch, offset) {
            write_log(
                "warn",
                "source",
                &format!(
                    "skipped offset {offset} of {} [{}]: librdkafka handed it over again",
                    message.topic(),
                    message.partition()
                ),
            );
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
    /// Adds the polled messages to their partitions' batches, each up to full: a full batch
    /// of a paused partition waits, and its other messages with it, while the other
    /// partitions go on. One queue for all held a paused partition's messages in front of
    /// the others', and its batch grew past full one message per wait (I-197).
    fn take_carry(&mut self) -> Result<Option<SourceEvent>, ConnectorError> {
        let ids: Vec<u32> = self.carry.keys().copied().collect();
        for id in ids {
            while !self.pending.get(&id).is_some_and(|p| self.full(p)) {
                let Some(message) = self.carry.get_mut(&id).and_then(|q| q.pop_front()) else {
                    break;
                };
                self.add(message);
            }
            if self.carry.get(&id).is_some_and(|q| q.is_empty()) {
                self.carry.remove(&id);
            }
        }
        self.ready(false)
    }

    /// Puts a polled chunk's messages in their partitions' queues.
    fn carry_chunk(&mut self, chunk: Vec<Fetched>) {
        let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        for fetched in chunk {
            let id = table.id(fetched.message.topic(), fetched.message.partition());
            self.carry.entry(id).or_default().push_back(fetched);
        }
    }
}

impl Source for KafkaSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if let Some(event) = self.initial.pop_front() {
                return Ok(event);
            }
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
                    Ok(Some(Polled::Messages(chunk))) => self.carry_chunk(chunk),
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
            // round trip to the coordinator on every batch (I-85). librdkafka reports no
            // result, so the poll thread commits the positions again every RECOMMIT_EVERY,
            // and synchronously before a partition is given up and at close. Committed here
            // means handed to librdkafka: after a crash, a batch can be read again, as
            // at-least-once allows.
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
            durable_positions: true,
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
                    let _ = commit_within(&consumer, &list, COMMIT_WITHIN);
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
    producer: Arc<FutureProducer<SinkContext>>,
    topic: String,
    timeout: Duration,
    /// Ends the broker watch.
    stop: Arc<AtomicBool>,
}

impl KafkaSink {
    pub fn new(config: &Value) -> Result<KafkaSink, ConnectorError> {
        let topic = text(config, "topic")
            .ok_or_else(|| ConnectorError::fatal("Kafka sink settings need topic."))?;
        let mut client = client_config(config)?;
        let servers = client
            .get("bootstrap.servers")
            .unwrap_or_default()
            .to_string();
        client
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("linger.ms", "5")
            .set("compression.type", "lz4")
            .set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);
        let producer: Arc<FutureProducer<SinkContext>> = Arc::new(
            client
                .create_with_context(SinkContext)
                .map_err(connector_error)?,
        );
        let stop = Arc::new(AtomicBool::new(false));
        watch_brokers(
            "sink",
            Arc::downgrade(&producer),
            topic.clone(),
            servers,
            stop.clone(),
            |producer: &FutureProducer<SinkContext>, topic| {
                broker_answers(producer.client(), topic)
            },
            |producer: &FutureProducer<SinkContext>, servers| {
                add_brokers(producer.client(), servers)
            },
        )?;
        Ok(KafkaSink {
            producer,
            topic,
            timeout: Duration::from_secs(30),
            stop,
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

impl Drop for KafkaSink {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
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
    fn a_message_read_twice_in_one_assignment_is_skipped() {
        let mut read = ReadPositions::default();
        let first = AssignmentEpoch(1);
        assert!(read.first_read(0, first, 10));
        assert!(read.first_read(0, first, 11));
        assert!(!read.first_read(0, first, 11), "the same offset again");
        assert!(!read.first_read(0, first, 9), "an earlier offset");
        // Gaps (compaction, transaction markers) are fine.
        assert!(read.first_read(0, first, 20));
        // Each partition has its own position.
        assert!(read.first_read(1, first, 5));
        // A new assignment reads from the committed offset again: at-least-once.
        assert!(read.first_read(0, AssignmentEpoch(2), 12));
        assert!(!read.first_read(0, AssignmentEpoch(2), 12));
    }

    #[test]
    fn include_metadata_wraps_values_as_mage_does() {
        let mut message = rdkafka::message::OwnedMessage::new(
            Some(br#"{"n": 1}"#.to_vec()),
            Some(b"k-1".to_vec()),
            "orders".into(),
            rdkafka::Timestamp::CreateTime(1_792_000_000_123),
            3,
            42,
            None,
        );
        let wrapped: Value =
            serde_json::from_slice(&with_metadata(&message, br#"{"n": 1}"#)).unwrap();
        assert_eq!(
            wrapped,
            serde_json::json!({"data": {"n": 1}, "metadata": {"key": "k-1", "partition": 3,
                "offset": 42, "time": 1_792_000_000_123_i64, "topic": "orders"}})
        );
        message = rdkafka::message::OwnedMessage::new(
            Some(b"plain text".to_vec()),
            None,
            "orders".into(),
            rdkafka::Timestamp::NotAvailable,
            0,
            1,
            None,
        );
        let wrapped: Value =
            serde_json::from_slice(&with_metadata(&message, b"plain text")).unwrap();
        assert_eq!(wrapped["data"], "plain text");
        assert_eq!(wrapped["metadata"]["key"], Value::Null);
    }

    #[test]
    fn the_bootstrap_servers_come_back_after_three_failed_checks_in_a_row() {
        let mut checks = BrokerChecks::default();
        assert!(!checks.checked(false));
        assert!(!checks.checked(false));
        assert!(!checks.checked(true), "an answer starts the count again");
        assert!(!checks.checked(false));
        assert!(!checks.checked(false));
        assert!(checks.checked(false));
        assert!(!checks.checked(false), "and again three checks later");
        assert!(!checks.checked(false));
        assert!(checks.checked(false));
    }

    #[test]
    fn a_client_without_brokers_bootstraps_again_within_30_seconds() {
        let client = client_config(&serde_json::json!({"bootstrap_server": "kafka:9092"})).unwrap();
        assert_eq!(
            client.get("metadata.recovery.rebootstrap.trigger.ms"),
            Some(REBOOTSTRAP_AFTER)
        );
        let tuned = client_config(&serde_json::json!({
            "bootstrap_server": "kafka:9092",
            "librdkafka": {"metadata.recovery.rebootstrap.trigger.ms": 60000},
        }))
        .unwrap();
        assert_eq!(
            tuned.get("metadata.recovery.rebootstrap.trigger.ms"),
            Some("60000")
        );
    }

    #[test]
    fn event_streams_credentials_become_brokers_and_sign_in() {
        let credentials = r#"{"api_key": "k3y", "kafka_brokers_sasl":
            ["broker-0.eventstreams.cloud.ibm.com:9093", "broker-1.eventstreams.cloud.ibm.com:9093"],
            "user": "token"}"#;
        let client = client_config(&serde_json::json!({
            "event_streams_credentials": credentials,
        }))
        .unwrap();
        assert_eq!(
            client.get("bootstrap.servers"),
            Some(
                "broker-0.eventstreams.cloud.ibm.com:9093,broker-1.eventstreams.cloud.ibm.com:9093"
            )
        );
        assert_eq!(client.get("sasl.username"), Some("token"));
        assert_eq!(client.get("sasl.password"), Some("k3y"));
        assert_eq!(client.get("sasl.mechanism"), Some("PLAIN"));
        assert!(client_config(&serde_json::json!({"event_streams_credentials": "{}"})).is_err());
    }

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
