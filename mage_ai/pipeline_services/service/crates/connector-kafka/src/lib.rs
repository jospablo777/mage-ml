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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, ConnectorError,
    Envelope, EnvelopeBuilder, Header, PartitionId, Position, RecordBatch, Sink, SinkCapabilities,
    Source, SourceCapabilities, SourceEvent, read_envelope,
};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header as KafkaHeader, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
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

/// (topic, partition) to the stream's partition ids, and each one's current epoch.
#[derive(Debug, Default)]
struct Table {
    ids: HashMap<(String, i32), u32>,
    names: Vec<(String, i32)>,
    epochs: BTreeMap<u32, AssignmentEpoch>,
    next_epoch: u64,
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

struct RebalanceContext {
    table: Arc<Mutex<Table>>,
    events: mpsc::UnboundedSender<SourceEvent>,
}

impl ClientContext for RebalanceContext {}

impl ConsumerContext for RebalanceContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(list) = rebalance {
            let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
            for element in list.elements() {
                let id = table.id(element.topic(), element.partition());
                if let Some(epoch) = table.epochs.remove(&id) {
                    let _ = self.events.send(SourceEvent::Revoking {
                        partition: PartitionId(id),
                        epoch,
                        within: Duration::from_secs(10),
                    });
                }
            }
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(list) = rebalance {
            let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
            for element in list.elements() {
                let id = table.id(element.topic(), element.partition());
                let epoch = table.assign(id);
                let _ = self.events.send(SourceEvent::Assigned {
                    partition: PartitionId(id),
                    epoch,
                });
            }
        }
    }
}

struct Pending {
    builder: EnvelopeBuilder,
    epoch: AssignmentEpoch,
    first: i64,
    last: i64,
    started: Instant,
}

pub struct KafkaSource {
    consumer: Arc<StreamConsumer<RebalanceContext>>,
    table: Arc<Mutex<Table>>,
    events: mpsc::UnboundedReceiver<SourceEvent>,
    pending: BTreeMap<u32, Pending>,
    seq: BTreeMap<u32, u64>,
    paused: BTreeSet<u32>,
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
            .set(
                "auto.offset.reset",
                text(config, "auto_offset_reset").unwrap_or_else(|| "latest".into()),
            );
        let table = Arc::new(Mutex::new(Table::default()));
        let (tx, events) = mpsc::unbounded_channel();
        let consumer: StreamConsumer<RebalanceContext> = client
            .create_with_context(RebalanceContext {
                table: table.clone(),
                events: tx,
            })
            .map_err(connector_error)?;
        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        consumer.subscribe(&names).map_err(connector_error)?;
        Ok(KafkaSource {
            consumer: Arc::new(consumer),
            table,
            events,
            pending: BTreeMap::new(),
            seq: BTreeMap::new(),
            paused: BTreeSet::new(),
            limits,
        })
    }

    fn handle(&mut self, event: SourceEvent) -> SourceEvent {
        match &event {
            SourceEvent::Revoking { partition, .. } => {
                // Messages read in the revoked epoch are not delivered.
                self.pending.remove(&partition.0);
                self.paused.remove(&partition.0);
            }
            SourceEvent::Assigned { partition, .. } => {
                self.seq.insert(partition.0, 0);
                if self.paused.contains(&partition.0) {
                    self.pause(*partition);
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

    /// A batch that is full, or (when `due`) the oldest one past its wait.
    fn ready(&mut self, due: bool) -> Result<Option<SourceEvent>, ConnectorError> {
        let full = self.pending.iter().find(|(_, p)| {
            p.builder.len() >= self.limits.max_records || p.builder.bytes() >= self.limits.max_bytes
        });
        if let Some((id, _)) = full {
            let id = *id;
            return self.take(id);
        }
        let now = Instant::now();
        let oldest = self
            .pending
            .iter()
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
            .values()
            .map(|p| (p.started + self.limits.max_wait).saturating_duration_since(now))
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
}

impl Source for KafkaSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            loop {
                if let Ok(event) = self.events.try_recv() {
                    return Ok(self.handle(event));
                }
                if let Some(batch) = self.ready(false)? {
                    return Ok(batch);
                }
                let wait = self.wait();
                let consumer = self.consumer.clone();
                match tokio::time::timeout(wait, consumer.recv()).await {
                    Ok(Ok(message)) => {
                        let (topic, partition) = (message.topic().to_string(), message.partition());
                        let (id, epoch) = {
                            let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
                            let id = table.id(&topic, partition);
                            (id, table.epochs.get(&id).copied())
                        };
                        // A message of a partition no longer assigned is dropped; the next
                        // owner reads it.
                        let Some(epoch) = epoch else { continue };
                        let headers = message
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
                        let envelope = Envelope {
                            key: message.key().map(<[u8]>::to_vec),
                            value: message.payload().map(<[u8]>::to_vec).unwrap_or_default(),
                            headers,
                            partition: id,
                            position: Some(message.offset()),
                            timestamp: message.timestamp().to_millis(),
                        };
                        let offset = message.offset();
                        let pending = self.pending.entry(id).or_insert_with(|| Pending {
                            builder: EnvelopeBuilder::new(),
                            epoch,
                            first: offset,
                            last: offset,
                            started: Instant::now(),
                        });
                        pending.builder.push(&envelope);
                        pending.last = offset;
                    }
                    Ok(Err(error)) => return Err(connector_error(error)),
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
                let table = self.table.lock().unwrap_or_else(|p| p.into_inner());
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
                }
            }
            let consumer = self.consumer.clone();
            match tokio::task::spawn_blocking(move || consumer.commit(&list, CommitMode::Sync))
                .await
            {
                Ok(Ok(())) => CommitOutcome::Committed,
                Ok(Err(error)) => match error.rdkafka_error_code() {
                    Some(RDKafkaErrorCode::IllegalGeneration)
                    | Some(RDKafkaErrorCode::RebalanceInProgress)
                    | Some(RDKafkaErrorCode::UnknownMemberId) => CommitOutcome::Stale,
                    _ => CommitOutcome::Failed(connector_error(error)),
                },
                Err(error) => CommitOutcome::Failed(ConnectorError::retryable(error.to_string())),
            }
        })
    }

    fn pause(&mut self, partition: PartitionId) {
        self.paused.insert(partition.0);
        if let Some(list) = self.tpl(partition.0) {
            let _ = self.consumer.pause(&list);
        }
    }

    fn resume(&mut self, partition: PartitionId) {
        self.paused.remove(&partition.0);
        if let Some(list) = self.tpl(partition.0) {
            let _ = self.consumer.resume(&list);
        }
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
            self.consumer.unsubscribe();
        })
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

impl Sink for KafkaSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let messages =
                read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
            let mut deliveries = Vec::with_capacity(messages.len());
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
                deliveries.push(self.producer.send(record, self.timeout));
            }
            for delivery in deliveries {
                if let Err((error, _)) = delivery.await {
                    return Err(connector_error(error));
                }
            }
            Ok(())
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
