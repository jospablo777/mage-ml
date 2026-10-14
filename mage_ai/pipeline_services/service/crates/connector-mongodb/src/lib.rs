//! MongoDB change stream source and collection sink for streams, with MageML's settings.
//!
//! Source (`connection_str`, `database`, `collection`, `batch_size`, `pipeline`,
//! `full_document`, `start_after`, `operation_time`): a change stream on the collection, or on
//! the database without one. Each change reaches the blocks as JSON: ObjectIds as their hex
//! text, dates as ISO 8601 text, decimals as exact text, binary as base64, and the cluster
//! time as `{"t", "i"}`. Once every sink took a batch, the resume token of its last change
//! is saved in the database's `checkpoint_collection` (`_mage_stream_checkpoints`, which the
//! stream does not watch), so a restarted service resumes after it, on any machine.
//!
//! Sink (`connection_string`, `database_name`, `collection_name`, `unique_constraints`): a
//! batch is inserted at once; with `unique_constraints`, a message that has those fields
//! updates or inserts the document they select, the last message of a key in a batch
//! winning. A message that is not an object is stored as `{"data": ...}`, as in MageML.
//! Integers past 64 bits become Decimal128, which holds every digit.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use base64::Engine as _;
use futures_util::StreamExt;
use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, ConnectorError,
    Envelope, EnvelopeBuilder, PartitionId, Position, RecordBatch, Sink, SinkCapabilities, Source,
    SourceCapabilities, SourceEvent, read_envelope,
};
use mongodb::bson::{self, Bson, Document, doc};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::FullDocumentType;
use mongodb::{Client, Collection};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

const PARTITION: PartitionId = PartitionId(0);
const CHECKPOINTS: &str = "_mage_stream_checkpoints";

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

fn mongo_error(label: &str, error: mongodb::error::Error) -> ConnectorError {
    ConnectorError::retryable(format!("MongoDB {label}: {error}"))
}

/// A BSON value as the JSON blocks get (see the module's documentation).
pub fn bson_to_json(value: &Bson) -> Value {
    let base64 =
        |bytes: &[u8]| Value::String(base64::engine::general_purpose::STANDARD.encode(bytes));
    match value {
        Bson::Double(x) => serde_json::Number::from_f64(*x)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(x.to_string())),
        Bson::String(s) => Value::String(s.clone()),
        Bson::Array(items) => Value::Array(items.iter().map(bson_to_json).collect()),
        Bson::Document(document) => document_to_json(document),
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::Int32(n) => Value::from(*n),
        Bson::Int64(n) => Value::from(*n),
        Bson::ObjectId(id) => Value::String(id.to_hex()),
        Bson::DateTime(at) => Value::String(
            at.try_to_rfc3339_string()
                .unwrap_or_else(|_| at.timestamp_millis().to_string()),
        ),
        Bson::Timestamp(ts) => serde_json::json!({"t": ts.time, "i": ts.increment}),
        Bson::Decimal128(d) => Value::String(d.to_string()),
        Bson::Binary(binary) => base64(&binary.bytes),
        Bson::RegularExpression(regex) => {
            Value::String(format!("/{}/{}", regex.pattern, regex.options))
        }
        Bson::JavaScriptCode(code) => Value::String(code.clone()),
        Bson::JavaScriptCodeWithScope(code) => Value::String(code.code.clone()),
        Bson::Symbol(s) => Value::String(s.clone()),
        Bson::MaxKey => Value::String("MaxKey".into()),
        Bson::MinKey => Value::String("MinKey".into()),
        Bson::DbPointer(_) => Value::Null,
    }
}

fn document_to_json(document: &Document) -> Value {
    let mut object = Map::new();
    for (key, item) in document {
        object.insert(key.clone(), bson_to_json(item));
    }
    Value::Object(object)
}

/// A message's JSON as BSON: integers past 64 bits as Decimal128, with every digit. The
/// numbers are read from the JSON text: serde_json rounds integers past 64 bits to floats.
pub fn json_to_bson(json: &str) -> Result<Bson, String> {
    let raw: &RawValue = serde_json::from_str(json).map_err(|e| e.to_string())?;
    raw_to_bson(raw)
}

fn raw_to_bson(raw: &RawValue) -> Result<Bson, String> {
    let text = raw.get().trim();
    match text.as_bytes().first() {
        Some(b'{') => {
            let object: ordered::Fields = serde_json::from_str(text).map_err(|e| e.to_string())?;
            let mut document = Document::new();
            for (key, item) in object.0 {
                document.insert(key, raw_to_bson(&item)?);
            }
            Ok(Bson::Document(document))
        }
        Some(b'[') => {
            let items: Vec<Box<RawValue>> =
                serde_json::from_str(text).map_err(|e| e.to_string())?;
            Ok(Bson::Array(
                items
                    .iter()
                    .map(|i| raw_to_bson(i))
                    .collect::<Result<_, _>>()?,
            ))
        }
        Some(b'"') => Ok(Bson::String(
            serde_json::from_str(text).map_err(|e| e.to_string())?,
        )),
        Some(b't') => Ok(Bson::Boolean(true)),
        Some(b'f') => Ok(Bson::Boolean(false)),
        Some(b'n') => Ok(Bson::Null),
        Some(_) if text.contains(['.', 'e', 'E']) => text
            .parse::<f64>()
            .map(Bson::Double)
            .map_err(|e| e.to_string()),
        Some(_) => Ok(match text.parse::<i64>() {
            Ok(n) => match i32::try_from(n) {
                Ok(small) => Bson::Int32(small),
                Err(_) => Bson::Int64(n),
            },
            Err(_) => text
                .parse::<bson::Decimal128>()
                .map(Bson::Decimal128)
                .map_err(|e| format!("{text}: {e}"))?,
        }),
        None => Err("an empty value".into()),
    }
}

/// An object's fields in their order, each value's JSON text kept.
mod ordered {
    use serde::de::{Deserializer, MapAccess, Visitor};
    use serde_json::value::RawValue;

    pub struct Fields(pub Vec<(String, Box<RawValue>)>);

    impl<'de> serde::Deserialize<'de> for Fields {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct Visit;
            impl<'de> Visitor<'de> for Visit {
                type Value = Fields;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Fields, A::Error> {
                    let mut fields = Vec::new();
                    while let Some((key, value)) = map.next_entry::<String, Box<RawValue>>()? {
                        fields.push((key, value));
                    }
                    Ok(Fields(fields))
                }
            }
            deserializer.deserialize_map(Visit)
        }
    }
}

struct Pending {
    builder: EnvelopeBuilder,
    first: u64,
    last: u64,
    started: Instant,
}

pub struct MongoSource {
    url: String,
    database: String,
    collection: Option<String>,
    checkpoints: String,
    /// The stream's name: the checkpoint document's id.
    name: String,
    config: Value,
    limits: BatchLimits,
    client: Option<Client>,
    stream: Option<ChangeStream<Document>>,
    epoch: u64,
    seq: u64,
    delivered: u64,
    /// The resume token of each delivered change not yet committed, by position.
    tokens: BTreeMap<u64, Bson>,
    pending: Option<Pending>,
    paused: bool,
}

impl MongoSource {
    /// `name`: the stream's name, which keys its saved resume token.
    pub fn new(
        config: &Value,
        limits: BatchLimits,
        name: &str,
    ) -> Result<MongoSource, ConnectorError> {
        let url = text(config, "connection_str")
            .ok_or_else(|| ConnectorError::fatal("MongoDB source settings need connection_str."))?;
        let database = text(config, "database")
            .ok_or_else(|| ConnectorError::fatal("MongoDB source settings need database."))?;
        Ok(MongoSource {
            url,
            database,
            collection: text(config, "collection"),
            checkpoints: text(config, "checkpoint_collection")
                .unwrap_or_else(|| CHECKPOINTS.into()),
            name: name.to_string(),
            config: config.clone(),
            limits,
            client: None,
            stream: None,
            epoch: 0,
            seq: 0,
            delivered: 0,
            tokens: BTreeMap::new(),
            pending: None,
            paused: false,
        })
    }

    fn checkpoint_collection(&self, client: &Client) -> Collection<Document> {
        client
            .database(&self.database)
            .collection(&self.checkpoints)
    }

    fn namespace(&self) -> String {
        match &self.collection {
            Some(collection) => format!("{}.{collection}", self.database),
            None => self.database.clone(),
        }
    }

    async fn connect(&mut self) -> Result<SourceEvent, ConnectorError> {
        let client = Client::with_uri_str(&self.url)
            .await
            .map_err(|e| mongo_error("connecting", e))?;
        // Resume after the last change every sink took, when one was saved for this
        // database and collection.
        let saved = self
            .checkpoint_collection(&client)
            .find_one(doc! {"_id": &self.name})
            .await
            .map_err(|e| mongo_error("reading the checkpoint", e))?
            .filter(|d| d.get_str("namespace").ok() == Some(self.namespace().as_str()))
            .and_then(|d| d.get("resume_token").cloned());
        let mut pipeline = vec![doc! {"$match": {"ns.coll": {"$ne": &self.checkpoints}}}];
        if let Some(Value::Array(stages)) = self.config.get("pipeline") {
            for stage in stages {
                if let Ok(Bson::Document(document)) = json_to_bson(&stage.to_string()) {
                    pipeline.push(document);
                }
            }
        }
        let database = client.database(&self.database);
        let mut options = mongodb::options::ChangeStreamOptions::default();
        if let Some(size) = self.config.get("batch_size").and_then(Value::as_u64) {
            options.batch_size = Some(size as u32);
        }
        options.full_document = match text(&self.config, "full_document").as_deref() {
            Some("updateLookup") => Some(FullDocumentType::UpdateLookup),
            Some("whenAvailable") => Some(FullDocumentType::WhenAvailable),
            Some("required") => Some(FullDocumentType::Required),
            _ => None,
        };
        options.max_await_time = Some(self.limits.max_wait.max(Duration::from_millis(50)));
        if let Some(token) = &saved {
            options.resume_after = Some(
                bson::deserialize_from_bson::<ResumeToken>(token.clone()).map_err(|e| {
                    ConnectorError::fatal(format!("MongoDB: the saved resume token: {e}"))
                })?,
            );
        } else if let Some(Value::Object(_)) = self.config.get("start_after") {
            options.start_after = json_to_bson(&self.config["start_after"].to_string())
                .ok()
                .and_then(|token| bson::deserialize_from_bson::<ResumeToken>(token).ok());
        } else if let Some(seconds) = self.config.get("operation_time").and_then(Value::as_u64) {
            options.start_at_operation_time = Some(bson::Timestamp {
                time: seconds as u32,
                increment: 0,
            });
        }
        let stream = match &self.collection {
            Some(collection) => {
                database
                    .collection::<Document>(collection)
                    .watch()
                    .pipeline(pipeline)
                    .with_options(options)
                    .await
            }
            None => {
                database
                    .watch()
                    .pipeline(pipeline)
                    .with_options(options)
                    .await
            }
        }
        .map_err(|e| {
            if saved.is_some() {
                ConnectorError::fatal(format!(
                    "MongoDB: the change stream cannot resume after the last committed change, \
                     which is no longer in the oplog; delete the document {} of {} to start at \
                     the current time: {e}",
                    self.name, self.checkpoints
                ))
            } else {
                mongo_error("opening the change stream", e)
            }
        })?;
        self.stream = Some(stream.with_type::<Document>());
        self.client = Some(client);
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

    fn lose(&mut self) -> SourceEvent {
        self.stream = None;
        self.client = None;
        self.pending = None;
        self.tokens.clear();
        SourceEvent::Revoking {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
            within: Duration::from_millis(0),
        }
    }

    fn add(&mut self, change: Document) {
        self.delivered += 1;
        let position = self.delivered;
        if let Some(token) = change.get("_id") {
            self.tokens.insert(position, token.clone());
        }
        let value = serde_json::to_vec(&document_to_json(&change)).unwrap_or_default();
        let envelope = Envelope {
            key: None,
            value,
            headers: Vec::new(),
            partition: PARTITION.0,
            position: Some(position as i64),
            timestamp: change
                .get_datetime("wallTime")
                .ok()
                .map(|at| at.timestamp_millis()),
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

impl Source for MongoSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if self.stream.is_none() {
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
                let stream = self.stream.as_mut().expect("connected");
                match tokio::time::timeout(wait, stream.next()).await {
                    Ok(Some(Ok(change))) => self.add(change),
                    Ok(Some(Err(_))) | Ok(None) => return Ok(self.lose()),
                    Err(_) => return Ok(self.take()?.unwrap_or(SourceEvent::Idle)),
                }
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            let mut last_token = None;
            for commit in commits {
                if commit.epoch != AssignmentEpoch(self.epoch) || self.client.is_none() {
                    return CommitOutcome::Stale;
                }
                let Position::Offset(last) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "MongoDB acknowledges change positions",
                    ));
                };
                let later = self.tokens.split_off(&(last as u64 + 1));
                let done = std::mem::replace(&mut self.tokens, later);
                if let Some((_, token)) = done.into_iter().next_back() {
                    last_token = Some(token);
                }
            }
            let Some(token) = last_token else {
                return CommitOutcome::Committed;
            };
            let client = self.client.as_ref().expect("checked");
            let saved = self
                .checkpoint_collection(client)
                .update_one(
                    doc! {"_id": &self.name},
                    doc! {"$set": {"resume_token": token, "namespace": self.namespace()}},
                )
                .upsert(true)
                .await;
            match saved {
                Ok(_) => CommitOutcome::Committed,
                Err(error) => CommitOutcome::Failed(mongo_error("saving the checkpoint", error)),
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
            ack_kind: AckKind::Offset,
            // One change stream: a second instance would see the same changes.
            max_instances: Some(1),
            needs_keepalive: false,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.stream = None;
            self.client = None;
        })
    }
}

pub struct MongoSink {
    url: String,
    database: String,
    collection: String,
    unique: Vec<String>,
    client: tokio::sync::OnceCell<Client>,
}

impl MongoSink {
    pub fn new(config: &Value) -> Result<MongoSink, ConnectorError> {
        let need = |key: &str| {
            text(config, key)
                .ok_or_else(|| ConnectorError::fatal(format!("MongoDB sink settings need {key}.")))
        };
        Ok(MongoSink {
            url: need("connection_string")?,
            database: need("database_name")?,
            collection: need("collection_name")?,
            unique: config
                .get("unique_constraints")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            client: tokio::sync::OnceCell::new(),
        })
    }

    async fn collection(&self) -> Result<Collection<Document>, ConnectorError> {
        let client = self
            .client
            .get_or_try_init(|| Client::with_uri_str(&self.url))
            .await
            .map_err(|e| mongo_error("connecting", e.clone()))?;
        Ok(client.database(&self.database).collection(&self.collection))
    }
}

impl Sink for MongoSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let messages =
                read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
            let collection = self.collection().await?;
            let mut inserts = Vec::new();
            // The last message of each key in the batch, as one by one.
            let mut upserts: Vec<(Document, Document)> = Vec::new();
            let mut by_key: BTreeMap<String, usize> = BTreeMap::new();
            for (row, message) in messages.iter().enumerate() {
                let text = std::str::from_utf8(&message.value).map_err(|_| {
                    ConnectorError::fatal(format!("MongoDB: message {row} is not UTF-8"))
                })?;
                let value = json_to_bson(text).map_err(|e| {
                    ConnectorError::fatal(format!("MongoDB: message {row} is not JSON: {e}"))
                })?;
                let document = match value {
                    Bson::Document(document) => document,
                    other => doc! {"data": other},
                };
                let keyed = !self.unique.is_empty()
                    && self.unique.iter().all(|key| document.contains_key(key));
                if !keyed {
                    inserts.push(document);
                    continue;
                }
                let mut filter = Document::new();
                for key in &self.unique {
                    filter.insert(
                        key.clone(),
                        document.get(key).cloned().unwrap_or(Bson::Null),
                    );
                }
                let key = filter.to_string();
                match by_key.get(&key) {
                    Some(&index) => upserts[index] = (filter, document),
                    None => {
                        by_key.insert(key, upserts.len());
                        upserts.push((filter, document));
                    }
                }
            }
            if !inserts.is_empty() {
                collection
                    .insert_many(inserts)
                    .await
                    .map_err(|e| mongo_error("inserting", e))?;
            }
            let updates =
                futures_util::stream::iter(upserts.into_iter().map(|(filter, document)| {
                    let collection = collection.clone();
                    async move {
                        collection
                            .update_one(filter, doc! {"$set": document})
                            .upsert(true)
                            .await
                    }
                }))
                .buffer_unordered(64);
            futures_util::pin_mut!(updates);
            while let Some(result) = updates.next().await {
                result.map_err(|e| mongo_error("updating", e))?;
            }
            Ok(())
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: !self.unique.is_empty(),
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bson_becomes_the_json_blocks_get() {
        let id = bson::oid::ObjectId::parse_str("652a1f0e8b3c4d5e6f708192").unwrap();
        let document = doc! {
            "_id": id,
            "at": bson::DateTime::from_millis(1_792_000_000_123),
            "price": "12.30".parse::<bson::Decimal128>().unwrap(),
            "n": 7_i64,
            "clusterTime": bson::Timestamp { time: 5, increment: 2 },
        };
        let json = document_to_json(&document);
        assert_eq!(json["_id"], "652a1f0e8b3c4d5e6f708192");
        assert_eq!(json["at"], "2026-10-14T17:46:40.123Z");
        assert_eq!(json["price"], "12.30");
        assert_eq!(json["n"], 7);
        assert_eq!(json["clusterTime"], serde_json::json!({"t": 5, "i": 2}));
    }

    #[test]
    fn json_integers_past_64_bits_keep_every_digit() {
        let Ok(Bson::Document(document)) =
            json_to_bson(r#"{"big": 1180591620717411303425, "n": 3, "x": 0.5}"#)
        else {
            panic!()
        };
        assert_eq!(
            document.get("big").unwrap().to_string(),
            "1180591620717411303425"
        );
        assert!(matches!(document.get("big"), Some(Bson::Decimal128(_))));
        assert_eq!(document.get("n"), Some(&Bson::Int32(3)));
        assert_eq!(document.get("x"), Some(&Bson::Double(0.5)));
    }
}
