//! PostgreSQL change data capture source: logical replication with pgoutput, as MageML's
//! Python source reads it, with its settings and its messages.
//!
//! Each changed row reaches the blocks as a JSON object of its columns and `_mage_operation`
//! (insert, update or delete), `_mage_schema`, `_mage_table`, `_mage_lsn`, `_mage_commit_time`
//! and, for deletes, `_mage_deleted_at`. Values keep their types: integers and numerics as
//! JSON numbers with every digit, booleans, json and jsonb as JSON, arrays as JSON arrays,
//! timestamps as ISO 8601 text in UTC, bytea as base64, the rest as PostgreSQL writes it. A
//! delete has the key columns only, unless the table has REPLICA IDENTITY FULL; a primary key
//! that changes is a delete of the old key and then the update. A large value an update did
//! not change is not in the log; it is read from the table by key.
//!
//! The replication slot holds the position: once every sink took a batch, the end of the last
//! transaction whose changes are all in it is confirmed to the server, so a restarted service
//! continues after it, on any machine. Changes of a transaction that was partly handled come
//! again. While the stream reads nothing of interest, the slot follows the server's log, so it
//! does not keep the log of other tables' changes.
//!
//! Settings: `host`, `port`, `database`, `user`, `password`, `replication_slot`
//! (`mage_slot`), `publication_name` (`mage_pub`), `tables` (schema.table; without them every
//! table of the publication), `create_slot` (the slot and the publication when missing;
//! `true`), `start_lsn` (where a new slot starts), `connect_timeout`, `sslmode`,
//! `sslrootcert`.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, ConnectorError,
    Envelope, EnvelopeBuilder, PartitionId, Position, Source, SourceCapabilities, SourceEvent,
};
use serde_json::Value;
use tokio_postgres::{Client, Config, SimpleQueryMessage};

use crate::replication::{
    POSTGRES_EPOCH_MICROS, Replicated, Replication, Target, identifier, parse_lsn,
};
use crate::{classify, flag, text, tls, tls_mode};

const PARTITION: PartitionId = PartitionId(0);
/// How often the server hears where the stream is, also while it is paused. The server drops
/// a connection it has not heard from in `wal_sender_timeout` (60 seconds by default).
const STATUS_EVERY: Duration = Duration::from_secs(10);

/// How a source groups messages into batches.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_wait: Duration,
}

#[derive(Debug, Clone)]
struct Settings {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: Option<String>,
    slot: String,
    publication: String,
    /// schema.table
    tables: Vec<String>,
    create_slot: bool,
    start_lsn: u64,
    sslmode: Option<String>,
    sslrootcert: Option<String>,
    connect_timeout: Duration,
}

impl Settings {
    fn new(config: &Value) -> Result<Settings, ConnectorError> {
        let need = |key: &str| {
            text(config, key).ok_or_else(|| {
                ConnectorError::fatal(format!("PostgreSQL source settings need {key}."))
            })
        };
        let port = text(config, "port")
            .unwrap_or_else(|| "5432".into())
            .parse::<u16>()
            .map_err(|_| ConnectorError::fatal("PostgreSQL port is a number."))?;
        let tables: Vec<String> = match config.get("tables") {
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            Some(Value::String(list)) => list.split(',').map(str::to_string).collect(),
            _ => Vec::new(),
        };
        let tables = tables
            .iter()
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .map(|t| {
                if t.contains('.') {
                    t.to_string()
                } else {
                    format!("public.{t}")
                }
            })
            .collect();
        let start_lsn = match text(config, "start_lsn") {
            Some(lsn) => parse_lsn(&lsn).ok_or_else(|| {
                ConnectorError::fatal(format!(
                    "PostgreSQL start_lsn {lsn} is not a log position such as 0/16B3748."
                ))
            })?,
            None => 0,
        };
        let settings = Settings {
            host: need("host")?,
            port,
            database: need("database")?,
            user: text(config, "user")
                .or_else(|| text(config, "username"))
                .ok_or_else(|| ConnectorError::fatal("PostgreSQL source settings need user."))?,
            password: text(config, "password"),
            slot: text(config, "replication_slot").unwrap_or_else(|| "mage_slot".into()),
            publication: text(config, "publication_name").unwrap_or_else(|| "mage_pub".into()),
            tables,
            create_slot: flag(config, "create_slot", true),
            start_lsn,
            sslmode: text(config, "sslmode"),
            sslrootcert: text(config, "sslrootcert"),
            connect_timeout: Duration::from_secs(
                text(config, "connect_timeout")
                    .and_then(|t| t.parse().ok())
                    .unwrap_or(10),
            ),
        };
        // Checked now, so a bad mode or certificate stops the service at start-up.
        tls_mode(settings.sslmode.as_deref(), settings.sslrootcert.as_deref())?;
        Ok(settings)
    }

    fn target(&self) -> Target<'_> {
        Target {
            host: &self.host,
            port: self.port,
            database: &self.database,
            user: &self.user,
            password: self.password.as_deref(),
            sslmode: self.sslmode.as_deref(),
            sslrootcert: self.sslrootcert.as_deref(),
            connect_timeout: self.connect_timeout,
        }
    }
}

struct Column {
    name: String,
    /// `"name":`, written before each value.
    key: Vec<u8>,
    type_oid: u32,
    is_key: bool,
}

struct Relation {
    namespace: String,
    name: String,
    /// d (the primary key), n (nothing), f (every column), i (an index).
    identity: u8,
    columns: Vec<Column>,
    selected: bool,
    /// `,"_mage_schema":...,"_mage_table":...`
    suffix: Vec<u8>,
}

#[derive(Clone)]
enum Cell<'a> {
    Null,
    /// A large value the change did not modify, which is not in the log.
    Unchanged,
    Text(Cow<'a, [u8]>),
}

struct Pending {
    builder: EnvelopeBuilder,
    first: u64,
    last: u64,
    started: Instant,
}

pub struct PgCdcSource {
    settings: Settings,
    limits: BatchLimits,
    connection: Option<Replication>,
    client: Option<Client>,
    relations: HashMap<u32, Relation>,
    epoch: u64,
    seq: u64,
    /// Messages handed over, which numbers their positions.
    delivered: u64,
    /// The last position every sink took.
    committed: u64,
    /// The end of each transaction whose last change is at the position.
    ends: BTreeMap<u64, u64>,
    /// The log through which everything was handled.
    confirmed: u64,
    reported: u64,
    last_status: Instant,
    in_transaction: bool,
    commit_micros: i64,
    /// `,"_mage_commit_time":"..."` of the transaction being read.
    commit_time: Vec<u8>,
    pending: Option<Pending>,
    paused: bool,
    broken: bool,
    warned_truncate: bool,
}

impl PgCdcSource {
    pub fn new(config: &Value, limits: BatchLimits) -> Result<PgCdcSource, ConnectorError> {
        Ok(PgCdcSource {
            settings: Settings::new(config)?,
            limits,
            connection: None,
            client: None,
            relations: HashMap::new(),
            epoch: 0,
            seq: 0,
            delivered: 0,
            committed: 0,
            ends: BTreeMap::new(),
            confirmed: 0,
            reported: 0,
            last_status: Instant::now(),
            in_transaction: false,
            commit_micros: 0,
            commit_time: Vec::new(),
            pending: None,
            paused: false,
            broken: false,
            warned_truncate: false,
        })
    }

    fn label(&self) -> String {
        let s = &self.settings;
        format!("{}:{}/{}", s.host, s.port, s.database)
    }

    async fn client(&self) -> Result<Client, ConnectorError> {
        let s = &self.settings;
        let mut config = Config::new();
        config
            .host(&s.host)
            .port(s.port)
            .dbname(&s.database)
            .user(&s.user)
            .application_name("mage-stream")
            .connect_timeout(s.connect_timeout);
        if let Some(password) = &s.password {
            config.password(password);
        }
        let (mode, check, root) = tls_mode(s.sslmode.as_deref(), s.sslrootcert.as_deref())?;
        let connector = tls::connector(check, root.as_deref())
            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL TLS: {e}")))?;
        config.ssl_mode(mode);
        let (client, connection) = config
            .connect(connector)
            .await
            .map_err(|e| classify(&format!("connecting to {}", self.label()), e))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// Creates the publication and the slot when they do not exist. A publication without
    /// tables is FOR ALL TABLES, which needs a superuser.
    async fn prepare(&self, client: &Client) -> Result<(), ConnectorError> {
        let s = &self.settings;
        let found = client
            .query_opt(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                &[&s.publication],
            )
            .await
            .map_err(|e| classify("reading publications", e))?;
        if found.is_none() {
            let target = if s.tables.is_empty() {
                "FOR ALL TABLES".to_string()
            } else {
                let names: Vec<String> = s.tables.iter().map(|t| qualified(t)).collect();
                format!("FOR TABLE {}", names.join(", "))
            };
            let created = client
                .batch_execute(&format!(
                    "CREATE PUBLICATION {} {target}",
                    identifier(&s.publication)
                ))
                .await;
            if let Err(error) = created {
                // Another instance made it first.
                if error.code().map(|c| c.code()) != Some("42710") {
                    return Err(classify(
                        &format!("creating publication {}", s.publication),
                        error,
                    ));
                }
            }
        }
        let found = client
            .query_opt(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&s.slot],
            )
            .await
            .map_err(|e| classify("reading replication slots", e))?;
        if found.is_none() {
            let created = client
                .query(
                    "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&s.slot],
                )
                .await;
            if let Err(error) = created
                && error.code().map(|c| c.code()) != Some("42710")
            {
                return Err(classify(
                    &format!("creating replication slot {}", s.slot),
                    error,
                ));
            }
        }
        Ok(())
    }

    async fn connect(&mut self) -> Result<SourceEvent, ConnectorError> {
        let client = self.client().await?;
        if self.settings.create_slot {
            self.prepare(&client).await?;
        }
        let mut connection = Replication::connect(&self.settings.target()).await?;
        connection
            .start(
                &self.settings.slot,
                &self.settings.publication,
                self.settings.start_lsn.max(self.confirmed),
            )
            .await?;
        self.connection = Some(connection);
        self.client = Some(client);
        self.relations.clear();
        self.in_transaction = false;
        self.committed = self.delivered;
        self.last_status = Instant::now();
        self.broken = false;
        self.epoch += 1;
        self.seq = 0;
        Ok(SourceEvent::Assigned {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
        })
    }

    fn lose(&mut self) -> SourceEvent {
        self.connection = None;
        self.client = None;
        self.pending = None;
        self.ends.clear();
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

    /// Tells the server where the stream is when it is time to, or when it moved on.
    async fn report(&mut self, now: bool) -> Result<(), ConnectorError> {
        if !now && self.confirmed == self.reported && self.last_status.elapsed() < STATUS_EVERY {
            return Ok(());
        }
        if let Some(connection) = self.connection.as_mut() {
            connection.status(self.confirmed).await?;
            self.reported = self.confirmed;
            self.last_status = Instant::now();
        }
        Ok(())
    }

    fn push(&mut self, value: Vec<u8>) {
        self.delivered += 1;
        let position = self.delivered;
        let envelope = Envelope {
            key: None,
            value,
            headers: Vec::new(),
            partition: PARTITION.0,
            position: Some(position as i64),
            timestamp: Some((self.commit_micros + POSTGRES_EPOCH_MICROS) / 1000),
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

    async fn handle(&mut self, start: u64, payload: Bytes) -> Result<(), ConnectorError> {
        let mut reader = Reader::new(&payload);
        match reader.u8()? {
            b'B' => {
                reader.u64()?;
                self.commit_micros = reader.i64()?;
                self.in_transaction = true;
                self.commit_time.clear();
                self.commit_time
                    .extend_from_slice(b",\"_mage_commit_time\":\"");
                self.commit_time
                    .extend_from_slice(commit_time(self.commit_micros).as_bytes());
                self.commit_time.push(b'"');
            }
            b'C' => {
                reader.u8()?;
                reader.u64()?;
                let end = reader.u64()?;
                self.in_transaction = false;
                if self.committed == self.delivered {
                    // Every change of the transaction, if any, was taken already.
                    self.confirmed = self.confirmed.max(end);
                } else {
                    self.ends.insert(self.delivered, end);
                }
            }
            b'R' => {
                let id = reader.u32()?;
                let namespace = reader.string()?.to_string();
                let name = reader.string()?.to_string();
                let identity = reader.u8()?;
                let count = reader.i16()?.max(0) as usize;
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    let flags = reader.u8()?;
                    let column = reader.string()?.to_string();
                    let type_oid = reader.u32()?;
                    reader.i32()?;
                    let mut key = serde_json::to_vec(&column).unwrap_or_default();
                    key.push(b':');
                    columns.push(Column {
                        name: column,
                        key,
                        type_oid,
                        is_key: flags & 1 == 1,
                    });
                }
                let full = format!("{namespace}.{name}");
                let selected =
                    self.settings.tables.is_empty() || self.settings.tables.contains(&full);
                let mut suffix = b",\"_mage_schema\":".to_vec();
                suffix.extend(serde_json::to_vec(&namespace).unwrap_or_default());
                suffix.extend_from_slice(b",\"_mage_table\":");
                suffix.extend(serde_json::to_vec(&name).unwrap_or_default());
                self.relations.insert(
                    id,
                    Relation {
                        namespace,
                        name,
                        identity,
                        columns,
                        selected,
                        suffix,
                    },
                );
            }
            kind @ (b'I' | b'U' | b'D') => {
                let id = reader.u32()?;
                match self.relations.get(&id) {
                    Some(relation) if relation.selected => {}
                    Some(_) => return Ok(()),
                    None => {
                        return Err(ConnectorError::fatal(format!(
                            "PostgreSQL sent a change of relation {id} before describing it"
                        )));
                    }
                }
                for row in self.change(kind, id, start, &mut reader).await? {
                    self.push(row);
                }
            }
            b'T' if !self.warned_truncate => {
                self.warned_truncate = true;
                eprintln!(
                    "warning: PostgreSQL stream source on {}: TRUNCATE is not replicated; \
                         truncate the destination too.",
                    self.label()
                );
            }
            // Types, origins and logical messages.
            _ => {}
        }
        Ok(())
    }

    async fn change(
        &mut self,
        kind: u8,
        id: u32,
        start: u64,
        reader: &mut Reader<'_>,
    ) -> Result<Vec<Vec<u8>>, ConnectorError> {
        let relation = &self.relations[&id];
        let mut rows = Vec::with_capacity(1);
        let mut marker = reader.u8()?;
        let mut old = None;
        if kind != b'I' && (marker == b'K' || marker == b'O') {
            old = Some(reader.tuple()?);
            if kind == b'U' {
                marker = reader.u8()?;
            }
        }
        if kind == b'D' {
            let Some(old) = old else {
                return Ok(rows);
            };
            let keys_only = relation.identity != b'f';
            rows.push(self.row(relation, &old, keys_only, b"delete", start, true));
            return Ok(rows);
        }
        if marker != b'N' {
            return Err(ConnectorError::fatal(
                "PostgreSQL sent a change without its new row",
            ));
        }
        let mut new = reader.tuple()?;
        if new.iter().any(|c| matches!(c, Cell::Unchanged)) {
            if let Some(old) = &old {
                for (cell, before) in new.iter_mut().zip(old) {
                    if matches!(cell, Cell::Unchanged) && !matches!(before, Cell::Unchanged) {
                        *cell = before.clone();
                    }
                }
            }
            if new.iter().any(|c| matches!(c, Cell::Unchanged)) {
                let read = self.read_unchanged(relation, &new).await?;
                for (cell, value) in new.iter_mut().zip(read) {
                    if let Some(value) = value {
                        *cell = value;
                    }
                }
            }
        }
        if kind == b'U'
            && let Some(old) = &old
        {
            let key_changed = relation
                .columns
                .iter()
                .zip(old.iter().zip(&new))
                .any(|(c, (a, b))| c.is_key && !same(a, b));
            if key_changed {
                rows.push(self.row(relation, old, true, b"delete", start, true));
            }
        }
        let operation: &[u8] = if kind == b'I' { b"insert" } else { b"update" };
        rows.push(self.row(relation, &new, false, operation, start, false));
        Ok(rows)
    }

    /// Large values the change did not modify, read from the table by the row's key.
    async fn read_unchanged(
        &self,
        relation: &Relation,
        row: &[Cell<'_>],
    ) -> Result<Vec<Option<Cell<'static>>>, ConnectorError> {
        let mut out = vec![None; row.len()];
        let keys: Vec<(&Column, &Cell)> = relation
            .columns
            .iter()
            .zip(row)
            .filter(|(c, _)| c.is_key)
            .collect();
        let missing: Vec<usize> = row
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c, Cell::Unchanged))
            .map(|(i, _)| i)
            .collect();
        let client = self.client.as_ref().expect("connected");
        if keys.is_empty() || keys.iter().any(|(_, c)| !matches!(c, Cell::Text(_))) {
            eprintln!(
                "warning: unchanged large values of {}.{} are not in the log, and the table \
                 has no key to read them by; set REPLICA IDENTITY FULL.",
                relation.namespace, relation.name
            );
            return Ok(out);
        }
        let selected: Vec<String> = missing
            .iter()
            .map(|i| format!("{}::text", identifier(&relation.columns[*i].name)))
            .collect();
        let condition: Vec<String> = keys
            .iter()
            .map(|(column, cell)| {
                let Cell::Text(value) = cell else {
                    unreachable!("checked")
                };
                format!(
                    "{} = '{}'",
                    identifier(&column.name),
                    String::from_utf8_lossy(value).replace('\'', "''")
                )
            })
            .collect();
        let query = format!(
            "SELECT {} FROM {}.{} WHERE {}",
            selected.join(", "),
            identifier(&relation.namespace),
            identifier(&relation.name),
            condition.join(" AND ")
        );
        let messages = client
            .simple_query(&query)
            .await
            .map_err(|e| classify("reading unchanged values", e))?;
        for message in messages {
            if let SimpleQueryMessage::Row(found) = message {
                for (n, i) in missing.iter().enumerate() {
                    out[*i] = Some(match found.get(n) {
                        Some(value) => Cell::Text(Cow::Owned(value.as_bytes().to_vec())),
                        None => Cell::Null,
                    });
                }
                break;
            }
        }
        Ok(out)
    }

    fn row(
        &self,
        relation: &Relation,
        cells: &[Cell<'_>],
        keys_only: bool,
        operation: &[u8],
        start: u64,
        deleted: bool,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.push(b'{');
        let mut first = true;
        for (column, cell) in relation.columns.iter().zip(cells) {
            if keys_only && !column.is_key {
                continue;
            }
            let value = match cell {
                Cell::Unchanged => continue,
                Cell::Null => None,
                Cell::Text(text) => Some(text),
            };
            if !first {
                out.push(b',');
            }
            first = false;
            out.extend_from_slice(&column.key);
            match value {
                None => out.extend_from_slice(b"null"),
                Some(text) => write_value(&mut out, column.type_oid, text),
            }
        }
        if !first {
            out.push(b',');
        }
        out.extend_from_slice(b"\"_mage_operation\":\"");
        out.extend_from_slice(operation);
        out.push(b'"');
        out.extend_from_slice(&relation.suffix);
        out.extend_from_slice(format!(",\"_mage_lsn\":{start}").as_bytes());
        out.extend_from_slice(&self.commit_time);
        if deleted {
            out.extend_from_slice(b",\"_mage_deleted_at\":\"");
            out.extend_from_slice(
                chrono::Utc::now()
                    .format("%Y-%m-%d %H:%M:%S%.6f")
                    .to_string()
                    .as_bytes(),
            );
            out.push(b'"');
        }
        out.push(b'}');
        out
    }
}

fn same(a: &Cell<'_>, b: &Cell<'_>) -> bool {
    match (a, b) {
        (Cell::Null, Cell::Null) => true,
        (Cell::Text(a), Cell::Text(b)) => a == b,
        // An unchanged value is the same.
        (Cell::Unchanged, _) | (_, Cell::Unchanged) => true,
        _ => false,
    }
}

fn qualified(table: &str) -> String {
    match table.split_once('.') {
        Some((schema, name)) => format!("{}.{}", identifier(schema), identifier(name)),
        None => identifier(table),
    }
}

fn commit_time(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros + POSTGRES_EPOCH_MICROS)
        .map(|at| at.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string())
        .unwrap_or_default()
}

/// The element type of the array types whose elements become JSON values.
fn array_element(type_oid: u32) -> Option<u32> {
    Some(match type_oid {
        1000 => 16,
        1001 => 17,
        1005 => 21,
        1007 => 23,
        1016 => 20,
        1028 => 26,
        1021 => 700,
        1022 => 701,
        1231 => 1700,
        199 => 114,
        3807 => 3802,
        1115 => 1114,
        1185 => 1184,
        1009 | 1015 | 1014 | 2951 | 1182 | 1183 | 1270 | 1186 => 25,
        _ => return None,
    })
}

fn is_number(text: &[u8]) -> bool {
    match text {
        [b'-', rest @ ..] => rest.first().is_some_and(u8::is_ascii_digit),
        _ => text.first().is_some_and(u8::is_ascii_digit),
    }
}

fn write_string(out: &mut Vec<u8>, text: &[u8]) {
    let text = String::from_utf8_lossy(text);
    serde_json::to_writer(&mut *out, text.as_ref()).expect("writing to a vector");
}

/// A value as the blocks get it, by the column's type.
fn write_value(out: &mut Vec<u8>, type_oid: u32, text: &[u8]) {
    match type_oid {
        16 => out.extend_from_slice(if text == b"t" { b"true" } else { b"false" }),
        // Integers and numerics keep every digit; NaN and infinities are text.
        20 | 21 | 23 | 26 | 700 | 701 | 1700 if is_number(text) => out.extend_from_slice(text),
        114 | 3802 => out.extend_from_slice(text),
        17 => match text.strip_prefix(b"\\x").and_then(decode_hex) {
            Some(bytes) => {
                out.push(b'"');
                out.extend_from_slice(
                    base64::engine::general_purpose::STANDARD
                        .encode(bytes)
                        .as_bytes(),
                );
                out.push(b'"');
            }
            None => write_string(out, text),
        },
        1114 | 1184 if text.first().is_some_and(u8::is_ascii_digit) => {
            let mut iso = text.to_vec();
            if let Some(space) = iso.iter().position(|b| *b == b' ') {
                iso[space] = b'T';
            }
            if type_oid == 1184 && iso.ends_with(b"+00") {
                iso.extend_from_slice(b":00");
            }
            write_string(out, &iso);
        }
        _ => match array_element(type_oid) {
            Some(element) => {
                let mark = out.len();
                if !write_array(out, element, text) {
                    out.truncate(mark);
                    write_string(out, text);
                }
            }
            None => write_string(out, text),
        },
    }
}

fn decode_hex(text: &[u8]) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    text.chunks(2)
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect()
}

/// An array as a JSON array, its elements by their type; false when the text is not an
/// array PostgreSQL writes.
fn write_array(out: &mut Vec<u8>, element: u32, text: &[u8]) -> bool {
    let mut text = text;
    // Bounds other than 1, such as [0:2]={1,2,3}.
    if text.first() == Some(&b'[') {
        match text.iter().position(|b| *b == b'=') {
            Some(i) => text = &text[i + 1..],
            None => return false,
        }
    }
    let mut at = 0;
    if !array_level(out, element, text, &mut at) {
        return false;
    }
    at == text.len()
}

fn array_level(out: &mut Vec<u8>, element: u32, text: &[u8], at: &mut usize) -> bool {
    if text.get(*at) != Some(&b'{') {
        return false;
    }
    *at += 1;
    out.push(b'[');
    if text.get(*at) == Some(&b'}') {
        *at += 1;
        out.push(b']');
        return true;
    }
    loop {
        match text.get(*at) {
            Some(b'{') => {
                if !array_level(out, element, text, at) {
                    return false;
                }
            }
            Some(b'"') => {
                *at += 1;
                let mut value = Vec::new();
                loop {
                    match text.get(*at) {
                        Some(b'\\') => {
                            let Some(c) = text.get(*at + 1) else {
                                return false;
                            };
                            value.push(*c);
                            *at += 2;
                        }
                        Some(b'"') => {
                            *at += 1;
                            break;
                        }
                        Some(c) => {
                            value.push(*c);
                            *at += 1;
                        }
                        None => return false,
                    }
                }
                write_value(out, element, &value);
            }
            Some(_) => {
                let begin = *at;
                while let Some(c) = text.get(*at) {
                    if *c == b',' || *c == b'}' {
                        break;
                    }
                    *at += 1;
                }
                let value = &text[begin..*at];
                if value.eq_ignore_ascii_case(b"NULL") {
                    out.extend_from_slice(b"null");
                } else {
                    write_value(out, element, value);
                }
            }
            None => return false,
        }
        match text.get(*at) {
            Some(b',') => {
                *at += 1;
                out.push(b',');
            }
            Some(b'}') => {
                *at += 1;
                out.push(b']');
                return true;
            }
            _ => return false,
        }
    }
}

/// Reads the fields of a pgoutput message.
struct Reader<'a> {
    data: &'a [u8],
    at: usize,
}

fn malformed() -> ConnectorError {
    ConnectorError::fatal("PostgreSQL sent a pgoutput message MageML cannot read")
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Reader<'a> {
        Reader { data, at: 0 }
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8], ConnectorError> {
        let end = self.at.checked_add(n).filter(|e| *e <= self.data.len());
        let end = end.ok_or_else(malformed)?;
        let out = &self.data[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, ConnectorError> {
        Ok(self.bytes(1)?[0])
    }

    fn i16(&mut self) -> Result<i16, ConnectorError> {
        Ok(i16::from_be_bytes(self.bytes(2)?.try_into().expect("2")))
    }

    fn i32(&mut self) -> Result<i32, ConnectorError> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into().expect("4")))
    }

    fn u32(&mut self) -> Result<u32, ConnectorError> {
        Ok(u32::from_be_bytes(self.bytes(4)?.try_into().expect("4")))
    }

    fn i64(&mut self) -> Result<i64, ConnectorError> {
        Ok(i64::from_be_bytes(self.bytes(8)?.try_into().expect("8")))
    }

    fn u64(&mut self) -> Result<u64, ConnectorError> {
        Ok(u64::from_be_bytes(self.bytes(8)?.try_into().expect("8")))
    }

    fn string(&mut self) -> Result<&'a str, ConnectorError> {
        let rest = &self.data[self.at..];
        let end = rest.iter().position(|b| *b == 0).ok_or_else(malformed)?;
        self.at += end + 1;
        std::str::from_utf8(&rest[..end]).map_err(|_| malformed())
    }

    fn tuple(&mut self) -> Result<Vec<Cell<'a>>, ConnectorError> {
        let count = self.i16()?.max(0) as usize;
        let mut cells = Vec::with_capacity(count);
        for _ in 0..count {
            cells.push(match self.u8()? {
                b'n' => Cell::Null,
                b'u' => Cell::Unchanged,
                b't' | b'b' => {
                    let length = self.i32()?.max(0) as usize;
                    Cell::Text(Cow::Borrowed(self.bytes(length)?))
                }
                _ => return Err(malformed()),
            });
        }
        Ok(cells)
    }
}

impl Source for PgCdcSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if self.connection.is_none() {
                return self.connect().await;
            }
            loop {
                if self.broken {
                    return Ok(self.lose());
                }
                if self.full() {
                    return Ok(self.take()?.expect("full"));
                }
                if self.report(false).await.is_err() {
                    return Ok(self.lose());
                }
                if self.paused {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
                }
                let wait = self.wait();
                let connection = self.connection.as_mut().expect("connected");
                match tokio::time::timeout(wait, connection.next()).await {
                    Ok(Ok(Replicated::Data { start, payload })) => {
                        if let Err(error) = self.handle(start, payload).await {
                            if error.retryable {
                                return Ok(self.lose());
                            }
                            return Err(error);
                        }
                    }
                    Ok(Ok(Replicated::Keepalive { wal_end, reply })) => {
                        // Nothing is waiting for the sinks: the slot may follow the log, so
                        // the server frees the log of changes the stream does not read.
                        if self.committed == self.delivered && !self.in_transaction {
                            self.confirmed = self.confirmed.max(wal_end);
                        }
                        if self.report(reply).await.is_err() {
                            return Ok(self.lose());
                        }
                    }
                    Ok(Err(error)) if error.retryable => return Ok(self.lose()),
                    Ok(Err(error)) => return Err(error),
                    Err(_) => return Ok(self.take()?.unwrap_or(SourceEvent::Idle)),
                }
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            for commit in commits {
                if commit.epoch != AssignmentEpoch(self.epoch) || self.connection.is_none() {
                    return CommitOutcome::Stale;
                }
                let Position::Offset(last) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "PostgreSQL acknowledges change positions",
                    ));
                };
                let last = last as u64;
                let later = self.ends.split_off(&(last + 1));
                let done = std::mem::replace(&mut self.ends, later);
                if let Some((_, end)) = done.into_iter().next_back() {
                    self.confirmed = self.confirmed.max(end);
                }
                self.committed = self.committed.max(last);
            }
            match self.report(false).await {
                Ok(()) => CommitOutcome::Committed,
                Err(error) => {
                    self.broken = true;
                    CommitOutcome::Failed(error)
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
            ack_kind: AckKind::Lsn,
            // One slot: the server lets one connection read it.
            max_instances: Some(1),
            needs_keepalive: true,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let _ = self.report(true).await;
            if let Some(connection) = self.connection.take() {
                connection.close().await;
            }
            self.client = None;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(type_oid: u32, text: &str) -> String {
        let mut out = Vec::new();
        write_value(&mut out, type_oid, text.as_bytes());
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn values_keep_their_types_and_digits() {
        assert_eq!(value(16, "t"), "true");
        assert_eq!(value(20, "9223372036854775807"), "9223372036854775807");
        assert_eq!(
            value(1700, "12345678901234567890.123456789"),
            "12345678901234567890.123456789"
        );
        assert_eq!(value(1700, "NaN"), "\"NaN\"");
        assert_eq!(value(701, "-Infinity"), "\"-Infinity\"");
        assert_eq!(value(701, "1e+20"), "1e+20");
        assert_eq!(value(3802, "{\"k\": [1, null]}"), "{\"k\": [1, null]}");
        assert_eq!(value(17, "\\x00ff"), "\"AP8=\"");
        assert_eq!(value(25, "it's \"one\"\n"), "\"it's \\\"one\\\"\\n\"");
        assert_eq!(
            value(1184, "2024-01-01 12:00:00.123456+00"),
            "\"2024-01-01T12:00:00.123456+00:00\""
        );
        assert_eq!(
            value(1114, "2024-01-01 12:00:00"),
            "\"2024-01-01T12:00:00\""
        );
        assert_eq!(value(1184, "infinity"), "\"infinity\"");
        assert_eq!(value(2950, "0000-0009"), "\"0000-0009\"");
    }

    #[test]
    fn arrays_become_json_arrays() {
        assert_eq!(
            value(1009, "{a,\"b c\",NULL,\"NULL\"}"),
            "[\"a\",\"b c\",null,\"NULL\"]"
        );
        assert_eq!(value(1007, "{{1,2},{3,NULL}}"), "[[1,2],[3,null]]");
        assert_eq!(value(1007, "[0:1]={1,2}"), "[1,2]");
        assert_eq!(value(1007, "{}"), "[]");
        assert_eq!(
            value(1009, "{\"a\\\"b\",\"c\\\\d\"}"),
            "[\"a\\\"b\",\"c\\\\d\"]"
        );
        assert_eq!(value(3807, "{\"{\\\"a\\\": 1}\"}"), "[{\"a\": 1}]");
        assert_eq!(value(1000, "{t,f}"), "[true,false]");
        // Not an array PostgreSQL writes: kept as text.
        assert_eq!(value(1007, "{1,2"), "\"{1,2\"");
    }

    #[test]
    fn commit_times_are_iso_in_utc() {
        assert_eq!(commit_time(0), "2000-01-01T00:00:00.000000+00:00");
        assert_eq!(commit_time(1_500_000), "2000-01-01T00:00:01.500000+00:00");
    }

    #[test]
    fn settings_follow_the_python_source() {
        let settings = Settings::new(&serde_json::json!({
            "host": "db", "database": "app", "user": "me",
            "tables": "orders, sales.items", "start_lsn": "0/16B3748",
        }))
        .unwrap();
        assert_eq!(settings.tables, ["public.orders", "sales.items"]);
        assert_eq!(
            (settings.slot.as_str(), settings.publication.as_str()),
            ("mage_slot", "mage_pub")
        );
        assert_eq!(settings.start_lsn, 0x16B3748);
        assert!(settings.create_slot);
        assert!(Settings::new(&serde_json::json!({"host": "db", "database": "app"})).is_err());
        assert!(
            Settings::new(&serde_json::json!({
                "host": "db", "database": "app", "user": "me", "start_lsn": "nope",
            }))
            .is_err()
        );
    }

    #[test]
    fn tuples_read_nulls_unchanged_values_and_text() {
        let mut payload = vec![0, 3, b'n', b'u', b't', 0, 0, 0, 2];
        payload.extend_from_slice(b"hi");
        let cells = Reader::new(&payload).tuple().unwrap();
        assert!(matches!(cells[0], Cell::Null));
        assert!(matches!(cells[1], Cell::Unchanged));
        assert!(matches!(&cells[2], Cell::Text(t) if t.as_ref() == b"hi"));
        assert!(Reader::new(&[0, 1, b't', 0, 0, 0, 9]).tuple().is_err());
    }
}
