//! The MySQL change capture source (ADR-0048): an initial copy without a global lock, then the
//! binary log from the position read before the copy.
//!
//! Two tasks read on connections of their own and send what they read through bounded
//! channels, so the stream's reads are safe to cancel on its ticks: the copy (one
//! `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` over every table), then the binary
//! log (as a replica, with a heartbeat every 30 seconds, so a dead connection is noticed).
//!
//! Changes made between reading the position and starting the snapshot come again after the
//! copy; the mirror's keyed upserts and deletes make that harmless, which is why
//! `binlog_row_image=FULL` is required.
//!
//! Positions: each change is numbered as it is handed over. When a transaction ends (its XID,
//! or a statement outside one), its end in the log is kept with the number of changes handed
//! over by then; once every sink took them, that end is saved through [`PositionRecord`], and a
//! restart reads the log from it. A transaction handled in part comes again. Nothing is saved
//! until the copy's rows are all in the sinks, so a copy cut short starts over.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, ConnectorError,
    Envelope, EnvelopeBuilder, PartitionId, Position, Source, SourceCapabilities, SourceEvent,
    TableCopy,
};
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn};
use mysql_common::binlog::events::{EventData, OptionalMetadataField, RowsEventData};
use mysql_common::binlog::row::BinlogRow;
use serde_json::Value;

use crate::ddl::{self, Ddl, Name};
use crate::gtid::{Gtid, GtidSet, uuid};
use crate::values::{Kind, kind_of, write_binlog, write_text};
use crate::{LogSettings, Settings, classify, connect, log_position, log_status, quote, tables};

const PARTITION: PartitionId = PartitionId(0);
/// Rows a copy reads ahead, and changes the log reader reads ahead.
const READ_AHEAD: usize = 10_000;
/// The server sends a heartbeat this often while the log is quiet; nothing for three of them
/// is a lost connection.
const HEARTBEAT: Duration = Duration::from_secs(30);
/// How often an idle source asks whether it is caught up ([`Source::caught_up`]).
const PROBE_EVERY: Duration = Duration::from_secs(1);
/// A transaction takes its time before it is in the log: a caught-up time is moved back by
/// this much, so one committing at the probe is never counted as read.
const CAUGHT_UP_MARGIN_MICROS: i64 = 1_000_000;

/// How a source groups messages into batches.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_wait: Duration,
}

/// Where a stream keeps its position in the binary log: the service's state files.
pub trait PositionRecord: Send + Sync {
    fn load(&self) -> Result<Option<String>, ConnectorError>;
    fn save(&self, position: &str) -> Result<(), ConnectorError>;
}

/// A position in the binary log: a file and an offset in it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LogPosition {
    file: String,
    offset: u64,
}

impl LogPosition {
    /// For ordering: the file's sequence number (`binlog.000012`), then the offset.
    fn order(&self) -> (u64, u64) {
        let sequence = self
            .file
            .rsplit('.')
            .next()
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        (sequence, self.offset)
    }

    fn text(&self) -> String {
        format!("{}:{}", self.file, self.offset)
    }

    /// The saved form: the server it is a position of, the file and the offset, and with
    /// GTIDs on, the transactions handled.
    fn saved(&self, source: &str, gtids: Option<&GtidSet>) -> String {
        let mut saved =
            serde_json::json!({"source": source, "file": self.file, "offset": self.offset});
        if let Some(gtids) = gtids {
            saved["gtids"] = gtids.text().into();
        }
        saved.to_string()
    }

    /// A saved position of this server, with its GTIDs; None for another server's (its
    /// settings changed).
    fn read_saved(text: &str, source: &str) -> Option<(LogPosition, Option<GtidSet>)> {
        let saved: Value = serde_json::from_str(text).ok()?;
        if saved.get("source")?.as_str()? != source {
            return None;
        }
        let position = LogPosition {
            file: saved.get("file")?.as_str()?.to_string(),
            offset: saved.get("offset")?.as_u64()?,
        };
        let gtids = match saved.get("gtids").and_then(Value::as_str) {
            Some(text) => Some(GtidSet::parse(text).ok()?),
            None => None,
        };
        Some((position, gtids))
    }
}

/// What the log reader found.
enum Change {
    Row(Vec<u8>),
    /// A transaction ended here; its GTID, with GTIDs on.
    End(LogPosition, Option<Gtid>),
}

struct Copy {
    /// Each row's message with the index of its table.
    rows: tokio::sync::mpsc::Receiver<Result<(usize, Vec<u8>), ConnectorError>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Copy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A table of the initial copy, as the PostgreSQL source keeps it.
struct CopyTable {
    table: String,
    estimate: Option<i64>,
    copied: i64,
    /// Once every row was read: the position of its last row, done when the sinks took it.
    last: Option<u64>,
}

struct Reader {
    changes: tokio::sync::mpsc::Receiver<Result<Change, ConnectorError>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Pending {
    builder: EnvelopeBuilder,
    first: u64,
    last: u64,
    started: Instant,
}

pub struct MySqlCdcSource {
    settings: Arc<Settings>,
    limits: BatchLimits,
    record: Option<Arc<dyn PositionRecord>>,
    connected: bool,
    epoch: u64,
    seq: u64,
    /// Messages handed over, which numbers their positions.
    delivered: u64,
    /// The last position every sink took.
    committed: u64,
    /// Transaction ends, by the number of messages handed over when they were read.
    ends: BTreeMap<u64, LogPosition>,
    /// Their GTIDs, with GTIDs on, by the same numbers.
    end_gtids: BTreeMap<u64, Vec<Gtid>>,
    /// With GTIDs on: the transactions every sink took, saved with the position, from which a
    /// restart reads on any server of the topology.
    gtids: Option<GtidSet>,
    gtids_changed: bool,
    /// Where the log is read from after the copy, or now.
    start: Option<LogPosition>,
    saved: Option<LogPosition>,
    copy: Option<Copy>,
    copy_tables: Vec<CopyTable>,
    /// The last copied row's position, until the sinks took it.
    copy_last: Option<u64>,
    reader: Option<Reader>,
    pending: Option<Pending>,
    paused: bool,
    /// The end of the last transaction read from the log.
    read_through: Option<LogPosition>,
    /// The server's log position and clock (Unix microseconds) when it was asked; once the
    /// log was read through it with nothing left for the sinks, the clock is the caught-up
    /// time.
    probe: Option<(LogPosition, i64)>,
    probed_at: Instant,
    /// A connection of its own for the probes.
    prober: Option<Conn>,
    caught_up: Option<i64>,
}

impl MySqlCdcSource {
    pub fn new(config: &Value, limits: BatchLimits) -> Result<MySqlCdcSource, ConnectorError> {
        Ok(MySqlCdcSource {
            settings: Arc::new(Settings::new(config)?),
            limits,
            record: None,
            connected: false,
            epoch: 0,
            seq: 0,
            delivered: 0,
            committed: 0,
            ends: BTreeMap::new(),
            end_gtids: BTreeMap::new(),
            gtids: None,
            gtids_changed: false,
            start: None,
            saved: None,
            copy: None,
            copy_tables: Vec::new(),
            copy_last: None,
            reader: None,
            pending: None,
            paused: false,
            read_through: None,
            probe: None,
            probed_at: Instant::now(),
            prober: None,
            caught_up: None,
        })
    }

    /// Nothing read is waiting for the sinks.
    fn settled(&self) -> bool {
        self.pending.is_none() && self.committed == self.delivered && self.copy_last.is_none()
    }

    /// A probe the log was read through, with everything in the sinks: caught up.
    fn settle_probe(&mut self) {
        let Some((at, clock)) = &self.probe else {
            return;
        };
        let read = self
            .read_through
            .as_ref()
            .is_some_and(|r| r.order() >= at.order());
        if read && self.settled() {
            let through = clock - CAUGHT_UP_MARGIN_MICROS;
            self.caught_up = Some(self.caught_up.map_or(through, |t| t.max(through)));
            self.probe = None;
        }
    }

    /// With nothing left for the sinks: the server's log position and clock, which the
    /// stream is caught up through once it read that far. A failed probe is tried again
    /// later; it never stops the stream.
    async fn probe(&mut self) {
        if self.probe.is_some() || !self.settled() || self.probed_at.elapsed() < PROBE_EVERY {
            return;
        }
        self.probed_at = Instant::now();
        if self.prober.is_none() {
            self.prober = connect(&self.settings).await.ok();
        }
        let Some(conn) = self.prober.as_mut() else {
            return;
        };
        let position = log_position(conn).await;
        let clock: Result<Option<i64>, _> = conn
            .query_first("SELECT CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000 AS SIGNED)")
            .await;
        match (position, clock) {
            (Ok((file, offset)), Ok(Some(clock))) => {
                self.probe = Some((LogPosition { file, offset }, clock));
                self.settle_probe();
            }
            _ => self.prober = None,
        }
    }

    pub fn with_position_record(mut self, record: Arc<dyn PositionRecord>) -> MySqlCdcSource {
        self.record = Some(record);
        self
    }

    fn source_name(&self) -> String {
        format!("{}:{}", self.settings.host, self.settings.port)
    }

    fn save(&mut self, position: LogPosition) -> Result<(), ConnectorError> {
        if self.saved.as_ref() == Some(&position) && !self.gtids_changed {
            return Ok(());
        }
        if let Some(record) = &self.record {
            record.save(&position.saved(&self.source_name(), self.gtids.as_ref()))?;
        }
        self.saved = Some(position);
        self.gtids_changed = false;
        Ok(())
    }

    /// Transactions every sink took.
    fn handled(&mut self, gtids: impl IntoIterator<Item = Gtid>) {
        if let Some(set) = self.gtids.as_mut() {
            for (source, gno) in gtids {
                set.add(&source, gno);
                self.gtids_changed = true;
            }
        }
    }

    async fn connect(&mut self) -> Result<SourceEvent, ConnectorError> {
        let mut conn = connect(&self.settings).await?;
        let log = LogSettings::read(&mut conn)
            .await
            .map_err(|e| classify("reading the binary log settings", e))?;
        let problems = log.problems();
        if !problems.is_empty() {
            return Err(ConnectorError::fatal(format!(
                "MySQL {} cannot be read for changes: {}",
                self.settings.label(),
                problems.join(" ")
            )));
        }
        let saved = match &self.record {
            Some(record) => record
                .load()?
                .and_then(|text| LogPosition::read_saved(&text, &self.source_name())),
            None => None,
        };
        self.reader = None;
        self.copy = None;
        self.pending = None;
        self.ends.clear();
        self.end_gtids.clear();
        self.copy_last = None;
        match saved {
            Some((position, gtids)) => {
                self.saved = Some(position.clone());
                self.start = Some(position);
                self.gtids = gtids;
            }
            None => {
                let status = log_status(&mut conn).await?;
                let start = LogPosition {
                    file: status.file,
                    offset: status.offset,
                };
                self.gtids = status.gtids;
                self.gtids_changed = true;
                if self.settings.snapshot {
                    let (copy, progress) = self.begin_copy(&mut conn, &start).await?;
                    self.copy = Some(copy);
                    self.copy_tables = progress;
                } else {
                    // Nothing came before it: a restart may start here.
                    self.save(start.clone())?;
                }
                self.start = Some(start);
            }
        }
        let _ = conn.disconnect().await;
        self.committed = self.delivered;
        self.connected = true;
        self.epoch += 1;
        self.seq = 0;
        Ok(SourceEvent::Assigned {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
        })
    }

    /// Starts the copy: the tables' estimates, then a task that reads them in one snapshot on
    /// a connection of its own.
    async fn begin_copy(
        &self,
        conn: &mut Conn,
        start: &LogPosition,
    ) -> Result<(Copy, Vec<CopyTable>), ConnectorError> {
        let names = tables(conn, &self.settings).await?;
        let mut progress = Vec::with_capacity(names.len());
        for (db, table) in &names {
            let estimate: Option<Option<i64>> = conn
                .exec_first(
                    "SELECT table_rows FROM information_schema.tables \
                     WHERE table_schema = ? AND table_name = ?",
                    (db.clone(), table.clone()),
                )
                .await
                .unwrap_or(None);
            progress.push(CopyTable {
                table: format!("{db}.{table}"),
                estimate: estimate.flatten(),
                copied: 0,
                last: None,
            });
        }
        let estimate: i64 = progress.iter().filter_map(|t| t.estimate).sum();
        eprintln!(
            "MySQL stream source on {}: copying {} table(s), about {estimate} rows, then \
             following their changes from {}",
            self.settings.label(),
            names.len(),
            start.text()
        );
        let reader = connect(&self.settings).await?;
        let (send, rows) = tokio::sync::mpsc::channel(READ_AHEAD);
        let at = start.text();
        let task = tokio::spawn(async move {
            if let Err(error) = copy_tables(reader, &names, &at, &send).await {
                let _ = send.send(Err(error)).await;
            }
        });
        Ok((Copy { rows, task }, progress))
    }

    /// The copy reads the table at `index` now: the tables before it were read through the
    /// last position handed over.
    fn copy_reached(&mut self, index: usize) {
        let delivered = self.delivered;
        for table in self.copy_tables.iter_mut().take(index) {
            table.last.get_or_insert(delivered);
        }
    }

    async fn copy_step(&mut self) -> Result<SourceEvent, ConnectorError> {
        loop {
            if self.full() {
                return Ok(self.take()?.expect("full"));
            }
            let wait = self.wait();
            let copy = self.copy.as_mut().expect("copying");
            match tokio::time::timeout(wait, copy.rows.recv()).await {
                Ok(Some(Ok((index, value)))) => {
                    self.copy_reached(index);
                    if let Some(table) = self.copy_tables.get_mut(index) {
                        table.copied += 1;
                    }
                    self.push(value);
                }
                Ok(Some(Err(error))) => {
                    self.copy = None;
                    return Err(error);
                }
                Ok(None) => {
                    self.copy_reached(self.copy_tables.len());
                    let copied: i64 = self.copy_tables.iter().map(|t| t.copied).sum();
                    self.copy = None;
                    eprintln!(
                        "MySQL stream source on {}: copied {copied} rows; following changes",
                        self.settings.label()
                    );
                    if self.committed < self.delivered {
                        self.copy_last = Some(self.delivered);
                    } else {
                        let start = self.start.clone().expect("a start");
                        self.save(start)?;
                    }
                    return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
                }
                Err(_) => return Ok(self.take()?.unwrap_or(SourceEvent::Idle)),
            }
        }
    }

    async fn start_reader(&mut self) -> Result<(), ConnectorError> {
        let start = self.start.clone().expect("a start");
        let settings = self.settings.clone();
        let conn = connect(&settings).await?;
        let (send, changes) = tokio::sync::mpsc::channel(READ_AHEAD);
        self.read_through = Some(start.clone());
        let gtids = self.gtids.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = read_log(conn, settings, start, gtids, &send).await {
                let _ = send.send(Err(error)).await;
            }
        });
        self.reader = Some(Reader { changes, task });
        Ok(())
    }

    async fn change_step(&mut self) -> Result<SourceEvent, ConnectorError> {
        loop {
            if self.full() {
                return Ok(self.take()?.expect("full"));
            }
            if self.paused {
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
            }
            let wait = self.wait();
            let reader = self.reader.as_mut().expect("reading");
            match tokio::time::timeout(wait, reader.changes.recv()).await {
                Ok(Some(Ok(Change::Row(value)))) => self.push(value),
                Ok(Some(Ok(Change::End(position, gtid)))) => {
                    self.read_through = Some(position.clone());
                    // Nothing waits for the sinks: the end can be saved now.
                    if self.pending.is_none() && self.committed == self.delivered {
                        self.handled(gtid);
                        self.save(position)?;
                    } else {
                        self.ends.insert(self.delivered, position);
                        if let Some(gtid) = gtid {
                            self.end_gtids.entry(self.delivered).or_default().push(gtid);
                        }
                    }
                    self.settle_probe();
                }
                Ok(Some(Err(error))) => {
                    self.reader = None;
                    return Err(error);
                }
                Ok(None) => {
                    self.reader = None;
                    return Err(ConnectorError::retryable("MySQL: the log reader stopped"));
                }
                Err(_) => {
                    if self.pending.is_none() {
                        self.probe().await;
                    }
                    return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
                }
            }
        }
    }

    fn lose(&mut self) -> SourceEvent {
        self.connected = false;
        self.copy = None;
        self.copy_last = None;
        self.reader = None;
        self.pending = None;
        self.ends.clear();
        self.end_gtids.clear();
        self.probe = None;
        self.read_through = None;
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

    fn push(&mut self, value: Vec<u8>) {
        self.delivered += 1;
        let position = self.delivered;
        let envelope = Envelope {
            key: None,
            value,
            headers: Vec::new(),
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
    }
}

impl Source for MySqlCdcSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if !self.connected {
                return self.connect().await;
            }
            let step = if self.copy.is_some() {
                self.copy_step().await
            } else {
                if self.reader.is_none()
                    && let Err(error) = self.start_reader().await
                {
                    if error.retryable {
                        return Ok(self.lose());
                    }
                    return Err(error);
                }
                self.change_step().await
            };
            match step {
                Ok(event) => Ok(event),
                Err(error) if error.retryable => {
                    eprintln!("MySQL stream source on {}: {error}", self.settings.label());
                    Ok(self.lose())
                }
                Err(error) => Err(error),
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            for commit in commits {
                if commit.epoch != AssignmentEpoch(self.epoch) || !self.connected {
                    return CommitOutcome::Stale;
                }
                let Position::Offset(last) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "MySQL acknowledges change positions",
                    ));
                };
                self.committed = self.committed.max(last as u64);
            }
            // Every copied row is in the sinks: the log is read from the copy's start, saved.
            if let Some(last) = self.copy_last
                && self.copy.is_none()
                && self.committed >= last
            {
                let start = self.start.clone().expect("a start");
                if let Err(error) = self.save(start) {
                    return CommitOutcome::Failed(error);
                }
                self.copy_last = None;
            }
            if self.copy_last.is_none() {
                let later = self.end_gtids.split_off(&(self.committed + 1));
                let done = std::mem::replace(&mut self.end_gtids, later);
                self.handled(done.into_values().flatten());
                let later = self.ends.split_off(&(self.committed + 1));
                let done = std::mem::replace(&mut self.ends, later);
                if let Some((_, end)) = done.into_iter().next_back()
                    && let Err(error) = self.save(end)
                {
                    return CommitOutcome::Failed(error);
                }
            }
            self.settle_probe();
            CommitOutcome::Committed
        })
    }

    fn pause(&mut self, _partition: PartitionId) {
        self.paused = true;
    }

    fn resume(&mut self, _partition: PartitionId) {
        self.paused = false;
    }

    /// While the initial copy runs: the rows left by the tables' estimates.
    fn lag(&self) -> BTreeMap<u32, i64> {
        if self.copy.is_none() {
            return BTreeMap::new();
        }
        let left = self
            .copy_tables
            .iter()
            .map(|t| (t.estimate.unwrap_or(0) - t.copied).max(0))
            .sum();
        BTreeMap::from([(PARTITION.0, left)])
    }

    fn caught_up(&self) -> Option<i64> {
        self.caught_up
    }

    fn copy_progress(&self) -> Vec<TableCopy> {
        self.copy_tables
            .iter()
            .map(|t| TableCopy {
                table: t.table.clone(),
                estimate: t.estimate,
                copied: t.copied,
                done: t.last.is_some_and(|last| self.committed >= last),
            })
            .collect()
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            replayable: true,
            ordered: true,
            ack_kind: AckKind::Lsn,
            // One reader keeps one position.
            max_instances: Some(1),
            needs_keepalive: false,
            durable_positions: false,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.reader = None;
            self.copy = None;
            self.connected = false;
        })
    }
}

/// Each table's rows as messages (`_mage_operation: snapshot`), in one consistent snapshot.
async fn copy_tables(
    mut conn: Conn,
    tables: &[(String, String)],
    at: &str,
    send: &tokio::sync::mpsc::Sender<Result<(usize, Vec<u8>), ConnectorError>>,
) -> Result<(), ConnectorError> {
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        .await
        .map_err(|e| classify("starting the initial copy", e))?;
    for (index, (db, table)) in tables.iter().enumerate() {
        let doing = format!("copying {db}.{table}");
        let mut result = conn
            .query_iter(format!("SELECT * FROM {}.{}", quote(db), quote(table)))
            .await
            .map_err(|e| classify(&doing, e))?;
        let columns = result.columns().map(|c| c.to_vec()).unwrap_or_default();
        let keys: Vec<Vec<u8>> = columns.iter().map(|c| key(&c.name_str())).collect();
        let kinds: Vec<Kind> = columns.iter().map(kind_of).collect();
        let suffix = suffix(b"snapshot", db, table, at, None);
        let mut rows = result
            .stream::<mysql_async::Row>()
            .await
            .map_err(|e| classify(&doing, e))?
            .ok_or_else(|| ConnectorError::fatal(format!("MySQL {doing}: no result")))?;
        while let Some(row) = rows.next().await {
            let row = row.map_err(|e| classify(&doing, e))?;
            let mut out = Vec::with_capacity(256);
            out.push(b'{');
            for (i, (key, kind)) in keys.iter().zip(&kinds).enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(key);
                match row.as_ref(i) {
                    Some(mysql_async::Value::Bytes(bytes)) => {
                        write_text(&mut out, kind, Some(bytes))
                    }
                    Some(mysql_async::Value::NULL) | None => write_text(&mut out, kind, None),
                    Some(other) => {
                        let text = other.as_sql(true);
                        write_text(&mut out, kind, Some(text.trim_matches('\'').as_bytes()))
                    }
                }
            }
            out.extend_from_slice(&suffix);
            if send.send(Ok((index, out))).await.is_err() {
                // The stream stopped reading.
                return Ok(());
            }
        }
        drop(rows);
    }
    conn.query_drop("COMMIT")
        .await
        .map_err(|e| classify("ending the initial copy", e))?;
    let _ = conn.disconnect().await;
    Ok(())
}

/// `"name":`, written before a value.
fn key(name: &str) -> Vec<u8> {
    let mut key = serde_json::to_vec(name).unwrap_or_default();
    key.push(b':');
    key
}

/// The change capture's own fields, closing a message's object.
fn suffix(operation: &[u8], db: &str, table: &str, at: &str, time: Option<&str>) -> Vec<u8> {
    let mut out = b",\"_mage_operation\":\"".to_vec();
    out.extend_from_slice(operation);
    out.extend_from_slice(b"\",\"_mage_schema\":");
    out.extend(serde_json::to_vec(db).unwrap_or_default());
    out.extend_from_slice(b",\"_mage_table\":");
    out.extend(serde_json::to_vec(table).unwrap_or_default());
    out.extend_from_slice(b",\"_mage_binlog\":");
    out.extend(serde_json::to_vec(at).unwrap_or_default());
    if let Some(time) = time {
        out.extend_from_slice(b",\"_mage_commit_time\":\"");
        out.extend_from_slice(time.as_bytes());
        out.push(b'"');
        if operation == b"delete" {
            out.extend_from_slice(b",\"_mage_deleted_at\":\"");
            out.extend_from_slice(time.as_bytes());
            out.push(b'"');
        }
    }
    out.push(b'}');
    out
}

fn commit_time(micros: i64) -> String {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|at| at.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string())
        .unwrap_or_default()
}

/// A table the log names, with what its rows need: whether the stream reads it, its ENUM and
/// SET columns' names (from the table map's metadata), in column order.
struct Table {
    db: String,
    name: String,
    selected: bool,
    enums: Vec<Vec<String>>,
    sets: Vec<Vec<String>>,
}

/// The tables whose changes the log is read for: the named ones, or every table of the
/// database, one made after the stream started too (a new table's rows all come in the log).
struct Selection {
    named: Vec<Name>,
    database: Option<String>,
}

impl Selection {
    async fn new(conn: &mut Conn, settings: &Settings) -> Result<Selection, ConnectorError> {
        Ok(Selection {
            named: tables(conn, settings).await?,
            database: settings
                .tables
                .is_empty()
                .then(|| settings.database.clone()),
        })
    }

    fn has(&self, db: &str, table: &str) -> bool {
        match &self.database {
            Some(database) => database == db,
            None => self.named.iter().any(|(d, t)| d == db && t == table),
        }
    }

    /// What to say of a statement that changes a table read without row changes.
    fn warning(&self, change: &Ddl) -> Option<String> {
        let text = |(db, table): &Name| format!("{db}.{table}");
        match change {
            Ddl::Truncate(table) if self.has(&table.0, &table.1) => Some(format!(
                "TRUNCATE of {} is not replicated; truncate the destination too.",
                text(table)
            )),
            Ddl::Drop(table) if self.has(&table.0, &table.1) => Some(format!(
                "DROP TABLE of {} is not replicated; the destination keeps its rows.",
                text(table)
            )),
            Ddl::DropDatabase(db)
                if self.database.as_deref() == Some(db.as_str())
                    || self.named.iter().any(|(d, _)| d == db) =>
            {
                Some(format!(
                    "DROP DATABASE {db} is not replicated; the destination keeps its rows."
                ))
            }
            Ddl::Rename(from, to) => match (self.has(&from.0, &from.1), self.has(&to.0, &to.1)) {
                (true, false) => Some(format!(
                    "{} was renamed to {}, which the stream does not read: its changes from \
                     now on are not read. Add {} to the source's tables to read them.",
                    text(from),
                    text(to),
                    text(to)
                )),
                (false, true) => Some(format!(
                    "{} was renamed to {}, which the stream reads: its rows from before were \
                     not copied, only its changes from now on are read. Delete the stream's \
                     state to copy the tables again.",
                    text(from),
                    text(to)
                )),
                (true, true) => Some(format!(
                    "{} was renamed to {}: its changes from now on come as {}'s; the rows \
                     the destination has stay under {}.",
                    text(from),
                    text(to),
                    text(to),
                    text(from)
                )),
                (false, false) => None,
            },
            _ => None,
        }
    }
}

/// The binary log from `start`, as changes and transaction ends.
async fn read_log(
    mut conn: Conn,
    settings: Arc<Settings>,
    start: LogPosition,
    gtids: Option<GtidSet>,
    send: &tokio::sync::mpsc::Sender<Result<Change, ConnectorError>>,
) -> Result<(), ConnectorError> {
    let wanted = Selection::new(&mut conn, &settings).await?;
    // The server sends a heartbeat when the log is quiet (nanoseconds).
    conn.query_drop(format!(
        "SET @master_heartbeat_period = {}",
        HEARTBEAT.as_nanos()
    ))
    .await
    .map_err(|e| classify("asking for heartbeats", e))?;
    let server_id = settings.server_id.unwrap_or_else(random_server_id);
    // With GTIDs the server sends what the set lacks, on whichever server it is now.
    let request = match &gtids {
        Some(set) => BinlogStreamRequest::new(server_id)
            .with_gtid()
            .with_gtid_set(set.sids().map_err(ConnectorError::fatal)?),
        None => BinlogStreamRequest::new(server_id)
            .with_filename(start.file.as_bytes())
            .with_pos(start.offset),
    };
    let from = match &gtids {
        Some(set) if !set.is_empty() => format!("the GTIDs after {}", set.text()),
        _ => start.text(),
    };
    let mut stream = conn.get_binlog_stream(request).await.map_err(|e| {
        let error = classify(&format!("reading the binary log from {from}"), e);
        if error.message.contains("1236") || error.message.contains("purged") {
            ConnectorError::fatal(format!(
                "{} The saved position is no longer in the server's binary log (it keeps \
                     binlog_expire_logs_seconds of it), or the server's GTIDs are of another \
                     topology: delete the stream's state to copy the tables again.",
                error.message
            ))
        } else {
            error
        }
    })?;
    eprintln!(
        "MySQL stream source on {}: reading changes from {from} (replica id {server_id})",
        settings.label(),
    );
    let mut file = start.file.clone();
    let mut known: HashMap<u64, Table> = HashMap::new();
    let mut commit_micros: Option<i64> = None;
    // The GTID of the transaction being read, with GTIDs on.
    let mut transaction: Option<Gtid> = None;
    loop {
        let event = match tokio::time::timeout(HEARTBEAT * 3, stream.next()).await {
            Err(_) => {
                return Err(ConnectorError::retryable(
                    "MySQL: no change and no heartbeat for 90 seconds; reconnecting",
                ));
            }
            Ok(None) => return Err(ConnectorError::retryable("MySQL closed the binary log")),
            Ok(Some(Err(error))) => return Err(classify("reading the binary log", error)),
            Ok(Some(Ok(event))) => event,
        };
        let header = event.header();
        let end = u64::from(header.log_pos());
        let data = event
            .read_data()
            .map_err(|e| ConnectorError::fatal(format!("MySQL: a log event: {e}")))?;
        let mut found: Vec<Change> = Vec::new();
        match data {
            Some(EventData::RotateEvent(rotate)) => {
                file = rotate.name().into_owned();
            }
            Some(EventData::GtidEvent(gtid)) => {
                commit_micros =
                    Some(gtid.immediate_commit_timestamp() as i64).filter(|micros| *micros > 0);
                transaction = Some((uuid(gtid.sid()), gtid.gno()));
            }
            Some(EventData::AnonymousGtidEvent(gtid)) => {
                commit_micros =
                    Some(gtid.0.immediate_commit_timestamp() as i64).filter(|micros| *micros > 0);
                transaction = None;
            }
            Some(EventData::QueryEvent(query)) => {
                for change in ddl::read(&query.query(), &query.schema()) {
                    if let Some(warning) = wanted.warning(&change) {
                        eprintln!(
                            "warning: MySQL stream source on {}: {warning}",
                            settings.label()
                        );
                    }
                }
                if !query.query_raw().eq_ignore_ascii_case(b"BEGIN") && end > 0 {
                    // A statement of its own (DDL), or a COMMIT outside InnoDB.
                    found.push(Change::End(
                        LogPosition {
                            file: file.clone(),
                            offset: end,
                        },
                        transaction.take(),
                    ));
                }
            }
            Some(EventData::XidEvent(_)) => {
                if end > 0 {
                    found.push(Change::End(
                        LogPosition {
                            file: file.clone(),
                            offset: end,
                        },
                        transaction.take(),
                    ));
                }
            }
            Some(EventData::TableMapEvent(map)) => {
                let db = map.database_name().into_owned();
                let name = map.table_name().into_owned();
                let selected = wanted.has(&db, &name);
                let mut enums = Vec::new();
                let mut sets = Vec::new();
                if selected {
                    for field in map.iter_optional_meta() {
                        match field {
                            Ok(OptionalMetadataField::EnumStrValue(values)) => {
                                for column in values.iter_values().flatten() {
                                    enums.push(
                                        column
                                            .values()
                                            .iter()
                                            .map(|v| v.value().into_owned())
                                            .collect(),
                                    );
                                }
                            }
                            Ok(OptionalMetadataField::SetStrValue(values)) => {
                                for column in values.iter_values().flatten() {
                                    sets.push(
                                        column
                                            .values()
                                            .iter()
                                            .map(|v| v.value().into_owned())
                                            .collect(),
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                }
                known.insert(
                    map.table_id(),
                    Table {
                        db,
                        name,
                        selected,
                        enums,
                        sets,
                    },
                );
            }
            Some(EventData::RowsEvent(rows)) => {
                let Some(table) = known.get(&rows.table_id()).filter(|t| t.selected) else {
                    continue;
                };
                if matches!(rows, RowsEventData::PartialUpdateRowsEvent(_)) {
                    return Err(ConnectorError::fatal(
                        "MySQL wrote a partial JSON update (binlog_row_value_options=\
                         PARTIAL_JSON): set binlog_row_value_options to ''",
                    ));
                }
                let Some(map) = stream.get_tme(rows.table_id()) else {
                    return Err(ConnectorError::fatal(format!(
                        "MySQL: a change of {}.{} came without its table map",
                        table.db, table.name
                    )));
                };
                let micros =
                    commit_micros.unwrap_or_else(|| i64::from(header.timestamp()) * 1_000_000);
                let time = commit_time(micros);
                let at = format!("{file}:{end}");
                let mut kinds: Option<Vec<Kind>> = None;
                for row in rows.rows(map) {
                    let (before, after) = row
                        .map_err(|e| ConnectorError::fatal(format!("MySQL: a changed row: {e}")))?;
                    let columns_of = before.as_ref().or(after.as_ref()).expect("a row image");
                    let kinds = kinds.get_or_insert_with(|| column_kinds(columns_of, table));
                    let message = |row: &BinlogRow, operation: &[u8]| {
                        row_message(row, kinds, operation, table, &at, &time)
                    };
                    match (&rows, before, after) {
                        (
                            RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_),
                            _,
                            Some(after),
                        ) => {
                            found.push(Change::Row(message(&after, b"insert")?));
                        }
                        (
                            RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_),
                            Some(before),
                            _,
                        ) => {
                            found.push(Change::Row(message(&before, b"delete")?));
                        }
                        (_, Some(before), Some(after)) => {
                            // A changed primary key: the old key goes first.
                            if key_changed(&before, &after, kinds)? {
                                found.push(Change::Row(message(&before, b"delete")?));
                            }
                            found.push(Change::Row(message(&after, b"update")?));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        for change in found {
            if send.send(Ok(change)).await.is_err() {
                return Ok(());
            }
        }
    }
}

/// A replica id for this connection: one the server's other replicas are unlikely to use.
fn random_server_id() -> u32 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (1 << 30) | ((nanos ^ std::process::id().rotate_left(16)) & ((1 << 30) - 1))
}

/// Each column's kind, ENUM and SET columns with their names.
fn column_kinds(row: &BinlogRow, table: &Table) -> Vec<Kind> {
    let (mut enums, mut sets) = (table.enums.iter(), table.sets.iter());
    row.columns_ref()
        .iter()
        .map(|column| match column.column_type() {
            ColumnType::MYSQL_TYPE_ENUM => Kind::Enum(enums.next().cloned().unwrap_or_default()),
            ColumnType::MYSQL_TYPE_SET => Kind::Set(sets.next().cloned().unwrap_or_default()),
            _ => kind_of(column),
        })
        .collect()
}

fn row_message(
    row: &BinlogRow,
    kinds: &[Kind],
    operation: &[u8],
    table: &Table,
    at: &str,
    time: &str,
) -> Result<Vec<u8>, ConnectorError> {
    let mut out = Vec::with_capacity(256);
    out.push(b'{');
    for (i, column) in row.columns_ref().iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend(key(&column.name_str()));
        write_binlog(&mut out, &kinds[i], row.as_ref(i)).map_err(|e| {
            ConnectorError::fatal(format!(
                "MySQL {}.{} column {}: {e}",
                table.db,
                table.name,
                column.name_str()
            ))
        })?;
    }
    out.extend(suffix(operation, &table.db, &table.name, at, Some(time)));
    Ok(out)
}

fn key_changed(
    before: &BinlogRow,
    after: &BinlogRow,
    kinds: &[Kind],
) -> Result<bool, ConnectorError> {
    for (i, column) in before.columns_ref().iter().enumerate() {
        if !column.flags().contains(ColumnFlags::PRI_KEY_FLAG) {
            continue;
        }
        let (mut a, mut b) = (Vec::new(), Vec::new());
        write_binlog(&mut a, &kinds[i], before.as_ref(i)).map_err(ConnectorError::fatal)?;
        write_binlog(&mut b, &kinds[i], after.as_ref(i)).map_err(ConnectorError::fatal)?;
        if a != b {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use mage_stream_ports::read_envelope;

    use super::*;

    #[derive(Default)]
    struct Saved(Mutex<Option<String>>);

    impl PositionRecord for Saved {
        fn load(&self) -> Result<Option<String>, ConnectorError> {
            Ok(self.0.lock().unwrap().clone())
        }

        fn save(&self, position: &str) -> Result<(), ConnectorError> {
            *self.0.lock().unwrap() = Some(position.to_string());
            Ok(())
        }
    }

    #[test]
    fn a_saved_position_belongs_to_its_server() {
        let at = LogPosition {
            file: "binlog.000003".into(),
            offset: 4567,
        };
        let saved = at.saved("db:3306", None);
        assert_eq!(
            LogPosition::read_saved(&saved, "db:3306"),
            Some((at.clone(), None))
        );
        assert_eq!(LogPosition::read_saved(&saved, "other:3306"), None);
        assert_eq!(LogPosition::read_saved("not json", "db:3306"), None);
        // With GTIDs on, the transactions handled are saved with it.
        let gtids = GtidSet::parse("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42").unwrap();
        let saved = at.saved("db:3306", Some(&gtids));
        assert_eq!(
            LogPosition::read_saved(&saved, "db:3306"),
            Some((at, Some(gtids)))
        );
    }

    #[test]
    fn positions_order_by_file_sequence_then_offset() {
        let at = |file: &str, offset| LogPosition {
            file: file.into(),
            offset,
        };
        assert!(at("binlog.000010", 4).order() > at("binlog.000009", 99_999).order());
        assert!(at("binlog.000010", 500).order() > at("binlog.000010", 400).order());
    }

    #[test]
    fn a_message_ends_with_the_change_captures_fields() {
        let ended = String::from_utf8(suffix(
            b"delete",
            "shop",
            "orders",
            "binlog.000003:4567",
            Some("2026-11-28T10:00:00.000000+00:00"),
        ))
        .unwrap();
        assert_eq!(
            ended,
            ",\"_mage_operation\":\"delete\",\"_mage_schema\":\"shop\",\"_mage_table\":\"orders\",\
             \"_mage_binlog\":\"binlog.000003:4567\",\"_mage_commit_time\":\
             \"2026-11-28T10:00:00.000000+00:00\",\"_mage_deleted_at\":\
             \"2026-11-28T10:00:00.000000+00:00\"}"
        );
    }

    #[test]
    fn ddl_on_a_table_read_is_named_in_a_warning() {
        let name = |db: &str, table: &str| (db.to_string(), table.to_string());
        let listed = Selection {
            named: vec![name("shop", "orders")],
            database: None,
        };
        let warning = |selection: &Selection, statement: &str| {
            ddl::read(statement, "shop")
                .iter()
                .filter_map(|change| selection.warning(change))
                .collect::<Vec<_>>()
                .join(" | ")
        };
        assert_eq!(
            warning(&listed, "TRUNCATE orders"),
            "TRUNCATE of shop.orders is not replicated; truncate the destination too."
        );
        assert!(warning(&listed, "DROP TABLE `orders`").starts_with("DROP TABLE of shop.orders"));
        assert!(warning(&listed, "DROP DATABASE shop").starts_with("DROP DATABASE shop"));
        assert!(
            warning(&listed, "RENAME TABLE orders TO old")
                .contains("Add shop.old to the source's tables")
        );
        // A swap: the listed name now has another table's rows, never copied.
        let swap = warning(&listed, "RENAME TABLE orders TO old, new TO orders");
        assert!(
            swap.contains("its rows from before were not copied"),
            "{swap}"
        );
        assert_eq!(warning(&listed, "TRUNCATE other"), "");
        assert_eq!(warning(&listed, "DROP DATABASE audit"), "");

        // All of a database: a rename inside it moves the changes to the new name.
        let all = Selection {
            named: vec![],
            database: Some("shop".into()),
        };
        assert!(all.has("shop", "made_later"));
        assert!(!all.has("audit", "events"));
        assert!(
            warning(&all, "ALTER TABLE orders RENAME TO old")
                .contains("its changes from now on come as shop.old's")
        );
    }

    /// Each message, with its text as written under `_raw` (parsing loses a number's
    /// trailing zeros).
    fn messages(batch: &mage_stream_ports::RecordBatch) -> Vec<Value> {
        read_envelope(batch)
            .unwrap()
            .into_iter()
            .map(|e| {
                let mut value: Value = serde_json::from_slice(&e.value).unwrap();
                value["_raw"] = String::from_utf8(e.value).unwrap().into();
                value
            })
            .collect()
    }

    async fn sql(conn: &mut Conn, statements: &str) {
        conn.query_drop(statements).await.unwrap();
    }

    /// Reads batches until `count` messages came, acknowledging each.
    async fn read(source: &mut MySqlCdcSource, count: usize) -> Vec<Value> {
        let mut found = Vec::new();
        let until = Instant::now() + Duration::from_secs(30);
        while found.len() < count && Instant::now() < until {
            if let SourceEvent::Batch { batch, meta } = source.next_event().await.unwrap() {
                found.extend(messages(&batch));
                let commits = [Commit {
                    partition: PARTITION,
                    epoch: meta.id.epoch,
                    position: meta.last.clone(),
                }];
                assert_eq!(source.commit(&commits).await, CommitOutcome::Committed);
            }
        }
        found
    }

    /// I-595: with no tables named, a table made after the stream started is read from its
    /// first row (it was read only after a restart, without the rows before it).
    #[tokio::test]
    async fn a_table_made_after_the_start_is_read_when_the_whole_database_is() {
        let Some(mut config) = crate::tests::server() else {
            eprintln!("MAGE_TEST_MYSQL_URL is not set: skipped");
            return;
        };
        let mut admin = connect(&Settings::new(&config).unwrap()).await.unwrap();
        if !LogSettings::read(&mut admin)
            .await
            .unwrap()
            .problems()
            .is_empty()
        {
            eprintln!("the test server is not ready for change capture: skipped");
            return;
        }
        let database = format!("cdc_all_{}", std::process::id());
        sql(
            &mut admin,
            &format!(
                "DROP DATABASE IF EXISTS {database}; CREATE DATABASE {database}; \
                 CREATE TABLE {database}.first (id INT PRIMARY KEY)"
            ),
        )
        .await;
        config["database"] = database.clone().into();
        config["snapshot"] = "never".into();
        let limits = BatchLimits {
            max_records: 100,
            max_bytes: 1 << 20,
            max_wait: Duration::from_millis(200),
        };
        let mut source = MySqlCdcSource::new(&config, limits).unwrap();
        assert!(matches!(
            source.next_event().await.unwrap(),
            SourceEvent::Assigned { .. }
        ));
        // The reader starts with the first ask after Assigned, and lists the tables then:
        // let it start before the new table is made.
        let until = Instant::now() + Duration::from_millis(1500);
        while Instant::now() < until {
            assert!(matches!(
                source.next_event().await.unwrap(),
                SourceEvent::Idle
            ));
        }
        sql(
            &mut admin,
            &format!(
                "INSERT INTO {database}.first VALUES (1); \
                 CREATE TABLE {database}.later (id INT PRIMARY KEY, item VARCHAR(10)); \
                 INSERT INTO {database}.later VALUES (1, 'tea'), (2, 'cake'); \
                 TRUNCATE {database}.first"
            ),
        )
        .await;
        let found = read(&mut source, 3).await;
        let tables: Vec<(String, i64)> = found
            .iter()
            .map(|m| {
                (
                    m["_mage_table"].as_str().unwrap().to_string(),
                    m["id"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            tables,
            [
                ("first".into(), 1),
                ("later".into(), 1),
                ("later".into(), 2)
            ]
        );
        assert_eq!(found[1]["item"], "tea");
        source.close().await;
        sql(&mut admin, &format!("DROP DATABASE {database}")).await;
    }

    #[tokio::test]
    async fn a_copy_then_its_changes_then_a_restart_from_the_saved_position() {
        let Some(mut config) = crate::tests::server() else {
            eprintln!("MAGE_TEST_MYSQL_URL is not set: skipped");
            return;
        };
        let settings = Settings::new(&config).unwrap();
        let mut admin = connect(&settings).await.unwrap();
        let log = LogSettings::read(&mut admin).await.unwrap();
        if !log.problems().is_empty() {
            eprintln!("the test server is not ready for change capture: skipped");
            return;
        }
        let id = std::process::id();
        let table = format!("cdc_orders_{id}");
        let other = format!("cdc_other_{id}");
        sql(
            &mut admin,
            &format!(
                "DROP TABLE IF EXISTS {table}, {other}; \
                 CREATE TABLE {table} (id INT PRIMARY KEY, item VARCHAR(20), price DECIMAL(8,2), \
                   size ENUM('small','large'), tags SET('hot','new'), at DATETIME(3), \
                   paid TIMESTAMP(6) NULL, doc JSON, raw VARBINARY(4)); \
                 CREATE TABLE {other} (id INT PRIMARY KEY); \
                 INSERT INTO {table} VALUES \
                   (1, 'tea', 2.50, 'small', 'hot,new', '2026-11-28 10:00:00.120', \
                    '2026-11-28 10:00:00.250000', '{{\"a\": [1, 2]}}', 0x0001), \
                   (2, 'cake', 4.00, 'large', '', NULL, NULL, NULL, NULL)"
            ),
        )
        .await;
        config["tables"] = serde_json::json!([table.clone()]);
        config["snapshot"] = "initial".into();
        let saved = Arc::new(Saved::default());
        let limits = BatchLimits {
            max_records: 100,
            max_bytes: 1 << 20,
            max_wait: Duration::from_millis(200),
        };
        let mut source = MySqlCdcSource::new(&config, limits)
            .unwrap()
            .with_position_record(saved.clone());

        assert!(matches!(
            source.next_event().await.unwrap(),
            SourceEvent::Assigned { .. }
        ));
        let copied = read(&mut source, 2).await;
        assert_eq!(copied.len(), 2);
        let first = &copied[0];
        assert_eq!(first["_mage_operation"], "snapshot");
        assert_eq!(first["item"], "tea");
        assert!(
            first["_raw"].as_str().unwrap().contains("\"price\":2.50,"),
            "{first}"
        );
        assert_eq!(first["size"], "small");
        assert_eq!(first["tags"], "hot,new");
        assert_eq!(first["at"], "2026-11-28T10:00:00.12");
        assert_eq!(first["paid"], "2026-11-28T10:00:00.25+00:00");
        assert_eq!(first["doc"], serde_json::json!({"a": [1, 2]}));
        assert_eq!(first["raw"], "AAE=");
        assert_eq!(copied[1]["tags"], "");
        assert!(copied[1]["at"].is_null());

        // Changes after the copy, a table the stream does not read, and a changed key.
        sql(
            &mut admin,
            &format!(
                "INSERT INTO {other} VALUES (1); \
                 INSERT INTO {table} (id, item, price, size, tags, at, paid, doc, raw) VALUES \
                   (3, 'pie', 3.10, 'large', 'new', '2026-11-28 11:00:00', \
                    '2026-11-28 10:00:00.250000', '[true]', 0x02); \
                 UPDATE {table} SET item = 'green tea' WHERE id = 1; \
                 UPDATE {table} SET id = 20 WHERE id = 2; \
                 DELETE FROM {table} WHERE id = 3"
            ),
        )
        .await;
        let changes = read(&mut source, 5).await;
        let summary: Vec<(String, i64)> = changes
            .iter()
            .map(|m| {
                (
                    m["_mage_operation"].as_str().unwrap().to_string(),
                    m["id"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                ("insert".into(), 3),
                ("update".into(), 1),
                ("delete".into(), 2),
                ("update".into(), 20),
                ("delete".into(), 3)
            ]
        );
        // The same values from the log as from the copy.
        let pie = &changes[0];
        assert!(
            pie["_raw"].as_str().unwrap().contains("\"price\":3.10,"),
            "{pie}"
        );
        assert_eq!(pie["size"], "large");
        assert_eq!(pie["tags"], "new");
        assert_eq!(pie["at"], "2026-11-28T11:00:00");
        assert_eq!(pie["paid"], "2026-11-28T10:00:00.25+00:00");
        assert_eq!(pie["doc"], serde_json::json!([true]));
        assert_eq!(pie["raw"], "Ag==");
        assert_eq!(changes[1]["item"], "green tea");
        assert!(changes[4]["_mage_deleted_at"].is_string());
        assert!(changes[0]["_mage_binlog"].as_str().unwrap().contains(':'));
        // Everything was acknowledged: the last transaction's end is saved.
        let saved_at = saved.0.lock().unwrap().clone().expect("a saved position");
        source.close().await;

        // A restart reads from the saved position: only what came after.
        sql(
            &mut admin,
            &format!("UPDATE {table} SET item = 'apple pie' WHERE id = 1"),
        )
        .await;
        let mut restarted = MySqlCdcSource::new(&config, limits)
            .unwrap()
            .with_position_record(saved.clone());
        restarted.next_event().await.unwrap();
        assert!(restarted.copy.is_none(), "a saved position copies nothing");
        let again = read(&mut restarted, 1).await;
        assert_eq!(again.len(), 1, "{again:?}");
        assert_eq!(again[0]["item"], "apple pie");
        assert_ne!(saved.0.lock().unwrap().clone().unwrap(), saved_at);
        // Nothing more to read and everything acknowledged: caught up through about now.
        let until = Instant::now() + Duration::from_secs(15);
        while restarted.caught_up().is_none() && Instant::now() < until {
            restarted.next_event().await.unwrap();
        }
        let now = chrono::Utc::now().timestamp_micros();
        let through = restarted.caught_up().expect("caught up within 15 s");
        assert!(
            through <= now && through > now - 20_000_000,
            "{through} {now}"
        );
        restarted.close().await;

        // With GTIDs on, the transactions handled are saved, and a restart reads by them: a
        // file and offset mean nothing on another server after a failover; a GTID set does.
        let gtid_mode: Option<String> = admin.query_first("SELECT @@gtid_mode").await.unwrap();
        if gtid_mode.as_deref() == Some("ON") {
            let text = saved.0.lock().unwrap().clone().unwrap();
            let mut position: Value = serde_json::from_str(&text).unwrap();
            assert!(
                position["gtids"].as_str().is_some_and(|g| !g.is_empty()),
                "{text}"
            );
            position["file"] = "binlog.999999".into();
            position["offset"] = 4.into();
            *saved.0.lock().unwrap() = Some(position.to_string());
            sql(
                &mut admin,
                &format!("UPDATE {table} SET item = 'after failover' WHERE id = 1"),
            )
            .await;
            let mut moved = MySqlCdcSource::new(&config, limits)
                .unwrap()
                .with_position_record(saved.clone());
            moved.next_event().await.unwrap();
            let after = read(&mut moved, 1).await;
            assert_eq!(after.len(), 1, "{after:?}");
            assert_eq!(after[0]["item"], "after failover");
            moved.close().await;
        } else {
            eprintln!("gtid_mode is not ON: the GTID restart is not tested here");
        }
        sql(&mut admin, &format!("DROP TABLE {table}, {other}")).await;
    }
}
