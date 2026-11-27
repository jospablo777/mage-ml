//! The ports of stream processing: what a source, a sink and a transform session do, in
//! terms of Arrow record batches with a fixed envelope (ADR-0019).
//!
//! Connectors are chosen at run time from the manifest, so the traits are object safe: their
//! methods return boxed futures. Implementations live in connector crates; the engine drives
//! them with the state machine of `mage-stream-core`.

pub mod check;
mod envelope;

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub mod sql;
mod sql_reserved;

pub use arrow_array::RecordBatch;
pub use envelope::{
    Envelope, EnvelopeBuilder, Header, TIMER_TICK_HEADER, envelope_schema, is_timer_tick,
    read_envelope, timer_tick,
};
pub use mage_stream_core::{AssignmentEpoch, BatchMeta, Commit, PartitionId, Position};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An error of a connector; `retryable` says whether trying again can succeed (an outage) or
/// not (invalid settings, a rejected message).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorError {
    pub message: String,
    pub retryable: bool,
}

impl ConnectorError {
    pub fn retryable(message: impl Into<String>) -> ConnectorError {
        ConnectorError {
            message: message.into(),
            retryable: true,
        }
    }

    pub fn fatal(message: impl Into<String>) -> ConnectorError {
        ConnectorError {
            message: message.into(),
            retryable: false,
        }
    }
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConnectorError {}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceEvent {
    /// A batch read from one partition, in the envelope schema.
    Batch { batch: RecordBatch, meta: BatchMeta },
    Assigned {
        partition: PartitionId,
        epoch: AssignmentEpoch,
    },
    /// The partition is about to go; acknowledge what is done within `within`.
    Revoking {
        partition: PartitionId,
        epoch: AssignmentEpoch,
        within: Duration,
    },
    /// Nothing arrived within the batch wait; lets the driver tick.
    Idle,
    /// A finite source has nothing more (tests, replays).
    End,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed,
    /// The positions belong to an epoch the source no longer holds.
    Stale,
    Failed(ConnectorError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckKind {
    /// Kafka: an offset per partition.
    Offset,
    /// RabbitMQ, NATS: each message or a cumulative tag.
    Message,
    /// PostgreSQL: a log sequence number confirmed to the server.
    Lsn,
    /// HTTP: the pending responses are answered.
    Response,
    /// Nothing to acknowledge (tests).
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCapabilities {
    /// A restart reads unacknowledged messages again.
    pub replayable: bool,
    /// Messages of one partition arrive in order.
    pub ordered: bool,
    pub ack_kind: AckKind,
    /// How many instances of a stream may read at once; None when unlimited.
    pub max_instances: Option<u32>,
    /// The connection must be kept alive while the stream is paused.
    pub needs_keepalive: bool,
    /// A message keeps its position after a restart (Kafka offsets), so sinks may name
    /// objects and deduplicate by position across runs. Sources that number what they
    /// deliver from 1 again at each start must not claim it: a sink would take a new
    /// message for one it already wrote.
    pub durable_positions: bool,
}

pub trait Source: Send {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>>;
    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome>;
    fn pause(&mut self, partition: PartitionId);
    fn resume(&mut self, partition: PartitionId);
    /// The stream finished with a partition it was revoking ([`SourceEvent::Revoking`]):
    /// what was done is acknowledged, and the source may hand the partition over.
    fn revoked(&mut self, _partition: PartitionId, _epoch: AssignmentEpoch) {}
    /// The stream stopped on an error that needs a person; nothing more is acknowledged.
    fn failed(&mut self, _reason: &str) {}
    /// Messages behind the end of each partition this instance reads, where the source
    /// knows it.
    fn lag(&self) -> std::collections::BTreeMap<u32, i64> {
        std::collections::BTreeMap::new()
    }
    /// The source's own clock (Unix microseconds) through which every change it has was
    /// read and acknowledged by every sink, where the source can tell (a change capture
    /// source with nothing left to read). The stream hands it to the sinks on idle ticks.
    fn caught_up(&self) -> Option<i64> {
        None
    }
    fn capabilities(&self) -> SourceCapabilities;
    fn close(&mut self) -> BoxFuture<'_, ()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SinkCapabilities {
    /// Writes of the same rows twice are absorbed (keys or dedupe ids).
    pub idempotent: bool,
    pub transactional: bool,
}

/// A batch's confirmation by its target, after [`Sink::queue`].
pub type Confirmation = BoxFuture<'static, Result<(), ConnectorError>>;

pub trait Sink: Send + Sync {
    /// Writes the batch and returns once the target confirmed it (the receipt).
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>>;
    /// Hands the batch to the target in order and returns its confirmation, so the writer
    /// queues the next batch while this one is confirmed. A sink whose target keeps the order
    /// of what it was handed (a Kafka producer) splits the two; the default writes the batch
    /// and is confirmed at once.
    fn queue<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<Confirmation, ConnectorError>> {
        Box::pin(async move {
            self.write(batch, meta).await?;
            Ok(Box::pin(async { Ok(()) }) as Confirmation)
        })
    }
    fn flush(&self) -> BoxFuture<'_, ()>;
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::default()
    }
    /// Every change the source made through `through` (Unix microseconds, the source's
    /// clock) is written ([`Source::caught_up`]): a sink that keeps how far it is (a
    /// PostgreSQL history) moves on, though nothing new came.
    fn caught_up(&self, _through: i64) -> BoxFuture<'_, Result<(), ConnectorError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Exactly-once delivery: the stream's writes go into one open transaction, and a commit
/// is that transaction's, with the source's positions in it (Kafka to Kafka: a
/// transactional producer and `send_offsets_to_transaction`). The sinks of such a stream
/// write through it.
pub trait Transaction: Send + Sync {
    /// Commits every write since the last commit together with these positions, then opens
    /// the next transaction. An error means nothing of it is visible.
    fn commit<'a>(&'a self, commits: &'a [Commit]) -> BoxFuture<'a, Result<(), ConnectorError>>;
    /// Discards every write since the last commit.
    fn abort(&self) -> BoxFuture<'_, ()>;
}

/// Exactly-once into a database (Kafka to PostgreSQL, `EXACTLY_ONCE_POSTGRES.md`): the
/// database keeps the source's positions, written in the same transaction as the rows. The
/// source names each commit's position; the transaction stores it.
pub trait PositionNames: Send + Sync {
    /// Each commit as (its partition's name, the next position to read), refused when the
    /// partition is no longer this reader's.
    fn named(&self, commits: &[Commit]) -> Result<Vec<(String, i64)>, ConnectorError>;
    /// The positions were committed: the source reads on past them.
    fn committed(&self, named: &[(String, i64)]);
}

/// Where a source starts the partitions it is assigned, read from the database that keeps
/// its positions. Claiming a partition fences its earlier reader: that reader's next commit
/// of it is refused.
pub trait StartPositions: Send + Sync {
    /// For each partition name, the next position to read; None when none is kept.
    fn claim(&self, partitions: &[String]) -> Result<Vec<Option<i64>>, ConnectorError>;
}

/// A row user code rejected, by its index in the input batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowError {
    pub row: usize,
    pub error: String,
}

/// What a stream's blocks made of one batch: each block's output that a sink writes, by block
/// uuid (streams branch, and sinks follow different blocks).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TransformOutput {
    pub outputs: std::collections::BTreeMap<String, RecordBatch>,
    pub row_errors: Vec<RowError>,
    /// What each block did with the batch, for the stream's status.
    pub blocks: Vec<BlockStats>,
    /// The versions of the broadcast values the blocks ran with, as `name@version,...`;
    /// None when they had none.
    pub broadcast: Option<String>,
}

/// One block's work on one batch.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BlockStats {
    pub block: String,
    pub messages_in: u64,
    pub messages_out: u64,
    pub micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransformError {
    /// The block raised an error for the whole batch.
    User(String),
    /// The worker process died.
    Crash(String),
    Timeout(String),
    /// The worker answered something the engine cannot read.
    Protocol(String),
}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransformError::User(m) => write!(f, "{m}"),
            TransformError::Crash(m) => write!(f, "the worker stopped: {m}"),
            TransformError::Timeout(m) => write!(f, "the transform timed out: {m}"),
            TransformError::Protocol(m) => write!(f, "the worker answered badly: {m}"),
        }
    }
}

/// A stream's blocks, kept loaded between batches.
pub trait TransformSession: Send {
    fn transform(
        &mut self,
        batch: RecordBatch,
    ) -> BoxFuture<'_, Result<TransformOutput, TransformError>>;
    fn close(&mut self) -> BoxFuture<'_, ()>;

    /// The source is about to commit these positions: a session that keeps state saves the
    /// state as of them first, so a restart resumes both from the same point. An error
    /// holds the commit back.
    fn committed<'a>(&'a mut self, _commits: &'a [Commit]) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    /// The partition went to another reader: its state here is no longer current.
    /// Exactly-once: the transaction with these positions committed (after `committed`
    /// saved the state for them).
    fn transaction_committed(&mut self, _commits: &[Commit]) {}
    fn revoked(&mut self, _partition: PartitionId) {}

    /// Whether the session keeps state that `committed` saves. A session that keeps none
    /// is not asked before a commit, so a commit never waits for a transform in progress.
    fn keeps_state(&self) -> bool {
        false
    }
}
