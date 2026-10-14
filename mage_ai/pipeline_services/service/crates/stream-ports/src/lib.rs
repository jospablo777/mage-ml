//! The ports of stream processing: what a source, a sink and a transform session do, in
//! terms of Arrow record batches with a fixed envelope (ADR-0019).
//!
//! Connectors are chosen at run time from the manifest, so the traits are object safe: their
//! methods return boxed futures. Implementations live in connector crates; the engine drives
//! them with the state machine of `mage-stream-core`.

mod envelope;

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub mod sql;
mod sql_reserved;

pub use arrow_array::RecordBatch;
pub use envelope::{Envelope, EnvelopeBuilder, Header, envelope_schema, read_envelope};
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
}

/// A row user code rejected, by its index in the input batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowError {
    pub row: usize,
    pub error: String,
}

/// What a stream's blocks made of one batch: each block's output that a sink writes, by block
/// uuid (streams branch, and sinks follow different blocks).
#[derive(Debug, Clone, PartialEq)]
pub struct TransformOutput {
    pub outputs: std::collections::BTreeMap<String, RecordBatch>,
    pub row_errors: Vec<RowError>,
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
}
