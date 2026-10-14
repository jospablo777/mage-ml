//! Identifiers of stream work.

use std::fmt;

/// An index into a stream's table of partitions: a (topic, partition) for Kafka, a queue for
/// RabbitMQ, a consumer for NATS, the replication slot for PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(pub u32);

/// One assignment of a partition to this runner. A rebalance or a reconnect starts a new
/// epoch; work of an older epoch is stale and never commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssignmentEpoch(pub u64);

/// Where a source is in a partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Position {
    /// A Kafka offset.
    Offset(i64),
    /// A PostgreSQL log sequence number.
    Lsn(u64),
    /// A RabbitMQ delivery tag, valid within its channel's epoch.
    Tag(u64),
    /// A source's own token.
    Opaque(Vec<u8>),
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Position::Offset(o) => write!(f, "offset {o}"),
            Position::Lsn(l) => write!(f, "LSN {:X}/{:X}", l >> 32, l & 0xFFFF_FFFF),
            Position::Tag(t) => write!(f, "tag {t}"),
            Position::Opaque(b) => write!(f, "{} bytes", b.len()),
        }
    }
}

/// A batch: its partition, the epoch it was read in, and its order within the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchId {
    pub partition: PartitionId,
    pub epoch: AssignmentEpoch,
    pub seq: u64,
}

impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "partition {} epoch {} batch {}",
            self.partition.0, self.epoch.0, self.seq
        )
    }
}

/// What the runner knows of a batch without its data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchMeta {
    pub id: BatchId,
    pub first: Position,
    pub last: Position,
    pub records: u32,
    pub bytes: u64,
}

/// A sink of the stream, by its index in the stream's sink list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SinkId(pub u16);

impl SinkId {
    /// The dead-letter sink, as the writer of a whole batch that failed.
    pub const DEAD_LETTER: SinkId = SinkId(u16::MAX);
    /// The dead-letter sink as the writer of the messages of a batch that failed on their
    /// own (row-level dead letters): one more confirmation the batch needs.
    pub const REJECTED_ROWS: SinkId = SinkId(u16::MAX - 1);
}
