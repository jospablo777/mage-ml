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

/// The bit of a timer batch's `seq`: the driver makes these batches, which carry no message
/// and no position, so their ids never meet a source's.
pub const TIMER_SEQ: u64 = 1 << 63;

impl BatchId {
    /// A batch the driver made to fire timers on an idle partition.
    pub fn is_timer(&self) -> bool {
        self.seq & TIMER_SEQ != 0
    }
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

impl BatchMeta {
    /// A written row's key, unique within the stream's partition and the same when the batch
    /// is written again: its own position when it has one (rows passed through), otherwise
    /// its index in the batch's input range (rows the blocks made).
    pub fn row_key(&self, position: Option<i64>, index: usize) -> String {
        match position {
            Some(position) => position.to_string(),
            None => format!("{}-{}#{index}", self.first, self.last),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(first: u64, last: u64) -> BatchMeta {
        BatchMeta {
            id: BatchId {
                partition: PartitionId(0),
                epoch: AssignmentEpoch(1),
                seq: 0,
            },
            first: Position::Tag(first),
            last: Position::Tag(last),
            records: (last - first + 1) as u32,
            bytes: 0,
        }
    }

    #[test]
    fn row_keys_are_unique_across_batches_and_stable_for_a_replay() {
        let (a, b) = (meta(1, 50), meta(51, 100));
        let keys: std::collections::BTreeSet<String> = (0..45)
            .flat_map(|i| [a.row_key(None, i), b.row_key(None, i)])
            .collect();
        assert_eq!(keys.len(), 90);
        assert_eq!(a.row_key(None, 3), meta(1, 50).row_key(None, 3));
        assert_eq!(a.row_key(Some(17), 3), "17");
    }
}
