//! Local stream connectors.
//!
//! - [`MemorySource`]: messages held in memory, read in batches per partition. It honors
//!   commits, pauses and epochs like a broker, and can rebalance or restart on demand, so the
//!   runner and the conformance suite can test delivery without a broker.
//! - [`MemorySink`]: records what it receives; can fail on purpose.
//! - [`StdoutSink`]: one JSON line per message on standard output, for development.

use std::collections::{BTreeSet, VecDeque};
use std::io::Write as _;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, ConnectorError,
    Envelope, EnvelopeBuilder, PartitionId, Position, RecordBatch, Sink, Source,
    SourceCapabilities, SourceEvent, read_envelope,
};

#[derive(Debug)]
struct PartitionState {
    messages: Vec<Envelope>,
    /// Index of the next message to read.
    cursor: usize,
    /// Index of the first message not acknowledged.
    committed: usize,
    epoch: AssignmentEpoch,
    seq: u64,
}

#[derive(Debug)]
struct SourceState {
    partitions: Vec<PartitionState>,
    events: VecDeque<SourceEvent>,
    paused: BTreeSet<u32>,
    batch_size: usize,
    /// Return End when everything was read, instead of Idle.
    finite: bool,
    commits: Vec<Commit>,
    next: usize,
}

/// Inspect and steer a [`MemorySource`] from a test while the runner owns it.
#[derive(Clone, Debug)]
pub struct MemorySourceHandle(Arc<Mutex<SourceState>>);

impl MemorySourceHandle {
    fn lock(&self) -> MutexGuard<'_, SourceState> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Per partition, the index of the first message not acknowledged.
    pub fn committed(&self) -> Vec<usize> {
        self.lock().partitions.iter().map(|p| p.committed).collect()
    }

    /// Every commit the source accepted, in order.
    pub fn commits(&self) -> Vec<Commit> {
        self.lock().commits.clone()
    }

    pub fn paused(&self) -> Vec<u32> {
        self.lock().paused.iter().copied().collect()
    }

    /// Revokes a partition and assigns it again in a new epoch; reading restarts at the
    /// first unacknowledged message, as a broker does after a rebalance.
    pub fn rebalance(&self, partition: u32) {
        let mut state = self.lock();
        let p = &mut state.partitions[partition as usize];
        let old = p.epoch;
        p.epoch = AssignmentEpoch(old.0 + 1);
        p.cursor = p.committed;
        p.seq = 0;
        let new = p.epoch;
        state.paused.remove(&partition);
        state.events.push_back(SourceEvent::Revoking {
            partition: PartitionId(partition),
            epoch: old,
            within: Duration::from_secs(5),
        });
        state.events.push_back(SourceEvent::Assigned {
            partition: PartitionId(partition),
            epoch: new,
        });
    }

    /// A new source over the same messages and acknowledgements, as after a crash.
    pub fn restart(&self) -> MemorySource {
        let state = self.lock();
        let partitions: Vec<Vec<Envelope>> = state
            .partitions
            .iter()
            .map(|p| p.messages.clone())
            .collect();
        let committed: Vec<usize> = state.partitions.iter().map(|p| p.committed).collect();
        let epochs: Vec<AssignmentEpoch> = state.partitions.iter().map(|p| p.epoch).collect();
        let (batch_size, finite) = (state.batch_size, state.finite);
        drop(state);
        let source = MemorySource::new(partitions, batch_size, finite);
        {
            let mut new_state = source.handle.lock();
            new_state.events.clear();
            for (i, p) in new_state.partitions.iter_mut().enumerate() {
                p.committed = committed[i];
                p.cursor = committed[i];
                p.epoch = AssignmentEpoch(epochs[i].0 + 1);
            }
            let assigned: Vec<SourceEvent> = new_state
                .partitions
                .iter()
                .enumerate()
                .map(|(i, p)| SourceEvent::Assigned {
                    partition: PartitionId(i as u32),
                    epoch: p.epoch,
                })
                .collect();
            new_state.events.extend(assigned);
        }
        source
    }
}

pub struct MemorySource {
    handle: MemorySourceHandle,
}

impl MemorySource {
    /// `partitions[i]` holds partition i's messages; positions are their indexes.
    pub fn new(partitions: Vec<Vec<Envelope>>, batch_size: usize, finite: bool) -> MemorySource {
        let events = (0..partitions.len())
            .map(|i| SourceEvent::Assigned {
                partition: PartitionId(i as u32),
                epoch: AssignmentEpoch(1),
            })
            .collect();
        let partitions = partitions
            .into_iter()
            .enumerate()
            .map(|(i, messages)| PartitionState {
                messages: messages
                    .into_iter()
                    .enumerate()
                    .map(|(n, m)| Envelope {
                        partition: i as u32,
                        position: Some(n as i64),
                        ..m
                    })
                    .collect(),
                cursor: 0,
                committed: 0,
                epoch: AssignmentEpoch(1),
                seq: 0,
            })
            .collect();
        MemorySource {
            handle: MemorySourceHandle(Arc::new(Mutex::new(SourceState {
                partitions,
                events,
                paused: BTreeSet::new(),
                batch_size: batch_size.max(1),
                finite,
                commits: Vec::new(),
                next: 0,
            }))),
        }
    }

    pub fn handle(&self) -> MemorySourceHandle {
        self.handle.clone()
    }

    fn read(&self) -> Result<SourceEvent, ConnectorError> {
        let mut state = self.handle.lock();
        if let Some(event) = state.events.pop_front() {
            return Ok(event);
        }
        let count = state.partitions.len();
        let batch_size = state.batch_size;
        for offset in 0..count {
            let index = (state.next + offset) % count;
            if state.paused.contains(&(index as u32)) {
                continue;
            }
            let p = &mut state.partitions[index];
            if p.cursor >= p.messages.len() {
                continue;
            }
            let end = (p.cursor + batch_size).min(p.messages.len());
            let mut builder = EnvelopeBuilder::new();
            for message in &p.messages[p.cursor..end] {
                builder.push(message);
            }
            let bytes = builder.bytes() as u64;
            let batch = builder
                .finish()
                .map_err(|e| ConnectorError::fatal(e.to_string()))?;
            let meta = BatchMeta {
                id: BatchId {
                    partition: PartitionId(index as u32),
                    epoch: p.epoch,
                    seq: p.seq,
                },
                first: Position::Offset(p.cursor as i64),
                last: Position::Offset(end as i64 - 1),
                records: (end - p.cursor) as u32,
                bytes,
            };
            p.seq += 1;
            p.cursor = end;
            state.next = index + 1;
            return Ok(SourceEvent::Batch { batch, meta });
        }
        let done = state
            .partitions
            .iter()
            .all(|p| p.cursor >= p.messages.len());
        Ok(if done && state.finite {
            SourceEvent::End
        } else {
            SourceEvent::Idle
        })
    }
}

impl Source for MemorySource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        let event = self.read();
        Box::pin(async move { event })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        let mut state = self.handle.lock();
        let mut outcome = CommitOutcome::Committed;
        for commit in commits {
            let p = &mut state.partitions[commit.partition.0 as usize];
            match commit.position {
                Position::Offset(last) if p.epoch == commit.epoch => {
                    p.committed = p.committed.max(last as usize + 1);
                }
                _ => outcome = CommitOutcome::Stale,
            }
        }
        if outcome == CommitOutcome::Committed {
            state.commits.extend_from_slice(commits);
        }
        Box::pin(async move { outcome })
    }

    fn pause(&mut self, partition: PartitionId) {
        self.handle.lock().paused.insert(partition.0);
    }

    fn resume(&mut self, partition: PartitionId) {
        self.handle.lock().paused.remove(&partition.0);
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            replayable: true,
            ordered: true,
            ack_kind: AckKind::Offset,
            max_instances: Some(1),
            needs_keepalive: false,
            durable_positions: true,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[derive(Debug, Default)]
struct SinkState {
    received: Vec<Envelope>,
    batches: Vec<BatchMeta>,
    /// Failures to return before writing: (retryable, message).
    failures: VecDeque<(bool, String)>,
    /// After this many successful writes, every write fails (retryable): an outage.
    outage_after: Option<usize>,
}

/// Records every message written, in order; can fail on purpose.
#[derive(Clone, Debug, Default)]
pub struct MemorySink(Arc<Mutex<SinkState>>);

impl MemorySink {
    pub fn new() -> MemorySink {
        MemorySink::default()
    }

    fn lock(&self) -> MutexGuard<'_, SinkState> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn received(&self) -> Vec<Envelope> {
        self.lock().received.clone()
    }

    pub fn batches(&self) -> Vec<BatchMeta> {
        self.lock().batches.clone()
    }

    /// After `writes` successful writes, every write fails as if the target were down.
    pub fn outage_after(&self, writes: usize) {
        self.lock().outage_after = Some(writes);
    }

    /// The next `count` writes fail.
    pub fn fail_next(&self, count: usize, retryable: bool, message: &str) {
        let mut state = self.lock();
        for _ in 0..count {
            state.failures.push_back((retryable, message.to_string()));
        }
    }
}

impl Sink for MemorySink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        let result = (|| {
            let mut state = self.lock();
            if let Some((retryable, message)) = state.failures.pop_front() {
                return Err(ConnectorError { message, retryable });
            }
            if state.outage_after.is_some_and(|n| state.batches.len() >= n) {
                return Err(ConnectorError::retryable("the sink is down"));
            }
            let messages =
                read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
            state.received.extend(messages);
            state.batches.push(meta.clone());
            Ok(())
        })();
        Box::pin(async move { result })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// One JSON line per message on standard output: the value as JSON when it is JSON, as
/// text otherwise, with its partition and position.
#[derive(Debug, Default)]
pub struct StdoutSink;

/// A message as one JSON line. The value is written as the JSON it is, never parsed into
/// numbers of 64 bits, so every digit stays; a value that is not JSON is written as text.
pub fn message_json(message: &Envelope) -> String {
    let value = match serde_json::from_slice::<&serde_json::value::RawValue>(&message.value) {
        Ok(raw) => raw.get().to_string(),
        Err(_) => serde_json::Value::String(String::from_utf8_lossy(&message.value).into_owned())
            .to_string(),
    };
    let position = message
        .position
        .map_or_else(|| "null".to_string(), |p| p.to_string());
    let key = message.key.as_ref().map_or_else(String::new, |key| {
        format!(
            ",\"key\":{}",
            serde_json::Value::String(String::from_utf8_lossy(key).into_owned())
        )
    });
    format!(
        "{{\"partition\":{},\"position\":{position},\"value\":{value}{key}}}",
        message.partition
    )
}

impl Sink for StdoutSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        let result = read_envelope(batch)
            .map_err(|e| ConnectorError::fatal(e.to_string()))
            .and_then(|messages| {
                let mut out = std::io::stdout().lock();
                for message in &messages {
                    writeln!(out, "{}", message_json(message))
                        .map_err(|e| ConnectorError::retryable(e.to_string()))?;
                }
                out.flush()
                    .map_err(|e| ConnectorError::retryable(e.to_string()))
            });
        Box::pin(async move { result })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stdout_line_keeps_the_value_exactly_and_text_as_text() {
        let exact = Envelope {
            value: br#"{"big": 123456789012345678901234567890, "x": 0.1}"#.to_vec(),
            partition: 2,
            position: Some(7),
            key: Some(b"k\"1".to_vec()),
            ..Envelope::default()
        };
        assert_eq!(
            message_json(&exact),
            r#"{"partition":2,"position":7,"value":{"big": 123456789012345678901234567890, "x": 0.1},"key":"k\"1"}"#
        );
        let text = Envelope {
            value: b"plain \"text\"".to_vec(),
            ..Envelope::default()
        };
        let line: serde_json::Value = serde_json::from_str(&message_json(&text)).unwrap();
        assert_eq!(line["value"], "plain \"text\"");
        assert_eq!(line["position"], serde_json::Value::Null);
    }
}
