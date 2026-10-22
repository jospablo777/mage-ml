//! A stream's window block as a [`TransformSession`]: the engine keeps each partition's open
//! windows (mage_stream_core::window) and emits a message for each window its partition's
//! watermark closes, with the key, the window's start and end and the aggregates.
//!
//! State is saved with the positions it reflects. After each batch the session keeps a
//! snapshot of the partition's windows as of the batch's last position; before the source
//! commits a position, the snapshot for it is written to
//! `<data>/state/<pipeline>/<block>/partition-<n>.json`, so a restart resumes windows and
//! positions from the same point. With offsets (Kafka, change capture) a restarted stream
//! skips the messages its saved windows already hold, which the source may read again when
//! it stopped between saving and committing; queues redeliver only what they did not
//! acknowledge. A partition that moves to another reader drops its windows here; the state
//! files are on this instance's volume.

use std::collections::BTreeMap;

use mage_service_core::manifest::EventTime;
use mage_stream_core::window::{WindowSpec, Windows, key_of};
use mage_stream_ports::{
    BoxFuture, Commit, Envelope, EnvelopeBuilder, PartitionId, Position, RecordBatch,
    TransformError, TransformOutput, TransformSession, read_envelope,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PartitionState {
    /// The last position whose message the windows hold.
    applied: Option<i64>,
    /// The latest event time seen.
    latest: Option<i64>,
    windows: Windows,
}

pub struct WindowSession {
    block: String,
    spec: WindowSpec,
    time: EventTime,
    files: crate::state_files::StateFiles,
    /// Whether positions stay the same across restarts (offsets), so seen ones can be skipped.
    stable_positions: bool,
    partitions: BTreeMap<u32, PartitionState>,
    /// Per partition: snapshots as of batch ends, oldest first, until a commit saves one.
    pending: BTreeMap<u32, Vec<(i64, PartitionState)>>,
    /// Events for windows already closed, and events without a time.
    pub late: u64,
    pub untimed: u64,
}

fn position_number(position: &Position) -> Option<i64> {
    match position {
        Position::Offset(offset) => Some(*offset),
        Position::Lsn(lsn) => i64::try_from(*lsn).ok(),
        Position::Tag(tag) => i64::try_from(*tag).ok(),
        Position::Opaque(_) => None,
    }
}

fn iso(millis: i64) -> String {
    chrono::DateTime::from_timestamp_millis(millis)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| millis.to_string())
}

impl WindowSession {
    pub fn new(
        block: &str,
        spec: WindowSpec,
        time: Option<EventTime>,
        files: impl Into<crate::state_files::StateFiles>,
        stable_positions: bool,
    ) -> Result<WindowSession, String> {
        spec.check()?;
        Ok(WindowSession {
            block: block.to_string(),
            spec,
            time: time.unwrap_or(EventTime {
                field: None,
                format: Default::default(),
                lateness_ms: 0,
                on_late: mage_service_core::manifest::LatePolicy::Pass,
            }),
            files: files.into(),
            stable_positions,
            partitions: BTreeMap::new(),
            pending: BTreeMap::new(),
            late: 0,
            untimed: 0,
        })
    }

    fn file(partition: u32) -> String {
        format!("partition-{partition}.json")
    }

    /// The partition's state: in memory, else as saved, else empty.
    fn state(&mut self, partition: u32) -> Result<&mut PartitionState, String> {
        if !self.partitions.contains_key(&partition) {
            let file = Self::file(partition);
            let state = match self.files.read(&file)? {
                Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                    format!(
                        "the saved windows in {} are unreadable: {e}",
                        self.files.describe(&file)
                    )
                })?,
                None => PartitionState::default(),
            };
            self.partitions.insert(partition, state);
        }
        Ok(self.partitions.get_mut(&partition).expect("inserted"))
    }

    fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        let messages = read_envelope(batch).map_err(|e| e.to_string())?;
        let mut out = EnvelopeBuilder::new();
        let lateness = self.time.lateness_ms as i64;
        let time_settings = self.time.clone();
        let stable = self.stable_positions;
        let spec = self.spec.clone();
        let mut touched = Vec::new();
        for message in &messages {
            let state = self.state(message.partition)?;
            if stable
                && let (Some(position), Some(applied)) = (message.position, state.applied)
                && position <= applied
            {
                // Already in the saved windows: read again after a restart.
                continue;
            }
            let event: Value = serde_json::from_slice(&message.value).unwrap_or(Value::Null);
            let Some(time) = crate::stream_time::event_time_of(
                &time_settings,
                &message.value,
                message.timestamp,
            ) else {
                self.untimed += 1;
                continue;
            };
            let state = self.state(message.partition)?;
            let watermark = state.latest.map(|l| l - lateness);
            let key = key_of(&event, spec.key.as_deref());
            let added = state.windows.add(&spec, &key, time, &event, watermark);
            state.latest = Some(state.latest.map_or(time, |l| l.max(time)));
            if let Some(position) = message.position {
                state.applied = Some(state.applied.map_or(position, |a| a.max(position)));
            }
            if !added {
                self.late += 1;
            }
            if !touched.contains(&message.partition) {
                touched.push(message.partition);
            }
        }
        for partition in touched {
            let state = self.state(partition)?;
            let Some(latest) = state.latest else { continue };
            for closed in state.windows.close(&spec, latest - lateness) {
                let mut value = Map::new();
                if spec.key.is_some() {
                    value.insert("key".into(), Value::String(closed.key.clone()));
                }
                value.insert("window_start".into(), Value::String(iso(closed.start)));
                value.insert("window_end".into(), Value::String(iso(closed.end)));
                value.extend(closed.fields);
                out.push(&Envelope {
                    key: spec.key.as_ref().map(|_| closed.key.clone().into_bytes()),
                    value: serde_json::to_vec(&Value::Object(value)).unwrap_or_default(),
                    headers: vec![],
                    partition,
                    position: None,
                    timestamp: Some(closed.end),
                });
            }
            let snapshot = state.clone();
            if let Some(applied) = snapshot.applied {
                self.pending
                    .entry(partition)
                    .or_default()
                    .push((applied, snapshot));
            }
        }
        out.finish().map_err(|e| e.to_string())
    }

    /// Saves each committed partition's windows as of its committed position.
    fn save(&mut self, commits: &[Commit]) -> Result<(), String> {
        for commit in commits {
            let partition = commit.partition.0;
            let Some(pending) = self.pending.get_mut(&partition) else {
                continue;
            };
            let committed = position_number(&commit.position);
            // The newest snapshot at or before the committed position.
            let chosen = match committed {
                Some(position) => pending.iter().rposition(|(at, _)| *at <= position),
                None => pending.len().checked_sub(1),
            };
            let Some(index) = chosen else { continue };
            let (_, snapshot) = pending[index].clone();
            pending.drain(..=index);
            let bytes = serde_json::to_vec(&snapshot).map_err(|e| e.to_string())?;
            self.files.write(&Self::file(partition), &bytes)?;
        }
        Ok(())
    }

    /// Open windows, over the partitions.
    #[cfg(test)]
    pub fn open_windows(&self) -> usize {
        self.partitions.values().map(|s| s.windows.len()).sum()
    }
}

impl TransformSession for WindowSession {
    fn transform(
        &mut self,
        batch: RecordBatch,
    ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
        Box::pin(async move {
            let started = std::time::Instant::now();
            let rows_in = batch.num_rows() as u64;
            let closed = self.apply(&batch).map_err(TransformError::Crash)?;
            let mut output = TransformOutput::default();
            output.blocks.push(mage_stream_ports::BlockStats {
                block: self.block.clone(),
                messages_in: rows_in,
                messages_out: closed.num_rows() as u64,
                micros: started.elapsed().as_micros() as u64,
            });
            output.outputs.insert(self.block.clone(), closed);
            Ok(output)
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn committed<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move { self.save(commits) })
    }

    fn revoked(&mut self, partition: PartitionId) {
        self.partitions.remove(&partition.0);
        self.pending.remove(&partition.0);
    }

    fn keeps_state(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use mage_service_core::manifest::{LatePolicy, TimeFormat};
    use mage_stream_core::window::{Aggregate, WindowKind};
    use mage_stream_ports::AssignmentEpoch;
    use serde_json::json;

    use super::*;

    fn session(dir: &std::path::Path) -> WindowSession {
        WindowSession::new(
            "per_card",
            WindowSpec {
                key: Some("card".into()),
                kind: WindowKind::Tumbling { size_ms: 60_000 },
                aggregates: vec![
                    ("n".into(), Aggregate::Count),
                    ("total".into(), Aggregate::Sum("amount".into())),
                ],
            },
            Some(EventTime {
                field: Some("at".into()),
                format: TimeFormat::EpochS,
                lateness_ms: 0,
                on_late: LatePolicy::Pass,
            }),
            dir.to_path_buf(),
            true,
        )
        .unwrap()
    }

    fn batch(events: &[(i64, i64, &str, f64)]) -> RecordBatch {
        let mut builder = EnvelopeBuilder::new();
        for (position, at, card, amount) in events {
            builder.push(&Envelope {
                key: None,
                value: serde_json::to_vec(&json!({"at": at, "card": card, "amount": amount}))
                    .unwrap(),
                headers: vec![],
                partition: 0,
                position: Some(*position),
                timestamp: None,
            });
        }
        builder.finish().unwrap()
    }

    fn results(output: &TransformOutput) -> Vec<Value> {
        read_envelope(&output.outputs["per_card"])
            .unwrap()
            .iter()
            .map(|m| serde_json::from_slice(&m.value).unwrap())
            .collect()
    }

    fn commit(position: i64) -> Commit {
        Commit {
            partition: PartitionId(0),
            epoch: AssignmentEpoch(1),
            position: Position::Offset(position),
        }
    }

    #[tokio::test]
    async fn a_window_closes_when_the_watermark_passes_it_and_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut windows = session(dir.path());
        let first = windows
            .transform(batch(&[
                (0, 0, "a", 1.0),
                (1, 30, "a", 2.0),
                (2, 59, "b", 5.0),
            ]))
            .await
            .unwrap();
        assert!(results(&first).is_empty(), "nothing closed yet");
        windows.committed(&[commit(2)]).await.unwrap();

        // A restart: the saved windows come back, and positions 0 to 2 read again are skipped.
        let mut restarted = session(dir.path());
        let output = restarted
            .transform(batch(&[
                (1, 30, "a", 2.0),
                (2, 59, "b", 5.0),
                (3, 61, "a", 4.0),
            ]))
            .await
            .unwrap();
        let closed = results(&output);
        assert_eq!(
            closed,
            [
                json!({"key": "a", "window_start": "1970-01-01T00:00:00.000Z",
                       "window_end": "1970-01-01T00:01:00.000Z", "n": 2, "total": 3.0}),
                json!({"key": "b", "window_start": "1970-01-01T00:00:00.000Z",
                       "window_end": "1970-01-01T00:01:00.000Z", "n": 1, "total": 5.0}),
            ]
        );
        assert_eq!(restarted.open_windows(), 1);
    }

    #[tokio::test]
    async fn the_saved_windows_are_the_committed_ones_not_the_newest() {
        let dir = tempfile::tempdir().unwrap();
        let mut windows = session(dir.path());
        windows.transform(batch(&[(0, 0, "a", 1.0)])).await.unwrap();
        windows
            .transform(batch(&[(1, 10, "a", 1.0)]))
            .await
            .unwrap();
        // Only position 0 is committed: the save holds one event, and position 1 is read
        // again after a restart and counted then.
        windows.committed(&[commit(0)]).await.unwrap();
        let mut restarted = session(dir.path());
        restarted
            .transform(batch(&[(1, 10, "a", 1.0), (2, 70, "a", 1.0)]))
            .await
            .map(|output| {
                let closed = results(&output);
                assert_eq!(closed[0]["n"], 2, "{closed:?}");
            })
            .unwrap();
    }

    #[tokio::test]
    async fn an_event_for_a_closed_window_is_counted_late() {
        let dir = tempfile::tempdir().unwrap();
        let mut windows = session(dir.path());
        windows
            .transform(batch(&[(0, 120, "a", 1.0), (1, 5, "a", 1.0)]))
            .await
            .unwrap();
        assert_eq!(windows.late, 1);
    }
}
