//! Keyed state of a stream block: a JSON value per key, per partition. The block gets the
//! values of each batch's keys and sends back what it changed (stream_python.rs).
//!
//! Saved with the positions it reflects, as windows are (stream_window.rs): each batch's
//! changes are kept with the batch's last position, and before the source commits a position
//! the changes through it are applied to the saved state, written to
//! `<data>/state/<pipeline>/<block>/partition-<n>.json`. A restart loads it and, with offsets,
//! skips the messages it already holds.

use std::collections::BTreeMap;
use std::path::PathBuf;

use mage_stream_ports::{Commit, Position};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Saved {
    /// The last position whose message the values hold.
    applied: Option<i64>,
    values: BTreeMap<String, Value>,
}

/// A batch's changes: a key's new value, or None when the key was removed.
pub type Changes = BTreeMap<String, Option<Value>>;

#[derive(Default)]
struct Partition {
    saved: Saved,
    /// The saved values with the changes not yet committed.
    current: BTreeMap<String, Value>,
    applied: Option<i64>,
    /// Each batch's changes, by its last position, oldest first.
    pending: Vec<(Option<i64>, Changes)>,
}

pub struct KeyedState {
    dir: PathBuf,
    partitions: BTreeMap<u32, Partition>,
}

fn position_number(position: &Position) -> Option<i64> {
    match position {
        Position::Offset(offset) => Some(*offset),
        Position::Lsn(lsn) => i64::try_from(*lsn).ok(),
        Position::Tag(tag) => i64::try_from(*tag).ok(),
        Position::Opaque(_) => None,
    }
}

impl KeyedState {
    pub fn new(dir: PathBuf) -> KeyedState {
        KeyedState {
            dir,
            partitions: BTreeMap::new(),
        }
    }

    fn file(&self, partition: u32) -> PathBuf {
        self.dir.join(format!("partition-{partition}.json"))
    }

    fn partition(&mut self, partition: u32) -> Result<&mut Partition, String> {
        if !self.partitions.contains_key(&partition) {
            let file = self.file(partition);
            let saved: Saved = match std::fs::read(&file) {
                Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                    format!("the saved state in {} is unreadable: {e}", file.display())
                })?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Saved::default(),
                Err(error) => return Err(format!("reading {}: {error}", file.display())),
            };
            self.partitions.insert(
                partition,
                Partition {
                    current: saved.values.clone(),
                    applied: saved.applied,
                    saved,
                    pending: Vec::new(),
                },
            );
        }
        Ok(self.partitions.get_mut(&partition).expect("inserted"))
    }

    /// The last position the state holds in the partition.
    pub fn applied(&mut self, partition: u32) -> Result<Option<i64>, String> {
        Ok(self.partition(partition)?.applied)
    }

    /// The values of these keys, for a batch.
    pub fn slice(&mut self, partition: u32, keys: &[String]) -> Result<Map<String, Value>, String> {
        let state = self.partition(partition)?;
        Ok(keys
            .iter()
            .filter_map(|key| Some((key.clone(), state.current.get(key)?.clone())))
            .collect())
    }

    /// A batch's changes (null removes a key), as of the batch's last position.
    pub fn apply(
        &mut self,
        partition: u32,
        last: Option<i64>,
        changes: Changes,
    ) -> Result<(), String> {
        let state = self.partition(partition)?;
        for (key, value) in &changes {
            match value {
                Some(value) => {
                    state.current.insert(key.clone(), value.clone());
                }
                None => {
                    state.current.remove(key);
                }
            }
        }
        if let Some(last) = last {
            state.applied = Some(state.applied.map_or(last, |a| a.max(last)));
        }
        state.pending.push((last, changes));
        Ok(())
    }

    /// Saves each committed partition's state as of its committed position.
    pub fn save(&mut self, commits: &[Commit]) -> Result<(), String> {
        for commit in commits {
            let partition = commit.partition.0;
            let file = self.file(partition);
            let dir = self.dir.clone();
            let Some(state) = self.partitions.get_mut(&partition) else {
                continue;
            };
            let committed = position_number(&commit.position);
            let through = state
                .pending
                .iter()
                .take_while(|(at, _)| match (at, committed) {
                    (Some(at), Some(committed)) => *at <= committed,
                    // Without comparable positions, everything read so far is committed.
                    _ => true,
                })
                .count();
            if through == 0 {
                continue;
            }
            for (at, changes) in state.pending.drain(..through) {
                for (key, value) in changes {
                    match value {
                        Some(value) => {
                            state.saved.values.insert(key, value);
                        }
                        None => {
                            state.saved.values.remove(&key);
                        }
                    }
                }
                if let Some(at) = at {
                    state.saved.applied = Some(state.saved.applied.map_or(at, |a| a.max(at)));
                }
            }
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("creating {}: {e}", dir.display()))?;
            let temporary = file.with_extension("json.tmp");
            let bytes = serde_json::to_vec(&state.saved).map_err(|e| e.to_string())?;
            {
                use std::io::Write;
                let mut handle = std::fs::File::create(&temporary)
                    .map_err(|e| format!("writing {}: {e}", temporary.display()))?;
                handle.write_all(&bytes).map_err(|e| e.to_string())?;
                handle.sync_all().map_err(|e| e.to_string())?;
            }
            std::fs::rename(&temporary, &file)
                .map_err(|e| format!("saving {}: {e}", file.display()))?;
        }
        Ok(())
    }

    pub fn revoked(&mut self, partition: u32) {
        self.partitions.remove(&partition);
    }
}

#[cfg(test)]
mod tests {
    use mage_stream_ports::{AssignmentEpoch, PartitionId};
    use serde_json::json;

    use super::*;

    fn commit(position: i64) -> Commit {
        Commit {
            partition: PartitionId(0),
            epoch: AssignmentEpoch(1),
            position: Position::Offset(position),
        }
    }

    fn changes(pairs: &[(&str, Option<Value>)]) -> BTreeMap<String, Option<Value>> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn the_saved_state_is_the_committed_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = KeyedState::new(dir.path().to_path_buf());
        state
            .apply(
                0,
                Some(4),
                changes(&[("a", Some(json!(1))), ("b", Some(json!(2)))]),
            )
            .unwrap();
        state
            .apply(0, Some(9), changes(&[("a", Some(json!(5))), ("b", None)]))
            .unwrap();
        assert_eq!(
            state.slice(0, &["a".into(), "b".into()]).unwrap(),
            json!({"a": 5}).as_object().unwrap().clone()
        );
        state.save(&[commit(4)]).unwrap();

        let mut restarted = KeyedState::new(dir.path().to_path_buf());
        assert_eq!(restarted.applied(0).unwrap(), Some(4));
        assert_eq!(
            restarted.slice(0, &["a".into(), "b".into()]).unwrap(),
            json!({"a": 1, "b": 2}).as_object().unwrap().clone()
        );
        // The rest of the first instance's changes save with the next commit.
        state.save(&[commit(9)]).unwrap();
        let mut again = KeyedState::new(dir.path().to_path_buf());
        assert_eq!(again.applied(0).unwrap(), Some(9));
        assert_eq!(again.slice(0, &["b".into()]).unwrap(), Map::new());
    }
}
