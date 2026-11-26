//! Keyed state of a stream block: a JSON value and a timer per key, per partition. The
//! block gets the values and timers of each batch's keys, and the keys whose timers fired,
//! and sends back what it changed (stream_python.rs, stream_rust.rs).
//!
//! A timer is a time in milliseconds: event time when the stream has `time`, else the time
//! batches arrive. It fires when a batch of its partition moves the watermark (or the
//! arrival time) to it or past it: the engine sends the key with that batch, with its state,
//! and removes the timer unless the block sets it again. Timers fire with batches, so their
//! outputs and changes commit with the batch's positions.
//!
//! Saved with the positions it reflects, as windows are (stream_window.rs): each batch's
//! changes are kept with the batch's last position, and before the source commits a position
//! the changes through it are applied to the saved state, written to
//! `<data>/state/<pipeline>/<block>/partition-<n>.json`, or in the object store of
//! MAGE_SERVICE_STATE_URL (state_files.rs). A restart loads it and, with offsets, skips the
//! messages it already holds.
//!
//! With exactly-once delivery (Kafka transactions) a message the saved state holds is not
//! done: the transaction that wrote its outputs may not have committed, and skipped, its
//! outputs would be lost. So the file also keeps the state as of the last committed
//! transaction (`committed`), and the first message read in a partition after a load tells
//! which one stands: when the newest state already holds it, its transaction did not commit,
//! and the committed state is used instead (`reconcile`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use mage_service_core::manifest::EventTime;
use mage_stream_core::{EventClock, PartitionId};
use mage_stream_ports::{Commit, Position};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Saved {
    /// The last position whose message the values hold.
    applied: Option<i64>,
    values: BTreeMap<String, Value>,
    /// Per key, when its timer fires.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    timers: BTreeMap<String, i64>,
    /// Exactly-once: the state as of the last committed transaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    committed: Option<Box<Saved>>,
}

impl Saved {
    /// This state without its committed one (which is never nested).
    fn alone(&self) -> Saved {
        Saved {
            committed: None,
            ..self.clone()
        }
    }
}

/// A batch's changes: a key's new value, or None when the key was removed.
pub type Changes = BTreeMap<String, Option<Value>>;
/// A batch's timer changes: a key's new time, or None when its timer was removed.
pub type TimerChanges = BTreeMap<String, Option<i64>>;

/// What a batch changed in a block's state.
#[derive(Debug, Clone, Default)]
pub struct Update {
    pub values: Changes,
    pub timers: TimerChanges,
}

#[derive(Default)]
pub struct Partition {
    saved: Saved,
    /// The saved values with the changes not yet committed.
    current: BTreeMap<String, Value>,
    /// The saved timers with the changes not yet committed.
    timers: BTreeMap<String, i64>,
    applied: Option<i64>,
    /// Each batch's changes, by its last position, oldest first.
    pending: Vec<(Option<i64>, Update)>,
    /// Exactly-once: the state as of the last committed transaction, once reconciled.
    committed: Option<Saved>,
    /// Exactly-once: the partition's first message since the load was looked at.
    reconciled: bool,
}

pub struct KeyedState {
    files: crate::state_files::StateFiles,
    /// Shared with the service API, which reads keys (`view`).
    partitions: StateView,
    exactly_once: bool,
}

/// A block's keyed state as the service API reads it: the partitions this instance holds.
pub type StateView = Arc<RwLock<BTreeMap<u32, Partition>>>;

/// What a key holds in a view: its partition, value and timer.
#[derive(Debug, Clone, PartialEq)]
pub struct Found {
    pub partition: u32,
    pub value: Option<Value>,
    pub timer: Option<i64>,
}

/// A key's current value and timer, from the partitions the view holds; changes of batches
/// not yet committed included.
pub fn lookup(view: &StateView, key: &str) -> Option<Found> {
    let partitions = view.read().unwrap_or_else(|p| p.into_inner());
    partitions.iter().find_map(|(partition, state)| {
        let value = state.current.get(key).cloned();
        let timer = state.timers.get(key).copied();
        (value.is_some() || timer.is_some()).then_some(Found {
            partition: *partition,
            value,
            timer,
        })
    })
}

/// Per partition the view holds: its keys with a value and its timers.
pub fn counts(view: &StateView) -> Vec<(u32, usize, usize)> {
    let partitions = view.read().unwrap_or_else(|p| p.into_inner());
    partitions
        .iter()
        .map(|(partition, state)| (*partition, state.current.len(), state.timers.len()))
        .collect()
}

fn position_number(position: &Position) -> Option<i64> {
    match position {
        Position::Offset(offset) => Some(*offset),
        Position::Lsn(lsn) => i64::try_from(*lsn).ok(),
        Position::Tag(tag) => i64::try_from(*tag).ok(),
        Position::Opaque(_) => None,
    }
}

fn merge<T>(
    into: &mut BTreeMap<String, T>,
    changes: impl IntoIterator<Item = (String, Option<T>)>,
) {
    for (key, value) in changes {
        match value {
            Some(value) => {
                into.insert(key, value);
            }
            None => {
                into.remove(&key);
            }
        }
    }
}

impl KeyedState {
    pub fn new(files: impl Into<crate::state_files::StateFiles>) -> KeyedState {
        KeyedState {
            files: files.into(),
            partitions: StateView::default(),
            exactly_once: false,
        }
    }

    /// Exactly-once delivery: the saved state falls back to the last committed one (module
    /// docs).
    pub fn exactly_once(mut self, on: bool) -> KeyedState {
        self.exactly_once = on;
        self
    }

    pub fn view(&self) -> StateView {
        self.partitions.clone()
    }

    fn file(partition: u32) -> String {
        format!("partition-{partition}.json")
    }

    /// Runs `f` on the partition's state, loaded from its file the first time.
    fn with<T>(&self, partition: u32, f: impl FnOnce(&mut Partition) -> T) -> Result<T, String> {
        let file = Self::file(partition);
        let mut partitions = self.partitions.write().unwrap_or_else(|p| p.into_inner());
        let state = match partitions.entry(partition) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let saved: Saved = match self.files.read(&file)? {
                    Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                        format!(
                            "the saved state in {} is unreadable: {e}",
                            self.files.describe(&file)
                        )
                    })?,
                    None => Saved::default(),
                };
                entry.insert(Partition {
                    current: saved.values.clone(),
                    timers: saved.timers.clone(),
                    applied: saved.applied,
                    saved,
                    pending: Vec::new(),
                    committed: None,
                    reconciled: false,
                })
            }
        };
        Ok(f(state))
    }

    /// The last position the state holds in the partition.
    pub fn applied(&mut self, partition: u32) -> Result<Option<i64>, String> {
        self.with(partition, |state| state.applied)
    }

    /// Exactly-once, at the partition's first message since the state was loaded (`first`,
    /// the first offset its assignment reads): a saved state that already holds it was saved
    /// for a transaction that did not commit, so the last committed state stands. Returns
    /// whether it fell back.
    pub fn reconcile(&mut self, partition: u32, first: i64) -> Result<bool, String> {
        let exactly_once = self.exactly_once;
        self.with(partition, |state| {
            if !exactly_once || state.reconciled {
                return false;
            }
            state.reconciled = true;
            let committed = state.saved.committed.take();
            if state.saved.applied.is_some_and(|applied| first <= applied) {
                let base = committed.map(|c| c.alone()).unwrap_or_default();
                state.current = base.values.clone();
                state.timers = base.timers.clone();
                state.applied = base.applied;
                state.pending.clear();
                state.committed = Some(base.clone());
                state.saved = base;
                true
            } else {
                state.committed = Some(state.saved.alone());
                false
            }
        })
    }

    /// Exactly-once: the transaction with these positions committed, so the state saved
    /// for them is the committed state now.
    pub fn transaction_committed(&mut self, commits: &[Commit]) {
        if !self.exactly_once {
            return;
        }
        let mut partitions = self.partitions.write().unwrap_or_else(|p| p.into_inner());
        for commit in commits {
            if let Some(state) = partitions.get_mut(&commit.partition.0) {
                state.committed = Some(state.saved.alone());
            }
        }
    }

    /// The values of these keys, for a batch.
    pub fn slice(&mut self, partition: u32, keys: &[String]) -> Result<Map<String, Value>, String> {
        self.with(partition, |state| {
            keys.iter()
                .filter_map(|key| Some((key.clone(), state.current.get(key)?.clone())))
                .collect()
        })
    }

    /// The timers of these keys, for a batch.
    pub fn timers(
        &mut self,
        partition: u32,
        keys: &[String],
    ) -> Result<Map<String, Value>, String> {
        self.with(partition, |state| {
            keys.iter()
                .filter_map(|key| Some((key.clone(), Value::from(*state.timers.get(key)?))))
                .collect()
        })
    }

    /// The timers due at `time`: at it or before it.
    pub fn due(&mut self, partition: u32, time: i64) -> Result<BTreeMap<String, i64>, String> {
        self.with(partition, |state| {
            state
                .timers
                .iter()
                .filter(|(_, at)| **at <= time)
                .map(|(key, at)| (key.clone(), *at))
                .collect()
        })
    }

    /// A batch's changes (null removes a key or a timer), as of the batch's last position.
    pub fn apply(
        &mut self,
        partition: u32,
        last: Option<i64>,
        update: Update,
    ) -> Result<(), String> {
        self.with(partition, |state| {
            merge(&mut state.current, update.values.clone());
            merge(&mut state.timers, update.timers.clone());
            if let Some(last) = last {
                state.applied = Some(state.applied.map_or(last, |a| a.max(last)));
            }
            state.pending.push((last, update));
        })
    }

    /// Saves each committed partition's state as of its committed position.
    pub fn save(&mut self, commits: &[Commit]) -> Result<(), String> {
        for commit in commits {
            let partition = commit.partition.0;
            // The file is written outside the lock; the API keeps reading meanwhile.
            let saved = {
                let mut partitions = self.partitions.write().unwrap_or_else(|p| p.into_inner());
                let Some(state) = partitions.get_mut(&partition) else {
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
                for (at, update) in state.pending.drain(..through) {
                    merge(&mut state.saved.values, update.values);
                    merge(&mut state.saved.timers, update.timers);
                    if let Some(at) = at {
                        state.saved.applied = Some(state.saved.applied.map_or(at, |a| a.max(at)));
                    }
                }
                if self.exactly_once {
                    // Written with the new state: the transaction may not commit.
                    state.saved.committed = state.committed.clone().map(Box::new);
                }
                serde_json::to_vec(&state.saved).map_err(|e| e.to_string())?
            };
            self.files.write(&Self::file(partition), &saved)?;
        }
        Ok(())
    }

    pub fn revoked(&mut self, partition: u32) {
        self.partitions
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&partition);
    }
}

/// The keyed state of a stream's blocks, for a session that runs them: which blocks keep
/// state and by what field, the messages each batch skips, what it sends and the changes it
/// gets back.
pub struct BlockStates {
    /// Per block with keyed state: its uuid, the source messages' field that keys it, the
    /// store.
    blocks: Vec<(String, String, KeyedState)>,
    /// Whether positions stay the same across restarts (offsets), so held ones are skipped.
    stable: bool,
    /// The stream's event time, and per partition its watermark for timers; without it,
    /// timers are in the time batches arrive.
    time: Option<(EventTime, EventClock)>,
}

/// A batch's messages as keyed state sees them.
#[derive(Default)]
pub struct BatchKeys {
    /// The last position of the messages that run.
    pub last: Option<i64>,
    /// Per block, the keys of the messages that run and of the timers that fire.
    keys: Vec<BTreeSet<String>>,
    /// Per block, the timers that fire with the batch.
    fired: Vec<BTreeMap<String, i64>>,
    /// The time timers are judged by: the partition's watermark after the batch, or the
    /// batch's arrival.
    pub watermark: Option<i64>,
}

impl BatchKeys {
    /// Whether any timer fires with the batch.
    pub fn fires_any(&self) -> bool {
        self.fired.iter().any(|fired| !fired.is_empty())
    }
}

/// What a batch sends to the blocks with keyed state, per block.
pub struct Outgoing {
    pub state: Value,
    pub timers: Value,
    pub fired: Value,
    pub watermark: Option<i64>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

impl BlockStates {
    /// The blocks with `state_key` in their configuration, keeping state in `state`.
    pub fn new(
        order: &[mage_service_core::manifest::Block],
        state: &crate::state_files::StateFiles,
        stable: bool,
        time: Option<EventTime>,
    ) -> BlockStates {
        let blocks = order
            .iter()
            .filter_map(|block| {
                let key = block.configuration.get("state_key")?.as_str()?.to_string();
                Some((
                    block.uuid.clone(),
                    key,
                    KeyedState::new(state.child(&crate::executor::sanitize(&block.uuid))),
                ))
            })
            .collect();
        let time = time.map(|settings| {
            let clock = EventClock::new(settings.lateness_ms as i64);
            (settings, clock)
        });
        BlockStates {
            blocks,
            stable,
            time,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Per block, its field and what the service API reads of its state.
    pub fn views(&self) -> Vec<(String, String, StateView)> {
        self.blocks
            .iter()
            .map(|(block, field, store)| (block.clone(), field.clone(), store.view()))
            .collect()
    }

    /// The field that keys a block's state.
    pub fn key_field(&self, block: &str) -> Option<&str> {
        self.blocks
            .iter()
            .find(|(uuid, _, _)| uuid == block)
            .map(|(_, field, _)| field.as_str())
    }

    /// Exactly-once delivery for every block's state (KeyedState::exactly_once).
    pub fn exactly_once(mut self, on: bool) -> BlockStates {
        self.blocks = self
            .blocks
            .into_iter()
            .map(|(block, field, store)| (block, field, store.exactly_once(on)))
            .collect();
        self
    }

    /// The last position the state holds in the partition, when positions are stable: a
    /// message at or before it was run before a restart and is not run again. `first` is
    /// the batch's first message position: with exactly-once, the state of a transaction
    /// that did not commit gives way to the committed one first (KeyedState::reconcile).
    pub fn held(&mut self, partition: u32, first: Option<i64>) -> Result<Option<i64>, String> {
        if let Some(first) = first {
            for (_, _, store) in &mut self.blocks {
                store.reconcile(partition, first)?;
            }
        }
        match self.blocks.first_mut() {
            Some((_, _, store)) if self.stable => store.applied(partition),
            _ => Ok(None),
        }
    }

    pub fn batch(&self) -> BatchKeys {
        BatchKeys {
            last: None,
            keys: vec![Default::default(); self.blocks.len()],
            fired: vec![Default::default(); self.blocks.len()],
            watermark: None,
        }
    }

    /// Notes a message that runs: its position, its key for each block, and its event time.
    pub fn note(&mut self, batch: &mut BatchKeys, envelope: &mage_stream_ports::Envelope) {
        if self.blocks.is_empty() {
            return;
        }
        batch.last = envelope.position.or(batch.last);
        let value: Value = serde_json::from_slice(&envelope.value).unwrap_or(Value::Null);
        for (index, (_, field, _)) in self.blocks.iter().enumerate() {
            batch.keys[index].insert(mage_stream_core::window::key_of(&value, Some(field)));
        }
        if let Some((settings, clock)) = &mut self.time {
            let time =
                crate::stream_time::event_time_of(settings, &envelope.value, envelope.timestamp);
            clock.observe(PartitionId(envelope.partition), &[time]);
        }
    }

    /// The timers the batch fires: due at the partition's watermark after its messages, or
    /// at the time it arrived.
    pub fn fire(&mut self, partition: u32, batch: &mut BatchKeys) -> Result<(), String> {
        if self.blocks.is_empty() {
            return Ok(());
        }
        batch.watermark = match &self.time {
            Some((_, clock)) => clock.watermark(PartitionId(partition)),
            None => Some(now_ms()),
        };
        let Some(watermark) = batch.watermark else {
            return Ok(());
        };
        for (index, (_, _, store)) in self.blocks.iter_mut().enumerate() {
            let due = store.due(partition, watermark)?;
            batch.keys[index].extend(due.keys().cloned());
            batch.fired[index] = due;
        }
        Ok(())
    }

    /// Per block, the values and timers of the batch's keys and the timers that fire; None
    /// without keyed state.
    pub fn outgoing(
        &mut self,
        partition: u32,
        batch: &BatchKeys,
    ) -> Result<Option<Outgoing>, String> {
        if self.blocks.is_empty() {
            return Ok(None);
        }
        let (mut state, mut timers, mut fired) = (Map::new(), Map::new(), Map::new());
        for (index, (block, _, store)) in self.blocks.iter_mut().enumerate() {
            let wanted: Vec<String> = batch.keys[index].iter().cloned().collect();
            state.insert(
                block.clone(),
                Value::Object(store.slice(partition, &wanted)?),
            );
            timers.insert(
                block.clone(),
                Value::Object(store.timers(partition, &wanted)?),
            );
            fired.insert(
                block.clone(),
                Value::Object(
                    batch.fired[index]
                        .iter()
                        .map(|(key, at)| (key.clone(), Value::from(*at)))
                        .collect(),
                ),
            );
        }
        Ok(Some(Outgoing {
            state: Value::Object(state),
            timers: Value::Object(timers),
            fired: Value::Object(fired),
            watermark: batch.watermark,
        }))
    }

    /// The changes a batch made, per block, as of the batch's last position. Timers that
    /// fired are removed unless the block set them again.
    pub fn apply(
        &mut self,
        partition: u32,
        batch: &BatchKeys,
        values: &BTreeMap<String, Changes>,
        timers: &BTreeMap<String, TimerChanges>,
    ) -> Result<(), String> {
        for (index, (block, _, store)) in self.blocks.iter_mut().enumerate() {
            let mut update = Update {
                values: values.get(block.as_str()).cloned().unwrap_or_default(),
                timers: batch.fired[index]
                    .keys()
                    .map(|k| (k.clone(), None))
                    .collect(),
            };
            if let Some(changes) = timers.get(block.as_str()) {
                update.timers.extend(changes.clone());
            }
            store.apply(partition, batch.last, update)?;
        }
        Ok(())
    }

    pub fn save(&mut self, commits: &[Commit]) -> Result<(), String> {
        for (_, _, store) in &mut self.blocks {
            store.save(commits)?;
        }
        Ok(())
    }

    pub fn transaction_committed(&mut self, commits: &[Commit]) {
        for (_, _, store) in &mut self.blocks {
            store.transaction_committed(commits);
        }
    }

    pub fn revoked(&mut self, partition: u32) {
        for (_, _, store) in &mut self.blocks {
            store.revoked(partition);
        }
        if let Some((_, clock)) = &mut self.time {
            clock.forget(PartitionId(partition));
        }
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

    fn values(pairs: &[(&str, Option<Value>)]) -> Update {
        Update {
            values: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            timers: TimerChanges::new(),
        }
    }

    #[test]
    fn the_saved_state_is_the_committed_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = KeyedState::new(dir.path().to_path_buf());
        state
            .apply(
                0,
                Some(4),
                values(&[("a", Some(json!(1))), ("b", Some(json!(2)))]),
            )
            .unwrap();
        state
            .apply(0, Some(9), values(&[("a", Some(json!(5))), ("b", None)]))
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

    #[test]
    fn timers_fire_once_when_due_and_are_saved_with_the_committed_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = KeyedState::new(dir.path().to_path_buf());
        let timers = |pairs: &[(&str, Option<i64>)]| Update {
            values: Changes::new(),
            timers: pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        };
        state
            .apply(0, Some(1), timers(&[("a", Some(100)), ("b", Some(200))]))
            .unwrap();
        assert_eq!(
            state.due(0, 150).unwrap(),
            BTreeMap::from_iter([("a".to_string(), 100)])
        );
        assert_eq!(
            state.timers(0, &["b".into(), "c".into()]).unwrap(),
            json!({"b": 200}).as_object().unwrap().clone()
        );
        // "a" fired and was removed; "b" moved later.
        state
            .apply(0, Some(2), timers(&[("a", None), ("b", Some(300))]))
            .unwrap();
        assert!(state.due(0, 250).unwrap().is_empty());
        state.save(&[commit(1)]).unwrap();
        let mut restarted = KeyedState::new(dir.path().to_path_buf());
        // Saved through position 1: both timers as they were then.
        assert_eq!(restarted.due(0, 1_000).unwrap().len(), 2);
        state.save(&[commit(2)]).unwrap();
        let mut again = KeyedState::new(dir.path().to_path_buf());
        assert_eq!(
            again.due(0, 1_000).unwrap(),
            BTreeMap::from_iter([("b".to_string(), 300)])
        );
    }

    fn value_of(state: &mut KeyedState, key: &str) -> Option<Value> {
        state
            .slice(0, &[key.to_string()])
            .unwrap()
            .get(key)
            .cloned()
    }

    #[test]
    fn exactly_once_falls_back_to_the_last_committed_state_after_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = KeyedState::new(dir.path().to_path_buf()).exactly_once(true);
        state.reconcile(0, 0).unwrap();
        state
            .apply(0, Some(10), values(&[("a", Some(json!(1)))]))
            .unwrap();
        state.save(&[commit(10)]).unwrap();
        state.transaction_committed(&[commit(10)]);
        // A timer batch saves between commits; then the next commit's save, and a crash
        // before its transaction commits.
        state
            .apply(0, Some(15), values(&[("a", Some(json!(2)))]))
            .unwrap();
        state.save(&[commit(15)]).unwrap();
        state
            .apply(0, Some(20), values(&[("a", Some(json!(3)))]))
            .unwrap();
        state.save(&[commit(20)]).unwrap();

        // The source reads from 11 again: the state of 15 and 20 never committed.
        let mut restarted = KeyedState::new(dir.path().to_path_buf()).exactly_once(true);
        assert!(restarted.reconcile(0, 11).unwrap());
        assert_eq!(value_of(&mut restarted, "a"), Some(json!(1)));
        assert_eq!(restarted.applied(0).unwrap(), Some(10));
        // Asked again (each batch asks), it does not fall back twice.
        restarted
            .apply(0, Some(12), values(&[("a", Some(json!(9)))]))
            .unwrap();
        assert!(!restarted.reconcile(0, 13).unwrap());
        assert_eq!(value_of(&mut restarted, "a"), Some(json!(9)));
    }

    #[test]
    fn exactly_once_keeps_the_state_of_a_committed_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = KeyedState::new(dir.path().to_path_buf()).exactly_once(true);
        state.reconcile(0, 0).unwrap();
        state
            .apply(0, Some(10), values(&[("a", Some(json!(1)))]))
            .unwrap();
        state.save(&[commit(10)]).unwrap();
        state.transaction_committed(&[commit(10)]);
        state
            .apply(0, Some(20), values(&[("a", Some(json!(2)))]))
            .unwrap();
        state.save(&[commit(20)]).unwrap();
        state.transaction_committed(&[commit(20)]);

        // Committed through 20: the source reads from 21.
        let mut restarted = KeyedState::new(dir.path().to_path_buf()).exactly_once(true);
        assert!(!restarted.reconcile(0, 21).unwrap());
        assert_eq!(value_of(&mut restarted, "a"), Some(json!(2)));
    }

    #[test]
    fn at_least_once_holds_what_it_saved_and_skips_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = KeyedState::new(dir.path().to_path_buf());
        state
            .apply(0, Some(20), values(&[("a", Some(json!(3)))]))
            .unwrap();
        state.save(&[commit(20)]).unwrap();
        let mut restarted = KeyedState::new(dir.path().to_path_buf());
        assert!(!restarted.reconcile(0, 11).unwrap());
        assert_eq!(restarted.applied(0).unwrap(), Some(20));
        assert_eq!(value_of(&mut restarted, "a"), Some(json!(3)));
    }

    #[test]
    fn the_view_reads_current_values_and_timers_by_key() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = KeyedState::new(dir.path().to_path_buf());
        let view = state.view();
        assert_eq!(lookup(&view, "a"), None);
        state
            .apply(3, Some(1), values(&[("a", Some(json!({"n": 1})))]))
            .unwrap();
        state
            .apply(
                3,
                Some(2),
                Update {
                    values: Changes::new(),
                    timers: TimerChanges::from_iter([("b".to_string(), Some(50))]),
                },
            )
            .unwrap();
        // Not committed yet, and read anyway.
        assert_eq!(
            lookup(&view, "a"),
            Some(Found {
                partition: 3,
                value: Some(json!({"n": 1})),
                timer: None
            })
        );
        assert_eq!(lookup(&view, "b").unwrap().timer, Some(50));
        assert_eq!(counts(&view), [(3, 1, 1)]);
        state.revoked(3);
        assert_eq!(lookup(&view, "a"), None);
    }
}
