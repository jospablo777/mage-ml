//! Broadcast values of a stream (BROADCAST_STATE.md): named JSON values every instance of a
//! stream reads, such as thresholds, rules, flags or the model version a block scores with,
//! changed while it runs through the service API, without a restart.
//!
//! They are kept with the stream's state, `broadcast/` in the pipeline's state folder or
//! object store prefix: `values.json` holds each value with its version, when it changed and
//! who changed it, and `history/<name>/<version>.json` every version, removals included. A
//! session takes one snapshot per batch, so a batch never sees two versions of a value; each
//! instance reads `values.json` again every second, so with a shared store every instance
//! has a new version within about a second.
//!
//! Two writes to one value at the same time on two instances can both get the same version
//! (the last one wins); writes on one instance are serialized.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::state_files::StateFiles;

const FOLDER: &str = "broadcast";
const VALUES: &str = "values.json";
/// The largest value, and the largest of all values of a stream together: they go with
/// each batch to the blocks.
pub const MAX_BYTES: usize = 1024 * 1024;
const MAX_NAME: usize = 64;

/// The current values by name, as `values.json` holds them.
type Values = BTreeMap<String, Entry>;

/// A value and its version, as `values.json` and the API have them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub value: Box<RawValue>,
    pub version: u64,
    pub at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// One version in the history: a value set, or the value removed (with the value it had).
/// Its own fields, not a flattened entry: a flattened field cannot hold a raw value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub name: String,
    pub value: Box<RawValue>,
    pub version: u64,
    pub at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub removed: bool,
}

impl Change {
    fn of(name: &str, entry: &Entry, removed: bool) -> Change {
        Change {
            name: name.to_string(),
            value: entry.value.clone(),
            version: entry.version,
            at: entry.at.clone(),
            by: entry.by.clone(),
            reason: entry.reason.clone(),
            removed,
        }
    }
}

/// The values a batch runs with.
#[derive(Debug)]
pub struct Snapshot {
    /// `{"name": value, ...}`, as the blocks get it; values keep their exact text.
    pub values: Box<RawValue>,
    pub versions: BTreeMap<String, u64>,
}

impl Snapshot {
    fn of(entries: &BTreeMap<String, Entry>) -> Result<Snapshot, String> {
        let mut text = String::from("{");
        for (i, (name, entry)) in entries.iter().enumerate() {
            if i > 0 {
                text.push(',');
            }
            text.push_str(&serde_json::to_string(name).map_err(|e| e.to_string())?);
            text.push(':');
            text.push_str(entry.value.get());
        }
        text.push('}');
        Ok(Snapshot {
            values: RawValue::from_string(text).map_err(|e| e.to_string())?,
            versions: entries
                .iter()
                .map(|(name, entry)| (name.clone(), entry.version))
                .collect(),
        })
    }

    /// `threshold@3,rules@1`, for traces and logs; empty without values.
    pub fn label(&self) -> String {
        self.versions
            .iter()
            .map(|(name, version)| format!("{name}@{version}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// A batch's pinned snapshot; dropped, batches take the newest again.
pub struct Pinned(Arc<Broadcast>);

impl Drop for Pinned {
    fn drop(&mut self) {
        *self.0.pinned.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BroadcastError {
    /// The request is wrong: the name, the value or its size.
    Invalid(String),
    /// There is no value of that name.
    Missing(String),
    /// The store could not be read or written.
    Store(String),
}

impl std::fmt::Display for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BroadcastError::Invalid(m) | BroadcastError::Missing(m) | BroadcastError::Store(m) => {
                f.write_str(m)
            }
        }
    }
}

/// A broadcast value's name: lowercase letters, digits, `_` and `-`.
pub fn check_name(name: &str) -> Result<(), BroadcastError> {
    let ok = !name.is_empty()
        && name.len() <= MAX_NAME
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '-'));
    if ok {
        Ok(())
    } else {
        Err(BroadcastError::Invalid(format!(
            "{name:?} is not a broadcast name: use up to {MAX_NAME} lowercase letters, digits, \
             _ and -, such as fraud_threshold."
        )))
    }
}

/// A stream's broadcast values.
pub struct Broadcast {
    files: StateFiles,
    current: RwLock<Arc<Snapshot>>,
    /// The bytes of `values.json` the snapshot was made from, to see a change.
    read: Mutex<Option<Vec<u8>>>,
    /// The snapshot of the batch in progress, while a chain of sessions runs it: each
    /// session of the chain runs it with the same values.
    pinned: Mutex<Option<Arc<Snapshot>>>,
    writes: Mutex<()>,
}

impl Broadcast {
    /// The broadcast values kept in a stream's state, read now.
    pub fn open(state: &StateFiles) -> Result<Broadcast, String> {
        let broadcast = Broadcast {
            files: state.child(FOLDER),
            current: RwLock::new(Arc::new(Snapshot::of(&BTreeMap::new())?)),
            read: Mutex::new(None),
            pinned: Mutex::new(None),
            writes: Mutex::new(()),
        };
        broadcast.refresh()?;
        Ok(broadcast)
    }

    /// The values a batch starting now runs with: the batch's pinned snapshot in a chain,
    /// the newest otherwise.
    pub fn current(&self) -> Arc<Snapshot> {
        if let Some(pinned) = self
            .pinned
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            return pinned.clone();
        }
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Holds the newest snapshot for one batch until the guard is dropped, so every session
    /// of a chain runs the batch with the same values. A stream runs one batch at a time.
    pub fn pin(self: &Arc<Self>) -> Pinned {
        let snapshot = self.current();
        *self.pinned.lock().unwrap_or_else(|p| p.into_inner()) = Some(snapshot);
        Pinned(self.clone())
    }

    /// `values.json` as read, and its values by name.
    fn entries(&self) -> Result<(Option<Vec<u8>>, Values), String> {
        let bytes = self.files.read(VALUES)?;
        let entries = match &bytes {
            Some(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| format!("{}: {e}", self.files.describe(VALUES)))?,
            None => BTreeMap::new(),
        };
        Ok((bytes, entries))
    }

    /// Reads the values again; returns the new snapshot when they changed.
    pub fn refresh(&self) -> Result<Option<Arc<Snapshot>>, String> {
        let (bytes, entries) = self.entries()?;
        let mut read = self.read.lock().unwrap_or_else(|p| p.into_inner());
        if *read == bytes {
            return Ok(None);
        }
        let snapshot = Arc::new(Snapshot::of(&entries)?);
        *self.current.write().unwrap_or_else(|p| p.into_inner()) = snapshot.clone();
        *read = bytes;
        Ok(Some(snapshot))
    }

    /// The current values with their versions.
    pub fn list(&self) -> Result<BTreeMap<String, Entry>, String> {
        Ok(self.entries()?.1)
    }

    /// Every version of a value, oldest first.
    pub fn history(&self, name: &str) -> Result<Vec<Change>, BroadcastError> {
        check_name(name)?;
        let folder = self.files.child("history").child(name);
        let mut changes = Vec::new();
        for file in folder.list().map_err(BroadcastError::Store)? {
            if let Some(bytes) = folder.read(&file).map_err(BroadcastError::Store)? {
                changes.push(
                    serde_json::from_slice(&bytes)
                        .map_err(|e| BroadcastError::Store(format!("{file}: {e}")))?,
                );
            }
        }
        Ok(changes)
    }

    /// The last version a value had, removed or not; 0 for a new name.
    fn last_version(
        &self,
        name: &str,
        entries: &BTreeMap<String, Entry>,
    ) -> Result<u64, BroadcastError> {
        if let Some(entry) = entries.get(name) {
            return Ok(entry.version);
        }
        let files = self
            .files
            .child("history")
            .child(name)
            .list()
            .map_err(BroadcastError::Store)?;
        Ok(files
            .iter()
            .filter_map(|f| f.strip_suffix(".json")?.parse::<u64>().ok())
            .max()
            .unwrap_or(0))
    }

    fn record(&self, change: &Change) -> Result<(), BroadcastError> {
        let bytes = serde_json::to_vec(change).map_err(|e| BroadcastError::Store(e.to_string()))?;
        self.files
            .child("history")
            .child(&change.name)
            .write(&format!("{:010}.json", change.version), &bytes)
            .map_err(BroadcastError::Store)
    }

    fn save(&self, entries: &BTreeMap<String, Entry>) -> Result<(), BroadcastError> {
        let bytes =
            serde_json::to_vec_pretty(entries).map_err(|e| BroadcastError::Store(e.to_string()))?;
        self.files
            .write(VALUES, &bytes)
            .map_err(BroadcastError::Store)?;
        self.refresh().map_err(BroadcastError::Store)?;
        Ok(())
    }

    /// Sets a value: its next version, recorded in the history before it takes effect.
    pub fn put(
        &self,
        name: &str,
        value: Box<RawValue>,
        by: Option<String>,
        reason: Option<String>,
    ) -> Result<Entry, BroadcastError> {
        check_name(name)?;
        if value.get().len() > MAX_BYTES {
            return Err(BroadcastError::Invalid(format!(
                "broadcast {name} is {} bytes; a value is at most {MAX_BYTES} bytes, as it goes \
                 with each batch to the blocks.",
                value.get().len()
            )));
        }
        let _write = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        let (_, mut entries) = self.entries().map_err(BroadcastError::Store)?;
        let others: usize = entries
            .iter()
            .filter(|(n, _)| n.as_str() != name)
            .map(|(_, e)| e.value.get().len())
            .sum();
        if others + value.get().len() > MAX_BYTES {
            return Err(BroadcastError::Invalid(format!(
                "the stream's broadcast values would be {} bytes together; they are at most \
                 {MAX_BYTES} bytes, as they go with each batch to the blocks.",
                others + value.get().len()
            )));
        }
        let entry = Entry {
            value,
            version: self.last_version(name, &entries)? + 1,
            at: chrono::Utc::now().to_rfc3339(),
            by,
            reason,
        };
        self.record(&Change::of(name, &entry, false))?;
        entries.insert(name.to_string(), entry.clone());
        self.save(&entries)?;
        Ok(entry)
    }

    /// Removes a value; the history keeps it, and a later value of the name goes on from its
    /// version.
    pub fn remove(
        &self,
        name: &str,
        by: Option<String>,
        reason: Option<String>,
    ) -> Result<Entry, BroadcastError> {
        check_name(name)?;
        let _write = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        let (_, mut entries) = self.entries().map_err(BroadcastError::Store)?;
        let Some(last) = entries.remove(name) else {
            return Err(BroadcastError::Missing(format!(
                "the stream has no broadcast value {name}."
            )));
        };
        let entry = Entry {
            value: last.value,
            version: last.version + 1,
            at: chrono::Utc::now().to_rfc3339(),
            by,
            reason,
        };
        self.record(&Change::of(name, &entry, true))?;
        self.save(&entries)?;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(text: &str) -> Box<RawValue> {
        RawValue::from_string(text.to_string()).unwrap()
    }

    #[test]
    fn a_value_changes_version_by_version_and_every_version_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateFiles::Local(dir.path().to_path_buf());
        let broadcast = Broadcast::open(&state).unwrap();
        assert_eq!(broadcast.current().values.get(), "{}");

        let before = broadcast.current();
        let first = broadcast
            .put("threshold", raw("0.80"), Some("ana".into()), None)
            .unwrap();
        assert_eq!(first.version, 1);
        broadcast
            .put("rules", raw(r#"{"max": 3}"#), None, None)
            .unwrap();
        let second = broadcast
            .put(
                "threshold",
                raw("0.9000000000000000001"),
                None,
                Some("v2".into()),
            )
            .unwrap();
        assert_eq!(second.version, 2);

        // A batch that took the snapshot before still has it whole.
        assert_eq!(before.values.get(), "{}");
        let now = broadcast.current();
        // Values keep their exact text, digits included.
        assert_eq!(
            now.values.get(),
            r#"{"rules":{"max": 3},"threshold":0.9000000000000000001}"#
        );
        assert_eq!(now.label(), "rules@1,threshold@2");

        // Another instance on the same store reads the same values.
        let other = Broadcast::open(&state).unwrap();
        assert_eq!(other.current().label(), "rules@1,threshold@2");
        assert!(other.refresh().unwrap().is_none(), "nothing changed");
        broadcast.put("threshold", raw("0.95"), None, None).unwrap();
        assert_eq!(
            other.refresh().unwrap().unwrap().label(),
            "rules@1,threshold@3"
        );

        let removed = broadcast.remove("rules", None, None).unwrap();
        assert_eq!(removed.version, 2);
        assert_eq!(broadcast.current().label(), "threshold@3");
        // A value set again after its removal goes on from its last version.
        assert_eq!(
            broadcast
                .put("rules", raw("[]"), None, None)
                .unwrap()
                .version,
            3
        );

        let history = broadcast.history("threshold").unwrap();
        let versions: Vec<_> = history.iter().map(|c| c.version).collect();
        assert_eq!(versions, vec![1, 2, 3]);
        assert_eq!(history[0].by.as_deref(), Some("ana"));
        assert_eq!(history[0].value.get(), "0.80");
        let rules: Vec<_> = broadcast
            .history("rules")
            .unwrap()
            .iter()
            .map(|c| (c.version, c.removed))
            .collect();
        assert_eq!(rules, vec![(1, false), (2, true), (3, false)]);
    }

    #[test]
    fn a_chain_runs_its_batch_with_the_values_it_started_with() {
        let dir = tempfile::tempdir().unwrap();
        let broadcast =
            Arc::new(Broadcast::open(&StateFiles::Local(dir.path().to_path_buf())).unwrap());
        broadcast.put("threshold", raw("1"), None, None).unwrap();
        let pinned = broadcast.pin();
        // Set while the batch's first session ran: its later sessions still get version 1.
        broadcast.put("threshold", raw("2"), None, None).unwrap();
        assert_eq!(broadcast.current().label(), "threshold@1");
        drop(pinned);
        assert_eq!(broadcast.current().label(), "threshold@2");
    }

    #[test]
    fn names_sizes_and_missing_values_are_explained() {
        let dir = tempfile::tempdir().unwrap();
        let broadcast = Broadcast::open(&StateFiles::Local(dir.path().to_path_buf())).unwrap();
        for name in ["", "Threshold", "../x", "a.b"] {
            assert!(
                matches!(check_name(name), Err(BroadcastError::Invalid(_))),
                "{name}"
            );
        }
        let big = format!("\"{}\"", "x".repeat(MAX_BYTES));
        assert!(matches!(
            broadcast.put("big", raw(&big), None, None),
            Err(BroadcastError::Invalid(m)) if m.contains("at most")
        ));
        let half = format!("\"{}\"", "x".repeat(MAX_BYTES / 2 + 10));
        broadcast.put("a", raw(&half), None, None).unwrap();
        assert!(matches!(
            broadcast.put("b", raw(&half), None, None),
            Err(BroadcastError::Invalid(m)) if m.contains("together")
        ));
        // Replacing a value counts its new size, not both.
        broadcast.put("a", raw(&half), None, None).unwrap();
        assert!(matches!(
            broadcast.remove("nope", None, None),
            Err(BroadcastError::Missing(_))
        ));
        assert!(broadcast.current().versions.contains_key("a"));
    }

    #[test]
    fn a_damaged_values_file_is_reported_and_the_last_snapshot_kept() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateFiles::Local(dir.path().to_path_buf());
        let broadcast = Broadcast::open(&state).unwrap();
        broadcast.put("threshold", raw("1"), None, None).unwrap();
        state.child(FOLDER).write(VALUES, b"{not json").unwrap();
        let error = broadcast.refresh().unwrap_err();
        assert!(error.contains("values.json"), "{error}");
        assert_eq!(broadcast.current().label(), "threshold@1");
    }
}
