//! Savepoints of a stream's state (SAVEPOINTS.md): a named copy of a stopped stream's state
//! files (keyed state, open windows) and of the Kafka offsets that state reflects, to start a
//! new version from, roll back to, or fork.
//!
//! `mage-service savepoint <name>` takes one of a stopped service: after a graceful stop the
//! state files and the consumer group's committed offsets agree. It is kept with the state,
//! `savepoints/<name>/` in the pipeline's state folder or object store prefix: `state/` holds
//! the files and `positions.json` the offsets.
//!
//! `POST /v1/streams/{pipeline}/savepoints` takes one of a running stream ([`take_running`]):
//! it pauses the stream, waits until it is settled (nothing in flight, every position done
//! committed and its state saved first), takes the savepoint as of that point, and resumes
//! it, unless it was paused before.
//!
//! With MAGE_SERVICE_START_FROM_SAVEPOINT=<name>, a stream starts from it: its state files
//! become the savepoint's (newer ones removed), and each partition starts at the savepoint's
//! offset when it is first assigned. `restored.json` records the savepoint restored, so a
//! later restart with the setting still there goes on from where the stream got to instead
//! of rewinding again.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use mage_stream_ports::{ConnectorError, StartPositions};
use serde::{Deserialize, Serialize};

use crate::state_files::StateFiles;

pub const START_FROM: &str = "MAGE_SERVICE_START_FROM_SAVEPOINT";
const FOLDER: &str = "savepoints";
const RESTORED: &str = "restored.json";

#[derive(Debug, Serialize, Deserialize)]
struct Positions {
    savepoint: String,
    taken_at: String,
    /// Each partition's next offset, by `topic/partition`.
    partitions: BTreeMap<String, i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Restored {
    savepoint: String,
    at: String,
}

/// A savepoint name: letters, digits, `_`, `-` and `.`, as a folder name.
pub fn check_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= 100
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "{name:?} is not a savepoint name: use letters, digits, _, - and ., such as \
             before-v2."
        ))
    }
}

/// The files that are the stream's state, not its savepoints or their bookkeeping, nor its
/// broadcast values, which are set from outside and not rolled back with it.
fn is_state(name: &str) -> bool {
    !name.starts_with(&format!("{FOLDER}/")) && !name.starts_with("broadcast/") && name != RESTORED
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// What a savepoint took: state files copied and partitions with an offset.
#[derive(Debug, PartialEq, Eq)]
pub struct Taken {
    pub files: usize,
    pub partitions: usize,
}

/// Takes savepoint `name` of a stopped stream: its state files, and `positions`, the next
/// offset of each partition.
pub fn take(
    state: &StateFiles,
    name: &str,
    positions: Vec<(String, i64)>,
) -> Result<Taken, String> {
    check_name(name)?;
    let saved = state.child(FOLDER).child(name);
    if saved.read("positions.json")?.is_some() {
        return Err(format!(
            "savepoint {name} exists already ({}); take it under another name.",
            saved.describe("positions.json")
        ));
    }
    let files = state.copy_to(&saved.child("state"), |n| !is_state(n))?;
    let partitions = positions.len();
    let document = Positions {
        savepoint: name.to_string(),
        taken_at: now(),
        partitions: positions.into_iter().collect(),
    };
    saved.write(
        "positions.json",
        &serde_json::to_vec_pretty(&document).map_err(|e| e.to_string())?,
    )?;
    Ok(Taken { files, partitions })
}

/// Starts the stream from savepoint `name` unless it was restored already: its state files
/// replace the stream's, and the returned start positions start each partition at the
/// savepoint's offset when it is first assigned. None when it was restored before.
pub fn restore(state: &StateFiles, name: &str) -> Result<Option<Arc<dyn StartPositions>>, String> {
    check_name(name)?;
    if let Some(bytes) = state.read(RESTORED)?
        && let Ok(restored) = serde_json::from_slice::<Restored>(&bytes)
        && restored.savepoint == name
    {
        return Ok(None);
    }
    let saved = state.child(FOLDER).child(name);
    let bytes = saved.read("positions.json")?.ok_or_else(|| {
        format!(
            "there is no savepoint {name} ({}); take one with `mage-service savepoint {name}`.",
            saved.describe("positions.json")
        )
    })?;
    let positions: Positions = serde_json::from_slice(&bytes)
        .map_err(|e| format!("savepoint {name}: positions.json: {e}"))?;
    let kept: HashSet<String> = saved.child("state").list()?.into_iter().collect();
    for current in state.list()? {
        if is_state(&current) && !kept.contains(&current) {
            // Newer than the savepoint: its state would be past the restored offsets.
            state.remove(&current)?;
        }
    }
    saved.child("state").copy_to(state, |_| false)?;
    state.write(
        RESTORED,
        &serde_json::to_vec(&Restored {
            savepoint: name.to_string(),
            at: now(),
        })
        .map_err(|e| e.to_string())?,
    )?;
    Ok(Some(Arc::new(FromSavepoint {
        positions: positions.partitions.into_iter().collect(),
        started: Mutex::new(HashSet::new()),
    })))
}

/// Each partition at the savepoint's offset when it is first assigned; later assignments
/// (a rebalance that gives it back) go on from the group's committed offset.
struct FromSavepoint {
    positions: HashMap<String, i64>,
    started: Mutex<HashSet<String>>,
}

impl StartPositions for FromSavepoint {
    fn claim(&self, partitions: &[String]) -> Result<Vec<Option<i64>>, ConnectorError> {
        let mut started = self.started.lock().unwrap_or_else(|p| p.into_inner());
        Ok(partitions
            .iter()
            .map(|name| {
                if started.insert(name.clone()) {
                    self.positions.get(name).copied()
                } else {
                    None
                }
            })
            .collect())
    }
}

/// Why a savepoint of a running stream was not taken.
#[derive(Debug)]
pub enum Refusal {
    /// The request is wrong: the name, or a stream that cannot have savepoints.
    Invalid(String),
    /// The service runs no such stream.
    Missing(String),
    /// The stream did not settle in time; it was resumed.
    Unsettled(String),
    /// Reading the offsets or writing the savepoint failed.
    Failed(String),
}

/// How long a running stream has to settle for a savepoint: its batches in flight to finish
/// and their positions to be committed.
const SETTLE_WITHIN: std::time::Duration = std::time::Duration::from_secs(120);

/// Resumes a stream paused for a savepoint, on every way out, unless it was paused before.
struct Resume(Option<tokio::sync::watch::Sender<bool>>);

impl Drop for Resume {
    fn drop(&mut self) {
        if let Some(switch) = self.0.take() {
            let _ = switch.send(false);
        }
    }
}

/// Takes savepoint `name` of a stream while the service runs: pauses it, waits until it is
/// settled, copies its state and the committed offsets, and resumes it. Returns what was
/// taken and how long the stream was paused for it.
pub async fn take_running(
    service: &Arc<crate::service::Service>,
    pipeline: &str,
    name: &str,
) -> Result<(Taken, std::time::Duration), Refusal> {
    use mage_service_core::manifest::ConnectorKind;

    check_name(name).map_err(Refusal::Invalid)?;
    let stream = service
        .manifest
        .pipelines
        .iter()
        .find(|p| p.uuid == pipeline)
        .and_then(|p| p.stream.as_ref())
        .ok_or_else(|| Refusal::Missing(format!("The service has no stream {pipeline}.")))?;
    if stream.source.kind != ConnectorKind::Kafka {
        return Err(Refusal::Invalid(format!(
            "Savepoints keep Kafka offsets; stream {pipeline} reads {}.",
            stream.source.kind.as_str()
        )));
    }
    let switch = service
        .stream_pause
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(pipeline)
        .cloned()
        .ok_or_else(|| Refusal::Missing(format!("Stream {pipeline} is not running here.")))?;
    let status = service
        .streams
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(pipeline)
        .cloned()
        .ok_or_else(|| Refusal::Missing(format!("Stream {pipeline} is not running here.")))?;
    let paused_before = *switch.borrow();
    let _resume = Resume((!paused_before).then(|| switch.clone()));
    if !paused_before {
        let _ = switch.send(true);
    }
    let started = std::time::Instant::now();
    loop {
        // A stream that was already paused reports settled at once; one paused now does so
        // once its batches in flight are done and committed.
        let settled = status.lock().map(|s| s.settled).unwrap_or(false);
        if settled && (paused_before || started.elapsed() > std::time::Duration::from_millis(50)) {
            break;
        }
        if started.elapsed() > SETTLE_WITHIN {
            return Err(Refusal::Unsettled(format!(
                "Stream {pipeline} did not settle within {} seconds (its batches in flight \
                 finishing and their positions committed); it was resumed and no savepoint \
                 was taken.",
                SETTLE_WITHIN.as_secs()
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let lookup = |key: &str| {
        service
            .extra_env
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .or_else(|| std::env::var(key).ok())
    };
    let config = mage_service_core::io_config::render(
        &serde_json::Value::Object(stream.source.config.clone()),
        &lookup,
    );
    let state = service.state_files(pipeline);
    let name = name.to_string();
    let taken = tokio::task::spawn_blocking(move || -> Result<Taken, Refusal> {
        #[cfg(feature = "kafka")]
        let positions = mage_connector_kafka::committed_positions(&config)
            .map_err(|e| Refusal::Failed(e.message))?;
        #[cfg(not(feature = "kafka"))]
        let positions: Vec<(String, i64)> = {
            let _ = config;
            return Err(Refusal::Invalid(
                "savepoints need the engine built with Kafka (the kafka feature).".into(),
            ));
        };
        take(&state, &name, positions).map_err(|e| {
            if e.contains("exists already") {
                Refusal::Invalid(e)
            } else {
                Refusal::Failed(e)
            }
        })
    })
    .await
    .map_err(|e| Refusal::Failed(e.to_string()))??;
    Ok((taken, started.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_starts_once_from_its_savepoint_with_its_state_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateFiles::Local(dir.path().join("state"));
        state
            .child("count")
            .write("partition-0.json", b"{\"n\": 10}")
            .unwrap();
        let taken = take(&state, "before-v2", vec![("orders/0".into(), 10)]).unwrap();
        assert_eq!(
            taken,
            Taken {
                files: 1,
                partitions: 1
            }
        );
        assert!(
            take(&state, "before-v2", vec![])
                .unwrap_err()
                .contains("exists already")
        );

        // The stream went on, and made a partition the savepoint does not have.
        state
            .child("count")
            .write("partition-0.json", b"{\"n\": 25}")
            .unwrap();
        state
            .child("count")
            .write("partition-1.json", b"{\"n\": 3}")
            .unwrap();

        let starts = restore(&state, "before-v2").unwrap().expect("restored");
        assert_eq!(
            state
                .child("count")
                .read("partition-0.json")
                .unwrap()
                .unwrap(),
            b"{\"n\": 10}"
        );
        assert_eq!(state.child("count").read("partition-1.json").unwrap(), None);
        let names = vec!["orders/0".to_string(), "orders/1".to_string()];
        assert_eq!(starts.claim(&names).unwrap(), vec![Some(10), None]);
        // Assigned again later: from the group's offsets, not back to the savepoint.
        assert_eq!(starts.claim(&names).unwrap(), vec![None, None]);
        // A restart with the setting still there does not rewind.
        assert!(restore(&state, "before-v2").unwrap().is_none());
        // The savepoint is still there to roll back to under another restore.
        assert!(
            state
                .list()
                .unwrap()
                .contains(&"savepoints/before-v2/positions.json".into())
        );
    }

    #[test]
    fn a_restore_keeps_the_broadcast_values_set_since() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateFiles::Local(dir.path().to_path_buf());
        let broadcast = crate::broadcast::Broadcast::open(&state).unwrap();
        let value = |text: &str| serde_json::value::RawValue::from_string(text.into()).unwrap();
        broadcast.put("threshold", value("1"), None, None).unwrap();
        let taken = take(&state, "v1", vec![]).unwrap();
        assert_eq!(taken.files, 0, "broadcast values are not stream state");
        broadcast.put("threshold", value("2"), None, None).unwrap();
        restore(&state, "v1").unwrap();
        broadcast.refresh().unwrap();
        assert_eq!(broadcast.current().label(), "threshold@2");
        assert_eq!(broadcast.history("threshold").unwrap().len(), 2);
    }

    #[test]
    fn names_and_missing_savepoints_are_explained() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateFiles::Local(dir.path().to_path_buf());
        assert!(check_name("../x").is_err());
        assert!(check_name("v2.1-rollback").is_ok());
        let error = restore(&state, "nope").err().unwrap();
        assert!(error.contains("there is no savepoint nope"), "{error}");
    }
}
