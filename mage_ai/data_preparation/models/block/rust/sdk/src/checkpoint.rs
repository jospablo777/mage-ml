//! Checkpoints inside long blocks: `context.checkpoint` keeps a block's progress across the
//! attempts of one block run, so a retried attempt resumes where the last one saved.
//!
//! ```ignore
//! let start: u64 = context.checkpoint.load_or("page", 0)?;
//! for page in start..pages {
//!     write(fetch(page)?)?;
//!     context.checkpoint.save("page", &(page + 1))?;
//! }
//! ```
//!
//! Values are JSON, at most 1 MB each: progress, not data. Each save is atomic (a temporary
//! file, fsync, a rename). The files are the ones Python blocks write
//! (`mage_ai/pipeline_services/runtime/checkpoints.py`): `<key>.json` in the block run's
//! folder, which the run removes when the block completes.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;

const MAX_BYTES: usize = 1024 * 1024;

/// A block run's checkpoints: in its folder, or in memory when the run gives none (a run
/// outside a pipeline run).
#[derive(Debug, Clone, Default)]
pub struct Checkpoint {
    dir: Option<PathBuf>,
    memory: Arc<Mutex<BTreeMap<String, String>>>,
}

fn check_key(key: &str) -> anyhow::Result<&str> {
    let valid = !key.is_empty()
        && key.len() <= 200
        && !key.starts_with('.')
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if !valid {
        bail!("A checkpoint key is 1 to 200 letters, digits, -, _ or . (not first), not {key:?}.");
    }
    Ok(key)
}

impl Checkpoint {
    pub fn in_dir(dir: Option<PathBuf>) -> Checkpoint {
        Checkpoint {
            dir,
            memory: Arc::default(),
        }
    }

    fn path(&self, dir: &Path, key: &str) -> anyhow::Result<PathBuf> {
        Ok(dir.join(format!("{}.json", check_key(key)?)))
    }

    /// The value last saved under `key` in this block run.
    pub fn load<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<Option<T>> {
        let text = match &self.dir {
            None => self
                .memory
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(check_key(key)?)
                .cloned(),
            Some(dir) => match std::fs::read_to_string(self.path(dir, key)?) {
                Ok(text) => Some(text),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => return Err(e).context(format!("reading checkpoint {key:?}")),
            },
        };
        text.map(|t| serde_json::from_str(&t).context(format!("checkpoint {key:?}")))
            .transpose()
    }

    /// The value last saved under `key`, or `default`.
    pub fn load_or<T: DeserializeOwned>(&self, key: &str, default: T) -> anyhow::Result<T> {
        Ok(self.load(key)?.unwrap_or(default))
    }

    /// Keeps `value` under `key` for the next attempt of this block run.
    pub fn save<T: Serialize>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        let text = serde_json::to_string(value).context(format!(
            "Checkpoint {key:?} takes a value that serializes to JSON"
        ))?;
        if text.len() > MAX_BYTES {
            bail!(
                "Checkpoint {key:?} is over 1 MB: keep progress in it (an offset, a cursor), and \
                 the data in the block's output or a store."
            );
        }
        let Some(dir) = &self.dir else {
            self.memory
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(check_key(key)?.to_string(), text);
            return Ok(());
        };
        let path = self.path(dir, key)?;
        std::fs::create_dir_all(dir).context("creating the checkpoints folder")?;
        let temporary = dir.join(format!(
            ".{key}.{}.{}.tmp",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let written = (|| {
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(text.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temporary, &path)?;
            // The rename is durable once the folder's entry is.
            if let Ok(folder) = std::fs::File::open(dir) {
                let _ = folder.sync_all();
            }
            Ok::<(), std::io::Error>(())
        })();
        if let Err(error) = written {
            let _ = std::fs::remove_file(&temporary);
            return Err(error).context(format!("saving checkpoint {key:?}"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_saved_value_is_there_for_the_next_attempt_and_bad_ones_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let first = Checkpoint::in_dir(Some(dir.path().join("block")));
        assert_eq!(first.load::<u64>("page").unwrap(), None);
        first.save("page", &7u64).unwrap();
        first
            .save("done", &vec!["a.csv".to_string(), "b.csv".to_string()])
            .unwrap();

        // The next attempt is another process with the same folder.
        let next = Checkpoint::in_dir(Some(dir.path().join("block")));
        assert_eq!(next.load_or("page", 0u64).unwrap(), 7);
        assert_eq!(
            next.load::<Vec<String>>("done").unwrap().unwrap(),
            ["a.csv", "b.csv"]
        );
        // The file Python writes and reads.
        assert_eq!(
            std::fs::read_to_string(dir.path().join("block/page.json")).unwrap(),
            "7"
        );
        let names: Vec<String> = std::fs::read_dir(dir.path().join("block"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().all(|n| !n.ends_with(".tmp")), "{names:?}");

        for key in ["", "../x", ".hidden", "a/b"] {
            assert!(next.save(key, &1).is_err(), "{key}");
        }
        let large = "x".repeat(MAX_BYTES + 1);
        let error = next.save("big", &large).unwrap_err().to_string();
        assert!(error.contains("over 1 MB"), "{error}");
    }

    #[test]
    fn without_a_folder_it_lasts_for_the_execution() {
        let checkpoint = Checkpoint::in_dir(None);
        checkpoint.save("n", &3).unwrap();
        assert_eq!(checkpoint.clone().load::<i32>("n").unwrap(), Some(3));
    }
}
