//! Where a stream's state is saved (keyed state, windows): a folder of the service's data
//! directory, or, with MAGE_SERVICE_STATE_URL, an object store (`s3://bucket/prefix`, `gs://`,
//! `az://`, `file:///path`) that every instance reads. A partition's state then follows the
//! partition when a rebalance moves it to another instance; with a folder, it stays with
//! the instance that had it.

use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "object-store")]
use mage_connector_objectstore::blocking::BlockingStore;

#[derive(Clone)]
pub enum StateFiles {
    Local(PathBuf),
    #[cfg(feature = "object-store")]
    Remote(Arc<BlockingStore>, String),
}

impl From<PathBuf> for StateFiles {
    fn from(dir: PathBuf) -> StateFiles {
        StateFiles::Local(dir)
    }
}

/// The store of MAGE_SERVICE_STATE_URL; None without it.
pub fn open_store() -> Result<Option<Arc<StoreHandle>>, String> {
    let Some(url) = std::env::var("MAGE_SERVICE_STATE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
    else {
        return Ok(None);
    };
    #[cfg(feature = "object-store")]
    {
        let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
        Ok(Some(Arc::new(StoreHandle(Arc::new(BlockingStore::open(
            url.trim(),
            endpoint.as_deref(),
        )?)))))
    }
    #[cfg(not(feature = "object-store"))]
    {
        Err(format!(
            "MAGE_SERVICE_STATE_URL is {url}, but this engine is built without object storage \
             (the object-store feature)"
        ))
    }
}

/// The opened state store, shared by the service's streams.
pub struct StoreHandle(#[cfg(feature = "object-store")] Arc<BlockingStore>);

impl StateFiles {
    /// The state of one pipeline: a folder under `local`, or a prefix in the store.
    pub fn for_pipeline(
        store: Option<&Arc<StoreHandle>>,
        local: PathBuf,
        service: &str,
        pipeline: &str,
    ) -> StateFiles {
        match store {
            #[cfg(feature = "object-store")]
            Some(handle) => StateFiles::Remote(
                handle.0.clone(),
                format!(
                    "{}/{}",
                    crate::executor::sanitize(service),
                    crate::executor::sanitize(pipeline)
                ),
            ),
            _ => {
                let _ = (service, pipeline, store);
                StateFiles::Local(local)
            }
        }
    }

    /// A folder or prefix inside this one.
    pub fn child(&self, name: &str) -> StateFiles {
        match self {
            StateFiles::Local(dir) => StateFiles::Local(dir.join(name)),
            #[cfg(feature = "object-store")]
            StateFiles::Remote(store, prefix) => {
                StateFiles::Remote(store.clone(), format!("{prefix}/{name}"))
            }
        }
    }

    /// Where a file is, for messages.
    pub fn describe(&self, name: &str) -> String {
        match self {
            StateFiles::Local(dir) => dir.join(name).display().to_string(),
            #[cfg(feature = "object-store")]
            StateFiles::Remote(store, prefix) => store.describe(&format!("{prefix}/{name}")),
        }
    }

    /// A saved file; None when there is none.
    pub fn read(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        match self {
            StateFiles::Local(dir) => {
                let file = dir.join(name);
                match std::fs::read(&file) {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(error) => Err(format!("reading {}: {error}", file.display())),
                }
            }
            #[cfg(feature = "object-store")]
            StateFiles::Remote(store, prefix) => store.get(&format!("{prefix}/{name}")),
        }
    }

    /// Every file under this folder or prefix, at any depth, relative to it and sorted;
    /// files being written (`.tmp`) are left out.
    pub fn list(&self) -> Result<Vec<String>, String> {
        match self {
            StateFiles::Local(dir) => {
                let mut found = Vec::new();
                let mut folders = vec![dir.clone()];
                while let Some(folder) = folders.pop() {
                    let entries = match std::fs::read_dir(&folder) {
                        Ok(entries) => entries,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(error) => return Err(format!("listing {}: {error}", folder.display())),
                    };
                    for entry in entries {
                        let path = entry.map_err(|e| e.to_string())?.path();
                        if path.is_dir() {
                            folders.push(path);
                        } else if path.extension().is_none_or(|e| e != "tmp")
                            && let Ok(relative) = path.strip_prefix(dir)
                        {
                            found.push(relative.to_string_lossy().replace('\\', "/"));
                        }
                    }
                }
                found.sort();
                Ok(found)
            }
            #[cfg(feature = "object-store")]
            StateFiles::Remote(store, prefix) => store.list(prefix),
        }
    }

    /// Copies every file here to `target` under the same names, except those `skip` names;
    /// returns how many were copied.
    pub fn copy_to(
        &self,
        target: &StateFiles,
        skip: impl Fn(&str) -> bool,
    ) -> Result<usize, String> {
        let mut copied = 0;
        for name in self.list()? {
            if skip(&name) {
                continue;
            }
            let Some(bytes) = self.read(&name)? else {
                continue;
            };
            let (folder, file) = match name.rsplit_once('/') {
                Some((folder, file)) => (Some(folder), file),
                None => (None, name.as_str()),
            };
            match folder {
                Some(folder) => target.child(folder).write(file, &bytes)?,
                None => target.write(file, &bytes)?,
            }
            copied += 1;
        }
        Ok(copied)
    }

    /// Saves a file whole: a reader finds the old one or the new one, never part of it.
    pub fn write(&self, name: &str, bytes: &[u8]) -> Result<(), String> {
        match self {
            StateFiles::Local(dir) => {
                std::fs::create_dir_all(dir)
                    .map_err(|e| format!("creating {}: {e}", dir.display()))?;
                let file = dir.join(name);
                let temporary = file.with_extension("json.tmp");
                {
                    use std::io::Write;
                    let mut handle = std::fs::File::create(&temporary)
                        .map_err(|e| format!("writing {}: {e}", temporary.display()))?;
                    handle.write_all(bytes).map_err(|e| e.to_string())?;
                    handle.sync_all().map_err(|e| e.to_string())?;
                }
                std::fs::rename(&temporary, &file)
                    .map_err(|e| format!("saving {}: {e}", file.display()))
            }
            #[cfg(feature = "object-store")]
            StateFiles::Remote(store, prefix) => {
                store.put(&format!("{prefix}/{name}"), bytes.to_vec())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_folder_lists_and_copies_its_files_at_every_depth() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateFiles::Local(dir.path().join("state"));
        state
            .child("count")
            .write("partition-0.json", b"{\"a\": 1}")
            .unwrap();
        state
            .child("count")
            .write("partition-1.json", b"{}")
            .unwrap();
        state
            .child("totals")
            .write("partition-0.json", b"[]")
            .unwrap();
        state.write("top.json", b"1").unwrap();
        assert_eq!(
            state.list().unwrap(),
            vec![
                "count/partition-0.json",
                "count/partition-1.json",
                "top.json",
                "totals/partition-0.json"
            ]
        );
        let copy = StateFiles::Local(dir.path().join("savepoints/first"));
        let copied = state.copy_to(&copy, |name| name == "top.json").unwrap();
        assert_eq!(copied, 3);
        assert_eq!(
            copy.child("count")
                .read("partition-0.json")
                .unwrap()
                .unwrap(),
            b"{\"a\": 1}"
        );
        assert_eq!(copy.read("top.json").unwrap(), None);
        assert!(
            StateFiles::Local(dir.path().join("none"))
                .list()
                .unwrap()
                .is_empty()
        );
    }
}
