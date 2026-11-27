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
