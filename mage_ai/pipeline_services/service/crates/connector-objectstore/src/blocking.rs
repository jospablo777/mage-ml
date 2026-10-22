//! Small objects read and written synchronously, for state a stream keeps (keyed state and
//! windows): the store runs on a thread of its own with its own runtime, so callers work
//! from any thread or runtime, as the run history's PostgreSQL connection does.

use std::sync::{Arc, mpsc};

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

type Job = Box<dyn FnOnce(&tokio::runtime::Runtime, &Arc<dyn ObjectStore>) + Send>;

/// A store at a URL (`s3://bucket/prefix`, `gs://`, `az://`, `file:///path`); keys are
/// relative to its prefix.
pub struct BlockingStore {
    jobs: mpsc::Sender<Job>,
    prefix: Path,
    url: String,
}

impl BlockingStore {
    /// Opens the store; credentials come from the environment, and for S3-compatible
    /// stores AWS_ENDPOINT_URL (or `endpoint`) and AWS_REGION.
    pub fn open(url: &str, endpoint: Option<&str>) -> Result<BlockingStore, String> {
        let parsed = url::Url::parse(url).map_err(|e| format!("state url {url}: {e}"))?;
        let mut config = serde_json::Map::new();
        if let Some(endpoint) = endpoint {
            config.insert("endpoint_url".into(), endpoint.into());
        }
        let (store, prefix) =
            crate::open(&parsed, &serde_json::Value::Object(config)).map_err(|e| e.to_string())?;
        let (jobs, receive) = mpsc::channel::<Job>();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("state store runtime: {e}"))?;
        std::thread::Builder::new()
            .name("state-store".into())
            .spawn(move || {
                while let Ok(job) = receive.recv() {
                    job(&runtime, &store);
                }
            })
            .map_err(|e| format!("state store thread: {e}"))?;
        Ok(BlockingStore {
            jobs,
            prefix,
            url: url.trim_end_matches('/').to_string(),
        })
    }

    fn path(&self, key: &str) -> Path {
        let mut path = self.prefix.clone();
        for part in key.split('/').filter(|p| !p.is_empty()) {
            path = path.join(part);
        }
        path
    }

    /// Where a key is, for messages.
    pub fn describe(&self, key: &str) -> String {
        format!("{}/{key}", self.url)
    }

    fn call<T: Send + 'static>(
        &self,
        work: impl FnOnce(&tokio::runtime::Runtime, &Arc<dyn ObjectStore>) -> T + Send + 'static,
    ) -> Result<T, String> {
        let (reply, answer) = mpsc::channel();
        self.jobs
            .send(Box::new(move |runtime, store| {
                let _ = reply.send(work(runtime, store));
            }))
            .map_err(|_| "the state store stopped".to_string())?;
        answer
            .recv()
            .map_err(|_| "the state store stopped".to_string())
    }

    /// The object at a key; None when there is none.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let path = self.path(key);
        let where_ = self.describe(key);
        self.call(move |runtime, store| {
            runtime.block_on(async {
                match store.get(&path).await {
                    Ok(result) => result
                        .bytes()
                        .await
                        .map(|bytes| Some(bytes.to_vec()))
                        .map_err(|e| format!("reading {where_}: {e}")),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(format!("reading {where_}: {e}")),
                }
            })
        })?
    }

    /// Writes the object at a key, whole: a reader sees the old one or the new one.
    pub fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        let path = self.path(key);
        let where_ = self.describe(key);
        self.call(move |runtime, store| {
            runtime.block_on(async {
                store
                    .put(&path, PutPayload::from(bytes))
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("writing {where_}: {e}"))
            })
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objects_round_trip_and_a_missing_one_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}/state", dir.path().display());
        let store = BlockingStore::open(&url, None).unwrap();
        assert_eq!(store.get("svc/p/b/partition-0.json").unwrap(), None);
        store
            .put("svc/p/b/partition-0.json", b"{\"a\": 1}".to_vec())
            .unwrap();
        store
            .put("svc/p/b/partition-0.json", b"{\"a\": 2}".to_vec())
            .unwrap();
        assert_eq!(
            store.get("svc/p/b/partition-0.json").unwrap().unwrap(),
            b"{\"a\": 2}"
        );
        assert!(dir.path().join("state/svc/p/b/partition-0.json").is_file());
        assert!(store.describe("k").ends_with("/state/k"));
    }
}
