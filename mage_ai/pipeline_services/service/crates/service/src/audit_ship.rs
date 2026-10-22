//! Copies the audit records (audit.rs) to an object store, MAGE_SERVICE_AUDIT_URL
//! (`s3://bucket/prefix`, `gs://`, `az://`, `file:///path`), or to a PostgreSQL table
//! (`postgres://`, audit_pg.rs). The files are on the instance's disk, which a container
//! loses when it restarts or moves.
//!
//! Every MAGE_SERVICE_AUDIT_SHIP_SECONDS (30) and when the service stops, the complete lines
//! added to each file since the last copy go up as one object,
//! `<service>/<instance>/<model>/<file>/<first line>.jsonl`, the first line counted from 0
//! and padded so the names sort in order. Joined in that order, a file's objects are the
//! file, so its hash chain checks as it does on disk. `<file>.shipped` beside a file holds
//! how far it was copied; a copy repeated after a crash writes the same object again.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// One object holds at most this much of a file; a larger backlog goes up in several.
const MAX_OBJECT_BYTES: u64 = 64 * 1024 * 1024;

/// What one pass copied.
#[derive(Debug, Default, PartialEq)]
pub struct Shipped {
    pub objects: u64,
    pub records: u64,
    pub errors: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
struct Progress {
    bytes: u64,
    lines: u64,
}

/// Writes objects at keys relative to the store's prefix.
pub trait Store: Send + Sync {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String>;
}

#[cfg(feature = "object-store")]
impl Store for mage_connector_objectstore::blocking::BlockingStore {
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        mage_connector_objectstore::blocking::BlockingStore::put(self, key, bytes)
    }
}

pub struct Shipper {
    store: Arc<dyn Store>,
    audit_dir: PathBuf,
    /// `<service>/<instance>`.
    prefix: String,
}

/// The shipper of MAGE_SERVICE_AUDIT_URL; None without it.
pub fn open(audit_dir: PathBuf, service: &str, instance: &str) -> Result<Option<Shipper>, String> {
    let Some(url) = std::env::var("MAGE_SERVICE_AUDIT_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
    else {
        return Ok(None);
    };
    let url = url.trim().to_string();
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let table = std::env::var("MAGE_SERVICE_AUDIT_TABLE")
            .ok()
            .filter(|t| !t.trim().is_empty())
            .unwrap_or_else(|| "mage_audit_records".into());
        let store = crate::audit_pg::PgStore::connect(&url, table.trim())?;
        return Ok(Some(Shipper::new(
            Arc::new(store),
            audit_dir,
            service,
            instance,
        )));
    }
    #[cfg(feature = "object-store")]
    {
        let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
        let store = mage_connector_objectstore::blocking::BlockingStore::open(
            url.trim(),
            endpoint.as_deref(),
        )?;
        Ok(Some(Shipper::new(
            Arc::new(store),
            audit_dir,
            service,
            instance,
        )))
    }
    #[cfg(not(feature = "object-store"))]
    {
        let _ = (audit_dir, service, instance);
        Err(format!(
            "MAGE_SERVICE_AUDIT_URL is {url}, but this engine is built without object storage \
             (the object-store feature)"
        ))
    }
}

/// How often the records are copied: MAGE_SERVICE_AUDIT_SHIP_SECONDS, 30 by default.
pub fn interval() -> std::time::Duration {
    let seconds = std::env::var("MAGE_SERVICE_AUDIT_SHIP_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|s| *s > 0)
        .unwrap_or(30);
    std::time::Duration::from_secs(seconds)
}

fn progress_path(file: &Path) -> PathBuf {
    let mut name = file.as_os_str().to_owned();
    name.push(".shipped");
    PathBuf::from(name)
}

fn read_progress(file: &Path) -> Progress {
    std::fs::read(progress_path(file))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn write_progress(file: &Path, progress: &Progress) -> Result<(), String> {
    let path = progress_path(file);
    let temporary = path.with_extension("shipped.tmp");
    let bytes = serde_json::to_vec(progress).map_err(|e| e.to_string())?;
    std::fs::write(&temporary, bytes)
        .and_then(|_| std::fs::rename(&temporary, &path))
        .map_err(|e| format!("saving {}: {e}", path.display()))
}

/// The bytes of `file` from `start` up to its last complete line, at most `limit`.
fn complete_lines(file: &Path, start: u64, limit: u64) -> Result<Vec<u8>, String> {
    let mut handle =
        std::fs::File::open(file).map_err(|e| format!("reading {}: {e}", file.display()))?;
    handle
        .seek(SeekFrom::Start(start))
        .map_err(|e| format!("reading {}: {e}", file.display()))?;
    let mut bytes = Vec::new();
    handle
        .take(limit)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("reading {}: {e}", file.display()))?;
    // A worker may be writing the last line.
    match bytes.iter().rposition(|b| *b == b'\n') {
        Some(end) => bytes.truncate(end + 1),
        None => bytes.clear(),
    }
    Ok(bytes)
}

impl Shipper {
    pub fn new(store: Arc<dyn Store>, audit_dir: PathBuf, service: &str, instance: &str) -> Self {
        Shipper {
            store,
            audit_dir,
            prefix: format!(
                "{}/{}",
                crate::executor::sanitize(service),
                crate::executor::sanitize(instance)
            ),
        }
    }

    /// Copies what every audit file gained since the last pass.
    pub fn ship(&self) -> Shipped {
        let mut shipped = Shipped::default();
        let models = std::fs::read_dir(&self.audit_dir)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter(|e| e.path().is_dir());
        for model in models {
            let model_name = model.file_name().to_string_lossy().into_owned();
            let mut files: Vec<PathBuf> = std::fs::read_dir(model.path())
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
                .collect();
            files.sort();
            for file in files {
                if let Err(error) = self.ship_file(&model_name, &file, &mut shipped) {
                    shipped.errors.push(error);
                }
            }
        }
        shipped
    }

    fn ship_file(&self, model: &str, file: &Path, shipped: &mut Shipped) -> Result<(), String> {
        let Some(stem) = file.file_stem().map(|s| s.to_string_lossy().into_owned()) else {
            return Ok(());
        };
        let mut progress = read_progress(file);
        loop {
            let size = std::fs::metadata(file)
                .map_err(|e| format!("reading {}: {e}", file.display()))?
                .len();
            if size <= progress.bytes {
                return Ok(());
            }
            let bytes = complete_lines(file, progress.bytes, MAX_OBJECT_BYTES)?;
            if bytes.is_empty() {
                return Ok(());
            }
            let lines = bytes.iter().filter(|b| **b == b'\n').count() as u64;
            let length = bytes.len() as u64;
            let key = format!(
                "{}/{}/{stem}/{:012}.jsonl",
                self.prefix,
                crate::executor::sanitize(model),
                progress.lines
            );
            self.store.put(&key, bytes)?;
            progress.bytes += length;
            progress.lines += lines;
            write_progress(file, &progress)?;
            shipped.objects += 1;
            shipped.records += lines;
        }
    }
}

/// Whether every complete line of `file` is copied; true without a copy record only for a
/// file with none.
pub fn is_shipped(file: &Path) -> bool {
    let size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
    read_progress(file).bytes >= size
}

/// Removes the copy record of a removed audit file.
pub fn forget(file: &Path) {
    let _ = std::fs::remove_file(progress_path(file));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Memory {
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        fail: Mutex<bool>,
    }

    impl Store for Memory {
        fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
            if *self.fail.lock().unwrap() {
                return Err("store unavailable".into());
            }
            self.objects.lock().unwrap().insert(key.into(), bytes);
            Ok(())
        }
    }

    fn append(file: &Path, text: &str) {
        let mut handle = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file)
            .unwrap();
        handle.write_all(text.as_bytes()).unwrap();
    }

    fn setup() -> (tempfile::TempDir, Arc<Memory>, Shipper, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let model_dir = dir.path().join("churn");
        std::fs::create_dir_all(&model_dir).unwrap();
        let store = Arc::new(Memory::default());
        let shipper = Shipper::new(store.clone(), dir.path().to_path_buf(), "scores", "pod-1");
        (dir, store, shipper, model_dir.join("2026-10-22-41.jsonl"))
    }

    fn joined(store: &Memory) -> String {
        store
            .objects
            .lock()
            .unwrap()
            .values()
            .map(|b| String::from_utf8(b.clone()).unwrap())
            .collect()
    }

    #[test]
    fn complete_lines_go_up_in_order_and_a_partial_one_waits() {
        let (_dir, store, shipper, file) = setup();
        append(&file, "{\"n\":1}\n{\"n\":2}\n{\"n\":");

        let first = shipper.ship();
        assert_eq!((first.objects, first.records), (1, 2));
        append(&file, "3}\n{\"n\":4}\n");
        let second = shipper.ship();
        assert_eq!((second.objects, second.records), (1, 2));
        assert_eq!(shipper.ship(), Shipped::default());

        let keys: Vec<String> = store.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(
            keys,
            [
                "scores/pod-1/churn/2026-10-22-41/000000000000.jsonl",
                "scores/pod-1/churn/2026-10-22-41/000000000002.jsonl",
            ]
        );
        assert_eq!(
            joined(&store),
            std::fs::read_to_string(&file).unwrap(),
            "the objects joined are the file"
        );
        assert!(is_shipped(&file));
    }

    #[test]
    fn a_failed_copy_is_retried_from_where_it_stopped() {
        let (_dir, store, shipper, file) = setup();
        append(&file, "{\"n\":1}\n");
        *store.fail.lock().unwrap() = true;

        let failed = shipper.ship();
        assert_eq!(failed.errors.len(), 1);
        assert!(!is_shipped(&file));

        *store.fail.lock().unwrap() = false;
        append(&file, "{\"n\":2}\n");
        let retried = shipper.ship();
        assert_eq!((retried.objects, retried.records), (1, 2));
        assert_eq!(joined(&store), "{\"n\":1}\n{\"n\":2}\n");
    }

    #[test]
    fn a_restarted_service_continues_from_the_copy_record() {
        let (dir, store, shipper, file) = setup();
        append(&file, "{\"n\":1}\n");
        shipper.ship();
        drop(shipper);

        append(&file, "{\"n\":2}\n");
        let restarted = Shipper::new(store.clone(), dir.path().to_path_buf(), "scores", "pod-1");
        let shipped = restarted.ship();

        assert_eq!(shipped.records, 1);
        assert_eq!(store.objects.lock().unwrap().len(), 2);
        assert_eq!(joined(&store), "{\"n\":1}\n{\"n\":2}\n");
    }

    #[test]
    fn retention_keeps_a_file_until_it_is_copied() {
        let (dir, _store, shipper, _file) = setup();
        let old = dir.path().join("churn").join("2001-01-01-7.jsonl");
        append(&old, "{\"n\":1}\n");

        assert_eq!(crate::audit::prune(dir.path(), 30, true), 0);
        assert!(old.exists());

        shipper.ship();
        assert_eq!(crate::audit::prune(dir.path(), 30, true), 1);
        assert!(!old.exists());
        assert!(
            !progress_path(&old).exists(),
            "the copy record goes with the file"
        );
    }

    #[test]
    fn the_chain_checks_on_the_joined_objects() {
        let (dir, store, shipper, file) = setup();
        let mut previous: Option<String> = None;
        for n in 0..5 {
            let line = serde_json::json!({"key": n, "previous": previous}).to_string();
            append(&file, &format!("{line}\n"));
            previous = Some(crate::audit::sha2_lite::sha256_hex(line.as_bytes()));
            shipper.ship();
        }

        let copy = dir.path().join("copy").join("churn");
        std::fs::create_dir_all(&copy).unwrap();
        std::fs::write(copy.join("2026-10-22-41.jsonl"), joined(&store)).unwrap();
        let chains = crate::audit::verify(&dir.path().join("copy"), "churn");
        assert_eq!(chains.len(), 1);
        assert_eq!((chains[0].records, chains[0].broken_at), (5, None));
    }
}
