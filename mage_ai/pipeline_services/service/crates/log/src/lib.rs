//! Logs: the service's own events and every line blocks print.
//!
//! Each line goes to stdout (stderr for `mage-service run`), where `docker logs` and log
//! collectors read it, as text or as JSON (`MAGE_SERVICE_LOG_FORMAT=json`), and a run's lines also go to its log file, which
//! `GET /v1/runs/{id}/logs` returns. Nothing is shortened or dropped.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::json;

use mage_service_core::time::{now, timestamp};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Format {
    Text,
    Json,
}

static FORMAT: OnceLock<Format> = OnceLock::new();
static REDACTED: OnceLock<Vec<String>> = OnceLock::new();

/// Secret values to replace with `***` in every log line and stored error.
pub fn set_redactions(values: Vec<String>) {
    let _ = REDACTED.set(values);
}

pub fn redact(text: &str) -> String {
    match REDACTED.get() {
        Some(values) if !values.is_empty() => mage_service_core::redact::redact(text, values),
        _ => text.to_string(),
    }
}
static STDOUT: Mutex<()> = Mutex::new(());
static TO_STDERR: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Sends log lines to stderr: `mage-service run` prints its summary, or its JSON, alone on
/// stdout.
pub fn to_stderr() {
    TO_STDERR.store(true, std::sync::atomic::Ordering::Relaxed);
}

pub fn init(format: Format) {
    let _ = FORMAT.set(format);
}

fn format() -> Format {
    *FORMAT.get().unwrap_or(&Format::Text)
}

fn emit(level: &str, run: Option<&str>, block: Option<&str>, stream: Option<&str>, message: &str) {
    let message = &redact(message);
    let line = match format() {
        Format::Json => json!({
            "ts": timestamp(now()),
            "level": level,
            "run": run,
            "block": block,
            "stream": stream,
            "message": message,
        })
        .to_string(),
        Format::Text => {
            let mut prefix = String::new();
            if let Some(block) = block {
                prefix.push_str(&format!("[{block}] "));
            }
            format!("{} {level:<5} {prefix}{message}", timestamp(now()))
        }
    };
    let _guard = STDOUT.lock().unwrap_or_else(|p| p.into_inner());
    if TO_STDERR.load(std::sync::atomic::Ordering::Relaxed) {
        let _ = writeln!(std::io::stderr().lock(), "{line}");
    } else {
        let _ = writeln!(std::io::stdout().lock(), "{line}");
    }
}

pub fn info(message: &str) {
    emit("info", None, None, None, message);
}

pub fn warn(message: &str) {
    emit("warn", None, None, None, message);
}

pub fn error(message: &str) {
    emit("error", None, None, None, message);
}

/// The log of one run: a JSON-lines file, also echoed to stdout.
#[derive(Clone)]
pub struct RunLog {
    run_id: Arc<str>,
    file: Arc<Mutex<LogFile>>,
}

/// A log file that, past `max_bytes`, moves to `<name>.1` (older ones to `.2` and on, up to
/// `keep`) and starts again: a stream runs without end, and its log must not fill the disk.
/// Every line also goes to stdout, where the platform keeps its own history.
struct LogFile {
    file: File,
    path: PathBuf,
    written: u64,
    max_bytes: u64,
    keep: u32,
}

impl LogFile {
    fn open(path: &Path, max_bytes: u64, keep: u32) -> std::io::Result<LogFile> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let written = file.metadata()?.len();
        Ok(LogFile {
            file,
            path: path.to_path_buf(),
            written,
            max_bytes,
            keep,
        })
    }

    fn write_line(&mut self, line: &str) {
        if self.written > 0 && self.written + line.len() as u64 + 1 > self.max_bytes {
            // A failed rotation keeps writing to the current file rather than losing lines.
            let _ = self.rotate();
        }
        if writeln!(self.file, "{line}").is_ok() {
            self.written += line.len() as u64 + 1;
        }
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        let numbered = |n: u32| {
            let mut name = self.path.as_os_str().to_owned();
            name.push(format!(".{n}"));
            PathBuf::from(name)
        };
        let _ = std::fs::remove_file(numbered(self.keep));
        for n in (1..self.keep).rev() {
            let from = numbered(n);
            if from.exists() {
                std::fs::rename(&from, numbered(n + 1))?;
            }
        }
        if self.keep > 0 {
            std::fs::rename(&self.path, numbered(1))?;
        } else {
            std::fs::remove_file(&self.path)?;
        }
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.written = 0;
        Ok(())
    }
}

/// A stream log's size before it rotates, and how many rotated files stay:
/// `MAGE_SERVICE_STREAM_LOG_MB` (64) and `MAGE_SERVICE_STREAM_LOG_FILES` (4).
pub fn stream_log_limits() -> (u64, u32) {
    let number = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default)
    };
    (
        number("MAGE_SERVICE_STREAM_LOG_MB", 64) * 1024 * 1024,
        number("MAGE_SERVICE_STREAM_LOG_FILES", 4) as u32,
    )
}

impl RunLog {
    pub fn create(path: &Path, run_id: &str) -> std::io::Result<RunLog> {
        Self::rotating(path, run_id, u64::MAX, 0)
    }

    /// A log that rotates past `max_bytes`, keeping `keep` older files.
    pub fn rotating(
        path: &Path,
        run_id: &str,
        max_bytes: u64,
        keep: u32,
    ) -> std::io::Result<RunLog> {
        Ok(RunLog {
            run_id: run_id.into(),
            file: Arc::new(Mutex::new(LogFile::open(path, max_bytes, keep)?)),
        })
    }

    pub fn write(&self, level: &str, block: Option<&str>, stream: Option<&str>, message: &str) {
        let message = &redact(message);
        emit(level, Some(&self.run_id), block, stream, message);
        let line = json!({
            "ts": timestamp(now()),
            "level": level,
            "block": block,
            "stream": stream,
            "message": message,
        });
        let mut file = self.file.lock().unwrap_or_else(|p| p.into_inner());
        file.write_line(&line.to_string());
    }

    pub fn info(&self, block: Option<&str>, message: &str) {
        self.write("info", block, None, message);
    }

    pub fn warn(&self, block: Option<&str>, message: &str) {
        self.write("warning", block, None, message);
    }

    pub fn error(&self, block: Option<&str>, message: &str) {
        self.write("error", block, None, message);
    }
}

#[cfg(test)]
mod rotation_tests {
    use super::*;

    #[test]
    fn a_stream_log_rotates_past_its_size_and_keeps_its_last_files() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("stream.log");
        let log = RunLog::rotating(&path, "s", 2_000, 2).unwrap();

        for n in 0..100 {
            log.info(None, &format!("message {n:03} {}", "x".repeat(50)));
        }

        let read = |p: &Path| std::fs::read_to_string(p).unwrap_or_default();
        let current = read(&path);
        let first = read(&folder.path().join("stream.log.1"));
        let second = read(&folder.path().join("stream.log.2"));
        assert!(!folder.path().join("stream.log.3").exists());
        for text in [&current, &first, &second] {
            assert!(!text.is_empty());
            assert!(text.len() as u64 <= 2_000, "{} bytes", text.len());
        }
        // The newest lines are in the current file, in order, none cut.
        let last: serde_json::Value =
            serde_json::from_str(current.lines().last().unwrap()).unwrap();
        assert_eq!(last["message"], format!("message 099 {}", "x".repeat(50)));
        assert!(
            second
                .lines()
                .chain(first.lines())
                .chain(current.lines())
                .all(|l| { serde_json::from_str::<serde_json::Value>(l).is_ok() })
        );
    }

    #[test]
    fn a_run_log_never_rotates() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("run.log");
        let log = RunLog::create(&path, "r").unwrap();
        for n in 0..50 {
            log.info(None, &format!("line {n} {}", "y".repeat(100)));
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 50);
        assert!(!folder.path().join("run.log.1").exists());
    }
}
