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

/// Where a run's lines also go, besides its file: the shared run history, so every instance
/// of a service serves the log of a run another instance ran.
pub trait LineSink: Send + Sync {
    /// The run's line number `seq` (0 for the first line of its log).
    fn line(&self, run_id: &str, seq: u64, line: &str);
    /// Writes what is held, before the run's status is recorded.
    fn flush(&self);
}

/// The log of one run: a JSON-lines file, also echoed to stdout.
#[derive(Clone)]
pub struct RunLog {
    run_id: Arc<str>,
    file: Arc<Mutex<LogFile>>,
    sink: Option<Arc<dyn LineSink>>,
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
    /// Lines written, those the file had when opened included: the next line's number.
    lines: u64,
}

impl LogFile {
    fn open(path: &Path, max_bytes: u64, keep: u32) -> std::io::Result<LogFile> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let written = file.metadata()?.len();
        let lines = if written > 0 {
            std::fs::read(path)?.iter().filter(|b| **b == b'\n').count() as u64
        } else {
            0
        };
        Ok(LogFile {
            file,
            path: path.to_path_buf(),
            written,
            max_bytes,
            keep,
            lines,
        })
    }

    /// Writes a line and returns its number.
    fn write_line(&mut self, line: &str) -> u64 {
        if self.written > 0 && self.written + line.len() as u64 + 1 > self.max_bytes {
            // A failed rotation keeps writing to the current file rather than losing lines.
            let _ = self.rotate();
        }
        if writeln!(self.file, "{line}").is_ok() {
            self.written += line.len() as u64 + 1;
        }
        self.lines += 1;
        self.lines - 1
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
            sink: None,
        })
    }

    /// The log, with its lines also sent to `sink`.
    pub fn with_sink(mut self, sink: Arc<dyn LineSink>) -> RunLog {
        self.sink = Some(sink);
        self
    }

    /// Sends the lines the sink holds; a no-op without one.
    pub fn flush(&self) {
        if let Some(sink) = &self.sink {
            sink.flush();
        }
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
        let line = line.to_string();
        let mut file = self.file.lock().unwrap_or_else(|p| p.into_inner());
        let seq = file.write_line(&line);
        // Under the file's lock, so the sink gets lines in their order.
        if let Some(sink) = &self.sink {
            sink.line(&self.run_id, seq, &line);
        }
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

#[cfg(test)]
mod sink_tests {
    use serde_json::Value;

    use super::*;

    #[derive(Default)]
    struct Recorder {
        lines: Mutex<Vec<(String, u64, Value)>>,
        flushes: Mutex<u32>,
    }

    impl LineSink for Recorder {
        fn line(&self, run_id: &str, seq: u64, line: &str) {
            let value = serde_json::from_str(line).unwrap();
            self.lines
                .lock()
                .unwrap()
                .push((run_id.to_string(), seq, value));
        }

        fn flush(&self) {
            *self.flushes.lock().unwrap() += 1;
        }
    }

    #[test]
    fn the_sink_gets_each_line_with_its_number_and_a_reopened_log_continues() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.log");
        let recorder = Arc::new(Recorder::default());
        let log = RunLog::create(&path, "r1")
            .unwrap()
            .with_sink(recorder.clone());
        log.info(None, "started");
        log.info(Some("load"), "loading");
        log.flush();
        {
            let lines = recorder.lines.lock().unwrap();
            assert_eq!(lines.len(), 2);
            assert_eq!((lines[0].0.as_str(), lines[0].1), ("r1", 0));
            assert_eq!(lines[1].1, 1);
            assert_eq!(lines[1].2["block"], "load");
            assert_eq!(lines[1].2["message"], "loading");
        }
        assert_eq!(*recorder.flushes.lock().unwrap(), 1);

        // The same run's log opened again (the service restarted): numbering goes on.
        let again = RunLog::create(&path, "r1")
            .unwrap()
            .with_sink(recorder.clone());
        again.warn(None, "resumed");
        assert_eq!(recorder.lines.lock().unwrap()[2].1, 2);
    }

    #[test]
    fn without_a_sink_flush_does_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let log = RunLog::create(&dir.path().join("run.log"), "r2").unwrap();
        log.info(None, "only the file");
        log.flush();
    }
}
