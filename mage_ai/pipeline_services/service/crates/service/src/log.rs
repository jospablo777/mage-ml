//! Logs: the service's own events and every line blocks print.
//!
//! Each line goes to stdout (stderr for `mage-service run`), where `docker logs` and log
//! collectors read it, as text or as JSON (`MAGE_SERVICE_LOG_FORMAT=json`), and a run's lines also go to its log file, which
//! `GET /v1/runs/{id}/logs` returns. Nothing is shortened or dropped.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::json;

use crate::ledger::{now, timestamp};

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
        Some(values) if !values.is_empty() => crate::secrets::redact(text, values),
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
    file: Arc<Mutex<File>>,
}

impl RunLog {
    pub fn create(path: &Path, run_id: &str) -> std::io::Result<RunLog> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(RunLog {
            run_id: run_id.into(),
            file: Arc::new(Mutex::new(file)),
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
        let _ = writeln!(file, "{line}");
    }

    pub fn info(&self, block: Option<&str>, message: &str) {
        self.write("info", block, None, message);
    }

    pub fn error(&self, block: Option<&str>, message: &str) {
        self.write("error", block, None, message);
    }
}
