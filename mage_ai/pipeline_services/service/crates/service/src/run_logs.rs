//! Run log lines kept in a shared run history (PostgreSQL), so every instance of a service
//! serves the log of a run another instance ran, also after that instance is gone.
//!
//! Lines are held and written in batches: every [`WRITE_EVERY`], and when a run flushes its
//! log before its status is recorded, so a reader that sees the run finished finds its
//! lines. A failed write keeps the lines for the next one, up to [`MAX_HELD`].

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use mage_ledger::Ledger;
use mage_log::LineSink;

const WRITE_EVERY: Duration = Duration::from_millis(250);
/// Lines held while the run history refuses writes; older ones are dropped past it (the
/// files keep them).
const MAX_HELD: usize = 100_000;

pub struct SharedLogs {
    ledger: Ledger,
    held: Mutex<Vec<(String, u64, String)>>,
    /// Serializes writes, so a flush waits for a batch in flight and lines keep their order.
    writing: Mutex<()>,
}

impl SharedLogs {
    /// The sink for a run history that keeps logs, writing in the background; None
    /// otherwise (a SQLite one: one instance, whose files are enough).
    pub fn start(ledger: Ledger) -> Option<Arc<SharedLogs>> {
        if !ledger.keeps_logs() {
            return None;
        }
        let logs = Arc::new(SharedLogs {
            ledger,
            held: Mutex::new(Vec::new()),
            writing: Mutex::new(()),
        });
        let weak: Weak<SharedLogs> = Arc::downgrade(&logs);
        std::thread::Builder::new()
            .name("run-logs".into())
            .spawn(move || {
                while let Some(logs) = weak.upgrade() {
                    logs.write();
                    drop(logs);
                    std::thread::sleep(WRITE_EVERY);
                }
            })
            .ok()?;
        Some(logs)
    }

    fn write(&self) {
        let _writing = self.writing.lock().unwrap_or_else(|p| p.into_inner());
        let lines = std::mem::take(&mut *self.held.lock().unwrap_or_else(|p| p.into_inner()));
        if lines.is_empty() {
            return;
        }
        if let Err(error) = self.ledger.append_logs(lines.clone()) {
            crate::log::warn(&format!(
                "Keeping {} run log line(s) in the run history failed, retrying: {error}",
                lines.len()
            ));
            let mut held = self.held.lock().unwrap_or_else(|p| p.into_inner());
            let newer = std::mem::replace(&mut *held, lines);
            held.extend(newer);
            if held.len() > MAX_HELD {
                let dropped = held.len() - MAX_HELD;
                held.drain(..dropped);
            }
        }
    }
}

impl LineSink for SharedLogs {
    fn line(&self, run_id: &str, seq: u64, line: &str) {
        self.held.lock().unwrap_or_else(|p| p.into_inner()).push((
            run_id.to_string(),
            seq,
            line.to_string(),
        ));
    }

    fn flush(&self) {
        self.write();
    }
}
