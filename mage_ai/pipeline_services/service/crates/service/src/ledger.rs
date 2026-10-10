//! The run history: runs, block runs, schedule occurrences and pipeline settings, in
//! SQLite with a write-ahead log.
//!
//! A run is committed before its request is acknowledged, so an accepted run survives a
//! restart. Idempotency keys and schedule occurrences are unique, so a repeated request or
//! a restart during a schedule tick creates no second run.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, SecondsFormat, Utc};
use mage_service_core::protocol::{BlockRunSummary, BlockStatus, RunDetail, RunStatus, RunSummary};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    status TEXT NOT NULL,
    source TEXT NOT NULL,
    idempotency_key TEXT,
    variables TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS runs_idempotency
    ON runs (pipeline, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS runs_pipeline_created ON runs (pipeline, created_at);
CREATE INDEX IF NOT EXISTS runs_status ON runs (status);
CREATE TABLE IF NOT EXISTS block_runs (
    run_id TEXT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    block TEXT NOT NULL,
    position INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    finished_at TEXT,
    error TEXT,
    outputs TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (run_id, block)
);
CREATE TABLE IF NOT EXISTS occurrences (
    trigger_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_settings (
    pipeline TEXT PRIMARY KEY,
    paused INTEGER NOT NULL DEFAULT 0,
    max_concurrent_runs INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS leases (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
"#;

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

pub fn timestamp(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn parse_time(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

fn duration_ms(start: &Option<String>, end: &Option<String>) -> Option<u64> {
    let start = parse_time(start.as_deref()?)?;
    let end = parse_time(end.as_deref()?)?;
    Some((end - start).num_milliseconds().max(0) as u64)
}

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error("the run history failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("the run history holds invalid data: {0}")]
    Data(String),
}

pub type Result<T> = std::result::Result<T, LedgerError>;

#[derive(Clone)]
pub struct Ledger {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    pub paused: bool,
    pub max_concurrent_runs: u32,
    pub revision: u64,
}

pub struct NewRun<'a> {
    pub pipeline: &'a str,
    pub source: &'a str,
    pub variables: &'a Map<String, Value>,
    pub idempotency_key: Option<&'a str>,
    pub blocks: &'a [String],
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Ledger> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        // A run is acknowledged after its commit reaches the write-ahead log.
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(10))?;
        connection.execute_batch(SCHEMA)?;
        Ok(Ledger {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    #[cfg(test)]
    pub fn in_memory() -> Result<Ledger> {
        let connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(SCHEMA)?;
        Ok(Ledger {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // A panic while holding the lock leaves a consistent database: every write is a
        // single statement or transaction.
        self.connection.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Creates a queued run with its block runs; with an idempotency key already used, it
    /// returns the earlier run and false.
    pub fn create_run(&self, new: NewRun) -> Result<(String, bool)> {
        let mut connection = self.lock();
        let transaction = connection.transaction()?;
        if let Some(key) = new.idempotency_key {
            let existing: Option<String> = transaction
                .query_row(
                    "SELECT id FROM runs WHERE pipeline = ?1 AND idempotency_key = ?2",
                    params![new.pipeline, key],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(id) = existing {
                return Ok((id, false));
            }
        }
        let id = uuid::Uuid::now_v7().to_string();
        let variables =
            serde_json::to_string(new.variables).map_err(|e| LedgerError::Data(e.to_string()))?;
        transaction.execute(
            "INSERT INTO runs (id, pipeline, status, source, idempotency_key, variables, \
             created_at) VALUES (?1, ?2, 'queued', ?3, ?4, ?5, ?6)",
            params![
                id,
                new.pipeline,
                new.source,
                new.idempotency_key,
                variables,
                timestamp(now())
            ],
        )?;
        for (position, block) in new.blocks.iter().enumerate() {
            transaction.execute(
                "INSERT INTO block_runs (run_id, block, position, status) \
                 VALUES (?1, ?2, ?3, 'pending')",
                params![id, block, position as i64],
            )?;
        }
        transaction.commit()?;
        Ok((id, true))
    }

    /// Creates the run of a schedule occurrence once; None when it exists already.
    pub fn create_occurrence_run(&self, trigger_key: &str, new: NewRun) -> Result<Option<String>> {
        {
            let connection = self.lock();
            let exists: Option<String> = connection
                .query_row(
                    "SELECT run_id FROM occurrences WHERE trigger_key = ?1",
                    params![trigger_key],
                    |row| row.get(0),
                )
                .optional()?;
            if exists.is_some() {
                return Ok(None);
            }
        }
        let (run_id, _) = self.create_run(new)?;
        let connection = self.lock();
        let inserted = connection.execute(
            "INSERT OR IGNORE INTO occurrences (trigger_key, run_id, created_at) \
             VALUES (?1, ?2, ?3)",
            params![trigger_key, run_id, timestamp(now())],
        )?;
        if inserted == 0 {
            // Another writer recorded this occurrence first; this run never existed.
            connection.execute("DELETE FROM runs WHERE id = ?1", params![run_id])?;
            return Ok(None);
        }
        Ok(Some(run_id))
    }

    pub fn has_occurrence(&self, trigger_key: &str) -> Result<bool> {
        let connection = self.lock();
        Ok(connection
            .query_row(
                "SELECT 1 FROM occurrences WHERE trigger_key = ?1",
                params![trigger_key],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// The oldest queued run of a pipeline, marked running; None when none waits.
    pub fn claim_next(&self, pipeline: &str) -> Result<Option<String>> {
        let mut connection = self.lock();
        let transaction = connection.transaction()?;
        let id: Option<String> = transaction
            .query_row(
                "SELECT id FROM runs WHERE pipeline = ?1 AND status = 'queued' \
                 ORDER BY created_at, id LIMIT 1",
                params![pipeline],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(id) = &id {
            transaction.execute(
                "UPDATE runs SET status = 'running', started_at = ?2 WHERE id = ?1",
                params![id, timestamp(now())],
            )?;
        }
        transaction.commit()?;
        Ok(id)
    }

    pub fn finish_run(&self, id: &str, status: RunStatus, error: Option<&str>) -> Result<()> {
        let error = error.map(crate::log::redact);
        self.lock().execute(
            "UPDATE runs SET status = ?2, finished_at = ?3, error = ?4 WHERE id = ?1",
            params![id, status.as_str(), timestamp(now()), error],
        )?;
        Ok(())
    }

    pub fn start_block(&self, run_id: &str, block: &str, attempt: u32) -> Result<()> {
        self.lock().execute(
            "UPDATE block_runs SET status = 'running', attempts = ?3, \
             started_at = COALESCE(started_at, ?4), error = NULL \
             WHERE run_id = ?1 AND block = ?2",
            params![run_id, block, attempt, timestamp(now())],
        )?;
        Ok(())
    }

    pub fn finish_block(
        &self,
        run_id: &str,
        block: &str,
        status: BlockStatus,
        error: Option<&str>,
        outputs: &[Value],
    ) -> Result<()> {
        let outputs =
            serde_json::to_string(outputs).map_err(|e| LedgerError::Data(e.to_string()))?;
        let error = error.map(crate::log::redact);
        self.lock().execute(
            "UPDATE block_runs SET status = ?3, finished_at = ?4, error = ?5, outputs = ?6 \
             WHERE run_id = ?1 AND block = ?2",
            params![
                run_id,
                block,
                status.as_str(),
                timestamp(now()),
                error,
                outputs
            ],
        )?;
        Ok(())
    }

    /// Runs a killed process left running or queued block runs behind; marks them failed.
    pub fn recover_interrupted(&self) -> Result<Vec<String>> {
        let mut connection = self.lock();
        let transaction = connection.transaction()?;
        let ids: Vec<String> = {
            let mut statement =
                transaction.prepare("SELECT id FROM runs WHERE status = 'running'")?;
            statement
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        let finished = timestamp(now());
        for id in &ids {
            transaction.execute(
                "UPDATE runs SET status = 'failed', finished_at = ?2, error = ?3 WHERE id = ?1",
                params![
                    id,
                    finished,
                    "The service stopped while this run was running."
                ],
            )?;
            transaction.execute(
                "UPDATE block_runs SET status = CASE WHEN status = 'running' THEN 'failed' \
                 ELSE 'cancelled' END, finished_at = ?2, \
                 error = CASE WHEN status = 'running' THEN ?3 ELSE error END \
                 WHERE run_id = ?1 AND status IN ('running', 'pending')",
                params![
                    id,
                    finished,
                    "The service stopped while this block was running."
                ],
            )?;
        }
        transaction.commit()?;
        Ok(ids)
    }

    pub fn settings(&self, pipeline: &str, default_max: u32) -> Result<Settings> {
        let connection = self.lock();
        connection.execute(
            "INSERT OR IGNORE INTO pipeline_settings (pipeline, max_concurrent_runs) \
             VALUES (?1, ?2)",
            params![pipeline, default_max],
        )?;
        Ok(connection.query_row(
            "SELECT paused, max_concurrent_runs, revision FROM pipeline_settings \
             WHERE pipeline = ?1",
            params![pipeline],
            |row| {
                Ok(Settings {
                    paused: row.get::<_, i64>(0)? != 0,
                    max_concurrent_runs: row.get::<_, i64>(1)? as u32,
                    revision: row.get::<_, i64>(2)? as u64,
                })
            },
        )?)
    }

    /// Changes settings only at the expected revision; returns the new settings, or the
    /// current ones and false on a revision conflict.
    pub fn update_settings(
        &self,
        pipeline: &str,
        expected_revision: u64,
        paused: Option<bool>,
        max_concurrent_runs: Option<u32>,
    ) -> Result<(Settings, bool)> {
        let changed = self.lock().execute(
            "UPDATE pipeline_settings SET paused = COALESCE(?3, paused), \
             max_concurrent_runs = COALESCE(?4, max_concurrent_runs), revision = revision + 1 \
             WHERE pipeline = ?1 AND revision = ?2",
            params![
                pipeline,
                expected_revision,
                paused.map(i64::from),
                max_concurrent_runs
            ],
        )?;
        let settings = self.settings(pipeline, max_concurrent_runs.unwrap_or(1))?;
        Ok((settings, changed == 1))
    }

    pub fn count(&self, pipeline: &str, status: RunStatus) -> Result<u64> {
        let connection = self.lock();
        Ok(connection.query_row(
            "SELECT COUNT(*) FROM runs WHERE pipeline = ?1 AND status = ?2",
            params![pipeline, status.as_str()],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    pub fn list_runs(
        &self,
        pipeline: Option<&str>,
        status: Option<RunStatus>,
        limit: u32,
    ) -> Result<Vec<RunSummary>> {
        let connection = self.lock();
        let mut statement = connection.prepare(
            "SELECT id, pipeline, status, source, created_at, started_at, finished_at, error \
             FROM runs WHERE (?1 IS NULL OR pipeline = ?1) AND (?2 IS NULL OR status = ?2) \
             ORDER BY created_at DESC, id DESC LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![pipeline, status.map(RunStatus::as_str), limit],
            run_summary,
        )?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn run(&self, id: &str) -> Result<Option<RunDetail>> {
        let connection = self.lock();
        let found = connection
            .query_row(
                "SELECT id, pipeline, status, source, created_at, started_at, finished_at, \
                 error, variables FROM runs WHERE id = ?1",
                params![id],
                |row| Ok((run_summary(row)?, row.get::<_, String>(8)?)),
            )
            .optional()?;
        let Some((run, variables)) = found else {
            return Ok(None);
        };
        let mut statement = connection.prepare(
            "SELECT block, status, attempts, started_at, finished_at, error, outputs \
             FROM block_runs WHERE run_id = ?1 ORDER BY position",
        )?;
        let blocks = statement
            .query_map(params![id], |row| {
                let started_at: Option<String> = row.get(3)?;
                let finished_at: Option<String> = row.get(4)?;
                let status: String = row.get(1)?;
                let outputs: String = row.get(6)?;
                Ok(BlockRunSummary {
                    block: row.get(0)?,
                    status: BlockStatus::parse(&status).unwrap_or(BlockStatus::Pending),
                    attempts: row.get::<_, i64>(2)? as u32,
                    duration_ms: duration_ms(&started_at, &finished_at),
                    started_at,
                    finished_at,
                    error: row.get(5)?,
                    outputs: serde_json::from_str(&outputs).unwrap_or_default(),
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(Some(RunDetail {
            run,
            variables: serde_json::from_str(&variables).unwrap_or_default(),
            blocks,
        }))
    }

    /// Durations and finish times of the last finished runs, oldest first.
    pub fn recent_finished(&self, pipeline: &str, limit: u32) -> Result<Vec<(String, u64)>> {
        let connection = self.lock();
        let mut statement = connection.prepare(
            "SELECT started_at, finished_at FROM runs WHERE pipeline = ?1 \
             AND status IN ('completed', 'failed') AND started_at IS NOT NULL \
             ORDER BY finished_at DESC LIMIT ?2",
        )?;
        let mut rows: Vec<(String, u64)> = statement
            .query_map(params![pipeline, limit], |row| {
                let started: Option<String> = row.get(0)?;
                let finished: Option<String> = row.get(1)?;
                Ok((
                    finished.clone().unwrap_or_default(),
                    duration_ms(&started, &finished).unwrap_or(0),
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;
        rows.reverse();
        Ok(rows)
    }

    pub fn last_error(&self, pipeline: &str) -> Result<Option<String>> {
        let connection = self.lock();
        Ok(connection
            .query_row(
                "SELECT finished_at, error FROM runs WHERE pipeline = ?1 AND status = 'failed' \
                 ORDER BY finished_at DESC LIMIT 1",
                params![pipeline],
                |row| {
                    let at: Option<String> = row.get(0)?;
                    let error: Option<String> = row.get(1)?;
                    Ok(error.map(|e| format!("{}: {e}", at.unwrap_or_default())))
                },
            )
            .optional()?
            .flatten())
    }

    pub fn running_count(&self, pipeline: &str) -> Result<u32> {
        Ok(self.count(pipeline, RunStatus::Running)? as u32)
    }

    /// Takes or renews a named lease; false while another owner holds it.
    pub fn acquire_lease(&self, name: &str, owner: &str, seconds: i64) -> Result<bool> {
        let current = now();
        let expires = timestamp(current + chrono::Duration::seconds(seconds));
        let changed = self.lock().execute(
            "INSERT INTO leases (name, owner, expires_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT (name) DO UPDATE SET owner = excluded.owner, \
             expires_at = excluded.expires_at \
             WHERE leases.owner = excluded.owner OR leases.expires_at < ?4",
            params![name, owner, expires, timestamp(current)],
        )?;
        Ok(changed == 1)
    }

    /// Deletes finished runs older than the cutoff; returns their ids.
    pub fn prune(&self, older_than: DateTime<Utc>) -> Result<Vec<String>> {
        let connection = self.lock();
        let mut statement = connection.prepare(
            "DELETE FROM runs WHERE status IN ('completed', 'failed', 'cancelled') \
             AND finished_at < ?1 RETURNING id",
        )?;
        let ids = statement
            .query_map(params![timestamp(older_than)], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(ids)
    }
}

fn run_summary(row: &rusqlite::Row) -> rusqlite::Result<RunSummary> {
    let status: String = row.get(2)?;
    let started_at: Option<String> = row.get(5)?;
    let finished_at: Option<String> = row.get(6)?;
    Ok(RunSummary {
        id: row.get(0)?,
        pipeline: row.get(1)?,
        status: RunStatus::parse(&status).unwrap_or(RunStatus::Failed),
        source: row.get(3)?,
        created_at: row.get(4)?,
        duration_ms: duration_ms(&started_at, &finished_at),
        started_at,
        finished_at,
        error: row.get(7)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_run<'a>(
        variables: &'a Map<String, Value>,
        key: Option<&'a str>,
        blocks: &'a [String],
    ) -> NewRun<'a> {
        NewRun {
            pipeline: "etl",
            source: "api",
            variables,
            idempotency_key: key,
            blocks,
        }
    }

    #[test]
    fn an_idempotency_key_returns_the_first_run() {
        let ledger = Ledger::in_memory().unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        let (first, created) = ledger
            .create_run(new_run(&vars, Some("k"), &blocks))
            .unwrap();
        assert!(created);
        let (second, created) = ledger
            .create_run(new_run(&vars, Some("k"), &blocks))
            .unwrap();
        assert!(!created);
        assert_eq!(first, second);
        let (third, _) = ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        assert_ne!(first, third);
    }

    #[test]
    fn runs_are_claimed_in_order_once() {
        let ledger = Ledger::in_memory().unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        let (a, _) = ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        let (b, _) = ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        assert_eq!(ledger.claim_next("etl").unwrap(), Some(a.clone()));
        assert_eq!(ledger.claim_next("etl").unwrap(), Some(b));
        assert_eq!(ledger.claim_next("etl").unwrap(), None);
        assert_eq!(ledger.running_count("etl").unwrap(), 2);
        ledger.finish_run(&a, RunStatus::Completed, None).unwrap();
        assert_eq!(ledger.count("etl", RunStatus::Completed).unwrap(), 1);
    }

    #[test]
    fn interrupted_runs_fail_with_their_running_block() {
        let ledger = Ledger::in_memory().unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string(), "b".to_string()];
        let (id, _) = ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        ledger.claim_next("etl").unwrap();
        ledger.start_block(&id, "a", 1).unwrap();
        assert_eq!(ledger.recover_interrupted().unwrap(), vec![id.clone()]);
        let run = ledger.run(&id).unwrap().unwrap();
        assert_eq!(run.run.status, RunStatus::Failed);
        assert_eq!(run.blocks[0].status, BlockStatus::Failed);
        assert_eq!(run.blocks[1].status, BlockStatus::Cancelled);
    }

    #[test]
    fn an_occurrence_creates_one_run() {
        let ledger = Ledger::in_memory().unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        let first = ledger
            .create_occurrence_run(
                "etl/daily/2026-10-10T00:00:00Z",
                new_run(&vars, None, &blocks),
            )
            .unwrap();
        assert!(first.is_some());
        let again = ledger
            .create_occurrence_run(
                "etl/daily/2026-10-10T00:00:00Z",
                new_run(&vars, None, &blocks),
            )
            .unwrap();
        assert!(again.is_none());
        assert_eq!(ledger.list_runs(Some("etl"), None, 10).unwrap().len(), 1);
    }

    #[test]
    fn settings_change_only_at_the_expected_revision() {
        let ledger = Ledger::in_memory().unwrap();
        let settings = ledger.settings("etl", 2).unwrap();
        assert_eq!(
            settings,
            Settings {
                paused: false,
                max_concurrent_runs: 2,
                revision: 1
            }
        );
        let (updated, ok) = ledger.update_settings("etl", 1, Some(true), None).unwrap();
        assert!(ok);
        assert!(updated.paused);
        assert_eq!(updated.revision, 2);
        let (_, ok) = ledger.update_settings("etl", 1, Some(false), None).unwrap();
        assert!(!ok);
    }

    #[test]
    fn a_lease_has_one_owner_until_it_expires() {
        let ledger = Ledger::in_memory().unwrap();
        assert!(ledger.acquire_lease("scheduler", "a", 30).unwrap());
        assert!(!ledger.acquire_lease("scheduler", "b", 30).unwrap());
        assert!(ledger.acquire_lease("scheduler", "a", 30).unwrap());
        assert!(ledger.acquire_lease("other", "b", -1).unwrap());
        assert!(ledger.acquire_lease("other", "c", 30).unwrap());
    }
}
