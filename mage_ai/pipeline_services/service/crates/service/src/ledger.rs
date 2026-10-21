//! The run history: runs, block runs, schedule occurrences and pipeline settings, in
//! SQLite with a write-ahead log.
//!
//! A run is committed before its request is acknowledged, so an accepted run survives a
//! restart. Idempotency keys and schedule occurrences are unique, so a repeated request or
//! a restart during a schedule tick creates no second run.
//!
//! Each process that opens the ledger is an instance with a lease in database time, renewed
//! every few seconds. A claimed run belongs to its instance, and the run's writes (its
//! status, its block runs) apply only while that instance still holds it: a process the
//! others gave up on cannot overwrite what happened since. Recovery fails the runs of
//! instances whose lease expired, not the runs of every instance, which it did when a
//! second instance started on the same ledger (I-22).
//!
//! Commits do not checkpoint the write-ahead log: a checkpoint writes pages to the database
//! file and syncs it, which held the ledger's lock for 0.2 to 0.9 s and stalled the runs
//! behind it. A background thread checkpoints on its own connection instead.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

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
CREATE TABLE IF NOT EXISTS instances (
    id TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    pid INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
"#;

/// The database's time, as the ledger writes timestamps; leases compare in it, so instances
/// on hosts with different clocks agree (PostgreSQL will use the server's time the same way).
const DB_NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";
/// An instance's lease, and how often it is renewed: an instance silent for this long is
/// taken for dead and its runs are failed.
pub const INSTANCE_LEASE_SECONDS: i64 = 30;
const HEARTBEAT_EVERY: Duration = Duration::from_secs(5);

/// Columns added after the first release: existing ledgers get them in place.
fn migrate(connection: &Connection) -> rusqlite::Result<()> {
    let has_retry_of: bool = connection
        .prepare("SELECT 1 FROM pragma_table_info('runs') WHERE name = 'retry_of'")?
        .exists([])?;
    if !has_retry_of {
        connection.execute_batch("ALTER TABLE runs ADD COLUMN retry_of TEXT")?;
    }
    let has_owner: bool = connection
        .prepare("SELECT 1 FROM pragma_table_info('runs') WHERE name = 'owner'")?
        .exists([])?;
    if !has_owner {
        // Runs from before have no owner: recovery treats them as an expired instance's.
        connection.execute_batch("ALTER TABLE runs ADD COLUMN owner TEXT")?;
    }
    Ok(())
}

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

const CHECKPOINT_EVERY: Duration = Duration::from_secs(1);
/// Above this size the checkpoint also truncates the log, waiting for readers if it must.
const WAL_LIMIT: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct Ledger {
    connection: Arc<Mutex<Connection>>,
    /// This process's instance id: the owner of the runs it claims.
    instance: Arc<str>,
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
        Ledger::open_on(path, &this_host())
    }

    fn open_on(path: &Path, host: &str) -> Result<Ledger> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        // A run is acknowledged after its commit reaches the write-ahead log.
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(10))?;
        connection.execute_batch(SCHEMA)?;
        migrate(&connection)?;
        connection.pragma_update(None, "wal_autocheckpoint", 0)?;
        let checkpoints = Connection::open(path)?;
        checkpoints.busy_timeout(std::time::Duration::from_secs(10))?;
        let ledger = Ledger::register(connection, host)?;
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        start_checkpoints(
            checkpoints,
            PathBuf::from(wal),
            Arc::downgrade(&ledger.connection),
        );
        start_heartbeats(ledger.clone());
        Ok(ledger)
    }

    /// Registers this process as an instance with a fresh lease.
    fn register(connection: Connection, host: &str) -> Result<Ledger> {
        let instance: Arc<str> = uuid::Uuid::now_v7().to_string().into();
        connection.execute(
            &format!(
                "INSERT INTO instances (id, host, pid, started_at, expires_at) \
                 VALUES (?1, ?2, ?3, {DB_NOW}, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?4))"
            ),
            params![
                instance.as_ref(),
                host,
                std::process::id(),
                format!("+{INSTANCE_LEASE_SECONDS} seconds")
            ],
        )?;
        // An instance on this host whose process is gone stopped: its runs are recovered
        // now, not when its lease ends. A restarted container is pid 1 again, so this pid
        // under another instance id is gone too.
        let same_host: Vec<(String, i64)> = connection
            .prepare(&format!(
                "SELECT id, pid FROM instances WHERE host = ?1 AND host <> '' AND id <> ?2 \
                 AND expires_at > {DB_NOW}"
            ))?
            .query_map(params![host, instance.as_ref()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        for (id, pid) in same_host {
            if pid == i64::from(std::process::id()) || !process_alive(pid) {
                connection.execute(
                    &format!("UPDATE instances SET expires_at = {DB_NOW} WHERE id = ?1"),
                    params![id],
                )?;
            }
        }
        Ok(Ledger {
            connection: Arc::new(Mutex::new(connection)),
            instance,
        })
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// Renews this instance's lease; false when it had expired, and others may have failed
    /// its runs meanwhile. It is renewed either way: a failed run is no longer `running`, so
    /// the fences keep this instance out of it, and the runs it claims from now on are kept.
    /// Deletes the rows of instances long expired.
    pub fn heartbeat(&self) -> Result<bool> {
        let connection = self.lock();
        let live = connection
            .prepare(&format!(
                "SELECT 1 FROM instances WHERE id = ?1 AND expires_at > {DB_NOW}"
            ))?
            .exists(params![self.instance.as_ref()])?;
        connection.execute(
            &format!(
                "INSERT INTO instances (id, host, pid, started_at, expires_at) \
                 VALUES (?1, '', ?2, {DB_NOW}, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?3)) \
                 ON CONFLICT (id) DO UPDATE SET expires_at = excluded.expires_at"
            ),
            params![
                self.instance.as_ref(),
                std::process::id(),
                format!("+{INSTANCE_LEASE_SECONDS} seconds")
            ],
        )?;
        connection.execute(
            "DELETE FROM instances WHERE expires_at < \
             strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 day')",
            [],
        )?;
        Ok(live)
    }

    /// Ends this instance's lease at once, so a restart recovers its runs without waiting
    /// for the lease to expire.
    pub fn release(&self) -> Result<()> {
        self.lock().execute(
            &format!("UPDATE instances SET expires_at = {DB_NOW} WHERE id = ?1"),
            params![self.instance.as_ref()],
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub fn in_memory() -> Result<Ledger> {
        let connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(SCHEMA)?;
        migrate(&connection)?;
        Ledger::register(connection, "")
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // A panic while holding the lock leaves a consistent database: every write is a
        // single statement or transaction.
        self.connection.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Creates a queued run with its block runs. With an idempotency key already used, it
    /// returns the earlier run and false, unless that run failed or was cancelled: then the
    /// new run takes the key and records the earlier one in `retry_of`. A key used to return
    /// a failed run forever, so a caller's retry could never run the pipeline again.
    pub fn create_run(&self, new: NewRun) -> Result<(String, bool)> {
        let mut connection = self.lock();
        let transaction = connection.transaction()?;
        let mut retry_of: Option<String> = None;
        if let Some(key) = new.idempotency_key {
            let existing: Option<(String, String)> = transaction
                .query_row(
                    "SELECT id, status FROM runs WHERE pipeline = ?1 AND idempotency_key = ?2",
                    params![new.pipeline, key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            match existing {
                Some((id, status)) if status == "failed" || status == "cancelled" => {
                    transaction.execute(
                        "UPDATE runs SET idempotency_key = NULL WHERE id = ?1",
                        params![id],
                    )?;
                    retry_of = Some(id);
                }
                Some((id, _)) => return Ok((id, false)),
                None => {}
            }
        }
        let id = uuid::Uuid::now_v7().to_string();
        let variables =
            serde_json::to_string(new.variables).map_err(|e| LedgerError::Data(e.to_string()))?;
        transaction.execute(
            "INSERT INTO runs (id, pipeline, status, source, idempotency_key, variables, \
             created_at, retry_of) VALUES (?1, ?2, 'queued', ?3, ?4, ?5, ?6, ?7)",
            params![
                id,
                new.pipeline,
                new.source,
                new.idempotency_key,
                variables,
                timestamp(now()),
                retry_of
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
                "UPDATE runs SET status = 'running', started_at = ?2, owner = ?3 \
                 WHERE id = ?1 AND status = 'queued'",
                params![id, timestamp(now()), self.instance.as_ref()],
            )?;
        }
        transaction.commit()?;
        Ok(id)
    }

    /// Finishes a queued run, or a running run this instance holds; false when the run is
    /// another instance's or already finished (its recovery, a cancel), which then stands.
    pub fn finish_run(&self, id: &str, status: RunStatus, error: Option<&str>) -> Result<bool> {
        let error = error.map(crate::log::redact);
        let changed = self.lock().execute(
            "UPDATE runs SET status = ?2, finished_at = ?3, error = ?4 WHERE id = ?1 \
             AND (status = 'queued' OR (status = 'running' AND owner = ?5))",
            params![
                id,
                status.as_str(),
                timestamp(now()),
                error,
                self.instance.as_ref()
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn start_block(&self, run_id: &str, block: &str, attempt: u32) -> Result<()> {
        self.lock().execute(
            "UPDATE block_runs SET status = 'running', attempts = ?3, \
             started_at = COALESCE(started_at, ?4), error = NULL \
             WHERE run_id = ?1 AND block = ?2 AND EXISTS (SELECT 1 FROM runs \
             WHERE runs.id = block_runs.run_id AND runs.status = 'running' \
             AND runs.owner = ?5)",
            params![
                run_id,
                block,
                attempt,
                timestamp(now()),
                self.instance.as_ref()
            ],
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
             WHERE run_id = ?1 AND block = ?2 AND EXISTS (SELECT 1 FROM runs \
             WHERE runs.id = block_runs.run_id AND runs.status = 'running' \
             AND runs.owner = ?7)",
            params![
                run_id,
                block,
                status.as_str(),
                timestamp(now()),
                error,
                outputs,
                self.instance.as_ref()
            ],
        )?;
        Ok(())
    }

    /// A completed block of a chain, with its own span inside the chain's time.
    pub fn finish_chained_block(
        &self,
        run_id: &str,
        block: &str,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        outputs: &[Value],
    ) -> Result<()> {
        let outputs =
            serde_json::to_string(outputs).map_err(|e| LedgerError::Data(e.to_string()))?;
        self.lock().execute(
            "UPDATE block_runs SET status = 'completed', started_at = ?3, finished_at = ?4, \
             error = NULL, outputs = ?5 WHERE run_id = ?1 AND block = ?2 \
             AND EXISTS (SELECT 1 FROM runs WHERE runs.id = block_runs.run_id \
             AND runs.status = 'running' AND runs.owner = ?6)",
            params![
                run_id,
                block,
                timestamp(started_at),
                timestamp(finished_at),
                outputs,
                self.instance.as_ref()
            ],
        )?;
        Ok(())
    }

    /// The runs of instances whose lease expired (a killed or stalled process, or a run from
    /// before owners were recorded): marks them and their unfinished block runs failed. The
    /// runs of live instances, this one included, are left alone.
    pub fn recover_interrupted(&self) -> Result<Vec<String>> {
        let mut connection = self.lock();
        let transaction = connection.transaction()?;
        let ids: Vec<String> = {
            let mut statement = transaction.prepare(&format!(
                "SELECT id FROM runs WHERE status = 'running' AND (owner IS NULL OR owner \
                 NOT IN (SELECT id FROM instances WHERE expires_at > {DB_NOW}))"
            ))?;
            statement
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        let finished = timestamp(now());
        for id in &ids {
            transaction.execute(
                "UPDATE runs SET status = 'failed', finished_at = ?2, error = ?3 \
                 WHERE id = ?1 AND status = 'running'",
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
            "SELECT id, pipeline, status, source, created_at, started_at, finished_at, error, \
             retry_of FROM runs WHERE (?1 IS NULL OR pipeline = ?1) AND (?2 IS NULL OR status = ?2) \
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
                 error, retry_of, variables FROM runs WHERE id = ?1",
                params![id],
                |row| Ok((run_summary(row)?, row.get::<_, String>(9)?)),
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
    /// Times are the database's: instances on hosts with different clocks agree.
    pub fn acquire_lease(&self, name: &str, owner: &str, seconds: i64) -> Result<bool> {
        let changed = self.lock().execute(
            &format!(
                "INSERT INTO leases (name, owner, expires_at) \
                 VALUES (?1, ?2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?3)) \
                 ON CONFLICT (name) DO UPDATE SET owner = excluded.owner, \
                 expires_at = excluded.expires_at \
                 WHERE leases.owner = excluded.owner OR leases.expires_at < {DB_NOW}"
            ),
            params![name, owner, format!("{seconds:+} seconds")],
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

/// This machine's name, which with a pid tells whether an instance's process is gone.
fn this_host() -> String {
    let mut name = [0u8; 256];
    // SAFETY: the buffer is valid for its length; the name is NUL terminated within it.
    if unsafe { libc::gethostname(name.as_mut_ptr().cast(), name.len()) } != 0 {
        return String::new();
    }
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    String::from_utf8_lossy(&name[..end]).into_owned()
}

/// Whether a process with this pid runs on this machine. A reused pid reads as alive: the
/// instance's lease then decides, which is the safe side.
fn process_alive(pid: i64) -> bool {
    // 0 and negative pids address process groups.
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    // SAFETY: signal 0 checks the process without signalling it.
    let found = unsafe { libc::kill(pid, 0) } == 0;
    found || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Renews the instance's lease and fails the runs of expired instances, every few seconds
/// while the ledger is open.
fn start_heartbeats(ledger: Ledger) {
    let connection = Arc::downgrade(&ledger.connection);
    let instance = ledger.instance.clone();
    drop(ledger);
    std::thread::Builder::new()
        .name("ledger-heartbeats".into())
        .spawn(move || {
            loop {
                std::thread::sleep(HEARTBEAT_EVERY);
                let Some(connection) = connection.upgrade() else {
                    break;
                };
                let ledger = Ledger {
                    connection,
                    instance: instance.clone(),
                };
                match ledger.heartbeat() {
                    Ok(true) => {}
                    Ok(false) => crate::log::warn(
                        "This instance's lease in the run history had expired; runs it was \
                         running may have been failed by another instance.",
                    ),
                    Err(e) => crate::log::warn(&format!("The run history heartbeat failed: {e}")),
                }
                match ledger.recover_interrupted() {
                    Ok(ids) if !ids.is_empty() => crate::log::warn(&format!(
                        "Failed {} run(s) of an instance that stopped: {}",
                        ids.len(),
                        ids.join(", ")
                    )),
                    Ok(_) => {}
                    Err(e) => crate::log::warn(&format!("Recovering runs failed: {e}")),
                }
            }
        })
        .expect("the heartbeat thread starts");
}

/// Checkpoints every second while the ledger is open.
fn start_checkpoints(connection: Connection, wal: PathBuf, ledger: Weak<Mutex<Connection>>) {
    std::thread::Builder::new()
        .name("ledger-checkpoints".into())
        .spawn(move || {
            loop {
                std::thread::sleep(CHECKPOINT_EVERY);
                if ledger.upgrade().is_none() {
                    break;
                }
                if let Err(e) = checkpoint(&connection, &wal, WAL_LIMIT) {
                    crate::log::warn(&format!("The run history checkpoint failed: {e}"));
                }
            }
        })
        .expect("the checkpoint thread starts");
}

/// Copies committed pages from the log into the database. PASSIVE never waits for writers;
/// a log over `limit` bytes is truncated, so it cannot grow without bound under load.
fn checkpoint(connection: &Connection, wal: &Path, limit: u64) -> rusqlite::Result<()> {
    let size = std::fs::metadata(wal).map(|m| m.len()).unwrap_or(0);
    let mode = if size > limit { "TRUNCATE" } else { "PASSIVE" };
    connection.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |_| Ok(()))
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
        retry_of: row.get(8)?,
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
    fn commits_leave_the_checkpoint_to_the_background() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("runs.db");
        let ledger = Ledger::open(&path).unwrap();
        let automatic: i64 = ledger
            .lock()
            .query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))
            .unwrap();
        assert_eq!(automatic, 0);
        let empty = std::fs::metadata(&path).unwrap().len();
        let vars = Map::new();
        let blocks = vec!["a".to_string(), "b".to_string()];
        for _ in 0..50 {
            ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::fs::metadata(&path).unwrap().len() <= empty {
            assert!(
                std::time::Instant::now() < deadline,
                "no checkpoint wrote the runs to the database"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn a_log_over_the_limit_is_truncated() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("runs.db");
        let ledger = Ledger::open(&path).unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        for _ in 0..20 {
            ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        }
        let wal = folder.path().join("runs.db-wal");
        let other = Connection::open(&path).unwrap();
        checkpoint(&other, &wal, 0).unwrap();
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
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
    fn a_key_whose_run_failed_or_was_cancelled_runs_again() {
        let ledger = Ledger::in_memory().unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        for status in [RunStatus::Failed, RunStatus::Cancelled] {
            let key = format!("key-{}", status.as_str());
            let (first, _) = ledger
                .create_run(new_run(&vars, Some(&key), &blocks))
                .unwrap();
            ledger.finish_run(&first, status, Some("boom")).unwrap();

            let (retry, created) = ledger
                .create_run(new_run(&vars, Some(&key), &blocks))
                .unwrap();

            assert!(created, "{status:?}");
            assert_ne!(retry, first);
            assert_eq!(
                ledger.run(&retry).unwrap().unwrap().run.retry_of,
                Some(first.clone())
            );
            // The key now names the retry.
            let (again, created) = ledger
                .create_run(new_run(&vars, Some(&key), &blocks))
                .unwrap();
            assert!(!created);
            assert_eq!(again, retry);
        }
    }

    #[test]
    fn a_key_whose_run_completed_returns_that_run() {
        let ledger = Ledger::in_memory().unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        let (first, _) = ledger
            .create_run(new_run(&vars, Some("k"), &blocks))
            .unwrap();
        ledger
            .finish_run(&first, RunStatus::Completed, None)
            .unwrap();

        let (second, created) = ledger
            .create_run(new_run(&vars, Some("k"), &blocks))
            .unwrap();

        assert!(!created);
        assert_eq!(second, first);
        assert_eq!(ledger.run(&first).unwrap().unwrap().run.retry_of, None);
    }

    #[test]
    fn a_ledger_from_an_earlier_release_gets_the_new_column() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("runs.db");
        {
            let old = Connection::open(&path).unwrap();
            old.execute_batch(SCHEMA).unwrap();
            old.execute_batch("ALTER TABLE runs DROP COLUMN retry_of")
                .ok();
        }
        let ledger = Ledger::open(&path).unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        let (id, _) = ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        assert_eq!(ledger.run(&id).unwrap().unwrap().run.retry_of, None);
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
        // The process stopped: a live instance's runs are left alone.
        assert!(ledger.recover_interrupted().unwrap().is_empty());
        ledger.release().unwrap();
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

    /// Two processes on one ledger file, on two hosts: the ledger of each.
    fn two_instances() -> (tempfile::TempDir, Ledger, Ledger) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runs.db");
        let first = Ledger::open_on(&path, "first").unwrap();
        let second = Ledger::open_on(&path, "second").unwrap();
        (dir, first, second)
    }

    fn expire(ledger: &Ledger, instance: &str) {
        ledger
            .lock()
            .execute(
                "UPDATE instances SET expires_at = '2000-01-01T00:00:00.000Z' WHERE id = ?1",
                params![instance],
            )
            .unwrap();
    }

    fn status(ledger: &Ledger, id: &str) -> RunStatus {
        ledger.run(id).unwrap().unwrap().run.status
    }

    #[test]
    fn an_instance_recovers_only_the_runs_of_expired_instances() {
        let (_dir, first, second) = two_instances();
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        let (id, _) = first
            .create_run(new_run(&variables, None, &blocks))
            .unwrap();
        assert_eq!(
            first.claim_next("etl").unwrap().as_deref(),
            Some(id.as_str())
        );

        // Another instance starting does not fail a live instance's run.
        assert!(second.recover_interrupted().unwrap().is_empty());
        assert_eq!(status(&second, &id), RunStatus::Running);

        // Its lease expired (a killed or stalled process): the run is failed once.
        expire(&second, first.instance());
        assert_eq!(second.recover_interrupted().unwrap(), vec![id.clone()]);
        assert_eq!(status(&second, &id), RunStatus::Failed);
        assert!(second.recover_interrupted().unwrap().is_empty());
    }

    #[test]
    fn a_stale_instance_cannot_overwrite_what_happened_since() {
        let (_dir, first, second) = two_instances();
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        let (id, _) = first
            .create_run(new_run(&variables, None, &blocks))
            .unwrap();
        first.claim_next("etl").unwrap();
        first.start_block(&id, "load", 1).unwrap();
        expire(&second, first.instance());
        second.recover_interrupted().unwrap();

        // The stalled process wakes up and reports success: nothing changes.
        first
            .finish_block(&id, "load", BlockStatus::Completed, None, &[])
            .unwrap();
        assert!(!first.finish_run(&id, RunStatus::Completed, None).unwrap());
        let detail = second.run(&id).unwrap().unwrap();
        assert_eq!(detail.run.status, RunStatus::Failed);
        assert_eq!(detail.blocks[0].status, BlockStatus::Failed);
        // Its heartbeat tells it it was given up on, and renews its lease: the runs it
        // claims from now on are its own again.
        assert!(!first.heartbeat().unwrap());
        assert!(first.heartbeat().unwrap());
        let (next, _) = first
            .create_run(new_run(&variables, Some("next"), &blocks))
            .unwrap();
        first.claim_next("etl").unwrap();
        assert!(second.recover_interrupted().unwrap().is_empty());
        assert!(first.finish_run(&next, RunStatus::Completed, None).unwrap());
    }

    #[test]
    fn a_run_from_before_owners_and_a_released_instance_are_recovered_at_once() {
        let (_dir, first, second) = two_instances();
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        let (old, _) = first
            .create_run(new_run(&variables, None, &blocks))
            .unwrap();
        first
            .lock()
            .execute(
                "UPDATE runs SET status = 'running', owner = NULL WHERE id = ?1",
                params![old],
            )
            .unwrap();
        let (mine, _) = first
            .create_run(new_run(&variables, Some("k"), &blocks))
            .unwrap();
        first.claim_next("etl").unwrap();
        first.claim_next("etl").unwrap();
        assert_eq!(second.recover_interrupted().unwrap(), vec![old]);
        first.release().unwrap();
        assert_eq!(second.recover_interrupted().unwrap(), vec![mine]);
    }

    #[test]
    fn a_queued_run_is_cancelled_by_any_instance_and_a_claimed_one_by_its_owner_only() {
        let (_dir, first, second) = two_instances();
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        let (queued, _) = first
            .create_run(new_run(&variables, None, &blocks))
            .unwrap();
        assert!(
            second
                .finish_run(
                    &queued,
                    RunStatus::Cancelled,
                    Some("Cancelled before it started.")
                )
                .unwrap()
        );
        let (claimed, _) = first
            .create_run(new_run(&variables, Some("other"), &blocks))
            .unwrap();
        first.claim_next("etl").unwrap();
        assert!(
            !second
                .finish_run(&claimed, RunStatus::Cancelled, None)
                .unwrap()
        );
        assert!(
            first
                .finish_run(&claimed, RunStatus::Completed, None)
                .unwrap()
        );
    }

    #[test]
    fn a_restart_on_the_same_host_recovers_the_runs_of_a_gone_process_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runs.db");
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        // A killed process (no such pid), a restarted container (this pid under another
        // instance) and a live process on another host.
        let mut ids = Vec::new();
        for (host, pid) in [
            ("box", 999_999_999_i64),
            ("box", -1),
            ("other", 999_999_999),
        ] {
            let old = Ledger::open_on(&path, host).unwrap();
            let (id, _) = old.create_run(new_run(&variables, None, &blocks)).unwrap();
            old.claim_next("etl").unwrap();
            let pid = if pid == -1 {
                i64::from(std::process::id())
            } else {
                pid
            };
            old.lock()
                .execute(
                    "UPDATE instances SET pid = ?1 WHERE id = ?2",
                    params![pid, old.instance()],
                )
                .unwrap();
            ids.push(id);
        }
        let restarted = Ledger::open_on(&path, "box").unwrap();
        let mut recovered = restarted.recover_interrupted().unwrap();
        recovered.sort();
        let mut expected = ids[..2].to_vec();
        expected.sort();
        assert_eq!(recovered, expected);
    }
}
