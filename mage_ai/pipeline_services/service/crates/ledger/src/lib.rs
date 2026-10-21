//! The run history: runs, block runs, schedule occurrences and pipeline settings, in
//! SQLite with a write-ahead log, or in PostgreSQL for instances on several machines.
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
//! A pipeline's `max_concurrent_runs` holds across instances: a claim counts the running
//! runs and takes the next queued one in one transaction, serialized per pipeline (SQLite's
//! write lock, a PostgreSQL advisory lock). A cancel reaches a run on another instance
//! through the run's row, which its owner reads every dispatch.
//!
//! The schema is versioned (`PRAGMA user_version` in SQLite, `schema_migrations` in
//! PostgreSQL). An engine refuses a run history newer than it knows, and migrates an older
//! one at start unless told not to (`mage-service ledger migrate` then does it).
//!
//! SQLite commits do not checkpoint the write-ahead log: a checkpoint writes pages to the
//! database file and syncs it, which held the ledger's lock for 0.2 to 0.9 s and stalled
//! the runs behind it. A background thread checkpoints on its own connection instead.

pub mod sql;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use chrono::{DateTime, Utc};
use mage_service_core::protocol::{BlockRunSummary, BlockStatus, RunDetail, RunStatus, RunSummary};
use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::sql::{Db, Postgres, Row, cells};

/// SQLite's schema at version 1, the first versioned one. Ledgers from 0.2 (version 0) have
/// these tables without the later columns, which version 1 adds in place.
const SQLITE_SCHEMA: &str = r#"
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

/// The columns version 1 adds to a 0.2 ledger's runs.
const SQLITE_V1_COLUMNS: &[(&str, &str)] = &[
    ("retry_of", "TEXT"),
    // Runs from before have no owner: recovery treats them as an expired instance's.
    ("owner", "TEXT"),
    ("cancel_requested", "INTEGER NOT NULL DEFAULT 0"),
];

/// The SQLite schema version this engine writes.
pub const SQLITE_VERSION: i64 = 1;

/// PostgreSQL migrations, in order: version N is the Nth. Types are the SQLite ones' (text
/// timestamps in ISO 8601 UTC, integers), so a statement means the same on both. Version 2
/// keeps run logs (`run_logs`) and version 3 block output files up to a size
/// (`run_outputs`), so every instance serves what another ran; a SQLite run history has one
/// instance, whose files are enough.
const POSTGRES_MIGRATIONS: &[&str] = &[
    r#"
CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    status TEXT NOT NULL,
    source TEXT NOT NULL,
    idempotency_key TEXT,
    variables TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    error TEXT,
    retry_of TEXT,
    owner TEXT,
    cancel_requested BIGINT NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX runs_idempotency
    ON runs (pipeline, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX runs_pipeline_created ON runs (pipeline, created_at);
CREATE INDEX runs_status ON runs (status);
CREATE TABLE block_runs (
    run_id TEXT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    block TEXT NOT NULL,
    position BIGINT NOT NULL,
    status TEXT NOT NULL,
    attempts BIGINT NOT NULL DEFAULT 0,
    started_at TEXT,
    finished_at TEXT,
    error TEXT,
    outputs TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (run_id, block)
);
CREATE TABLE occurrences (
    trigger_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE pipeline_settings (
    pipeline TEXT PRIMARY KEY,
    paused BIGINT NOT NULL DEFAULT 0,
    max_concurrent_runs BIGINT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1
);
CREATE TABLE leases (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE TABLE instances (
    id TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    pid BIGINT NOT NULL,
    started_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE TABLE service (
    name TEXT NOT NULL
);
"#,
    r#"
CREATE TABLE run_logs (
    run_id TEXT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    line TEXT NOT NULL,
    PRIMARY KEY (run_id, seq)
);
"#,
    r#"
CREATE TABLE run_outputs (
    run_id TEXT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    block TEXT NOT NULL,
    idx BIGINT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (run_id, block, idx)
);
"#,
];

/// An instance's lease, and how often it is renewed: an instance silent for this long is
/// taken for dead and its runs are failed.
pub const INSTANCE_LEASE_SECONDS: i64 = 30;
const HEARTBEAT_EVERY: Duration = Duration::from_secs(5);

pub use mage_service_core::time::{now, timestamp};

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
    #[error("the run history failed: {0}")]
    Postgres(String),
    #[error("the run history holds invalid data: {0}")]
    Data(String),
    #[error("{0}")]
    Version(String),
}

impl From<tokio_postgres::Error> for LedgerError {
    fn from(error: tokio_postgres::Error) -> LedgerError {
        // The server's message, not only "db error".
        match error.as_db_error() {
            Some(db) => LedgerError::Postgres(db.to_string()),
            None => LedgerError::Postgres(error.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, LedgerError>;

const CHECKPOINT_EVERY: Duration = Duration::from_secs(1);
/// Above this size the checkpoint also truncates the log, waiting for readers if it must.
const WAL_LIMIT: u64 = 64 * 1024 * 1024;

/// Where the run history is.
#[derive(Debug, Clone)]
pub enum Location {
    Sqlite(PathBuf),
    Postgres {
        url: String,
        schema: String,
        /// The service's name, recorded at first use: a schema holds one service's runs.
        service: String,
    },
}

enum Store {
    Sqlite(Mutex<Connection>),
    Postgres(Postgres),
}

impl Store {
    /// Runs `work` on the connection, each statement on its own.
    fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut dyn Db) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        match self {
            Store::Sqlite(connection) => work(&mut *lock(connection)),
            Store::Postgres(postgres) => postgres.run(move |connection| work(connection)),
        }
    }

    /// Runs `work` in a write transaction, committed when it returns Ok.
    fn transaction<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut dyn Db) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        match self {
            Store::Sqlite(connection) => {
                let mut connection = lock(connection);
                // IMMEDIATE takes the write lock first, so two processes cannot both read
                // and then fail to upgrade.
                let mut transaction = rusqlite::Transaction::new(
                    &mut connection,
                    rusqlite::TransactionBehavior::Immediate,
                )?;
                let value = work(&mut transaction)?;
                transaction.commit()?;
                Ok(value)
            }
            Store::Postgres(postgres) => {
                postgres.run(move |connection| connection.transaction(|t| work(t)))
            }
        }
    }
}

fn lock(connection: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    // A panic while holding the lock leaves a consistent database: every write is a single
    // statement or transaction.
    connection.lock().unwrap_or_else(|p| p.into_inner())
}

#[derive(Clone)]
pub struct Ledger {
    store: Arc<Store>,
    /// This process's instance id: the owner of the runs it claims.
    instance: Arc<str>,
    host: Arc<str>,
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

/// Brings a SQLite ledger to `SQLITE_VERSION`; refuses a newer one.
fn migrate_sqlite(connection: &mut Connection, path: &Path) -> Result<()> {
    let transaction =
        rusqlite::Transaction::new(connection, rusqlite::TransactionBehavior::Immediate)?;
    let version: i64 = transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > SQLITE_VERSION {
        return Err(LedgerError::Version(format!(
            "The run history {} is at version {version}; this engine knows versions up to \
             {SQLITE_VERSION}. Use the engine that wrote it, or a newer one.",
            path.display()
        )));
    }
    if version < 1 {
        transaction.execute_batch(SQLITE_SCHEMA)?;
        for (column, kind) in SQLITE_V1_COLUMNS {
            let exists = transaction
                .prepare("SELECT 1 FROM pragma_table_info('runs') WHERE name = ?1")?
                .exists([column])?;
            if !exists {
                transaction
                    .execute_batch(&format!("ALTER TABLE runs ADD COLUMN {column} {kind}"))?;
            }
        }
    }
    transaction.execute_batch(&format!("PRAGMA user_version = {SQLITE_VERSION}"))?;
    transaction.commit()?;
    Ok(())
}

/// The PostgreSQL schema's version, and the migrations this engine has that it lacks.
pub struct PostgresVersion {
    pub current: i64,
    pub known: i64,
}

/// Brings the schema to the newest version this engine knows, with `apply`; otherwise only
/// checks it. Serialized across instances by an advisory lock.
fn migrate_postgres(db: &mut dyn Db, schema: &str, apply: bool) -> Result<PostgresVersion> {
    let known = POSTGRES_MIGRATIONS.len() as i64;
    db.lock("migrate")?;
    let exists = db
        .query_one(
            "SELECT 1 FROM pg_namespace WHERE nspname = ?1",
            cells![schema],
        )?
        .is_some();
    let current = if exists {
        let has_table = db
            .query_one(
                "SELECT 1 FROM pg_tables WHERE schemaname = ?1 AND tablename = 'schema_migrations'",
                cells![schema],
            )?
            .is_some();
        if has_table {
            db.query_one(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                &[],
            )?
            .map(|row| row.int(0))
            .transpose()?
            .unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };
    if current > known {
        return Err(LedgerError::Version(format!(
            "The run history in schema {schema} is at version {current}; this engine knows \
             versions up to {known}. Use the engine that wrote it, or a newer one."
        )));
    }
    if current < known && apply {
        if !exists {
            db.batch(&format!(
                "CREATE SCHEMA {}",
                crate::sql::quote_identifier(schema)
            ))?;
        }
        db.batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, \
             applied_at TEXT NOT NULL)",
        )?;
        for (index, sql) in POSTGRES_MIGRATIONS
            .iter()
            .enumerate()
            .skip(current as usize)
        {
            db.batch(sql)?;
            db.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, db_now())",
                cells![index as i64 + 1],
            )?;
        }
    }
    Ok(PostgresVersion { current, known })
}

/// Checks or migrates a PostgreSQL run history without opening it as an instance, for
/// `mage-service ledger migrate`.
pub fn postgres_version(url: &str, schema: &str, apply: bool) -> Result<PostgresVersion> {
    let postgres = Postgres::connect(url, schema)?;
    let schema = schema.to_string();
    postgres.run(move |connection| {
        connection.transaction(|transaction| migrate_postgres(transaction, &schema, apply))
    })
}

impl Ledger {
    /// Opens the run history at `location`, migrating an older schema when `migrate`.
    pub fn open_at(location: &Location, migrate: bool) -> Result<Ledger> {
        match location {
            Location::Sqlite(path) => Ledger::open(path),
            Location::Postgres {
                url,
                schema,
                service,
            } => Ledger::open_postgres(url, schema, service, migrate, &this_host()),
        }
    }

    pub fn open(path: &Path) -> Result<Ledger> {
        Ledger::open_on(path, &this_host())
    }

    fn open_on(path: &Path, host: &str) -> Result<Ledger> {
        let mut connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        // A run is acknowledged after its commit reaches the write-ahead log.
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(10))?;
        migrate_sqlite(&mut connection, path)?;
        connection.pragma_update(None, "wal_autocheckpoint", 0)?;
        let checkpoints = Connection::open(path)?;
        checkpoints.busy_timeout(std::time::Duration::from_secs(10))?;
        let ledger = Ledger::register(Store::Sqlite(Mutex::new(connection)), host)?;
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        start_checkpoints(
            checkpoints,
            PathBuf::from(wal),
            Arc::downgrade(&ledger.store),
        );
        start_heartbeats(ledger.clone());
        Ok(ledger)
    }

    fn open_postgres(
        url: &str,
        schema: &str,
        service: &str,
        migrate: bool,
        host: &str,
    ) -> Result<Ledger> {
        let postgres = Postgres::connect(url, schema)?;
        let (schema_name, service_name) = (schema.to_string(), service.to_string());
        postgres.run(move |connection| {
            connection.transaction(|db| {
                let version = migrate_postgres(db, &schema_name, migrate)?;
                if version.current < version.known && !migrate {
                    return Err(LedgerError::Version(format!(
                        "The run history in schema {schema_name} is at version {} and this \
                         engine needs {}: run `mage-service ledger migrate`, or start without \
                         MAGE_SERVICE_LEDGER_MIGRATE=off.",
                        version.current, version.known
                    )));
                }
                match db.query_one("SELECT name FROM service", &[])? {
                    Some(row) if row.text(0)? != service_name => {
                        Err(LedgerError::Version(format!(
                            "The run history in schema {schema_name} is service {}'s, not \
                             {service_name}'s: give each service its own schema \
                             (MAGE_SERVICE_LEDGER_SCHEMA).",
                            row.text(0)?
                        )))
                    }
                    Some(_) => Ok(()),
                    None => db
                        .execute(
                            "INSERT INTO service (name) VALUES (?1)",
                            cells![service_name],
                        )
                        .map(|_| ()),
                }
            })
        })?;
        let ledger = Ledger::register(Store::Postgres(postgres), host)?;
        start_heartbeats(ledger.clone());
        Ok(ledger)
    }

    /// Registers this process as an instance with a fresh lease.
    fn register(store: Store, host: &str) -> Result<Ledger> {
        let instance: Arc<str> = uuid::Uuid::now_v7().to_string().into();
        let host_name: Arc<str> = host.into();
        let (id, host) = (instance.to_string(), host.to_string());
        store.transaction(move |db| {
            let pid = i64::from(std::process::id());
            db.execute(
                "INSERT INTO instances (id, host, pid, started_at, expires_at) \
                 VALUES (?1, ?2, ?3, db_now(), db_time(?4))",
                cells![
                    id.as_str(),
                    host.as_str(),
                    pid,
                    format!("+{INSTANCE_LEASE_SECONDS} seconds")
                ],
            )?;
            expire_gone_processes(db, &host, &id)?;
            Ok(())
        })?;
        Ok(Ledger {
            store: Arc::new(store),
            instance,
            host: host_name,
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
        let (id, host) = (self.instance.to_string(), self.host.to_string());
        self.store.run(move |db| {
            expire_gone_processes(db, &host, &id)?;
            let live = db
                .query_one(
                    "SELECT 1 FROM instances WHERE id = ?1 AND expires_at > db_now()",
                    cells![id.as_str()],
                )?
                .is_some();
            db.execute(
                "INSERT INTO instances (id, host, pid, started_at, expires_at) \
                 VALUES (?1, ?4, ?2, db_now(), db_time(?3)) \
                 ON CONFLICT (id) DO UPDATE SET expires_at = excluded.expires_at",
                cells![
                    id.as_str(),
                    i64::from(std::process::id()),
                    format!("+{INSTANCE_LEASE_SECONDS} seconds"),
                    host.as_str()
                ],
            )?;
            db.execute(
                "DELETE FROM instances WHERE expires_at < db_time('-1 day')",
                &[],
            )?;
            Ok(live)
        })
    }

    /// Ends this instance's lease at once, so a restart recovers its runs without waiting
    /// for the lease to expire.
    pub fn release(&self) -> Result<()> {
        let id = self.instance.to_string();
        self.store.run(move |db| {
            db.execute(
                "UPDATE instances SET expires_at = db_now() WHERE id = ?1",
                cells![id],
            )
            .map(|_| ())
        })
    }

    #[cfg(test)]
    pub fn in_memory() -> Result<Ledger> {
        let mut connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        migrate_sqlite(&mut connection, Path::new(":memory:"))?;
        Ledger::register(Store::Sqlite(Mutex::new(connection)), "")
    }

    /// Runs a statement as is, for tests that set up states the API does not make.
    #[cfg(test)]
    pub fn sql(&self, sql: &str, params: &[crate::sql::Cell]) -> u64 {
        let (sql, params) = (sql.to_string(), params.to_vec());
        self.store.run(move |db| db.execute(&sql, &params)).unwrap()
    }

    /// Creates a queued run with its block runs. With an idempotency key already used, it
    /// returns the earlier run and false, unless that run failed or was cancelled: then the
    /// new run takes the key and records the earlier one in `retry_of`. A key used to return
    /// a failed run forever, so a caller's retry could never run the pipeline again.
    pub fn create_run(&self, new: NewRun) -> Result<(String, bool)> {
        let owned = OwnedRun::from(&new)?;
        self.store.transaction(move |db| create_run_in(db, &owned))
    }

    /// Creates the run of a schedule occurrence once; None when it exists already.
    pub fn create_occurrence_run(&self, trigger_key: &str, new: NewRun) -> Result<Option<String>> {
        let owned = OwnedRun::from(&new)?;
        let key = trigger_key.to_string();
        self.store.transaction(move |db| {
            db.lock(&format!("occurrence:{key}"))?;
            if db
                .query_one(
                    "SELECT run_id FROM occurrences WHERE trigger_key = ?1",
                    cells![key.as_str()],
                )?
                .is_some()
            {
                return Ok(None);
            }
            let (run_id, _) = create_run_in(db, &owned)?;
            db.execute(
                "INSERT INTO occurrences (trigger_key, run_id, created_at) \
                 VALUES (?1, ?2, ?3)",
                cells![key.as_str(), run_id.as_str(), timestamp(now())],
            )?;
            Ok(Some(run_id))
        })
    }

    pub fn has_occurrence(&self, trigger_key: &str) -> Result<bool> {
        let key = trigger_key.to_string();
        self.store.run(move |db| {
            Ok(db
                .query_one(
                    "SELECT 1 FROM occurrences WHERE trigger_key = ?1",
                    cells![key],
                )?
                .is_some())
        })
    }

    /// The oldest queued run of a pipeline, marked running, while fewer than `max` of its
    /// runs are running on any instance; None otherwise.
    pub fn claim_next(&self, pipeline: &str, max: u32) -> Result<Option<String>> {
        let (pipeline, instance) = (pipeline.to_string(), self.instance.to_string());
        self.store.transaction(move |db| {
            db.lock(&format!("claim:{pipeline}"))?;
            let running = db
                .query_one(
                    "SELECT COUNT(*) FROM runs WHERE pipeline = ?1 AND status = 'running'",
                    cells![pipeline.as_str()],
                )?
                .map(|row| row.int(0))
                .transpose()?
                .unwrap_or(0);
            if running >= i64::from(max) {
                return Ok(None);
            }
            let claimed = db.query_one(
                "UPDATE runs SET status = 'running', started_at = ?2, owner = ?3 \
                 WHERE id = (SELECT id FROM runs WHERE pipeline = ?1 AND status = 'queued' \
                 ORDER BY created_at, id LIMIT 1) AND status = 'queued' RETURNING id",
                cells![pipeline.as_str(), timestamp(now()), instance.as_str()],
            )?;
            claimed.map(|row| row.text(0)).transpose()
        })
    }

    /// Finishes a queued run, or a running run this instance holds; false when the run is
    /// another instance's or already finished (its recovery, a cancel), which then stands.
    pub fn finish_run(&self, id: &str, status: RunStatus, error: Option<&str>) -> Result<bool> {
        let error = error.map(mage_log::redact);
        let (id, instance) = (id.to_string(), self.instance.to_string());
        self.store.run(move |db| {
            Ok(db.execute(
                "UPDATE runs SET status = ?2, finished_at = ?3, error = ?4 WHERE id = ?1 \
                 AND (status = 'queued' OR (status = 'running' AND owner = ?5))",
                cells![id, status.as_str(), timestamp(now()), error, instance],
            )? == 1)
        })
    }

    /// Asks the instance running a run to cancel it; false when it is not running.
    pub fn request_cancel(&self, id: &str) -> Result<bool> {
        let id = id.to_string();
        self.store.run(move |db| {
            Ok(db.execute(
                "UPDATE runs SET cancel_requested = 1 WHERE id = ?1 AND status = 'running'",
                cells![id],
            )? == 1)
        })
    }

    /// This instance's running runs that another instance asked to cancel.
    pub fn cancel_requests(&self) -> Result<Vec<String>> {
        let instance = self.instance.to_string();
        self.store.run(move |db| {
            db.query(
                "SELECT id FROM runs WHERE owner = ?1 AND status = 'running' \
                 AND cancel_requested = 1",
                cells![instance],
            )?
            .iter()
            .map(|row| row.text(0))
            .collect()
        })
    }

    /// The instance that ran or runs a run, and its host, when it is not this one.
    pub fn other_owner(&self, id: &str) -> Result<Option<(String, String)>> {
        let (id, instance) = (id.to_string(), self.instance.to_string());
        self.store.run(move |db| {
            let Some(row) = db.query_one(
                "SELECT runs.owner, instances.host FROM runs LEFT JOIN instances \
                 ON instances.id = runs.owner WHERE runs.id = ?1 AND runs.owner IS NOT NULL \
                 AND runs.owner <> ?2",
                cells![id, instance],
            )?
            else {
                return Ok(None);
            };
            Ok(Some((row.text(0)?, row.opt_text(1)?.unwrap_or_default())))
        })
    }

    pub fn start_block(&self, run_id: &str, block: &str, attempt: u32) -> Result<()> {
        let (run_id, block) = (run_id.to_string(), block.to_string());
        let instance = self.instance.to_string();
        self.store.run(move |db| {
            db.execute(
                "UPDATE block_runs SET status = 'running', attempts = ?3, \
                 started_at = COALESCE(started_at, ?4), error = NULL \
                 WHERE run_id = ?1 AND block = ?2 AND EXISTS (SELECT 1 FROM runs \
                 WHERE runs.id = block_runs.run_id AND runs.status = 'running' \
                 AND runs.owner = ?5)",
                cells![run_id, block, attempt, timestamp(now()), instance],
            )
            .map(|_| ())
        })
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
        let error = error.map(mage_log::redact);
        let (run_id, block) = (run_id.to_string(), block.to_string());
        let instance = self.instance.to_string();
        self.store.run(move |db| {
            db.execute(
                "UPDATE block_runs SET status = ?3, finished_at = ?4, error = ?5, outputs = ?6 \
                 WHERE run_id = ?1 AND block = ?2 AND EXISTS (SELECT 1 FROM runs \
                 WHERE runs.id = block_runs.run_id AND runs.status = 'running' \
                 AND runs.owner = ?7)",
                cells![
                    run_id,
                    block,
                    status.as_str(),
                    timestamp(now()),
                    error,
                    outputs,
                    instance
                ],
            )
            .map(|_| ())
        })
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
        let (run_id, block) = (run_id.to_string(), block.to_string());
        let instance = self.instance.to_string();
        self.store.run(move |db| {
            db.execute(
                "UPDATE block_runs SET status = 'completed', started_at = ?3, finished_at = ?4, \
                 error = NULL, outputs = ?5 WHERE run_id = ?1 AND block = ?2 \
                 AND EXISTS (SELECT 1 FROM runs WHERE runs.id = block_runs.run_id \
                 AND runs.status = 'running' AND runs.owner = ?6)",
                cells![
                    run_id,
                    block,
                    timestamp(started_at),
                    timestamp(finished_at),
                    outputs,
                    instance
                ],
            )
            .map(|_| ())
        })
    }

    /// The runs of instances whose lease expired (a killed or stalled process, or a run from
    /// before owners were recorded): marks them and their unfinished block runs failed. The
    /// runs of live instances, this one included, are left alone. One statement claims the
    /// runs, so two instances recovering at once fail each run once.
    pub fn recover_interrupted(&self) -> Result<Vec<String>> {
        self.store.transaction(move |db| {
            let finished = timestamp(now());
            let mut ids: Vec<String> = db
                .query(
                    "UPDATE runs SET status = 'failed', finished_at = ?1, error = ?2 \
                     WHERE status = 'running' AND (owner IS NULL OR owner NOT IN \
                     (SELECT id FROM instances WHERE expires_at > db_now())) RETURNING id",
                    cells![
                        finished.as_str(),
                        "The service stopped while this run was running."
                    ],
                )?
                .iter()
                .map(|row| row.text(0))
                .collect::<Result<_>>()?;
            ids.sort();
            for id in &ids {
                db.execute(
                    "UPDATE block_runs SET status = CASE WHEN status = 'running' THEN 'failed' \
                     ELSE 'cancelled' END, finished_at = ?2, \
                     error = CASE WHEN status = 'running' THEN ?3 ELSE error END \
                     WHERE run_id = ?1 AND status IN ('running', 'pending')",
                    cells![
                        id.as_str(),
                        finished.as_str(),
                        "The service stopped while this block was running."
                    ],
                )?;
            }
            Ok(ids)
        })
    }

    pub fn settings(&self, pipeline: &str, default_max: u32) -> Result<Settings> {
        let pipeline = pipeline.to_string();
        self.store
            .run(move |db| settings_in(db, &pipeline, default_max))
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
        let pipeline = pipeline.to_string();
        self.store.run(move |db| {
            let changed = db.execute(
                "UPDATE pipeline_settings SET paused = COALESCE(?3, paused), \
                 max_concurrent_runs = COALESCE(?4, max_concurrent_runs), \
                 revision = revision + 1 WHERE pipeline = ?1 AND revision = ?2",
                cells![
                    pipeline.as_str(),
                    expected_revision,
                    paused,
                    max_concurrent_runs
                ],
            )?;
            let settings = settings_in(db, &pipeline, max_concurrent_runs.unwrap_or(1))?;
            Ok((settings, changed == 1))
        })
    }

    pub fn count(&self, pipeline: &str, status: RunStatus) -> Result<u64> {
        let pipeline = pipeline.to_string();
        self.store.run(move |db| {
            Ok(db
                .query_one(
                    "SELECT COUNT(*) FROM runs WHERE pipeline = ?1 AND status = ?2",
                    cells![pipeline, status.as_str()],
                )?
                .map(|row| row.int(0))
                .transpose()?
                .unwrap_or(0) as u64)
        })
    }

    pub fn list_runs(
        &self,
        pipeline: Option<&str>,
        status: Option<RunStatus>,
        limit: u32,
    ) -> Result<Vec<RunSummary>> {
        let pipeline = pipeline.map(str::to_string);
        self.store.run(move |db| {
            db.query(
                "SELECT id, pipeline, status, source, created_at, started_at, finished_at, \
                 error, retry_of FROM runs WHERE (CAST(?1 AS TEXT) IS NULL OR pipeline = ?1) \
                 AND (CAST(?2 AS TEXT) IS NULL OR status = ?2) \
                 ORDER BY created_at DESC, id DESC LIMIT ?3",
                cells![pipeline, status.map(RunStatus::as_str), limit],
            )?
            .iter()
            .map(run_summary)
            .collect()
        })
    }

    pub fn run(&self, id: &str) -> Result<Option<RunDetail>> {
        let id = id.to_string();
        self.store.run(move |db| {
            let Some(row) = db.query_one(
                "SELECT id, pipeline, status, source, created_at, started_at, finished_at, \
                 error, retry_of, variables FROM runs WHERE id = ?1",
                cells![id.as_str()],
            )?
            else {
                return Ok(None);
            };
            let run = run_summary(&row)?;
            let variables = row.text(9)?;
            let blocks = db
                .query(
                    "SELECT block, status, attempts, started_at, finished_at, error, outputs \
                     FROM block_runs WHERE run_id = ?1 ORDER BY position",
                    cells![id.as_str()],
                )?
                .iter()
                .map(|row| {
                    let started_at = row.opt_text(3)?;
                    let finished_at = row.opt_text(4)?;
                    Ok(BlockRunSummary {
                        block: row.text(0)?,
                        status: BlockStatus::parse(&row.text(1)?).unwrap_or(BlockStatus::Pending),
                        attempts: row.int(2)? as u32,
                        duration_ms: duration_ms(&started_at, &finished_at),
                        started_at,
                        finished_at,
                        error: row.opt_text(5)?,
                        outputs: serde_json::from_str(&row.text(6)?).unwrap_or_default(),
                    })
                })
                .collect::<Result<_>>()?;
            Ok(Some(RunDetail {
                run,
                variables: serde_json::from_str(&variables).unwrap_or_default(),
                blocks,
            }))
        })
    }

    /// Durations and finish times of the last finished runs, oldest first.
    pub fn recent_finished(&self, pipeline: &str, limit: u32) -> Result<Vec<(String, u64)>> {
        let pipeline = pipeline.to_string();
        self.store.run(move |db| {
            let mut rows = db
                .query(
                    "SELECT started_at, finished_at FROM runs WHERE pipeline = ?1 \
                     AND status IN ('completed', 'failed') AND started_at IS NOT NULL \
                     ORDER BY finished_at DESC LIMIT ?2",
                    cells![pipeline, limit],
                )?
                .iter()
                .map(|row| {
                    let started = row.opt_text(0)?;
                    let finished = row.opt_text(1)?;
                    Ok((
                        finished.clone().unwrap_or_default(),
                        duration_ms(&started, &finished).unwrap_or(0),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            rows.reverse();
            Ok(rows)
        })
    }

    pub fn last_error(&self, pipeline: &str) -> Result<Option<String>> {
        let pipeline = pipeline.to_string();
        self.store.run(move |db| {
            let Some(row) = db.query_one(
                "SELECT finished_at, error FROM runs WHERE pipeline = ?1 AND status = 'failed' \
                 ORDER BY finished_at DESC LIMIT 1",
                cells![pipeline],
            )?
            else {
                return Ok(None);
            };
            let at = row.opt_text(0)?;
            Ok(row
                .opt_text(1)?
                .map(|e| format!("{}: {e}", at.unwrap_or_default())))
        })
    }

    pub fn running_count(&self, pipeline: &str) -> Result<u32> {
        Ok(self.count(pipeline, RunStatus::Running)? as u32)
    }

    /// Takes or renews a named lease; false while another owner holds it.
    /// Times are the database's: instances on hosts with different clocks agree.
    pub fn acquire_lease(&self, name: &str, owner: &str, seconds: i64) -> Result<bool> {
        let (name, owner) = (name.to_string(), owner.to_string());
        self.store.run(move |db| {
            Ok(db.execute(
                "INSERT INTO leases (name, owner, expires_at) VALUES (?1, ?2, db_time(?3)) \
                 ON CONFLICT (name) DO UPDATE SET owner = excluded.owner, \
                 expires_at = excluded.expires_at \
                 WHERE leases.owner = excluded.owner OR leases.expires_at < db_now()",
                cells![name, owner, format!("{seconds:+} seconds")],
            )? == 1)
        })
    }

    /// Deletes finished runs older than the cutoff; returns their ids.
    pub fn prune(&self, older_than: DateTime<Utc>) -> Result<Vec<String>> {
        self.store.run(move |db| {
            db.query(
                "DELETE FROM runs WHERE status IN ('completed', 'failed', 'cancelled') \
                 AND finished_at < ?1 RETURNING id",
                cells![timestamp(older_than)],
            )?
            .iter()
            .map(|row| row.text(0))
            .collect()
        })
    }

    /// Whether run logs are kept here as well as in files: a PostgreSQL run history, which
    /// instances share.
    pub fn keeps_logs(&self) -> bool {
        matches!(*self.store, Store::Postgres(_))
    }

    /// Adds run log lines (run, line number, JSON line); a line already kept is skipped.
    /// Nothing happens in a run history that keeps no logs.
    pub fn append_logs(&self, lines: Vec<(String, u64, String)>) -> Result<()> {
        if !self.keeps_logs() || lines.is_empty() {
            return Ok(());
        }
        self.store.run(move |db| {
            // 3 parameters a line, well under PostgreSQL's 65,535 a statement.
            for chunk in lines.chunks(1000) {
                let mut sql = String::from("INSERT INTO run_logs (run_id, seq, line) VALUES ");
                let mut params = Vec::with_capacity(chunk.len() * 3);
                for (index, (run_id, seq, line)) in chunk.iter().enumerate() {
                    if index > 0 {
                        sql.push_str(", ");
                    }
                    let n = index * 3;
                    sql.push_str(&format!("(?{}, ?{}, ?{})", n + 1, n + 2, n + 3));
                    params.push(crate::sql::Cell::from(run_id.as_str()));
                    params.push(crate::sql::Cell::Int(*seq as i64));
                    params.push(crate::sql::Cell::from(line.as_str()));
                }
                sql.push_str(" ON CONFLICT DO NOTHING");
                db.execute(&sql, &params)?;
            }
            Ok(())
        })
    }

    /// A run's kept log lines in order; None when the run history keeps none for it.
    pub fn logs(&self, run_id: &str) -> Result<Option<Vec<String>>> {
        if !self.keeps_logs() {
            return Ok(None);
        }
        let run_id = run_id.to_string();
        self.store.run(move |db| {
            let lines = db
                .query(
                    "SELECT line FROM run_logs WHERE run_id = ?1 ORDER BY seq",
                    cells![run_id],
                )?
                .iter()
                .map(|row| row.text(0))
                .collect::<Result<Vec<String>>>()?;
            Ok((!lines.is_empty()).then_some(lines))
        })
    }

    /// Keeps a block output's file (its `index`th output); a kept one is replaced. Nothing
    /// happens in a run history that keeps no logs.
    pub fn put_output(&self, run_id: &str, block: &str, index: usize, data: Vec<u8>) -> Result<()> {
        if !self.keeps_logs() {
            return Ok(());
        }
        let (run_id, block) = (run_id.to_string(), block.to_string());
        self.store.run(move |db| {
            db.execute(
                "INSERT INTO run_outputs (run_id, block, idx, data) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (run_id, block, idx) DO UPDATE SET data = excluded.data",
                cells![run_id, block, index as i64, data],
            )?;
            Ok(())
        })
    }

    /// A kept block output's file; None when the run history has none.
    pub fn output(&self, run_id: &str, block: &str, index: usize) -> Result<Option<Vec<u8>>> {
        if !self.keeps_logs() {
            return Ok(None);
        }
        let (run_id, block) = (run_id.to_string(), block.to_string());
        self.store.run(move |db| {
            db.query_one(
                "SELECT data FROM run_outputs WHERE run_id = ?1 AND block = ?2 AND idx = ?3",
                cells![run_id, block, index as i64],
            )?
            .map(|row| row.bytes(0))
            .transpose()
        })
    }

    /// Copies a SQLite run history into this one (PostgreSQL), skipping runs it has: the
    /// move from one machine's file to a shared database. Returns the runs copied.
    pub fn import_sqlite(&self, path: &Path) -> Result<u64> {
        let mut source =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let version: i64 = source.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version > SQLITE_VERSION {
            return Err(LedgerError::Version(format!(
                "{} is at version {version}; this engine knows versions up to {SQLITE_VERSION}.",
                path.display()
            )));
        }
        let has_retry_of = source
            .prepare("SELECT 1 FROM pragma_table_info('runs') WHERE name = 'retry_of'")?
            .exists([])?;
        let retry_of = if has_retry_of { "retry_of" } else { "NULL" };
        let tables = [
            (
                "INSERT INTO runs (id, pipeline, status, source, idempotency_key, variables, \
                 created_at, started_at, finished_at, error, retry_of) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) ON CONFLICT DO NOTHING",
                format!(
                    "SELECT id, pipeline, status, source, idempotency_key, variables, \
                     created_at, started_at, finished_at, error, {retry_of} FROM runs"
                ),
            ),
            (
                "INSERT INTO block_runs (run_id, block, position, status, attempts, started_at, \
                 finished_at, error, outputs) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
                 ON CONFLICT DO NOTHING",
                "SELECT run_id, block, position, status, attempts, started_at, finished_at, \
                 error, outputs FROM block_runs"
                    .to_string(),
            ),
            (
                "INSERT INTO occurrences (trigger_key, run_id, created_at) VALUES (?1, ?2, ?3) \
                 ON CONFLICT DO NOTHING",
                "SELECT trigger_key, run_id, created_at FROM occurrences".to_string(),
            ),
            (
                "INSERT INTO pipeline_settings (pipeline, paused, max_concurrent_runs, \
                 revision) VALUES (?1, ?2, ?3, ?4) ON CONFLICT DO NOTHING",
                "SELECT pipeline, paused, max_concurrent_runs, revision FROM pipeline_settings"
                    .to_string(),
            ),
        ];
        let mut runs = 0;
        for (index, (insert, select)) in tables.into_iter().enumerate() {
            let rows: Vec<Vec<crate::sql::Cell>> = source
                .query(&select, &[])?
                .into_iter()
                .map(Row::into_cells)
                .collect();
            let copied = self.store.transaction(move |db| {
                let mut count = 0;
                for row in &rows {
                    count += db.execute(insert, row)?;
                }
                Ok(count)
            })?;
            if index == 0 {
                runs = copied;
            }
        }
        Ok(runs)
    }
}

/// A new run's values, owned, for a transaction on another thread.
struct OwnedRun {
    pipeline: String,
    source: String,
    variables: String,
    idempotency_key: Option<String>,
    blocks: Vec<String>,
}

impl OwnedRun {
    fn from(new: &NewRun) -> Result<OwnedRun> {
        Ok(OwnedRun {
            pipeline: new.pipeline.to_string(),
            source: new.source.to_string(),
            variables: serde_json::to_string(new.variables)
                .map_err(|e| LedgerError::Data(e.to_string()))?,
            idempotency_key: new.idempotency_key.map(str::to_string),
            blocks: new.blocks.to_vec(),
        })
    }
}

fn create_run_in(db: &mut dyn Db, new: &OwnedRun) -> Result<(String, bool)> {
    let mut retry_of: Option<String> = None;
    if let Some(key) = &new.idempotency_key {
        db.lock(&format!("key:{}:{key}", new.pipeline))?;
        let existing = db.query_one(
            "SELECT id, status FROM runs WHERE pipeline = ?1 AND idempotency_key = ?2",
            cells![new.pipeline.as_str(), key.as_str()],
        )?;
        if let Some(row) = existing {
            let (id, status) = (row.text(0)?, row.text(1)?);
            if status == "failed" || status == "cancelled" {
                db.execute(
                    "UPDATE runs SET idempotency_key = NULL WHERE id = ?1",
                    cells![id.as_str()],
                )?;
                retry_of = Some(id);
            } else {
                return Ok((id, false));
            }
        }
    }
    let id = uuid::Uuid::now_v7().to_string();
    db.execute(
        "INSERT INTO runs (id, pipeline, status, source, idempotency_key, variables, \
         created_at, retry_of) VALUES (?1, ?2, 'queued', ?3, ?4, ?5, ?6, ?7)",
        cells![
            id.as_str(),
            new.pipeline.as_str(),
            new.source.as_str(),
            new.idempotency_key.as_deref(),
            new.variables.as_str(),
            timestamp(now()),
            retry_of
        ],
    )?;
    for (position, block) in new.blocks.iter().enumerate() {
        db.execute(
            "INSERT INTO block_runs (run_id, block, position, status) \
             VALUES (?1, ?2, ?3, 'pending')",
            cells![id.as_str(), block.as_str(), position as i64],
        )?;
    }
    Ok((id, true))
}

fn settings_in(db: &mut dyn Db, pipeline: &str, default_max: u32) -> Result<Settings> {
    db.execute(
        "INSERT INTO pipeline_settings (pipeline, max_concurrent_runs) VALUES (?1, ?2) \
         ON CONFLICT DO NOTHING",
        cells![pipeline, default_max],
    )?;
    let row = db
        .query_one(
            "SELECT paused, max_concurrent_runs, revision FROM pipeline_settings \
             WHERE pipeline = ?1",
            cells![pipeline],
        )?
        .ok_or_else(|| LedgerError::Data(format!("no settings for {pipeline}")))?;
    Ok(Settings {
        paused: row.int(0)? != 0,
        max_concurrent_runs: row.int(1)? as u32,
        revision: row.int(2)? as u64,
    })
}

fn run_summary(row: &Row) -> Result<RunSummary> {
    let started_at = row.opt_text(5)?;
    let finished_at = row.opt_text(6)?;
    Ok(RunSummary {
        id: row.text(0)?,
        pipeline: row.text(1)?,
        status: RunStatus::parse(&row.text(2)?).unwrap_or(RunStatus::Failed),
        source: row.text(3)?,
        created_at: row.text(4)?,
        duration_ms: duration_ms(&started_at, &finished_at),
        started_at,
        finished_at,
        error: row.opt_text(7)?,
        retry_of: row.opt_text(8)?,
    })
}

/// Ends the leases of the other instances on this host whose process is gone, so their runs
/// are recovered now, not when their lease ends. A restarted container is pid 1 again, so
/// this pid under another instance id is gone too.
fn expire_gone_processes(db: &mut dyn Db, host: &str, me: &str) -> Result<()> {
    let pid = i64::from(std::process::id());
    let same_host = db.query(
        "SELECT id, pid FROM instances WHERE host = ?1 AND host <> '' AND id <> ?2 \
         AND expires_at > db_now()",
        cells![host, me],
    )?;
    for row in same_host {
        let other = row.int(1)?;
        if other == pid || !process_alive(other) {
            db.execute(
                "UPDATE instances SET expires_at = db_now() WHERE id = ?1",
                cells![row.text(0)?],
            )?;
        }
    }
    Ok(())
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
    let store = Arc::downgrade(&ledger.store);
    let (instance, host) = (ledger.instance.clone(), ledger.host.clone());
    drop(ledger);
    std::thread::Builder::new()
        .name("ledger-heartbeats".into())
        .spawn(move || {
            loop {
                std::thread::sleep(HEARTBEAT_EVERY);
                let Some(store) = store.upgrade() else {
                    break;
                };
                let ledger = Ledger {
                    store,
                    instance: instance.clone(),
                    host: host.clone(),
                };
                match ledger.heartbeat() {
                    Ok(true) => {}
                    Ok(false) => mage_log::warn(
                        "This instance's lease in the run history had expired; runs it was \
                         running may have been failed by another instance.",
                    ),
                    Err(e) => mage_log::warn(&format!("The run history heartbeat failed: {e}")),
                }
                match ledger.recover_interrupted() {
                    Ok(ids) if !ids.is_empty() => mage_log::warn(&format!(
                        "Failed {} run(s) of an instance that stopped: {}",
                        ids.len(),
                        ids.join(", ")
                    )),
                    Ok(_) => {}
                    Err(e) => mage_log::warn(&format!("Recovering runs failed: {e}")),
                }
            }
        })
        .expect("the heartbeat thread starts");
}

/// Checkpoints every second while the ledger is open.
fn start_checkpoints(connection: Connection, wal: PathBuf, ledger: Weak<Store>) {
    std::thread::Builder::new()
        .name("ledger-checkpoints".into())
        .spawn(move || {
            loop {
                std::thread::sleep(CHECKPOINT_EVERY);
                if ledger.upgrade().is_none() {
                    break;
                }
                if let Err(e) = checkpoint(&connection, &wal, WAL_LIMIT) {
                    mage_log::warn(&format!("The run history checkpoint failed: {e}"));
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
    fn a_sqlite_run_history_keeps_no_run_logs() {
        let ledger = Ledger::in_memory().unwrap();
        assert!(!ledger.keeps_logs());
        ledger
            .append_logs(vec![("r".into(), 0, "{}".into())])
            .unwrap();
        assert_eq!(ledger.logs("r").unwrap(), None);
        ledger.put_output("r", "b", 0, vec![1]).unwrap();
        assert_eq!(ledger.output("r", "b", 0).unwrap(), None);
    }

    #[test]
    fn commits_leave_the_checkpoint_to_the_background() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("runs.db");
        let ledger = Ledger::open(&path).unwrap();
        let Store::Sqlite(connection) = ledger.store.as_ref() else {
            unreachable!()
        };
        let automatic: i64 = lock(connection)
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
    fn a_ledger_from_an_earlier_release_is_migrated_and_a_newer_one_refused() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("runs.db");
        {
            // 0.2's ledger: no version, no owner or cancel columns, one running run.
            let old = Connection::open(&path).unwrap();
            old.execute_batch(SQLITE_SCHEMA).unwrap();
            old.execute_batch(
                "INSERT INTO runs (id, pipeline, status, source, variables, created_at) \
                 VALUES ('old', 'etl', 'running', 'api', '{}', '2026-10-01T00:00:00.000Z')",
            )
            .unwrap();
        }
        let ledger = Ledger::open(&path).unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        let (id, _) = ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        assert_eq!(ledger.run(&id).unwrap().unwrap().run.retry_of, None);
        assert_eq!(
            ledger.recover_interrupted().unwrap(),
            vec!["old".to_string()]
        );
        ledger.release().unwrap();
        drop(ledger);
        Connection::open(&path)
            .unwrap()
            .execute_batch("PRAGMA user_version = 99")
            .unwrap();
        let refused = Ledger::open(&path).err().unwrap().to_string();
        assert!(refused.contains("at version 99"), "{refused}");
    }

    #[test]
    fn runs_are_claimed_in_order_once() {
        let ledger = Ledger::in_memory().unwrap();
        let vars = Map::new();
        let blocks = vec!["a".to_string()];
        let (a, _) = ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        let (b, _) = ledger.create_run(new_run(&vars, None, &blocks)).unwrap();
        assert_eq!(ledger.claim_next("etl", 100).unwrap(), Some(a.clone()));
        assert_eq!(ledger.claim_next("etl", 100).unwrap(), Some(b));
        assert_eq!(ledger.claim_next("etl", 100).unwrap(), None);
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
        ledger.claim_next("etl", 100).unwrap();
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
        ledger.sql(
            "UPDATE instances SET expires_at = '2000-01-01T00:00:00.000Z' WHERE id = ?1",
            cells![instance],
        );
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
            first.claim_next("etl", 100).unwrap().as_deref(),
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
        first.claim_next("etl", 100).unwrap();
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
        first.claim_next("etl", 100).unwrap();
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
        first.sql(
            "UPDATE runs SET status = 'running', owner = NULL WHERE id = ?1",
            cells![old.as_str()],
        );
        let (mine, _) = first
            .create_run(new_run(&variables, Some("k"), &blocks))
            .unwrap();
        first.claim_next("etl", 100).unwrap();
        first.claim_next("etl", 100).unwrap();
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
        first.claim_next("etl", 100).unwrap();
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
            old.claim_next("etl", 100).unwrap();
            let pid = if pid == -1 {
                i64::from(std::process::id())
            } else {
                pid
            };
            old.sql(
                "UPDATE instances SET pid = ?1 WHERE id = ?2",
                cells![pid, old.instance()],
            );
            ids.push(id);
        }
        let restarted = Ledger::open_on(&path, "box").unwrap();
        let mut recovered = restarted.recover_interrupted().unwrap();
        recovered.sort();
        let mut expected = ids[..2].to_vec();
        expected.sort();
        assert_eq!(recovered, expected);
    }

    #[test]
    fn max_concurrent_runs_holds_across_instances() {
        let (_dir, first, second) = two_instances();
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        for _ in 0..3 {
            first
                .create_run(new_run(&variables, None, &blocks))
                .unwrap();
        }
        let a = first.claim_next("etl", 2).unwrap().unwrap();
        assert!(second.claim_next("etl", 2).unwrap().is_some());
        // Two run, on two instances: the third waits on both.
        assert_eq!(first.claim_next("etl", 2).unwrap(), None);
        assert_eq!(second.claim_next("etl", 2).unwrap(), None);
        first.finish_run(&a, RunStatus::Completed, None).unwrap();
        assert!(second.claim_next("etl", 2).unwrap().is_some());
    }

    #[test]
    fn a_cancel_reaches_the_instance_running_the_run() {
        let (_dir, first, second) = two_instances();
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        let (id, _) = first
            .create_run(new_run(&variables, None, &blocks))
            .unwrap();
        assert!(
            !second.request_cancel(&id).unwrap(),
            "a queued run is not running"
        );
        first.claim_next("etl", 1).unwrap();
        assert!(second.cancel_requests().unwrap().is_empty());
        assert!(second.request_cancel(&id).unwrap());
        assert!(
            second.cancel_requests().unwrap().is_empty(),
            "not second's run"
        );
        assert_eq!(first.cancel_requests().unwrap(), vec![id.clone()]);
        first
            .finish_run(&id, RunStatus::Cancelled, Some("Cancelled."))
            .unwrap();
        assert!(first.cancel_requests().unwrap().is_empty());
    }
}

/// The run history on PostgreSQL, with `MAGE_TEST_POSTGRES_URL` (integration_tests/compose.yaml
/// serves one: postgres://postgres:test@localhost:15432/test). Each test has its own schema.
#[cfg(test)]
mod postgres_tests {
    use std::collections::BTreeSet;

    use super::*;

    fn url() -> Option<String> {
        let url = std::env::var("MAGE_TEST_POSTGRES_URL").ok();
        if url.is_none() {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
        }
        url
    }

    fn schema() -> String {
        format!("ledger_test_{}", uuid::Uuid::new_v4().simple())
    }

    fn open(url: &str, schema: &str, host: &str) -> Ledger {
        Ledger::open_postgres(url, schema, "svc", true, host).unwrap()
    }

    fn drop_schema(ledger: &Ledger, schema: &str) {
        ledger.sql(&format!("DROP SCHEMA {schema} CASCADE"), &[]);
    }

    fn run<'a>(
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
    fn the_run_history_works_on_postgresql() {
        let Some(url) = url() else { return };
        let schema = schema();
        let ledger = open(&url, &schema, "a");
        let mut variables = Map::new();
        variables.insert("n".into(), Value::from(3));
        let blocks = vec!["load".to_string(), "save".to_string()];

        let (id, created) = ledger
            .create_run(run(&variables, Some("k"), &blocks))
            .unwrap();
        assert!(created);
        assert_eq!(
            ledger
                .create_run(run(&variables, Some("k"), &blocks))
                .unwrap(),
            (id.clone(), false)
        );
        assert_eq!(ledger.count("etl", RunStatus::Queued).unwrap(), 1);
        assert_eq!(
            ledger.claim_next("etl", 1).unwrap().as_deref(),
            Some(id.as_str())
        );
        ledger.start_block(&id, "load", 1).unwrap();
        ledger
            .finish_block(
                &id,
                "load",
                BlockStatus::Completed,
                None,
                &[Value::from("out")],
            )
            .unwrap();
        ledger.start_block(&id, "save", 2).unwrap();
        ledger
            .finish_block(&id, "save", BlockStatus::Failed, Some("disk full"), &[])
            .unwrap();
        assert!(
            ledger
                .finish_run(&id, RunStatus::Failed, Some("disk full"))
                .unwrap()
        );
        let detail = ledger.run(&id).unwrap().unwrap();
        assert_eq!(detail.run.status, RunStatus::Failed);
        assert_eq!(detail.variables["n"], 3);
        assert_eq!(detail.blocks[0].status, BlockStatus::Completed);
        assert_eq!(detail.blocks[0].outputs, vec![Value::from("out")]);
        assert_eq!(
            (detail.blocks[1].attempts, detail.blocks[1].error.as_deref()),
            (2, Some("disk full"))
        );
        assert!(
            ledger
                .last_error("etl")
                .unwrap()
                .unwrap()
                .ends_with("disk full")
        );
        assert_eq!(ledger.recent_finished("etl", 10).unwrap().len(), 1);

        // A failed run's key starts a new run, which records the old one.
        let (retry, created) = ledger
            .create_run(run(&variables, Some("k"), &blocks))
            .unwrap();
        assert!(created);
        assert_eq!(
            ledger.run(&retry).unwrap().unwrap().run.retry_of.as_deref(),
            Some(id.as_str())
        );
        assert_eq!(ledger.list_runs(None, None, 10).unwrap().len(), 2);
        assert_eq!(
            ledger
                .list_runs(Some("etl"), Some(RunStatus::Queued), 10)
                .unwrap()
                .len(),
            1
        );
        assert!(
            ledger
                .list_runs(Some("other"), None, 10)
                .unwrap()
                .is_empty()
        );

        let key = "etl/daily/2026-10-21T00:00:00Z";
        assert!(
            ledger
                .create_occurrence_run(key, run(&variables, None, &blocks))
                .unwrap()
                .is_some()
        );
        assert!(
            ledger
                .create_occurrence_run(key, run(&variables, None, &blocks))
                .unwrap()
                .is_none()
        );
        assert!(ledger.has_occurrence(key).unwrap());

        let settings = ledger.settings("etl", 2).unwrap();
        assert_eq!(
            (
                settings.paused,
                settings.max_concurrent_runs,
                settings.revision
            ),
            (false, 2, 1)
        );
        let (updated, ok) = ledger
            .update_settings("etl", 1, Some(true), Some(4))
            .unwrap();
        assert!(ok && updated.paused && updated.max_concurrent_runs == 4);
        assert!(
            !ledger
                .update_settings("etl", 1, Some(false), None)
                .unwrap()
                .1
        );

        assert!(ledger.acquire_lease("scheduler", "a", 30).unwrap());
        assert!(!ledger.acquire_lease("scheduler", "b", 30).unwrap());
        assert!(ledger.acquire_lease("other", "b", -1).unwrap());
        assert!(ledger.acquire_lease("other", "c", 30).unwrap());

        let pruned = ledger
            .prune(Utc::now() + chrono::Duration::days(1))
            .unwrap();
        assert_eq!(pruned, vec![id.clone()]);
        assert!(ledger.run(&id).unwrap().is_none());
        drop_schema(&ledger, &schema);
    }

    #[test]
    fn instances_on_postgresql_fence_recover_and_share_the_limits() {
        let Some(url) = url() else { return };
        let schema = schema();
        let first = open(&url, &schema, "first");
        let second = open(&url, &schema, "second");
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        let (id, _) = first.create_run(run(&variables, None, &blocks)).unwrap();
        first.create_run(run(&variables, None, &blocks)).unwrap();
        assert_eq!(
            first.claim_next("etl", 1).unwrap().as_deref(),
            Some(id.as_str())
        );
        assert_eq!(
            second.claim_next("etl", 1).unwrap(),
            None,
            "max 1 across instances"
        );
        assert!(second.recover_interrupted().unwrap().is_empty());

        assert!(second.request_cancel(&id).unwrap());
        assert_eq!(first.cancel_requests().unwrap(), vec![id.clone()]);

        first.start_block(&id, "load", 1).unwrap();
        second.sql(
            "UPDATE instances SET expires_at = '2000-01-01T00:00:00.000Z' WHERE id = ?1",
            cells![first.instance()],
        );
        assert_eq!(second.recover_interrupted().unwrap(), vec![id.clone()]);
        first
            .finish_block(&id, "load", BlockStatus::Completed, None, &[])
            .unwrap();
        assert!(!first.finish_run(&id, RunStatus::Completed, None).unwrap());
        let detail = second.run(&id).unwrap().unwrap();
        assert_eq!(
            (detail.run.status, detail.blocks[0].status),
            (RunStatus::Failed, BlockStatus::Failed)
        );
        assert!(!first.heartbeat().unwrap());
        assert!(first.heartbeat().unwrap());
        assert!(second.claim_next("etl", 1).unwrap().is_some());
        drop_schema(&first, &schema);
    }

    #[test]
    fn concurrent_claims_on_postgresql_take_each_run_once_within_the_limit() {
        let Some(url) = url() else { return };
        let schema = schema();
        let instances: Vec<Ledger> = (0..3)
            .map(|i| open(&url, &schema, &format!("host{i}")))
            .collect();
        let variables = Map::new();
        let blocks = vec!["load".to_string()];
        let mut all = BTreeSet::new();
        for _ in 0..40 {
            all.insert(
                instances[0]
                    .create_run(run(&variables, None, &blocks))
                    .unwrap()
                    .0,
            );
        }
        // Racing to concurrency 4 again and again, until every run ran.
        let mut done = BTreeSet::new();
        while done.len() < all.len() {
            let claimed: Vec<(usize, String)> = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..12)
                    .map(|worker| {
                        let ledger = instances[worker % 3].clone();
                        scope.spawn(move || {
                            let mut mine = Vec::new();
                            while let Some(id) = ledger.claim_next("etl", 4).unwrap() {
                                mine.push((worker % 3, id));
                            }
                            mine
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .flat_map(|w| w.join().unwrap())
                    .collect()
            });
            let remaining = all.len() - done.len();
            assert_eq!(claimed.len(), remaining.min(4), "{claimed:?}");
            for (instance, id) in claimed {
                assert!(done.insert(id.clone()), "{id} claimed twice");
                assert!(
                    instances[instance]
                        .finish_run(&id, RunStatus::Completed, None)
                        .unwrap()
                );
            }
        }
        assert_eq!(done, all);

        // One key, many requests at once: one run.
        let ids: BTreeSet<String> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|worker| {
                    let ledger = instances[worker % 3].clone();
                    let variables = &variables;
                    let blocks = &blocks;
                    scope.spawn(move || {
                        ledger
                            .create_run(run(variables, Some("same"), blocks))
                            .unwrap()
                            .0
                    })
                })
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).collect()
        });
        assert_eq!(ids.len(), 1);
        drop_schema(&instances[0], &schema);
    }

    #[test]
    fn a_postgresql_run_history_keeps_run_logs_in_order_until_the_run_is_pruned() {
        let Some(url) = url() else { return };
        let schema = schema();
        let ledger = open(&url, &schema, "a");
        let variables = Map::new();
        let (id, _) = ledger.create_run(run(&variables, None, &[])).unwrap();
        assert!(ledger.keeps_logs());
        assert_eq!(ledger.logs(&id).unwrap(), None);

        let line = |seq: u64, text: &str| (id.clone(), seq, format!("{{\"message\": \"{text}\"}}"));
        ledger
            .append_logs(vec![line(1, "second"), line(0, "first")])
            .unwrap();
        // A batch written again after a retry: kept once.
        ledger
            .append_logs(vec![line(1, "second"), line(2, "third")])
            .unwrap();
        let many: Vec<_> = (3..2503).map(|seq| line(seq, "more")).collect();
        ledger.append_logs(many).unwrap();
        let lines = ledger.logs(&id).unwrap().unwrap();
        assert_eq!(lines.len(), 2503);
        assert_eq!(
            &lines[..3],
            [line(0, "first").2, line(1, "second").2, line(2, "third").2]
        );

        ledger.finish_run(&id, RunStatus::Completed, None).unwrap();
        let pruned = ledger.prune(now() + chrono::Duration::days(1)).unwrap();
        assert_eq!(pruned, vec![id.clone()]);
        assert_eq!(ledger.logs(&id).unwrap(), None);
        drop_schema(&ledger, &schema);
    }

    #[test]
    fn a_postgresql_run_history_keeps_output_files_until_the_run_is_pruned() {
        let Some(url) = url() else { return };
        let schema = schema();
        let ledger = open(&url, &schema, "a");
        let variables = Map::new();
        let (id, _) = ledger.create_run(run(&variables, None, &[])).unwrap();
        assert_eq!(ledger.output(&id, "load", 0).unwrap(), None);
        let bytes: Vec<u8> = (0..=255).cycle().take(70_000).collect();
        ledger.put_output(&id, "load", 0, vec![1, 2]).unwrap();
        ledger.put_output(&id, "load", 0, bytes.clone()).unwrap();
        assert_eq!(ledger.output(&id, "load", 0).unwrap(), Some(bytes));
        assert_eq!(ledger.output(&id, "load", 1).unwrap(), None);
        ledger.finish_run(&id, RunStatus::Completed, None).unwrap();
        ledger.prune(now() + chrono::Duration::days(1)).unwrap();
        assert_eq!(ledger.output(&id, "load", 0).unwrap(), None);
        drop_schema(&ledger, &schema);
    }

    #[test]
    fn a_postgresql_run_history_is_versioned_and_holds_one_service() {
        let Some(url) = url() else { return };
        let schema = schema();
        let refused = Ledger::open_postgres(&url, &schema, "svc", false, "a")
            .err()
            .unwrap();
        assert!(refused.to_string().contains("ledger migrate"), "{refused}");
        let version = postgres_version(&url, &schema, true).unwrap();
        assert_eq!(
            (version.current, version.known),
            (0, POSTGRES_MIGRATIONS.len() as i64)
        );
        let version = postgres_version(&url, &schema, true).unwrap();
        assert_eq!(version.current, version.known);
        let ledger = Ledger::open_postgres(&url, &schema, "svc", false, "a").unwrap();
        let other = Ledger::open_postgres(&url, &schema, "billing", true, "a")
            .err()
            .unwrap();
        assert!(
            other.to_string().contains("service svc's, not billing's"),
            "{other}"
        );
        ledger.sql(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (99, 'x')",
            &[],
        );
        let newer = Ledger::open_postgres(&url, &schema, "svc", true, "a")
            .err()
            .unwrap();
        assert!(newer.to_string().contains("at version 99"), "{newer}");
        drop_schema(&ledger, &schema);
    }

    #[test]
    fn a_sqlite_run_history_is_copied_into_postgresql_once() {
        let Some(url) = url() else { return };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runs.db");
        let variables = Map::new();
        let blocks = vec!["load".to_string(), "save".to_string()];
        {
            let local = Ledger::open(&path).unwrap();
            for key in ["a", "b", "c"] {
                let (id, _) = local
                    .create_run(run(&variables, Some(key), &blocks))
                    .unwrap();
                local.claim_next("etl", 5).unwrap();
                local.finish_run(&id, RunStatus::Completed, None).unwrap();
            }
            local.settings("etl", 3).unwrap();
            local.release().unwrap();
        }
        let schema = schema();
        let ledger = open(&url, &schema, "a");
        assert_eq!(ledger.import_sqlite(&path).unwrap(), 3);
        assert_eq!(ledger.import_sqlite(&path).unwrap(), 0);
        let runs = ledger
            .list_runs(Some("etl"), Some(RunStatus::Completed), 10)
            .unwrap();
        assert_eq!(runs.len(), 3);
        assert_eq!(ledger.run(&runs[0].id).unwrap().unwrap().blocks.len(), 2);
        assert_eq!(ledger.settings("etl", 1).unwrap().max_concurrent_runs, 3);
        // The keys came along: a repeated request returns the imported run.
        let (_, created) = ledger
            .create_run(run(&variables, Some("a"), &blocks))
            .unwrap();
        assert!(!created);
        drop_schema(&ledger, &schema);
    }
}
