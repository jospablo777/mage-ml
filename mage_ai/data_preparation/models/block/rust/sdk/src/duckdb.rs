//! DuckDB for Rust blocks (feature `duckdb`), with DuckDB built into the block's program.
//! Frames go in and out through Parquet, which both sides read and write column by column
//! with their types: no row is converted on its own.
//!
//! ```ignore
//! use mage::duckdb::{DuckDb, WritePolicy};
//! use mage::prelude::*;
//!
//! fn load_data() -> Result<DataFrame> {
//!     DuckDb::connect()?.load("SELECT * FROM orders WHERE amount > 10")
//! }
//!
//! fn export_data(totals: DataFrame) -> Result<()> {
//!     DuckDb::connect()?.write(&totals, "daily_totals", WritePolicy::Replace)?;
//!     Ok(())
//! }
//! ```
//!
//! The profile gives the database (DUCKDB_DATABASE: a file, relative to the project,
//! `:memory:`, or `md:name` for MotherDuck with MOTHERDUCK_TOKEN) and the schema of
//! unqualified tables (DUCKDB_SCHEMA, `main` by default), as for MageML's Python client.
//!
//! Reading: integers, floats, decimals, text, dates, times, timestamps (with their time
//! zone; DuckDB keeps microseconds), lists, structs and maps arrive with their types. HUGEINT, UHUGEINT, UUID,
//! INTERVAL, ENUM and BIT, which Parquet cannot carry exactly, arrive as their exact text.
//!
//! Writing: `Replace` swaps the table in one statement, so readers see the old rows or
//! the new ones; `Append` inserts by column name; `Fail` refuses an existing table. DuckDB
//! allows one writing process per database file: a block waits up to
//! MAGE_DUCKDB_LOCK_TIMEOUT seconds (300) for another one to close it.
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub use duckdb;
use duckdb::Connection;

use crate::prelude::*;

/// What `write` does when the table exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    /// Replace the table with the frame, in one statement.
    Replace,
    /// Insert the rows into the existing table, by column name; create it if missing.
    Append,
    /// Fail if the table exists.
    Fail,
}

pub struct DuckDb {
    connection: Connection,
    schema: String,
}

/// Types Parquet cannot carry exactly; queries return them as text.
const AS_TEXT: &[&str] = &["HUGEINT", "UHUGEINT", "UUID", "INTERVAL", "BIT"];

/// A DuckDB identifier, quoted.
pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Whether a column of this type is read as text.
fn read_as_text(column_type: &str) -> bool {
    let upper = column_type.trim().to_ascii_uppercase();
    AS_TEXT.contains(&upper.as_str()) || upper.starts_with("ENUM(")
}

/// A file for one frame on its way in or out, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Scratch {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            "mage-duckdb-{}-{}-{}.parquet",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        );
        Scratch(std::env::temp_dir().join(name))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn literal(&self) -> String {
        literal(&self.0.to_string_lossy())
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn lock_timeout() -> Duration {
    let seconds = std::env::var("MAGE_DUCKDB_LOCK_TIMEOUT")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(300.0);
    Duration::from_secs_f64(seconds.max(0.0))
}

impl DuckDb {
    /// Opens the database of the block's io_config profile.
    pub fn connect() -> Result<Self> {
        Self::connect_settings(&crate::profile::settings()?)
    }

    /// Opens DUCKDB_DATABASE with DUCKDB_SCHEMA and MOTHERDUCK_TOKEN.
    pub fn connect_settings(settings: &HashMap<String, String>) -> Result<Self> {
        let get = |key: &str| {
            settings
                .get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty() && v != "None")
        };
        let database = get("DUCKDB_DATABASE")
            .ok_or_else(|| anyhow!("The profile has no DUCKDB_DATABASE: a file or :memory:"))?;
        let mut db = Self::open_with(&database, get("MOTHERDUCK_TOKEN").as_deref())?;
        if let Some(schema) = get("DUCKDB_SCHEMA") {
            db.schema = schema;
        }
        Ok(db)
    }

    /// Opens a database file, `:memory:`, or `md:name` for MotherDuck.
    pub fn open(database: &str) -> Result<Self> {
        Self::open_with(database, None)
    }

    fn open_with(database: &str, motherduck_token: Option<&str>) -> Result<Self> {
        let timeout = lock_timeout();
        let deadline = Instant::now() + timeout;
        let mut delay = Duration::from_millis(50);
        loop {
            let opened = if database == ":memory:" {
                Connection::open_in_memory()
            } else if let (true, Some(token)) = (database.starts_with("md:"), motherduck_token) {
                duckdb::Config::default()
                    .with("motherduck_token", token)
                    .and_then(|config| Connection::open_with_flags(database, config))
            } else {
                Connection::open(database)
            };
            match opened {
                Ok(connection) => {
                    return Ok(DuckDb {
                        connection,
                        schema: "main".into(),
                    });
                }
                Err(error)
                    if error.to_string().to_lowercase().contains("lock")
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_secs(1));
                }
                Err(error) if error.to_string().to_lowercase().contains("lock") => {
                    bail!(
                        "The DuckDB database {database} is open for writing in another \
                         process, and the block waited {:.0} s for it. DuckDB allows one \
                         writing process per database file: close the other connection, or \
                         give blocks that run at the same time separate databases. \
                         MAGE_DUCKDB_LOCK_TIMEOUT sets the wait in seconds. DuckDB said: \
                         {error}",
                        timeout.as_secs_f64()
                    );
                }
                Err(error) => return Err(error).with_context(|| format!("Opening {database}")),
            }
        }
    }

    /// The connection, for anything else DuckDB offers.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Runs statements that return no rows.
    pub fn execute(&self, sql: &str) -> Result<()> {
        self.connection
            .execute_batch(sql)
            .with_context(|| format!("Running {}", first_line(sql)))
    }

    /// The query's columns and their DuckDB types.
    pub fn describe(&self, query: &str) -> Result<Vec<(String, String)>> {
        let query = query.trim().trim_end_matches(';');
        let mut statement = self
            .connection
            .prepare(&format!("DESCRIBE {query}"))
            .with_context(|| format!("Describing {}", first_line(query)))?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// The rows of a query as a frame.
    pub fn load(&self, query: &str) -> Result<DataFrame> {
        let query = query.trim().trim_end_matches(';');
        let columns = self.describe(query)?;
        let selected = if columns.iter().any(|(_, kind)| read_as_text(kind)) {
            let list: Vec<String> = columns
                .iter()
                .map(|(name, kind)| {
                    if read_as_text(kind) {
                        format!("CAST({} AS VARCHAR) AS {}", quote(name), quote(name))
                    } else {
                        quote(name)
                    }
                })
                .collect();
            format!("SELECT {} FROM ({query})", list.join(", "))
        } else {
            query.to_string()
        };
        let scratch = Scratch::new();
        self.execute(&format!(
            "COPY ({selected}) TO {} (FORMAT parquet)",
            scratch.literal()
        ))?;
        let file = File::open(scratch.path()).context("Reading the query's rows")?;
        Ok(ParquetReader::new(file).finish()?)
    }

    /// The table's name with its schema: `schema.table` as given, or the profile's schema.
    pub fn full_name(&self, table: &str) -> String {
        match table.split_once('.') {
            Some((schema, name)) => format!("{}.{}", quote(schema), quote(name)),
            None => format!("{}.{}", quote(&self.schema), quote(table)),
        }
    }

    fn exists(&self, table: &str) -> Result<bool> {
        let (schema, name) = table.split_once('.').unwrap_or((&self.schema, table));
        let count: i64 = self.connection.query_row(
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = ? AND table_name = ?",
            [schema, name],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Writes the frame to the table; returns the rows written.
    pub fn write(&self, frame: &DataFrame, table: &str, policy: WritePolicy) -> Result<usize> {
        let full = self.full_name(table);
        let schema = table
            .split_once('.')
            .map(|(schema, _)| schema)
            .unwrap_or(&self.schema);
        let exists = self.exists(table)?;
        if exists && policy == WritePolicy::Fail {
            bail!("The table {full} exists, and the write policy is Fail");
        }
        let scratch = Scratch::new();
        let mut copy = frame.clone();
        let file = File::create(scratch.path()).context("Writing the frame for DuckDB")?;
        ParquetWriter::new(file).finish(&mut copy)?;
        let source = format!("read_parquet({})", scratch.literal());
        let sql = match (policy, exists) {
            (WritePolicy::Append, true) => {
                format!("INSERT INTO {full} BY NAME SELECT * FROM {source}")
            }
            _ => format!(
                "CREATE SCHEMA IF NOT EXISTS {}; CREATE OR REPLACE TABLE {full} AS \
                 SELECT * FROM {source}",
                quote(schema)
            ),
        };
        self.execute(&sql)?;
        Ok(frame.height())
    }
}

fn first_line(sql: &str) -> String {
    let line = sql.trim().lines().next().unwrap_or_default();
    line.chars().take(120).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory() -> DuckDb {
        DuckDb::open(":memory:").unwrap()
    }

    #[test]
    fn a_frame_comes_back_with_its_types_and_values() {
        let db = memory();
        let at = Series::new("at".into(), [Some(1_717_236_000_123_456i64), None])
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap();
        let utc = Series::new("utc".into(), [Some(1_717_236_000_123i64), Some(0)])
            .cast(&DataType::Datetime(
                TimeUnit::Milliseconds,
                Some(TimeZone::UTC),
            ))
            .unwrap();
        let mut frame = df!(
            "id" => [Some(1i64), Some(2i64)],
            "big" => [Some(i64::MAX), None],
            "amount" => [Some(0.1f64), None],
            "label" => [Some("Zürich ✓"), Some("")],
            "flag" => [Some(true), None],
        )
        .unwrap();
        frame.with_column(at.into_column()).unwrap();
        frame.with_column(utc.into_column()).unwrap();
        let tags = Series::new(
            "tags".into(),
            [
                Series::new("".into(), [1i64, 2]),
                Series::new("".into(), Vec::<i64>::new()),
            ],
        );
        frame.with_column(tags.into_column()).unwrap();

        assert_eq!(db.write(&frame, "orders", WritePolicy::Replace).unwrap(), 2);
        let back = db.load("SELECT * FROM orders ORDER BY id").unwrap();

        assert!(back.equals_missing(&frame), "{back}\n{frame}");
        // DuckDB keeps timestamps in microseconds: the millisecond column comes back in
        // microseconds, with the same values.
        let mut expected = frame.schema().as_ref().clone();
        expected.set_dtype(
            "utc",
            DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
        );
        assert_eq!(back.schema().as_ref(), &expected);
    }

    #[test]
    fn types_parquet_cannot_carry_arrive_as_exact_text() {
        let db = memory();
        db.execute(
            "CREATE TYPE mood AS ENUM ('sad', 'ok'); \
             CREATE TABLE t AS SELECT \
               170141183460469231731687303715884105727::HUGEINT AS huge, \
               'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID AS id, \
               INTERVAL 1 MONTH + INTERVAL 2 DAY AS span, \
               'ok'::mood AS mood, \
               12345678901234567890.1234567890123456::DECIMAL(38, 16) AS amount",
        )
        .unwrap();
        let frame = db.load("SELECT * FROM t").unwrap();
        let text = |name: &str| frame.column(name).unwrap().str().unwrap().get(0).unwrap();

        assert_eq!(text("huge"), "170141183460469231731687303715884105727");
        assert_eq!(text("id"), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assert_eq!(text("span"), "1 month 2 days");
        assert_eq!(text("mood"), "ok");
        assert_eq!(
            frame.column("amount").unwrap().dtype(),
            &DataType::Decimal(38, 16)
        );
        assert_eq!(
            frame.column("amount").unwrap().get(0).unwrap().to_string(),
            "12345678901234567890.1234567890123456"
        );
    }

    #[test]
    fn append_goes_by_name_and_fail_refuses_an_existing_table() {
        let db = DuckDb::connect_settings(&HashMap::from([
            ("DUCKDB_DATABASE".to_string(), ":memory:".to_string()),
            ("DUCKDB_SCHEMA".to_string(), "sales".to_string()),
        ]))
        .unwrap();
        let first = df!("id" => [1i64], "name" => ["a"]).unwrap();
        let reordered = df!("name" => ["b"], "id" => [2i64]).unwrap();

        db.write(&first, "people", WritePolicy::Append).unwrap();
        db.write(&reordered, "people", WritePolicy::Append).unwrap();
        let back = db
            .load("SELECT id, name FROM sales.people ORDER BY id")
            .unwrap();
        assert_eq!(back, df!("id" => [1i64, 2], "name" => ["a", "b"]).unwrap());

        let error = db.write(&first, "people", WritePolicy::Fail).unwrap_err();
        assert!(error.to_string().contains("Fail"), "{error}");
        db.write(&first, "people", WritePolicy::Replace).unwrap();
        assert_eq!(
            db.load("SELECT count(*) AS n FROM sales.people")
                .unwrap()
                .column("n")
                .unwrap()
                .i64()
                .unwrap()
                .get(0),
            Some(1)
        );
    }

    #[test]
    fn errors_name_the_statement_and_the_database() {
        let db = memory();
        let error = db.load("SELECT * FROM missing_table").unwrap_err();
        assert!(format!("{error:#}").contains("missing_table"), "{error:#}");
        let Err(error) = DuckDb::connect_settings(&HashMap::new()) else {
            panic!("connected without DUCKDB_DATABASE");
        };
        assert!(error.to_string().contains("DUCKDB_DATABASE"), "{error}");
        assert!(read_as_text("ENUM('a', 'b')"));
        assert!(!read_as_text("VARCHAR"));
    }
}
