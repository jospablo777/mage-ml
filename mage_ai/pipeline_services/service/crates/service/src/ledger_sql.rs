//! The SQL the run history runs, on SQLite or PostgreSQL. Statements are written once, with
//! `?N` parameters and `db_now()` / `db_time(offset)` for the database's time as ISO 8601
//! text in UTC with milliseconds; each backend rewrites them for its dialect. Values are
//! integers, text or NULL, which both store the same way, so a statement means the same on
//! both.
//!
//! PostgreSQL runs on a thread of its own with its own runtime, so the ledger's calls stay
//! synchronous, as they are with SQLite, and work from any thread or runtime.

use std::collections::HashMap;
use std::sync::mpsc;

use crate::ledger::{LedgerError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Int(i64),
    Text(String),
}

impl From<&str> for Cell {
    fn from(value: &str) -> Cell {
        Cell::Text(value.to_string())
    }
}

impl From<String> for Cell {
    fn from(value: String) -> Cell {
        Cell::Text(value)
    }
}

impl From<&String> for Cell {
    fn from(value: &String) -> Cell {
        Cell::Text(value.clone())
    }
}

impl From<i64> for Cell {
    fn from(value: i64) -> Cell {
        Cell::Int(value)
    }
}

impl From<u32> for Cell {
    fn from(value: u32) -> Cell {
        Cell::Int(i64::from(value))
    }
}

impl From<u64> for Cell {
    fn from(value: u64) -> Cell {
        Cell::Int(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl From<bool> for Cell {
    fn from(value: bool) -> Cell {
        Cell::Int(i64::from(value))
    }
}

impl<T: Into<Cell>> From<Option<T>> for Cell {
    fn from(value: Option<T>) -> Cell {
        value.map_or(Cell::Null, Into::into)
    }
}

/// Parameters of a statement: `cells![a, b]`.
macro_rules! cells {
    ($($value:expr),* $(,)?) => {
        &[$($crate::ledger_sql::Cell::from($value)),*]
    };
}
pub(crate) use cells;

pub struct Row(Vec<Cell>);

impl Row {
    pub fn into_cells(self) -> Vec<Cell> {
        self.0
    }

    fn cell(&self, index: usize) -> Result<&Cell> {
        self.0
            .get(index)
            .ok_or_else(|| LedgerError::Data(format!("no column {index}")))
    }

    pub fn text(&self, index: usize) -> Result<String> {
        self.opt_text(index)?
            .ok_or_else(|| LedgerError::Data(format!("column {index} is NULL")))
    }

    pub fn opt_text(&self, index: usize) -> Result<Option<String>> {
        match self.cell(index)? {
            Cell::Null => Ok(None),
            Cell::Text(text) => Ok(Some(text.clone())),
            Cell::Int(value) => Ok(Some(value.to_string())),
        }
    }

    pub fn int(&self, index: usize) -> Result<i64> {
        match self.cell(index)? {
            Cell::Int(value) => Ok(*value),
            Cell::Text(text) => text
                .parse()
                .map_err(|_| LedgerError::Data(format!("column {index} is not a number"))),
            Cell::Null => Err(LedgerError::Data(format!("column {index} is NULL"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Dialect {
    Sqlite,
    Postgres,
}

/// A connection or a transaction on one.
pub trait Db {
    fn execute(&mut self, sql: &str, params: &[Cell]) -> Result<u64>;
    fn query(&mut self, sql: &str, params: &[Cell]) -> Result<Vec<Row>>;
    /// Statements without parameters, separated by semicolons (schema changes).
    fn batch(&mut self, sql: &str) -> Result<()>;
    /// Serializes the transaction with every other one that takes this key, until it ends.
    /// SQLite's write transactions are serialized already.
    fn lock(&mut self, key: &str) -> Result<()>;

    fn query_one(&mut self, sql: &str, params: &[Cell]) -> Result<Option<Row>> {
        Ok(self.query(sql, params)?.into_iter().next())
    }
}

const SQLITE_FORMAT: &str = "'%Y-%m-%dT%H:%M:%fZ'";
const POSTGRES_FORMAT: &str = "'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'";

/// The statement in the dialect: `?N` parameters, `db_now()` and `db_time(offset)`, an
/// offset such as `'+30 seconds'` or a parameter holding one.
pub fn rewrite(sql: &str, dialect: Dialect) -> String {
    let mut out = String::with_capacity(sql.len() + 64);
    let mut rest = sql;
    loop {
        let now = rest.find("db_now()");
        let time = rest.find("db_time(");
        let next = match (now, time) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => break,
        };
        out.push_str(&rest[..next]);
        if Some(next) == now {
            out.push_str(&match dialect {
                Dialect::Sqlite => format!("strftime({SQLITE_FORMAT}, 'now')"),
                Dialect::Postgres => {
                    format!("to_char(clock_timestamp() AT TIME ZONE 'UTC', {POSTGRES_FORMAT})")
                }
            });
            rest = &rest[next + "db_now()".len()..];
        } else {
            let start = next + "db_time(".len();
            let end = start + rest[start..].find(')').expect("db_time( is closed");
            let offset = &rest[start..end];
            out.push_str(&match dialect {
                Dialect::Sqlite => format!("strftime({SQLITE_FORMAT}, 'now', {offset})"),
                Dialect::Postgres => format!(
                    "to_char((clock_timestamp() + CAST(CAST({offset} AS TEXT) AS INTERVAL)) \
                     AT TIME ZONE 'UTC', {POSTGRES_FORMAT})"
                ),
            });
            rest = &rest[end + 1..];
        }
    }
    out.push_str(rest);
    match dialect {
        Dialect::Sqlite => out,
        Dialect::Postgres => out.replace('?', "$"),
    }
}

// SQLite.

impl rusqlite::ToSql for Cell {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value, ValueRef};
        Ok(match self {
            Cell::Null => ToSqlOutput::Owned(Value::Null),
            Cell::Int(value) => ToSqlOutput::Owned(Value::Integer(*value)),
            Cell::Text(text) => ToSqlOutput::Borrowed(ValueRef::Text(text.as_bytes())),
        })
    }
}

fn sqlite_rows(
    connection: &rusqlite::Connection,
    sql: &str,
    params: &[Cell],
) -> rusqlite::Result<Vec<Row>> {
    use rusqlite::types::ValueRef;
    let mut statement = connection.prepare_cached(sql)?;
    let columns = statement.column_count();
    let mut rows = statement.query(rusqlite::params_from_iter(params.iter()))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let mut cells = Vec::with_capacity(columns);
        for index in 0..columns {
            cells.push(match row.get_ref(index)? {
                ValueRef::Null => Cell::Null,
                ValueRef::Integer(value) => Cell::Int(value),
                ValueRef::Real(value) => Cell::Int(value as i64),
                ValueRef::Text(text) | ValueRef::Blob(text) => {
                    Cell::Text(String::from_utf8_lossy(text).into_owned())
                }
            });
        }
        out.push(Row(cells));
    }
    Ok(out)
}

impl Db for rusqlite::Connection {
    fn execute(&mut self, sql: &str, params: &[Cell]) -> Result<u64> {
        let sql = rewrite(sql, Dialect::Sqlite);
        let mut statement = self.prepare_cached(&sql)?;
        Ok(statement.execute(rusqlite::params_from_iter(params.iter()))? as u64)
    }

    fn query(&mut self, sql: &str, params: &[Cell]) -> Result<Vec<Row>> {
        Ok(sqlite_rows(self, &rewrite(sql, Dialect::Sqlite), params)?)
    }

    fn batch(&mut self, sql: &str) -> Result<()> {
        Ok(self.execute_batch(sql)?)
    }

    fn lock(&mut self, _key: &str) -> Result<()> {
        Ok(())
    }
}

impl Db for rusqlite::Transaction<'_> {
    fn execute(&mut self, sql: &str, params: &[Cell]) -> Result<u64> {
        let sql = rewrite(sql, Dialect::Sqlite);
        let mut statement = self.prepare_cached(&sql)?;
        Ok(statement.execute(rusqlite::params_from_iter(params.iter()))? as u64)
    }

    fn query(&mut self, sql: &str, params: &[Cell]) -> Result<Vec<Row>> {
        Ok(sqlite_rows(self, &rewrite(sql, Dialect::Sqlite), params)?)
    }

    fn batch(&mut self, sql: &str) -> Result<()> {
        Ok(self.execute_batch(sql)?)
    }

    fn lock(&mut self, _key: &str) -> Result<()> {
        Ok(())
    }
}

// PostgreSQL.

impl tokio_postgres::types::ToSql for Cell {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        use tokio_postgres::types::{IsNull, Type};
        match self {
            Cell::Null => Ok(IsNull::Yes),
            Cell::Int(value) => match *ty {
                Type::INT2 => i16::try_from(*value)?.to_sql(ty, out),
                Type::INT4 => i32::try_from(*value)?.to_sql(ty, out),
                Type::INT8 => value.to_sql(ty, out),
                Type::BOOL => (*value != 0).to_sql(ty, out),
                _ => value.to_string().to_sql(ty, out),
            },
            Cell::Text(text) => match *ty {
                Type::INT2 | Type::INT4 | Type::INT8 => Cell::Int(text.parse()?).to_sql(ty, out),
                _ => text.as_str().to_sql(ty, out),
            },
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

fn postgres_row(row: &tokio_postgres::Row) -> Result<Row> {
    use tokio_postgres::types::Type;
    let mut cells = Vec::with_capacity(row.len());
    for (index, column) in row.columns().iter().enumerate() {
        let cell = match *column.type_() {
            Type::INT2 => row.try_get::<_, Option<i16>>(index)?.map(i64::from).into(),
            Type::INT4 => row.try_get::<_, Option<i32>>(index)?.map(i64::from).into(),
            Type::INT8 => row.try_get::<_, Option<i64>>(index)?.into(),
            Type::BOOL => row.try_get::<_, Option<bool>>(index)?.into(),
            // pg_advisory_xact_lock's result.
            Type::VOID => Cell::Null,
            _ => row.try_get::<_, Option<String>>(index)?.into(),
        };
        cells.push(cell);
    }
    Ok(Row(cells))
}

/// A job for the PostgreSQL thread: it gets the connection, made or remade as needed.
type Job = Box<dyn FnOnce(&mut PostgresConnection) + Send>;

/// The run history's PostgreSQL connection, on its thread.
pub struct Postgres {
    jobs: mpsc::Sender<Job>,
}

pub struct PostgresConnection {
    runtime: tokio::runtime::Runtime,
    url: String,
    schema: String,
    client: Option<tokio_postgres::Client>,
    statements: HashMap<String, tokio_postgres::Statement>,
}

impl Postgres {
    /// Connects now, so a wrong URL or password stops the service at start. The runtime
    /// and the connection live on the thread: blocking on them from a thread that drives
    /// another runtime would panic.
    pub fn connect(url: &str, schema: &str) -> Result<Postgres> {
        let (url, schema) = (url.to_string(), schema.to_string());
        let (jobs, receiver) = mpsc::channel::<Job>();
        let (ready, connected) = mpsc::sync_channel::<Result<()>>(1);
        std::thread::Builder::new()
            .name("ledger-postgres".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(LedgerError::Postgres(error.to_string())));
                        return;
                    }
                };
                let mut connection = PostgresConnection {
                    runtime,
                    url,
                    schema,
                    client: None,
                    statements: HashMap::new(),
                };
                let first = connection.connected();
                let failed = first.is_err();
                let _ = ready.send(first);
                if failed {
                    return;
                }
                while let Ok(job) = receiver.recv() {
                    job(&mut connection);
                }
            })
            .map_err(|e| LedgerError::Postgres(e.to_string()))?;
        connected
            .recv()
            .map_err(|_| LedgerError::Postgres("the connection's thread stopped".into()))??;
        Ok(Postgres { jobs })
    }

    /// Runs `work` on the connection's thread and waits for it.
    pub fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut PostgresConnection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (reply, result) = mpsc::sync_channel(1);
        self.jobs
            .send(Box::new(move |connection| {
                let _ = reply.send(work(connection));
            }))
            .map_err(|_| LedgerError::Postgres("the connection's thread stopped".into()))?;
        result
            .recv()
            .map_err(|_| LedgerError::Postgres("the connection's thread stopped".into()))?
    }
}

impl PostgresConnection {
    /// Connects when there is no connection or it closed.
    fn connected(&mut self) -> Result<()> {
        if self.client.as_ref().is_none_or(|c| c.is_closed()) {
            self.statements.clear();
            let client = self
                .runtime
                .block_on(mage_connector_postgres::connect_url(
                    &self.url,
                    "mage-service",
                ))
                .map_err(LedgerError::Postgres)?;
            // The schema may not exist yet: migrating makes it.
            let schema = quote_identifier(&self.schema);
            self.runtime
                .block_on(client.batch_execute(&format!("SET search_path TO {schema}")))?;
            self.client = Some(client);
        }
        Ok(())
    }

    fn statement(&mut self, sql: &str) -> Result<tokio_postgres::Statement> {
        if let Some(statement) = self.statements.get(sql) {
            return Ok(statement.clone());
        }
        self.connected()?;
        let client = self.client.as_ref().expect("connected");
        let statement = self.runtime.block_on(client.prepare(sql))?;
        self.statements.insert(sql.to_string(), statement.clone());
        Ok(statement)
    }

    /// Runs `work` in a transaction, committed when it returns Ok.
    pub fn transaction<T>(
        &mut self,
        work: impl FnOnce(&mut PostgresTransaction) -> Result<T>,
    ) -> Result<T> {
        self.connected()?;
        self.runtime.block_on(
            self.client
                .as_ref()
                .expect("connected")
                .batch_execute("BEGIN"),
        )?;
        let result = work(&mut PostgresTransaction { connection: self });
        let end = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
        let ended = match self.client.as_ref() {
            Some(client) => self.runtime.block_on(client.batch_execute(end)),
            None => Ok(()),
        };
        let value = result?;
        ended?;
        Ok(value)
    }

    fn rows(&mut self, sql: &str, params: &[Cell]) -> Result<Vec<Row>> {
        let sql = rewrite(sql, Dialect::Postgres);
        let statement = self.statement(&sql)?;
        let values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|c| c as _).collect();
        let client = self.client.as_ref().expect("connected");
        let rows = self
            .runtime
            .block_on(client.query(&statement, &values))
            .inspect_err(|_| self.statements.clear())?;
        rows.iter().map(postgres_row).collect()
    }

    fn changed(&mut self, sql: &str, params: &[Cell]) -> Result<u64> {
        let sql = rewrite(sql, Dialect::Postgres);
        let statement = self.statement(&sql)?;
        let values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|c| c as _).collect();
        let client = self.client.as_ref().expect("connected");
        Ok(self
            .runtime
            .block_on(client.execute(&statement, &values))
            .inspect_err(|_| self.statements.clear())?)
    }

    fn run_batch(&mut self, sql: &str) -> Result<()> {
        self.connected()?;
        let client = self.client.as_ref().expect("connected");
        Ok(self.runtime.block_on(client.batch_execute(sql))?)
    }

    /// The key is the schema's, so services in other schemas of the database do not wait.
    fn advisory_lock(&mut self, key: &str) -> Result<()> {
        let key = format!("mage-service:{}:{key}", self.schema);
        self.rows(
            "SELECT pg_advisory_xact_lock(hashtext(CAST(?1 AS TEXT)))",
            cells![key],
        )
        .map(|_| ())
    }
}

impl Db for PostgresConnection {
    fn execute(&mut self, sql: &str, params: &[Cell]) -> Result<u64> {
        self.changed(sql, params)
    }

    fn query(&mut self, sql: &str, params: &[Cell]) -> Result<Vec<Row>> {
        self.rows(sql, params)
    }

    fn batch(&mut self, sql: &str) -> Result<()> {
        self.run_batch(sql)
    }

    /// Outside a transaction the lock ends with the statement: callers lock in one.
    fn lock(&mut self, key: &str) -> Result<()> {
        self.advisory_lock(key)
    }
}

pub struct PostgresTransaction<'a> {
    connection: &'a mut PostgresConnection,
}

impl Db for PostgresTransaction<'_> {
    fn execute(&mut self, sql: &str, params: &[Cell]) -> Result<u64> {
        self.connection.changed(sql, params)
    }

    fn query(&mut self, sql: &str, params: &[Cell]) -> Result<Vec<Row>> {
        self.connection.rows(sql, params)
    }

    fn batch(&mut self, sql: &str) -> Result<()> {
        self.connection.run_batch(sql)
    }

    fn lock(&mut self, key: &str) -> Result<()> {
        self.connection.advisory_lock(key)
    }
}

/// A schema name as PostgreSQL reads it, quoted.
pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_statement_is_rewritten_for_each_dialect() {
        let sql = "UPDATE leases SET expires_at = db_time(?3) WHERE owner = ?1 \
                   AND expires_at < db_now() AND n = ?12";
        assert_eq!(
            rewrite(sql, Dialect::Sqlite),
            "UPDATE leases SET expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?3) \
             WHERE owner = ?1 AND expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now') AND n = ?12"
        );
        assert_eq!(
            rewrite(sql, Dialect::Postgres),
            "UPDATE leases SET expires_at = to_char((clock_timestamp() + \
             CAST(CAST($3 AS TEXT) AS INTERVAL)) AT TIME ZONE 'UTC', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') WHERE owner = $1 AND expires_at < \
             to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') \
             AND n = $12"
        );
    }

    #[test]
    fn a_row_reads_its_cells() {
        let row = Row(vec![Cell::Text("a".into()), Cell::Int(3), Cell::Null]);
        assert_eq!(row.text(0).unwrap(), "a");
        assert_eq!(row.int(1).unwrap(), 3);
        assert_eq!(row.opt_text(2).unwrap(), None);
        assert!(row.text(2).is_err());
        assert!(row.int(5).is_err());
    }
}
