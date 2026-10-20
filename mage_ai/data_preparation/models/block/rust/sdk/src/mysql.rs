//! MySQL and MariaDB for Rust blocks (feature `mysql`).
//!
//! ```ignore
//! use mage::mysql::{MySql, WritePolicy};
//! use mage::prelude::*;
//!
//! fn load_data() -> Result<DataFrame> {
//!     MySql::connect()?.query("SELECT * FROM orders WHERE created_at >= CURDATE()")
//! }
//!
//! fn export_data(totals: DataFrame) -> Result<()> {
//!     MySql::connect()?.write(&totals, "daily_totals", WritePolicy::Replace)?;
//!     Ok(())
//! }
//! ```
//!
//! Reading: rows come through the binary protocol (the text protocol for statements MySQL
//! cannot prepare) into typed columns. Signed and unsigned integers keep their width,
//! DECIMAL is exact (`Decimal(p, s)` up to 38 digits, text past that), DATETIME is a naive
//! datetime and TIMESTAMP one in UTC (the session runs in UTC), TIME a duration, and binary
//! strings bytes. Zero dates (`0000-00-00`) read as NULL.
//!
//! Writing: tables are created with the column types MageML's Python client uses (BIGINT,
//! DOUBLE, LONGTEXT, DATETIME(6), ...), and rows go in batches of prepared multi-row
//! INSERTs in one transaction, by column name.
use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use mysql::consts::{ColumnFlags, ColumnType};
use mysql::prelude::Queryable;
use mysql::{Column as MyColumn, Conn, OptsBuilder, SslOpts, TxOpts, Value};

use crate::prelude::*;

/// Placeholders MySQL accepts in one prepared statement.
const MAX_PLACEHOLDERS: usize = 65_535;
const MAX_BATCH_ROWS: usize = 5_000;
/// The character set of binary strings and BLOBs.
const BINARY_CHARSET: u16 = 63;
/// TIME holds -838:59:59 to 838:59:59.
const MAX_TIME_MICROS: i64 = (838 * 3600 + 59 * 60 + 59) * 1_000_000;

/// What `write` does when the table exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    /// Drop the table and create it again with the frame's types.
    Replace,
    /// Insert the rows into the existing table, by column name; create it if missing.
    Append,
    /// Fail if the table exists.
    Fail,
}

pub struct MySql {
    conn: Conn,
}

/// A MySQL identifier.
pub fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn full_name(table: &str) -> String {
    match table.split_once('.') {
        Some((database, name)) => format!("{}.{}", quote(database), quote(name)),
        None => quote(table),
    }
}

/// How a result column is read.
#[derive(Debug, Clone, PartialEq)]
enum Kind {
    Int(DataType),
    UInt(DataType),
    Float32,
    Float64,
    Decimal(usize, usize),
    DecimalText,
    Text,
    Binary,
    Date,
    DateTime,
    Timestamp,
    Time,
    Bits,
}

fn kind(column: &MyColumn) -> Kind {
    let unsigned = column.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    let integer = |signed: DataType, without_sign: DataType| {
        if unsigned {
            Kind::UInt(without_sign)
        } else {
            Kind::Int(signed)
        }
    };
    match column.column_type() {
        ColumnType::MYSQL_TYPE_TINY => integer(DataType::Int8, DataType::UInt8),
        ColumnType::MYSQL_TYPE_SHORT => integer(DataType::Int16, DataType::UInt16),
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
            integer(DataType::Int32, DataType::UInt32)
        }
        ColumnType::MYSQL_TYPE_LONGLONG => integer(DataType::Int64, DataType::UInt64),
        ColumnType::MYSQL_TYPE_YEAR => Kind::Int(DataType::Int16),
        ColumnType::MYSQL_TYPE_FLOAT => Kind::Float32,
        ColumnType::MYSQL_TYPE_DOUBLE => Kind::Float64,
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let scale = column.decimals() as usize;
            // The display length counts the sign and the decimal point.
            let precision = (column.column_length() as usize)
                .saturating_sub(usize::from(scale > 0))
                .saturating_sub(usize::from(!unsigned))
                .max(1);
            if precision <= 38 {
                Kind::Decimal(precision, scale)
            } else {
                Kind::DecimalText
            }
        }
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => Kind::Date,
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => Kind::DateTime,
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => Kind::Timestamp,
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => Kind::Time,
        ColumnType::MYSQL_TYPE_BIT => Kind::Bits,
        ColumnType::MYSQL_TYPE_GEOMETRY => Kind::Binary,
        ColumnType::MYSQL_TYPE_JSON => Kind::Text,
        _ if column.character_set() == BINARY_CHARSET => Kind::Binary,
        _ => Kind::Text,
    }
}

fn text_of(value: &Value) -> Option<String> {
    match value {
        Value::NULL => None,
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        Value::Int(v) => Some(v.to_string()),
        Value::UInt(v) => Some(v.to_string()),
        Value::Float(v) => Some(v.to_string()),
        Value::Double(v) => Some(v.to_string()),
        other => Some(other.as_sql(true).trim_matches('\'').to_string()),
    }
}

fn parse<T: std::str::FromStr>(value: &Value, column: &str) -> Result<Option<T>> {
    match text_of(value) {
        None => Ok(None),
        Some(text) => text
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| anyhow!("Column {column}: could not read {text:?}")),
    }
}

fn days(date: NaiveDate) -> i32 {
    date.num_days_from_ce() - 719_163
}

fn date_of(value: &Value, column: &str) -> Result<Option<NaiveDateTime>> {
    match value {
        Value::NULL => Ok(None),
        Value::Date(0, 0, 0, ..) => Ok(None),
        Value::Date(y, m, d, h, i, s, us) => {
            let date = NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
                .ok_or_else(|| anyhow!("Column {column}: invalid date {y}-{m}-{d}"))?;
            let time = NaiveTime::from_hms_micro_opt(*h as u32, *i as u32, *s as u32, *us)
                .ok_or_else(|| anyhow!("Column {column}: invalid time"))?;
            Ok(Some(date.and_time(time)))
        }
        other => {
            let text = text_of(other).unwrap_or_default();
            if text.starts_with("0000-00-00") {
                return Ok(None);
            }
            if let Ok(at) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f") {
                return Ok(Some(at));
            }
            NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                .map(|d| Some(d.and_hms_opt(0, 0, 0).unwrap()))
                .map_err(|_| anyhow!("Column {column}: could not read the date {text:?}"))
        }
    }
}

fn micros_of_time(value: &Value, column: &str) -> Result<Option<i64>> {
    match value {
        Value::NULL => Ok(None),
        Value::Time(negative, d, h, i, s, us) => {
            let micros = ((*d as i64 * 24 + *h as i64) * 3600 + *i as i64 * 60 + *s as i64)
                * 1_000_000
                + *us as i64;
            Ok(Some(if *negative { -micros } else { micros }))
        }
        other => {
            let text = text_of(other).unwrap_or_default();
            let (negative, rest) = match text.strip_prefix('-') {
                Some(rest) => (true, rest),
                None => (false, text.as_str()),
            };
            let (clock, fraction) = rest.split_once('.').unwrap_or((rest, ""));
            let parts: Vec<i64> = clock
                .split(':')
                .map(|p| p.parse::<i64>())
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| anyhow!("Column {column}: could not read the time {text:?}"))?;
            let [h, m, s] = parts[..] else {
                bail!("Column {column}: could not read the time {text:?}");
            };
            let fraction = format!("{fraction:0<6}");
            let us: i64 = fraction[..6].parse().unwrap_or(0);
            let micros = (h * 3600 + m * 60 + s) * 1_000_000 + us;
            Ok(Some(if negative { -micros } else { micros }))
        }
    }
}

fn bits_of(value: &Value) -> Option<u64> {
    match value {
        Value::NULL => None,
        Value::Bytes(bytes) => Some(bytes.iter().fold(0u64, |acc, b| (acc << 8) | *b as u64)),
        Value::UInt(v) => Some(*v),
        Value::Int(v) => Some(*v as u64),
        _ => None,
    }
}

/// One column's values as they are read.
fn column_of(name: &str, kind: Kind, values: &[Value]) -> Result<Column> {
    let name = PlSmallStr::from(name);
    let series = match kind {
        Kind::Int(dtype) => {
            let values: Vec<Option<i64>> = values
                .iter()
                .map(|v| parse(v, &name))
                .collect::<Result<_>>()?;
            Series::new(name, values).cast(&dtype)?
        }
        Kind::UInt(dtype) => {
            let values: Vec<Option<u64>> = values
                .iter()
                .map(|v| parse(v, &name))
                .collect::<Result<_>>()?;
            Series::new(name, values).cast(&dtype)?
        }
        Kind::Float32 => {
            let values: Vec<Option<f32>> = values
                .iter()
                .map(|v| match v {
                    Value::Float(f) => Ok(Some(*f)),
                    other => parse(other, &name),
                })
                .collect::<Result<_>>()?;
            Series::new(name, values)
        }
        Kind::Float64 => {
            let values: Vec<Option<f64>> = values
                .iter()
                .map(|v| match v {
                    Value::Double(f) => Ok(Some(*f)),
                    Value::Float(f) => Ok(Some(*f as f64)),
                    other => parse(other, &name),
                })
                .collect::<Result<_>>()?;
            Series::new(name, values)
        }
        Kind::Decimal(precision, scale) => {
            let values: Vec<Option<String>> = values.iter().map(text_of).collect();
            Series::new(name, values).strict_cast(&DataType::Decimal(precision, scale))?
        }
        Kind::DecimalText | Kind::Text => {
            let values: Vec<Option<String>> = values.iter().map(text_of).collect();
            Series::new(name, values)
        }
        Kind::Binary => {
            let values: Vec<Option<Vec<u8>>> = values
                .iter()
                .map(|v| match v {
                    Value::NULL => None,
                    Value::Bytes(bytes) => Some(bytes.clone()),
                    other => text_of(other).map(String::into_bytes),
                })
                .collect();
            Series::new(name, values)
        }
        Kind::Date => {
            let values: Vec<Option<i32>> = values
                .iter()
                .map(|v| Ok(date_of(v, &name)?.map(|at| days(at.date()))))
                .collect::<Result<_>>()?;
            Series::new(name, values).cast(&DataType::Date)?
        }
        Kind::DateTime | Kind::Timestamp => {
            let values: Vec<Option<i64>> = values
                .iter()
                .map(|v| Ok(date_of(v, &name)?.map(|at| at.and_utc().timestamp_micros())))
                .collect::<Result<_>>()?;
            let zone = (kind == Kind::Timestamp).then_some(TimeZone::UTC);
            Series::new(name, values).cast(&DataType::Datetime(TimeUnit::Microseconds, zone))?
        }
        Kind::Time => {
            let values: Vec<Option<i64>> = values
                .iter()
                .map(|v| micros_of_time(v, &name))
                .collect::<Result<_>>()?;
            Series::new(name, values).cast(&DataType::Duration(TimeUnit::Microseconds))?
        }
        Kind::Bits => {
            let values: Vec<Option<u64>> = values.iter().map(bits_of).collect();
            Series::new(name, values)
        }
    };
    Ok(series.into_column())
}

/// The MySQL type of a column that `write` creates, as MageML's Python client chooses it.
fn column_type(name: &str, series: &Series) -> Result<String> {
    Ok(match series.dtype() {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => "BIGINT".into(),
        DataType::UInt64 => "BIGINT UNSIGNED".into(),
        DataType::Int128 => "DECIMAL(39, 0)".into(),
        DataType::Float32 | DataType::Float64 => "DOUBLE".into(),
        DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) | DataType::Null => {
            "LONGTEXT".into()
        }
        DataType::Binary => "LONGBLOB".into(),
        DataType::Date => "DATE".into(),
        DataType::Datetime(_, _) => "DATETIME(6)".into(),
        DataType::Time => "TIME(6)".into(),
        DataType::Duration(_) => {
            let micros = series.cast(&DataType::Duration(TimeUnit::Microseconds))?;
            let micros = micros.cast(&DataType::Int64)?;
            let fits = micros
                .i64()?
                .iter()
                .flatten()
                .all(|v| v.abs() <= MAX_TIME_MICROS);
            // Longer durations are integer nanoseconds, as in the Python client.
            if fits { "TIME(6)" } else { "BIGINT" }.into()
        }
        DataType::Decimal(precision, scale) => format!("DECIMAL({precision}, {scale})"),
        other => bail!(
            "Column {name} is {other}, which MySql::write does not create a column for: \
             cast it to a supported type or create the table first"
        ),
    })
}

/// The values of a column as MySQL parameters.
fn parameters(series: &Series, column_type: Option<&str>) -> Result<Vec<Value>> {
    let name = series.name().to_string();
    let bytes = |v: Option<&[u8]>| v.map_or(Value::NULL, |b| Value::Bytes(b.to_vec()));
    Ok(match series.dtype() {
        DataType::Boolean => series
            .bool()?
            .iter()
            .map(|v| v.map_or(Value::NULL, |b| Value::Int(b as i64)))
            .collect(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let cast = series.cast(&DataType::Int64)?;
            cast.i64()?
                .iter()
                .map(|v| v.map_or(Value::NULL, Value::Int))
                .collect()
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let cast = series.cast(&DataType::UInt64)?;
            cast.u64()?
                .iter()
                .map(|v| v.map_or(Value::NULL, Value::UInt))
                .collect()
        }
        DataType::Float32 | DataType::Float64 => {
            let cast = series.cast(&DataType::Float64)?;
            cast.f64()?
                .iter()
                .map(|v| v.map_or(Value::NULL, Value::Double))
                .collect()
        }
        DataType::Binary => series.binary()?.iter().map(bytes).collect(),
        DataType::Date => {
            let cast = series.cast(&DataType::Int32)?;
            cast.i32()?
                .iter()
                .map(|v| {
                    v.map_or(Value::NULL, |d| {
                        let date = NaiveDate::from_num_days_from_ce_opt(d + 719_163).unwrap();
                        Value::Date(
                            date.year() as u16,
                            date.month() as u8,
                            date.day() as u8,
                            0,
                            0,
                            0,
                            0,
                        )
                    })
                })
                .collect()
        }
        DataType::Datetime(_, _) => {
            // Zoned values are written in UTC, the session's time zone.
            let micros = series
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?
                .cast(&DataType::Int64)?;
            micros
                .i64()?
                .iter()
                .map(|v| {
                    v.map_or(Ok(Value::NULL), |us| {
                        let at = DateTime::from_timestamp_micros(us)
                            .ok_or_else(|| anyhow!("Column {name}: datetime out of range"))?;
                        let naive = at.naive_utc();
                        Ok(Value::Date(
                            naive.year() as u16,
                            naive.month() as u8,
                            naive.day() as u8,
                            naive.hour() as u8,
                            naive.minute() as u8,
                            naive.second() as u8,
                            at.timestamp_subsec_micros(),
                        ))
                    })
                })
                .collect::<Result<_>>()?
        }
        DataType::Duration(_) | DataType::Time => {
            // Time is nanoseconds since midnight; durations are read in microseconds.
            let micros: Vec<Option<i64>> = if matches!(series.dtype(), DataType::Time) {
                let cast = series.cast(&DataType::Int64)?;
                cast.i64()?.iter().map(|v| v.map(|ns| ns / 1_000)).collect()
            } else {
                let cast = series
                    .cast(&DataType::Duration(TimeUnit::Microseconds))?
                    .cast(&DataType::Int64)?;
                cast.i64()?.iter().collect()
            };
            let as_integer =
                column_type.is_some_and(|t| !t.to_ascii_uppercase().starts_with("TIME"));
            micros
                .into_iter()
                .map(|v| {
                    v.map_or(Value::NULL, |us| {
                        if as_integer {
                            // Integer nanoseconds, as the Python client writes them.
                            return Value::Int(us * 1_000);
                        }
                        let negative = us < 0;
                        let us = us.unsigned_abs();
                        let seconds = us / 1_000_000;
                        Value::Time(
                            negative,
                            (seconds / 86_400) as u32,
                            ((seconds / 3600) % 24) as u8,
                            ((seconds / 60) % 60) as u8,
                            (seconds % 60) as u8,
                            (us % 1_000_000) as u32,
                        )
                    })
                })
                .collect()
        }
        DataType::String
        | DataType::Categorical(_, _)
        | DataType::Enum(_, _)
        | DataType::Decimal(_, _)
        | DataType::Int128
        | DataType::Null => {
            let text = series.cast(&DataType::String)?;
            text.str()?
                .iter()
                .map(|v| bytes(v.map(str::as_bytes)))
                .collect()
        }
        other => bail!("Column {name} is {other}, which MySql::write cannot write"),
    })
}

impl MySql {
    /// Connects with the block's io_config profile.
    pub fn connect() -> Result<Self> {
        Self::connect_settings(&crate::profile::settings()?)
    }

    /// Connects with MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER and MYSQL_PASSWORD,
    /// as MageML's Python client does. TLS is off unless MYSQL_SSL_MODE asks for it:
    /// `REQUIRED` encrypts without checking the certificate, `VERIFY_CA` checks that it
    /// chains to a trusted root (Mozilla's, or the PEM file in MYSQL_SSL_CA), and
    /// `VERIFY_IDENTITY` also checks that it names the host. SSH tunnels
    /// (MYSQL_CONNECTION_METHOD `ssh_tunnel`) are not supported here.
    pub fn connect_settings(settings: &HashMap<String, String>) -> Result<Self> {
        let get = |key: &str| {
            settings
                .get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        if get("MYSQL_CONNECTION_METHOD").is_some_and(|m| m == "ssh_tunnel") {
            bail!("MySql::connect does not open SSH tunnels; connect directly to the server");
        }
        let host = get("MYSQL_HOST").unwrap_or_else(|| "127.0.0.1".into());
        let port: u16 = match get("MYSQL_PORT") {
            Some(port) => port.parse().context("MYSQL_PORT")?,
            None => 3306,
        };
        let ssl = match get("MYSQL_SSL_MODE")
            .map(|m| m.to_ascii_uppercase())
            .as_deref()
        {
            None | Some("DISABLED") | Some("PREFERRED") => None,
            Some("REQUIRED") => Some(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ),
            Some(mode @ ("VERIFY_CA" | "VERIFY_IDENTITY")) => {
                let mut options =
                    SslOpts::default().with_danger_skip_domain_validation(mode == "VERIFY_CA");
                if let Some(path) = get("MYSQL_SSL_CA") {
                    options = options.with_root_cert_path(Some(std::path::PathBuf::from(path)));
                }
                Some(options)
            }
            Some(other) => bail!(
                "MYSQL_SSL_MODE {other} is not one of DISABLED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY"
            ),
        };
        let options = OptsBuilder::new()
            .ip_or_hostname(Some(host))
            .tcp_port(port)
            .user(get("MYSQL_USER"))
            .pass(get("MYSQL_PASSWORD"))
            .db_name(get("MYSQL_DATABASE"))
            .prefer_socket(false)
            .ssl_opts(ssl)
            // TIMESTAMP values are read and written in UTC.
            .init(vec!["SET time_zone = '+00:00'"]);
        let conn = Conn::new(options).context("Could not connect to MySQL")?;
        Ok(Self { conn })
    }

    /// The underlying `mysql::Conn`.
    pub fn conn(&mut self) -> &mut Conn {
        &mut self.conn
    }

    /// Runs a statement and returns the rows it changed.
    pub fn execute(&mut self, sql: &str) -> Result<u64> {
        self.conn
            .query_drop(sql)
            .map_err(|error| anyhow!("MySQL could not run {sql}: {error}"))?;
        Ok(self.conn.affected_rows())
    }

    /// The rows of a query, in typed columns.
    pub fn query(&mut self, sql: &str) -> Result<DataFrame> {
        let sql = sql.trim().trim_end_matches(';');
        let unsupported = match self.conn.exec_iter(sql, ()) {
            Ok(result) => return collect(result),
            // ER_UNSUPPORTED_PS: statements that cannot be prepared use the text protocol.
            Err(mysql::Error::MySqlError(error)) if error.code == 1295 => true,
            Err(error) => return Err(anyhow!("MySQL query failed: {error}")),
        };
        debug_assert!(unsupported);
        collect(
            self.conn
                .query_iter(sql)
                .map_err(|error| anyhow!("MySQL query failed: {error}"))?,
        )
    }

    /// Whether the table (`table` or `database.table`) exists.
    pub fn table_exists(&mut self, table: &str) -> Result<bool> {
        let (database, name) = match table.split_once('.') {
            Some((database, name)) => (Some(database), name),
            None => (None, table),
        };
        let found: Option<u8> = self.conn.exec_first(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? \
             AND table_schema = COALESCE(?, DATABASE())",
            (name, database),
        )?;
        Ok(found.is_some())
    }

    fn column_types(&mut self, table: &str) -> Result<HashMap<String, String>> {
        let (database, name) = match table.split_once('.') {
            Some((database, name)) => (Some(database), name),
            None => (None, table),
        };
        let rows: Vec<(String, String)> = self.conn.exec(
            "SELECT column_name, column_type FROM information_schema.columns \
             WHERE table_name = ? AND table_schema = COALESCE(?, DATABASE())",
            (name, database),
        )?;
        Ok(rows.into_iter().collect())
    }

    /// Writes the frame to the table and returns the rows written, in one transaction for
    /// the rows (MySQL commits CREATE and DROP on their own).
    pub fn write(&mut self, frame: &DataFrame, table: &str, policy: WritePolicy) -> Result<u64> {
        let name = full_name(table);
        let exists = self.table_exists(table)?;
        match (exists, policy) {
            (true, WritePolicy::Fail) => bail!("Table {table} already exists"),
            (true, WritePolicy::Replace) => {
                self.execute(&format!("DROP TABLE {name}"))?;
                self.create(frame, &name)?;
            }
            (false, _) => self.create(frame, &name)?,
            (true, WritePolicy::Append) => {}
        }
        if frame.height() == 0 || frame.width() == 0 {
            return Ok(0);
        }
        let types = self.column_types(table)?;
        let columns: Vec<Vec<Value>> = frame
            .columns()
            .iter()
            .map(|c| {
                let series = c.as_materialized_series();
                parameters(
                    series,
                    types.get(series.name().as_str()).map(String::as_str),
                )
            })
            .collect::<Result<_>>()?;
        let names: Vec<String> = frame
            .get_column_names()
            .iter()
            .map(|c| quote(c.as_str()))
            .collect();
        let width = names.len();
        let batch = (MAX_PLACEHOLDERS / width).clamp(1, MAX_BATCH_ROWS);
        let row = format!("({})", vec!["?"; width].join(", "));
        let mut tx = self.conn.start_transaction(TxOpts::default())?;
        let mut start = 0;
        while start < frame.height() {
            let end = (start + batch).min(frame.height());
            let statement = format!(
                "INSERT INTO {name} ({}) VALUES {}",
                names.join(", "),
                vec![row.as_str(); end - start].join(", ")
            );
            let mut values = Vec::with_capacity((end - start) * width);
            for index in start..end {
                for column in &columns {
                    values.push(column[index].clone());
                }
            }
            tx.exec_drop(&statement, values).map_err(|error| {
                anyhow!("MySQL could not insert rows {start} to {end} into {table}: {error}")
            })?;
            start = end;
        }
        tx.commit()?;
        Ok(frame.height() as u64)
    }

    fn create(&mut self, frame: &DataFrame, name: &str) -> Result<()> {
        let fields: Vec<String> = frame
            .columns()
            .iter()
            .map(|c| {
                let series = c.as_materialized_series();
                Ok(format!(
                    "{} {}",
                    quote(series.name().as_str()),
                    column_type(series.name().as_str(), series)?
                ))
            })
            .collect::<Result<_>>()?;
        self.execute(&format!("CREATE TABLE {name} ({})", fields.join(", ")))?;
        Ok(())
    }
}

fn collect<P: mysql::prelude::Protocol>(
    mut result: mysql::QueryResult<'_, '_, '_, P>,
) -> Result<DataFrame> {
    let mut frame = DataFrame::empty();
    // The last result set: statements before it (SET, ...) have none.
    while let Some(set) = result.iter() {
        let columns: Vec<MyColumn> = set.columns().as_ref().to_vec();
        let mut values: Vec<Vec<Value>> = vec![Vec::new(); columns.len()];
        for row in set {
            let row = row?;
            for (index, value) in row.unwrap().into_iter().enumerate() {
                values[index].push(value);
            }
        }
        if columns.is_empty() {
            continue;
        }
        let built: Vec<Column> = columns
            .iter()
            .zip(values.iter())
            .map(|(column, values)| column_of(&column.name_str(), kind(column), values))
            .collect::<Result<_>>()?;
        frame = DataFrame::new_infer_height(built)?;
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote("a`b"), "`a``b`");
        assert_eq!(full_name("db.t"), "`db`.`t`");
    }

    #[test]
    fn text_values_parse_as_their_column_kind() -> Result<()> {
        let bytes = |s: &str| Value::Bytes(s.as_bytes().to_vec());
        let ints = column_of("i", Kind::Int(DataType::Int16), &[bytes("-5"), Value::NULL])?;
        assert_eq!(ints.dtype(), &DataType::Int16);
        let dates = column_of("d", Kind::Date, &[bytes("2024-02-29"), bytes("0000-00-00")])?;
        assert_eq!(dates.null_count(), 1);
        assert_eq!(
            dates
                .as_materialized_series()
                .cast(&DataType::Int32)?
                .i32()?
                .get(0),
            Some(19782)
        );
        let times = column_of(
            "t",
            Kind::Time,
            &[bytes("-838:59:59.5"), Value::Time(false, 1, 2, 3, 4, 5)],
        )?;
        let micros = times.as_materialized_series().cast(&DataType::Int64)?;
        assert_eq!(micros.i64()?.get(0), Some(-(MAX_TIME_MICROS + 500_000)));
        assert_eq!(
            micros.i64()?.get(1),
            Some(((26 * 3600 + 3 * 60 + 4) * 1_000_000) + 5)
        );
        let exact = column_of("x", Kind::Decimal(10, 2), &[bytes("12345678.90")])?;
        assert_eq!(exact.dtype(), &DataType::Decimal(10, 2));
        let bits = column_of("b", Kind::Bits, &[Value::Bytes(vec![1, 0])])?;
        assert_eq!(bits.as_materialized_series().u64()?.get(0), Some(256));
        Ok(())
    }

    #[test]
    fn created_columns_follow_the_python_client() -> Result<()> {
        let s = |values: Series| column_type("c", &values);
        assert_eq!(s(Series::new("c".into(), [1i32]))?, "BIGINT");
        assert_eq!(s(Series::new("c".into(), [1u64]))?, "BIGINT UNSIGNED");
        assert_eq!(s(Series::new("c".into(), [1.5f32]))?, "DOUBLE");
        assert_eq!(s(Series::new("c".into(), ["x"]))?, "LONGTEXT");
        let long = Series::new("c".into(), [MAX_TIME_MICROS + 1])
            .cast(&DataType::Duration(TimeUnit::Microseconds))?;
        assert_eq!(s(long)?, "BIGINT");
        Ok(())
    }

    /// Against the integration tests' MySQL (MAGE_TEST_MYSQL_*); skipped without it.
    fn live() -> Option<MySql> {
        let host = std::env::var("MAGE_TEST_MYSQL_HOST").ok()?;
        let env = |key: &str, default: &str| std::env::var(key).unwrap_or_else(|_| default.into());
        let settings: HashMap<String, String> = [
            ("MYSQL_HOST", host),
            ("MYSQL_PORT", env("MAGE_TEST_MYSQL_PORT", "13306")),
            ("MYSQL_DATABASE", env("MAGE_TEST_MYSQL_DATABASE", "test")),
            ("MYSQL_USER", env("MAGE_TEST_MYSQL_USER", "root")),
            ("MYSQL_PASSWORD", env("MAGE_TEST_MYSQL_PASSWORD", "test")),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        Some(MySql::connect_settings(&settings).unwrap())
    }

    #[test]
    fn live_every_type_reads_exactly() -> Result<()> {
        let Some(mut client) = live() else {
            return Ok(());
        };
        let table = format!("rust_types_{}", std::process::id());
        client.execute(&format!("DROP TABLE IF EXISTS {table}"))?;
        client.execute(&format!(
            "CREATE TABLE {table} (t TINYINT, ut TINYINT UNSIGNED, b BIGINT, \
             ub BIGINT UNSIGNED, f FLOAT, d DOUBLE, n DECIMAL(12, 3), wide DECIMAL(65, 2), \
             s VARCHAR(20), bin VARBINARY(4), day DATE, at DATETIME(6), \
             ts TIMESTAMP(6) NULL, tm TIME(6), y YEAR, bits BIT(12), j JSON, \
             e ENUM('on', 'off'))"
        ))?;
        client.execute(&format!(
            "INSERT INTO {table} VALUES (-128, 255, -9223372036854775808, \
             18446744073709551615, 1.5, 2.25, -123456789.125, \
             123456789012345678901234567890123456789012345678901234567890123.45, 'é', \
             0x00ff, '2024-02-29', '2024-01-02 03:04:05.123456', \
             '2024-01-02 03:04:05.654321', '-838:59:59', 2024, b'100000000001', \
             '{{\"a\": 1}}', 'off'), \
             (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
             NULL, NULL, NULL, NULL, NULL, NULL)"
        ))?;
        let frame = client.query(&format!("SELECT * FROM {table}"))?;
        let dtype = |name: &str| frame.column(name).unwrap().dtype().clone();
        assert_eq!(dtype("t"), DataType::Int8);
        assert_eq!(dtype("ut"), DataType::UInt8);
        assert_eq!(dtype("ub"), DataType::UInt64);
        assert_eq!(dtype("f"), DataType::Float32);
        assert_eq!(dtype("n"), DataType::Decimal(12, 3));
        assert_eq!(dtype("wide"), DataType::String);
        assert_eq!(dtype("bin"), DataType::Binary);
        assert_eq!(dtype("day"), DataType::Date);
        assert_eq!(
            dtype("at"),
            DataType::Datetime(TimeUnit::Microseconds, None)
        );
        assert_eq!(
            dtype("ts"),
            DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
        );
        assert_eq!(dtype("tm"), DataType::Duration(TimeUnit::Microseconds));
        let first = |name: &str| frame.column(name).unwrap().get(0).unwrap().into_static();
        assert_eq!(first("b"), AnyValue::Int64(i64::MIN));
        assert_eq!(first("ub"), AnyValue::UInt64(u64::MAX));
        assert_eq!(first("s"), AnyValue::StringOwned("é".into()));
        assert_eq!(first("bits"), AnyValue::UInt64(2049));
        assert_eq!(
            first("wide"),
            AnyValue::StringOwned(
                "123456789012345678901234567890123456789012345678901234567890123.45".into()
            )
        );
        let micros = frame
            .column("tm")?
            .as_materialized_series()
            .cast(&DataType::Int64)?;
        assert_eq!(micros.i64()?.get(0), Some(-MAX_TIME_MICROS));
        let at = frame
            .column("at")?
            .as_materialized_series()
            .cast(&DataType::Int64)?;
        assert_eq!(at.i64()?.get(0), Some(1_704_164_645_123_456));
        assert_eq!(frame.column("e")?.str()?.get(0), Some("off"));
        assert_eq!(frame.height(), 2);
        assert!(frame.columns().iter().all(|c| c.null_count() == 1));
        client.execute(&format!("DROP TABLE {table}"))?;
        Ok(())
    }

    #[test]
    fn live_frames_round_trip_and_append_by_name() -> Result<()> {
        let Some(mut client) = live() else {
            return Ok(());
        };
        let table = format!("rust_round_trip_{}", std::process::id());
        client.execute(&format!("DROP TABLE IF EXISTS {table}"))?;
        let frame = df!(
            "id" => [1i64, 2, 3],
            "name" => [Some("a"), None, Some("é")],
            "amount" => [Some(1.5f64), Some(2.25), None],
            "big" => [u64::MAX, 0, 7],
            "flag" => [Some(true), Some(false), None],
            "day" => [Some(19724i32), None, Some(-25567)],
        )?
        .lazy()
        .with_columns([
            col("day").cast(DataType::Date),
            lit(1_704_164_645_123_456i64)
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .alias("at"),
            col("amount").cast(DataType::Decimal(10, 2)).alias("exact"),
            lit(-90_061_000_001i64)
                .cast(DataType::Duration(TimeUnit::Microseconds))
                .alias("took"),
        ])
        .collect()?;
        assert_eq!(client.write(&frame, &table, WritePolicy::Replace)?, 3);
        let back = client.query(&format!("SELECT * FROM {table} ORDER BY id"))?;
        // BOOLEAN is TINYINT(1) in MySQL: it reads as Int8.
        let expected = frame
            .clone()
            .lazy()
            .with_column(col("flag").cast(DataType::Int8))
            .collect()?;
        assert!(back.equals_missing(&expected), "{back}\n{expected}");

        let more = frame.select([
            "big", "id", "name", "amount", "flag", "day", "at", "exact", "took",
        ])?;
        client.write(&more.head(Some(1)), &table, WritePolicy::Append)?;
        let count = client.query(&format!("SELECT COUNT(*) AS n FROM {table}"))?;
        assert_eq!(count.column("n")?.get(0)?, AnyValue::Int64(4));
        let error = client
            .write(&frame, &table, WritePolicy::Fail)
            .unwrap_err()
            .to_string();
        assert!(error.contains("already exists"), "{error}");
        let failed = client
            .query("SELECT * FROM no_such_table")
            .unwrap_err()
            .to_string();
        assert!(failed.contains("no_such_table"), "{failed}");
        // A statement MySQL cannot prepare goes through the text protocol.
        let shown = client.query(&format!("SHOW CREATE TABLE {table}"))?;
        assert_eq!(shown.height(), 1);

        // Many rows go in batches.
        let many = df!("id" => (0..20_000i64).collect::<Vec<_>>())?;
        client.write(&many, &format!("{table}_many"), WritePolicy::Replace)?;
        let count = client.query(&format!(
            "SELECT COUNT(*) AS n, SUM(id) AS s FROM {table}_many"
        ))?;
        assert_eq!(count.column("n")?.get(0)?, AnyValue::Int64(20_000));
        client.execute(&format!("DROP TABLE {table}, {table}_many"))?;
        Ok(())
    }
}
