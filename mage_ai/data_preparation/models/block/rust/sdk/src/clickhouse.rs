//! ClickHouse for Rust blocks (feature `clickhouse`), over its HTTP interface with rows in
//! Arrow: query results arrive as an Arrow stream that Polars reads into columns, and
//! frames are inserted as one, with no conversion row by row.
//!
//! ```ignore
//! use mage::clickhouse::{ClickHouse, WritePolicy};
//! use mage::prelude::*;
//!
//! fn load_data() -> Result<DataFrame> {
//!     ClickHouse::connect()?.query("SELECT * FROM events WHERE day = today()")
//! }
//!
//! fn export_data(totals: DataFrame) -> Result<()> {
//!     ClickHouse::connect()?.write(&totals, "daily_totals", WritePolicy::Replace)?;
//!     Ok(())
//! }
//! ```
//!
//! Reading: ClickHouse sends most types in Arrow as they are. The others are cast in the
//! query first: Date and DateTime to Date32 and DateTime64 (they arrive as plain numbers
//! otherwise), and UUID, IP addresses, enums, 128 and 256-bit integers, decimals of more
//! than 38 digits and nested values holding any of these to their exact text.
//!
//! Writing: tables are created with the frame's types, each Nullable, with the engine
//! MageML's Python client uses (`MergeTree ORDER BY tuple()`); appends insert by column
//! name, and ClickHouse converts each value to its column's type.
use std::collections::HashMap;
use std::io::Cursor;
use std::time::Duration;

use serde_json::Value;

use crate::prelude::*;

/// The engine of the tables `write` creates, as in MageML's Python client.
pub const TABLE_ENGINE: &str = "MergeTree ORDER BY tuple()";

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

pub struct ClickHouse {
    url: String,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    agent: ureq::Agent,
}

/// A ClickHouse identifier, as MageML's Python client quotes it.
pub fn quote(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// The table's name with its database: `db.table` as given, or the profile's database.
fn full_name(table: &str, database: Option<&str>) -> String {
    match (table.split_once('.'), database) {
        (Some((db, name)), _) => format!("{}.{}", quote(db), quote(name)),
        (None, Some(db)) => format!("{}.{}", quote(db), quote(table)),
        (None, None) => quote(table),
    }
}

/// The type inside LowCardinality(...) and Nullable(...).
fn unwrapped(column_type: &str) -> &str {
    let mut inner = column_type.trim();
    loop {
        let stripped = ["LowCardinality(", "Nullable("].iter().find_map(|prefix| {
            inner
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_suffix(')'))
        });
        match stripped {
            Some(rest) => inner = rest.trim(),
            None => return inner,
        }
    }
}

fn decimal_precision(column_type: &str) -> Option<u32> {
    let rest = column_type.strip_prefix("Decimal(")?;
    rest.split(',').next()?.trim().parse().ok()
}

/// Types that Arrow cannot carry as they are, anywhere in a type.
fn has_text_only_type(column_type: &str) -> bool {
    let words: Vec<&str> = column_type
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .collect();
    words.iter().any(|word| {
        matches!(
            *word,
            "UUID"
                | "IPv4"
                | "IPv6"
                | "Enum8"
                | "Enum16"
                | "Int128"
                | "UInt128"
                | "Int256"
                | "UInt256"
                | "Decimal256"
                | "Nothing"
        )
    }) || column_type
        .match_indices("Decimal(")
        .any(|(at, _)| decimal_precision(&column_type[at..]).is_some_and(|p| p > 38))
}

/// The expression that reads a column of this type into Arrow exactly; None when the
/// column is read as it is.
fn read_expression(name: &str, column_type: &str) -> Option<String> {
    let column = quote(name);
    let inner = unwrapped(column_type);
    let nullable = column_type.contains("Nullable(");
    if has_text_only_type(column_type) {
        return Some(if nullable || inner == "Nothing" {
            format!("CAST(toString({column}), 'Nullable(String)') AS {column}")
        } else {
            format!("toString({column}) AS {column}")
        });
    }
    if inner == "Date" {
        return Some(format!("toDate32({column}) AS {column}"));
    }
    if inner == "DateTime" || inner.starts_with("DateTime(") {
        // DateTime64 keeps the column's time zone.
        return Some(format!("toDateTime64({column}, 0) AS {column}"));
    }
    let nested =
        inner.starts_with("Array(") || inner.starts_with("Map(") || inner.starts_with("Tuple(");
    if nested && (inner.contains("Date)") || inner.contains("Date,") || inner.contains("DateTime"))
    {
        return Some(format!("toString({column}) AS {column}"));
    }
    None
}

/// The ClickHouse type of a column that `write` creates, without Nullable.
fn column_type(name: &str, dtype: &DataType) -> Result<String> {
    Ok(match dtype {
        DataType::Boolean => "Bool".into(),
        DataType::Int8 => "Int8".into(),
        DataType::Int16 => "Int16".into(),
        DataType::Int32 => "Int32".into(),
        DataType::Int64 => "Int64".into(),
        DataType::Int128 => "Int128".into(),
        DataType::UInt8 => "UInt8".into(),
        DataType::UInt16 => "UInt16".into(),
        DataType::UInt32 => "UInt32".into(),
        DataType::UInt64 => "UInt64".into(),
        DataType::Float32 => "Float32".into(),
        DataType::Float64 => "Float64".into(),
        DataType::String | DataType::Binary => "String".into(),
        DataType::Categorical(_, _) | DataType::Enum(_, _) => "LowCardinality(String)".into(),
        DataType::Date => "Date32".into(),
        DataType::Datetime(unit, zone) => {
            let precision = match unit {
                TimeUnit::Milliseconds => 3,
                TimeUnit::Microseconds => 6,
                TimeUnit::Nanoseconds => 9,
            };
            match zone {
                Some(zone) => format!("DateTime64({precision}, '{zone}')"),
                None => format!("DateTime64({precision})"),
            }
        }
        // ClickHouse has no duration type: durations are stored in microseconds, as the
        // Python client stores them.
        DataType::Duration(_) => "Int64".into(),
        DataType::Decimal(precision, scale) => {
            format!("Decimal({precision}, {scale})")
        }
        other => bail!(
            "Column {name} is {other}, which ClickHouse::write does not create a column \
             for: cast it to a supported type or create the table first"
        ),
    })
}

fn nullable(name: &str, dtype: &DataType) -> Result<String> {
    let inner = column_type(name, dtype)?;
    Ok(match inner.strip_prefix("LowCardinality(") {
        Some(rest) => format!("LowCardinality(Nullable({rest})"),
        None => format!("Nullable({inner})"),
    })
}

impl ClickHouse {
    /// Connects with the block's io_config profile.
    pub fn connect() -> Result<Self> {
        Self::connect_settings(&crate::profile::settings()?)
    }

    /// Connects with CLICKHOUSE_HOST, CLICKHOUSE_PORT (the HTTP port, 8123 by default),
    /// CLICKHOUSE_INTERFACE (`http` or `https`), CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD
    /// and CLICKHOUSE_DATABASE, as MageML's Python client does.
    pub fn connect_settings(settings: &HashMap<String, String>) -> Result<Self> {
        let get = |key: &str| {
            settings
                .get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let host = get("CLICKHOUSE_HOST")
            .ok_or_else(|| anyhow!("The ClickHouse profile has no CLICKHOUSE_HOST"))?;
        let interface = get("CLICKHOUSE_INTERFACE").unwrap_or_else(|| "http".into());
        let port = get("CLICKHOUSE_PORT")
            .unwrap_or_else(|| if interface == "https" { "8443" } else { "8123" }.to_string());
        let url = if host.contains("://") {
            format!("{}:{port}/", host.trim_end_matches('/'))
        } else {
            format!("{interface}://{host}:{port}/")
        };
        let config = ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(30)))
            .http_status_as_error(false)
            .build();
        Ok(Self {
            url,
            user: get("CLICKHOUSE_USERNAME"),
            password: get("CLICKHOUSE_PASSWORD"),
            database: get("CLICKHOUSE_DATABASE"),
            agent: config.new_agent(),
        })
    }

    fn request(
        &self,
        settings: &[(&str, &str)],
    ) -> ureq::RequestBuilder<ureq::typestate::WithBody> {
        let mut request = self.agent.post(&self.url);
        if let Some(user) = &self.user {
            request = request.header("X-ClickHouse-User", user);
        }
        if let Some(password) = &self.password {
            request = request.header("X-ClickHouse-Key", password);
        }
        if let Some(database) = &self.database {
            request = request.query("database", database);
        }
        for (key, value) in settings {
            request = request.query(*key, *value);
        }
        request
    }

    fn checked(
        response: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
        what: &str,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        let mut response = response.map_err(|e| anyhow!("ClickHouse {what}: {e}"))?;
        if response.status() != 200 {
            let text = response.body_mut().read_to_string().unwrap_or_default();
            bail!(
                "ClickHouse {what} failed ({}): {}",
                response.status(),
                text.trim()
            );
        }
        Ok(response)
    }

    /// Runs a statement that returns no rows.
    pub fn execute(&self, sql: &str) -> Result<()> {
        let response = self.request(&[]).send(sql.trim().trim_end_matches(';'));
        let mut response = Self::checked(response, "statement")?;
        let _ = response.body_mut().read_to_vec();
        Ok(())
    }

    fn text(&self, sql: &str) -> Result<String> {
        let response = self.request(&[]).send(sql);
        Ok(Self::checked(response, "query")?
            .body_mut()
            .read_to_string()?)
    }

    /// The columns of a query's result: names and ClickHouse types.
    pub fn describe(&self, sql: &str) -> Result<Vec<(String, String)>> {
        let sql = sql.trim().trim_end_matches(';');
        let text = self.text(&format!("DESCRIBE ({sql}) FORMAT JSONEachRow"))?;
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let row: Value = serde_json::from_str(line)?;
                Ok((
                    row["name"].as_str().unwrap_or_default().to_string(),
                    row["type"].as_str().unwrap_or_default().to_string(),
                ))
            })
            .collect()
    }

    /// The rows of a query, read as an Arrow stream.
    pub fn query(&self, sql: &str) -> Result<DataFrame> {
        let sql = sql.trim().trim_end_matches(';');
        let columns = self.describe(sql)?;
        let casts: Vec<Option<String>> = columns
            .iter()
            .map(|(name, column_type)| read_expression(name, column_type))
            .collect();
        let statement = if casts.iter().all(Option::is_none) {
            format!("{sql}\nFORMAT ArrowStream")
        } else {
            let list: Vec<String> = columns
                .iter()
                .zip(&casts)
                .map(|((name, _), cast)| cast.clone().unwrap_or_else(|| quote(name)))
                .collect();
            format!(
                "SELECT {} FROM ({sql})\nFORMAT ArrowStream",
                list.join(", ")
            )
        };
        let response = self
            .request(&[("output_format_arrow_string_as_string", "1")])
            .send(statement.as_str());
        let response = Self::checked(response, "query")?;
        // Polars reads an Arrow stream from a seekable source, so the result is read whole.
        let bytes = response
            .into_body()
            .into_with_config()
            .limit(u64::MAX)
            .read_to_vec()
            .context("Could not read the result of the query from ClickHouse")?;
        IpcStreamReader::new(Cursor::new(&bytes))
            .finish()
            .map_err(|error| {
                // An error after the first rows is written into the stream as text.
                let tail = String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(2000)..]);
                match tail.find("Code: ") {
                    Some(at) => anyhow!("ClickHouse query failed: {}", tail[at..].trim()),
                    None => anyhow!("Could not read the Arrow result from ClickHouse: {error}"),
                }
            })
    }

    /// Whether the table exists.
    pub fn table_exists(&self, table: &str) -> Result<bool> {
        let name = full_name(table, self.database.as_deref());
        Ok(self.text(&format!("EXISTS TABLE {name}"))?.trim() == "1")
    }

    /// Writes the frame to the table (`table` or `database.table`) and returns the rows
    /// written.
    pub fn write(&self, frame: &DataFrame, table: &str, policy: WritePolicy) -> Result<u64> {
        let name = full_name(table, self.database.as_deref());
        let exists = self.table_exists(table)?;
        match (exists, policy) {
            (true, WritePolicy::Fail) => bail!("Table {table} already exists"),
            (true, WritePolicy::Replace) => {
                self.execute(&format!("DROP TABLE IF EXISTS {name}"))?;
                self.create(frame, &name)?;
            }
            (false, _) => self.create(frame, &name)?,
            (true, WritePolicy::Append) => {}
        }
        if frame.height() == 0 {
            return Ok(0);
        }
        let columns: Vec<String> = frame
            .get_column_names()
            .iter()
            .map(|c| quote(c.as_str()))
            .collect();
        let mut body = Vec::new();
        let mut frame = frame.clone();
        // The oldest Arrow layout (large strings, no views), which ClickHouse reads.
        IpcStreamWriter::new(&mut body)
            .with_compat_level(CompatLevel::oldest())
            .finish(&mut frame)
            .context("Could not encode the frame as Arrow")?;
        let statement = format!(
            "INSERT INTO {name} ({}) FORMAT ArrowStream",
            columns.join(", ")
        );
        let response = self.request(&[("query", statement.as_str())]).send(body);
        let mut response = Self::checked(response, "insert")?;
        let _ = response.body_mut().read_to_vec();
        Ok(frame.height() as u64)
    }

    fn create(&self, frame: &DataFrame, name: &str) -> Result<()> {
        let fields: Vec<String> = frame
            .columns()
            .iter()
            .map(|column| {
                Ok(format!(
                    "{} {}",
                    quote(column.name().as_str()),
                    nullable(column.name().as_str(), column.dtype())?
                ))
            })
            .collect::<Result<_>>()?;
        self.execute(&format!(
            "CREATE TABLE {name} ({}) ENGINE = {TABLE_ENGINE}",
            fields.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_quoted_as_the_python_client_quotes_them() {
        assert_eq!(quote("a`b\\c"), "`a\\`b\\\\c`");
        assert_eq!(full_name("t", Some("db")), "`db`.`t`");
        assert_eq!(full_name("other.t", Some("db")), "`other`.`t`");
        assert_eq!(full_name("t", None), "`t`");
    }

    #[test]
    fn columns_arrow_cannot_carry_are_cast() {
        assert_eq!(read_expression("i", "Int64"), None);
        assert_eq!(
            read_expression("s", "LowCardinality(Nullable(String))"),
            None
        );
        assert_eq!(read_expression("t", "DateTime64(6, 'UTC')"), None);
        assert_eq!(read_expression("d", "Decimal(38, 4)"), None);
        assert_eq!(read_expression("a", "Array(Int32)"), None);
        assert_eq!(
            read_expression("d", "Date").unwrap(),
            "toDate32(`d`) AS `d`"
        );
        assert_eq!(
            read_expression("d", "Nullable(Date)").unwrap(),
            "toDate32(`d`) AS `d`"
        );
        assert_eq!(
            read_expression("t", "DateTime('Europe/Paris')").unwrap(),
            "toDateTime64(`t`, 0) AS `t`"
        );
        assert_eq!(
            read_expression("u", "UUID").unwrap(),
            "toString(`u`) AS `u`"
        );
        assert_eq!(
            read_expression("u", "Nullable(UUID)").unwrap(),
            "CAST(toString(`u`), 'Nullable(String)') AS `u`"
        );
        assert!(read_expression("e", "Enum8('a' = 1)").is_some());
        assert!(read_expression("x", "Int128").is_some());
        assert!(read_expression("x", "Decimal(76, 2)").is_some());
        assert!(read_expression("x", "Array(UUID)").is_some());
        assert!(read_expression("x", "Array(Date)").is_some());
        assert!(read_expression("x", "Nullable(Nothing)").is_some());
        assert_eq!(read_expression("x", "Date32"), None);
    }

    #[test]
    fn created_columns_follow_the_frame_types() {
        let zone = DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC));
        assert_eq!(
            nullable("t", &zone).unwrap(),
            "Nullable(DateTime64(6, 'UTC'))"
        );
        assert_eq!(
            nullable("t", &DataType::Datetime(TimeUnit::Milliseconds, None)).unwrap(),
            "Nullable(DateTime64(3))"
        );
        assert_eq!(nullable("d", &DataType::Date).unwrap(), "Nullable(Date32)");
        assert_eq!(
            nullable("n", &DataType::UInt64).unwrap(),
            "Nullable(UInt64)"
        );
        let error = nullable("l", &DataType::List(Box::new(DataType::Int64)))
            .unwrap_err()
            .to_string();
        assert!(error.contains("Column l"), "{error}");
    }

    /// Against the integration tests' ClickHouse (MAGE_TEST_CLICKHOUSE_*); skipped without it.
    fn live() -> Option<ClickHouse> {
        let host = std::env::var("MAGE_TEST_CLICKHOUSE_HOST").ok()?;
        let settings: HashMap<String, String> = [
            ("CLICKHOUSE_HOST", host),
            (
                "CLICKHOUSE_PORT",
                std::env::var("MAGE_TEST_CLICKHOUSE_PORT").unwrap_or_else(|_| "18123".into()),
            ),
            (
                "CLICKHOUSE_USERNAME",
                std::env::var("MAGE_TEST_CLICKHOUSE_USER").unwrap_or_else(|_| "mage".into()),
            ),
            (
                "CLICKHOUSE_PASSWORD",
                std::env::var("MAGE_TEST_CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "test".into()),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        Some(ClickHouse::connect_settings(&settings).unwrap())
    }

    #[test]
    fn live_every_type_reads_exactly() -> Result<()> {
        let Some(client) = live() else { return Ok(()) };
        let frame = client.query(
            "SELECT toInt64(1) AS i, toNullable(toInt32(NULL)) AS ni, 'é' AS s, \
             toDate('2024-01-02') AS d, toDate32('1900-01-01') AS d32, \
             toDateTime('2024-01-02 03:04:05', 'UTC') AS dt, \
             toDateTime64('2024-01-02 03:04:05.123456', 6, 'Europe/Paris') AS dt6, \
             toDecimal128('1.2345', 4) AS dec, toDecimal256('1.5', 2) AS dec256, \
             toUUID('00000000-0000-0000-0000-000000000001') AS u, toIPv4('1.2.3.4') AS ip, \
             CAST('a', 'Enum8(\\'a\\' = 1)') AS e, toInt128(5) AS i128, \
             toUInt64(18446744073709551615) AS u64, [1, 2] AS arr, NULL AS n, \
             toNullable(toUUID('00000000-0000-0000-0000-000000000002')) AS nu",
        )?;
        let dtype = |name: &str| frame.column(name).unwrap().dtype().clone();
        assert_eq!(dtype("d"), DataType::Date);
        assert_eq!(dtype("d32"), DataType::Date);
        assert!(matches!(dtype("dt"), DataType::Datetime(_, Some(_))));
        assert!(matches!(
            dtype("dt6"),
            DataType::Datetime(TimeUnit::Microseconds, Some(_))
        ));
        assert!(matches!(dtype("dec"), DataType::Decimal(38, 4)));
        assert_eq!(dtype("u64"), DataType::UInt64);
        assert_eq!(dtype("u"), DataType::String);
        let text = |name: &str| {
            frame
                .column(name)
                .unwrap()
                .str()
                .unwrap()
                .get(0)
                .map(String::from)
        };
        assert_eq!(
            text("u").as_deref(),
            Some("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(text("ip").as_deref(), Some("1.2.3.4"));
        assert_eq!(text("e").as_deref(), Some("a"));
        assert_eq!(text("i128").as_deref(), Some("5"));
        // Exact, without trailing zeros: ClickHouse writes decimals that way as text.
        assert_eq!(text("dec256").as_deref(), Some("1.5"));
        assert_eq!(frame.column("n").unwrap().null_count(), 1);
        let day = frame.column("d").unwrap().date().unwrap().physical().get(0);
        assert_eq!(day, Some(19724));
        let seconds = frame.column("dt").unwrap().cast(&DataType::Int64)?;
        let stamp = seconds.i64()?.get(0).unwrap();
        let unit = match dtype("dt") {
            DataType::Datetime(unit, _) => unit,
            _ => unreachable!(),
        };
        let per_second = match unit {
            TimeUnit::Milliseconds => 1_000,
            TimeUnit::Microseconds => 1_000_000,
            TimeUnit::Nanoseconds => 1_000_000_000,
        };
        assert_eq!(stamp / per_second, 1_704_164_645);
        Ok(())
    }

    #[test]
    fn live_frames_round_trip_and_append_by_name() -> Result<()> {
        let Some(client) = live() else { return Ok(()) };
        let table = format!("rust_round_trip_{}", std::process::id());
        let frame = df!(
            "id" => [1i64, 2, 3],
            "name" => [Some("a"), None, Some("é")],
            "amount" => [Some(1.5f64), Some(2.25), None],
            "big" => [u64::MAX, 0, 7],
            "day" => [Some(19724i32), None, Some(-25567)],
        )?
        .lazy()
        .with_columns([
            col("day").cast(DataType::Date),
            lit(1_704_164_645_123_456i64)
                .cast(DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(TimeZone::UTC),
                ))
                .alias("at"),
            col("amount").cast(DataType::Decimal(10, 2)).alias("exact"),
        ])
        .collect()?;
        assert_eq!(client.write(&frame, &table, WritePolicy::Replace)?, 3);
        let back = client.query(&format!("SELECT * FROM {} ORDER BY id", quote(&table)))?;
        assert!(back.equals_missing(&frame), "{back}\n{frame}");

        // Append by name, in another column order.
        let more = frame.select(["big", "id", "name", "amount", "day", "at", "exact"])?;
        client.write(&more.head(Some(1)), &table, WritePolicy::Append)?;
        let count = client.query(&format!("SELECT count() AS n FROM {}", quote(&table)))?;
        assert_eq!(count.column("n")?.u64()?.get(0), Some(4));
        let error = client
            .write(&frame, &table, WritePolicy::Fail)
            .unwrap_err()
            .to_string();
        assert!(error.contains("already exists"), "{error}");
        let failed = client
            .query("SELECT throwIf(1, 'boom') AS x")
            .unwrap_err()
            .to_string();
        assert!(failed.contains("boom"), "{failed}");
        client.execute(&format!("DROP TABLE {}", quote(&table)))?;
        Ok(())
    }
}
