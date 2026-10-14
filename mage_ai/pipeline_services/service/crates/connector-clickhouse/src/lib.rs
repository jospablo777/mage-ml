//! ClickHouse sink for streams, over ClickHouse's HTTP interface.
//!
//! Each batch is one `INSERT ... FORMAT JSONEachRow` of the messages as the blocks made them
//! (only keys are renamed to column names), so ClickHouse reads every number from its text:
//! integers past 64 bits go into `Int256` columns exactly, and a value that does not fit its
//! column fails the insert with ClickHouse's reason. The insert carries
//! `insert_deduplication_token = <stream>:<partition>:<first>-<last>`: a batch written again
//! after a restart is dropped by ClickHouse on replicated tables and on the tables this sink
//! makes (`non_replicated_deduplication_window`).
//!
//! A missing table is made from the first batch (MergeTree, nullable columns typed from the
//! values, objects and lists as JSON text), and a new field adds a column. Column names
//! follow MageML's Python sinks.
//!
//! Settings: `host`, `port` (8123, or 8443 with `secure: true`), `username`, `password`,
//! `database`, `table`, `secure`, `auto_clean_name`, `case_sensitive`,
//! `allow_reserved_words`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mage_stream_core::Position;
use mage_stream_ports::sql::{Kind, clean_column};
use mage_stream_ports::{
    BatchMeta, BoxFuture, ConnectorError, RecordBatch, Sink, SinkCapabilities, read_envelope,
};
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::sync::Mutex;

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn flag(config: &Value, key: &str, default: bool) -> bool {
    match config.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => matches!(s.to_ascii_lowercase().as_str(), "true" | "yes" | "1"),
        _ => default,
    }
}

/// A name as written in ClickHouse SQL.
fn quote(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// The ClickHouse type of a column made from values of a kind.
fn ch_type(kind: Kind) -> &'static str {
    match kind {
        Kind::Boolean => "Nullable(Bool)",
        Kind::BigInt => "Nullable(Int64)",
        Kind::Double => "Nullable(Float64)",
        Kind::Numeric => "Nullable(Int256)",
        Kind::Json | Kind::Text => "Nullable(String)",
    }
}

fn position_text(position: &Position) -> String {
    match position {
        Position::Offset(o) => o.to_string(),
        Position::Lsn(l) => format!("{l:X}"),
        Position::Tag(t) => t.to_string(),
        Position::Opaque(b) => b.iter().map(|x| format!("{x:02x}")).collect(),
    }
}

#[derive(Debug, Clone)]
struct Settings {
    url: String,
    user: String,
    password: Option<String>,
    database: String,
    table: String,
    case_sensitive: bool,
    auto_clean: bool,
    allow_reserved: bool,
}

impl Settings {
    fn new(config: &Value) -> Result<Settings, ConnectorError> {
        let host = text(config, "host")
            .ok_or_else(|| ConnectorError::fatal("ClickHouse settings need host."))?;
        let secure = flag(config, "secure", false);
        let port =
            text(config, "port").unwrap_or_else(|| (if secure { "8443" } else { "8123" }).into());
        let scheme = if secure || port == "8443" {
            "https"
        } else {
            "http"
        };
        let host = host
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .trim_end_matches('/');
        Ok(Settings {
            url: format!("{scheme}://{host}:{port}/"),
            user: text(config, "username")
                .or_else(|| text(config, "user"))
                .unwrap_or_else(|| "default".into()),
            password: text(config, "password"),
            database: text(config, "database").unwrap_or_else(|| "default".into()),
            table: text(config, "table")
                .or_else(|| text(config, "table_name"))
                .ok_or_else(|| ConnectorError::fatal("ClickHouse settings need table."))?,
            case_sensitive: flag(config, "case_sensitive", false),
            auto_clean: flag(config, "auto_clean_name", true),
            allow_reserved: flag(config, "allow_reserved_words", false),
        })
    }

    fn column(&self, key: &str) -> String {
        if self.auto_clean {
            clean_column(key, self.case_sensitive, self.allow_reserved)
        } else {
            key.to_string()
        }
    }

    fn full_table(&self) -> String {
        format!("{}.{}", quote(&self.database), quote(&self.table))
    }
}

struct Client {
    agent: ureq::Agent,
    settings: Settings,
}

impl Client {
    /// Runs a statement; the body is the query's data (an insert's rows) when given.
    fn run(
        &self,
        query: &str,
        body: Option<&[u8]>,
        parameters: &[(&str, &str)],
    ) -> Result<String, ConnectorError> {
        let mut request = self
            .agent
            .post(&self.settings.url)
            .header("X-ClickHouse-User", &self.settings.user)
            .header("X-ClickHouse-Database", &self.settings.database);
        if let Some(password) = &self.settings.password {
            request = request.header("X-ClickHouse-Key", password);
        }
        let mut owned = vec![("query", query)];
        owned.extend_from_slice(parameters);
        for (key, value) in owned {
            request = request.query(key, value);
        }
        let response = match body {
            Some(body) => request.send(body),
            None => request.send_empty(),
        }
        .map_err(|e| {
            ConnectorError::retryable(format!(
                "ClickHouse at {} could not be reached: {e}",
                self.settings.url
            ))
        })?;
        let status = response.status().as_u16();
        let text = response.into_body().read_to_string().unwrap_or_default();
        match status {
            200 => Ok(text),
            500..=599 if !text.contains("Code: ") => Err(ConnectorError::retryable(format!(
                "ClickHouse answered {status}: {}",
                text.trim()
            ))),
            _ => {
                // ClickHouse's exceptions: TOO_MANY_PARTS and its kin are worth retrying.
                let retryable = [
                    "Code: 252",
                    "Code: 241",
                    "Code: 159",
                    "Code: 209",
                    "Code: 210",
                ]
                .iter()
                .any(|c| text.contains(c));
                let message = format!("ClickHouse: {}", text.trim());
                Err(if retryable {
                    ConnectorError::retryable(message)
                } else {
                    ConnectorError::fatal(message)
                })
            }
        }
    }

    fn columns(&self) -> Result<BTreeMap<String, String>, ConnectorError> {
        let text = self.run(
            "SELECT name, type FROM system.columns WHERE database = {db:String} AND table = \
             {table:String} FORMAT JSONCompact",
            None,
            &[
                ("param_db", &self.settings.database),
                ("param_table", &self.settings.table),
            ],
        )?;
        let parsed: Value = serde_json::from_str(&text)
            .map_err(|e| ConnectorError::fatal(format!("ClickHouse sent invalid JSON: {e}")))?;
        Ok(parsed["data"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|r| Some((r[0].as_str()?.to_string(), r[1].as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default())
    }

    fn write(&self, values: &[Vec<u8>], token: &str) -> Result<(), ConnectorError> {
        let settings = &self.settings;
        let mut kinds: BTreeMap<String, Option<Kind>> = BTreeMap::new();
        let mut names: BTreeMap<String, String> = BTreeMap::new();
        let mut body = Vec::with_capacity(values.iter().map(|v| v.len() + 1).sum());
        for (row, value) in values.iter().enumerate() {
            let text = std::str::from_utf8(value).map_err(|_| {
                ConnectorError::fatal(format!("ClickHouse: row {row} is not UTF-8"))
            })?;
            let fields: BTreeMap<String, Box<RawValue>> =
                serde_json::from_str(text).map_err(|_| {
                    ConnectorError::fatal(format!(
                        "ClickHouse: row {row} is not a JSON object; the sink writes objects"
                    ))
                })?;
            // The row with each key renamed to its column; values are left as they are.
            body.push(b'{');
            for (index, (key, raw)) in fields.iter().enumerate() {
                let column = settings.column(key);
                match names.get(&column) {
                    Some(other) if other != key => {
                        return Err(ConnectorError::fatal(format!(
                            "ClickHouse: fields {other:?} and {key:?} both become column \
                             {column}; rename one, or set auto_clean_name: false"
                        )));
                    }
                    _ => {
                        names.insert(column.clone(), key.clone());
                    }
                }
                if index > 0 {
                    body.push(b',');
                }
                body.extend_from_slice(
                    serde_json::to_string(&column).expect("a string").as_bytes(),
                );
                body.push(b':');
                body.extend_from_slice(raw.get().as_bytes());
                let kind = Kind::of(raw.get());
                let entry = kinds.entry(column).or_insert(None);
                *entry = match (*entry, kind) {
                    (Some(a), Some(b)) => Some(a.merge(b)),
                    (a, b) => a.or(b),
                };
            }
            body.extend_from_slice(b"}\n");
        }
        let columns = self.columns()?;
        if columns.is_empty() {
            let definitions: Vec<String> = kinds
                .iter()
                .map(|(c, k)| format!("{} {}", quote(c), ch_type(k.unwrap_or(Kind::Text))))
                .collect();
            self.run(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = MergeTree ORDER BY tuple() \
                     SETTINGS non_replicated_deduplication_window = 1000",
                    settings.full_table(),
                    definitions.join(", ")
                ),
                None,
                &[],
            )?;
        } else {
            let missing: Vec<String> = kinds
                .iter()
                .filter(|(c, _)| !columns.contains_key(*c))
                .map(|(c, k)| {
                    format!(
                        "ADD COLUMN IF NOT EXISTS {} {}",
                        quote(c),
                        ch_type(k.unwrap_or(Kind::Text))
                    )
                })
                .collect();
            if !missing.is_empty() {
                self.run(
                    &format!(
                        "ALTER TABLE {} {}",
                        settings.full_table(),
                        missing.join(", ")
                    ),
                    None,
                    &[],
                )?;
            }
        }
        self.run(
            &format!("INSERT INTO {} FORMAT JSONEachRow", settings.full_table()),
            Some(&body),
            &[
                ("insert_deduplication_token", token),
                ("input_format_json_read_objects_as_strings", "1"),
                ("input_format_json_read_arrays_as_strings", "1"),
                ("date_time_input_format", "best_effort"),
                ("input_format_skip_unknown_fields", "0"),
            ],
        )?;
        Ok(())
    }
}

pub struct ClickHouseSink {
    client: Arc<Client>,
    stream: String,
    lock: Mutex<()>,
}

impl ClickHouseSink {
    /// `stream`: the stream's name, part of each batch's deduplication token.
    pub fn new(config: &Value, stream: &str) -> Result<ClickHouseSink, ConnectorError> {
        let settings = Settings::new(config)?;
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(120)))
            .http_status_as_error(false)
            .build()
            .into();
        Ok(ClickHouseSink {
            client: Arc::new(Client { agent, settings }),
            stream: stream.to_string(),
            lock: Mutex::new(()),
        })
    }
}

impl Sink for ClickHouseSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let rows = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
            if rows.is_empty() {
                return Ok(());
            }
            let values: Vec<Vec<u8>> = rows.into_iter().map(|r| r.value).collect();
            let token = format!(
                "{}:{}:{}-{}",
                self.stream,
                meta.id.partition.0,
                position_text(&meta.first),
                position_text(&meta.last)
            );
            // Schema changes and inserts of one sink go one at a time.
            let _guard = self.lock.lock().await;
            let client = self.client.clone();
            tokio::task::spawn_blocking(move || client.write(&values, &token))
                .await
                .map_err(|e| ConnectorError::fatal(e.to_string()))?
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: true,
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn settings_make_the_url_and_need_a_table() {
        let settings =
            Settings::new(&json!({"host": "ch.example.com", "table": "events"})).unwrap();
        assert_eq!(settings.url, "http://ch.example.com:8123/");
        let secure =
            Settings::new(&json!({"host": "https://ch/", "secure": true, "table_name": "e"}))
                .unwrap();
        assert_eq!(secure.url, "https://ch:8443/");
        assert!(Settings::new(&json!({"host": "ch"})).is_err());
    }

    #[test]
    fn names_are_quoted_and_types_keep_values() {
        assert_eq!(quote("a`b"), "`a\\`b`");
        assert_eq!(ch_type(Kind::Numeric), "Nullable(Int256)");
        assert_eq!(ch_type(Kind::Json), "Nullable(String)");
    }
}
