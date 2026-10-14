//! PostgreSQL sink for streams.
//!
//! A batch is one transaction, committed before the batch is confirmed. Its messages, JSON
//! objects, go to PostgreSQL with COPY, each value as its text (numbers as the blocks wrote
//! their digits, strings unquoted, objects and arrays as JSON), and PostgreSQL reads each into
//! its column: numbers keep every digit, and a value that does not fit its column stops the
//! write with PostgreSQL's reason instead of being cut. Upserts COPY into a temporary table
//! first. A table with array columns gets the batch as one JSON array instead, read with
//! `jsonb_array_elements`.
//!
//! The table is made from the first batch when it does not exist, and a field the table does
//! not have adds a column, as streams change. Column names follow MageML's Python sink
//! (cleaned, lower case, reserved words prefixed with `_`), so the same stream writes the
//! same table from MageML and from a service. With `unique_constraints`, rows are upserts
//! (`unique_conflict_method`: `IGNORE`, the default, or `UPDATE`), so a replayed batch writes
//! nothing twice.
//!
//! Settings follow MageML's PostgreSQL sink: `host`, `port`, `database`, `username`,
//! `password`, `schema`, `table`, `unique_constraints`, `unique_conflict_method`,
//! `auto_clean_name`, `case_sensitive`, `allow_reserved_words`; and `sslmode`, `sslrootcert`
//! (a file, PEM, or base64 PEM), `connect_timeout`.

mod tls;

use std::collections::BTreeMap;
use std::time::Duration;

use mage_stream_ports::sql::{Kind, clean_column};
use mage_stream_ports::{
    BatchMeta, BoxFuture, ConnectorError, RecordBatch, Sink, SinkCapabilities, read_envelope,
};
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::sync::Mutex;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Config};

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

/// A name as written in SQL: always quoted, so any name and case is exact.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A text literal.
fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The PostgreSQL type of a column made from values of a kind.
fn pg_type(kind: Kind) -> &'static str {
    match kind {
        Kind::Boolean => "BOOLEAN",
        Kind::BigInt => "BIGINT",
        Kind::Double => "DOUBLE PRECISION",
        Kind::Numeric => "NUMERIC",
        Kind::Json => "JSONB",
        Kind::Text => "TEXT",
    }
}

/// How a column's value is read from a message `e`, by the column's type in the table.
fn read_expression(key: &str, column_type: &str) -> String {
    let key = literal(key);
    match column_type {
        "jsonb" | "json" => format!("(e->{key})::{column_type}"),
        "text" => format!("(e->>{key})"),
        t if t.ends_with("[]") => format!(
            "(SELECT array_agg(x::{elem}) FROM jsonb_array_elements_text(e->{key}) x)",
            elem = t.trim_end_matches("[]")
        ),
        t => format!("(e->>{key})::{t}"),
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: Option<String>,
    schema: String,
    table: String,
    unique: Vec<String>,
    update: bool,
    case_sensitive: bool,
    auto_clean: bool,
    allow_reserved: bool,
    sslmode: Option<String>,
    sslrootcert: Option<String>,
    connect_timeout: Option<u64>,
}

impl Settings {
    pub fn new(config: &Value) -> Result<Settings, ConnectorError> {
        let need = |key: &str| {
            text(config, key)
                .ok_or_else(|| ConnectorError::fatal(format!("PostgreSQL settings need {key}.")))
        };
        let unique = match config.get("unique_constraints") {
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        };
        let method = text(config, "unique_conflict_method")
            .unwrap_or_else(|| "IGNORE".into())
            .to_ascii_uppercase();
        if method != "IGNORE" && method != "UPDATE" {
            return Err(ConnectorError::fatal(
                "PostgreSQL unique_conflict_method is IGNORE or UPDATE.",
            ));
        }
        let port = text(config, "port")
            .unwrap_or_else(|| "5432".into())
            .parse::<u16>()
            .map_err(|_| ConnectorError::fatal("PostgreSQL port is a number."))?;
        Ok(Settings {
            host: need("host")?,
            port,
            database: need("database")?,
            user: text(config, "username")
                .or_else(|| text(config, "user"))
                .ok_or_else(|| ConnectorError::fatal("PostgreSQL settings need username."))?,
            password: text(config, "password"),
            schema: text(config, "schema").unwrap_or_else(|| "public".into()),
            table: need("table")?,
            unique,
            update: method == "UPDATE",
            case_sensitive: flag(config, "case_sensitive", false),
            auto_clean: flag(config, "auto_clean_name", true),
            allow_reserved: flag(config, "allow_reserved_words", false),
            sslmode: text(config, "sslmode"),
            sslrootcert: text(config, "sslrootcert"),
            connect_timeout: text(config, "connect_timeout").and_then(|t| t.parse().ok()),
        })
    }

    fn column(&self, key: &str) -> String {
        if self.auto_clean {
            clean_column(key, self.case_sensitive, self.allow_reserved)
        } else {
            key.to_string()
        }
    }

    fn label(&self) -> String {
        format!(
            "{}.{} on {}:{}/{}",
            self.schema, self.table, self.host, self.port, self.database
        )
    }

    fn full_table(&self) -> String {
        format!("{}.{}", quote(&self.schema), quote(&self.table))
    }
}

/// A certificate given as PEM text or base64 PEM, as a file the TLS setup reads.
fn root_file(value: &str) -> Result<String, ConnectorError> {
    const MARKER: &str = "-----BEGIN CERTIFICATE-----";
    if !value.contains(MARKER) && std::path::Path::new(value).exists() {
        return Ok(value.to_string());
    }
    let pem = if value.contains(MARKER) {
        value.to_string()
    } else {
        decode_base64(value)
            .and_then(|b| String::from_utf8(b).ok())
            .filter(|p| p.contains(MARKER))
            .ok_or_else(|| {
                ConnectorError::fatal(
                    "PostgreSQL sslrootcert is neither a file, PEM, nor base64 PEM.",
                )
            })?
    };
    let digest: u64 = pem.bytes().fold(0xcbf29ce484222325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x100000001b3)
    });
    let path = std::env::temp_dir().join(format!("mage-ca-{digest:016x}.pem"));
    if !path.exists() {
        std::fs::write(&path, pem)
            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL sslrootcert: {e}")))?;
    }
    Ok(path.display().to_string())
}

fn decode_base64(text: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let (mut buffer, mut bits) = (0u32, 0u32);
    for c in text
        .bytes()
        .filter(|c| !c.is_ascii_whitespace() && *c != b'=')
    {
        let value = ALPHABET.iter().position(|a| *a == c)? as u32;
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    Some(out)
}

fn classify(context: &str, error: tokio_postgres::Error) -> ConnectorError {
    let message = format!("PostgreSQL {context}: {}", describe(&error));
    let retryable = match error.code() {
        Some(code) => {
            let code = code.code();
            code.starts_with("08")
                || code.starts_with("53")
                || code.starts_with("57P")
                || code == "40001"
                || code == "40P01"
        }
        // No SQL state: the connection failed or closed.
        None => true,
    };
    if retryable {
        ConnectorError::retryable(message)
    } else {
        ConnectorError::fatal(message)
    }
}

fn describe(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => {
            let mut text = db.message().to_string();
            if let Some(detail) = db.detail() {
                text.push_str(&format!(" ({detail})"));
            }
            text
        }
        None => {
            let mut text = error.to_string();
            let mut source = std::error::Error::source(error);
            while let Some(inner) = source {
                text.push_str(&format!(": {inner}"));
                source = inner.source();
            }
            text
        }
    }
}

pub struct PgSink {
    settings: Settings,
    client: Mutex<Option<Client>>,
}

impl PgSink {
    pub fn new(config: &Value) -> Result<PgSink, ConnectorError> {
        let settings = Settings::new(config)?;
        // Checked now, so a bad mode or certificate stops the service at start-up.
        tls_for(&settings)?;
        Ok(PgSink {
            settings,
            client: Mutex::new(None),
        })
    }

    async fn connect(&self) -> Result<Client, ConnectorError> {
        let settings = &self.settings;
        let mut config = Config::new();
        config
            .host(&settings.host)
            .port(settings.port)
            .dbname(&settings.database)
            .user(&settings.user)
            .application_name("mage-stream");
        if let Some(password) = &settings.password {
            config.password(password);
        }
        if let Some(seconds) = settings.connect_timeout {
            config.connect_timeout(Duration::from_secs(seconds));
        }
        let (mode, connector) = tls_for(settings)?;
        config.ssl_mode(mode);
        let (client, connection) = config
            .connect(connector)
            .await
            .map_err(|e| classify(&format!("connecting to {}", settings.label()), e))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// The table's columns and their types, empty when it does not exist.
    async fn columns(
        client: &impl tokio_postgres::GenericClient,
        settings: &Settings,
    ) -> Result<BTreeMap<String, String>, ConnectorError> {
        let rows = client
            .query(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = \
                 c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND \
                 NOT a.attisdropped",
                &[&settings.schema, &settings.table],
            )
            .await
            .map_err(|e| classify("reading the table's columns", e))?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn write_batch(
        &self,
        client: &mut Client,
        values: &[Vec<u8>],
    ) -> Result<(), ConnectorError> {
        let settings = &self.settings;
        // The messages' fields, their columns, and the types the values ask for.
        let mut keys: BTreeMap<String, String> = BTreeMap::new();
        let mut kinds: BTreeMap<String, Option<Kind>> = BTreeMap::new();
        let mut rows: Vec<BTreeMap<String, Box<RawValue>>> = Vec::with_capacity(values.len());
        for (row, value) in values.iter().enumerate() {
            let text = std::str::from_utf8(value).map_err(|_| {
                ConnectorError::fatal(format!("PostgreSQL: row {row} is not UTF-8"))
            })?;
            let fields: BTreeMap<String, Box<RawValue>> =
                serde_json::from_str(text).map_err(|_| {
                    ConnectorError::fatal(format!(
                        "PostgreSQL: row {row} is not a JSON object; the sink writes objects"
                    ))
                })?;
            for (key, raw) in &fields {
                let column = settings.column(key);
                match keys.get(&column) {
                    Some(other) if other != key => {
                        return Err(ConnectorError::fatal(format!(
                            "PostgreSQL: fields {other:?} and {key:?} both become column \
                             {column}; rename one, or set auto_clean_name: false"
                        )));
                    }
                    _ => {
                        keys.insert(column.clone(), key.clone());
                    }
                }
                let kind = Kind::of(raw.get());
                let entry = kinds.entry(column).or_insert(None);
                *entry = match (*entry, kind) {
                    (Some(a), Some(b)) => Some(a.merge(b)),
                    (a, b) => a.or(b),
                };
            }
            rows.push(fields);
        }
        for unique in &settings.unique {
            if !keys.contains_key(&settings.column(unique)) && !keys.contains_key(unique) {
                return Err(ConnectorError::fatal(format!(
                    "PostgreSQL: the unique constraint column {unique} is not in the messages"
                )));
            }
        }
        let transaction = client
            .transaction()
            .await
            .map_err(|e| classify("starting a transaction", e))?;
        let mut columns = Self::columns(&transaction, settings).await?;
        if columns.is_empty() {
            let definitions: Vec<String> = kinds
                .iter()
                .map(|(column, kind)| {
                    let not_null = if settings
                        .unique
                        .iter()
                        .any(|u| &settings.column(u) == column)
                    {
                        " NOT NULL"
                    } else {
                        ""
                    };
                    format!(
                        "{} {}{not_null}",
                        quote(column),
                        pg_type(kind.unwrap_or(Kind::Text))
                    )
                })
                .chain((!settings.unique.is_empty()).then(|| {
                    format!(
                        "UNIQUE ({})",
                        settings
                            .unique
                            .iter()
                            .map(|u| quote(&settings.column(u)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }))
                .collect();
            transaction
                .batch_execute(&format!(
                    "CREATE SCHEMA IF NOT EXISTS {}; CREATE TABLE IF NOT EXISTS {} ({})",
                    quote(&settings.schema),
                    settings.full_table(),
                    definitions.join(", ")
                ))
                .await
                .map_err(|e| classify(&format!("creating {}", settings.label()), e))?;
            columns = Self::columns(&transaction, settings).await?;
        } else {
            let missing: Vec<String> = kinds
                .iter()
                .filter(|(column, _)| !columns.contains_key(*column))
                .map(|(column, kind)| {
                    format!(
                        "ADD COLUMN IF NOT EXISTS {} {}",
                        quote(column),
                        pg_type(kind.unwrap_or(Kind::Text))
                    )
                })
                .collect();
            if !missing.is_empty() {
                transaction
                    .batch_execute(&format!(
                        "ALTER TABLE {} {}",
                        settings.full_table(),
                        missing.join(", ")
                    ))
                    .await
                    .map_err(|e| classify("adding columns", e))?;
                columns = Self::columns(&transaction, settings).await?;
            }
        }
        let names: Vec<&String> = keys.keys().collect();
        let types: Vec<&str> = names
            .iter()
            .map(|column| columns.get(*column).map_or("text", String::as_str))
            .collect();
        if !types.iter().any(|t| t.ends_with("[]")) {
            self.copy_rows(&transaction, &names, &types, &keys, &rows)
                .await?;
            return transaction
                .commit()
                .await
                .map_err(|e| classify("committing", e));
        }
        let expressions: Vec<String> = names
            .iter()
            .map(|column| {
                let column_type = columns.get(*column).map_or("text", String::as_str);
                format!(
                    "{} AS {}",
                    read_expression(&keys[*column], column_type),
                    quote(column)
                )
            })
            .collect();
        let target: Vec<String> = names.iter().map(|c| quote(c)).collect();
        let unique: Vec<String> = settings
            .unique
            .iter()
            .map(|u| quote(&settings.column(u)))
            .collect();
        let mut sql = if unique.is_empty() {
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM jsonb_array_elements($1::jsonb) AS e",
                settings.full_table(),
                target.join(", "),
                expressions.join(", ")
            )
        } else {
            // A key twice in one batch: the last message wins, as it would one by one.
            format!(
                "INSERT INTO {table} ({cols}) SELECT {cols} FROM (SELECT DISTINCT ON ({keys}) * \
                 FROM (SELECT {exprs}, n FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY \
                 AS m(e, n)) AS r ORDER BY {keys}, n DESC) AS d",
                table = settings.full_table(),
                cols = target.join(", "),
                keys = unique.join(", "),
                exprs = expressions.join(", ")
            )
        };
        if !unique.is_empty() {
            let updates: Vec<String> = target
                .iter()
                .filter(|c| !unique.contains(c))
                .map(|c| format!("{c} = EXCLUDED.{c}"))
                .collect();
            if settings.update && !updates.is_empty() {
                sql.push_str(&format!(
                    " ON CONFLICT ({}) DO UPDATE SET {}",
                    unique.join(", "),
                    updates.join(", ")
                ));
            } else {
                sql.push_str(&format!(" ON CONFLICT ({}) DO NOTHING", unique.join(", ")));
            }
        }
        // The messages as one JSON array, the bytes as the blocks made them.
        let mut array = Vec::with_capacity(values.iter().map(|v| v.len() + 1).sum::<usize>() + 2);
        array.push(b'[');
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                array.push(b',');
            }
            array.extend_from_slice(value);
        }
        array.push(b']');
        let payload = String::from_utf8(array).expect("checked UTF-8 rows");
        let statement = transaction
            .prepare_typed(&sql, &[Type::TEXT])
            .await
            .map_err(|e| classify(&format!("preparing the write to {}", settings.label()), e))?;
        transaction
            .execute(&statement, &[&payload])
            .await
            .map_err(|e| classify(&format!("writing to {}", settings.label()), e))?;
        transaction
            .commit()
            .await
            .map_err(|e| classify("committing", e))?;
        Ok(())
    }
}

impl PgSink {
    /// The rows by COPY: into the table, or for upserts into a temporary table, then into the
    /// table with the last message of each key, as one by one.
    async fn copy_rows(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        names: &[&String],
        types: &[&str],
        keys: &BTreeMap<String, String>,
        rows: &[BTreeMap<String, Box<RawValue>>],
    ) -> Result<(), ConnectorError> {
        use futures_util::SinkExt;

        let settings = &self.settings;
        let target: Vec<String> = names.iter().map(|c| quote(c)).collect();
        let unique: Vec<String> = settings
            .unique
            .iter()
            .map(|u| quote(&settings.column(u)))
            .collect();
        let mut data = Vec::with_capacity(rows.len() * 64);
        for (index, row) in rows.iter().enumerate() {
            for (position, (column, column_type)) in names.iter().zip(types).enumerate() {
                if position > 0 {
                    data.push(b'\t');
                }
                copy_field(
                    &mut data,
                    row.get(&keys[*column]).map(AsRef::as_ref),
                    column_type,
                );
            }
            if !unique.is_empty() {
                data.extend_from_slice(format!("\t{index}").as_bytes());
            }
            data.push(b'\n');
        }
        let copy = if unique.is_empty() {
            format!(
                "COPY {} ({}) FROM STDIN",
                settings.full_table(),
                target.join(", ")
            )
        } else {
            transaction
                .batch_execute(&format!(
                    "CREATE TEMP TABLE mage_stage (LIKE {}) ON COMMIT DROP; \
                     ALTER TABLE mage_stage ADD COLUMN \"__mage_row\" BIGINT",
                    settings.full_table()
                ))
                .await
                .map_err(|e| classify("staging the batch", e))?;
            format!(
                "COPY mage_stage ({}, \"__mage_row\") FROM STDIN",
                target.join(", ")
            )
        };
        let sink = transaction
            .copy_in::<_, bytes::Bytes>(copy.as_str())
            .await
            .map_err(|e| classify(&format!("writing to {}", settings.label()), e))?;
        futures_util::pin_mut!(sink);
        sink.send(bytes::Bytes::from(data))
            .await
            .map_err(|e| classify(&format!("writing to {}", settings.label()), e))?;
        sink.finish()
            .await
            .map_err(|e| classify(&format!("writing to {}", settings.label()), e))?;
        if unique.is_empty() {
            return Ok(());
        }
        let updates: Vec<String> = target
            .iter()
            .filter(|c| !unique.contains(c))
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect();
        let conflict = if settings.update && !updates.is_empty() {
            format!(
                "ON CONFLICT ({}) DO UPDATE SET {}",
                unique.join(", "),
                updates.join(", ")
            )
        } else {
            format!("ON CONFLICT ({}) DO NOTHING", unique.join(", "))
        };
        // A key twice in one batch: the last message wins, as it would one by one.
        transaction
            .batch_execute(&format!(
                "INSERT INTO {table} ({cols}) SELECT {cols} FROM (SELECT DISTINCT ON ({keys}) * \
                 FROM mage_stage ORDER BY {keys}, \"__mage_row\" DESC) AS d {conflict}",
                table = settings.full_table(),
                cols = target.join(", "),
                keys = unique.join(", "),
            ))
            .await
            .map_err(|e| classify(&format!("writing to {}", settings.label()), e))
    }
}

/// A value in COPY's text format, read as `(e->>key)::type` would read it: null and a
/// missing field are NULL, a string is its text (for a JSON column, the JSON string), and
/// numbers, booleans, objects and arrays are their JSON text.
fn copy_field(out: &mut Vec<u8>, raw: Option<&RawValue>, column_type: &str) {
    let Some(text) = raw.map(RawValue::get) else {
        out.extend_from_slice(b"\\N");
        return;
    };
    if text == "null" {
        out.extend_from_slice(b"\\N");
        return;
    }
    let json_column = matches!(column_type, "jsonb" | "json");
    if !json_column
        && text.starts_with('"')
        && let Ok(string) = serde_json::from_str::<String>(text)
    {
        copy_escape(out, string.as_bytes());
        return;
    }
    copy_escape(out, text.as_bytes());
}

fn copy_escape(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        match byte {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            _ => out.push(byte),
        }
    }
}

fn tls_for(
    settings: &Settings,
) -> Result<
    (
        tokio_postgres::config::SslMode,
        tokio_postgres_rustls::MakeRustlsConnect,
    ),
    ConnectorError,
> {
    use tls::Check;
    use tokio_postgres::config::SslMode;

    let root = settings.sslrootcert.as_deref().map(root_file).transpose()?;
    let mode = settings.sslmode.as_deref().unwrap_or("prefer");
    let (mode, check) = match mode {
        "disable" => (SslMode::Disable, Check::None),
        "allow" | "prefer" => (SslMode::Prefer, Check::None),
        "require" if root.is_some() => (SslMode::Require, Check::Chain),
        "require" => (SslMode::Require, Check::None),
        "verify-ca" => (SslMode::Require, Check::Chain),
        "verify-full" => (SslMode::Require, Check::Full),
        other => {
            return Err(ConnectorError::fatal(format!(
                "PostgreSQL sslmode {other} is not one of disable, allow, prefer, require, \
                 verify-ca, verify-full"
            )));
        }
    };
    let connector = tls::connector(check, root.as_deref())
        .map_err(|e| ConnectorError::fatal(format!("PostgreSQL TLS: {e}")))?;
    Ok((mode, connector))
}

impl Sink for PgSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let rows = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
            if rows.is_empty() {
                return Ok(());
            }
            let values: Vec<Vec<u8>> = rows.into_iter().map(|r| r.value).collect();
            let mut guard = self.client.lock().await;
            if guard.as_ref().is_none_or(|c| c.is_closed()) {
                *guard = Some(self.connect().await?);
            }
            let client = guard.as_mut().expect("connected");
            let result = self.write_batch(client, &values).await;
            if let Err(error) = &result
                && error.retryable
            {
                // The next attempt connects again.
                *guard = None;
            }
            result
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: !self.settings.unique.is_empty(),
            transactional: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn columns_are_named_as_mage_names_them() {
        assert_eq!(clean_column("Order ID", false, false), "order_id");
        assert_eq!(clean_column("2nd try", false, false), "letter_2nd_try");
        assert_eq!(clean_column("select", false, false), "_select");
        assert_eq!(clean_column("select", false, true), "select");
        assert_eq!(clean_column("Größe", false, false), "größe");
        assert_eq!(clean_column("Price $", true, false), "Price_");
        assert!(
            mage_stream_ports::sql::RESERVED_WORDS
                .windows(2)
                .all(|w| w[0] < w[1]),
            "sorted"
        );
    }

    #[test]
    fn value_kinds_widen_without_losing_values() {
        assert_eq!(Kind::of("12"), Some(Kind::BigInt));
        assert_eq!(
            Kind::of("123456789012345678901234567890"),
            Some(Kind::Numeric)
        );
        assert_eq!(Kind::of("1.5e3"), Some(Kind::Double));
        assert_eq!(Kind::of("\"x\""), Some(Kind::Text));
        assert_eq!(Kind::of("{\"a\": 1}"), Some(Kind::Json));
        assert_eq!(Kind::of("null"), None);
        assert_eq!(Kind::BigInt.merge(Kind::Numeric), Kind::Numeric);
        assert_eq!(Kind::BigInt.merge(Kind::Text), Kind::Text);
        assert_eq!(Kind::Json.merge(Kind::BigInt), Kind::Json);
    }

    #[test]
    fn copy_fields_read_as_the_json_path_did() {
        let field = |json: &str, column_type: &str| {
            let raw: Box<RawValue> = serde_json::from_str(json).unwrap();
            let mut out = Vec::new();
            copy_field(&mut out, Some(&raw), column_type);
            String::from_utf8(out).unwrap()
        };
        assert_eq!(
            field("1180591620717411303425", "numeric"),
            "1180591620717411303425"
        );
        assert_eq!(
            field(r#""Zürich\ttab\nline\\""#, "text"),
            "Zürich\\ttab\\nline\\\\"
        );
        assert_eq!(field(r#""x""#, "jsonb"), r#""x""#);
        assert_eq!(field(r#"{"n":[1,2]}"#, "text"), r#"{"n":[1,2]}"#);
        assert_eq!(field("null", "bigint"), "\\N");
        assert_eq!(field("true", "boolean"), "true");
        let mut out = Vec::new();
        copy_field(&mut out, None, "text");
        assert_eq!(out, b"\\N");
    }

    #[test]
    fn values_are_read_by_the_column_type() {
        assert_eq!(read_expression("a'b", "bigint"), "(e->>'a''b')::bigint");
        assert_eq!(read_expression("x", "jsonb"), "(e->'x')::jsonb");
        assert_eq!(read_expression("x", "text"), "(e->>'x')");
        assert!(read_expression("tags", "integer[]").contains("jsonb_array_elements_text"));
    }

    #[test]
    fn settings_need_a_table_and_a_known_method() {
        let ok = json!({"host": "db", "database": "shop", "username": "u", "table": "orders"});
        let settings = Settings::new(&ok).unwrap();
        assert_eq!((settings.schema.as_str(), settings.port), ("public", 5432));
        assert!(!settings.label().contains("password"));
        let mut bad = ok.clone();
        bad["unique_conflict_method"] = json!("MERGE");
        assert!(Settings::new(&bad).is_err());
        assert!(Settings::new(&json!({"host": "db"})).is_err());
    }

    #[test]
    fn base64_certificates_decode() {
        assert_eq!(decode_base64("aGVsbG8=").unwrap(), b"hello");
        assert!(root_file("not a certificate").is_err());
    }
}
