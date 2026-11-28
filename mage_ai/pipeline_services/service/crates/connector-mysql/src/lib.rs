//! MySQL change capture source for streams (ADR-0048): the binary log in ROW format, read as
//! a replica reads it, with the PostgreSQL source's messages, so the same sinks (a mirror, a
//! history, a plain table) take either.
//!
//! Each changed row reaches the blocks as a JSON object of its columns and `_mage_operation`
//! (insert, update, delete, or snapshot for the initial copy), `_mage_schema` (the database),
//! `_mage_table`, `_mage_binlog` (`file:offset` of the change) and `_mage_commit_time`, and,
//! for deletes, `_mage_deleted_at`. Values are written by [`values`]. An update that changes a
//! primary key is a delete of the old key and then the update, as with PostgreSQL.
//!
//! Settings: `host`, `port` (3306), `database`, `user`, `password`, `tables` (`db.table`, or
//! `table` in `database`; none: every table of `database`), `server_id` (a replica id unique
//! on the server; a random one when unset), `snapshot` (`initial` copies the tables first;
//! `never`), `sslmode` (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`),
//! `sslrootcert`, `connect_timeout`.
//!
//! The server needs `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`,
//! `binlog_row_metadata=FULL` and `binlog_transaction_compression=OFF`; the user REPLICATION
//! SLAVE, REPLICATION CLIENT and SELECT. [`check_source`] says what is missing.

use std::time::Duration;

use mage_stream_ports::ConnectorError;
use mage_stream_ports::check::ConnectionReport;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::Value;

pub mod cdc;
mod gtid;
mod values;

pub use cdc::{BatchLimits, MySqlCdcSource, PositionRecord};

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// A name as written in SQL: always quoted, so any name and case is exact.
pub(crate) fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SslMode {
    Disable,
    /// TLS when the server offers it, without checking its certificate.
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
    /// (database, table)
    pub tables: Vec<(String, String)>,
    pub server_id: Option<u32>,
    pub snapshot: bool,
    pub sslmode: SslMode,
    pub sslrootcert: Option<String>,
    pub connect_timeout: Duration,
}

impl Settings {
    pub fn new(config: &Value) -> Result<Settings, ConnectorError> {
        let need = |key: &str| {
            text(config, key)
                .ok_or_else(|| ConnectorError::fatal(format!("MySQL source settings need {key}.")))
        };
        let database = need("database")?;
        let port = text(config, "port")
            .unwrap_or_else(|| "3306".into())
            .parse::<u16>()
            .map_err(|_| ConnectorError::fatal("MySQL port is a number."))?;
        let names: Vec<String> = match config.get("tables") {
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            Some(Value::String(list)) => list.split(',').map(str::to_string).collect(),
            _ => Vec::new(),
        };
        let tables = names
            .iter()
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .map(|t| match t.split_once('.') {
                Some((db, table)) => (db.to_string(), table.to_string()),
                None => (database.clone(), t.to_string()),
            })
            .collect();
        let server_id = match text(config, "server_id") {
            Some(id) => Some(id.parse::<u32>().ok().filter(|id| *id > 0).ok_or_else(|| {
                ConnectorError::fatal(format!(
                    "MySQL server_id {id} is a replica id from 1 to 4294967295."
                ))
            })?),
            None => None,
        };
        let sslmode = match text(config, "sslmode").as_deref() {
            None | Some("prefer") => SslMode::Prefer,
            Some("disable") => SslMode::Disable,
            Some("require") => SslMode::Require,
            Some("verify-ca") | Some("verify_ca") => SslMode::VerifyCa,
            Some("verify-full") | Some("verify_identity") => SslMode::VerifyFull,
            Some(other) => {
                return Err(ConnectorError::fatal(format!(
                    "MySQL sslmode is disable, prefer, require, verify-ca or verify-full, not \
                     {other}."
                )));
            }
        };
        Ok(Settings {
            host: need("host")?,
            port,
            user: text(config, "user")
                .or_else(|| text(config, "username"))
                .ok_or_else(|| ConnectorError::fatal("MySQL source settings need user."))?,
            password: text(config, "password"),
            database,
            tables,
            server_id,
            snapshot: match text(config, "snapshot").as_deref() {
                None | Some("never") => false,
                Some("initial") => true,
                Some(other) => {
                    return Err(ConnectorError::fatal(format!(
                        "MySQL snapshot is initial or never, not {other}."
                    )));
                }
            },
            sslmode,
            sslrootcert: text(config, "sslrootcert"),
            connect_timeout: Duration::from_secs(
                text(config, "connect_timeout")
                    .and_then(|t| t.parse().ok())
                    .unwrap_or(10),
            ),
        })
    }

    pub fn label(&self) -> String {
        format!("{}:{}/{}", self.host, self.port, self.database)
    }

    fn options(&self, ssl: Option<SslOpts>) -> OptsBuilder {
        OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(self.password.clone())
            .db_name(Some(self.database.clone()))
            .prefer_socket(false)
            // Times as UTC text in the copy; names and text as UTF-8.
            .init(vec!["SET time_zone = '+00:00'", "SET NAMES utf8mb4"])
            .ssl_opts(ssl)
    }

    fn ssl(&self) -> Option<SslOpts> {
        let roots = || {
            self.sslrootcert
                .as_ref()
                .map(|path| vec![std::path::PathBuf::from(path).into()])
                .unwrap_or_default()
        };
        match self.sslmode {
            SslMode::Disable => None,
            SslMode::Prefer | SslMode::Require => {
                Some(SslOpts::default().with_danger_accept_invalid_certs(true))
            }
            SslMode::VerifyCa => Some(
                SslOpts::default()
                    .with_root_certs(roots())
                    .with_danger_skip_domain_validation(true),
            ),
            SslMode::VerifyFull => Some(SslOpts::default().with_root_certs(roots())),
        }
    }
}

/// A connection, with TLS as `sslmode` says; `prefer` falls back to plain when the server has
/// no TLS.
pub(crate) async fn connect(s: &Settings) -> Result<Conn, ConnectorError> {
    let attempt = |ssl| {
        let options = s.options(ssl);
        async move {
            tokio::time::timeout(s.connect_timeout, Conn::new(options))
                .await
                .map_err(|_| {
                    ConnectorError::retryable(format!(
                        "MySQL {}: no answer within {} seconds",
                        s.label(),
                        s.connect_timeout.as_secs()
                    ))
                })?
                .map_err(|e| classify(&format!("connecting to {}", s.label()), e))
        }
    };
    match attempt(s.ssl()).await {
        Err(error) if s.sslmode == SslMode::Prefer && error.message.contains("SSL") => {
            attempt(None).await
        }
        found => found,
    }
}

/// A server error that retrying cannot fix (access, a missing table, bad settings) is fatal;
/// the rest (I/O, a lost connection) are retried.
pub(crate) fn classify(doing: &str, error: mysql_async::Error) -> ConnectorError {
    let message = format!("MySQL {doing}: {error}");
    match &error {
        mysql_async::Error::Server(server) => {
            // Access denied, unknown database or table, bad syntax, missing privilege.
            if matches!(
                server.code,
                1044 | 1045 | 1049 | 1064 | 1142 | 1146 | 1227 | 1236
            ) {
                ConnectorError::fatal(message)
            } else {
                ConnectorError::retryable(message)
            }
        }
        mysql_async::Error::Driver(driver) => {
            if matches!(driver, mysql_async::DriverError::NoClientSslFlagFromServer) {
                ConnectorError::fatal(format!("{message} (the server has no SSL)"))
            } else {
                ConnectorError::retryable(message)
            }
        }
        _ => ConnectorError::retryable(message),
    }
}

/// The server's binary log settings, and what change capture needs of them.
#[derive(Debug, Clone, Default)]
pub(crate) struct LogSettings {
    pub log_bin: bool,
    pub format: String,
    pub row_image: String,
    pub row_metadata: String,
    pub compression: bool,
    pub value_options: String,
    pub version: String,
}

impl LogSettings {
    pub async fn read(conn: &mut Conn) -> Result<LogSettings, mysql_async::Error> {
        let row: Option<(i64, String, String, String, i64, String, String)> = conn
            .query_first(
                "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, \
                 @@binlog_transaction_compression, @@binlog_row_value_options, @@version",
            )
            .await?;
        let Some((log_bin, format, image, metadata, compression, options, version)) = row else {
            return Ok(LogSettings::default());
        };
        Ok(LogSettings {
            log_bin: log_bin == 1,
            format,
            row_image: image,
            row_metadata: metadata,
            compression: compression == 1,
            value_options: options,
            version,
        })
    }

    /// Each setting change capture cannot run with, as what to set.
    pub fn problems(&self) -> Vec<String> {
        let mut found = Vec::new();
        if !self.log_bin {
            found.push("log_bin is off: start the server with the binary log on.".to_string());
        }
        let need = [
            ("binlog_format", &self.format, "ROW"),
            ("binlog_row_image", &self.row_image, "FULL"),
            ("binlog_row_metadata", &self.row_metadata, "FULL"),
        ];
        for (name, value, wanted) in need {
            if !value.eq_ignore_ascii_case(wanted) {
                found.push(format!("{name} is {value}; set it to {wanted}."));
            }
        }
        if self.compression {
            found.push(
                "binlog_transaction_compression is ON: compressed transactions hide where each \
                 change is in the log; set it to OFF."
                    .to_string(),
            );
        }
        if !self.value_options.is_empty() {
            found.push(format!(
                "binlog_row_value_options is {}: partial JSON updates hold only the change; set \
                 it to ''.",
                self.value_options
            ));
        }
        found
    }
}

/// Where the server's binary log is now: its file and offset, and with `gtid_mode=ON` the
/// GTIDs it holds, read together (`SHOW BINARY LOG STATUS`, or `SHOW MASTER STATUS` before
/// 8.2).
pub(crate) struct LogStatus {
    pub file: String,
    pub offset: u64,
    pub gtids: Option<gtid::GtidSet>,
}

pub(crate) async fn log_status(conn: &mut Conn) -> Result<LogStatus, ConnectorError> {
    let gtid_mode: Option<String> = conn
        .query_first("SELECT @@gtid_mode")
        .await
        .map_err(|e| classify("reading gtid_mode", e))?;
    let (file, offset, executed) = log_position_and_gtids(conn).await?;
    let gtids = if gtid_mode.as_deref() == Some("ON") {
        Some(
            gtid::GtidSet::parse(&executed)
                .map_err(|e| ConnectorError::fatal(format!("MySQL Executed_Gtid_Set: {e}")))?,
        )
    } else {
        None
    };
    Ok(LogStatus {
        file,
        offset,
        gtids,
    })
}

/// The server's current binary log file and offset.
pub(crate) async fn log_position(conn: &mut Conn) -> Result<(String, u64), ConnectorError> {
    let (file, offset, _) = log_position_and_gtids(conn).await?;
    Ok((file, offset))
}

async fn log_position_and_gtids(conn: &mut Conn) -> Result<(String, u64, String), ConnectorError> {
    let row: Option<mysql_async::Row> = match conn.query_first("SHOW BINARY LOG STATUS").await {
        Ok(row) => row,
        Err(mysql_async::Error::Server(server)) if server.code == 1064 => conn
            .query_first("SHOW MASTER STATUS")
            .await
            .map_err(|e| classify("reading the binary log position", e))?,
        Err(error) => return Err(classify("reading the binary log position", error)),
    };
    let row = row
        .ok_or_else(|| ConnectorError::fatal("MySQL has no binary log position: is log_bin on?"))?;
    let file: Option<String> = row.get(0);
    let position: Option<u64> = row.get(1);
    let executed: String = row
        .get::<Option<String>, _>(4)
        .flatten()
        .unwrap_or_default();
    match (file, position) {
        (Some(file), Some(position)) => Ok((file, position, executed)),
        _ => Err(ConnectorError::fatal(
            "MySQL answered SHOW BINARY LOG STATUS without a file and position.",
        )),
    }
}

/// The tables the stream reads: the named ones, or every table of the database.
pub(crate) async fn tables(
    conn: &mut Conn,
    s: &Settings,
) -> Result<Vec<(String, String)>, ConnectorError> {
    if !s.tables.is_empty() {
        return Ok(s.tables.clone());
    }
    conn.exec(
        "SELECT table_schema, table_name FROM information_schema.tables \
         WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name",
        (s.database.clone(),),
    )
    .await
    .map_err(|e| classify("listing the tables", e))
}

/// What the stream would find: the server's binary log settings, the user's privileges, the
/// tables and their keys. Only reads.
pub async fn check_source(config: &Value) -> ConnectionReport {
    let settings = match Settings::new(config) {
        Ok(settings) => settings,
        Err(error) => return ConnectionReport::failed(error.message),
    };
    let label = settings.label();
    let mut conn = match connect(&settings).await {
        Ok(conn) => conn,
        Err(error) => return ConnectionReport::failed(format!("Could not connect: {error}")),
    };
    let mut report =
        ConnectionReport::passed(format!("Connected to {label} as {}.", settings.user));
    match LogSettings::read(&mut conn).await {
        Ok(log) => {
            report = report
                .fact("version", &log.version)
                .fact("binlog_format", &log.format)
                .fact("binlog_row_image", &log.row_image)
                .fact("binlog_row_metadata", &log.row_metadata);
            for problem in log.problems() {
                report = report.fail(problem);
            }
        }
        Err(error) => {
            return report.fail(format!("Could not read the binary log settings: {error}"));
        }
    }
    match log_position(&mut conn).await {
        Ok((file, position)) => report = report.fact("binary log", format!("{file}:{position}")),
        Err(error) => {
            report = report.fail(format!(
                "{} The user needs REPLICATION CLIENT.",
                error.message
            ))
        }
    }
    let grants: Vec<String> = conn
        .query("SHOW GRANTS FOR CURRENT_USER()")
        .await
        .unwrap_or_default();
    let granted = grants.join(" ").to_ascii_uppercase();
    if !(granted.contains("REPLICATION SLAVE") || granted.contains("ALL PRIVILEGES")) {
        report = report.warn(
            "No REPLICATION SLAVE grant was found (it may come from a role): reading the binary \
             log needs it.",
        );
    }
    match tables(&mut conn, &settings).await {
        Ok(found) => {
            report = report.fact("tables", found.len());
            if found.is_empty() {
                report = report.fail(format!("{} has no tables to read.", settings.database));
            }
        }
        Err(error) => report = report.fail(error.message),
    }
    let _ = conn.disconnect().await;
    report
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The test server's settings from MAGE_TEST_MYSQL_URL (mysql://user:password@host:port/db).
    pub fn server() -> Option<Value> {
        let url = std::env::var("MAGE_TEST_MYSQL_URL").ok()?;
        let rest = url.strip_prefix("mysql://")?;
        let (credentials, rest) = rest.split_once('@')?;
        let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
        let (address, database) = rest.split_once('/')?;
        let (host, port) = address.split_once(':').unwrap_or((address, "3306"));
        Some(serde_json::json!({
            "host": host, "port": port, "user": user, "password": password,
            "database": database, "sslmode": "disable",
        }))
    }

    #[test]
    fn settings_name_tables_in_the_database_and_refuse_bad_values() {
        let settings = Settings::new(&serde_json::json!({
            "host": "db", "database": "shop", "user": "reader",
            "tables": ["orders", "audit.events"], "server_id": 77, "snapshot": "initial",
        }))
        .unwrap();
        assert_eq!(
            settings.tables,
            [
                ("shop".into(), "orders".into()),
                ("audit".into(), "events".into())
            ]
        );
        assert_eq!(
            (settings.port, settings.server_id, settings.snapshot),
            (3306, Some(77), true)
        );
        assert_eq!(settings.sslmode, SslMode::Prefer);
        for (key, value, said) in [
            ("server_id", "0", "server_id 0"),
            ("snapshot", "always", "snapshot is initial or never"),
            ("sslmode", "maybe", "sslmode is disable"),
        ] {
            let mut config = serde_json::json!({"host": "db", "database": "shop", "user": "r"});
            config[key] = value.into();
            let error = Settings::new(&config).unwrap_err();
            assert!(error.message.contains(said), "{}", error.message);
            assert!(!error.retryable);
        }
    }

    #[test]
    fn each_binary_log_setting_says_what_to_set() {
        let log = LogSettings {
            log_bin: true,
            format: "MIXED".into(),
            row_image: "MINIMAL".into(),
            row_metadata: "MINIMAL".into(),
            compression: true,
            value_options: "PARTIAL_JSON".into(),
            version: "8.4.0".into(),
        };
        let found = log.problems().join("\n");
        for expected in [
            "binlog_format is MIXED; set it to ROW",
            "binlog_row_image is MINIMAL; set it to FULL",
            "binlog_row_metadata is MINIMAL; set it to FULL",
            "binlog_transaction_compression is ON",
            "binlog_row_value_options is PARTIAL_JSON",
        ] {
            assert!(found.contains(expected), "{found}");
        }
        let ready = LogSettings {
            log_bin: true,
            format: "ROW".into(),
            row_image: "FULL".into(),
            row_metadata: "FULL".into(),
            ..Default::default()
        };
        assert!(ready.problems().is_empty());
    }

    #[tokio::test]
    async fn the_check_names_the_settings_change_capture_needs() {
        let Some(config) = server() else {
            eprintln!("MAGE_TEST_MYSQL_URL is not set: skipped");
            return;
        };
        let report = check_source(&config).await;
        let facts: std::collections::BTreeMap<_, _> = report.facts.iter().cloned().collect();
        assert_eq!(facts.get("binlog_format").map(String::as_str), Some("ROW"));
        assert!(facts.contains_key("binary log"), "{report:?}");
    }
}
