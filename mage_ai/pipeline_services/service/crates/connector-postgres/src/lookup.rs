//! The rows a stream's lookup block reads from PostgreSQL (service/src/stream_lookup.rs):
//! one query per batch for the keys its cache does not hold, with `$1` the keys as a text
//! array, such as `SELECT id, name FROM products WHERE id::text = ANY($1)`.
//!
//! Each row comes back through `row_to_json`, so its values keep PostgreSQL's types and
//! digits (numeric as JSON numbers, timestamps as ISO text, json and jsonb as they are), and
//! it stays that JSON text: a number is never read into a float. A row is the match of a key
//! when its `match` column, as text, is the key; the first row of a key stands.
//!
//! A lookup of a history table (history.rs, ADR-0043) reads each key's current version, or
//! with [`PgLookup::rows_as_of`] the version valid at each message's time; [`PgLookup::covers`]
//! says whether the history is through a batch's times.

use std::collections::{BTreeMap, HashMap};

use mage_stream_ports::ConnectorError;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::sync::Mutex;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Statement};

use crate::{Settings, classify, connect, quote};

pub struct PgLookup {
    settings: Settings,
    query: String,
    match_column: String,
    client: Mutex<Option<(Client, Statement)>>,
    /// A history table: its quoted name and its name in mageml_history_progress.
    history: Option<(String, String)>,
}

/// A message's time, ISO 8601 text or Unix milliseconds, as a timestamp.
fn time_of(text: &str) -> String {
    format!(
        "(CASE WHEN {text} ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN to_timestamp({text}::double precision \
         / 1000) ELSE {text}::timestamptz END)"
    )
}

impl PgLookup {
    /// `config`: connection settings as a PostgreSQL sink's (no table needed).
    pub fn new(
        config: &Value,
        query: &str,
        match_column: &str,
    ) -> Result<PgLookup, ConnectorError> {
        let mut config = config.clone();
        if let Some(object) = config.as_object_mut() {
            // The lookup reads a query, not a table; Settings asks for one.
            object
                .entry("table")
                .or_insert_with(|| Value::String("lookup".into()));
        }
        let settings = Settings::new(&config)?;
        crate::tls_for(&settings)?;
        if !query.contains("$1") {
            return Err(ConnectorError::fatal(
                "PostgreSQL lookup: the query must read the keys as $1, a text array, such as \
                 WHERE id::text = ANY($1).",
            ));
        }
        Ok(PgLookup {
            settings,
            query: query.to_string(),
            match_column: match_column.to_string(),
            client: Mutex::new(None),
            history: None,
        })
    }

    /// A lookup of history table `table` (`schema.table`, or a table of the connection's
    /// schema) whose `match_column` holds the key: [`PgLookup::rows`] reads current versions.
    pub fn history(
        config: &Value,
        table: &str,
        match_column: &str,
    ) -> Result<PgLookup, ConnectorError> {
        let (schema, name) = match table.split_once('.') {
            Some((schema, name)) => (schema.to_string(), name.to_string()),
            None => (
                config
                    .get("schema")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("public")
                    .to_string(),
                table.to_string(),
            ),
        };
        let full = format!("{}.{}", quote(&schema), quote(&name));
        let current = format!(
            "SELECT * FROM {full} WHERE {m}::text = ANY($1) AND {vt} IS NULL AND NOT {del}",
            m = quote(match_column),
            vt = quote(crate::history::VALID_TO),
            del = quote(crate::history::DELETED),
        );
        let mut lookup = PgLookup::new(config, &current, match_column)?;
        lookup.history = Some((full, format!("{schema}.{name}")));
        Ok(lookup)
    }

    async fn connected(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, Option<(Client, Statement)>>, ConnectorError> {
        let mut guard = self.client.lock().await;
        if guard.as_ref().is_none_or(|(c, _)| c.is_closed()) {
            let client = connect(&self.settings).await?;
            let statement = client
                .prepare_typed(
                    &format!("SELECT row_to_json(q)::text FROM ({}) AS q", self.query),
                    &[Type::TEXT_ARRAY],
                )
                .await
                .map_err(|e| classify("preparing the lookup query", e))?;
            *guard = Some((client, statement));
        }
        Ok(guard)
    }

    fn history_table(&self) -> Result<&(String, String), ConnectorError> {
        self.history
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("PostgreSQL lookup: as_of reads a history table"))
    }

    /// The version of each key valid at its time, in order; None where none was.
    pub async fn rows_as_of(
        &self,
        keys: &[String],
        times: &[String],
    ) -> Result<Vec<Option<Box<RawValue>>>, ConnectorError> {
        let (table, _) = self.history_table()?;
        let sql = format!(
            "SELECT k.n, row_to_json(h)::text FROM unnest($1::text[], $2::text[]) \
             WITH ORDINALITY AS k(key, at, n) JOIN {table} AS h ON h.{m}::text = k.key \
             AND h.{vf} <= {at} AND (h.{vt} IS NULL OR {at} < h.{vt}) AND NOT h.{del}",
            m = quote(&self.match_column),
            vf = quote(crate::history::VALID_FROM),
            vt = quote(crate::history::VALID_TO),
            del = quote(crate::history::DELETED),
            at = time_of("k.at"),
        );
        let mut guard = self.connected().await?;
        let (client, _) = guard.as_ref().expect("connected");
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        let times: Vec<&str> = times.iter().map(String::as_str).collect();
        let result = match client
            .prepare_typed(&sql, &[Type::TEXT_ARRAY, Type::TEXT_ARRAY])
            .await
        {
            Ok(statement) => client.query(&statement, &[&keys, &times]).await,
            Err(error) => Err(error),
        };
        let rows = match result {
            Ok(rows) => rows,
            Err(error) => {
                let error = classify("reading the history", error);
                if error.retryable {
                    *guard = None;
                }
                return Err(error);
            }
        };
        let mut found: Vec<Option<Box<RawValue>>> = vec![None; keys.len()];
        for row in rows {
            let n: i64 = row.get(0);
            let text: String = row.get(1);
            if let Some(slot) = found.get_mut((n - 1) as usize)
                && slot.is_none()
            {
                *slot = Some(RawValue::from_string(text).map_err(|e| {
                    ConnectorError::fatal(format!("PostgreSQL lookup: a row is not JSON: {e}"))
                })?);
            }
        }
        Ok(found)
    }

    /// Whether the history is through the latest of `times`: every change up to then is in
    /// it (mageml_history_progress).
    pub async fn covers(&self, times: &[String]) -> Result<bool, ConnectorError> {
        let (_, label) = self.history_table()?;
        let sql = format!(
            "SELECT coalesce((SELECT through FROM {schema}.{progress} WHERE history_table = $1) \
             >= (SELECT max({at}) FROM unnest($2::text[]) AS k(at)), false)",
            schema = quote(label.split('.').next().unwrap_or("public")),
            progress = quote(crate::history::PROGRESS_TABLE),
            at = time_of("k.at"),
        );
        let mut guard = self.connected().await?;
        let (client, _) = guard.as_ref().expect("connected");
        let times: Vec<&str> = times.iter().map(String::as_str).collect();
        let result = match client
            .prepare_typed(&sql, &[Type::TEXT, Type::TEXT_ARRAY])
            .await
        {
            Ok(statement) => client.query_one(&statement, &[label, &times]).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(row) => Ok(row.get(0)),
            // No progress table yet: the history has nothing.
            Err(error)
                if error.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE) =>
            {
                Ok(false)
            }
            Err(error) => {
                let error = classify("reading the history's progress", error);
                if error.retryable {
                    *guard = None;
                }
                Err(error)
            }
        }
    }

    /// The matching row of each key that has one, by key, as the JSON object PostgreSQL made.
    pub async fn rows(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, Box<RawValue>>, ConnectorError> {
        let mut guard = self.connected().await?;
        let (client, statement) = guard.as_ref().expect("connected");
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        let result = client.query(statement, &[&keys]).await;
        let rows = match result {
            Ok(rows) => rows,
            Err(error) => {
                let error = classify("running the lookup query", error);
                if error.retryable {
                    *guard = None;
                }
                return Err(error);
            }
        };
        let mut found = HashMap::with_capacity(rows.len());
        for row in rows {
            let text: String = row.get(0);
            let columns: BTreeMap<String, Box<RawValue>> =
                serde_json::from_str(&text).map_err(|e| {
                    ConnectorError::fatal(format!("PostgreSQL lookup: a row is not JSON: {e}"))
                })?;
            let Some(key) = columns.get(&self.match_column).and_then(|v| key_text(v)) else {
                return Err(ConnectorError::fatal(format!(
                    "PostgreSQL lookup: the query's rows have no {} column to match the keys \
                     with; select it, or set match to the column that holds the key.",
                    self.match_column
                )));
            };
            if let std::collections::hash_map::Entry::Vacant(slot) = found.entry(key) {
                slot.insert(RawValue::from_string(text).map_err(|e| {
                    ConnectorError::fatal(format!("PostgreSQL lookup: a row is not JSON: {e}"))
                })?);
            }
        }
        Ok(found)
    }
}

/// A value as the text a key is matched by: strings unquoted, numbers and booleans as their
/// JSON text (every digit, as written); None for null, objects and arrays.
pub fn key_text(value: &RawValue) -> Option<String> {
    let text = value.get().trim();
    match text.as_bytes().first()? {
        b'"' => serde_json::from_str::<String>(text).ok(),
        b'n' | b'{' | b'[' => None,
        _ => Some(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> Option<Value> {
        let url = std::env::var("MAGE_TEST_POSTGRES_URL").ok()?;
        let (rest, _, _) = mage_pg::split_tls(&url);
        let config: tokio_postgres::Config = rest.parse().unwrap();
        let tokio_postgres::config::Host::Tcp(host) = &config.get_hosts()[0] else {
            return None;
        };
        Some(serde_json::json!({
            "host": host,
            "port": config.get_ports().first().copied().unwrap_or(5432),
            "database": config.get_dbname().unwrap_or("postgres"),
            "username": config.get_user().unwrap_or("postgres"),
            "password": config.get_password().map(|p| String::from_utf8_lossy(p).to_string()),
            "sslmode": "disable",
        }))
    }

    #[tokio::test]
    async fn rows_keep_their_values_and_match_their_keys_as_text() {
        let Some(config) = settings() else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        let table = format!("lookup_products_{}", std::process::id());
        let client = connect(
            &Settings::new(&serde_json::json!({
                "table": "x", "host": config["host"], "port": config["port"],
                "database": config["database"], "username": config["username"],
                "password": config["password"], "sslmode": "disable",
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id BIGINT, name TEXT, \
                 price NUMERIC(30, 10), tags JSONB); INSERT INTO {table} VALUES \
                 (1, 'tea', 12345678901234567890.0123456789, '[\"hot\"]'), \
                 (2, 'cake', 3.5, NULL), (2, 'second', 0, NULL)"
            ))
            .await
            .unwrap();
        let lookup = PgLookup::new(
            &config,
            &format!("SELECT id, name, price, tags FROM {table} WHERE id::text = ANY($1)"),
            "id",
        )
        .unwrap();

        let rows = lookup
            .rows(&["1".into(), "2".into(), "9".into()])
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        // Exact numeric digits and JSON as it is.
        let tea: Value = serde_json::from_str(rows["1"].get()).unwrap();
        assert_eq!(tea["name"], "tea");
        assert!(
            rows["1"]
                .get()
                .contains("\"price\":12345678901234567890.0123456789"),
            "{}",
            rows["1"].get()
        );
        assert!(
            rows["2"].get().contains("\"cake\""),
            "the first row of a key stands"
        );
        client
            .batch_execute(&format!("DROP TABLE {table}"))
            .await
            .unwrap();

        let error = PgLookup::new(&config, "SELECT 1", "id").err().unwrap();
        assert!(error.message.contains("$1"), "{error:?}");
    }
}
