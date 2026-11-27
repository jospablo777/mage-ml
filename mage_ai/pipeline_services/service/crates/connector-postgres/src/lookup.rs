//! The rows a stream's lookup block reads from PostgreSQL (service/src/stream_lookup.rs):
//! one query per batch for the keys its cache does not hold, with `$1` the keys as a text
//! array, such as `SELECT id, name FROM products WHERE id::text = ANY($1)`.
//!
//! Each row comes back through `row_to_json`, so its values keep PostgreSQL's types and
//! digits (numeric as JSON numbers, timestamps as ISO text, json and jsonb as they are), and
//! it stays that JSON text: a number is never read into a float. A row is the match of a key
//! when its `match` column, as text, is the key; the first row of a key stands.

use std::collections::{BTreeMap, HashMap};

use mage_stream_ports::ConnectorError;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio::sync::Mutex;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Statement};

use crate::{Settings, classify, connect};

pub struct PgLookup {
    settings: Settings,
    query: String,
    match_column: String,
    client: Mutex<Option<(Client, Statement)>>,
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
        })
    }

    /// The matching row of each key that has one, by key, as the JSON object PostgreSQL made.
    pub async fn rows(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, Box<RawValue>>, ConnectorError> {
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
