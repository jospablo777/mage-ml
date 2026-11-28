//! The sink's mirror mode (ADR-0049): a table kept as its source is, from change capture.
//!
//! ```yaml
//! table: "{_mage_table}"      # each change into the table of its source table
//! mirror:
//!   key: [id]                 # or by table: {orders: [order_id], customers: [id]}
//!   deletes: delete
//! ```
//!
//! The table is a ReplacingMergeTree on the key, versioned by `_mage_version` (the change's
//! commit time in microseconds; 0 for copied rows) with `_mage_is_deleted`: ClickHouse keeps
//! each key's latest change and drops a deleted key, at merges and in `SELECT ... FINAL`. Of a
//! key's changes in one batch, the last is written. The change capture's own fields
//! (`_mage_operation`, `_mage_schema`, `_mage_table`, `_mage_deleted_at`) are not columns.

use std::collections::{BTreeMap, HashMap};

use mage_stream_ports::ConnectorError;
use serde_json::Value;
use serde_json::value::RawValue;

pub(crate) const VERSION: &str = "_mage_version";
pub(crate) const DELETED: &str = "_mage_is_deleted";
const OPERATION: &str = "_mage_operation";
const COMMIT_TIME: &str = "_mage_commit_time";
const NOT_COLUMNS: [&str; 4] = [OPERATION, "_mage_schema", "_mage_table", "_mage_deleted_at"];

pub(crate) type Change = BTreeMap<String, Box<RawValue>>;

#[derive(Debug, Clone, PartialEq)]
enum Keys {
    All(Vec<String>),
    ByTable(BTreeMap<String, Vec<String>>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mirror {
    keys: Keys,
}

fn names(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::String(one) if !one.is_empty() => Some(vec![one.clone()]),
        Value::Array(items) if !items.is_empty() => items
            .iter()
            .map(|v| v.as_str().filter(|s| !s.is_empty()).map(str::to_string))
            .collect(),
        _ => None,
    }
}

impl Mirror {
    pub fn from_config(config: &Value) -> Result<Option<Mirror>, ConnectorError> {
        let Some(mirror) = config.get("mirror").filter(|m| !m.is_null()) else {
            return Ok(None);
        };
        let bad = |what: &str| {
            ConnectorError::fatal(format!(
                "ClickHouse mirror: {what}; mirror is {{key: [id], deletes: delete}}, or key by \
                 table: {{orders: [order_id]}}."
            ))
        };
        let object = mirror.as_object().ok_or_else(|| bad("not a mapping"))?;
        let keys = match object.get("key") {
            Some(Value::Object(tables)) => Keys::ByTable(
                tables
                    .iter()
                    .map(|(table, key)| {
                        names(key)
                            .map(|k| (table.clone(), k))
                            .ok_or_else(|| bad(&format!("the key of {table} is not columns")))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Some(other) => Keys::All(names(other).ok_or_else(|| bad("key is not columns"))?),
            None => return Err(bad("it needs key")),
        };
        match object
            .get("deletes")
            .and_then(Value::as_str)
            .unwrap_or("delete")
        {
            "delete" => {}
            "soft" => {
                return Err(ConnectorError::fatal(
                    "ClickHouse mirror: deletes: soft is not supported. ClickHouse replaces \
                     whole rows, and a PostgreSQL delete carries only the key: the row's other \
                     columns would be lost. Use deletes: delete.",
                ));
            }
            other => return Err(bad(&format!("deletes is delete, not {other}"))),
        }
        Ok(Some(Mirror { keys }))
    }

    /// The key of the rows of `table` (the source table's name).
    pub fn key_of(&self, table: &str) -> Result<&[String], ConnectorError> {
        match &self.keys {
            Keys::All(key) => Ok(key),
            Keys::ByTable(tables) => tables.get(table).map(Vec::as_slice).ok_or_else(|| {
                ConnectorError::fatal(format!(
                    "ClickHouse mirror: changes of {table} came, and mirror.key names no key \
                     for it"
                ))
            }),
        }
    }
}

fn field(change: &Change, name: &str) -> String {
    change
        .get(name)
        .and_then(|v| serde_json::from_str::<String>(v.get()).ok())
        .unwrap_or_default()
}

/// `table` with `{_mage_table}` and `{_mage_schema}` filled in from a change; None when it
/// names neither.
pub(crate) fn routed(table: &str, change: &Change) -> Option<String> {
    if !table.contains('{') {
        return None;
    }
    Some(
        table
            .replace("{_mage_table}", &field(change, "_mage_table"))
            .replace("{_mage_schema}", &field(change, "_mage_schema")),
    )
}

/// The source table a change came from.
pub(crate) fn source_table(change: &Change) -> String {
    field(change, "_mage_table")
}

/// A change's version: its commit time in microseconds (ISO 8601 text, or Unix
/// milliseconds), 0 without one (copied rows).
fn version(change: &Change) -> u64 {
    let Some(raw) = change.get(COMMIT_TIME) else {
        return 0;
    };
    match serde_json::from_str::<Value>(raw.get()) {
        Ok(Value::String(text)) => chrono::DateTime::parse_from_rfc3339(&text)
            .map(|t| t.timestamp_micros().max(0) as u64)
            .unwrap_or(0),
        Ok(Value::Number(millis)) => millis.as_u64().map_or(0, |m| m.saturating_mul(1000)),
        _ => 0,
    }
}

fn raw(text: &str) -> Box<RawValue> {
    RawValue::from_string(text.to_string()).expect("valid JSON")
}

/// The rows a batch's changes write: per key its last change, with `_mage_version` and
/// `_mage_is_deleted`, without the change capture's own fields.
pub(crate) fn rows(changes: Vec<Change>, key: &[String]) -> Result<Vec<Change>, ConnectorError> {
    let mut out: Vec<Change> = Vec::with_capacity(changes.len());
    let mut by_key: HashMap<Vec<String>, usize> = HashMap::new();
    for mut change in changes {
        let deleted = field(&change, OPERATION) == "delete";
        let version = version(&change);
        let mut identity = Vec::with_capacity(key.len());
        for column in key {
            let value = change.get(column).map(|v| v.get().to_string());
            match value {
                Some(value) if value != "null" => identity.push(value),
                _ => {
                    return Err(ConnectorError::fatal(format!(
                        "ClickHouse mirror: a change has no value for its key column {column}"
                    )));
                }
            }
        }
        for name in NOT_COLUMNS {
            change.remove(name);
        }
        change.insert(VERSION.into(), raw(&version.to_string()));
        change.insert(DELETED.into(), raw(if deleted { "1" } else { "0" }));
        match by_key.get(&identity) {
            Some(index) => out[*index] = change,
            None => {
                by_key.insert(identity, out.len());
                out.push(change);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn change(value: Value) -> Change {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn settings_take_a_key_and_refuse_soft_deletes() {
        let mirror = Mirror::from_config(&json!({"mirror": {"key": {"orders": ["id"]}}}))
            .unwrap()
            .unwrap();
        assert_eq!(mirror.key_of("orders").unwrap(), ["id"]);
        assert!(
            mirror
                .key_of("lines")
                .unwrap_err()
                .message
                .contains("names no key")
        );
        let soft = Mirror::from_config(&json!({"mirror": {"key": "id", "deletes": "soft"}}));
        assert!(
            soft.unwrap_err()
                .message
                .contains("deletes: soft is not supported")
        );
        assert_eq!(Mirror::from_config(&json!({})).unwrap(), None);
    }

    #[test]
    fn each_key_keeps_its_last_change_with_its_version() {
        let changes = vec![
            change(
                json!({"id": 1, "item": "tea", "_mage_operation": "snapshot",
                          "_mage_table": "orders"}),
            ),
            change(
                json!({"id": 1, "item": "green tea", "_mage_operation": "update",
                          "_mage_commit_time": "2026-11-28T10:00:00.000001+00:00"}),
            ),
            change(json!({"id": 2, "item": "cake", "_mage_operation": "delete",
                          "_mage_commit_time": 1795860000000u64,
                          "_mage_deleted_at": "2026-11-28T10:00:00+00:00"})),
        ];
        let rows = rows(changes, &["id".to_string()]).unwrap();
        let rows: Vec<Value> = rows
            .iter()
            .map(|r| serde_json::from_str(&serde_json::to_string(r).unwrap()).unwrap())
            .collect();
        assert_eq!(
            rows,
            [
                json!({"id": 1, "item": "green tea", "_mage_commit_time":
                       "2026-11-28T10:00:00.000001+00:00", "_mage_version": 1795860000000001u64,
                       "_mage_is_deleted": 0}),
                json!({"id": 2, "item": "cake", "_mage_commit_time": 1795860000000u64,
                       "_mage_version": 1795860000000000u64, "_mage_is_deleted": 1}),
            ]
        );
        let missing = super::rows(vec![change(json!({"item": "x"}))], &["id".to_string()]);
        assert!(
            missing
                .unwrap_err()
                .message
                .contains("no value for its key column id")
        );
    }
}
