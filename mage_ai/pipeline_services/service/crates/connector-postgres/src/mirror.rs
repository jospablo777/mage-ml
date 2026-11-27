//! The sink's mirror mode (ADR-0045): a table kept as its source is, from change capture.
//!
//! ```yaml
//! table: "{_mage_table}"      # each change into the table of its source table
//! mirror:
//!   key: [id]                 # or by table: {orders: [order_id], customers: [id]}
//!   deletes: delete           # or soft: the row stays, with _mage_deleted_at
//! ```
//!
//! Rows upsert by the key; of a key's changes in one batch, the last wins. A delete
//! (`_mage_operation: delete`) deletes the row, or with `deletes: soft` sets its
//! `_mage_deleted_at` to the change's commit time; a later insert of the key clears it. The
//! change capture's own fields (`_mage_operation`, `_mage_schema`, `_mage_table`,
//! `_mage_deleted_at`) are not columns; `_mage_lsn` and `_mage_commit_time` are, so each
//! row says which change wrote it. A table the sink makes gets a unique key; an existing
//! one needs a unique index on it.

use std::collections::BTreeMap;

use mage_stream_ports::ConnectorError;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio_postgres::Client;

use crate::{PgSink, classify, quote};

pub const OPERATION: &str = "_mage_operation";
pub const DELETED_AT: &str = "_mage_deleted_at";
/// Fields of a change that are not columns of the mirror.
pub const NOT_COLUMNS: [&str; 4] = [OPERATION, "_mage_schema", "_mage_table", DELETED_AT];
pub const COMMIT_TIME: &str = "_mage_commit_time";

#[derive(Debug, Clone, PartialEq)]
pub enum Keys {
    All(Vec<String>),
    ByTable(BTreeMap<String, Vec<String>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Mirror {
    pub keys: Keys,
    pub soft: bool,
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
                "PostgreSQL mirror: {what}; mirror is {{key: [id], deletes: delete}}, or key \
                 by table: {{orders: [order_id]}}."
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
        let soft = match object
            .get("deletes")
            .and_then(Value::as_str)
            .unwrap_or("delete")
        {
            "delete" => false,
            "soft" => true,
            other => return Err(bad(&format!("deletes is delete or soft, not {other}"))),
        };
        Ok(Some(Mirror { keys, soft }))
    }

    /// The key of the rows of `table`.
    pub fn key_of(&self, table: &str) -> Result<&[String], ConnectorError> {
        match &self.keys {
            Keys::All(key) => Ok(key),
            Keys::ByTable(tables) => tables.get(table).map(Vec::as_slice).ok_or_else(|| {
                ConnectorError::fatal(format!(
                    "PostgreSQL mirror: changes of {table} came, and mirror.key names no key \
                     for it"
                ))
            }),
        }
    }
}

/// `table` with `{_mage_table}` and `{_mage_schema}` filled in from a change; None when it
/// names neither.
pub fn routed(table: &str, change: &BTreeMap<String, Box<RawValue>>) -> Option<String> {
    if !table.contains('{') {
        return None;
    }
    let field = |name: &str| {
        change
            .get(name)
            .and_then(|v| serde_json::from_str::<String>(v.get()).ok())
            .unwrap_or_default()
    };
    Some(
        table
            .replace("{_mage_table}", &field("_mage_table"))
            .replace("{_mage_schema}", &field("_mage_schema")),
    )
}

impl PgSink {
    /// The batch's changes applied to the table, in the transaction open on `client`.
    /// `columns` are the table's after the batch's fields were added; `ops` each row's
    /// operation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn write_mirror(
        &self,
        client: &Client,
        mirror: &Mirror,
        columns: &BTreeMap<String, String>,
        keys: &BTreeMap<String, String>,
        rows: &[BTreeMap<String, Box<RawValue>>],
        values: &[Vec<u8>],
        ops: &[String],
    ) -> Result<(), ConnectorError> {
        let settings = &self.settings;
        let key: Vec<String> = mirror
            .key_of(settings.source_table.as_deref().unwrap_or(&settings.table))?
            .iter()
            .map(|k| settings.column(k))
            .collect();
        for column in &key {
            if !keys.contains_key(column) {
                return Err(ConnectorError::fatal(format!(
                    "PostgreSQL mirror: the changes of {} have no {column}, a column of the key",
                    settings.table
                )));
            }
        }
        if mirror.soft && !columns.contains_key(DELETED_AT) {
            client
                .batch_execute(&format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} timestamptz",
                    settings.full_table(),
                    quote(DELETED_AT)
                ))
                .await
                .map_err(|e| classify("adding _mage_deleted_at", e))?;
        }
        let names: Vec<&String> = keys.keys().collect();
        let types: Vec<&str> = names
            .iter()
            .map(|c| columns.get(*c).map_or("text", String::as_str))
            .collect();
        crate::history::stage(
            client,
            settings,
            &names,
            &types,
            keys,
            rows,
            values,
            Some(ops),
        )
        .await?;
        let table = settings.full_table();
        let key_list = key.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ");
        let cols: Vec<String> = names.iter().map(|c| quote(c)).collect();
        let updates: Vec<String> = cols
            .iter()
            .filter(|c| !key.iter().any(|k| quote(k) == **c))
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .chain(mirror.soft.then(|| format!("{} = NULL", quote(DELETED_AT))))
            .collect();
        let conflict = if updates.is_empty() {
            "DO NOTHING".to_string()
        } else {
            format!("DO UPDATE SET {}", updates.join(", "))
        };
        let last = format!(
            "(SELECT DISTINCT ON ({key_list}) * FROM mage_stage ORDER BY {key_list}, \
             \"__mage_row\" DESC)"
        );
        let matches = key
            .iter()
            .map(|k| format!("t.{q} = d.{q}", q = quote(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let deletes = if mirror.soft {
            let time = if keys.contains_key(COMMIT_TIME) {
                format!("d.{}::timestamptz", quote(COMMIT_TIME))
            } else {
                "now()".to_string()
            };
            format!(
                "UPDATE {table} AS t SET {del} = {time} FROM {last} AS d \
                 WHERE d.\"__mage_op\" = 'delete' AND {matches}",
                del = quote(DELETED_AT),
            )
        } else {
            format!(
                "DELETE FROM {table} AS t USING {last} AS d \
                 WHERE d.\"__mage_op\" = 'delete' AND {matches}"
            )
        };
        client
            .batch_execute(&format!(
                "{deletes}; INSERT INTO {table} ({cols}) SELECT {cols} FROM {last} AS d \
                 WHERE coalesce(d.\"__mage_op\", '') <> 'delete' ON CONFLICT ({key_list}) \
                 {conflict}; DROP TABLE mage_stage",
                cols = cols.join(", "),
            ))
            .await
            .map_err(|e| {
                let error = classify(&format!("mirroring into {}", settings.label()), e);
                if error.message.contains("no unique or exclusion constraint") {
                    ConnectorError::fatal(format!(
                        "{}; mirror upserts by {key_list}: give {} a unique index on it",
                        error.message, settings.table
                    ))
                } else {
                    error
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::history::tests::{batch, server};
    use mage_stream_ports::Sink;

    fn change(table: &str, id: u32, name: &str, op: &str, lsn: u32) -> String {
        format!(
            r#"{{"_mage_table": "{table}", "_mage_schema": "shop", "id": {id}, "name": "{name}", "_mage_operation": "{op}", "_mage_lsn": {lsn}, "_mage_commit_time": "2026-11-0{lsn}T00:00:00Z"}}"#
        )
    }

    async fn table(client: &tokio_postgres::Client, name: &str) -> Vec<(i64, String, bool)> {
        let rows = client
            .query(
                &format!(
                    "SELECT id, _name, to_jsonb(t) ? '_mage_deleted_at' \
                     AND (to_jsonb(t) ->> '_mage_deleted_at') IS NOT NULL \
                     FROM {name} AS t ORDER BY id"
                ),
                &[],
            )
            .await
            .unwrap();
        rows.iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect()
    }

    #[tokio::test]
    async fn changes_upsert_by_key_deletes_delete_and_each_source_table_has_its_own() {
        let Some(mut config) = server() else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        let prefix = format!("mirror_{}", std::process::id());
        config["table"] = format!("{prefix}_{{_mage_table}}").into();
        // Keys by source table, with the copies named apart from it.
        config["mirror"] =
            serde_json::json!({"key": {"orders": ["id"], "customers": ["id"], "plain": ["id"]}});
        let sink = PgSink::new(&config).unwrap();
        let first = [
            change("orders", 1, "a", "insert", 1),
            change("orders", 2, "b", "insert", 1),
            change("orders", 2, "b2", "update", 2),
            change("customers", 7, "ann", "insert", 1),
            change("orders", 3, "c", "insert", 2),
            change("orders", 3, "", "delete", 3),
        ];
        let (b, m) = batch(&first.iter().map(String::as_str).collect::<Vec<_>>());
        sink.write(&b, &m).await.unwrap();
        // A replay changes nothing; then 1 is deleted.
        sink.write(&b, &m).await.unwrap();
        let (b, m) = batch(&[change("orders", 1, "", "delete", 4).as_str()]);
        sink.write(&b, &m).await.unwrap();

        let client = crate::connect(&sink.settings).await.unwrap();
        assert_eq!(
            table(&client, &format!("{prefix}_orders")).await,
            vec![(2, "b2".to_string(), false)]
        );
        assert_eq!(
            table(&client, &format!("{prefix}_customers")).await,
            vec![(7, "ann".to_string(), false)]
        );
        // The change's own fields are no columns; its position and time are.
        let columns: Vec<String> = client
            .query(
                "SELECT column_name::text FROM information_schema.columns \
                 WHERE table_name = $1 ORDER BY ordinal_position",
                &[&format!("{prefix}_orders")],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        // `name` is an SQL word: the column is `_name`, as MageML names it.
        assert_eq!(columns, ["_mage_commit_time", "_mage_lsn", "_name", "id"]);

        // Soft deletes keep the row, marked; inserting the key again clears the mark.
        config["mirror"] = serde_json::json!({"key": "id", "deletes": "soft"});
        let soft = PgSink::new(&config).unwrap();
        let (b, m) = batch(&[change("customers", 7, "", "delete", 5).as_str()]);
        soft.write(&b, &m).await.unwrap();
        assert_eq!(
            table(&client, &format!("{prefix}_customers")).await,
            vec![(7, "ann".to_string(), true)]
        );
        let (b, m) = batch(&[change("customers", 7, "ann2", "insert", 6).as_str()]);
        soft.write(&b, &m).await.unwrap();
        assert_eq!(
            table(&client, &format!("{prefix}_customers")).await,
            vec![(7, "ann2".to_string(), false)]
        );

        // An existing table without a unique key says what it needs.
        client
            .batch_execute(&format!(
                "CREATE TABLE {prefix}_plain (id BIGINT, _name TEXT, _mage_lsn BIGINT, \
                 _mage_commit_time TEXT)"
            ))
            .await
            .unwrap();
        let (b, m) = batch(&[change("plain", 1, "x", "insert", 1).as_str()]);
        let error = sink.write(&b, &m).await.err().unwrap();
        assert!(
            error.message.contains("give plain a unique index")
                || error.message.contains("unique index on it"),
            "{}",
            error.message
        );
        client
            .batch_execute(&format!(
                "DROP TABLE {prefix}_orders, {prefix}_customers, {prefix}_plain"
            ))
            .await
            .unwrap();
    }

    #[test]
    fn mirror_settings_name_keys_and_deletes() {
        let read = |v: Value| Mirror::from_config(&serde_json::json!({"mirror": v}));
        assert_eq!(
            read(serde_json::json!({"key": "id"})).unwrap(),
            Some(Mirror {
                keys: Keys::All(vec!["id".into()]),
                soft: false
            })
        );
        let by_table = read(serde_json::json!({
            "key": {"orders": ["order_id"], "lines": ["order_id", "line"]}, "deletes": "soft",
        }))
        .unwrap()
        .unwrap();
        assert!(by_table.soft);
        assert_eq!(by_table.key_of("lines").unwrap(), ["order_id", "line"]);
        assert!(by_table.key_of("other").is_err());
        for bad in [
            serde_json::json!({}),
            serde_json::json!({"key": []}),
            serde_json::json!({"key": "id", "deletes": "ignore"}),
        ] {
            assert!(read(bad).is_err());
        }
    }

    #[test]
    fn a_template_routes_each_change_to_its_source_table() {
        let change: BTreeMap<String, Box<RawValue>> =
            serde_json::from_str(r#"{"_mage_table": "orders", "_mage_schema": "shop", "id": 1}"#)
                .unwrap();
        assert_eq!(
            routed("{_mage_schema}_{_mage_table}", &change).as_deref(),
            Some("shop_orders")
        );
        assert_eq!(routed("orders_copy", &change), None);
    }
}
