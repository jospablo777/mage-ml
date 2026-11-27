//! The sink's history mode (ADR-0043): a table of each key's versions, a type 2 slowly
//! changing dimension, for temporal joins (`lookup` with `as_of`) and point-in-time queries.
//!
//! ```yaml
//! history:
//!   key: [id]                 # the columns of a row's key
//!   time: _mage_commit_time   # when a version starts: ISO 8601 text, or Unix milliseconds
//! ```
//!
//! Each message is a version of its key that starts at its time (`valid_from`) and ends where
//! the key's next version starts (`valid_to`, null while current). A change capture delete
//! (`_mage_operation: delete`) is a version marked `_mage_deleted`, which ends the one before
//! it. A unique index on the key and `valid_from` makes a replayed batch write nothing twice,
//! and `valid_to` is set again for the batch's keys, so a version that comes late lands in
//! its place. The same transaction keeps how far the history is in
//! `mageml_history_progress` (the table and the latest time written), which as-of lookups
//! wait on.

use std::collections::BTreeMap;

use mage_stream_ports::ConnectorError;
use serde_json::Value;
use serde_json::value::RawValue;
use tokio_postgres::Client;
use tokio_postgres::types::Type;

use crate::{PgSink, Settings, classify, copy_field, quote, read_expression};

pub const VALID_FROM: &str = "valid_from";
pub const VALID_TO: &str = "valid_to";
pub const DELETED: &str = "_mage_deleted";
pub const PROGRESS_TABLE: &str = "mageml_history_progress";
const OPERATION: &str = "_mage_operation";

#[derive(Debug, Clone, PartialEq)]
pub struct History {
    /// The key's fields, as the messages name them.
    pub key: Vec<String>,
    pub time: String,
}

impl History {
    pub fn from_config(config: &Value) -> Result<Option<History>, ConnectorError> {
        let Some(history) = config.get("history") else {
            return Ok(None);
        };
        if history.is_null() {
            return Ok(None);
        }
        let bad = || {
            ConnectorError::fatal(
                "PostgreSQL history: {key: [id], time: _mage_commit_time}, the columns of a \
                 row's key and the field its version starts at.",
            )
        };
        let object = history.as_object().ok_or_else(bad)?;
        let key: Vec<String> = match object.get("key") {
            Some(Value::String(one)) => vec![one.clone()],
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| v.as_str().map(str::to_string).ok_or_else(bad))
                .collect::<Result<_, _>>()?,
            _ => return Err(bad()),
        };
        if key.is_empty() {
            return Err(bad());
        }
        let time = match object.get("time") {
            None | Some(Value::Null) => "_mage_commit_time".to_string(),
            Some(Value::String(t)) if !t.is_empty() => t.clone(),
            _ => return Err(bad()),
        };
        Ok(Some(History { key, time }))
    }
}

/// The time column's value as a timestamp, by the column's type.
fn time_expression(column: &str, column_type: &str) -> String {
    let column = quote(column);
    if column_type.starts_with("timestamp") || column_type == "date" {
        format!("{column}::timestamptz")
    } else if matches!(
        column_type,
        "bigint" | "integer" | "smallint" | "numeric" | "double precision" | "real"
    ) || column_type.starts_with("numeric")
    {
        format!("to_timestamp({column}::double precision / 1000)")
    } else {
        format!("{column}::timestamptz")
    }
}

/// An index name PostgreSQL keeps (63 bytes at most).
fn index_name(table: &str) -> String {
    let mut name = format!("{table}_mageml_versions");
    if name.len() > 63 {
        let cut = (0..=63 - "_mageml_versions".len())
            .rev()
            .find(|i| table.is_char_boundary(*i))
            .unwrap_or(0);
        name = format!("{}_mageml_versions", &table[..cut]);
    }
    name
}

impl PgSink {
    /// The batch's versions, in the transaction open on `client`. `columns` are the table's
    /// after the batch's fields were added.
    pub(crate) async fn write_history(
        &self,
        client: &Client,
        history: &History,
        mut columns: BTreeMap<String, String>,
        keys: &BTreeMap<String, String>,
        rows: &[BTreeMap<String, Box<RawValue>>],
        values: &[Vec<u8>],
    ) -> Result<(), ConnectorError> {
        let settings = &self.settings;
        let key_columns: Vec<String> = history.key.iter().map(|k| settings.column(k)).collect();
        let time_column = settings.column(&history.time);
        for (field, column) in history.key.iter().zip(&key_columns) {
            if !keys.contains_key(column) {
                return Err(ConnectorError::fatal(format!(
                    "PostgreSQL history: the messages have no {field}, a column of the key"
                )));
            }
        }
        if !keys.contains_key(&time_column) {
            return Err(ConnectorError::fatal(format!(
                "PostgreSQL history: the messages have no {}, the time a version starts",
                history.time
            )));
        }
        for own in [VALID_FROM, VALID_TO, DELETED] {
            if keys.contains_key(own) {
                return Err(ConnectorError::fatal(format!(
                    "PostgreSQL history: the messages have a field {own}, which the history \
                     keeps itself; rename it"
                )));
            }
        }
        if !columns.contains_key(DELETED) {
            prepare_table(client, settings, &key_columns).await?;
            columns = PgSink::columns(client, settings).await?;
        }
        let names: Vec<&String> = keys.keys().collect();
        let types: Vec<&str> = names
            .iter()
            .map(|c| columns.get(*c).map_or("text", String::as_str))
            .collect();
        stage(client, settings, &names, &types, keys, rows, values, None).await?;

        let table = settings.full_table();
        let target: Vec<String> = names.iter().map(|c| quote(c)).collect();
        let key_list = key_columns
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        let time = time_expression(&time_column, columns[&time_column].as_str());
        let deleted = if keys.contains_key(OPERATION) {
            format!("coalesce({} = 'delete', false)", quote(OPERATION))
        } else {
            "false".to_string()
        };
        client
            .batch_execute(&format!(
                "UPDATE mage_stage SET {vf} = {time}, {del} = {deleted}",
                vf = quote(VALID_FROM),
                del = quote(DELETED),
            ))
            .await
            .map_err(|e| classify("reading the versions' times", e))?;
        let missing: i64 = client
            .query_one(
                &format!(
                    "SELECT count(*) FROM mage_stage WHERE {} IS NULL",
                    quote(VALID_FROM)
                ),
                &[],
            )
            .await
            .map_err(|e| classify("reading the versions' times", e))?
            .get(0);
        if missing > 0 {
            return Err(ConnectorError::fatal(format!(
                "PostgreSQL history: {missing} messages have no {}, the time a version starts",
                history.time
            )));
        }
        let progress = format!("{}.{}", quote(&settings.schema), quote(PROGRESS_TABLE));
        let label = format!("{}.{}", settings.schema, settings.table);
        let statement = client
            .prepare_typed(
                &format!(
                    "INSERT INTO {progress} (history_table, through, updated_at) \
                     SELECT $1, max({vf}), now() FROM mage_stage \
                     ON CONFLICT (history_table) DO UPDATE SET \
                     through = greatest({progress}.through, EXCLUDED.through), updated_at = now()",
                    vf = quote(VALID_FROM),
                ),
                &[Type::TEXT],
            )
            .await
            .map_err(|e| classify("keeping the history's progress", e))?;
        // A key's version twice in one batch: the last message wins, as it would one by
        // one. valid_to is set again for every version of the batch's keys.
        client
            .batch_execute(&format!(
                "INSERT INTO {table} ({cols}, {vf}, {del}) SELECT {cols}, {vf}, {del} FROM \
                 (SELECT DISTINCT ON ({key_list}, {vf}) * FROM mage_stage \
                 ORDER BY {key_list}, {vf}, \"__mage_row\" DESC) AS d \
                 ON CONFLICT ({key_list}, {vf}) DO NOTHING; \
                 UPDATE {table} AS t SET {vt} = n.next FROM (SELECT ctid AS id, \
                 lead({vf}) OVER (PARTITION BY {key_list} ORDER BY {vf}) AS next FROM {table} \
                 WHERE ({key_list}) IN (SELECT {key_list} FROM mage_stage)) AS n \
                 WHERE t.ctid = n.id AND t.{vt} IS DISTINCT FROM n.next",
                cols = target.join(", "),
                vf = quote(VALID_FROM),
                vt = quote(VALID_TO),
                del = quote(DELETED),
            ))
            .await
            .map_err(|e| classify(&format!("writing versions to {}", settings.label()), e))?;
        client
            .execute(&statement, &[&label])
            .await
            .map_err(|e| classify("keeping the history's progress", e))?;
        client
            .batch_execute("DROP TABLE mage_stage")
            .await
            .map_err(|e| classify("writing versions", e))?;
        Ok(())
    }

    /// The source read everything through `through` (Unix microseconds) and the history has
    /// it: its progress moves there, so lookups as of a later time do not wait for a change
    /// that is not coming. Only for histories timed by the change's commit, whose order
    /// the source's clock follows.
    pub(crate) async fn history_caught_up(
        &self,
        client: &Client,
        through: i64,
    ) -> Result<(), ConnectorError> {
        let settings = &self.settings;
        let Some(history) = &settings.history else {
            return Ok(());
        };
        if history.time != "_mage_commit_time" || settings.table.contains('{') {
            return Ok(());
        }
        let schema = quote(&settings.schema);
        let progress = format!("{schema}.{}", quote(PROGRESS_TABLE));
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema}; CREATE TABLE IF NOT EXISTS {progress} \
                 (history_table text PRIMARY KEY, through timestamptz NOT NULL, \
                 updated_at timestamptz NOT NULL)"
            ))
            .await
            .map_err(|e| classify("keeping the history's progress", e))?;
        let label = format!("{}.{}", settings.schema, settings.table);
        client
            .execute(
                &format!(
                    "INSERT INTO {progress} (history_table, through, updated_at) \
                     VALUES ($1, to_timestamp($2::float8 / 1000000), now()) \
                     ON CONFLICT (history_table) DO UPDATE SET \
                     through = greatest({progress}.through, EXCLUDED.through), updated_at = now()"
                ),
                &[&label, &(through as f64)],
            )
            .await
            .map_err(|e| classify("keeping the history's progress", e))?;
        Ok(())
    }
}

/// The version columns, the index that makes versions unique, and the progress table.
async fn prepare_table(
    client: &Client,
    settings: &Settings,
    key_columns: &[String],
) -> Result<(), ConnectorError> {
    let key_list = key_columns
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .batch_execute(&format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {vf} timestamptz, \
             ADD COLUMN IF NOT EXISTS {vt} timestamptz, \
             ADD COLUMN IF NOT EXISTS {del} boolean NOT NULL DEFAULT false; \
             CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {table} ({key_list}, {vf}); \
             CREATE TABLE IF NOT EXISTS {schema}.{progress} (history_table text PRIMARY KEY, \
             through timestamptz NOT NULL, updated_at timestamptz NOT NULL)",
            table = settings.full_table(),
            vf = quote(VALID_FROM),
            vt = quote(VALID_TO),
            del = quote(DELETED),
            index = quote(&index_name(&settings.table)),
            schema = quote(&settings.schema),
            progress = quote(PROGRESS_TABLE),
        ))
        .await
        .map_err(|e| classify(&format!("preparing the history {}", settings.label()), e))
}

/// The batch in the temporary table `mage_stage` (the table's columns and `__mage_row`), by
/// COPY, or for array columns from the batch as one JSON array. With `ops` (mirror mode),
/// each row's change operation is `__mage_op`; without (history mode), `_mage_deleted` is
/// left to the history to fill in.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn stage(
    client: &Client,
    settings: &Settings,
    names: &[&String],
    types: &[&str],
    keys: &BTreeMap<String, String>,
    rows: &[BTreeMap<String, Box<RawValue>>],
    values: &[Vec<u8>],
    ops: Option<&[String]>,
) -> Result<(), ConnectorError> {
    use futures_util::SinkExt;

    let extra = match ops {
        Some(_) => ", ADD COLUMN \"__mage_op\" TEXT".to_string(),
        None => format!(", ALTER COLUMN {} DROP NOT NULL", quote(DELETED)),
    };
    client
        .batch_execute(&format!(
            "CREATE TEMP TABLE mage_stage (LIKE {}) ON COMMIT DROP; \
             ALTER TABLE mage_stage ADD COLUMN \"__mage_row\" BIGINT{extra}",
            settings.full_table(),
        ))
        .await
        .map_err(|e| classify("staging the batch", e))?;
    let target: Vec<String> = names.iter().map(|c| quote(c)).collect();
    if types.iter().any(|t| t.ends_with("[]")) {
        let expressions: Vec<String> = names
            .iter()
            .zip(types)
            .map(|(column, column_type)| read_expression(&keys[*column], column_type))
            .collect();
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
        let statement = client
            .prepare_typed(
                &format!(
                    "INSERT INTO mage_stage ({}, \"__mage_row\"{}) SELECT {}, n{} FROM \
                     jsonb_array_elements($1::jsonb) WITH ORDINALITY AS m(e, n)",
                    target.join(", "),
                    if ops.is_some() { ", \"__mage_op\"" } else { "" },
                    expressions.join(", "),
                    if ops.is_some() {
                        ", e->>'_mage_operation'"
                    } else {
                        ""
                    },
                ),
                &[Type::TEXT],
            )
            .await
            .map_err(|e| classify("staging the batch", e))?;
        client
            .execute(&statement, &[&payload])
            .await
            .map_err(|e| classify("staging the batch", e))?;
        return Ok(());
    }
    let mut data = Vec::with_capacity(rows.len() * 64);
    for (index, row) in rows.iter().enumerate() {
        for (column, column_type) in names.iter().zip(types) {
            copy_field(
                &mut data,
                row.get(&keys[*column]).map(AsRef::as_ref),
                column_type,
            );
            data.push(b'\t');
        }
        data.extend_from_slice(index.to_string().as_bytes());
        if let Some(ops) = ops {
            data.push(b'\t');
            match ops.get(index).filter(|op| !op.is_empty()) {
                Some(op) => crate::copy_escape(&mut data, op.as_bytes()),
                None => data.extend_from_slice(b"\\N"),
            }
        }
        data.push(b'\n');
    }
    let copy = format!(
        "COPY mage_stage ({}, \"__mage_row\"{}) FROM STDIN",
        target.join(", "),
        if ops.is_some() { ", \"__mage_op\"" } else { "" },
    );
    let sink = client
        .copy_in::<_, bytes::Bytes>(copy.as_str())
        .await
        .map_err(|e| classify("staging the batch", e))?;
    futures_util::pin_mut!(sink);
    sink.send(bytes::Bytes::from(data))
        .await
        .map_err(|e| classify("staging the batch", e))?;
    sink.finish()
        .await
        .map_err(|e| classify("staging the batch", e))?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn server() -> Option<Value> {
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

    pub(crate) fn batch(
        messages: &[&str],
    ) -> (mage_stream_ports::RecordBatch, mage_stream_ports::BatchMeta) {
        use mage_stream_ports::{
            AssignmentEpoch, Envelope, EnvelopeBuilder, PartitionId, Position,
        };

        let mut builder = EnvelopeBuilder::new();
        for m in messages {
            builder.push(&Envelope {
                value: m.as_bytes().to_vec(),
                ..Default::default()
            });
        }
        let meta = mage_stream_ports::BatchMeta {
            id: mage_stream_core::BatchId {
                partition: PartitionId(0),
                epoch: AssignmentEpoch(1),
                seq: 0,
            },
            first: Position::Offset(0),
            last: Position::Offset(0),
            records: messages.len() as u32,
            bytes: 0,
        };
        (builder.finish().unwrap(), meta)
    }

    #[tokio::test]
    async fn versions_end_where_the_next_starts_deletes_close_and_replays_add_nothing() {
        use mage_stream_ports::Sink;

        let Some(mut config) = server() else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        let table = format!("history_products_{}", std::process::id());
        config["table"] = table.clone().into();
        config["history"] = serde_json::json!({"key": "id"});
        let sink = PgSink::new(&config).unwrap();
        let change = |id: u32, price: &str, op: &str, at: &str| {
            format!(
                r#"{{"id": {id}, "price": {price}, "_mage_operation": "{op}", "_mage_commit_time": "{at}"}}"#
            )
        };
        let first = [
            change(1, "10.5", "insert", "2026-11-01T00:00:00Z"),
            change(2, "3", "insert", "2026-11-01T00:00:00Z"),
            change(1, "12", "update", "2026-11-03T00:00:00Z"),
        ];
        let (b, m) = batch(&first.iter().map(String::as_str).collect::<Vec<_>>());
        sink.write(&b, &m).await.unwrap();
        // A replay of the batch, a late version of 1 (between its two), and 2 deleted.
        sink.write(&b, &m).await.unwrap();
        let later = [
            change(1, "11", "update", "2026-11-02T00:00:00Z"),
            change(2, "3", "delete", "2026-11-04T00:00:00Z"),
        ];
        let (b, m) = batch(&later.iter().map(String::as_str).collect::<Vec<_>>());
        sink.write(&b, &m).await.unwrap();

        let client = crate::connect(&sink.settings).await.unwrap();
        let rows = client
            .query(
                &format!(
                    "SELECT id, price::text, to_char(valid_from, 'MM-DD'), \
                     coalesce(to_char(valid_to, 'MM-DD'), '-'), _mage_deleted FROM {table} \
                     ORDER BY id, valid_from"
                ),
                &[],
            )
            .await
            .unwrap();
        let got: Vec<(i64, String, String, String, bool)> = rows
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
            .collect();
        let want = [
            (1, "10.5", "11-01", "11-02", false),
            (1, "11", "11-02", "11-03", false),
            (1, "12", "11-03", "-", false),
            (2, "3", "11-01", "11-04", false),
            (2, "3", "11-04", "-", true),
        ]
        .map(|(a, b, c, d, e)| (a, b.to_string(), c.to_string(), d.to_string(), e));
        assert_eq!(got, want);
        let through: String = client
            .query_one(
                "SELECT to_char(through, 'MM-DD') FROM mageml_history_progress \
                 WHERE history_table = $1",
                &[&format!("public.{table}")],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(through, "11-04");

        // Read back: current versions (2 is deleted), versions as of times, and coverage.
        let lookup = crate::lookup::PgLookup::history(&config, &table, "id").unwrap();
        let current = lookup.rows(&["1".into(), "2".into()]).await.unwrap();
        assert_eq!(current.len(), 1);
        assert!(
            current["1"].get().contains("\"price\":12"),
            "{}",
            current["1"].get()
        );
        let price = |row: &Option<Box<RawValue>>| {
            row.as_ref()
                .map(|r| serde_json::from_str::<Value>(r.get()).unwrap()["price"].to_string())
        };
        let as_of = lookup
            .rows_as_of(
                &["1", "1", "1", "2", "2", "3"].map(String::from),
                &[
                    "2026-11-02T12:00:00Z",
                    "1793534400000", // 2026-11-01T12:00:00Z in Unix milliseconds
                    "2026-10-01T00:00:00Z",
                    "2026-11-03T00:00:00Z",
                    "2026-11-05T00:00:00Z",
                    "2026-11-02T00:00:00Z",
                ]
                .map(String::from),
            )
            .await
            .unwrap();
        assert_eq!(
            as_of.iter().map(price).collect::<Vec<_>>(),
            [Some("11"), Some("10.5"), None, Some("3"), None, None].map(|p| p.map(String::from))
        );
        assert!(
            lookup
                .covers(&["2026-11-04T00:00:00Z".into()])
                .await
                .unwrap()
        );
        assert!(
            !lookup
                .covers(&["2026-11-05T00:00:00Z".into()])
                .await
                .unwrap()
        );
        // The source is caught up through 11-06 with no change since: the history is too,
        // and never goes back.
        let micros = |at: &str| {
            chrono::DateTime::parse_from_rfc3339(at)
                .unwrap()
                .timestamp_micros()
        };
        sink.caught_up(micros("2026-11-06T00:00:00Z"))
            .await
            .unwrap();
        sink.caught_up(micros("2026-11-02T00:00:00Z"))
            .await
            .unwrap();
        assert!(
            lookup
                .covers(&["2026-11-05T00:00:00Z".into()])
                .await
                .unwrap()
        );
        assert!(
            !lookup
                .covers(&["2026-11-07T00:00:00Z".into()])
                .await
                .unwrap()
        );
        // A history timed by a field of the messages, not the commit, stays where it is.
        let mut by_field = config.clone();
        by_field["history"] = serde_json::json!({"key": "id", "time": "updated_at"});
        PgSink::new(&by_field)
            .unwrap()
            .caught_up(micros("2026-11-09T00:00:00Z"))
            .await
            .unwrap();
        assert!(
            !lookup
                .covers(&["2026-11-07T00:00:00Z".into()])
                .await
                .unwrap()
        );
        client
            .batch_execute(&format!(
                "DROP TABLE {table}; DELETE FROM mageml_history_progress WHERE history_table = \
                 'public.{table}'"
            ))
            .await
            .unwrap();
    }

    #[test]
    fn history_settings_name_the_key_and_the_time() {
        let read = |v: Value| History::from_config(&serde_json::json!({"history": v}));
        assert_eq!(
            read(serde_json::json!({"key": "id"})).unwrap(),
            Some(History {
                key: vec!["id".into()],
                time: "_mage_commit_time".into()
            })
        );
        assert_eq!(
            read(serde_json::json!({"key": ["a", "b"], "time": "changed_at"}))
                .unwrap()
                .unwrap()
                .key,
            vec!["a", "b"]
        );
        for bad in [
            serde_json::json!({"time": "t"}),
            serde_json::json!({"key": []}),
            serde_json::json!("id"),
        ] {
            assert!(read(bad).is_err());
        }
        assert_eq!(History::from_config(&serde_json::json!({})).unwrap(), None);
    }

    #[test]
    fn times_read_by_the_column_type_and_index_names_fit() {
        assert_eq!(time_expression("t", "text"), "\"t\"::timestamptz");
        assert_eq!(
            time_expression("t", "bigint"),
            "to_timestamp(\"t\"::double precision / 1000)"
        );
        assert_eq!(
            time_expression("t", "timestamp with time zone"),
            "\"t\"::timestamptz"
        );
        assert_eq!(index_name("products"), "products_mageml_versions");
        assert!(index_name(&"x".repeat(80)).len() <= 63);
    }
}
