//! A stream's SQL block as a [`TransformSession`] (F11): each batch's messages are the table
//! `messages`, one row per JSON object and one column per field, and the block's query makes
//! the messages it sends on, one per row:
//!
//! ```sql
//! SELECT card, amount * 1.1 AS amount_usd, upper(country) AS country
//! FROM messages CROSS JOIN broadcast
//! WHERE amount > fraud_threshold
//! ```
//!
//! The query runs on Polars SQL in the engine, without Python. The stream's broadcast values
//! are the one-row table `broadcast`, a column per value, when the stream has any; a cross
//! join gives every message them (Polars SQL has no scalar subqueries in comparisons).
//! Mage's `{{ df_1 }}` names `messages` too. The query is stateless: each batch on its own.
//!
//! Columns follow the batch: a field no message of a batch has is not a column of it, and
//! numbers are read as 64-bit integers or floats. A message that is not a JSON object fails
//! on its own (to dead letters when the stream has a sink for them); a query that fails, such
//! as one naming a column the batch does not have, fails the batch.

use std::sync::Arc;

use mage_stream_ports::{
    BoxFuture, Envelope, EnvelopeBuilder, RecordBatch, RowError, TransformError, TransformOutput,
    TransformSession, read_envelope,
};
use polars::prelude::*;
use polars::sql::SQLContext;
use serde_json::Value;

use crate::broadcast::{Broadcast, Snapshot};

pub struct SqlSession {
    block: String,
    query: Arc<str>,
    broadcast: Arc<Broadcast>,
}

/// The query of a SQL block: Mage's `{{ df_1 }}`, its one upstream, is `messages`.
pub fn query_of(text: &str) -> Result<String, String> {
    let mut query = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        let Some(length) = rest[start..].find("}}") else {
            break;
        };
        let inner = rest[start + 2..start + length].trim();
        query.push_str(&rest[..start]);
        match inner {
            "df_1" => query.push_str("messages"),
            other => {
                return Err(format!(
                    "{{{{ {other} }}}} is not available in a stream: a stream's SQL block reads \
                     the table messages (its upstream block's messages, also {{{{ df_1 }}}}) and \
                     broadcast"
                ));
            }
        }
        rest = &rest[start + length + 2..];
    }
    query.push_str(rest);
    let query = query.trim().trim_end_matches(';').trim().to_string();
    if query.is_empty() {
        return Err("the SQL block has no query".into());
    }
    Ok(query)
}

impl SqlSession {
    pub fn new(block: &str, text: &str, broadcast: Arc<Broadcast>) -> Result<SqlSession, String> {
        Ok(SqlSession {
            block: block.to_string(),
            query: query_of(text)?.into(),
            broadcast,
        })
    }
}

/// A JSON object's fields in their order.
struct Fields(Vec<(String, Value)>);

impl<'de> serde::Deserialize<'de> for Fields {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Fields, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Fields;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Fields, A::Error> {
                let mut fields = Vec::new();
                while let Some(entry) = map.next_entry::<String, Value>()? {
                    fields.push(entry);
                }
                Ok(Fields(fields))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

/// A column's type, from its values in the batch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Null,
    Bool,
    Int,
    Float,
    Text,
    /// Mixed types, objects or arrays: each value's JSON text (a string as it is).
    Json,
}

fn kind_of(value: &Value) -> Kind {
    match value {
        Value::Null => Kind::Null,
        Value::Bool(_) => Kind::Bool,
        Value::Number(n) if n.is_i64() => Kind::Int,
        Value::Number(_) => Kind::Float,
        Value::String(_) => Kind::Text,
        Value::Array(_) | Value::Object(_) => Kind::Json,
    }
}

fn merge(a: Kind, b: Kind) -> Kind {
    match (a, b) {
        (Kind::Null, other) | (other, Kind::Null) => other,
        (a, b) if a == b => a,
        (Kind::Int, Kind::Float) | (Kind::Float, Kind::Int) => Kind::Float,
        _ => Kind::Json,
    }
}

fn column(name: &str, cells: &[Option<&Value>]) -> Column {
    let kind = cells
        .iter()
        .flatten()
        .fold(Kind::Null, |kind, value| merge(kind, kind_of(value)));
    let name = PlSmallStr::from(name);
    let series = match kind {
        Kind::Null => Series::full_null(name, cells.len(), &DataType::Null),
        Kind::Bool => Series::new(
            name,
            cells
                .iter()
                .map(|c| c.and_then(Value::as_bool))
                .collect::<Vec<_>>(),
        ),
        Kind::Int => Series::new(
            name,
            cells
                .iter()
                .map(|c| c.and_then(Value::as_i64))
                .collect::<Vec<_>>(),
        ),
        Kind::Float => Series::new(
            name,
            cells
                .iter()
                .map(|c| c.and_then(Value::as_f64))
                .collect::<Vec<_>>(),
        ),
        Kind::Text | Kind::Json => Series::new(
            name,
            cells
                .iter()
                .map(|c| match c {
                    None | Some(Value::Null) => None,
                    Some(Value::String(text)) => Some(text.clone()),
                    Some(other) => Some(other.to_string()),
                })
                .collect::<Vec<_>>(),
        ),
    };
    series.into_column()
}

/// JSON objects as a frame: a column per field, in the order fields first appear, typed by
/// its values (Boolean, Int64, Float64 or String; mixed types and nested values as their
/// JSON text). streaming/sql.py builds the editor's frames by the same rules.
fn frame(objects: &[Fields]) -> PolarsResult<DataFrame> {
    let mut names: Vec<&str> = Vec::new();
    let mut index: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for object in objects {
        for (name, _) in &object.0 {
            if !index.contains_key(name.as_str()) {
                index.insert(name, names.len());
                names.push(name);
            }
        }
    }
    let mut cells: Vec<Vec<Option<&Value>>> = vec![vec![None; objects.len()]; names.len()];
    for (row, object) in objects.iter().enumerate() {
        for (name, value) in &object.0 {
            cells[index[name.as_str()]][row] = Some(value);
        }
    }
    let columns: Vec<Column> = names
        .iter()
        .zip(&cells)
        .map(|(name, cells)| column(name, cells))
        .collect();
    DataFrame::new(objects.len(), columns)
}

fn objects(lines: &[u8]) -> Result<Vec<Fields>, String> {
    lines
        .split(|b| *b == b'\n')
        .filter(|line| !line.trim_ascii().is_empty())
        .map(|line| serde_json::from_slice::<Fields>(line).map_err(|e| e.to_string()))
        .collect()
}

/// A value as JSON text: numbers and booleans as they are, text quoted, a float that is
/// not finite as null, and other types (dates, lists) as their text.
fn write_value(out: &mut Vec<u8>, value: AnyValue) {
    let text = match value {
        AnyValue::Null => "null".to_string(),
        AnyValue::Boolean(b) => b.to_string(),
        AnyValue::Int8(n) => n.to_string(),
        AnyValue::Int16(n) => n.to_string(),
        AnyValue::Int32(n) => n.to_string(),
        AnyValue::Int64(n) => n.to_string(),
        AnyValue::UInt8(n) => n.to_string(),
        AnyValue::UInt16(n) => n.to_string(),
        AnyValue::UInt32(n) => n.to_string(),
        AnyValue::UInt64(n) => n.to_string(),
        AnyValue::Float32(f) => json_float(f64::from(f)),
        AnyValue::Float64(f) => json_float(f),
        AnyValue::String(text) => Value::from(text).to_string(),
        AnyValue::StringOwned(text) => Value::from(text.as_str()).to_string(),
        other => Value::from(other.to_string()).to_string(),
    };
    out.extend_from_slice(text.as_bytes());
}

fn json_float(f: f64) -> String {
    serde_json::Number::from_f64(f).map_or_else(|| "null".to_string(), |n| n.to_string())
}

/// A frame's rows as JSON objects, their fields in the frame's column order.
fn rows(frame: &DataFrame) -> Result<Vec<Vec<u8>>, String> {
    let names: Vec<String> = frame
        .get_column_names()
        .iter()
        .map(|name| Value::from(name.as_str()).to_string())
        .collect();
    let columns = frame.columns();
    let mut out = Vec::with_capacity(frame.height());
    for row in 0..frame.height() {
        let mut text = vec![b'{'];
        for (i, column) in columns.iter().enumerate() {
            if i > 0 {
                text.push(b',');
            }
            text.extend_from_slice(names[i].as_bytes());
            text.push(b':');
            write_value(&mut text, column.get(row).map_err(|e| e.to_string())?);
        }
        text.push(b'}');
        out.push(text);
    }
    Ok(out)
}

/// Runs the query on the batch's messages (JSON objects, one per line) and the broadcast
/// values; returns the rows as JSON objects.
pub fn run(
    query: &str,
    lines: &[u8],
    broadcast: Option<&Snapshot>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut context = SQLContext::new();
    let messages = objects(lines).map_err(|e| format!("reading the messages: {e}"))?;
    context.register(
        "messages",
        frame(&messages)
            .map_err(|e| format!("reading the messages: {e}"))?
            .lazy(),
    );
    if let Some(snapshot) = broadcast {
        let values = objects(snapshot.values.get().as_bytes())
            .map_err(|e| format!("reading the broadcast values: {e}"))?;
        context.register(
            "broadcast",
            frame(&values)
                .map_err(|e| format!("reading the broadcast values: {e}"))?
                .lazy(),
        );
    }
    let out = context
        .execute(query)
        .and_then(LazyFrame::collect)
        .map_err(|e| e.to_string())?;
    rows(&out)
}

impl TransformSession for SqlSession {
    fn transform(
        &mut self,
        batch: RecordBatch,
    ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
        Box::pin(async move {
            let envelopes =
                read_envelope(&batch).map_err(|e| TransformError::Protocol(e.to_string()))?;
            let partition = envelopes.first().map_or(0, |e| e.partition);
            let mut lines = Vec::new();
            let mut row_errors = Vec::new();
            let mut count = 0;
            for (row, envelope) in envelopes.iter().enumerate() {
                if mage_stream_ports::is_timer_tick(envelope) {
                    continue;
                }
                if let Some(error) = crate::stream_python::decode_error(envelope) {
                    row_errors.push(RowError { row, error });
                    continue;
                }
                let value = envelope.value.trim_ascii();
                if value.first() != Some(&b'{')
                    || serde_json::from_slice::<serde::de::IgnoredAny>(value).is_err()
                {
                    row_errors.push(RowError {
                        row,
                        error: format!(
                            "{}: a SQL block reads JSON objects, a row each; this message is not one",
                            self.block
                        ),
                    });
                    continue;
                }
                lines.extend_from_slice(value);
                lines.push(b'\n');
                count += 1;
            }
            let snapshot = self.broadcast.current();
            let with_values = !snapshot.versions.is_empty();
            let mut outputs = std::collections::BTreeMap::new();
            let mut messages_out = 0;
            let started = std::time::Instant::now();
            if count > 0 {
                let query = self.query.clone();
                let values = snapshot.clone();
                let rows = tokio::task::spawn_blocking(move || {
                    run(&query, &lines, with_values.then_some(values.as_ref()))
                })
                .await
                .map_err(|e| TransformError::Crash(e.to_string()))?
                .map_err(|e| TransformError::User(format!("{}: {e}", self.block)))?;
                messages_out = rows.len() as u64;
                let mut builder = EnvelopeBuilder::new();
                for row in rows {
                    builder.push(&Envelope {
                        value: row,
                        partition,
                        ..Envelope::default()
                    });
                }
                outputs.insert(
                    self.block.clone(),
                    builder
                        .finish()
                        .map_err(|e| TransformError::Protocol(e.to_string()))?,
                );
            }
            Ok(TransformOutput {
                outputs,
                row_errors,
                blocks: vec![mage_stream_ports::BlockStats {
                    block: self.block.clone(),
                    messages_in: count as u64,
                    messages_out,
                    micros: started.elapsed().as_micros() as u64,
                }],
                broadcast: with_values.then(|| snapshot.label()),
            })
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_files::StateFiles;

    fn rows(out: Vec<Vec<u8>>) -> Vec<serde_json::Value> {
        out.iter()
            .map(|row| serde_json::from_slice(row).unwrap())
            .collect()
    }

    #[test]
    fn a_query_filters_and_computes_over_the_batch() {
        let lines = b"{\"card\": \"a\", \"amount\": 50, \"country\": \"cr\"}\n\
                      {\"card\": \"b\", \"amount\": 150, \"country\": \"us\"}\n\
                      {\"card\": \"c\", \"amount\": 250.5}\n";
        let out = run(
            "SELECT card, amount * 2 AS doubled, upper(country) AS country FROM messages \
             WHERE amount > 100 ORDER BY card",
            lines,
            None,
        )
        .unwrap();
        assert_eq!(
            rows(out),
            vec![
                serde_json::json!({"card": "b", "doubled": 300.0, "country": "US"}),
                serde_json::json!({"card": "c", "doubled": 501.0, "country": null}),
            ]
        );
    }

    #[test]
    fn broadcast_values_are_a_table_and_aggregates_work() {
        let dir = tempfile::tempdir().unwrap();
        let broadcast = Broadcast::open(&StateFiles::Local(dir.path().to_path_buf())).unwrap();
        let value = serde_json::value::RawValue::from_string("100".into()).unwrap();
        broadcast.put("threshold", value, None, None).unwrap();
        let lines = b"{\"card\": \"a\", \"amount\": 50}\n{\"card\": \"a\", \"amount\": 150}\n\
                      {\"card\": \"b\", \"amount\": 120}\n";
        let out = run(
            "SELECT card, count(*) AS high FROM messages CROSS JOIN broadcast \
             WHERE amount > threshold GROUP BY card ORDER BY card",
            lines,
            Some(&broadcast.current()),
        )
        .unwrap();
        assert_eq!(
            rows(out),
            vec![
                serde_json::json!({"card": "a", "high": 1}),
                serde_json::json!({"card": "b", "high": 1}),
            ]
        );
    }

    #[test]
    fn case_union_and_subqueries_in_from_run_as_the_docs_say() {
        let lines = b"{\"card\": \"a\", \"amount\": 50}\n{\"card\": \"b\", \"amount\": 150}\n";
        let out = run(
            "SELECT card, CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS level \
             FROM (SELECT * FROM messages WHERE amount > 0) \
             UNION ALL SELECT 'z' AS card, 'none' AS level FROM messages WHERE amount > 1000",
            lines,
            None,
        )
        .unwrap();
        assert_eq!(
            rows(out),
            vec![
                serde_json::json!({"card": "a", "level": "low"}),
                serde_json::json!({"card": "b", "level": "high"}),
            ]
        );
        // What the editor and exports refuse (streaming/sql.py): the engine cannot run it.
        let error = run(
            "SELECT * FROM messages WHERE amount > (SELECT 1 AS t FROM messages)",
            lines,
            None,
        )
        .unwrap_err();
        assert!(error.contains("subquery"), "{error}");
    }

    /// The same case is in mage_ai/tests/streaming/test_sql.py: the editor builds the same
    /// frame and writes the same text.
    #[test]
    fn columns_keep_field_order_and_types_follow_the_values() {
        let lines = br#"{"id": 1, "ok": true, "score": 1.5, "tags": ["a"], "mixed": 1}
{"id": 2, "ok": null, "score": 2, "user": {"b": 1, "a": "x"}, "mixed": "two", "big": 18446744073709551615}
"#;
        let out = run("SELECT * FROM messages", lines, None).unwrap();
        let text: Vec<String> = out
            .into_iter()
            .map(|row| String::from_utf8(row).unwrap())
            .collect();
        assert_eq!(
            text,
            vec![
                r#"{"id":1,"ok":true,"score":1.5,"tags":"[\"a\"]","mixed":"1","user":null,"big":null}"#,
                r#"{"id":2,"ok":null,"score":2.0,"tags":null,"mixed":"two","user":"{\"a\":\"x\",\"b\":1}","big":1.8446744073709552e+19}"#,
            ]
        );
    }

    #[test]
    fn mages_upstream_reference_reads_messages_and_others_are_explained() {
        assert_eq!(
            query_of("SELECT * FROM {{ df_1 }};\n").unwrap(),
            "SELECT * FROM messages"
        );
        let error = query_of("SELECT * FROM {{ df_2 }}").unwrap_err();
        assert!(error.contains("df_2"), "{error}");
        assert!(query_of("  ;  ").is_err());
        let error = run("SELECT nope FROM messages", b"{\"a\": 1}\n", None).unwrap_err();
        assert!(error.contains("nope"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_message_that_is_not_an_object_fails_on_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let broadcast =
            Arc::new(Broadcast::open(&StateFiles::Local(dir.path().to_path_buf())).unwrap());
        let mut session =
            SqlSession::new("pick", "SELECT a + 1 AS b FROM messages", broadcast).unwrap();
        let mut builder = EnvelopeBuilder::new();
        for value in [&b"{\"a\": 1}"[..], b"[1, 2]", b"{\"a\": 2}", b"not json"] {
            builder.push(&Envelope {
                value: value.to_vec(),
                ..Envelope::default()
            });
        }
        let output = session.transform(builder.finish().unwrap()).await.unwrap();
        let errors: Vec<usize> = output.row_errors.iter().map(|e| e.row).collect();
        assert_eq!(errors, vec![1, 3]);
        let out = read_envelope(&output.outputs["pick"]).unwrap();
        let values: Vec<String> = out
            .iter()
            .map(|e| String::from_utf8(e.value.clone()).unwrap())
            .collect();
        assert_eq!(values, vec!["{\"b\":2}", "{\"b\":3}"]);
        assert_eq!(output.blocks[0].messages_in, 2);
        assert_eq!(output.broadcast, None);
    }
}
