//! A stream's lookup block as a [`TransformSession`]: each message gets the row of a table
//! that matches its key, read in one query per batch for the keys the cache does not hold,
//! and cached for `cache_seconds`.
//!
//! ```yaml
//! lookup:
//!   connection: {host: ..., database: ..., username: ..., password: ...}
//!   query: SELECT id, name, category FROM products WHERE id::text = ANY($1)
//!   key: product_id        # the message field whose value is looked up
//!   match: id              # the column of the query's rows that holds the key
//!   into: product          # the field the row goes into; without it the row's columns merge
//!   cache_seconds: 300     # how long a row (or its absence) is kept; 0 asks every batch
//!   cache_rows: 100000     # the most keys kept
//!   missing: keep          # keep (the field null), drop, or dead_letter
//! ```
//!
//! A key the query has no row for is cached too, so a batch does not ask for it again until
//! it expires. Merged columns never replace a field the message has. Messages and rows stay
//! JSON text field by field, so numbers keep every digit (a value is never read into a
//! float). A query that fails
//! because the database is unreachable is retried until it answers, as sinks are, so valid
//! messages never go to dead letters because the database is down; a query the database
//! refuses (a wrong column) stops the batch as a block error.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use mage_stream_ports::{
    BoxFuture, ConnectorError, Envelope, EnvelopeBuilder, RecordBatch, RowError, TransformError,
    TransformOutput, TransformSession, read_envelope,
};
use serde::Deserialize;
use serde_json::Value;
use serde_json::value::RawValue;

/// A JSON object field by field, each value its JSON text.
type Object = BTreeMap<String, Box<RawValue>>;
/// A row, as the JSON text the database made.
type Row = Box<RawValue>;

/// What a lookup reads its rows from: the keys' matching rows, by key.
pub trait Fetch: Send + Sync {
    fn rows<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<HashMap<String, Row>, ConnectorError>>;
}

impl Fetch for mage_connector_postgres::lookup::PgLookup {
    fn rows<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<HashMap<String, Row>, ConnectorError>> {
        Box::pin(mage_connector_postgres::lookup::PgLookup::rows(self, keys))
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Missing {
    /// The message goes on, with `into` null.
    #[default]
    Keep,
    Drop,
    /// The message goes to the dead-letter sink, saying which key had no row.
    DeadLetter,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupSpec {
    /// PostgreSQL connection settings, as a PostgreSQL sink's.
    pub connection: Value,
    pub query: String,
    pub key: String,
    #[serde(rename = "match")]
    pub match_column: String,
    #[serde(default)]
    pub into: Option<String>,
    #[serde(default = "default_cache_seconds")]
    pub cache_seconds: f64,
    #[serde(default = "default_cache_rows")]
    pub cache_rows: usize,
    #[serde(default)]
    pub missing: Missing,
}

fn default_cache_seconds() -> f64 {
    300.0
}

fn default_cache_rows() -> usize {
    100_000
}

/// Keys asked in one query, at most.
const KEYS_PER_QUERY: usize = 5000;
const RETRY_FIRST: Duration = Duration::from_millis(500);
const RETRY_MOST: Duration = Duration::from_secs(30);

struct Cached {
    at: Instant,
    row: Option<Row>,
}

/// A message as read: its envelope, its JSON object (None when it is not one) and its key.
type Parsed = (Envelope, Option<Object>, Option<String>);

pub struct LookupSession {
    block: String,
    spec: LookupSpec,
    fetch: Box<dyn Fetch>,
    log: Option<crate::log::RunLog>,
    cache: HashMap<String, Cached>,
    /// Keys in the order they were cached, for evicting the oldest past `cache_rows`.
    order: VecDeque<String>,
    /// Queries run and keys asked, for the tests and the status.
    pub queries: u64,
    pub keys_asked: u64,
}

impl LookupSession {
    pub fn new(
        block: &str,
        spec: LookupSpec,
        fetch: Box<dyn Fetch>,
        log: Option<crate::log::RunLog>,
    ) -> Result<LookupSession, String> {
        if spec.key.is_empty() || spec.match_column.is_empty() {
            return Err("lookup needs key (the message field) and match (the row's column)".into());
        }
        if spec.cache_seconds.is_nan() || spec.cache_seconds < 0.0 {
            return Err("lookup cache_seconds is 0 or more".into());
        }
        Ok(LookupSession {
            block: block.to_string(),
            spec,
            fetch,
            log,
            cache: HashMap::new(),
            order: VecDeque::new(),
            queries: 0,
            keys_asked: 0,
        })
    }

    fn fresh(&self, key: &str, now: Instant) -> bool {
        let ttl = Duration::from_secs_f64(self.spec.cache_seconds);
        self.cache
            .get(key)
            .is_some_and(|c| !ttl.is_zero() && now.duration_since(c.at) < ttl)
    }

    /// Rows for the keys, from the database; a retryable failure is retried until it
    /// answers.
    async fn ask(&mut self, keys: &[String]) -> Result<HashMap<String, Row>, TransformError> {
        let mut wait = RETRY_FIRST;
        let mut attempt = 0u32;
        loop {
            self.queries += 1;
            match self.fetch.rows(keys).await {
                Ok(rows) => return Ok(rows),
                Err(error) if error.retryable => {
                    attempt += 1;
                    if let Some(log) = &self.log {
                        log.write(
                            "warn",
                            Some(&self.block),
                            None,
                            &format!(
                                "the lookup query failed (attempt {attempt}), retrying in {:.1} s: \
                                 {}",
                                wait.as_secs_f64(),
                                error.message
                            ),
                        );
                    }
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(RETRY_MOST);
                }
                Err(error) => {
                    return Err(TransformError::User(format!(
                        "{}: the lookup query failed: {}",
                        self.block, error.message
                    )));
                }
            }
        }
    }

    fn remember(&mut self, key: String, row: Option<Row>, now: Instant) {
        if self.spec.cache_seconds == 0.0 {
            return;
        }
        if self
            .cache
            .insert(key.clone(), Cached { at: now, row })
            .is_none()
        {
            self.order.push_back(key);
        }
        while self.cache.len() > self.spec.cache_rows {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.cache.remove(&oldest);
                }
                None => break,
            }
        }
    }

    async fn apply(&mut self, batch: &RecordBatch) -> Result<TransformOutput, TransformError> {
        let envelopes =
            read_envelope(batch).map_err(|e| TransformError::Protocol(e.to_string()))?;
        let mut messages: Vec<Parsed> = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            let object = serde_json::from_slice::<Object>(&envelope.value).ok();
            let key = object
                .as_ref()
                .and_then(|o| o.get(&self.spec.key))
                .and_then(|v| mage_connector_postgres::lookup::key_text(v));
            messages.push((envelope, object, key));
        }
        let now = Instant::now();
        let mut wanted: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for (_, _, key) in &messages {
            if let Some(key) = key
                && !self.fresh(key, now)
                && seen.insert(key.clone())
            {
                wanted.push(key.clone());
            }
        }
        // Rows asked for this batch: kept for it even when the cache keeps none.
        let mut asked: HashMap<String, Option<Row>> = HashMap::new();
        for chunk in wanted.chunks(KEYS_PER_QUERY) {
            self.keys_asked += chunk.len() as u64;
            let mut rows = self.ask(chunk).await?;
            for key in chunk {
                let row = rows.remove(key);
                asked.insert(key.clone(), row.clone());
                self.remember(key.clone(), row, now);
            }
        }
        let mut builder = EnvelopeBuilder::new();
        let mut row_errors = Vec::new();
        for (index, (envelope, object, key)) in messages.into_iter().enumerate() {
            let row = key.as_ref().and_then(|k| match asked.get(k) {
                Some(row) => row.clone(),
                None => self.cache.get(k).and_then(|c| c.row.clone()),
            });
            if row.is_none() {
                match self.spec.missing {
                    Missing::Keep => {}
                    Missing::Drop => continue,
                    Missing::DeadLetter => {
                        row_errors.push(RowError {
                            row: index,
                            error: match &key {
                                Some(key) => {
                                    format!("{}: no row for {} {key}", self.block, self.spec.key)
                                }
                                None => format!(
                                    "{}: the message has no {} to look up",
                                    self.block, self.spec.key
                                ),
                            },
                        });
                        continue;
                    }
                }
            }
            let Some(mut object) = object else {
                // Not a JSON object: nothing to add the row to; it goes on as it is.
                builder.push(&envelope);
                continue;
            };
            match (&self.spec.into, row) {
                (Some(field), row) => {
                    let row = match row {
                        Some(row) => row,
                        None => RawValue::from_string("null".into())
                            .map_err(|e| TransformError::Crash(e.to_string()))?,
                    };
                    object.insert(field.clone(), row);
                }
                (None, Some(row)) => {
                    if let Ok(columns) = serde_json::from_str::<Object>(row.get()) {
                        for (column, value) in columns {
                            object.entry(column).or_insert(value);
                        }
                    }
                }
                (None, None) => {}
            }
            let value =
                serde_json::to_vec(&object).map_err(|e| TransformError::Crash(e.to_string()))?;
            builder.push(&Envelope { value, ..envelope });
        }
        let out = builder
            .finish()
            .map_err(|e| TransformError::Crash(e.to_string()))?;
        let mut output = TransformOutput {
            row_errors,
            ..TransformOutput::default()
        };
        output.outputs.insert(self.block.clone(), out);
        Ok(output)
    }
}

impl TransformSession for LookupSession {
    fn transform(
        &mut self,
        batch: RecordBatch,
    ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
        Box::pin(async move {
            let started = Instant::now();
            let rows_in = batch.num_rows() as u64;
            let mut output = self.apply(&batch).await?;
            let rows_out = output.outputs.values().map(|b| b.num_rows() as u64).sum();
            output.blocks.push(mage_stream_ports::BlockStats {
                block: self.block.clone(),
                messages_in: rows_in,
                messages_out: rows_out,
                micros: started.elapsed().as_micros() as u64,
            });
            Ok(output)
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;

    /// Products 1 and 2; counts the keys each query asks for, and fails the first `fail`
    /// queries as an unreachable database would.
    struct Products {
        asked: Arc<Mutex<Vec<Vec<String>>>>,
        fail: Arc<Mutex<u32>>,
    }

    impl Fetch for Products {
        fn rows<'a>(
            &'a self,
            keys: &'a [String],
        ) -> BoxFuture<'a, Result<HashMap<String, Row>, ConnectorError>> {
            Box::pin(async move {
                {
                    let mut fail = self.fail.lock().unwrap();
                    if *fail > 0 {
                        *fail -= 1;
                        return Err(ConnectorError::retryable("connection refused"));
                    }
                }
                self.asked.lock().unwrap().push(keys.to_vec());
                Ok(keys
                    .iter()
                    .filter(|k| *k == "1" || *k == "2")
                    .map(|k| {
                        let row = json!({"id": k.parse::<i64>().unwrap(), "name": format!("p{k}")});
                        (k.clone(), RawValue::from_string(row.to_string()).unwrap())
                    })
                    .collect())
            })
        }
    }

    fn session(spec: Value, fail: u32) -> (LookupSession, Arc<Mutex<Vec<Vec<String>>>>) {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let mut spec = spec;
        spec["connection"] = json!({});
        let session = LookupSession::new(
            "enrich",
            serde_json::from_value(spec).unwrap(),
            Box::new(Products {
                asked: asked.clone(),
                fail: Arc::new(Mutex::new(fail)),
            }),
            None,
        )
        .unwrap();
        (session, asked)
    }

    fn batch(values: &[Value]) -> RecordBatch {
        let mut builder = EnvelopeBuilder::new();
        for value in values {
            builder.push(&Envelope {
                value: serde_json::to_vec(value).unwrap(),
                ..Default::default()
            });
        }
        builder.finish().unwrap()
    }

    fn values(output: &TransformOutput) -> Vec<Value> {
        read_envelope(&output.outputs["enrich"])
            .unwrap()
            .into_iter()
            .map(|e| serde_json::from_slice(&e.value).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn rows_go_into_their_field_and_each_key_is_asked_once_while_cached() {
        let (mut lookup, asked) = session(
            json!({"query": "$1", "key": "product_id", "match": "id", "into": "product"}),
            0,
        );
        let first = lookup
            .transform(batch(&[
                json!({"product_id": 1, "qty": 2}),
                json!({"product_id": 1, "qty": 1}),
                json!({"product_id": 9}),
            ]))
            .await
            .unwrap();
        assert_eq!(
            values(&first),
            vec![
                json!({"product_id": 1, "qty": 2, "product": {"id": 1, "name": "p1"}}),
                json!({"product_id": 1, "qty": 1, "product": {"id": 1, "name": "p1"}}),
                json!({"product_id": 9, "product": null}),
            ]
        );
        lookup
            .transform(batch(&[
                json!({"product_id": 9}),
                json!({"product_id": "2"}),
            ]))
            .await
            .unwrap();
        // 1 and 9 once in the first batch; the second asks only for 2: 9 had no row, and
        // that is cached too.
        assert_eq!(
            *asked.lock().unwrap(),
            vec![vec!["1".to_string(), "9".into()], vec!["2".to_string()]]
        );
    }

    #[tokio::test]
    async fn numbers_keep_every_digit() {
        let (mut lookup, _) = session(
            json!({"query": "$1", "key": "id", "match": "id", "into": "product"}),
            0,
        );
        let mut builder = EnvelopeBuilder::new();
        builder.push(&Envelope {
            value: br#"{"id": 1, "amount": 12345678901234567890.0123456789}"#.to_vec(),
            ..Default::default()
        });
        let output = lookup.transform(builder.finish().unwrap()).await.unwrap();
        let text = String::from_utf8(
            read_envelope(&output.outputs["enrich"]).unwrap()[0]
                .value
                .clone(),
        )
        .unwrap();
        assert!(text.contains("12345678901234567890.0123456789"), "{text}");
    }

    #[tokio::test]
    async fn without_into_columns_merge_and_never_replace_the_messages_fields() {
        let (mut lookup, _) = session(json!({"query": "$1", "key": "id", "match": "id"}), 0);
        let output = lookup
            .transform(batch(&[json!({"id": 2, "name": "mine"}), json!({"id": 1})]))
            .await
            .unwrap();
        assert_eq!(
            values(&output),
            vec![
                json!({"id": 2, "name": "mine"}),
                json!({"id": 1, "name": "p1"})
            ]
        );
    }

    #[tokio::test]
    async fn missing_rows_are_dropped_or_sent_to_dead_letters() {
        let (mut drop, _) = session(
            json!({"query": "$1", "key": "id", "match": "id", "missing": "drop"}),
            0,
        );
        let output = drop
            .transform(batch(&[json!({"id": 1}), json!({"id": 7})]))
            .await
            .unwrap();
        assert_eq!(values(&output), vec![json!({"id": 1, "name": "p1"})]);

        let (mut dead, _) = session(
            json!({"query": "$1", "key": "id", "match": "id", "missing": "dead_letter"}),
            0,
        );
        let output = dead
            .transform(batch(&[
                json!({"id": 7}),
                json!({"other": 1}),
                json!({"id": 2}),
            ]))
            .await
            .unwrap();
        assert_eq!(values(&output), vec![json!({"id": 2, "name": "p2"})]);
        let errors: Vec<(usize, &str)> = output
            .row_errors
            .iter()
            .map(|e| (e.row, e.error.as_str()))
            .collect();
        assert_eq!(
            errors,
            vec![
                (0, "enrich: no row for id 7"),
                (1, "enrich: the message has no id to look up")
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreachable_database_is_retried_and_cache_rows_bounds_the_cache() {
        let (mut lookup, asked) = session(
            json!({"query": "$1", "key": "id", "match": "id", "cache_rows": 1}),
            2,
        );
        let output = lookup.transform(batch(&[json!({"id": 1})])).await.unwrap();
        assert_eq!(values(&output), vec![json!({"id": 1, "name": "p1"})]);
        assert_eq!(lookup.queries, 3, "two failures, then the answer");
        lookup.transform(batch(&[json!({"id": 2})])).await.unwrap();
        // Holding one key, the cache dropped 1 for 2: 1 is asked again.
        lookup.transform(batch(&[json!({"id": 1})])).await.unwrap();
        assert_eq!(asked.lock().unwrap().len(), 3);
        assert_eq!(lookup.cache.len(), 1);
    }

    #[tokio::test]
    async fn cache_seconds_zero_asks_every_batch() {
        let (mut lookup, asked) = session(
            json!({"query": "$1", "key": "id", "match": "id", "cache_seconds": 0}),
            0,
        );
        for _ in 0..3 {
            lookup
                .transform(batch(&[json!({"id": 1}), json!({"id": 1})]))
                .await
                .unwrap();
        }
        assert_eq!(asked.lock().unwrap().len(), 3);
    }
}
