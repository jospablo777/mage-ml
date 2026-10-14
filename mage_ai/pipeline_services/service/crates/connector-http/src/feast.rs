//! Feast sink: a stream's feature rows pushed to a Feast feature server's `POST /push`, as the
//! columns Feast takes (`{"push_source_name": ..., "df": {"column": [values]}, "to": ...}`).
//!
//! Each message is one row, a JSON object; its values are passed as written, every digit kept.
//! A row that is not an object fails the batch. Pushes to the online store are upserts by
//! entity and event time, so a batch replayed after a crash writes the same values again;
//! pushes to the offline store append, so a replay can add rows twice there.

use std::collections::{BTreeMap, BTreeSet};

use mage_stream_ports::{
    BatchMeta, BoxFuture, ConnectorError, RecordBatch, Sink, SinkCapabilities, read_envelope,
};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::sink::{Client, batch_key};

pub struct FeastSink {
    client: Client,
    /// Attempts at a batch Feast answers 500 for before the stream stops.
    attempts_on_500: u32,
    push_source: String,
    to: String,
    stream: String,
}

impl FeastSink {
    pub fn new(config: &Value, stream: &str) -> Result<FeastSink, ConnectorError> {
        let push_source = config
            .get("push_source_name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ConnectorError::fatal("Feast sink settings need push_source_name."))?;
        let to = config.get("to").and_then(Value::as_str).unwrap_or("online");
        if !matches!(to, "online" | "offline" | "online_and_offline") {
            return Err(ConnectorError::fatal(
                "the Feast sink's to is online, offline or online_and_offline",
            ));
        }
        Ok(FeastSink {
            client: Client::from_config(config, "Feast sink", "/push")?,
            attempts_on_500: 3,
            push_source: push_source.to_string(),
            to: to.to_string(),
            stream: stream.to_string(),
        })
    }

    /// The batch's rows as Feast's columns; a column a row lacks is null in that row.
    pub fn body(&self, batch: &RecordBatch) -> Result<String, ConnectorError> {
        let messages = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let mut rows: Vec<BTreeMap<String, Box<RawValue>>> = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let row = serde_json::from_slice(&message.value).map_err(|_| {
                ConnectorError::fatal(format!(
                    "Feast sink: message {} of the batch is not a JSON object; each message is a \
                     feature row",
                    index + 1
                ))
            })?;
            rows.push(row);
        }
        let columns: BTreeSet<&String> = rows.iter().flat_map(|row| row.keys()).collect();
        let df: Vec<String> = columns
            .iter()
            .map(|column| {
                let values: Vec<&str> = rows
                    .iter()
                    .map(|row| row.get(*column).map_or("null", |v| v.get()))
                    .collect();
                format!(
                    "{}:[{}]",
                    Value::String((*column).clone()),
                    values.join(",")
                )
            })
            .collect();
        Ok(format!(
            "{{\"push_source_name\":{},\"df\":{{{}}},\"to\":{}}}",
            Value::String(self.push_source.clone()),
            df.join(","),
            Value::String(self.to.clone())
        ))
    }
}

/// The wait before another attempt: 1, 2, 4 seconds.
async fn pause_before_retry(attempt: u32) {
    tokio::time::sleep(std::time::Duration::from_secs(1 << (attempt - 1).min(3))).await;
}

impl Sink for FeastSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let body = self.body(batch)?;
            let key = batch_key(&self.stream, meta);
            // Feast answers 500 for a row it cannot write (a value of the wrong type) as well
            // as for a passing fault of its store: a 500 is tried again a few times here, then
            // stops the stream with Feast's reason rather than retrying the batch for ever.
            let mut attempt = 0;
            loop {
                attempt += 1;
                match self
                    .client
                    .send(body.clone(), "application/json", key.clone())
                    .await?
                {
                    Ok(()) => return Ok(()),
                    Err((500, _)) if attempt < self.attempts_on_500 => {
                        pause_before_retry(attempt).await;
                    }
                    Err((500, message)) => {
                        return Err(ConnectorError::fatal(format!(
                            "{message} (after {attempt} attempts; a row Feast cannot write, or \
                             its store failing: the stream stops and keeps its position)"
                        )));
                    }
                    Err((status, message)) if status == 408 || status == 429 || status >= 500 => {
                        return Err(ConnectorError::retryable(message));
                    }
                    Err((_, message)) => return Err(ConnectorError::fatal(message)),
                }
            }
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            // Online pushes are upserts by entity and event time.
            idempotent: self.to == "online",
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mage_stream_ports::{Envelope, EnvelopeBuilder};

    fn batch(values: &[&str]) -> RecordBatch {
        let mut builder = EnvelopeBuilder::new();
        for value in values {
            builder.push(&Envelope {
                value: value.as_bytes().to_vec(),
                ..Default::default()
            });
        }
        builder.finish().unwrap()
    }

    fn sink(to: &str) -> FeastSink {
        FeastSink::new(
            &serde_json::json!({"url": "http://feast:6566/", "push_source_name": "orders_push",
                                "to": to}),
            "orders",
        )
        .unwrap()
    }

    #[test]
    fn rows_become_feast_columns_with_values_exact() {
        let body = sink("online")
            .body(&batch(&[
                r#"{"customer_id": 7, "amount_7d": 0.1, "event_timestamp": "2026-10-13T10:00:00Z"}"#,
                r#"{"customer_id": 123456789012345678901234567890, "event_timestamp": "2026-10-13T10:00:01Z"}"#,
            ]))
            .unwrap();

        assert_eq!(
            body,
            r#"{"push_source_name":"orders_push","df":{"amount_7d":[0.1,null],"customer_id":[7,123456789012345678901234567890],"event_timestamp":["2026-10-13T10:00:00Z","2026-10-13T10:00:01Z"]},"to":"online"}"#
        );
        assert_eq!(sink("online").client.url, "http://feast:6566/push");
    }

    #[test]
    fn a_row_that_is_not_an_object_fails_the_batch_and_settings_are_checked() {
        let error = sink("online")
            .body(&batch(&[r#"{"a": 1}"#, "[1, 2]"]))
            .unwrap_err();
        assert!(!error.retryable);
        assert!(error.message.contains("message 2"), "{error:?}");
        assert!(FeastSink::new(&serde_json::json!({"url": "http://feast"}), "s").is_err());
        assert!(
            FeastSink::new(
                &serde_json::json!({"url": "http://feast", "push_source_name": "p", "to": "both"}),
                "s"
            )
            .is_err()
        );
        assert!(sink("online").capabilities().idempotent);
        assert!(!sink("offline").capabilities().idempotent);
    }
}
