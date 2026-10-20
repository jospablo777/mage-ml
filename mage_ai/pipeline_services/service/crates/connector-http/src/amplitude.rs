//! Amplitude sink: a stream's rows sent to an Amplitude project with the Batch Event Upload
//! API (`POST /batch`, up to 2,000 events and 20 MB a request), as events or, with
//! `user_properties: true`, as user properties set by `$identify` events.
//!
//! Each message is one row, a JSON object, read as MageML's Python client reads rows
//! (`mage_ai.io.amplitude.amplitude_event`): Amplitude's event fields stay, `time` (or
//! `event_time`) becomes epoch milliseconds, and every other column is an event property or,
//! for user properties, a property to `$set`. `columns` names the row's columns for those
//! fields, such as `{"user_id": "customer_id", "time": "created_at"}`.
//!
//! Every event has an `insert_id`: the row's, else a SHA-256 of `key_columns`, else of the
//! event. Amplitude drops an `insert_id` it saw in the last 7 days, so a batch sent again
//! after a crash or a retry adds nothing twice, however the replay is batched.

use mage_stream_ports::{
    BatchMeta, BoxFuture, ConnectorError, RecordBatch, Sink, SinkCapabilities, read_envelope,
};
use serde_json::{Map, Value, json};

use crate::sink::{Client, batch_key};

/// A request takes up to 2,000 events and 20 MB; requests are kept to half of that size.
const MAX_EVENTS: usize = 2000;
const MAX_BYTES: usize = 10 * 1024 * 1024;
/// Amplitude asks to wait at least 15 s after a 429 of its upload APIs.
const THROTTLE_SECONDS: u64 = 15;

/// The fields of an event Amplitude takes; any other column is an event property.
const EVENT_FIELDS: &[&str] = &[
    "adid",
    "android_app_set_id",
    "android_id",
    "app_version",
    "carrier",
    "city",
    "country",
    "device_brand",
    "device_id",
    "device_manufacturer",
    "device_model",
    "dma",
    "event_id",
    "event_properties",
    "event_type",
    "group_properties",
    "groups",
    "idfa",
    "idfv",
    "insert_id",
    "ip",
    "language",
    "location_lat",
    "location_lng",
    "os_name",
    "os_version",
    "platform",
    "price",
    "productId",
    "quantity",
    "region",
    "revenue",
    "revenueType",
    "session_id",
    "time",
    "user_id",
    "user_properties",
];

/// The fields an `$identify` event keeps (the Identify API's); any other column, revenue
/// and price among them, is a user property to set.
const IDENTIFY_FIELDS: &[&str] = &[
    "app_version",
    "carrier",
    "city",
    "country",
    "device_brand",
    "device_id",
    "device_manufacturer",
    "device_model",
    "dma",
    "groups",
    "language",
    "os_name",
    "os_version",
    "platform",
    "region",
    "user_id",
    "user_properties",
];

pub struct AmplitudeSink {
    client: Client,
    api_key: String,
    region: String,
    event_type: Option<String>,
    identify: bool,
    columns: Vec<(String, String)>,
    key_columns: Vec<String>,
    min_id_length: Option<u64>,
    stream: String,
}

impl AmplitudeSink {
    pub fn new(config: &Value, stream: &str) -> Result<AmplitudeSink, ConnectorError> {
        let text = |key: &str| {
            config
                .get(key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let api_key = text("api_key").ok_or_else(|| {
            ConnectorError::fatal(
                "Amplitude sink settings need api_key: Amplitude > Settings > Projects > your \
                 project > General.",
            )
        })?;
        let region = text("region").unwrap_or_else(|| "US".into()).to_uppercase();
        let host = match (text("url"), region.as_str()) {
            (Some(url), _) => url,
            (None, "US") => "https://api2.amplitude.com".into(),
            (None, "EU") => "https://api.eu.amplitude.com".into(),
            (None, other) => {
                return Err(ConnectorError::fatal(format!(
                    "the Amplitude sink's region is US or EU, not {other}"
                )));
            }
        };
        let mut client_config = config.clone();
        client_config["url"] = Value::String(host);
        if client_config.get("timeout_seconds").is_none() {
            client_config["timeout_seconds"] = json!(60);
        }
        let columns = config
            .get("columns")
            .and_then(Value::as_object)
            .map(|map| {
                map.iter()
                    .filter_map(|(field, column)| {
                        column.as_str().map(|c| (field.clone(), c.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let key_columns = config
            .get("key_columns")
            .and_then(Value::as_array)
            .map(|keys| {
                keys.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AmplitudeSink {
            client: Client::from_config(&client_config, "Amplitude sink", "/batch")?,
            api_key,
            region,
            event_type: text("event_type"),
            identify: config
                .get("user_properties")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            columns,
            key_columns,
            min_id_length: config.get("min_id_length").and_then(Value::as_u64),
            stream: stream.to_string(),
        })
    }

    /// The batch's rows as Amplitude events; None for a row of user properties that are
    /// all empty, which has nothing to send.
    pub fn events(&self, batch: &RecordBatch) -> Result<Vec<Value>, ConnectorError> {
        let messages = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let mut events = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let row: Map<String, Value> = serde_json::from_slice(&message.value).map_err(|_| {
                ConnectorError::fatal(format!(
                    "Amplitude sink: message {} of the batch is not a JSON object; each message \
                     is a row",
                    index + 1
                ))
            })?;
            let event = self.event(row).map_err(|reason| {
                ConnectorError::fatal(format!(
                    "Amplitude sink: message {} of the batch: {reason}",
                    index + 1
                ))
            })?;
            events.extend(event);
        }
        Ok(events)
    }

    /// One row as an event, by the rules of the Python client's amplitude_event.
    pub fn event(&self, mut row: Map<String, Value>) -> Result<Option<Value>, String> {
        let mut keys = self.key_columns.clone();
        for (field, column) in &self.columns {
            if column != field
                && let Some(value) = row.remove(column)
            {
                row.insert(field.clone(), value);
                for key in keys.iter_mut().filter(|k| *k == column) {
                    *key = field.clone();
                }
            }
        }
        row.retain(|key, value| !value.is_null() && !key.starts_with("_mage_"));
        let insert_id = row.remove("insert_id").or_else(|| row.remove("$insert_id"));
        let moment = row.remove("time").or_else(|| row.remove("event_time"));
        let mut event = Map::new();
        let fields = if self.identify {
            IDENTIFY_FIELDS
        } else {
            EVENT_FIELDS
        };
        for field in fields {
            if let Some(value) = row.remove(*field) {
                event.insert((*field).to_string(), value);
            }
        }
        for field in ["user_id", "device_id"] {
            if let Some(Value::Number(number)) = event.get(field) {
                let text = number.to_string();
                event.insert(field.to_string(), Value::String(text));
            }
        }
        let has_id = |field: &str| {
            event
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        };
        if !has_id("user_id") && !has_id("device_id") {
            return Err(
                "an Amplitude event needs user_id or device_id; set columns to name \
                        the row's id column"
                    .into(),
            );
        }
        if let Some(moment) = moment {
            event.insert("time".into(), Value::from(epoch_millis(&moment)?));
        }

        if self.identify {
            let mut operations = match event.remove("user_properties") {
                Some(Value::Object(map)) => map,
                Some(_) => return Err("user_properties must be an object".into()),
                None => Map::new(),
            };
            let mut set: Map<String, Value> = Map::new();
            operations.retain(|key, value| {
                if key.starts_with('$') {
                    true
                } else {
                    set.insert(key.clone(), value.take());
                    false
                }
            });
            set.extend(row);
            if !set.is_empty() {
                match operations.get_mut("$set") {
                    Some(Value::Object(existing)) => existing.extend(set),
                    _ => {
                        operations.insert("$set".into(), Value::Object(set));
                    }
                }
            }
            if operations.is_empty() {
                return Ok(None);
            }
            event.insert("event_type".into(), "$identify".into());
            event.insert("user_properties".into(), Value::Object(operations));
            if let Some(id) = insert_id {
                event.insert("insert_id".into(), Value::String(text_of(&id)));
            }
            return Ok(Some(Value::Object(event)));
        }

        let event_type = event
            .get("event_type")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| self.event_type.clone())
            .ok_or(
                "an Amplitude event needs an event_type: a column, or event_type in the \
                    sink's settings",
            )?;
        event.insert("event_type".into(), Value::String(event_type.clone()));
        if !row.is_empty() {
            let mut properties = match event.remove("event_properties") {
                Some(Value::Object(map)) => map,
                _ => Map::new(),
            };
            let keyed: Vec<Value> = keys
                .iter()
                .filter_map(|k| row.get(k).or_else(|| event.get(k)).cloned())
                .collect();
            properties.extend(row);
            event.insert("event_properties".into(), Value::Object(properties));
            if insert_id.is_none() && !keys.is_empty() && keyed.len() == keys.len() {
                let mut parts = vec![Value::String(event_type)];
                parts.extend(keyed);
                event.insert(
                    "insert_id".into(),
                    Value::String(sha256(&Value::Array(parts))),
                );
                return Ok(Some(Value::Object(event)));
            }
        }
        let id = match insert_id {
            Some(id) => text_of(&id),
            None => sha256(&Value::Object(event.clone())),
        };
        event.insert("insert_id".into(), Value::String(id));
        Ok(Some(Value::Object(event)))
    }

    fn body(&self, events: &[Value]) -> String {
        let mut body = json!({"api_key": self.api_key, "events": events});
        if let Some(length) = self.min_id_length {
            body["options"] = json!({"min_id_length": length});
        }
        body.to_string()
    }

    /// Sends events in one request; 413 splits it in two.
    fn send<'a>(
        &'a self,
        events: &'a [Value],
        key: String,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let (status, answer) = self
                .client
                .exchange(self.body(events), "application/json", key.clone())
                .await?;
            if (200..300).contains(&status) {
                return Ok(());
            }
            let answer: Value = serde_json::from_str(&answer).unwrap_or(Value::String(answer));
            if status == 413 && events.len() > 1 {
                let (first, second) = events.split_at(events.len() / 2);
                self.send(first, format!("{key}:a")).await?;
                return self.send(second, format!("{key}:b")).await;
            }
            Err(self.explain(status, &answer, events).await)
        })
    }

    async fn explain(&self, status: u16, answer: &Value, events: &[Value]) -> ConnectorError {
        let error = answer
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                answer
                    .as_str()
                    .unwrap_or_default()
                    .chars()
                    .take(300)
                    .collect()
            });
        let lower = error.to_lowercase();
        if lower.contains("api key") || lower.contains("api_key") {
            let other = if self.region == "US" { "EU" } else { "US" };
            return ConnectorError::fatal(format!(
                "Amplitude sink: Amplitude refused the API key ({error}). Check it, and the \
                 region: events go to {} ({}); a project stored in {other} needs region {other}",
                self.region, self.client.url
            ));
        }
        if status == 429 {
            let over: Vec<String> = ["exceeded_daily_quota_users", "exceeded_daily_quota_devices"]
                .iter()
                .filter_map(|key| answer.get(*key).and_then(Value::as_object))
                .flat_map(|map| map.keys().cloned())
                .collect();
            if !over.is_empty() {
                return ConnectorError::fatal(format!(
                    "Amplitude sink: Amplitude refused events of {} users or devices that sent \
                     over 500,000 events in the last 24 hours ({}); leave them out or ask \
                     Amplitude to raise the limit",
                    over.len(),
                    over.iter().take(5).cloned().collect::<Vec<_>>().join(", ")
                ));
            }
            // A user or device over 1,000 events a second: the stream tries the batch again
            // after Amplitude's 15 s; insert_ids keep the events sent already from doubling.
            tokio::time::sleep(std::time::Duration::from_secs(THROTTLE_SECONDS)).await;
            return ConnectorError::retryable(format!("Amplitude sink: throttled (429): {error}"));
        }
        if status == 408 || status >= 500 {
            return ConnectorError::retryable(format!(
                "Amplitude sink: Amplitude answered {status}: {error}"
            ));
        }
        let mut details = Vec::new();
        if let Some(field) = answer.get("missing_field").and_then(Value::as_str) {
            details.push(format!("missing field {field}"));
        }
        for (key, kind) in [
            ("events_with_invalid_fields", "invalid"),
            ("events_with_missing_fields", "missing"),
        ] {
            for (field, indexes) in answer
                .get(key)
                .and_then(Value::as_object)
                .into_iter()
                .flatten()
            {
                let indexes = indexes.as_array().cloned().unwrap_or_default();
                let first = indexes
                    .first()
                    .and_then(Value::as_u64)
                    .and_then(|i| events.get(i as usize));
                let who = first
                    .and_then(|e| e.get("user_id").or_else(|| e.get("device_id")))
                    .and_then(Value::as_str)
                    .unwrap_or("?");
                let event_type = first
                    .and_then(|e| e.get("event_type"))
                    .and_then(Value::as_str)
                    .unwrap_or("?");
                details.push(format!(
                    "{field} {kind} in {} of {} events (the first: {event_type} of {who})",
                    indexes.len(),
                    events.len()
                ));
            }
        }
        if details
            .iter()
            .any(|d| d.starts_with("user_id") || d.starts_with("device_id"))
        {
            details.push(
                "Amplitude needs ids of at least 5 characters; min_id_length lowers it".into(),
            );
        }
        ConnectorError::fatal(format!(
            "Amplitude sink: Amplitude refused the events ({status}): {error}. {}",
            details.join("; ")
        ))
    }
}

/// Requests of up to 2,000 events and 10 MB.
fn chunks(events: &[Value]) -> Vec<&[Value]> {
    let mut chunks = Vec::new();
    let (mut start, mut size) = (0, 0);
    for (index, event) in events.iter().enumerate() {
        let length = event.to_string().len() + 1;
        if index > start && (index - start >= MAX_EVENTS || size + length > MAX_BYTES) {
            chunks.push(&events[start..index]);
            start = index;
            size = 0;
        }
        size += length;
    }
    if start < events.len() {
        chunks.push(&events[start..]);
    }
    chunks
}

fn text_of(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_string)
}

fn sha256(value: &Value) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, value.to_string().as_bytes());
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// Milliseconds since the epoch, from epoch seconds or milliseconds, or an ISO 8601 time
/// (UTC when it has no offset).
pub fn epoch_millis(value: &Value) -> Result<i64, String> {
    match value {
        Value::Number(number) => {
            let seconds_or_millis = number
                .as_f64()
                .ok_or_else(|| format!("time {number} is not a number of seconds"))?;
            // Seconds until the year 5138; milliseconds after.
            Ok(if seconds_or_millis.abs() < 1e11 {
                (seconds_or_millis * 1000.0).round() as i64
            } else {
                seconds_or_millis as i64
            })
        }
        Value::String(text) => {
            let text = text.trim();
            if let Ok(number) = text.parse::<i64>() {
                return epoch_millis(&Value::from(number));
            }
            if let Ok(moment) = chrono::DateTime::parse_from_rfc3339(text) {
                return Ok(moment.timestamp_millis());
            }
            for format in [
                "%Y-%m-%dT%H:%M:%S%.f",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%dT%H:%M",
                "%Y-%m-%d %H:%M",
            ] {
                if let Ok(moment) = chrono::NaiveDateTime::parse_from_str(text, format) {
                    return Ok(moment.and_utc().timestamp_millis());
                }
            }
            for format in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%.f%#z"] {
                if let Ok(moment) = chrono::DateTime::parse_from_str(text, format) {
                    return Ok(moment.timestamp_millis());
                }
            }
            if let Ok(day) = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d") {
                return Ok(day
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_millis());
            }
            Err(format!(
                "time {text:?} is not an ISO 8601 time or epoch seconds"
            ))
        }
        other => Err(format!("time {other} is not a time")),
    }
}

impl Sink for AmplitudeSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let events = self.events(batch)?;
            let key = batch_key(&self.stream, meta);
            for (index, chunk) in chunks(&events).into_iter().enumerate() {
                self.send(chunk, format!("{key}:{index}")).await?;
            }
            Ok(())
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            // Events carry insert_ids Amplitude deduplicates for 7 days; setting a user
            // property again changes nothing.
            idempotent: true,
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mage_stream_ports::{Envelope, EnvelopeBuilder};

    fn sink(config: Value) -> AmplitudeSink {
        let mut config = config;
        config["api_key"] = json!("test-api-key");
        AmplitudeSink::new(&config, "orders").unwrap()
    }

    fn row(text: &str) -> Map<String, Value> {
        serde_json::from_str(text).unwrap()
    }

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

    #[test]
    fn rows_become_events_with_fields_times_and_properties() {
        let sink = sink(json!({}));
        let event = sink
            .event(row(
                r#"{"user_id": 123456, "event_type": "Purchase", "time": "2024-06-01T10:00:00Z",
                    "price": 9.5, "sku": "merch-1", "_mage_created_at": "x", "note": null}"#,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(event["user_id"], "123456");
        assert_eq!(event["time"], 1717236000000_i64);
        assert_eq!(event["price"], 9.5);
        assert_eq!(event["event_properties"], json!({"sku": "merch-1"}));
        assert_eq!(event["insert_id"].as_str().unwrap().len(), 64);
        assert_eq!(sink.client.url, "https://api2.amplitude.com/batch");
    }

    #[test]
    fn times_of_every_kind_are_epoch_milliseconds() {
        for value in [
            json!("2024-06-01T10:00:00Z"),
            json!("2024-06-01T12:00:00+02:00"),
            json!("2024-06-01 10:00:00"),
            json!("2024-06-01 10:00:00.000000"),
            json!("2024-06-01T10:00"),
            json!(1717236000),
            json!(1717236000.0),
            json!(1717236000000_i64),
            json!("1717236000000"),
        ] {
            assert_eq!(epoch_millis(&value), Ok(1717236000000), "{value}");
        }
        assert!(epoch_millis(&json!("yesterday")).is_err());
        assert!(epoch_millis(&json!(true)).is_err());
    }

    #[test]
    fn columns_name_the_fields_and_keys_follow_them() {
        let sink = sink(json!({
            "event_type": "Order Completed",
            "columns": {"user_id": "customer_id", "time": "ordered_at"},
            "key_columns": ["order_id"],
        }));
        let first = sink
            .event(row(
                r#"{"order_id": 7, "customer_id": "customer-1", "ordered_at": 1717236000, "status": "paid"}"#,
            ))
            .unwrap()
            .unwrap();
        let refunded = sink
            .event(row(
                r#"{"order_id": 7, "customer_id": "customer-1", "ordered_at": 1717236000, "status": "refunded"}"#,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(first["event_type"], "Order Completed");
        assert_eq!(first["user_id"], "customer-1");
        assert_eq!(first["time"], 1717236000000_i64);
        assert_eq!(
            first["event_properties"],
            json!({"order_id": 7, "status": "paid"})
        );
        assert_eq!(first["insert_id"], refunded["insert_id"]);
    }

    #[test]
    fn the_same_event_has_the_same_insert_id_and_another_does_not() {
        let sink = sink(json!({}));
        let text =
            r#"{"user_id": "user-1", "event_type": "Play", "time": 1717236000000, "song": "a"}"#;
        let one = sink.event(row(text)).unwrap().unwrap();
        let again = sink.event(row(text)).unwrap().unwrap();
        let other = sink
            .event(row(r#"{"user_id": "user-1", "event_type": "Play", "time": 1717236000000, "song": "b"}"#))
            .unwrap()
            .unwrap();
        assert_eq!(one["insert_id"], again["insert_id"]);
        assert_ne!(one["insert_id"], other["insert_id"]);
        let exported = sink
            .event(row(
                r#"{"$insert_id": "abc", "user_id": "user-1", "event_type": "Play"}"#,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(exported["insert_id"], "abc");
    }

    #[test]
    fn user_properties_are_set_on_identify_events() {
        let sink = sink(json!({"user_properties": true, "columns": {"user_id": "customer_id"}}));
        let event = sink
            .event(row(
                r#"{"customer_id": "customer-1", "churn_risk": 0.8, "country": "Spain",
                    "user_properties": {"$add": {"purchases": 1}}}"#,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(event["event_type"], "$identify");
        assert_eq!(event["user_id"], "customer-1");
        let revenue = sink
            .event(row(
                r#"{"customer_id": "customer-3", "revenue": 19.5, "platform": "Web"}"#,
            ))
            .unwrap()
            .unwrap();
        assert_eq!(
            revenue["user_properties"],
            json!({"$set": {"revenue": 19.5}})
        );
        assert_eq!(revenue["platform"], "Web");
        assert_eq!(event["country"], "Spain");
        assert_eq!(
            event["user_properties"],
            json!({"$add": {"purchases": 1}, "$set": {"churn_risk": 0.8}})
        );
        assert!(event.get("insert_id").is_none());
        let empty = sink
            .event(row(r#"{"customer_id": "customer-2", "segment": null}"#))
            .unwrap();
        assert!(empty.is_none());
    }

    #[test]
    fn rows_without_ids_or_event_types_fail_the_batch_by_number() {
        let sink = sink(json!({}));
        let error = sink
            .events(&batch(&[
                r#"{"user_id": "user-1", "event_type": "x"}"#,
                r#"{"event_type": "x"}"#,
            ]))
            .unwrap_err();
        assert!(!error.retryable);
        assert!(error.message.contains("message 2"), "{error:?}");
        assert!(error.message.contains("user_id or device_id"), "{error:?}");
        let error = sink
            .events(&batch(&[r#"{"user_id": "user-1"}"#]))
            .unwrap_err();
        assert!(error.message.contains("event_type"), "{error:?}");
        assert!(sink.events(&batch(&["[1]"])).is_err());
    }

    #[test]
    fn settings_are_checked() {
        assert!(AmplitudeSink::new(&json!({}), "s").is_err());
        assert!(AmplitudeSink::new(&json!({"api_key": "k", "region": "APAC"}), "s").is_err());
        let eu = AmplitudeSink::new(&json!({"api_key": "k", "region": "eu"}), "s").unwrap();
        assert_eq!(eu.client.url, "https://api.eu.amplitude.com/batch");
        assert!(eu.capabilities().idempotent);
    }

    #[test]
    fn requests_hold_at_most_2000_events() {
        let events: Vec<Value> = (0..4500)
            .map(|i| json!({"user_id": format!("user-{i}")}))
            .collect();
        let sizes: Vec<usize> = chunks(&events).iter().map(|c| c.len()).collect();
        assert_eq!(sizes, vec![2000, 2000, 500]);
        assert!(chunks(&[]).is_empty());
    }

    /// A receiver that answers each request with the next status and body, and keeps the
    /// request bodies.
    fn receiver(answers: Vec<(u16, &'static str)>) -> (String, std::sync::mpsc::Receiver<Value>) {
        use std::io::{BufRead, BufReader, Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for (status, answer) in answers {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line.to_ascii_lowercase().starts_with("content-length:") {
                        length = line[15..].trim().parse().unwrap();
                    }
                    if line == "\r\n" {
                        break;
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                tx.send(serde_json::from_slice(&body).unwrap()).unwrap();
                write!(
                    stream,
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{answer}",
                    answer.len()
                )
                .unwrap();
            }
        });
        (url, rx)
    }

    fn meta() -> BatchMeta {
        BatchMeta {
            id: mage_stream_core::BatchId {
                partition: mage_stream_ports::PartitionId(0),
                epoch: mage_stream_ports::AssignmentEpoch(1),
                seq: 0,
            },
            first: mage_stream_ports::Position::Offset(0),
            last: mage_stream_ports::Position::Offset(3),
            records: 4,
            bytes: 10,
        }
    }

    fn local_sink(url: &str) -> AmplitudeSink {
        sink(json!({"url": url, "allow_private": true, "min_id_length": 3}))
    }

    #[tokio::test]
    async fn a_batch_is_sent_with_the_key_and_split_when_too_large() {
        let (url, requests) = receiver(vec![
            (413, r#"{"code": 413, "error": "Payload too large"}"#),
            (200, r#"{"code": 200, "events_ingested": 2}"#),
            (200, r#"{"code": 200, "events_ingested": 2}"#),
        ]);
        let rows: Vec<String> = (0..4)
            .map(|i| format!(r#"{{"user_id": "user-{i}", "event_type": "Play"}}"#))
            .collect();
        let rows: Vec<&str> = rows.iter().map(String::as_str).collect();

        local_sink(&url)
            .write(&batch(&rows), &meta())
            .await
            .unwrap();

        let first = requests.recv().unwrap();
        assert_eq!(first["api_key"], "test-api-key");
        assert_eq!(first["options"], json!({"min_id_length": 3}));
        assert_eq!(first["events"].as_array().unwrap().len(), 4);
        assert_eq!(
            requests.recv().unwrap()["events"].as_array().unwrap().len(),
            2
        );
        assert_eq!(requests.recv().unwrap()["events"][0]["user_id"], "user-2");
    }

    #[tokio::test]
    async fn refusals_are_explained_and_server_errors_retried() {
        let (url, _requests) = receiver(vec![
            (
                400,
                r#"{"code": 400, "error": "Request missing required field",
                    "events_with_invalid_fields": {"user_id": [1]}}"#,
            ),
            (
                400,
                r#"{"code": 400, "error": "Invalid API key: test-api-key"}"#,
            ),
            (503, "unavailable"),
            (
                429,
                r#"{"code": 429, "exceeded_daily_quota_users": {"user-1": 500001}}"#,
            ),
        ]);
        let sink = local_sink(&url);
        let rows = batch(&[
            r#"{"user_id": "user-1", "event_type": "Play"}"#,
            r#"{"user_id": "u-2", "event_type": "Skip"}"#,
        ]);

        let invalid = sink.write(&rows, &meta()).await.unwrap_err();
        assert!(!invalid.retryable);
        assert!(
            invalid
                .message
                .contains("user_id invalid in 1 of 2 events (the first: Skip of u-2)"),
            "{invalid:?}"
        );
        assert!(invalid.message.contains("min_id_length"), "{invalid:?}");

        let key = sink.write(&rows, &meta()).await.unwrap_err();
        assert!(!key.retryable);
        assert!(key.message.contains("region EU"), "{key:?}");

        assert!(sink.write(&rows, &meta()).await.unwrap_err().retryable);

        let quota = sink.write(&rows, &meta()).await.unwrap_err();
        assert!(!quota.retryable);
        assert!(
            quota.message.contains("500,000") && quota.message.contains("user-1"),
            "{quota:?}"
        );
    }
}
