//! InfluxDB 2 sink for streams, with MageML's settings.
//!
//! Each batch goes to `/api/v2/write` as line protocol, in requests of up to 5,000 points
//! or 8 MB, and is confirmed once InfluxDB accepted every request. A message is a point:
//! `{"data": ..., "metadata": {"measurement", "time", "tags"}}`, or the message itself as
//! `data`. An object's values are the fields; a list's items or a scalar are `_value`
//! fields. Integers keep every digit (`i`, or `u` past i64; past u64 they are text), lists
//! and objects are JSON text, and nulls are left out. `time` (or `timestamp`) is in
//! milliseconds; messages without one get the batch's time in nanoseconds plus their
//! position, so points with the same measurement and tags do not overwrite each other.
//!
//! Settings: `url`, `token`, `org`, `bucket` (data), `measurement` (default).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mage_stream_ports::{
    BatchMeta, BoxFuture, ConnectorError, RecordBatch, Sink, SinkCapabilities, read_envelope,
};
use object_store::ClientOptions;
use object_store::client::{HttpClient, HttpConnector, HttpRequestBody, ReqwestConnector};
use serde_json::Value;
use serde_json::value::RawValue;

const MAX_POINTS: usize = 5_000;
const MAX_BYTES: usize = 8 * 1024 * 1024;

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

pub struct InfluxDbSink {
    write_url: String,
    token: String,
    measurement: String,
    http: HttpClient,
}

impl InfluxDbSink {
    pub fn new(config: &Value) -> Result<InfluxDbSink, ConnectorError> {
        let need = |key: &str| {
            text(config, key)
                .ok_or_else(|| ConnectorError::fatal(format!("InfluxDB settings need {key}.")))
        };
        let url = need("url")?;
        let url = url.trim_end_matches('/').to_string();
        let token = need("token")?;
        let org = need("org")?;
        let bucket = text(config, "bucket").unwrap_or_else(|| "data".into());
        let options = ClientOptions::new()
            .with_timeout(Duration::from_secs(120))
            .with_connect_timeout(Duration::from_secs(10))
            .with_allow_http(url.starts_with("http://"));
        let http = ReqwestConnector::default()
            .connect(&options)
            .map_err(|e| ConnectorError::fatal(format!("InfluxDB client: {e}")))?;
        Ok(InfluxDbSink {
            write_url: format!(
                "{url}/api/v2/write?org={}&bucket={}&precision=ns",
                percent_encode(&org),
                percent_encode(&bucket)
            ),
            token,
            measurement: text(config, "measurement").unwrap_or_else(|| "default".into()),
            http,
        })
    }

    /// The line protocol request bodies of a batch, whose messages without a time are
    /// stamped `now` plus their position, in nanoseconds.
    fn bodies(&self, values: &[Vec<u8>], now: i128) -> Result<Vec<Vec<u8>>, ConnectorError> {
        let mut bodies = Vec::new();
        let mut body = Vec::new();
        let mut count = 0;
        let mut position = 0i128;
        for value in values {
            for line in points(value, &self.measurement, now, &mut position)? {
                if count > 0 && (count >= MAX_POINTS || body.len() + line.len() > MAX_BYTES) {
                    bodies.push(std::mem::take(&mut body));
                    count = 0;
                }
                body.extend_from_slice(line.as_bytes());
                body.push(b'\n');
                count += 1;
            }
        }
        if !body.is_empty() {
            bodies.push(body);
        }
        Ok(bodies)
    }

    async fn post(&self, body: Vec<u8>) -> Result<(), ConnectorError> {
        let request = http::Request::builder()
            .method("POST")
            .uri(&self.write_url)
            .header("authorization", format!("Token {}", self.token))
            .header("content-type", "text/plain; charset=utf-8")
            .body(HttpRequestBody::from(body))
            .map_err(|e| ConnectorError::fatal(format!("InfluxDB: {e}")))?;
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|e| ConnectorError::retryable(format!("InfluxDB: {e}")))?;
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let bytes = response.into_body().bytes().await.unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes[..bytes.len().min(2000)]).to_string();
        let message = format!("InfluxDB answered {status}: {text}");
        Err(if status == 429 || status >= 500 {
            ConnectorError::retryable(message)
        } else {
            ConnectorError::fatal(message)
        })
    }
}

fn percent_encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Measurements escape commas and spaces; tag keys, tag values and field keys also `=`.
fn escape(text: &str, equals: bool) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            ',' | ' ' => out.push('\\'),
            '=' if equals => out.push('\\'),
            '\n' => {
                out.push_str("\\n");
                continue;
            }
            _ => {}
        }
        out.push(c);
    }
    out
}

fn string_field(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

/// A field value as line protocol, from its JSON text: None for null.
fn field_value(raw: &RawValue) -> Option<String> {
    let text = raw.get().trim();
    match text.as_bytes().first()? {
        b'n' => None,
        b't' | b'f' => Some(text.to_string()),
        b'"' => Some(string_field(&serde_json::from_str::<String>(text).ok()?)),
        b'{' | b'[' => Some(string_field(text)),
        _ if text.contains(['.', 'e', 'E']) => Some(text.to_string()),
        _ => Some(if text.parse::<i64>().is_ok() {
            format!("{text}i")
        } else if text.parse::<u64>().is_ok() {
            format!("{text}u")
        } else {
            // Past 64 bits: InfluxDB has no such integers, so the digits stay as text.
            string_field(text)
        }),
    }
}

/// The points of one message, as lines.
fn points(
    value: &[u8],
    default_measurement: &str,
    now: i128,
    position: &mut i128,
) -> Result<Vec<String>, ConnectorError> {
    let message: Box<RawValue> = serde_json::from_slice(value)
        .map_err(|e| ConnectorError::fatal(format!("InfluxDB: a message is not JSON: {e}")))?;
    let object = serde_json::from_str::<serde_json::Map<String, Value>>(message.get()).ok();
    let (data, metadata) = match &object {
        Some(map) if map.contains_key("data") => {
            let raw: std::collections::HashMap<String, Box<RawValue>> =
                serde_json::from_str(message.get()).expect("an object");
            (
                raw.get("data").cloned().expect("data"),
                map.get("metadata").cloned(),
            )
        }
        _ => (message.clone(), None),
    };
    let metadata = metadata.unwrap_or(Value::Null);
    let measurement = metadata
        .get("measurement")
        .and_then(Value::as_str)
        .unwrap_or(default_measurement);
    let mut series = escape(measurement, false);
    if let Some(tags) = metadata.get("tags").and_then(Value::as_object) {
        let mut tags: Vec<_> = tags.iter().collect();
        tags.sort_by(|a, b| a.0.cmp(b.0));
        for (key, value) in tags {
            let value = match value {
                Value::String(s) => s.clone(),
                Value::Null => continue,
                other => other.to_string(),
            };
            series.push_str(&format!(",{}={}", escape(key, true), escape(&value, true)));
        }
    }
    let time = metadata
        .get("time")
        .or_else(|| metadata.get("timestamp"))
        .and_then(Value::as_i64)
        .map(|ms| ms as i128 * 1_000_000);

    let field_sets: Vec<Vec<(String, String)>> = match data.get().trim().as_bytes().first() {
        Some(b'{') => {
            let fields = serde_json::from_str::<ordered::Fields>(data.get())
                .map(|fields| fields.0)
                .unwrap_or_default();
            vec![
                fields
                    .iter()
                    .filter_map(|(k, v)| field_value(v).map(|v| (escape(k, true), v)))
                    .collect(),
            ]
        }
        Some(b'[') => serde_json::from_str::<Vec<Box<RawValue>>>(data.get())
            .unwrap_or_default()
            .iter()
            .map(|v| {
                field_value(v)
                    .map(|v| ("_value".to_string(), v))
                    .into_iter()
                    .collect()
            })
            .collect(),
        _ => vec![
            field_value(&data)
                .map(|v| ("_value".to_string(), v))
                .into_iter()
                .collect(),
        ],
    };

    let mut lines = Vec::new();
    for fields in field_sets {
        if fields.is_empty() {
            return Err(ConnectorError::fatal(format!(
                "InfluxDB: a message has no field to write (only nulls): {}",
                String::from_utf8_lossy(&value[..value.len().min(200)])
            )));
        }
        let fields: Vec<String> = fields
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        let stamp = time.unwrap_or_else(|| {
            *position += 1;
            now + *position - 1
        });
        lines.push(format!("{series} {} {stamp}", fields.join(",")));
    }
    Ok(lines)
}

/// An object's fields in their order, with their JSON text.
mod ordered {
    use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
    use serde_json::value::RawValue;

    pub struct Fields(pub Vec<(String, Box<RawValue>)>);

    impl<'de> Deserialize<'de> for Fields {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct OrderedVisitor;
            impl<'de> Visitor<'de> for OrderedVisitor {
                type Value = Fields;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("an object")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Fields, A::Error> {
                    let mut out = Vec::new();
                    while let Some((key, value)) = map.next_entry::<String, Box<RawValue>>()? {
                        out.push((key, value));
                    }
                    Ok(Fields(out))
                }
            }
            deserializer.deserialize_map(OrderedVisitor)
        }
    }
}

fn now_ns() -> i128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i128)
        .unwrap_or(0)
}

impl Sink for InfluxDbSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let envelopes = read_envelope(batch)
                .map_err(|e| ConnectorError::fatal(format!("InfluxDB: {e}")))?;
            let values: Vec<Vec<u8>> = envelopes.into_iter().map(|e| e.value).collect();
            for body in self.bodies(&values, now_ns())? {
                self.post(body).await?;
            }
            Ok(())
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        // Points with a given time replace themselves; those stamped by the sink do not.
        SinkCapabilities {
            idempotent: false,
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(message: &str) -> Vec<String> {
        let mut position = 0;
        points(message.as_bytes(), "default", 1_000, &mut position).unwrap()
    }

    #[test]
    fn integers_keep_every_digit_and_types_map() {
        assert_eq!(
            lines(
                r#"{"n": 9223372036854775807, "u": 18446744073709551615, "big": 1180591620717411303424, "f": 1.5, "ok": true, "s": "Zürich \"x\"", "list": [1, 2], "none": null}"#
            ),
            vec![
                r#"default n=9223372036854775807i,u=18446744073709551615u,big="1180591620717411303424",f=1.5,ok=true,s="Zürich \"x\"",list="[1, 2]" 1000"#
            ]
        );
    }

    #[test]
    fn metadata_sets_measurement_tags_and_time_and_names_are_escaped() {
        assert_eq!(
            lines(
                r#"{"data": {"bees count": 23}, "metadata": {"measurement": "my census", "time": 1693472675783, "tags": {"site": "a,b", "kind": "x=y"}}}"#
            ),
            vec![r#"my\ census,kind=x\=y,site=a\,b bees\ count=23i 1693472675783000000"#]
        );
    }

    #[test]
    fn messages_without_a_time_get_distinct_times() {
        let sink_values: Vec<Vec<u8>> = (0..3)
            .map(|i| format!("{{\"n\": {i}}}").into_bytes())
            .collect();
        let mut position = 0;
        let stamps: Vec<String> = sink_values
            .iter()
            .flat_map(|v| points(v, "m", 5_000, &mut position).unwrap())
            .map(|l| l.rsplit(' ').next().unwrap().to_string())
            .collect();
        assert_eq!(stamps, vec!["5000", "5001", "5002"]);
    }

    #[test]
    fn lists_and_scalars_are_value_fields_and_empty_messages_are_refused() {
        assert_eq!(
            lines("[23, 30]"),
            vec!["default _value=23i 1000", "default _value=30i 1001"]
        );
        assert_eq!(lines(r#"{"data": 2.5}"#), vec!["default _value=2.5 1000"]);
        let mut position = 0;
        assert!(points(br#"{"a": null}"#, "m", 0, &mut position).is_err());
    }

    #[test]
    fn org_and_bucket_are_url_encoded() {
        assert_eq!(percent_encode("my org/x"), "my%20org%2Fx");
    }
}
