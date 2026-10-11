//! A client for Feast's feature server REST API, for Rust blocks (feature `feast`).
//!
//! It follows the same rules as MageML's Python client (mage_ai/feature_stores/feast.py),
//! from docs/development/feast-integration.md:
//!
//! - Large integers: Feast rounds an Int64 column above 2**53 when it has a NULL in the
//!   same request, so such rows go in requests where each of those columns is complete or
//!   all NULL.
//! - Event order: the online store keeps the last write, so rows go in event-time order.
//! - Timestamps: Feast reads integers as nanoseconds and naive values as UTC; the timestamp
//!   column must be a datetime with a time zone.
//! - Errors: only connection errors, timeouts, 429, 502, 503 and 504 are retried.
//! - Reads: values are mapped by the response's feature names; integers stay exact.
//!
//! ```ignore
//! use mage::feast::FeastClient;
//!
//! let feast = FeastClient::from_env()?; // FEAST_URL, FEAST_TOKEN
//! let features = feast.get_online_features(&entities, &["driver_hourly_stats:conv_rate"])?;
//! feast.push("driver_stats_push", &frame, "event_timestamp")?;
//! ```
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use polars::prelude::*;
use serde_json::{Map, Number, Value, json};

/// Integers above this lose precision as float64.
pub const EXACT_FLOAT_INTEGER: i64 = 1 << 53;
const RETRIED_STATUSES: [u16; 4] = [429, 502, 503, 504];

pub struct FeastClient {
    url: String,
    token: Option<String>,
    retries: u32,
    backoff: Duration,
    batch_size: usize,
    agent: ureq::Agent,
}

impl FeastClient {
    /// A client for the feature server at url, such as http://feast:6566.
    pub fn new(url: impl Into<String>) -> Self {
        Self::with_timeout(url, Duration::from_secs(30))
    }

    pub fn with_timeout(url: impl Into<String>, timeout: Duration) -> Self {
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .http_status_as_error(false)
            .build();
        Self {
            url: url.into().trim_end_matches('/').to_string(),
            token: None,
            retries: 3,
            backoff: Duration::from_millis(500),
            batch_size: 1000,
            agent: config.into(),
        }
    }

    /// The feature server from FEAST_URL, with FEAST_TOKEN as a bearer token when set.
    pub fn from_env() -> Result<Self> {
        let url = std::env::var("FEAST_URL")
            .ok()
            .filter(|url| !url.is_empty())
            .ok_or_else(|| anyhow!("No feature server URL: set FEAST_URL."))?;
        let mut client = Self::new(url);
        client.token = std::env::var("FEAST_TOKEN").ok().filter(|t| !t.is_empty());
        Ok(client)
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Attempts after the first for transient failures, waiting backoff * 2**attempt.
    pub fn retries(mut self, retries: u32, backoff: Duration) -> Self {
        self.retries = retries;
        self.backoff = backoff;
        self
    }

    /// Rows per push request (1000 by default).
    pub fn batch_size(mut self, rows: usize) -> Self {
        self.batch_size = rows.max(1);
        self
    }

    fn request(&self, method: &str, route: &str, body: Option<&Value>) -> Result<Value> {
        let url = format!("{}{route}", self.url);
        for attempt in 0..=self.retries {
            let last = attempt == self.retries;
            let response = if method == "GET" {
                let mut request = self.agent.get(&url);
                if let Some(token) = &self.token {
                    request = request.header("Authorization", format!("Bearer {token}"));
                }
                request.call()
            } else {
                let mut request = self.agent.post(&url);
                if let Some(token) = &self.token {
                    request = request.header("Authorization", format!("Bearer {token}"));
                }
                request.send_json(body.cloned().unwrap_or(Value::Null))
            };
            let mut response = match response {
                Ok(response) => response,
                Err(error) => {
                    if last {
                        bail!(
                            "{route}: the feature server at {} did not answer after {} attempts: {error}",
                            self.url,
                            attempt + 1
                        );
                    }
                    std::thread::sleep(self.backoff * 2u32.pow(attempt));
                    continue;
                }
            };
            let status = response.status().as_u16();
            if RETRIED_STATUSES.contains(&status) && !last {
                std::thread::sleep(self.backoff * 2u32.pow(attempt));
                continue;
            }
            let text = response.body_mut().read_to_string().unwrap_or_default();
            if status >= 400 {
                let shown: String = text.chars().take(4000).collect();
                bail!(
                    "{route} failed with {status}: {}",
                    if shown.is_empty() {
                        "(empty body)"
                    } else {
                        &shown
                    }
                );
            }
            if text.trim().is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_str(&text).with_context(|| format!("{route}: invalid JSON"));
        }
        unreachable!()
    }

    /// Whether the feature server answers /health.
    pub fn health(&self) -> bool {
        self.request("GET", "/health", None).is_ok()
    }

    /// The online features of the entities (a frame of entity key columns), one row per
    /// entity row in request order. A feature Feast reports NOT_FOUND is NULL.
    pub fn get_online_features(
        &self,
        entities: &DataFrame,
        features: &[&str],
    ) -> Result<DataFrame> {
        let mut body = Map::new();
        body.insert("entities".into(), columns_json(entities)?);
        body.insert("features".into(), json!(features));
        self.read(Value::Object(body))
    }

    /// The online features of a feature service.
    pub fn get_online_features_service(
        &self,
        entities: &DataFrame,
        feature_service: &str,
    ) -> Result<DataFrame> {
        let mut body = Map::new();
        body.insert("entities".into(), columns_json(entities)?);
        body.insert("feature_service".into(), json!(feature_service));
        self.read(Value::Object(body))
    }

    fn read(&self, body: Value) -> Result<DataFrame> {
        let payload = self.request("POST", "/get-online-features", Some(&body))?;
        let names = payload["metadata"]["feature_names"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let results = payload["results"].as_array().cloned().unwrap_or_default();
        if names.len() != results.len() {
            bail!(
                "/get-online-features returned {} results for {} names",
                results.len(),
                names.len()
            );
        }
        let mut columns = Vec::with_capacity(names.len());
        for (name, result) in names.iter().zip(results.iter()) {
            let name = name.as_str().unwrap_or_default();
            let values = result["values"].as_array().cloned().unwrap_or_default();
            let statuses = result["statuses"].as_array().cloned().unwrap_or_default();
            let values: Vec<Value> = values
                .into_iter()
                .enumerate()
                .map(|(i, value)| match statuses.get(i).and_then(Value::as_str) {
                    Some("NOT_FOUND") => Value::Null,
                    _ => value,
                })
                .collect();
            columns.push(column_from_json(name, &values));
        }
        Ok(DataFrame::new_infer_height(columns)?)
    }

    /// Pushes the rows of a frame to a push source; returns the number of requests.
    pub fn push(
        &self,
        push_source: &str,
        frame: &DataFrame,
        timestamp_column: &str,
    ) -> Result<usize> {
        let mut sent = 0;
        for rows in requests_for(frame, timestamp_column, self.batch_size)? {
            self.request(
                "POST",
                "/push",
                Some(&json!({"push_source_name": push_source, "df": rows, "to": "online"})),
            )?;
            sent += 1;
        }
        Ok(sent)
    }

    /// Writes the rows of a frame to a feature view's online store, as push does.
    pub fn write_to_online_store(
        &self,
        feature_view: &str,
        frame: &DataFrame,
        timestamp_column: &str,
    ) -> Result<usize> {
        let mut sent = 0;
        for rows in requests_for(frame, timestamp_column, self.batch_size)? {
            self.request(
                "POST",
                "/write-to-online-store",
                Some(&json!({"feature_view_name": feature_view, "df": rows})),
            )?;
            sent += 1;
        }
        Ok(sent)
    }

    /// Loads the offline store's rows between start and end (UTC) into the online store.
    pub fn materialize(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        feature_views: &[&str],
    ) -> Result<()> {
        let mut body = json!({
            "start_ts": start.to_rfc3339_opts(SecondsFormat::Micros, false),
            "end_ts": end.to_rfc3339_opts(SecondsFormat::Micros, false),
        });
        if !feature_views.is_empty() {
            body["feature_views"] = json!(feature_views);
        }
        self.request("POST", "/materialize", Some(&body))
            .map(|_| ())
    }
}

/// Each column of the frame as a JSON array.
fn columns_json(frame: &DataFrame) -> Result<Value> {
    let mut out = Map::new();
    for column in frame.columns() {
        let name = column.name().to_string();
        let values = (0..frame.height())
            .map(|i| value_json(&column.get(i)?, &name))
            .collect::<Result<Vec<_>>>()?;
        out.insert(name, Value::Array(values));
    }
    Ok(Value::Object(out))
}

fn micros(value: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Nanoseconds => value / 1_000,
        TimeUnit::Microseconds => value,
        TimeUnit::Milliseconds => value * 1_000,
    }
}

fn value_json(value: &AnyValue, column: &str) -> Result<Value> {
    Ok(match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => Value::Bool(*b),
        AnyValue::Int8(v) => json!(v),
        AnyValue::Int16(v) => json!(v),
        AnyValue::Int32(v) => json!(v),
        AnyValue::Int64(v) => json!(v),
        AnyValue::UInt8(v) => json!(v),
        AnyValue::UInt16(v) => json!(v),
        AnyValue::UInt32(v) => json!(v),
        AnyValue::UInt64(v) => json!(v),
        AnyValue::Float32(v) => float_json(f64::from(*v), column)?,
        AnyValue::Float64(v) => float_json(*v, column)?,
        AnyValue::String(s) => Value::String((*s).to_string()),
        AnyValue::StringOwned(s) => Value::String(s.to_string()),
        AnyValue::Date(days) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719_163)
                .ok_or_else(|| anyhow!("{column}: date out of range"))?;
            Value::String(date.format("%Y-%m-%d").to_string())
        }
        AnyValue::Datetime(v, unit, tz) => datetime_json(*v, *unit, tz.is_some(), column)?,
        AnyValue::DatetimeOwned(v, unit, tz) => datetime_json(*v, *unit, tz.is_some(), column)?,
        other => bail!("{column}: Feast has no type for {other:?}; cast the column first"),
    })
}

fn float_json(value: f64, column: &str) -> Result<Value> {
    if value.is_nan() {
        return Ok(Value::Null);
    }
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| anyhow!("{column} holds {value}, which JSON cannot carry"))
}

fn datetime_json(value: i64, unit: TimeUnit, zoned: bool, column: &str) -> Result<Value> {
    if !zoned {
        bail!(
            "{column} has no time zone. Feast reads it as UTC; make it time zone aware, such as \
             with .dt().replace_time_zone(Some(\"UTC\"))"
        );
    }
    let time = DateTime::<Utc>::from_timestamp_micros(micros(value, unit))
        .ok_or_else(|| anyhow!("{column}: timestamp out of range"))?;
    Ok(Value::String(
        time.to_rfc3339_opts(SecondsFormat::Micros, false),
    ))
}

/// The df of each push request: rows in event-time order, split where a large integer
/// column changes between holding NULLs and not, and at batch_size rows.
fn requests_for(
    frame: &DataFrame,
    timestamp_column: &str,
    batch_size: usize,
) -> Result<Vec<Value>> {
    if frame.height() == 0 {
        return Ok(vec![]);
    }
    let timestamps = frame.column(timestamp_column).map_err(|_| {
        anyhow!(
            "The frame has no {timestamp_column:?} column; pass the push source's timestamp_field"
        )
    })?;
    match timestamps.dtype() {
        DataType::Datetime(_, Some(_)) => {}
        DataType::Datetime(_, None) => bail!(
            "{timestamp_column} has no time zone. Feast reads it as UTC; make it time zone aware"
        ),
        dtype if dtype.is_integer() => bail!(
            "{timestamp_column} holds integers; Feast reads them as nanoseconds, so epoch \
             seconds land in 1970. Convert it to a datetime with a time zone"
        ),
        dtype => bail!("{timestamp_column} is {dtype}; it must be a datetime with a time zone"),
    }
    if timestamps.null_count() > 0 {
        bail!(
            "{timestamp_column} has {} empty values",
            timestamps.null_count()
        );
    }
    // Stable: rows with the same event time keep their order, so the last one wins.
    let sorted = frame.sort(
        [timestamp_column],
        SortMultipleOptions::default().with_maintain_order(true),
    )?;
    let risky: Vec<String> = sorted
        .columns()
        .iter()
        .filter(|c| c.dtype().is_integer() && c.null_count() > 0)
        .filter(|c| {
            let series = c.as_materialized_series();
            let cast = series.cast(&DataType::Int64).ok();
            cast.and_then(|s| {
                let ints = s.i64().ok()?.clone();
                let max = ints.max().map(|v| v.unsigned_abs());
                let min = ints.min().map(|v| v.unsigned_abs());
                Some(max.max(min).unwrap_or(0) > EXACT_FLOAT_INTEGER as u64)
            })
            .unwrap_or(false)
        })
        .map(|c| c.name().to_string())
        .collect();
    let pattern = |i: usize| -> Vec<bool> {
        risky
            .iter()
            .map(|name| {
                matches!(
                    sorted.column(name).and_then(|c| c.get(i)),
                    Ok(AnyValue::Null)
                )
            })
            .collect()
    };
    let mut out = Vec::new();
    let mut start = 0;
    while start < sorted.height() {
        let current = pattern(start);
        let mut end = start + 1;
        while end < sorted.height() && end - start < batch_size && pattern(end) == current {
            end += 1;
        }
        out.push(columns_json(&sorted.slice(start as i64, end - start))?);
        start = end;
    }
    Ok(out)
}

/// A column from JSON values: Int64 when every value is an integer, Float64 when they are
/// numbers, Boolean, or String.
fn column_from_json(name: &str, values: &[Value]) -> Column {
    let present: Vec<&Value> = values.iter().filter(|v| !v.is_null()).collect();
    if !present.is_empty() && present.iter().all(|v| v.is_i64()) {
        let data: Vec<Option<i64>> = values.iter().map(Value::as_i64).collect();
        return Column::new(name.into(), data);
    }
    if !present.is_empty() && present.iter().all(|v| v.is_number()) {
        let data: Vec<Option<f64>> = values.iter().map(Value::as_f64).collect();
        return Column::new(name.into(), data);
    }
    if !present.is_empty() && present.iter().all(|v| v.is_boolean()) {
        let data: Vec<Option<bool>> = values.iter().map(Value::as_bool).collect();
        return Column::new(name.into(), data);
    }
    let data: Vec<Option<String>> = values
        .iter()
        .map(|v| match v {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        })
        .collect();
    Column::new(name.into(), data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(column: Column) -> Column {
        column
            .cast(&DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            ))
            .unwrap()
    }

    #[test]
    fn rows_go_in_event_order_and_large_integers_next_to_nulls_split_requests() {
        let big = EXACT_FLOAT_INTEGER + 1;
        let frame = DataFrame::new_infer_height(vec![
            Column::new("driver_id".into(), [1i64, 2, 3, 4]),
            Column::new("trips".into(), [Some(big), None, Some(big + 2), None]),
            Column::new("small".into(), [Some(1i64), None, Some(3), Some(4)]),
            utc(Column::new(
                "event_timestamp".into(),
                [4_000_000i64, 1_000_000, 2_000_000, 3_000_000],
            )),
        ])
        .unwrap();
        let requests = requests_for(&frame, "event_timestamp", 1000).unwrap();
        let trips: Vec<Value> = requests.iter().map(|r| r["trips"].clone()).collect();
        assert_eq!(
            trips,
            vec![json!([null]), json!([big + 2]), json!([null]), json!([big])]
        );
        assert_eq!(
            requests[0]["event_timestamp"],
            json!(["1970-01-01T00:00:01.000000+00:00"])
        );
    }

    #[test]
    fn small_integers_with_nulls_go_in_batches() {
        let frame = DataFrame::new_infer_height(vec![
            Column::new("trips".into(), [Some(1i64), None, Some(3)]),
            utc(Column::new("event_timestamp".into(), [1i64, 2, 3])),
        ])
        .unwrap();
        assert_eq!(requests_for(&frame, "event_timestamp", 2).unwrap().len(), 2);
    }

    #[test]
    fn timestamps_feast_would_misread_are_refused() {
        let naive = DataFrame::new_infer_height(vec![
            Column::new("event_timestamp".into(), [1i64])
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                .unwrap(),
        ])
        .unwrap();
        let error = requests_for(&naive, "event_timestamp", 10)
            .unwrap_err()
            .to_string();
        assert!(error.contains("no time zone"), "{error}");
        let epoch =
            DataFrame::new_infer_height(vec![Column::new("event_timestamp".into(), [1i64])])
                .unwrap();
        let error = requests_for(&epoch, "event_timestamp", 10)
            .unwrap_err()
            .to_string();
        assert!(error.contains("nanoseconds"), "{error}");
    }

    #[test]
    fn read_values_keep_integers_exact_and_not_found_is_null() {
        let big = EXACT_FLOAT_INTEGER + 1;
        let column = column_from_json("trips", &[json!(big), Value::Null, json!(5)]);
        assert_eq!(column.dtype(), &DataType::Int64);
        assert_eq!(column.get(0).unwrap(), AnyValue::Int64(big));
        let text = column_from_json("city", &[json!("a"), Value::Null]);
        assert_eq!(text.dtype(), &DataType::String);
    }

    /// Against a real feature server: MAGE_TEST_FEAST_URL (integration_tests/compose.yaml).
    #[test]
    fn a_feature_server_keeps_large_integers_next_to_nulls_exact() {
        let Ok(url) = std::env::var("MAGE_TEST_FEAST_URL") else {
            eprintln!("skipped: MAGE_TEST_FEAST_URL unset");
            return;
        };
        let feast = FeastClient::new(url);
        assert!(feast.health());
        let first = 9_000_000_000_000 + (std::process::id() as i64) * 10;
        let when = 1_735_689_600_000_000i64; // 2025-01-01T00:00:00Z
        let frame = DataFrame::new_infer_height(vec![
            Column::new("driver_id".into(), [first, first + 1]),
            utc(Column::new("event_timestamp".into(), [when, when])),
            utc(Column::new("created".into(), [when, when])),
            Column::new("conv_rate".into(), [0.5f64, 0.5]),
            Column::new("acc_rate".into(), [0.25f64, 0.25]),
            Column::new("avg_daily_trips".into(), [Some((1i64 << 60) + 1), None]),
            Column::new("city".into(), ["c", "c"]),
            Column::new("active".into(), [true, true]),
        ])
        .unwrap();
        feast
            .push("driver_stats_push", &frame, "event_timestamp")
            .unwrap();
        let entities =
            DataFrame::new_infer_height(vec![Column::new("driver_id".into(), [first, first + 1])])
                .unwrap();
        let read = feast
            .get_online_features(&entities, &["driver_hourly_stats:avg_daily_trips"])
            .unwrap();
        let trips = read.column("avg_daily_trips").unwrap();
        assert_eq!(trips.get(0).unwrap(), AnyValue::Int64((1i64 << 60) + 1));
        assert_eq!(trips.get(1).unwrap(), AnyValue::Null);
    }
}
