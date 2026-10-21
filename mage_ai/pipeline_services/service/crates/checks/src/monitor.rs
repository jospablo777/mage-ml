//! Monitors: a block's output profiled on every run (crate::profile) and compared with a
//! baseline run's profile, check by check: row counts, missing values, drift of each
//! column's distribution, the newest timestamp, and a model's performance where the output
//! holds labels. Each check passes, warns, fails, or holds when there is too little data to
//! decide.
//!
//! MageML runs (mage_ai/orchestration/monitors.py, through ColumnAtlas) and exported
//! services use this one implementation; the settings are the monitor as MageML parsed
//! its YAML file.

use std::collections::{BTreeMap, HashMap};

use polars::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::error::{Error, guarded};
use crate::table::Table;

const RESULTS: [&str; 4] = ["pass", "hold", "warn", "fail"];
/// Metrics where a larger value is worse.
const LOWER_IS_BETTER: [&str; 4] = ["log_loss", "mae", "rmse", "mape"];

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Drift {
    pub method: String,
    pub warn: f64,
    pub fail: f64,
}

impl Default for Drift {
    fn default() -> Self {
        Drift {
            method: "js".into(),
            warn: 0.1,
            fail: 0.2,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Missing {
    pub max: Option<f64>,
    pub max_increase: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Rows {
    pub min: Option<f64>,
    pub max_change: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Freshness {
    pub column: Option<String>,
    /// Seconds.
    pub max_age: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ColumnRules {
    pub drift: Option<Drift>,
    pub missing: Option<Missing>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Performance {
    pub task: String,
    pub target: String,
    pub prediction: String,
    pub score: Option<String>,
    pub positive: Value,
    pub metrics: Vec<String>,
    pub min: HashMap<String, f64>,
    pub max: HashMap<String, f64>,
    pub max_drop: HashMap<String, f64>,
}

/// A monitor's settings, as MageML's load_monitor gives them (its other settings, such as
/// the engines, are read and left alone).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Monitor {
    pub name: String,
    pub output: String,
    pub baseline: String,
    pub columns: Option<Vec<String>>,
    pub drift: Drift,
    pub missing: Missing,
    pub rows: Rows,
    pub freshness: Freshness,
    pub min_rows: u64,
    pub fail_block: bool,
    pub bins: usize,
    pub top: usize,
    pub column_rules: HashMap<String, ColumnRules>,
    pub rolling: usize,
    /// With a previous baseline, the run this many runs back ends the baseline: 7 compares
    /// a daily run with the same weekday a week earlier.
    pub lag: usize,
    pub performance: Performance,
}

impl Default for Monitor {
    fn default() -> Self {
        Monitor {
            name: String::new(),
            output: "output_0".into(),
            baseline: "previous".into(),
            columns: None,
            drift: Drift::default(),
            missing: Missing::default(),
            rows: Rows::default(),
            freshness: Freshness::default(),
            min_rows: 30,
            fail_block: false,
            bins: 20,
            top: 20,
            column_rules: HashMap::new(),
            rolling: 1,
            lag: 1,
            performance: Performance::default(),
        }
    }
}

impl Monitor {
    pub fn parse(text: &str) -> Result<Monitor, Error> {
        serde_json::from_str(text)
            .map_err(|error| Error::invalid(format!("Invalid monitor settings: {error}")))
    }

    /// The earlier runs (of `count`, oldest first) a previous baseline adds up: `rolling`
    /// runs ending `lag` runs back. Empty while there are fewer than `lag` runs.
    pub fn baseline_window(&self, count: usize) -> std::ops::Range<usize> {
        let lag = self.lag.max(1);
        if count < lag {
            return 0..0;
        }
        let end = count + 1 - lag;
        end.saturating_sub(self.rolling.max(1))..end
    }

    pub fn has_performance(&self) -> bool {
        !self.performance.target.is_empty()
    }

    /// The profile request of a run: the monitor's columns (and its freshness column), bins
    /// and top values, over the reference edges.
    pub fn profile_request(&self, edges: &Value) -> Value {
        let mut request = json!({"bins": self.bins, "top": self.top, "edges": edges});
        if let Some(columns) = &self.columns {
            let mut columns = columns.clone();
            if let Some(column) = &self.freshness.column
                && !columns.contains(column)
            {
                columns.push(column.clone());
            }
            request["columns"] = json!(columns);
        }
        request
    }
}

/// Python's round(value, digits): the nearest decimal, ties to even.
fn round_to(value: f64, digits: usize) -> f64 {
    format!("{value:.digits$}").parse().unwrap_or(value)
}

/// Python's format(value, '.{precision}g').
fn general(value: f64, precision: usize) -> String {
    if value == 0.0 {
        return "0".into();
    }
    if !value.is_finite() {
        return if value.is_nan() {
            "nan".into()
        } else if value > 0.0 {
            "inf".into()
        } else {
            "-inf".into()
        };
    }
    let scientific = format!("{:.*e}", precision.saturating_sub(1), value);
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let trim = |text: &str| -> String {
        if text.contains('.') {
            text.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            text.to_string()
        }
    };
    if exponent < -4 || exponent >= precision as i32 {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim(mantissa), exponent.abs())
    } else {
        let decimals = (precision as i32 - 1 - exponent).max(0) as usize;
        trim(&format!("{value:.decimals$}"))
    }
}

fn percent(share: f64) -> String {
    format!("{:.1}%", share * 100.0)
}

fn number(value: &Value) -> f64 {
    value.as_f64().unwrap_or(0.0)
}

fn worst<'a>(results: impl Iterator<Item = &'a str>) -> &'static str {
    results
        .filter_map(|r| RESULTS.iter().position(|known| *known == r))
        .max()
        .map(|i| RESULTS[i])
        .unwrap_or("pass")
}

fn check(name: &str, column: Option<&str>, value: Value, result: &str, detail: String) -> Value {
    json!({"check": name, "column": column, "value": value, "result": result, "detail": detail})
}

/// Jensen-Shannon divergence (natural log) of two count vectors; None if one is empty.
pub fn jensen_shannon(first: &[f64], second: &[f64]) -> Option<f64> {
    let (total_a, total_b): (f64, f64) = (first.iter().sum(), second.iter().sum());
    if total_a <= 0.0 || total_b <= 0.0 {
        return None;
    }
    let p: Vec<f64> = first.iter().map(|a| a / total_a).collect();
    let q: Vec<f64> = second.iter().map(|b| b / total_b).collect();
    let m: Vec<f64> = p.iter().zip(&q).map(|(a, b)| (a + b) / 2.0).collect();
    let kl = |x: &[f64]| -> f64 {
        x.iter()
            .zip(&m)
            .filter(|(a, _)| **a > 0.0)
            .map(|(a, b)| a * (a / b).ln())
            .sum()
    };
    Some(((kl(&p) + kl(&q)) / 2.0).max(0.0))
}

/// Population stability index of two count vectors; an empty bin counts as 0.0001 of its
/// side, the usual guard against log(0); None if one side is empty.
pub fn population_stability(first: &[f64], second: &[f64]) -> Option<f64> {
    let (total_a, total_b): (f64, f64) = (first.iter().sum(), second.iter().sum());
    if total_a <= 0.0 || total_b <= 0.0 {
        return None;
    }
    Some(
        first
            .iter()
            .zip(second)
            .map(|(a, b)| {
                let p = (a / total_a).max(1e-4);
                let q = (b / total_b).max(1e-4);
                (q - p) * (q / p).ln()
            })
            .sum(),
    )
}

fn divergence_name(method: &str) -> &'static str {
    if method == "psi" {
        "population stability index"
    } else {
        "Jensen-Shannon divergence"
    }
}

fn top_counts(column: &Value) -> Vec<(String, f64)> {
    column["top"]
        .as_array()
        .map(|pairs| {
            pairs
                .iter()
                .map(|pair| {
                    let value = match &pair[0] {
                        Value::String(text) => text.clone(),
                        other => other.to_string(),
                    };
                    (value, number(&pair[1]))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn category_counts(column: &Value, keys: &[String]) -> Vec<f64> {
    let top: HashMap<String, f64> = top_counts(column).into_iter().collect();
    let listed: f64 = top.values().sum();
    let kept: f64 = keys
        .iter()
        .map(|k| top.get(k).copied().unwrap_or(0.0))
        .sum();
    let other = number(&column["other"]) + listed - kept;
    keys.iter()
        .map(|k| top.get(k).copied().unwrap_or(0.0))
        .chain(std::iter::once(other))
        .collect()
}

fn parse_time(text: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let text = text.replace('Z', "+00:00").replace(" UTC", "+00:00");
    if let Ok(time) = chrono::DateTime::parse_from_rfc3339(&text.replacen(' ', "T", 1)) {
        return Some(time.with_timezone(&chrono::Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%dT%H:%M:%S%.f%:z"] {
        if let Ok(time) = chrono::DateTime::parse_from_str(&text, format) {
            return Some(time.with_timezone(&chrono::Utc));
        }
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(time) = chrono::NaiveDateTime::parse_from_str(&text, format) {
            return Some(time.and_utc());
        }
    }
    chrono::NaiveDate::parse_from_str(&text, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|time| time.and_utc())
}

/// Each check of the monitor for the current profile against the baseline's.
pub fn compare(
    monitor: &Monitor,
    current: &Value,
    baseline: Option<&Value>,
    execution_date: Option<chrono::DateTime<chrono::Utc>>,
) -> Value {
    let mut checks = Vec::new();
    let rows = current["rows"].as_u64().unwrap_or(0);
    let baseline_rows = baseline.and_then(|b| b["rows"].as_u64());
    let enough = rows >= monitor.min_rows && baseline_rows.unwrap_or(0) >= monitor.min_rows;

    if let Some(min) = monitor.rows.min {
        checks.push(check(
            "rows",
            None,
            json!(rows),
            if (rows as f64) < min { "fail" } else { "pass" },
            format!("{rows} rows; at least {}", min.trunc() as i64),
        ));
    }
    if let Some(max_change) = monitor.rows.max_change {
        match baseline_rows.filter(|b| *b > 0) {
            None => checks.push(check(
                "row_change",
                None,
                Value::Null,
                "hold",
                "no baseline rows to compare with".into(),
            )),
            Some(before) => {
                let change = (rows as f64 - before as f64).abs() / before as f64;
                checks.push(check(
                    "row_change",
                    None,
                    json!(round_to(change, 4)),
                    if change > max_change { "fail" } else { "pass" },
                    format!("{rows} rows against {before} ({} change)", percent(change)),
                ));
            }
        }
    }

    let empty = Vec::new();
    let baseline_columns: HashMap<&str, &Value> = baseline
        .and_then(|b| b["columns"].as_array())
        .unwrap_or(&empty)
        .iter()
        .filter_map(|c| c["name"].as_str().map(|name| (name, c)))
        .collect();
    for column in current["columns"].as_array().unwrap_or(&empty) {
        let name = column["name"].as_str().unwrap_or("");
        let rules = monitor.column_rules.get(name);
        let missing_rules = rules
            .and_then(|r| r.missing.as_ref())
            .unwrap_or(&monitor.missing);
        let drift_rules = rules
            .and_then(|r| r.drift.as_ref())
            .unwrap_or(&monitor.drift);
        let column_rows = number(&column["rows"]);
        let share = if column_rows > 0.0 {
            number(&column["missing"]) / column_rows
        } else {
            0.0
        };
        let reference = baseline_columns.get(name).copied();
        if let Some(max) = missing_rules.max {
            checks.push(check(
                "missing",
                Some(name),
                json!(round_to(share, 4)),
                if share > max { "fail" } else { "pass" },
                format!("{} missing; at most {}", percent(share), percent(max)),
            ));
        }
        if let (Some(max_increase), Some(reference)) = (missing_rules.max_increase, reference) {
            let reference_rows = number(&reference["rows"]);
            let before = if reference_rows > 0.0 {
                number(&reference["missing"]) / reference_rows
            } else {
                0.0
            };
            let increase = share - before;
            checks.push(check(
                "missing_increase",
                Some(name),
                json!(round_to(increase, 4)),
                if increase > max_increase {
                    "fail"
                } else {
                    "pass"
                },
                format!("{} missing against {}", percent(share), percent(before)),
            ));
        }

        let kind = column["kind"].as_str().unwrap_or("");
        if !matches!(kind, "numeric" | "string" | "boolean") {
            continue;
        }
        let Some(reference) = reference else {
            checks.push(check(
                "drift",
                Some(name),
                Value::Null,
                "hold",
                "no baseline for this column".into(),
            ));
            continue;
        };
        if !enough {
            checks.push(check(
                "drift",
                Some(name),
                Value::Null,
                "hold",
                format!(
                    "too few rows to compare ({rows} now, {} in the baseline; {} needed)",
                    baseline_rows.unwrap_or(0),
                    monitor.min_rows
                ),
            ));
            continue;
        }
        let (first, second) = if kind == "numeric" {
            let counts = |value: &Value| -> Vec<f64> {
                value["counts"]
                    .as_array()
                    .map(|c| c.iter().map(number).collect())
                    .unwrap_or_default()
            };
            (counts(reference), counts(column))
        } else {
            let keys: Vec<String> = top_counts(reference).into_iter().map(|(k, _)| k).collect();
            (
                category_counts(reference, &keys),
                category_counts(column, &keys),
            )
        };
        let method = drift_rules.method.as_str();
        let divergence = if first.len() == second.len() {
            if method == "psi" {
                population_stability(&first, &second)
            } else {
                jensen_shannon(&first, &second)
            }
        } else {
            None
        };
        let Some(divergence) = divergence else {
            checks.push(check(
                "drift",
                Some(name),
                Value::Null,
                "hold",
                "no values to compare".into(),
            ));
            continue;
        };
        let result = if divergence >= drift_rules.fail {
            "fail"
        } else if divergence >= drift_rules.warn {
            "warn"
        } else {
            "pass"
        };
        let mut entry = check(
            "drift",
            Some(name),
            json!(round_to(divergence, 4)),
            result,
            format!("{} {divergence:.4}", divergence_name(method)),
        );
        entry["method"] = json!(method);
        checks.push(entry);
    }

    if let Some(name) = &monitor.freshness.column {
        let column = current["columns"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .find(|c| c["name"].as_str() == Some(name));
        let newest = column.and_then(|c| c["max"].as_str()).and_then(parse_time);
        let reference = execution_date.unwrap_or_else(chrono::Utc::now);
        match newest {
            None => checks.push(check(
                "freshness",
                Some(name),
                Value::Null,
                "hold",
                "no timestamps to check".into(),
            )),
            Some(newest) => {
                let age = (reference - newest).num_microseconds().unwrap_or(i64::MAX) as f64
                    / 1_000_000.0;
                let max_age = monitor.freshness.max_age.unwrap_or(f64::INFINITY);
                checks.push(check(
                    "freshness",
                    Some(name),
                    json!(age.round_ties_even() as i64),
                    if age > max_age { "fail" } else { "pass" },
                    format!("newest value {:.1} h before the run", age / 3600.0),
                ));
            }
        }
    }
    checks.extend(performance_checks(
        monitor,
        current.get("performance"),
        baseline.and_then(|b| b.get("performance")),
    ));
    let result = worst(checks.iter().filter_map(|c| c["result"].as_str()));
    json!({"result": result, "checks": checks})
}

fn performance_checks(
    monitor: &Monitor,
    current: Option<&Value>,
    baseline: Option<&Value>,
) -> Vec<Value> {
    if !monitor.has_performance() {
        return Vec::new();
    }
    let settings = &monitor.performance;
    let current = current.filter(|c| !c.is_null());
    let error = current.and_then(|c| c["error"].as_str());
    let Some(current) = current.filter(|_| error.is_none()) else {
        return vec![check(
            "performance",
            None,
            Value::Null,
            "hold",
            error.unwrap_or("no labeled rows").into(),
        )];
    };
    let mut checks = Vec::new();
    for metric in &settings.metrics {
        let Some(value) = current[metric.as_str()].as_f64().filter(|v| v.is_finite()) else {
            checks.push(check(
                metric,
                None,
                Value::Null,
                "hold",
                format!("{metric} could not be computed"),
            ));
            continue;
        };
        let mut result = "pass";
        let mut reasons = Vec::new();
        if let Some(min) = settings.min.get(metric)
            && value < *min
        {
            result = "fail";
            reasons.push(format!("below the min {}", python_number(*min)));
        }
        if let Some(max) = settings.max.get(metric)
            && value > *max
        {
            result = "fail";
            reasons.push(format!("above the max {}", python_number(*max)));
        }
        let before = baseline.and_then(|b| b[metric.as_str()].as_f64());
        if let (Some(max_drop), Some(before)) = (settings.max_drop.get(metric), before) {
            let drop = if LOWER_IS_BETTER.contains(&metric.as_str()) {
                value - before
            } else {
                before - value
            };
            if drop > *max_drop {
                result = "fail";
                reasons.push(format!(
                    "{} worse than the baseline ({})",
                    general(drop, 4),
                    general(before, 4)
                ));
            }
        }
        let rows = current["rows"].as_u64().unwrap_or(0);
        let mut detail = format!("{metric} {} on {rows} labeled rows", general(value, 4));
        if !reasons.is_empty() {
            detail.push_str(&format!("; {}", reasons.join("; ")));
        }
        checks.push(check(
            metric,
            None,
            json!(round_to(value, 6)),
            result,
            detail,
        ));
    }
    checks
}

/// A float as Python prints it in an f-string: 0.8, 1.0, 1e-05.
fn python_number(value: f64) -> String {
    let text = format!("{value:?}");
    if value.fract() == 0.0 && value.abs() < 1e16 && !text.contains('e') {
        format!("{value:.1}")
    } else {
        text
    }
}

/// One baseline record from several, whose profiles share their bin edges: the rolling
/// baseline of a monitor (records oldest first, each with its profile and run id).
pub fn merge(records: &[Value]) -> Value {
    if records.len() == 1 {
        return records[0].clone();
    }
    let Some(newest) = records.last() else {
        return Value::Null;
    };
    let mut order: Vec<String> = Vec::new();
    let mut columns: HashMap<String, Value> = HashMap::new();
    for record in records {
        for column in record["profile"]["columns"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let name = column["name"].as_str().unwrap_or("").to_string();
            let Some(merged) = columns.get_mut(&name) else {
                order.push(name.clone());
                columns.insert(name, column.clone());
                continue;
            };
            merged["rows"] = json!(number(&merged["rows"]) as u64 + number(&column["rows"]) as u64);
            merged["missing"] =
                json!(number(&merged["missing"]) as u64 + number(&column["missing"]) as u64);
            let has_counts = column["counts"].as_array().is_some_and(|c| !c.is_empty());
            if has_counts && merged["edges"] == column["edges"] {
                let summed: Vec<u64> = merged["counts"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .zip(column["counts"].as_array().into_iter().flatten())
                    .map(|(a, b)| number(a) as u64 + number(b) as u64)
                    .collect();
                merged["counts"] = json!(summed);
            }
            if column.get("top").is_some() {
                let mut tally: BTreeMap<String, u64> = BTreeMap::new();
                for (value, count) in top_counts(merged).into_iter().chain(top_counts(column)) {
                    *tally.entry(value).or_default() += count as u64;
                }
                let mut top: Vec<(String, u64)> = tally.into_iter().collect();
                top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                merged["top"] = json!(
                    top.into_iter()
                        .map(|(value, count)| json!([value, count]))
                        .collect::<Vec<_>>()
                );
                merged["other"] =
                    json!(number(&merged["other"]) as u64 + number(&column["other"]) as u64);
            }
        }
    }
    let mut profile = json!({
        "rows": records.iter().map(|r| number(&r["profile"]["rows"]) as u64).sum::<u64>(),
        "columns": order.iter().map(|name| columns[name].clone()).collect::<Vec<_>>(),
    });
    let performances: Vec<&Value> = records
        .iter()
        .map(|r| &r["profile"]["performance"])
        .collect();
    if performances
        .iter()
        .all(|p| p.is_object() && p.get("error").is_none_or(Value::is_null))
    {
        let weights: Vec<f64> = performances.iter().map(|p| number(&p["rows"])).collect();
        let total = weights.iter().sum::<f64>().max(1.0);
        let mut keys: Vec<String> = performances
            .iter()
            .flat_map(|p| p.as_object().into_iter().flatten())
            .filter(|(key, value)| value.is_number() && key.as_str() != "rows")
            .map(|(key, _)| key.clone())
            .collect();
        keys.sort();
        keys.dedup();
        let mut merged = Map::new();
        for key in keys {
            let sum: f64 = performances
                .iter()
                .zip(&weights)
                .map(|(p, w)| p[key.as_str()].as_f64().unwrap_or(0.0) * w)
                .sum();
            merged.insert(key, json!(sum / total));
        }
        profile["performance"] = Value::Object(merged);
    }
    let mut record = newest.clone();
    record["profile"] = profile;
    record["baseline_runs"] = json!(
        records
            .iter()
            .map(|r| r["pipeline_run_id"].clone())
            .collect::<Vec<_>>()
    );
    record["baseline_records"] = json!(
        records
            .iter()
            .map(|r| json!({"partition": r.get("partition").cloned().unwrap_or(Value::Null)}))
            .collect::<Vec<_>>()
    );
    record
}

// ---- performance ----------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum Label {
    Number(f64),
    Text(String),
}

impl Label {
    fn from_any(value: AnyValue<'_>) -> Option<Label> {
        match value {
            AnyValue::Null => None,
            AnyValue::Boolean(b) => Some(Label::Number(if b { 1.0 } else { 0.0 })),
            AnyValue::String(text) => Some(Label::Text(text.to_string())),
            AnyValue::StringOwned(text) => Some(Label::Text(text.to_string())),
            other => other
                .extract::<f64>()
                .filter(|v| !v.is_nan())
                .map(Label::Number),
        }
    }

    fn from_json(value: &Value) -> Label {
        match value {
            Value::Bool(b) => Label::Number(if *b { 1.0 } else { 0.0 }),
            Value::Number(n) => Label::Number(n.as_f64().unwrap_or(0.0)),
            Value::String(text) => Label::Text(text.clone()),
            other => Label::Text(other.to_string()),
        }
    }

    fn number(&self) -> Option<f64> {
        match self {
            Label::Number(n) => Some(*n),
            Label::Text(_) => None,
        }
    }
}

fn labels(frame: &DataFrame, name: &str) -> Result<Vec<Option<Label>>, Error> {
    let column = frame.column(name)?.as_materialized_series().clone();
    Ok((0..column.len())
        .map(|i| column.get(i).ok().and_then(Label::from_any))
        .collect())
}

fn distinct(values: &[&Label]) -> Vec<Label> {
    let mut seen: Vec<Label> = Vec::new();
    for value in values {
        if !seen.contains(value) {
            seen.push((*value).clone());
        }
    }
    seen
}

/// Precision, recall and F1 of one class, 0 where undefined (sklearn's zero_division=0).
fn class_scores(target: &[&Label], prediction: &[&Label], class: &Label) -> (f64, f64, f64) {
    let (mut tp, mut fp, mut fn_) = (0.0, 0.0, 0.0);
    for (t, p) in target.iter().zip(prediction) {
        match (*t == class, *p == class) {
            (true, true) => tp += 1.0,
            (false, true) => fp += 1.0,
            (true, false) => fn_ += 1.0,
            _ => {}
        }
    }
    let precision = if tp + fp > 0.0 { tp / (tp + fp) } else { 0.0 };
    let recall = if tp + fn_ > 0.0 { tp / (tp + fn_) } else { 0.0 };
    let f1 = if 2.0 * tp + fp + fn_ > 0.0 {
        2.0 * tp / (2.0 * tp + fp + fn_)
    } else {
        0.0
    };
    (precision, recall, f1)
}

/// The area under the ROC curve: the Mann-Whitney statistic, ties at half (sklearn's
/// roc_auc_score). None with one class only.
fn roc_auc(positive: &[bool], score: &[f64]) -> Option<f64> {
    let positives = positive.iter().filter(|p| **p).count() as f64;
    let negatives = positive.len() as f64 - positives;
    if positives == 0.0 || negatives == 0.0 {
        return None;
    }
    let mut order: Vec<usize> = (0..score.len()).collect();
    order.sort_by(|a, b| score[*a].total_cmp(&score[*b]));
    let mut ranks = vec![0.0; score.len()];
    let mut i = 0;
    while i < order.len() {
        let mut j = i;
        while j + 1 < order.len() && score[order[j + 1]] == score[order[i]] {
            j += 1;
        }
        let rank = (i + j) as f64 / 2.0 + 1.0;
        for k in i..=j {
            ranks[order[k]] = rank;
        }
        i = j + 1;
    }
    let positive_ranks: f64 = ranks
        .iter()
        .zip(positive)
        .filter(|(_, p)| **p)
        .map(|(r, _)| r)
        .sum();
    Some((positive_ranks - positives * (positives + 1.0) / 2.0) / (positives * negatives))
}

fn metric(
    name: &str,
    settings: &Performance,
    target: &[&Label],
    prediction: &[&Label],
    score: &[f64],
) -> Option<f64> {
    let n = target.len() as f64;
    let positive = Label::from_json(&settings.positive);
    let numeric = || -> Option<(Vec<f64>, Vec<f64>)> {
        let t: Option<Vec<f64>> = target.iter().map(|l| l.number()).collect();
        let p: Option<Vec<f64>> = prediction.iter().map(|l| l.number()).collect();
        Some((t?, p?))
    };
    match name {
        "accuracy" => Some(
            target
                .iter()
                .zip(prediction)
                .filter(|(t, p)| t == p)
                .count() as f64
                / n,
        ),
        "precision" | "recall" | "f1" => {
            let classes = distinct(target);
            let pick = |(precision, recall, f1): (f64, f64, f64)| match name {
                "precision" => precision,
                "recall" => recall,
                _ => f1,
            };
            if classes.len() <= 2 && classes.contains(&positive) {
                Some(pick(class_scores(target, prediction, &positive)))
            } else {
                let mut all: Vec<&Label> = target.to_vec();
                all.extend(prediction.iter().copied());
                let mut labels = distinct(&all);
                labels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let total: f64 = labels
                    .iter()
                    .map(|class| pick(class_scores(target, prediction, class)))
                    .sum();
                Some(total / labels.len() as f64)
            }
        }
        "roc_auc" => {
            let truth: Vec<bool> = target.iter().map(|t| **t == positive).collect();
            roc_auc(&truth, score)
        }
        "log_loss" => {
            let eps = f64::EPSILON;
            let total: f64 = target
                .iter()
                .zip(score)
                .map(|(t, s)| {
                    let p = s.clamp(eps, 1.0 - eps);
                    if **t == positive {
                        -p.ln()
                    } else {
                        -(1.0 - p).ln()
                    }
                })
                .sum();
            Some(total / n)
        }
        "mae" | "rmse" | "mape" | "r2" => {
            let (t, p) = numeric()?;
            match name {
                "mae" => Some(t.iter().zip(&p).map(|(a, b)| (a - b).abs()).sum::<f64>() / n),
                "rmse" => {
                    Some((t.iter().zip(&p).map(|(a, b)| (a - b).powi(2)).sum::<f64>() / n).sqrt())
                }
                "mape" => Some(
                    t.iter()
                        .zip(&p)
                        .map(|(a, b)| (a - b).abs() / a.abs().max(f64::EPSILON))
                        .sum::<f64>()
                        / n,
                ),
                _ => {
                    let mean = t.iter().sum::<f64>() / n;
                    let residual: f64 = t.iter().zip(&p).map(|(a, b)| (a - b).powi(2)).sum();
                    let total: f64 = t.iter().map(|a| (a - mean).powi(2)).sum();
                    Some(if total == 0.0 {
                        if residual == 0.0 { 1.0 } else { 0.0 }
                    } else {
                        1.0 - residual / total
                    })
                }
            }
        }
        _ => None,
    }
}

/// The performance metrics of a model's predictions in an output with their labels: the
/// rows with a label, a prediction (and a score where one is set), as MageML computed them
/// with scikit-learn.
pub fn performance(settings: &Performance, path: &str) -> Value {
    match guarded(path, || performance_unguarded(settings, path)) {
        Ok(value) => value,
        Err(error) => json!({"error": format!(
            "the performance columns could not be read: {}", error.message()
        )}),
    }
}

fn performance_unguarded(settings: &Performance, path: &str) -> Result<Value, Error> {
    let table = Table::open(path, 0)?;
    let mut names = vec![settings.target.clone(), settings.prediction.clone()];
    if let Some(score) = &settings.score
        && !names.contains(score)
    {
        names.push(score.clone());
    }
    let frame = table.collect(
        table
            .scan()?
            .select(names.iter().map(|n| col(n.as_str())).collect::<Vec<_>>()),
    )?;
    let target = labels(&frame, &settings.target)?;
    let prediction = labels(&frame, &settings.prediction)?;
    let score: Vec<Option<f64>> = match &settings.score {
        Some(name) => labels(&frame, name)?
            .into_iter()
            .map(|l| l.and_then(|l| l.number()))
            .collect(),
        None => vec![Some(0.0); target.len()],
    };
    let mut rows: Vec<(&Label, &Label, f64)> = Vec::new();
    for ((t, p), s) in target.iter().zip(&prediction).zip(&score) {
        if let (Some(t), Some(p), Some(s)) = (t, p, s) {
            rows.push((t, p, *s));
        }
    }
    if rows.is_empty() {
        return Ok(json!({"error": "no rows have both a label and a prediction", "rows": 0}));
    }
    let target: Vec<&Label> = rows.iter().map(|r| r.0).collect();
    let prediction: Vec<&Label> = rows.iter().map(|r| r.1).collect();
    let score: Vec<f64> = rows.iter().map(|r| r.2).collect();
    let mut result = Map::new();
    result.insert("rows".into(), json!(rows.len()));
    for name in &settings.metrics {
        let value = metric(name, settings, &target, &prediction, &score).filter(|v| v.is_finite());
        result.insert(name.clone(), json!(value));
    }
    Ok(Value::Object(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitor(settings: Value) -> Monitor {
        Monitor::parse(&settings.to_string()).unwrap()
    }

    #[test]
    fn divergences_match_the_formulas() {
        assert_eq!(jensen_shannon(&[1.0, 1.0], &[1.0, 1.0]), Some(0.0));
        let js = jensen_shannon(&[1.0, 0.0], &[0.0, 1.0]).unwrap();
        assert!((js - std::f64::consts::LN_2).abs() < 1e-12);
        assert_eq!(jensen_shannon(&[0.0], &[1.0]), None);
        let psi = population_stability(&[50.0, 50.0], &[90.0, 10.0]).unwrap();
        let expected = (0.9 - 0.5) * (0.9f64 / 0.5).ln() + (0.1 - 0.5) * (0.1f64 / 0.5).ln();
        assert!((psi - expected).abs() < 1e-12);
    }

    #[test]
    fn numbers_print_as_python_prints_them() {
        assert_eq!(general(0.123456, 4), "0.1235");
        assert_eq!(general(12345.0, 4), "1.234e+04");
        assert_eq!(general(0.00001234, 4), "1.234e-05");
        assert_eq!(general(2.0, 4), "2");
        assert_eq!(python_number(0.8), "0.8");
        assert_eq!(python_number(1.0), "1.0");
        assert_eq!(round_to(0.12345678, 4), 0.1235);
        assert_eq!(percent(0.0625), "6.2%");
    }

    #[test]
    fn rows_missing_and_drift_are_checked_against_the_baseline() {
        let watcher = monitor(json!({
            "name": "scores", "min_rows": 2,
            "rows": {"min": 3, "max_change": 0.5},
            "missing": {"max": 0.2, "max_increase": 0.05},
        }));
        let baseline = json!({"rows": 4, "columns": [
            {"name": "x", "kind": "numeric", "rows": 4, "missing": 0, "edges": [1.0],
             "counts": [2, 2]},
            {"name": "s", "kind": "string", "rows": 4, "missing": 0,
             "top": [["a", 2], ["b", 2]], "other": 0},
        ]});
        let current = json!({"rows": 2, "columns": [
            {"name": "x", "kind": "numeric", "rows": 2, "missing": 1, "edges": [1.0],
             "counts": [0, 1]},
            {"name": "s", "kind": "string", "rows": 2, "missing": 0,
             "top": [["c", 2]], "other": 0},
        ]});
        let result = compare(&watcher, &current, Some(&baseline), None);
        assert_eq!(result["result"], "fail");
        let checks: Vec<(String, String)> = result["checks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    format!("{} {}", c["check"].as_str().unwrap(), c["column"]),
                    c["result"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert!(
            checks.contains(&("rows null".into(), "fail".into())),
            "{checks:?}"
        );
        assert!(
            checks.contains(&("row_change null".into(), "pass".into())),
            "{checks:?}"
        );
        assert!(
            checks.contains(&("missing \"x\"".into(), "fail".into())),
            "{checks:?}"
        );
        assert!(
            checks.contains(&("drift \"s\"".into(), "fail".into())),
            "{checks:?}"
        );
        let rows = &result["checks"][0];
        assert_eq!(rows["detail"], "2 rows; at least 3");
    }

    #[test]
    fn the_baseline_window_ends_lag_runs_back() {
        let watcher = |rolling, lag| Monitor {
            rolling,
            lag,
            ..Default::default()
        };
        assert_eq!(watcher(1, 1).baseline_window(5), 4..5);
        assert_eq!(watcher(3, 1).baseline_window(5), 2..5);
        assert_eq!(watcher(1, 3).baseline_window(5), 2..3);
        assert_eq!(watcher(2, 3).baseline_window(5), 1..3);
        assert_eq!(watcher(1, 7).baseline_window(6), 0..0);
        assert_eq!(watcher(9, 2).baseline_window(4), 0..3);
    }

    #[test]
    fn a_rolling_baseline_sums_counts_and_tops() {
        let record = |run: u64, counts: [u64; 2], top: Value| {
            json!({"pipeline_run_id": run, "partition": format!("p{run}"), "profile": {
                "rows": 4, "columns": [
                    {"name": "x", "kind": "numeric", "rows": 4, "missing": 0, "edges": [1.0],
                     "counts": counts},
                    {"name": "s", "kind": "string", "rows": 4, "missing": 0, "top": top,
                     "other": 1},
                ],
            }})
        };
        let merged = merge(&[
            record(1, [1, 3], json!([["a", 3]])),
            record(2, [2, 2], json!([["b", 2], ["a", 1]])),
        ]);
        assert_eq!(merged["pipeline_run_id"], 2);
        assert_eq!(merged["baseline_runs"], json!([1, 2]));
        assert_eq!(merged["profile"]["rows"], 8);
        assert_eq!(merged["profile"]["columns"][0]["counts"], json!([3, 5]));
        assert_eq!(
            merged["profile"]["columns"][1]["top"],
            json!([["a", 4], ["b", 2]])
        );
        assert_eq!(merged["profile"]["columns"][1]["other"], 2);
    }

    #[test]
    fn classification_and_regression_metrics() {
        let settings = Performance {
            target: "y".into(),
            prediction: "p".into(),
            score: Some("s".into()),
            positive: json!(1),
            metrics: vec![
                "accuracy".into(),
                "precision".into(),
                "recall".into(),
                "f1".into(),
                "roc_auc".into(),
                "log_loss".into(),
            ],
            ..Default::default()
        };
        let mut frame = df!(
            "y" => [Some(1i64), Some(0), Some(1), Some(0), None],
            "p" => [1i64, 1, 1, 0, 1],
            "s" => [0.9f64, 0.6, 0.4, 0.2, 0.5],
        )
        .unwrap();
        let file = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();
        ParquetWriter::new(std::fs::File::create(file.path()).unwrap())
            .finish(&mut frame)
            .unwrap();
        let result = performance(&settings, file.path().to_str().unwrap());
        assert_eq!(result["rows"], 4);
        assert_eq!(result["accuracy"], 0.75);
        assert!((result["precision"].as_f64().unwrap() - 2.0 / 3.0).abs() < 1e-12);
        assert_eq!(result["recall"], 1.0);
        assert!((result["f1"].as_f64().unwrap() - 0.8).abs() < 1e-12);
        assert_eq!(result["roc_auc"], 0.75);
        let expected = -((0.9f64).ln() + (0.4f64).ln() + (0.4f64).ln() + (0.8f64).ln()) / 4.0;
        assert!((result["log_loss"].as_f64().unwrap() - expected).abs() < 1e-12);
    }
}
