//! GET /api/monitor_stats/<stats type>: the overview's and the monitor pages' counts and
//! times, as MonitorStatResource and MonitorStats answer them
//! (mage_ai/orchestration/monitor/monitor_stats.py).
//!
//! - `pipeline_run_count`: per trigger, per day, runs by status, or by pipeline type and
//!   status with `group_by_pipeline_type`.
//! - `pipeline_run_time`: per day, the mean time of the finished runs, under the pipeline.
//! - `block_run_count`: per block, per day, block runs by status.
//! - `block_run_time`: per block, per day, the mean time of the finished block runs.
//!
//! Every query reads the runs of this project's triggers between `start_time` (30 days
//! before `end_time` by default) and `end_time` (now). Days are the database's: `to_char`
//! on PostgreSQL, `strftime` on SQLite, as the server reads them. The means add the runs in
//! the database's order, which the server does too, so they can differ in the last bits:
//! the comparison of this route allows for it (`close`).

use std::collections::BTreeMap;
use std::path::Path;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_json::{Map, Value, json};

use crate::gateway_shadow::{Metadata, Query, Raw, read_rows};

pub const STATS_TYPES: &[&str] = &[
    "block_run_count",
    "block_run_time",
    "pipeline_run_count",
    "pipeline_run_time",
];

/// The stats type of /api/monitor_stats/<type>.
pub fn stats_type(path: &str) -> Option<&str> {
    let kind = path
        .trim_end_matches('/')
        .strip_prefix("/api/monitor_stats/")?;
    (!kind.is_empty() && !kind.contains('/')).then_some(kind)
}

/// A time as dateutil reads the ISO texts the pages send: `2026-10-22`,
/// `2026-10-22T08:00:00`, with fractions, `Z` or an offset. None for any other text, which
/// the gateway leaves to the server.
fn parse_time(text: &str) -> Option<Time> {
    let text = text.trim();
    if let Ok(day) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Some(Time::Naive(day.and_hms_opt(0, 0, 0)?));
    }
    let with_t = text.replacen(' ', "T", 1);
    if let Ok(at) = DateTime::parse_from_rfc3339(&with_t) {
        return Some(Time::Zoned(at));
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(at) = NaiveDateTime::parse_from_str(&with_t, format) {
            return Some(Time::Naive(at));
        }
    }
    None
}

#[derive(Clone, Copy, Debug)]
enum Time {
    Naive(NaiveDateTime),
    Zoned(DateTime<chrono::FixedOffset>),
}

impl Time {
    /// As SQLAlchemy binds it: on SQLite the wall time as text, the offset dropped; on
    /// PostgreSQL a zoned time as itself, a naive one in the session's zone, which is UTC
    /// for MageML's database.
    fn bind(self, metadata: &Metadata) -> Raw {
        match (self, metadata) {
            (Time::Naive(at), Metadata::Sqlite(_)) => Raw::Text(sqlite_text(at)),
            (Time::Zoned(at), Metadata::Sqlite(_)) => Raw::Text(sqlite_text(at.naive_local())),
            (Time::Naive(at), Metadata::Postgres(_)) => Raw::Time(at.and_utc()),
            (Time::Zoned(at), Metadata::Postgres(_)) => Raw::Time(at.with_timezone(&Utc)),
        }
    }

    fn minus_days(self, days: i64) -> Time {
        match self {
            Time::Naive(at) => Time::Naive(at - chrono::Duration::days(days)),
            Time::Zoned(at) => Time::Zoned(at - chrono::Duration::days(days)),
        }
    }
}

fn sqlite_text(at: NaiveDateTime) -> String {
    at.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// Why the gateway leaves a monitor_stats query to the server; None when it answers it.
/// A stats type it does not know is found when it answers (the server gives no stats).
pub fn unsupported(query: &Query, repo_path: Option<&Path>) -> Option<&'static str> {
    if repo_path.is_none() {
        return Some("no repository path (or the project platform)");
    }
    for key in ["start_time", "end_time"] {
        if let Some(text) = query.get(key)
            && parse_time(text).is_none()
        {
            return Some("a time the gateway does not read");
        }
    }
    if let Some(id) = query.get("pipeline_schedule_id")
        && id.parse::<i64>().is_err()
    {
        return Some("a pipeline_schedule_id that is not a number");
    }
    None
}

/// The answer of GET /api/monitor_stats/<type>.
pub async fn monitor_stats(
    metadata: &Metadata,
    repo_path: &Path,
    path: &str,
    query: &Query,
) -> Result<Value, String> {
    let kind = stats_type(path).ok_or_else(|| format!("{path} names no stats type"))?;
    if !STATS_TYPES.contains(&kind) {
        return Err(format!(
            "{}the stats type {kind}",
            crate::gateway_schedules::UNSUPPORTED
        ));
    }
    let end = match query.get("end_time") {
        Some(text) => parse_time(text).ok_or_else(|| format!("end_time {text}"))?,
        None => Time::Zoned(Utc::now().fixed_offset()),
    };
    let start = match query.get("start_time") {
        Some(text) => parse_time(text).ok_or_else(|| format!("start_time {text}"))?,
        None => end.minus_days(30),
    };
    let filter = Filter {
        repo_path,
        pipeline: query.get("pipeline_uuid").map(str::to_string),
        schedule: query
            .get("pipeline_schedule_id")
            .map(|id| {
                id.parse::<i64>()
                    .map_err(|_| format!("pipeline_schedule_id {id}"))
            })
            .transpose()?,
        start,
        end,
    };
    let stats = match kind {
        "pipeline_run_count" => {
            let by_type = query.get("group_by_pipeline_type").is_some();
            pipeline_run_count(metadata, &filter, by_type).await?
        }
        "pipeline_run_time" => pipeline_run_time(metadata, &filter).await?,
        "block_run_count" => block_run_count(metadata, &filter).await?,
        "block_run_time" => block_run_time(metadata, &filter).await?,
        _ => return Err(format!("No monitor stats {kind}.")),
    };
    Ok(json!({"metadata": null, "monitor_stat": {"stats": stats, "stats_type": kind}}))
}

struct Filter<'a> {
    repo_path: &'a Path,
    pipeline: Option<String>,
    schedule: Option<i64>,
    start: Time,
    end: Time,
}

/// The WHERE clauses on the runs (`r`) of this project's triggers, and their parameters.
/// `times` is the column the range applies to: the run's or the block run's created_at.
fn clauses(metadata: &Metadata, filter: &Filter, times: &str) -> (String, Vec<Raw>) {
    let mut params = vec![Raw::Text(filter.repo_path.display().to_string())];
    let mut clauses = vec![
        "r.pipeline_schedule_id IN (SELECT id FROM pipeline_schedule \
         WHERE repo_path = {1} OR repo_path IS NULL)"
            .to_string(),
    ];
    let mut add = |clause: &str, raw: Raw| {
        params.push(raw);
        clauses.push(clause.replace("{}", &format!("{{{}}}", params.len())));
    };
    if let Some(pipeline) = &filter.pipeline {
        add("r.pipeline_uuid = {}", Raw::Text(pipeline.clone()));
    }
    add(&format!("{times} >= {{}}"), filter.start.bind(metadata));
    add(&format!("{times} <= {{}}"), filter.end.bind(metadata));
    if let Some(id) = filter.schedule {
        add("r.pipeline_schedule_id = {}", Raw::Int(id));
    }
    (clauses.join(" AND "), params)
}

/// The day of a created_at column, as the server's format_datetime gives it.
fn day(metadata: &Metadata, column: &str) -> String {
    match metadata {
        Metadata::Postgres(_) => format!("to_char({column}, 'YYYY-MM-DD')"),
        Metadata::Sqlite(_) => format!("strftime('%Y-%m-%d', {column})"),
    }
}

/// Statuses are stored by member name; the server writes their values.
fn status_value(raw: &Raw) -> String {
    match raw {
        Raw::Text(name) => name.to_lowercase(),
        _ => String::from("null"),
    }
}

fn text(raw: &Raw) -> Option<String> {
    match raw {
        Raw::Text(text) => Some(text.clone()),
        _ => None,
    }
}

async fn pipeline_run_count(
    metadata: &Metadata,
    filter: &Filter<'_>,
    by_type: bool,
) -> Result<Value, String> {
    let (clauses, params) = clauses(metadata, filter, "r.created_at");
    let status = status_column(metadata, "r.status");
    let sql = format!(
        "SELECT r.pipeline_schedule_id, r.pipeline_uuid, {status}, s.name, {} \
         FROM pipeline_run r JOIN pipeline_schedule s ON r.pipeline_schedule_id = s.id \
         WHERE {clauses}",
        day(metadata, "r.created_at"),
    );
    let rows = read_rows(metadata, &sql, &params).await?;
    // A pipeline that no longer exists keeps its runs; they count without a type.
    let mut types: BTreeMap<String, Option<Value>> = BTreeMap::new();
    let mut stats: Map<String, Value> = Map::new();
    for row in rows {
        let schedule = match &row[0] {
            Raw::Int(id) => id.to_string(),
            _ => "no_pipeline_schedule_id".to_string(),
        };
        let name = if matches!(row[0], Raw::Int(_)) {
            text(&row[3]).map(Value::from).unwrap_or(Value::Null)
        } else {
            Value::from("no_pipeline_schedule_name")
        };
        let pipeline = text(&row[1]).unwrap_or_default();
        let ds = text(&row[4]).map(Value::from).unwrap_or(Value::Null);
        let ds_key = ds.as_str().unwrap_or("null").to_string();
        let entry = stats
            .entry(schedule)
            .or_insert_with(|| json!({"data": {}, "name": name}));
        let day = entry["data"]
            .as_object_mut()
            .expect("data is an object")
            .entry(ds_key)
            .or_insert_with(|| json!({}));
        let counts = if by_type {
            let kind = types
                .entry(pipeline.clone())
                .or_insert_with(|| pipeline_type(filter.repo_path, &pipeline));
            let Some(Value::String(kind)) = kind.clone() else {
                continue;
            };
            day.as_object_mut()
                .expect("a day is an object")
                .entry(kind)
                .or_insert_with(|| json!({}))
        } else {
            day
        };
        let counts = counts.as_object_mut().expect("counts are an object");
        let key = status_value(&row[2]);
        let count = counts.get(&key).and_then(Value::as_i64).unwrap_or(0) + 1;
        counts.insert(key, Value::from(count));
    }
    Ok(Value::Object(stats))
}

/// The pipeline's type from its metadata.yaml (`python` when it names none); None when the
/// pipeline no longer exists.
fn pipeline_type(repo_path: &Path, uuid: &str) -> Option<Value> {
    let file = repo_path.join("pipelines").join(uuid).join("metadata.yaml");
    if !file.is_file() {
        return None;
    }
    crate::gateway_shadow::pipeline_config(repo_path, uuid).map(|config| config.kind)
}

fn status_column(metadata: &Metadata, column: &str) -> String {
    match metadata {
        Metadata::Postgres(_) => format!("CAST({column} AS TEXT)"),
        Metadata::Sqlite(_) => column.to_string(),
    }
}

/// Microseconds between two times read from the database.
fn micros_between(start: &Raw, end: &Raw) -> Option<i64> {
    let read = |raw: &Raw| -> Option<NaiveDateTime> {
        match raw {
            Raw::Time(at) => Some(at.naive_utc()),
            Raw::Text(text) => NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .or_else(|| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").ok()),
            _ => None,
        }
    };
    (read(end)? - read(start)?).num_microseconds()
}

/// The mean of the positive times, in seconds as timedelta.total_seconds gives them; 0
/// (an integer, as the server writes it) without any.
fn mean_seconds(times: &[i64]) -> Value {
    let positive: Vec<f64> = times
        .iter()
        .filter(|micros| **micros > 0)
        .map(|micros| *micros as f64 / 1e6)
        .collect();
    if positive.is_empty() {
        return Value::from(0);
    }
    Value::from(positive.iter().sum::<f64>() / positive.len() as f64)
}

async fn pipeline_run_time(metadata: &Metadata, filter: &Filter<'_>) -> Result<Value, String> {
    let (clauses, params) = clauses(metadata, filter, "r.created_at");
    let sql = format!(
        "SELECT r.created_at, r.completed_at, {} FROM pipeline_run r \
         WHERE {clauses} AND r.completed_at IS NOT NULL AND r.created_at IS NOT NULL",
        day(metadata, "r.created_at"),
    );
    let rows = read_rows(metadata, &sql, &params).await?;
    let mut by_day: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for row in &rows {
        let ds = text(&row[2]).unwrap_or_else(|| "null".into());
        let micros = micros_between(&row[0], &row[1]).unwrap_or(0);
        by_day.entry(ds).or_default().push(micros);
    }
    let data: Map<String, Value> = by_day
        .into_iter()
        .map(|(ds, times)| (ds, mean_seconds(&times)))
        .collect();
    let name = filter
        .pipeline
        .clone()
        .map(Value::from)
        .unwrap_or(Value::Null);
    let key = filter.pipeline.clone().unwrap_or_else(|| "null".into());
    Ok(json!({ key: {"data": data, "name": name} }))
}

async fn block_run_count(metadata: &Metadata, filter: &Filter<'_>) -> Result<Value, String> {
    let (clauses, params) = clauses(metadata, filter, "b.created_at");
    let status = status_column(metadata, "b.status");
    let ds = day(metadata, "b.created_at");
    let sql = format!(
        "SELECT b.block_uuid, {status}, COUNT(b.block_uuid), {ds} \
         FROM block_run b JOIN pipeline_run r ON r.id = b.pipeline_run_id \
         WHERE {clauses} GROUP BY b.block_uuid, b.status, {ds}"
    );
    let rows = read_rows(metadata, &sql, &params).await?;
    let mut stats: Map<String, Value> = Map::new();
    for row in rows {
        let name = text(&row[0]).unwrap_or_default();
        let ds = text(&row[3]).unwrap_or_else(|| "null".into());
        let count = match row[2] {
            Raw::Int(n) => n,
            _ => 0,
        };
        let entry = stats
            .entry(name.clone())
            .or_insert_with(|| json!({"data": {}, "name": name}));
        entry["data"]
            .as_object_mut()
            .expect("data is an object")
            .entry(ds)
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("a day is an object")
            .insert(status_value(&row[1]), Value::from(count));
    }
    Ok(Value::Object(stats))
}

async fn block_run_time(metadata: &Metadata, filter: &Filter<'_>) -> Result<Value, String> {
    let (clauses, params) = clauses(metadata, filter, "b.created_at");
    let sql = format!(
        "SELECT b.block_uuid, b.created_at, b.completed_at, {} \
         FROM block_run b JOIN pipeline_run r ON r.id = b.pipeline_run_id \
         WHERE {clauses} AND b.completed_at IS NOT NULL",
        day(metadata, "b.created_at"),
    );
    let rows = read_rows(metadata, &sql, &params).await?;
    let mut by_block: BTreeMap<String, BTreeMap<String, Vec<i64>>> = BTreeMap::new();
    for row in &rows {
        let block = text(&row[0]).unwrap_or_default();
        let ds = text(&row[3]).unwrap_or_else(|| "null".into());
        let micros = micros_between(&row[1], &row[2]).unwrap_or(0);
        by_block
            .entry(block)
            .or_default()
            .entry(ds)
            .or_default()
            .push(micros);
    }
    let stats: Map<String, Value> = by_block
        .into_iter()
        .map(|(block, days)| {
            let data: Map<String, Value> = days
                .into_iter()
                .map(|(ds, times)| (ds, mean_seconds(&times)))
                .collect();
            (block.clone(), json!({"data": data, "name": block}))
        })
        .collect();
    Ok(Value::Object(stats))
}

/// Whether two answers are equal, numbers within a relative 1e-9: the means add runs in
/// the database's order, which can change between the server's read and the gateway's.
pub fn close(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0),
            _ => x == y,
        },
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| close(v, w)))
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| close(v, w))
        }
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_as_the_pages_send_them() {
        assert!(matches!(
            parse_time("2026-10-22T00:00:00.000Z"),
            Some(Time::Zoned(_))
        ));
        assert!(matches!(
            parse_time("2026-10-22T08:00:00+02:00"),
            Some(Time::Zoned(_))
        ));
        assert!(matches!(parse_time("2026-10-22"), Some(Time::Naive(_))));
        assert!(matches!(
            parse_time("2026-10-22 08:30:00"),
            Some(Time::Naive(_))
        ));
        assert!(parse_time("yesterday").is_none());
        assert!(parse_time("1792627200").is_none());
    }

    #[test]
    fn sqlite_gets_the_wall_time_as_text() {
        let sqlite = Metadata::Sqlite("/tmp/x.db".into());
        let zoned = parse_time("2026-10-22T08:00:00+02:00").unwrap();
        assert!(matches!(zoned.bind(&sqlite),
            Raw::Text(text) if text == "2026-10-22 08:00:00.000000"));
    }

    #[test]
    fn means_follow_the_server() {
        assert_eq!(mean_seconds(&[]), json!(0));
        assert_eq!(mean_seconds(&[-5, 0]), json!(0));
        assert_eq!(mean_seconds(&[1_500_000, 2_500_000, -1]), json!(2.0));
        assert_eq!(mean_seconds(&[1]), json!(0.000001));
    }

    #[test]
    fn numbers_compare_within_a_relative_tolerance() {
        assert!(close(&json!({"a": 0.1 + 0.2}), &json!({"a": 0.3})));
        assert!(!close(&json!({"a": 0.3}), &json!({"a": 0.31})));
        assert!(!close(&json!({"a": 1}), &json!({"b": 1})));
        assert!(close(&json!(0), &json!(0.0)));
    }

    #[test]
    fn paths_and_requests_the_gateway_leaves_to_the_server() {
        let repo = Path::new("/repo");
        let q = |text: &str| Query::parse(text);
        assert_eq!(
            stats_type("/api/monitor_stats/block_run_count"),
            Some("block_run_count")
        );
        assert_eq!(stats_type("/api/monitor_stats/"), None);
        assert!(unsupported(&q("start_time=2026-10-01T00:00:00.000Z"), Some(repo)).is_none());
        assert!(unsupported(&q("start_time=soon"), Some(repo)).is_some());
        assert!(unsupported(&q("pipeline_schedule_id=x"), Some(repo)).is_some());
        assert!(unsupported(&q(""), None).is_some());
    }
}

#[cfg(test)]
mod postgres_tests {
    use super::*;

    const SCHEMA: &str = "
        CREATE TYPE blockrunstatus AS ENUM ('INITIAL', 'RUNNING', 'COMPLETED', 'FAILED');
        CREATE TYPE pipelinerunstatus AS ENUM ('INITIAL', 'RUNNING', 'COMPLETED', 'FAILED');
        CREATE TABLE pipeline_schedule (id serial PRIMARY KEY, name varchar(255),
            pipeline_uuid varchar(255), repo_path varchar(255));
        CREATE TABLE pipeline_run (id serial PRIMARY KEY, created_at timestamptz,
            pipeline_schedule_id integer, pipeline_uuid varchar(255),
            status pipelinerunstatus, completed_at timestamptz);
        CREATE TABLE block_run (id serial PRIMARY KEY, created_at timestamptz,
            pipeline_run_id integer, block_uuid varchar(255), status blockrunstatus,
            completed_at timestamptz);
        INSERT INTO pipeline_schedule (name, pipeline_uuid, repo_path) VALUES
            ('daily', 'etl', '/repo'), ('other project', 'etl', '/elsewhere');
        INSERT INTO pipeline_run (created_at, pipeline_schedule_id, pipeline_uuid, status,
            completed_at) VALUES
            ('2026-10-20 10:00:00+00', 1, 'etl', 'COMPLETED', '2026-10-20 10:00:01.5+00'),
            ('2026-10-20 11:00:00+00', 1, 'etl', 'FAILED', '2026-10-20 11:00:02.5+00'),
            ('2026-10-21 10:00:00+00', 1, 'etl', 'RUNNING', null),
            ('2026-10-21 10:00:00+00', 2, 'etl', 'COMPLETED', '2026-10-21 10:00:09+00');
        INSERT INTO block_run (created_at, pipeline_run_id, block_uuid, status, completed_at)
            VALUES
            ('2026-10-20 10:00:00+00', 1, 'load', 'COMPLETED', '2026-10-20 10:00:01+00'),
            ('2026-10-20 11:00:00+00', 2, 'load', 'FAILED', '2026-10-20 11:00:03+00'),
            ('2026-10-21 10:00:00+00', 4, 'load', 'COMPLETED', '2026-10-21 10:00:05+00');
    ";

    #[tokio::test]
    async fn the_four_stats_on_postgresql() {
        let Ok(url) = std::env::var("MAGE_TEST_POSTGRES_URL") else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        let schema = format!("monitor_{}", uuid::Uuid::new_v4().simple());
        let admin = mage_pg::connect_url(&url, "test").await.unwrap();
        admin
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; SET search_path TO {schema}; {SCHEMA}"
            ))
            .await
            .unwrap();
        let separator = if url.contains('?') { '&' } else { '?' };
        let metadata = Metadata::parse(&format!(
            "{url}{separator}options=-c%20search_path%3D{schema}%20-c%20TimeZone%3DUTC"
        ))
        .unwrap();
        let repo = Path::new("/repo");
        let window = "start_time=2026-10-01T00:00:00.000Z&end_time=2026-10-31";
        let ask = |kind: &'static str, extra: &'static str| {
            let metadata = &metadata;
            async move {
                let query = Query::parse(&format!("{window}&{extra}"));
                monitor_stats(
                    metadata,
                    repo,
                    &format!("/api/monitor_stats/{kind}"),
                    &query,
                )
                .await
                .unwrap()["monitor_stat"]["stats"]
                    .clone()
            }
        };
        let counts = ask("pipeline_run_count", "").await;
        let typed = ask("pipeline_run_count", "group_by_pipeline_type=1").await;
        let times = ask("pipeline_run_time", "pipeline_uuid=etl").await;
        let block_counts = ask("block_run_count", "pipeline_uuid=etl").await;
        let block_times = ask("block_run_time", "pipeline_uuid=etl").await;
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();

        // The other project's trigger and its run are left out.
        assert_eq!(
            counts,
            json!({"1": {"name": "daily", "data": {
                "2026-10-20": {"completed": 1, "failed": 1},
                "2026-10-21": {"running": 1},
            }}})
        );
        // No pipelines folder here: the runs count under no type, their days stay.
        assert_eq!(
            typed,
            json!({"1": {"name": "daily", "data": {"2026-10-20": {}, "2026-10-21": {}}}})
        );
        assert_eq!(
            times,
            json!({"etl": {"name": "etl", "data": {"2026-10-20": 2.0}}})
        );
        assert_eq!(
            block_counts,
            json!({"load": {"name": "load", "data": {
                "2026-10-20": {"completed": 1, "failed": 1},
            }}})
        );
        assert_eq!(
            block_times,
            json!({"load": {"name": "load", "data": {"2026-10-20": 2.0}}})
        );
    }
}
