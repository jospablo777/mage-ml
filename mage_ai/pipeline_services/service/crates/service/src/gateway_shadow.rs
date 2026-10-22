//! Shadow reads (ADR-0007): for each route given with --shadow, the gateway still serves
//! the server's answer, then computes its own from the metadata database and compares the
//! two. `/__gateway/shadow`, from loopback only, counts equal, different and failed
//! answers per route and keeps the last differences. A route moves to the gateway only
//! after its shadow reads stay equal.
//!
//! Routes: `block_runs` (GET /api/block_runs, with pipeline_run_id, pipeline_uuid,
//! order_by, _limit and _offset as BlockRunResource reads them) and `pipeline_run`
//! (GET /api/pipeline_runs/<id>, the run page's detail with its block runs).

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Mutex;

use serde_json::{Map, Value, json};

const RECENT_DIFFERENCES: usize = 20;
const ROUTES: &[&str] = &["block_runs", "pipeline_run"];
/// DatabaseResource.DEFAULT_LIMIT.
const DEFAULT_LIMIT: i64 = 40;

/// MageML's metadata database: the SQLite file or PostgreSQL URL of the server's
/// db_connection_url.
#[derive(Debug, Clone)]
pub enum Metadata {
    Sqlite(PathBuf),
    Postgres(String),
}

impl Metadata {
    pub fn parse(url: &str) -> Result<Metadata, String> {
        // SQLAlchemy's form: sqlite:////absolute/path, or sqlite:///relative/path.
        if let Some(path) = url.strip_prefix("sqlite:///") {
            return Ok(Metadata::Sqlite(PathBuf::from(path)));
        }
        if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            return Ok(Metadata::Postgres(url.to_string()));
        }
        Err(format!(
            "The metadata database {} is neither sqlite:/// nor postgresql://.",
            redact_url(url)
        ))
    }
}

fn redact_url(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(scheme), Some(at)) if at > scheme => {
            format!("{}://...@{}", &url[..scheme], &url[at + 1..])
        }
        _ => url.to_string(),
    }
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
struct Counts {
    equal: u64,
    /// Equal once read again: the rows changed between the server's read and the gateway's.
    equal_after_reread: u64,
    different: u64,
    failed: u64,
}

/// The server's answer read again, for the same request; None when it did not answer 200.
pub type Reread = Box<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Vec<u8>>> + Send>>
        + Send
        + Sync,
>;
const REREADS: u32 = 2;
const REREAD_AFTER: std::time::Duration = std::time::Duration::from_millis(100);

pub struct Shadow {
    metadata: Metadata,
    routes: Vec<String>,
    counts: Mutex<BTreeMap<String, Counts>>,
    differences: Mutex<VecDeque<Value>>,
}

impl Shadow {
    pub fn new(metadata: Metadata, routes: Vec<String>) -> Result<Shadow, String> {
        for route in &routes {
            if !ROUTES.contains(&route.as_str()) {
                return Err(format!(
                    "No shadow read for {route}; there are {}.",
                    ROUTES.join(", ")
                ));
            }
        }
        Ok(Shadow {
            metadata,
            routes,
            counts: Mutex::new(BTreeMap::new()),
            differences: Mutex::new(VecDeque::new()),
        })
    }

    /// The shadowed route a request is, when it is one.
    pub fn route_for(&self, method: &hyper::Method, path: &str) -> Option<&'static str> {
        if method != hyper::Method::GET {
            return None;
        }
        let enabled = |route: &str| self.routes.iter().any(|r| r == route);
        let path = path.trim_end_matches('/');
        if path == "/api/block_runs" && enabled("block_runs") {
            return Some("block_runs");
        }
        let run = path.strip_prefix("/api/pipeline_runs/")?;
        (enabled("pipeline_run") && !run.is_empty() && run.bytes().all(|b| b.is_ascii_digit()))
            .then_some("pipeline_run")
    }

    /// Compares the server's answer with the gateway's own and counts the outcome.
    /// Compares the server's answer with the gateway's own and counts the outcome. Rows
    /// change between the two reads while runs run, so a difference is read again from both,
    /// up to REREADS times, before it counts.
    pub async fn compare(
        &self,
        route: &'static str,
        path: &str,
        query: &str,
        served: &[u8],
        reread: Reread,
    ) {
        let mut served = served.to_vec();
        let mut attempt = 0;
        let outcome = loop {
            let pair = match serde_json::from_slice::<Value>(&served) {
                Ok(served) => match self.answer(route, path, query).await {
                    Ok(own) => Ok((served, own)),
                    Err(error) => Err(error),
                },
                Err(error) => Err(format!("The server's answer is not JSON: {error}")),
            };
            match pair {
                Ok((served, own)) if served == own => break Ok(attempt),
                Ok(pair) if attempt == REREADS => break Err(Some(pair)),
                Ok(_) => {}
                Err(error) => {
                    self.note(
                        route,
                        json!({"route": route, "query": query, "failed": error}),
                    );
                    break Err(None);
                }
            }
            attempt += 1;
            tokio::time::sleep(REREAD_AFTER).await;
            match reread().await {
                Some(again) => served = again,
                None => break Err(None),
            }
        };
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let entry = counts.entry(route.to_string()).or_default();
        match outcome {
            Ok(0) => entry.equal += 1,
            Ok(_) => entry.equal_after_reread += 1,
            Err(None) => entry.failed += 1,
            Err(Some((served, own))) => {
                entry.different += 1;
                drop(counts);
                self.note(route, json!({"route": route, "query": query, "difference": first_difference(&served, &own)}));
            }
        }
    }

    fn note(&self, route: &str, difference: Value) {
        crate::log::warn(&format!(
            "Gateway shadow read of {route} differs: {difference}"
        ));
        let mut recent = self.differences.lock().unwrap_or_else(|p| p.into_inner());
        if recent.len() == RECENT_DIFFERENCES {
            recent.pop_front();
        }
        recent.push_back(difference);
    }

    pub fn report(&self) -> Value {
        let counts = self
            .counts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let recent: Vec<Value> = self
            .differences
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .cloned()
            .collect();
        json!({"routes": counts, "recent_differences": recent})
    }

    async fn answer(&self, route: &str, path: &str, query: &str) -> Result<Value, String> {
        match route {
            "block_runs" => block_runs(&self.metadata, &Query::parse(query)).await,
            "pipeline_run" => {
                let id = path
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .and_then(|id| id.parse::<i64>().ok())
                    .ok_or_else(|| format!("{path} names no run"))?;
                pipeline_run(&self.metadata, id).await
            }
            _ => Err(format!("No shadow read for {route}.")),
        }
    }
}

/// Where two answers first differ, as a path and the two values.
fn first_difference(served: &Value, own: &Value) -> Value {
    fn walk(path: String, a: &Value, b: &Value) -> Option<Value> {
        match (a, b) {
            (Value::Object(x), Value::Object(y)) => {
                let keys: std::collections::BTreeSet<&String> = x.keys().chain(y.keys()).collect();
                keys.into_iter().find_map(|k| {
                    walk(
                        format!("{path}.{k}"),
                        x.get(k).unwrap_or(&Value::Null),
                        y.get(k).unwrap_or(&Value::Null),
                    )
                    .or_else(|| (x.contains_key(k) != y.contains_key(k)).then(|| json!({"path": format!("{path}.{k}"), "server": x.contains_key(k), "gateway": y.contains_key(k)})))
                })
            }
            (Value::Array(x), Value::Array(y)) if x.len() == y.len() => x
                .iter()
                .zip(y)
                .enumerate()
                .find_map(|(i, (a, b))| walk(format!("{path}[{i}]"), a, b)),
            _ if a == b => None,
            _ => Some(json!({"path": path, "server": a, "gateway": b})),
        }
    }
    walk(String::from("$"), served, own).unwrap_or(Value::Null)
}

/// The query string as Tornado reads it: each name's first value.
struct Query(BTreeMap<String, String>);

impl Query {
    fn parse(query: &str) -> Query {
        let mut values = BTreeMap::new();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            values
                .entry(percent_decode(name))
                .or_insert_with(|| percent_decode(value));
        }
        Query(values)
    }

    /// A value Mage reads with `query.get(name, [None])[0]` and then tests for truth.
    fn get(&self, name: &str) -> Option<&str> {
        self.0
            .get(name)
            .map(String::as_str)
            .filter(|v| !v.is_empty())
    }
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                match u8::from_str_radix(
                    std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                    16,
                ) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            byte => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The BlockRun columns `order_by` may name (BlockRunResource sorts by any attribute).
const BLOCK_RUN_COLUMNS: &[&str] = &[
    "attempt",
    "block_uuid",
    "completed_at",
    "created_at",
    "id",
    "metrics",
    "pipeline_run_id",
    "started_at",
    "status",
    "updated_at",
];

/// GET /api/block_runs as BlockRunResource.collection and DatabaseResource answer it.
async fn block_runs(metadata: &Metadata, query: &Query) -> Result<Value, String> {
    let limit = match query.get("_limit") {
        Some(text) => text
            .parse::<i64>()
            .map_err(|_| format!("_limit {text} is not a number"))?,
        None => DEFAULT_LIMIT,
    };
    let offset = match query.get("_offset") {
        Some(text) => text
            .parse::<i64>()
            .map_err(|_| format!("_offset {text} is not a number"))?,
        None => 0,
    };
    let mut filters = Vec::new();
    let mut params: Vec<Param> = Vec::new();
    if let Some(run) = query.get("pipeline_run_id")
        && let Ok(id) = run.parse::<i64>()
    {
        params.push(Param::Int(id));
        filters.push(format!(
            "a.pipeline_run_id = {}",
            placeholder(metadata, params.len())
        ));
    }
    if let Some(pipeline) = query.get("pipeline_uuid") {
        params.push(Param::Text(pipeline.to_string()));
        filters.push(format!(
            "c.pipeline_uuid = {}",
            placeholder(metadata, params.len())
        ));
    }
    let order = match query.get("order_by") {
        Some(order_by) => {
            let mut parts = order_by.split(' ');
            let column = parts.next().unwrap_or_default();
            let direction = parts.next().unwrap_or("asc").to_lowercase();
            if !BLOCK_RUN_COLUMNS.contains(&column) || !matches!(direction.as_str(), "asc" | "desc")
            {
                return Err(format!(
                    "order_by {order_by} is not a block run column and direction"
                ));
            }
            let direction = direction.to_uppercase();
            format!("a.{column} {direction}, a.id {direction}")
        }
        // The id last breaks ties, as BlockRunResource orders them.
        None => "a.started_at DESC, a.created_at DESC, a.completed_at DESC, a.id DESC".to_string(),
    };
    let from = format!(
        "FROM block_run AS a JOIN pipeline_run AS b ON a.pipeline_run_id = b.id \
         JOIN pipeline_schedule AS c ON b.pipeline_schedule_id = c.id{}",
        if filters.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", filters.join(" AND "))
        }
    );
    let rows_sql = format!(
        "SELECT a.block_uuid, a.completed_at, a.created_at, a.id, a.pipeline_run_id, \
         a.started_at, a.status, a.updated_at, c.id, c.name {from} ORDER BY {order} LIMIT {} OFFSET {}",
        limit + 1,
        offset
    );
    let count_sql = format!("SELECT count(*) {from}");
    let (count, rows) = match metadata {
        Metadata::Sqlite(path) => {
            let path = path.clone();
            tokio::task::spawn_blocking(move || {
                sqlite_block_runs(&path, &count_sql, &rows_sql, &params)
            })
            .await
            .map_err(|e| e.to_string())??
        }
        Metadata::Postgres(url) => postgres_block_runs(url, &count_sql, &rows_sql, &params).await?,
    };
    let has_next = rows.len() as i64 > limit;
    let shown: Vec<Value> = rows.into_iter().take(limit.max(0) as usize).collect();
    Ok(json!({"block_runs": shown, "metadata": {"count": count, "next": has_next}}))
}

#[derive(Debug, Clone)]
enum Param {
    Int(i64),
    Text(String),
}

fn placeholder(metadata: &Metadata, n: usize) -> String {
    match metadata {
        Metadata::Sqlite(_) => format!("?{n}"),
        Metadata::Postgres(_) => format!("${n}"),
    }
}

fn block_run(values: [Value; 10]) -> Value {
    let [
        block_uuid,
        completed_at,
        created_at,
        id,
        pipeline_run_id,
        started_at,
        status,
        updated_at,
        schedule_id,
        schedule_name,
    ] = values;
    let mut row = Map::new();
    row.insert("block_uuid".into(), block_uuid);
    row.insert("completed_at".into(), completed_at);
    row.insert("created_at".into(), created_at);
    row.insert("id".into(), id);
    row.insert("pipeline_run_id".into(), pipeline_run_id);
    row.insert("started_at".into(), started_at);
    // SQLAlchemy stores an Enum's member name; the API shows its value, the name in lower case.
    row.insert(
        "status".into(),
        status
            .as_str()
            .map(|s| Value::String(s.to_lowercase()))
            .unwrap_or(Value::Null),
    );
    row.insert("updated_at".into(), updated_at);
    row.insert("pipeline_schedule_id".into(), schedule_id);
    row.insert("pipeline_schedule_name".into(), schedule_name);
    Value::Object(row)
}

/// A SQLite DATETIME as Python's isoformat of the datetime SQLAlchemy reads from it:
/// `2026-10-22 02:15:09.069058` is `2026-10-22T02:15:09.069058`, and a zero fraction
/// disappears.
fn sqlite_time(text: Option<String>) -> Value {
    let Some(text) = text else {
        return Value::Null;
    };
    let (seconds, fraction) = match text.split_once('.') {
        Some((seconds, fraction)) => (seconds, fraction),
        None => (text.as_str(), ""),
    };
    let iso = seconds.replacen(' ', "T", 1);
    let digits: String = fraction
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.is_empty() || digits.chars().all(|c| c == '0') {
        Value::String(iso)
    } else {
        Value::String(format!("{iso}.{digits:0<6}"))
    }
}

fn sqlite_block_runs(
    path: &std::path::Path,
    count_sql: &str,
    rows_sql: &str,
    params: &[Param],
) -> Result<(i64, Vec<Value>), String> {
    use rusqlite::{OpenFlags, ToSql, types::ToSqlOutput};
    let connection = rusqlite::Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("{}: {e}", path.display()))?;
    let values: Vec<ToSqlOutput> = params
        .iter()
        .map(|p| match p {
            Param::Int(v) => v.to_sql().unwrap(),
            Param::Text(v) => ToSqlOutput::from(v.clone()),
        })
        .collect();
    let refs: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
    let count: i64 = connection
        .query_row(count_sql, refs.as_slice(), |row| row.get(0))
        .map_err(|e| e.to_string())?;
    let mut statement = connection.prepare(rows_sql).map_err(|e| e.to_string())?;
    let rows = statement
        .query_map(refs.as_slice(), |row| {
            let text = |i| row.get::<_, Option<String>>(i);
            let int = |i| row.get::<_, Option<i64>>(i);
            Ok(block_run([
                text(0)?.map(Value::String).unwrap_or(Value::Null),
                sqlite_time(text(1)?),
                sqlite_time(text(2)?),
                int(3)?.map(Value::from).unwrap_or(Value::Null),
                int(4)?.map(Value::from).unwrap_or(Value::Null),
                sqlite_time(text(5)?),
                text(6)?.map(Value::String).unwrap_or(Value::Null),
                sqlite_time(text(7)?),
                int(8)?.map(Value::from).unwrap_or(Value::Null),
                text(9)?.map(Value::String).unwrap_or(Value::Null),
            ]))
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok((count, rows))
}

/// A PostgreSQL timestamptz as Python's isoformat in the session's UTC: a `+00:00`
/// offset, and no fraction when it is zero.
fn postgres_time(at: Option<chrono::DateTime<chrono::Utc>>) -> Value {
    match at {
        None => Value::Null,
        Some(at) => {
            let micros = at.timestamp_subsec_micros();
            let base = at.format("%Y-%m-%dT%H:%M:%S");
            Value::String(if micros == 0 {
                format!("{base}+00:00")
            } else {
                format!("{base}.{micros:06}+00:00")
            })
        }
    }
}

async fn postgres_block_runs(
    url: &str,
    count_sql: &str,
    rows_sql: &str,
    params: &[Param],
) -> Result<(i64, Vec<Value>), String> {
    use tokio_postgres::types::ToSql;
    let client = mage_pg::connect_url(url, "mageml-gateway").await?;
    let owned: Vec<Box<dyn ToSql + Sync + Send>> = params
        .iter()
        .map(|p| -> Box<dyn ToSql + Sync + Send> {
            match p {
                Param::Int(v) => Box::new(*v as i32),
                Param::Text(v) => Box::new(v.clone()),
            }
        })
        .collect();
    let refs: Vec<&(dyn ToSql + Sync)> = owned
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let count: i64 = client
        .query_one(count_sql, &refs)
        .await
        .map_err(|e| e.to_string())?
        .get(0);
    let rows = client
        .query(rows_sql, &refs)
        .await
        .map_err(|e| e.to_string())?
        .iter()
        .map(|row| {
            let time = |i| postgres_time(row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(i));
            let text = |i| {
                row.get::<_, Option<String>>(i)
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            };
            let int = |i| {
                row.get::<_, Option<i32>>(i)
                    .map(Value::from)
                    .unwrap_or(Value::Null)
            };
            block_run([
                text(0),
                time(1),
                time(2),
                int(3),
                int(4),
                time(5),
                text(6),
                time(7),
                int(8),
                text(9),
            ])
        })
        .collect();
    Ok((count, rows))
}

/// How a column's value reads in an answer.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Int,
    Text,
    /// A datetime as `str(datetime)` writes it (BaseModel.to_dict).
    Time,
    Json,
    /// An Enum stored by member name and shown by value, the name in lower case.
    Enum,
    Bool,
}

/// PipelineRunPresenter's default attributes, the detail's fields besides block_runs.
const RUN_FIELDS: &[(&str, Kind)] = &[
    ("backfill_id", Kind::Int),
    ("completed_at", Kind::Time),
    ("created_at", Kind::Time),
    ("event_variables", Kind::Json),
    ("execution_date", Kind::Time),
    ("executor_type", Kind::Enum),
    ("id", Kind::Int),
    ("metrics", Kind::Json),
    ("passed_sla", Kind::Bool),
    ("pipeline_schedule_id", Kind::Int),
    ("pipeline_uuid", Kind::Text),
    ("started_at", Kind::Time),
    ("status", Kind::Enum),
    ("updated_at", Kind::Time),
    ("variables", Kind::Json),
];
/// Where pipeline_schedule_id is in RUN_FIELDS.
const RUN_SCHEDULE: usize = 9;

/// Every column of block_run, as BaseModel.to_dict gives them.
const BLOCK_RUN_FIELDS: &[(&str, Kind)] = &[
    ("id", Kind::Int),
    ("created_at", Kind::Time),
    ("updated_at", Kind::Time),
    ("pipeline_run_id", Kind::Int),
    ("block_uuid", Kind::Text),
    ("status", Kind::Enum),
    ("completed_at", Kind::Time),
    ("started_at", Kind::Time),
    ("metrics", Kind::Json),
    ("attempt", Kind::Int),
];

fn select_list(metadata: &Metadata, fields: &[(&str, Kind)]) -> String {
    fields
        .iter()
        .map(|(name, kind)| match (metadata, kind) {
            // PostgreSQL keeps Enums as types of their own and JSON as json; both read as text.
            (Metadata::Postgres(_), Kind::Enum | Kind::Json) => format!("{name}::text"),
            _ => name.to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A value read from either database, before it takes its field's kind.
enum Raw {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Time(chrono::DateTime<chrono::Utc>),
}

fn field_value(kind: Kind, raw: Raw) -> Result<Value, String> {
    Ok(match (kind, raw) {
        (_, Raw::Null) => Value::Null,
        (Kind::Int, Raw::Int(v)) => Value::from(v),
        (Kind::Bool, Raw::Bool(v)) => Value::Bool(v),
        (Kind::Bool, Raw::Int(v)) => Value::Bool(v != 0),
        (Kind::Text, Raw::Text(v)) => Value::String(v),
        (Kind::Enum, Raw::Text(v)) => Value::String(v.to_lowercase()),
        (Kind::Json, Raw::Text(v)) => {
            serde_json::from_str(&v).map_err(|e| format!("JSON {v}: {e}"))?
        }
        (Kind::Time, Raw::Text(v)) => str_time_sqlite(&v),
        (Kind::Time, Raw::Time(at)) => str_time_utc(at),
        (kind, _) => return Err(format!("A {kind:?} column held another type")),
    })
}

/// `str(datetime)` of the naive datetime SQLAlchemy reads from SQLite text: the date, a
/// space, the time, and the microseconds only when they are not zero.
fn str_time_sqlite(text: &str) -> Value {
    match sqlite_time(Some(text.to_string())) {
        Value::String(iso) => Value::String(iso.replacen('T', " ", 1)),
        other => other,
    }
}

/// `str(datetime)` of an aware datetime in UTC, as PostgreSQL gives it to SQLAlchemy.
fn str_time_utc(at: chrono::DateTime<chrono::Utc>) -> Value {
    match postgres_time(Some(at)) {
        Value::String(iso) => Value::String(iso.replacen('T', " ", 1)),
        other => other,
    }
}

fn object(fields: &[(&str, Kind)], raws: Vec<Raw>) -> Result<Map<String, Value>, String> {
    let mut row = Map::new();
    for ((name, kind), raw) in fields.iter().zip(raws) {
        row.insert(name.to_string(), field_value(*kind, raw)?);
    }
    Ok(row)
}

/// GET /api/pipeline_runs/<id>: the run's presenter attributes, and its block runs with
/// the trigger's id and name, ordered by created_at as the presenter sorts them.
async fn pipeline_run(metadata: &Metadata, id: i64) -> Result<Value, String> {
    let run_sql = format!(
        "SELECT {} FROM pipeline_run WHERE id = {}",
        select_list(metadata, RUN_FIELDS),
        placeholder(metadata, 1)
    );
    let blocks_sql = format!(
        "SELECT {} FROM block_run WHERE pipeline_run_id = {} ORDER BY id",
        select_list(metadata, BLOCK_RUN_FIELDS),
        placeholder(metadata, 1)
    );
    let schedule_sql = format!(
        "SELECT id, name FROM pipeline_schedule WHERE id = {}",
        placeholder(metadata, 1)
    );
    let (run, blocks, schedule) = match metadata {
        Metadata::Sqlite(path) => {
            let path = path.clone();
            tokio::task::spawn_blocking(move || {
                sqlite_run(&path, id, &run_sql, &blocks_sql, &schedule_sql)
            })
            .await
            .map_err(|e| e.to_string())??
        }
        Metadata::Postgres(url) => {
            postgres_run(url, id, &run_sql, &blocks_sql, &schedule_sql).await?
        }
    };
    let mut run = object(RUN_FIELDS, run)?;
    let (schedule_id, schedule_name) = schedule.ok_or("The run's trigger is gone")?;
    let schedule_name = schedule_name.map(Value::String).unwrap_or(Value::Null);
    let mut block_runs = blocks
        .into_iter()
        .map(|raws| {
            let mut block = object(BLOCK_RUN_FIELDS, raws)?;
            block.insert("pipeline_schedule_id".into(), Value::from(schedule_id));
            block.insert("pipeline_schedule_name".into(), schedule_name.clone());
            Ok(Value::Object(block))
        })
        .collect::<Result<Vec<_>, String>>()?;
    // Python's sorted() by created_at, stable on equal times.
    block_runs.sort_by(|a, b| {
        let at = |v: &Value| v["created_at"].as_str().unwrap_or_default().to_string();
        at(a).cmp(&at(b))
    });
    run.insert("block_runs".into(), Value::Array(block_runs));
    // The detail's envelope carries metadata, null.
    Ok(json!({ "pipeline_run": run, "metadata": null }))
}

type RunRows = (Vec<Raw>, Vec<Vec<Raw>>, Option<(i64, Option<String>)>);

fn sqlite_raw(value: rusqlite::types::ValueRef<'_>) -> Raw {
    use rusqlite::types::ValueRef;
    match value {
        ValueRef::Null => Raw::Null,
        ValueRef::Integer(v) => Raw::Int(v),
        ValueRef::Real(v) => Raw::Text(v.to_string()),
        ValueRef::Text(v) | ValueRef::Blob(v) => Raw::Text(String::from_utf8_lossy(v).into_owned()),
    }
}

fn sqlite_run(
    path: &std::path::Path,
    id: i64,
    run_sql: &str,
    blocks_sql: &str,
    schedule_sql: &str,
) -> Result<RunRows, String> {
    use rusqlite::OpenFlags;
    let connection = rusqlite::Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("{}: {e}", path.display()))?;
    let read = |row: &rusqlite::Row<'_>, n: usize| -> rusqlite::Result<Vec<Raw>> {
        (0..n).map(|i| row.get_ref(i).map(sqlite_raw)).collect()
    };
    let run = connection
        .query_row(run_sql, [id], |row| read(row, RUN_FIELDS.len()))
        .map_err(|e| e.to_string())?;
    let schedule_id = match &run[RUN_SCHEDULE] {
        Raw::Int(v) => Some(*v),
        _ => None,
    };
    let mut statement = connection.prepare(blocks_sql).map_err(|e| e.to_string())?;
    let blocks = statement
        .query_map([id], |row| read(row, BLOCK_RUN_FIELDS.len()))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let schedule = match schedule_id {
        Some(schedule_id) => connection
            .query_row(schedule_sql, [schedule_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .ok(),
        None => None,
    };
    Ok((run, blocks, schedule))
}

fn postgres_raw(row: &tokio_postgres::Row, i: usize, kind: Kind) -> Raw {
    let raw = match kind {
        Kind::Int => row.get::<_, Option<i32>>(i).map(|v| Raw::Int(v.into())),
        Kind::Bool => row.get::<_, Option<bool>>(i).map(Raw::Bool),
        Kind::Time => row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
            .map(Raw::Time),
        Kind::Text | Kind::Json | Kind::Enum => row.get::<_, Option<String>>(i).map(Raw::Text),
    };
    raw.unwrap_or(Raw::Null)
}

async fn postgres_run(
    url: &str,
    id: i64,
    run_sql: &str,
    blocks_sql: &str,
    schedule_sql: &str,
) -> Result<RunRows, String> {
    let client = mage_pg::connect_url(url, "mageml-gateway").await?;
    let id = i32::try_from(id).map_err(|_| format!("run {id} is out of range"))?;
    let row = client
        .query_one(run_sql, &[&id])
        .await
        .map_err(|e| e.to_string())?;
    let run: Vec<Raw> = RUN_FIELDS
        .iter()
        .enumerate()
        .map(|(i, (_, kind))| postgres_raw(&row, i, *kind))
        .collect();
    let schedule_id: Option<i32> = row.get(RUN_SCHEDULE);
    let blocks = client
        .query(blocks_sql, &[&id])
        .await
        .map_err(|e| e.to_string())?
        .iter()
        .map(|row| {
            BLOCK_RUN_FIELDS
                .iter()
                .enumerate()
                .map(|(i, (_, kind))| postgres_raw(row, i, *kind))
                .collect()
        })
        .collect();
    let schedule = match schedule_id {
        Some(schedule_id) => client
            .query_opt(schedule_sql, &[&schedule_id])
            .await
            .map_err(|e| e.to_string())?
            .map(|row| {
                (
                    i64::from(row.get::<_, i32>(0)),
                    row.get::<_, Option<String>>(1),
                )
            }),
        None => None,
    };
    Ok((run, blocks, schedule))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_times_read_as_python_writes_them() {
        assert_eq!(
            sqlite_time(Some("2026-10-22 02:15:09.069058".into())),
            "2026-10-22T02:15:09.069058"
        );
        assert_eq!(
            sqlite_time(Some("2026-10-22 02:15:08".into())),
            "2026-10-22T02:15:08"
        );
        assert_eq!(
            sqlite_time(Some("2026-10-22 02:15:08.000000".into())),
            "2026-10-22T02:15:08"
        );
        assert_eq!(sqlite_time(None), Value::Null);
        let at = chrono::DateTime::parse_from_rfc3339("2026-10-22T02:15:09.069058Z")
            .unwrap()
            .to_utc();
        assert_eq!(postgres_time(Some(at)), "2026-10-22T02:15:09.069058+00:00");
    }

    #[test]
    fn to_dict_times_have_a_space() {
        assert_eq!(
            str_time_sqlite("2026-10-22 02:15:09.069058"),
            "2026-10-22 02:15:09.069058"
        );
        assert_eq!(
            str_time_sqlite("2026-10-22 02:15:09.000000"),
            "2026-10-22 02:15:09"
        );
        let at = chrono::DateTime::parse_from_rfc3339("2026-10-22T02:15:09Z")
            .unwrap()
            .to_utc();
        assert_eq!(str_time_utc(at), "2026-10-22 02:15:09+00:00");
        assert_eq!(
            field_value(Kind::Enum, Raw::Text("LOCAL_PYTHON".into())).unwrap(),
            "local_python"
        );
        assert_eq!(field_value(Kind::Bool, Raw::Int(0)).unwrap(), false);
        assert_eq!(
            field_value(Kind::Json, Raw::Text("null".into())).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn run_paths_are_digits() {
        let shadow =
            Shadow::new(Metadata::Sqlite("/x".into()), vec!["pipeline_run".into()]).unwrap();
        let get = hyper::Method::GET;
        assert_eq!(
            shadow.route_for(&get, "/api/pipeline_runs/12"),
            Some("pipeline_run")
        );
        assert_eq!(
            shadow.route_for(&get, "/api/pipeline_runs/12/block_runs"),
            None
        );
        assert_eq!(shadow.route_for(&get, "/api/pipeline_runs/"), None);
        assert_eq!(shadow.route_for(&get, "/api/block_runs"), None);
        assert_eq!(
            shadow.route_for(&hyper::Method::PUT, "/api/pipeline_runs/12"),
            None
        );
    }

    #[test]
    fn queries_read_each_names_first_value() {
        let query = Query::parse("pipeline_uuid=a%20b&order_by=id+desc&pipeline_uuid=c&_limit=");
        assert_eq!(query.get("pipeline_uuid"), Some("a b"));
        assert_eq!(query.get("order_by"), Some("id desc"));
        assert_eq!(query.get("_limit"), None);
    }

    #[test]
    fn a_difference_names_its_path() {
        let served =
            json!({"block_runs": [{"id": 1, "status": "completed"}], "metadata": {"count": 1}});
        let own = json!({"block_runs": [{"id": 1, "status": "failed"}], "metadata": {"count": 1}});
        assert_eq!(
            first_difference(&served, &own),
            json!({"path": "$.block_runs[0].status", "server": "completed", "gateway": "failed"})
        );
    }

    #[test]
    fn database_urls() {
        assert!(
            matches!(Metadata::parse("sqlite:////tmp/mage-ai.db"), Ok(Metadata::Sqlite(p)) if p == std::path::Path::new("/tmp/mage-ai.db"))
        );
        assert!(
            Metadata::parse("mysql://x")
                .unwrap_err()
                .contains("neither")
        );
        assert_eq!(
            redact_url("postgresql://u:secret@h:5432/db"),
            "postgresql://...@h:5432/db"
        );
    }
}
