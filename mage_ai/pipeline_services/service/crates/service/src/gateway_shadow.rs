//! Shadow reads (ADR-0007): for each route given with --shadow, the gateway still serves
//! the server's answer, then computes its own from the metadata database and compares the
//! two. `/__gateway/shadow`, from loopback only, counts equal, different and failed
//! answers per route and keeps the last differences. A route moves to the gateway only
//! after its shadow reads stay equal.
//!
//! Routes: `block_runs` (GET /api/block_runs, with pipeline_run_id, pipeline_uuid,
//! order_by, _limit and _offset as BlockRunResource reads them).

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Mutex;

use serde_json::{Map, Value, json};

const RECENT_DIFFERENCES: usize = 20;
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
    different: u64,
    failed: u64,
}

pub struct Shadow {
    metadata: Metadata,
    routes: Vec<String>,
    counts: Mutex<BTreeMap<String, Counts>>,
    differences: Mutex<VecDeque<Value>>,
}

impl Shadow {
    pub fn new(metadata: Metadata, routes: Vec<String>) -> Result<Shadow, String> {
        for route in &routes {
            if route != "block_runs" {
                return Err(format!("No shadow read for {route}; there is block_runs."));
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
        match path.trim_end_matches('/') {
            "/api/block_runs" if self.routes.iter().any(|r| r == "block_runs") => {
                Some("block_runs")
            }
            _ => None,
        }
    }

    /// Compares the server's answer with the gateway's own and counts the outcome.
    pub async fn compare(&self, route: &'static str, query: &str, served: &[u8]) {
        let outcome = match serde_json::from_slice::<Value>(served) {
            Ok(served) => match self.answer(route, query).await {
                Ok(own) => Some((served, own)),
                Err(error) => {
                    self.note(
                        route,
                        json!({"route": route, "query": query, "failed": error}),
                    );
                    None
                }
            },
            Err(error) => {
                self.note(route, json!({"route": route, "query": query, "failed": format!("The server's answer is not JSON: {error}")}));
                None
            }
        };
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let entry = counts.entry(route.to_string()).or_default();
        match outcome {
            None => entry.failed += 1,
            Some((served, own)) if served == own => entry.equal += 1,
            Some((served, own)) => {
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

    async fn answer(&self, route: &str, query: &str) -> Result<Value, String> {
        match route {
            "block_runs" => block_runs(&self.metadata, &Query::parse(query)).await,
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
            format!("a.{column} {}", direction.to_uppercase())
        }
        None => "a.started_at DESC, a.created_at DESC, a.completed_at DESC".to_string(),
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
