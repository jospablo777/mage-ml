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
const ROUTES: &[&str] = &["block_runs", "pipeline_run", "pipeline_runs"];
/// DatabaseResource.DEFAULT_LIMIT.
const DEFAULT_LIMIT: i64 = 40;

/// MageML's metadata database: the SQLite file or PostgreSQL URL of the server's
/// db_connection_url.
#[derive(Clone)]
pub enum Metadata {
    Sqlite(PathBuf),
    Postgres(std::sync::Arc<Pg>),
}

impl std::fmt::Debug for Metadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Metadata::Sqlite(path) => write!(f, "Sqlite({})", path.display()),
            // Never the password.
            Metadata::Postgres(pg) => write!(f, "Postgres({})", redact_url(&pg.url)),
        }
    }
}

/// One PostgreSQL connection for the shadow reads, opened on the first and opened again
/// after an error: the run page asks every few seconds, and each new connection is a
/// server process.
pub struct Pg {
    url: String,
    client: tokio::sync::Mutex<Option<std::sync::Arc<tokio_postgres::Client>>>,
}

impl Pg {
    async fn client(&self) -> Result<std::sync::Arc<tokio_postgres::Client>, String> {
        let mut client = self.client.lock().await;
        if let Some(open) = client.as_ref().filter(|c| !c.is_closed()) {
            return Ok(open.clone());
        }
        let open = std::sync::Arc::new(mage_pg::connect_url(&self.url, "mageml-gateway").await?);
        *client = Some(open.clone());
        Ok(open)
    }

    async fn forget(&self) {
        *self.client.lock().await = None;
    }
}

impl Metadata {
    pub fn parse(url: &str) -> Result<Metadata, String> {
        // SQLAlchemy's form: sqlite:////absolute/path, or sqlite:///relative/path.
        if let Some(path) = url.strip_prefix("sqlite:///") {
            return Ok(Metadata::Sqlite(PathBuf::from(path)));
        }
        if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            return Ok(Metadata::Postgres(std::sync::Arc::new(Pg {
                url: url.to_string(),
                client: tokio::sync::Mutex::new(None),
            })));
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
    /// Answers the server refused (401 or 403): their data is not compared.
    refused: u64,
    /// The gateway's own allow or refuse against the server's (gateway_auth.rs).
    decisions_agree: u64,
    decisions_disagree: u64,
    decisions_failed: u64,
    /// Answered by the gateway itself (--serve).
    served: u64,
    /// Queries the gateway does not answer yet (pipeline_runs_unsupported).
    unsupported: u64,
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
    auth: Option<crate::gateway_auth::AuthSettings>,
    /// Routes the gateway answers itself when its decision allows the read (--serve).
    serve: Vec<String>,
    /// The project's folder (MAGE_GATEWAY_REPO_PATH), for the pipelines' names and tags.
    repo_path: Option<PathBuf>,
    counts: Mutex<BTreeMap<String, Counts>>,
    differences: Mutex<VecDeque<Value>>,
}

impl Shadow {
    #[cfg(test)]
    pub fn new(
        metadata: Metadata,
        routes: Vec<String>,
        auth: Option<crate::gateway_auth::AuthSettings>,
    ) -> Result<Shadow, String> {
        Shadow::serving(metadata, routes, auth, Vec::new())
    }

    pub fn serving(
        metadata: Metadata,
        routes: Vec<String>,
        auth: Option<crate::gateway_auth::AuthSettings>,
        serve: Vec<String>,
    ) -> Result<Shadow, String> {
        for route in &serve {
            if !routes.contains(route) {
                return Err(format!("--serve {route} needs --shadow {route} too."));
            }
        }
        if !serve.is_empty() && auth.is_none() {
            return Err("--serve needs the server's authentication settings \
                        (MAGE_GATEWAY_REQUIRE_AUTHENTICATION, MAGE_GATEWAY_CLIENT_ID)."
                .to_string());
        }
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
            auth,
            serve,
            repo_path: std::env::var("MAGE_GATEWAY_REPO_PATH")
                .ok()
                .map(PathBuf::from),
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
        if path == "/api/pipeline_runs" && enabled("pipeline_runs") {
            return Some("pipeline_runs");
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
        headers: &hyper::HeaderMap,
        reread: Reread,
    ) {
        // MageML answers a refusal with 200 and an error: its code says which.
        let error_code = serde_json::from_slice::<Value>(served).ok().and_then(|v| {
            v.get("error")
                .map(|e| e.get("code").and_then(Value::as_i64).unwrap_or(0))
        });
        let refused = matches!(error_code, Some(401 | 403));
        if let Some(settings) = &self.auth
            && (error_code.is_none() || refused)
        {
            let decided = self.decide(settings, route, path, query, headers).await;
            let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            let entry = counts.entry(route.to_string()).or_default();
            match decided {
                Ok(Some(allowed)) if allowed != refused => entry.decisions_agree += 1,
                Ok(Some(allowed)) => {
                    entry.decisions_disagree += 1;
                    drop(counts);
                    self.note(
                        route,
                        json!({"route": route, "query": query, "decision": {
                            "server": if refused { "refused" } else { "allowed" },
                            "gateway": if allowed { "allowed" } else { "refused" },
                        }}),
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    entry.decisions_failed += 1;
                    drop(counts);
                    self.note(
                        route,
                        json!({"route": route, "query": query, "decision_failed": error}),
                    );
                }
            }
        }
        if route == "pipeline_runs"
            && error_code.is_none()
            && pipeline_runs_unsupported(query, self.repo_path.as_deref()).is_some()
        {
            let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            counts.entry(route.to_string()).or_default().unsupported += 1;
            return;
        }
        if error_code.is_some() {
            if refused {
                let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
                counts.entry(route.to_string()).or_default().refused += 1;
            }
            return;
        }
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

    /// The gateway's own answer for a request it serves: Some when the route is served and
    /// its decision allows the read; None sends the request to the server, which answers
    /// refusals and anything the gateway cannot answer as it always has.
    pub async fn serve(
        &self,
        route: &'static str,
        path: &str,
        query: &str,
        headers: &hyper::HeaderMap,
    ) -> Option<Value> {
        let settings = self.auth.as_ref()?;
        if !self.serve.iter().any(|r| r == route) {
            return None;
        }
        if route == "pipeline_runs"
            && pipeline_runs_unsupported(query, self.repo_path.as_deref()).is_some()
        {
            return None;
        }
        match self.decide(settings, route, path, query, headers).await {
            Ok(Some(true)) => {}
            _ => return None,
        }
        let own = self.answer(route, path, query).await.ok()?;
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        counts.entry(route.to_string()).or_default().served += 1;
        Some(own)
    }

    /// Whether the gateway would serve the request: the caller as the middleware finds it,
    /// then has_at_least_viewer_role on the pipeline the route reads, or the project. None
    /// when there is nothing to decide (a run that does not exist).
    async fn decide(
        &self,
        settings: &crate::gateway_auth::AuthSettings,
        route: &str,
        path: &str,
        query: &str,
        headers: &hyper::HeaderMap,
    ) -> Result<Option<bool>, String> {
        use crate::gateway_auth::{Caller, Entity, caller, may_view};
        let query = Query::parse(query);
        let user = match caller(&self.metadata, settings, headers, &query.0).await? {
            Caller::Open => return Ok(Some(true)),
            // Both routes take the private scope only.
            Caller::Public | Caller::Refused(_) => return Ok(Some(false)),
            Caller::User(user) => user,
        };
        let pipeline = match route {
            "block_runs" | "pipeline_runs" => query.get("pipeline_uuid").map(str::to_string),
            "pipeline_run" => {
                let id = path
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or_default();
                let id = id
                    .parse::<i64>()
                    .map_err(|_| format!("{path} names no run"))?;
                let rows = read_rows(
                    &self.metadata,
                    "SELECT pipeline_uuid FROM pipeline_run WHERE id = {1}",
                    &[Raw::Int(id)],
                )
                .await?;
                match rows.first().and_then(|r| r.first()) {
                    Some(Raw::Text(uuid)) => Some(uuid.clone()),
                    Some(_) => None,
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        let entity = match &pipeline {
            Some(uuid) => Entity::Pipeline(uuid),
            None => Entity::Project,
        };
        may_view(&self.metadata, settings, user, entity)
            .await
            .map(Some)
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
            "pipeline_runs" => {
                let repo = self.repo_path.as_deref().ok_or("no repository path")?;
                pipeline_runs(&self.metadata, repo, &Query::parse(query)).await
            }
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
    // PostgreSQL keeps the status as an enum type of its own; it reads as text.
    let status = match metadata {
        Metadata::Postgres(_) => "a.status::text",
        Metadata::Sqlite(_) => "a.status",
    };
    let rows_sql = format!(
        "SELECT a.block_uuid, a.completed_at, a.created_at, a.id, a.pipeline_run_id, \
         a.started_at, {status}, a.updated_at, c.id, c.name {from} ORDER BY {order} LIMIT {} OFFSET {}",
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
        Metadata::Postgres(pg) => {
            let found = postgres_block_runs(pg, &count_sql, &rows_sql, &params).await;
            if found.is_err() {
                pg.forget().await;
            }
            found?
        }
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
    use rusqlite::{ToSql, types::ToSqlOutput};
    let connection = pooled(path)?;
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
    pg: &Pg,
    count_sql: &str,
    rows_sql: &str,
    params: &[Param],
) -> Result<(i64, Vec<Value>), String> {
    use tokio_postgres::types::ToSql;
    let client = pg.client().await?;
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
        .try_get(0)
        .map_err(|e| e.to_string())?;
    let rows = client
        .query(rows_sql, &refs)
        .await
        .map_err(|e| e.to_string())?
        .iter()
        .map(|row| -> Result<Value, String> {
            let time =
                |i| pg_get::<Option<chrono::DateTime<chrono::Utc>>>(row, i).map(postgres_time);
            let text = |i| {
                pg_get::<Option<String>>(row, i)
                    .map(|v| v.map(Value::String).unwrap_or(Value::Null))
            };
            let int = |i| {
                pg_get::<Option<i32>>(row, i).map(|v| v.map(Value::from).unwrap_or(Value::Null))
            };
            Ok(block_run([
                text(0)?,
                time(1)?,
                time(2)?,
                int(3)?,
                int(4)?,
                time(5)?,
                text(6)?,
                time(7)?,
                int(8)?,
                text(9)?,
            ]))
        })
        .collect::<Result<Vec<_>, String>>()?;
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
#[derive(Debug, Clone)]
pub(crate) enum Raw {
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
        Metadata::Postgres(pg) => {
            let found = postgres_run(pg, id, &run_sql, &blocks_sql, &schedule_sql).await;
            if found.is_err() {
                pg.forget().await;
            }
            found?
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
    let connection = pooled(path)?;
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

/// A column's value, or an error that names it: a type the gateway does not expect counts
/// as a failed shadow read, where `Row::get` panicked.
fn pg_get<'a, T: tokio_postgres::types::FromSql<'a>>(
    row: &'a tokio_postgres::Row,
    i: usize,
) -> Result<T, String> {
    row.try_get(i).map_err(|e| {
        let name = row.columns().get(i).map(|c| c.name()).unwrap_or("?");
        format!("column {name}: {e}")
    })
}

fn postgres_raw(row: &tokio_postgres::Row, i: usize, kind: Kind) -> Result<Raw, String> {
    let raw = match kind {
        Kind::Int => pg_get::<Option<i32>>(row, i)?.map(|v| Raw::Int(v.into())),
        Kind::Bool => pg_get::<Option<bool>>(row, i)?.map(Raw::Bool),
        Kind::Time => pg_get::<Option<chrono::DateTime<chrono::Utc>>>(row, i)?.map(Raw::Time),
        Kind::Text | Kind::Json | Kind::Enum => pg_get::<Option<String>>(row, i)?.map(Raw::Text),
    };
    Ok(raw.unwrap_or(Raw::Null))
}

async fn postgres_run(
    pg: &Pg,
    id: i64,
    run_sql: &str,
    blocks_sql: &str,
    schedule_sql: &str,
) -> Result<RunRows, String> {
    let client = pg.client().await?;
    let id = i32::try_from(id).map_err(|_| format!("run {id} is out of range"))?;
    let row = client
        .query_one(run_sql, &[&id])
        .await
        .map_err(|e| e.to_string())?;
    let run: Vec<Raw> = RUN_FIELDS
        .iter()
        .enumerate()
        .map(|(i, (_, kind))| postgres_raw(&row, i, *kind))
        .collect::<Result<_, _>>()?;
    let schedule_id: Option<i32> = pg_get(&row, RUN_SCHEDULE)?;
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
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schedule = match schedule_id {
        Some(schedule_id) => client
            .query_opt(schedule_sql, &[&schedule_id])
            .await
            .map_err(|e| e.to_string())?
            .map(|row| -> Result<_, String> {
                Ok((
                    i64::from(pg_get::<i32>(&row, 0)?),
                    pg_get::<Option<String>>(&row, 1)?,
                ))
            })
            .transpose()?,
        None => None,
    };
    Ok((run, blocks, schedule))
}

/// Read-only SQLite connections kept per database: each read opened one, and a runs list
/// made seven reads.
static SQLITE_POOL: std::sync::LazyLock<Mutex<BTreeMap<PathBuf, Vec<rusqlite::Connection>>>> =
    std::sync::LazyLock::new(|| Mutex::new(BTreeMap::new()));
const SQLITE_POOL_SIZE: usize = 8;

struct Pooled {
    path: PathBuf,
    connection: Option<rusqlite::Connection>,
}

impl std::ops::Deref for Pooled {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &rusqlite::Connection {
        self.connection.as_ref().expect("a pooled connection")
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            let mut pool = SQLITE_POOL.lock().unwrap_or_else(|p| p.into_inner());
            let kept = pool.entry(self.path.clone()).or_default();
            if kept.len() < SQLITE_POOL_SIZE {
                kept.push(connection);
            }
        }
    }
}

fn pooled(path: &std::path::Path) -> Result<Pooled, String> {
    let kept = SQLITE_POOL
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get_mut(path)
        .and_then(Vec::pop);
    let connection = match kept {
        Some(connection) => connection,
        None => rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| format!("{}: {e}", path.display()))?,
    };
    Ok(Pooled {
        path: path.to_path_buf(),
        connection: Some(connection),
    })
}

/// Rows of a query whose parameters are written `{1}`, `{2}`: `?1` on SQLite, `$1` on
/// PostgreSQL. Values read by their column's type (gateway_auth.rs).
pub(crate) async fn read_rows(
    metadata: &Metadata,
    template: &str,
    params: &[Raw],
) -> Result<Vec<Vec<Raw>>, String> {
    let mut sql = template.to_string();
    for n in (1..=params.len()).rev() {
        sql = sql.replace(&format!("{{{n}}}"), &placeholder(metadata, n));
    }
    match metadata {
        Metadata::Sqlite(path) => {
            let (path, params) = (path.clone(), params.to_vec());
            tokio::task::spawn_blocking(move || -> Result<Vec<Vec<Raw>>, String> {
                use rusqlite::{ToSql, types::ToSqlOutput};
                let connection = pooled(&path)?;
                let values: Vec<ToSqlOutput> = params
                    .iter()
                    .map(|p| match p {
                        Raw::Int(v) => ToSqlOutput::from(*v),
                        Raw::Text(v) => ToSqlOutput::from(v.clone()),
                        Raw::Bool(v) => ToSqlOutput::from(*v),
                        _ => ToSqlOutput::from(rusqlite::types::Null),
                    })
                    .collect();
                let refs: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
                let mut statement = connection.prepare(&sql).map_err(|e| e.to_string())?;
                let width = statement.column_count();
                statement
                    .query_map(refs.as_slice(), |row| {
                        (0..width).map(|i| row.get_ref(i).map(sqlite_raw)).collect()
                    })
                    .map_err(|e| e.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())
            })
            .await
            .map_err(|e| e.to_string())?
        }
        Metadata::Postgres(pg) => {
            use tokio_postgres::types::{ToSql, Type};
            let client = pg.client().await?;
            let owned: Vec<Box<dyn ToSql + Sync + Send>> = params
                .iter()
                .map(|p| -> Box<dyn ToSql + Sync + Send> {
                    match p {
                        // MageML's ids and integers are int4.
                        Raw::Int(v) => Box::new(*v as i32),
                        Raw::Text(v) => Box::new(v.clone()),
                        Raw::Bool(v) => Box::new(*v),
                        _ => Box::new(Option::<String>::None),
                    }
                })
                .collect();
            let refs: Vec<&(dyn ToSql + Sync)> = owned
                .iter()
                .map(|b| b.as_ref() as &(dyn ToSql + Sync))
                .collect();
            let rows = match client.query(sql.as_str(), &refs).await {
                Ok(rows) => rows,
                Err(error) => {
                    pg.forget().await;
                    return Err(error.to_string());
                }
            };
            rows.iter()
                .map(|row| {
                    row.columns()
                        .iter()
                        .enumerate()
                        .map(|(i, column)| {
                            let raw = match *column.type_() {
                                Type::INT2 => {
                                    pg_get::<Option<i16>>(row, i)?.map(|v| Raw::Int(v.into()))
                                }
                                Type::INT4 => {
                                    pg_get::<Option<i32>>(row, i)?.map(|v| Raw::Int(v.into()))
                                }
                                Type::INT8 => pg_get::<Option<i64>>(row, i)?.map(Raw::Int),
                                Type::BOOL => pg_get::<Option<bool>>(row, i)?.map(Raw::Bool),
                                Type::TIMESTAMPTZ => {
                                    pg_get::<Option<chrono::DateTime<chrono::Utc>>>(row, i)?
                                        .map(Raw::Time)
                                }
                                Type::TIMESTAMP => pg_get::<Option<chrono::NaiveDateTime>>(row, i)?
                                    .map(|at| Raw::Time(at.and_utc())),
                                _ => pg_get::<Option<String>>(row, i)?.map(Raw::Text),
                            };
                            Ok(raw.unwrap_or(Raw::Null))
                        })
                        .collect::<Result<Vec<_>, String>>()
                })
                .collect()
        }
    }
}

/// Why the gateway does not answer a GET /api/pipeline_runs query yet; None when it does.
/// It answers the runs pages' queries: filters by pipeline, status, backfill and trigger
/// scope, an order, pages, tags and the pipelines' names.
pub(crate) fn pipeline_runs_unsupported(
    query: &str,
    repo_path: Option<&std::path::Path>,
) -> Option<&'static str> {
    let query = Query::parse(query);
    if repo_path.is_none() {
        return Some("no repository path (or the project platform)");
    }
    for key in [
        "pipeline_tag[]",
        "pipeline_type",
        "global_data_product_uuid",
        "start_timestamp",
        "end_timestamp",
        "include_pipeline_type",
    ] {
        if query.get(key).is_some() {
            return Some("a filter the gateway does not read");
        }
    }
    if query.get("status") == Some("last_run_failed") {
        return Some("last_run_failed");
    }
    // PipelineRunResource groups a pipeline's retries unless told not to.
    if query.get("pipeline_uuid").is_some() && query.get("disable_retries_grouping").is_none() {
        return Some("grouped retries");
    }
    if query.get("_limit") == Some("0") {
        return Some("_limit=0");
    }
    None
}

/// GET /api/pipeline_runs as PipelineRunResource and its LIST presenter answer it.
async fn pipeline_runs(
    metadata: &Metadata,
    repo_path: &std::path::Path,
    query: &Query,
) -> Result<Value, String> {
    let limit = match query.get("_limit") {
        Some(text) => text.parse::<i64>().map_err(|_| format!("_limit {text}"))?,
        None => DEFAULT_LIMIT,
    };
    let offset = match query.get("_offset") {
        Some(text) => text.parse::<i64>().map_err(|_| format!("_offset {text}"))?,
        None => 0,
    };
    let mut filters = vec!["s.global_data_product_uuid IS NULL".to_string()];
    let mut params: Vec<Raw> = Vec::new();
    let param = |raw: Raw, params: &mut Vec<Raw>| {
        params.push(raw);
        format!("{{{}}}", params.len())
    };
    if query.get("include_all_pipeline_schedules").is_none() {
        let p = param(Raw::Text(repo_path.display().to_string()), &mut params);
        filters.push(format!("(s.repo_path = {p} OR s.repo_path IS NULL)"));
    }
    if let Some(backfill) = query.get("backfill_id") {
        let id = backfill
            .parse::<i64>()
            .map_err(|_| format!("backfill_id {backfill}"))?;
        let p = param(Raw::Int(id), &mut params);
        filters.push(format!("r.backfill_id = {p}"));
    }
    if let Some(pipeline) = query.get("pipeline_uuid") {
        let p = param(Raw::Text(pipeline.to_string()), &mut params);
        filters.push(format!("r.pipeline_uuid = {p}"));
    }
    let list = |text: &str| text.split(',').map(str::to_string).collect::<Vec<_>>();
    if let Some(uuids) = query.get("pipeline_uuid[]") {
        let ps: Vec<String> = list(uuids)
            .into_iter()
            .map(|u| param(Raw::Text(u), &mut params))
            .collect();
        filters.push(format!("r.pipeline_uuid IN ({})", ps.join(", ")));
    }
    // Statuses are stored by member name.
    let status_text = match metadata {
        Metadata::Postgres(_) => "CAST(r.status AS TEXT)",
        Metadata::Sqlite(_) => "r.status",
    };
    if let Some(status) = query.get("status") {
        let p = param(Raw::Text(status.to_uppercase()), &mut params);
        filters.push(format!("{status_text} = {p}"));
    }
    if let Some(statuses) = query.get("status[]") {
        let ps: Vec<String> = list(statuses)
            .into_iter()
            .map(|s| param(Raw::Text(s.to_uppercase()), &mut params))
            .collect();
        filters.push(format!("{status_text} IN ({})", ps.join(", ")));
    }
    let order = match query.get("order_by[]") {
        Some(order_by) => {
            let mut parts = Vec::new();
            for term in order_by.split(',') {
                let mut words = term.trim().split(' ');
                let column = words.next().unwrap_or_default();
                let direction = words.next().unwrap_or("asc").to_lowercase();
                if !RUN_FIELDS.iter().any(|(name, _)| *name == column)
                    || !matches!(direction.as_str(), "asc" | "desc")
                {
                    return Err(format!("order_by[] {order_by}"));
                }
                parts.push(format!("r.{column} {}", direction.to_uppercase()));
            }
            parts.join(", ")
        }
        None => "r.execution_date DESC, r.id DESC".to_string(),
    };
    let from = format!(
        "FROM pipeline_run r JOIN pipeline_schedule s ON r.pipeline_schedule_id = s.id WHERE {}",
        filters.join(" AND ")
    );
    let columns: Vec<String> = RUN_FIELDS
        .iter()
        .map(|(name, kind)| match (metadata, kind) {
            (Metadata::Postgres(_), Kind::Enum | Kind::Json) => format!("CAST(r.{name} AS TEXT)"),
            _ => format!("r.{name}"),
        })
        .collect();
    let schedule_type = match metadata {
        Metadata::Postgres(_) => "CAST(s.schedule_type AS TEXT)",
        Metadata::Sqlite(_) => "s.schedule_type",
    };
    let count = read_rows(metadata, &format!("SELECT count(*) {from}"), &params).await?;
    let count = match count.first().and_then(|r| r.first()) {
        Some(Raw::Int(n)) => *n,
        _ => 0,
    };
    let rows = read_rows(
        metadata,
        &format!(
            "SELECT {}, s.name, s.token, {schedule_type}, s.repo_path {from} ORDER BY {order} \
             LIMIT {} OFFSET {}",
            columns.join(", "),
            limit + 1,
            offset
        ),
        &params,
    )
    .await?;
    let has_next = rows.len() as i64 > limit;
    let rows: Vec<Vec<Raw>> = rows.into_iter().take(limit.max(0) as usize).collect();

    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r[6] {
            Raw::Int(id) => Some(id),
            _ => None,
        })
        .collect();
    let mut blocks_by_run: BTreeMap<i64, Vec<Value>> = BTreeMap::new();
    if !ids.is_empty() {
        let block_columns: Vec<String> = BLOCK_RUN_FIELDS
            .iter()
            .map(|(name, kind)| match (metadata, kind) {
                (Metadata::Postgres(_), Kind::Enum | Kind::Json) => format!("CAST({name} AS TEXT)"),
                _ => name.to_string(),
            })
            .collect();
        let placeholders: Vec<String> = (1..=ids.len()).map(|n| format!("{{{n}}}")).collect();
        let blocks = read_rows(
            metadata,
            &format!(
                "SELECT {} FROM block_run WHERE pipeline_run_id IN ({}) ORDER BY id",
                block_columns.join(", "),
                placeholders.join(", ")
            ),
            &ids.iter().map(|id| Raw::Int(*id)).collect::<Vec<_>>(),
        )
        .await?;
        for raws in blocks {
            let run = match raws[3] {
                Raw::Int(run) => run,
                _ => continue,
            };
            blocks_by_run
                .entry(run)
                .or_default()
                .push(Value::Object(object(BLOCK_RUN_FIELDS, raws)?));
        }
    }

    let tags = query.get("include_pipeline_tags").is_some();
    let mut runs = Vec::new();
    for mut raws in rows {
        let extra = raws.split_off(RUN_FIELDS.len());
        let mut run = object(RUN_FIELDS, raws)?;
        let id = run["id"].as_i64().unwrap_or_default();
        let blocks = blocks_by_run.remove(&id).unwrap_or_default();
        let completed = blocks.iter().filter(|b| b["status"] == "completed").count();
        run.insert("block_runs_count".into(), Value::from(blocks.len()));
        run.insert("completed_block_runs_count".into(), Value::from(completed));
        run.insert("block_runs".into(), Value::Array(blocks));
        let text = |raw: &Raw| match raw {
            Raw::Text(v) => Value::String(v.clone()),
            _ => Value::Null,
        };
        run.insert("pipeline_schedule_name".into(), text(&extra[0]));
        run.insert("pipeline_schedule_token".into(), text(&extra[1]));
        run.insert(
            "pipeline_schedule_type".into(),
            match &extra[2] {
                Raw::Text(v) => Value::String(v.to_lowercase()),
                _ => Value::Null,
            },
        );
        run.insert("repo_path".into(), text(&extra[3]));
        // Both keys are in every item of the LIST format; null unless asked for.
        let tag_value = if tags {
            let uuid = run["pipeline_uuid"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            pipeline_tags(repo_path, &uuid)
        } else {
            Value::Null
        };
        run.insert("pipeline_tags".into(), tag_value);
        run.insert("pipeline_type".into(), Value::Null);
        runs.push(Value::Object(run));
    }
    let mut meta = Map::new();
    meta.insert("count".into(), Value::from(count));
    meta.insert("next".into(), Value::Bool(has_next));
    if query.get("include_pipeline_uuids").is_some() {
        meta.insert(
            "pipeline_uuids".into(),
            Value::Array(pipeline_names(repo_path)),
        );
    }
    Ok(json!({ "pipeline_runs": runs, "metadata": meta }))
}

/// Pipeline.get_all_pipelines: the folders of pipelines/ that hold a metadata.yaml, in the
/// order the directory lists them.
fn pipeline_names(repo_path: &std::path::Path) -> Vec<Value> {
    std::fs::read_dir(repo_path.join("pipelines"))
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|e| e.path().join("metadata.yaml").is_file())
                .map(|e| Value::String(e.file_name().to_string_lossy().into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

/// PipelineRun.pipeline_tags: the tags of the pipeline's metadata.yaml, or null without
/// them (or without the file).
fn pipeline_tags(repo_path: &std::path::Path, uuid: &str) -> Value {
    // Read again only when the file changed: a runs page lists thirty runs a few times a
    // minute, mostly of the same pipelines.
    static TAGS: std::sync::LazyLock<Mutex<BTreeMap<PathBuf, (std::time::SystemTime, Value)>>> =
        std::sync::LazyLock::new(|| Mutex::new(BTreeMap::new()));
    let path = repo_path.join("pipelines").join(uuid).join("metadata.yaml");
    let Ok(modified) = std::fs::metadata(&path).and_then(|m| m.modified()) else {
        return Value::Null;
    };
    if let Some((at, tags)) = TAGS.lock().unwrap_or_else(|p| p.into_inner()).get(&path)
        && *at == modified
    {
        return tags.clone();
    }
    let tags = read_pipeline_tags(&path);
    TAGS.lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(path, (modified, tags.clone()));
    tags
}

fn read_pipeline_tags(path: &std::path::Path) -> Value {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Value::Null;
    };
    let Ok(documents) = yaml_rust2::YamlLoader::load_from_str(&text) else {
        return Value::Null;
    };
    match documents.first().map(|doc| &doc["tags"]) {
        Some(yaml_rust2::Yaml::Array(items)) => {
            Value::Array(items.iter().map(yaml_value).collect())
        }
        Some(yaml_rust2::Yaml::BadValue) | None | Some(yaml_rust2::Yaml::Null) => Value::Null,
        Some(other) => yaml_value(other),
    }
}

fn yaml_value(yaml: &yaml_rust2::Yaml) -> Value {
    use yaml_rust2::Yaml;
    match yaml {
        Yaml::String(s) => Value::String(s.clone()),
        Yaml::Integer(i) => Value::from(*i),
        Yaml::Real(r) => r.parse::<f64>().map(Value::from).unwrap_or(Value::Null),
        Yaml::Boolean(b) => Value::Bool(*b),
        Yaml::Array(items) => Value::Array(items.iter().map(yaml_value).collect()),
        _ => Value::Null,
    }
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
        let shadow = Shadow::new(
            Metadata::Sqlite("/x".into()),
            vec!["pipeline_run".into()],
            None,
        )
        .unwrap();
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
    fn run_lists_answer_the_runs_pages_queries_only() {
        let repo = std::path::Path::new("/project");
        let page = "disable_retries_grouping=true&include_pipeline_tags=true&_limit=30";
        assert_eq!(pipeline_runs_unsupported(page, Some(repo)), None);
        assert_eq!(
            pipeline_runs_unsupported("status=completed", Some(repo)),
            None
        );
        assert!(pipeline_runs_unsupported(page, None).is_some());
        assert!(pipeline_runs_unsupported("pipeline_tag[]=a", Some(repo)).is_some());
        assert!(pipeline_runs_unsupported("status=last_run_failed", Some(repo)).is_some());
        // One pipeline's runs come with their retries grouped unless told not to.
        assert!(pipeline_runs_unsupported("pipeline_uuid=p", Some(repo)).is_some());
        assert_eq!(
            pipeline_runs_unsupported("pipeline_uuid=p&disable_retries_grouping=1", Some(repo)),
            None
        );
    }

    #[test]
    fn pipeline_tags_come_from_metadata_yaml() {
        let folder = tempfile::tempdir().unwrap();
        let write = |uuid: &str, text: &str| {
            let dir = folder.path().join("pipelines").join(uuid);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("metadata.yaml"), text).unwrap();
        };
        write("block", "name: a\ntags:\n- nightly\n- finance\n");
        write("inline", "tags: [one, 2]\n");
        write("none", "name: c\n");
        assert_eq!(
            pipeline_tags(folder.path(), "block"),
            json!(["nightly", "finance"])
        );
        assert_eq!(pipeline_tags(folder.path(), "inline"), json!(["one", 2]));
        assert_eq!(pipeline_tags(folder.path(), "none"), Value::Null);
        assert_eq!(pipeline_tags(folder.path(), "missing"), Value::Null);
        let mut names: Vec<Value> = pipeline_names(folder.path());
        names.sort_by_key(|v| v.as_str().unwrap_or_default().to_string());
        assert_eq!(names, vec![json!("block"), json!("inline"), json!("none")]);
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

#[cfg(test)]
mod postgres_tests {
    use super::*;

    /// MageML's three tables in a schema of their own, with statuses as a PostgreSQL enum
    /// as SQLAlchemy makes them.
    const SCHEMA: &str = "
        CREATE TYPE blockrunstatus AS ENUM ('INITIAL', 'RUNNING', 'COMPLETED', 'FAILED');
        CREATE TYPE pipelinerunstatus AS ENUM ('INITIAL', 'RUNNING', 'COMPLETED', 'FAILED');
        CREATE TYPE executortype AS ENUM ('LOCAL_PYTHON', 'K8S');
        CREATE TABLE pipeline_schedule (id serial PRIMARY KEY, name varchar(255),
            pipeline_uuid varchar(255));
        CREATE TABLE pipeline_run (id serial PRIMARY KEY, created_at timestamptz,
            updated_at timestamptz, pipeline_schedule_id integer, pipeline_uuid varchar(255),
            execution_date timestamptz, status pipelinerunstatus, completed_at timestamptz,
            variables json, passed_sla boolean, event_variables json, metrics json,
            backfill_id integer, executor_type executortype, started_at timestamptz);
        CREATE TABLE block_run (id serial PRIMARY KEY, created_at timestamptz,
            updated_at timestamptz, pipeline_run_id integer, block_uuid varchar(255),
            status blockrunstatus, completed_at timestamptz, started_at timestamptz,
            metrics json, attempt integer NOT NULL DEFAULT 0);
        INSERT INTO pipeline_schedule (name, pipeline_uuid) VALUES ('daily', 'etl');
        INSERT INTO pipeline_run (created_at, updated_at, pipeline_schedule_id, pipeline_uuid,
            execution_date, status, variables, passed_sla, metrics, executor_type)
            VALUES ('2026-10-22 02:15:08+00', '2026-10-22 02:15:09+00', 1, 'etl',
            '2026-10-22 02:15:08.797857+00', 'COMPLETED', '{\"day\": 1}', false, null,
            'LOCAL_PYTHON');
        INSERT INTO block_run (created_at, updated_at, pipeline_run_id, block_uuid, status,
            started_at, completed_at, metrics, attempt) VALUES
            ('2026-10-22 02:15:08+00', '2026-10-22 02:15:09+00', 1, 'load', 'COMPLETED',
             '2026-10-22 02:15:08.934197+00', '2026-10-22 02:15:09.069058+00', '{\"rows\": 3}', 1),
            ('2026-10-22 02:15:08+00', '2026-10-22 02:15:09+00', 1, 'export', 'FAILED',
             null, null, null, 0);
    ";

    #[tokio::test]
    async fn postgres_answers_and_one_connection_for_every_read() {
        let Ok(url) = std::env::var("MAGE_TEST_POSTGRES_URL") else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        let schema = format!("shadow_{}", uuid::Uuid::new_v4().simple());
        let admin = mage_pg::connect_url(&url, "test").await.unwrap();
        admin
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; SET search_path TO {schema}; {SCHEMA}"
            ))
            .await
            .unwrap();
        let separator = if url.contains('?') { '&' } else { '?' };
        let metadata = Metadata::parse(&format!(
            "{url}{separator}options=-c%20search_path%3D{schema}"
        ))
        .unwrap();

        let listed = block_runs(&metadata, &Query::parse("pipeline_uuid=etl")).await;
        let detail = pipeline_run(&metadata, 1).await;
        let Metadata::Postgres(pg) = &metadata else {
            unreachable!()
        };
        let first = pg.client().await.unwrap();
        let again = block_runs(&metadata, &Query::parse("")).await.unwrap();
        let second = pg.client().await.unwrap();
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();

        let listed = listed.unwrap();
        assert_eq!(listed["metadata"], json!({"count": 2, "next": false}));
        // Newest first by started_at, and PostgreSQL puts a missing one first.
        assert_eq!(listed["block_runs"][0]["block_uuid"], "export");
        assert_eq!(
            listed["block_runs"][1]["completed_at"],
            "2026-10-22T02:15:09.069058+00:00"
        );
        assert_eq!(listed["block_runs"][1]["status"], "completed");
        let run = &detail.unwrap()["pipeline_run"];
        assert_eq!(run["execution_date"], "2026-10-22 02:15:08.797857+00:00");
        assert_eq!(run["executor_type"], "local_python");
        assert_eq!(run["variables"], json!({"day": 1}));
        assert_eq!(run["passed_sla"], false);
        assert_eq!(run["block_runs"][0]["metrics"], json!({"rows": 3}));
        assert_eq!(run["block_runs"][1]["pipeline_schedule_name"], "daily");
        assert_eq!(again["metadata"]["count"], 2);
        assert!(
            std::sync::Arc::ptr_eq(&first, &second),
            "a second connection was opened"
        );
    }
}
