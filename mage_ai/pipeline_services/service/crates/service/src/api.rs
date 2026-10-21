//! The HTTP API.
//!
//! Reads need the read token or the full token; starting runs, cancelling and control
//! commands need the full token. `/healthz` and `/metrics` need none: they hold counts and
//! durations, never data, variables or errors.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use mage_service_core::protocol::{
    ControlCommand, ControlReceipt, ControlRequest, RunDetail, RunRequest, RunStatus,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::service::{AdmissionError, BUCKETS, Service};

#[derive(Clone)]
pub struct Auth {
    pub token: Option<String>,
    pub read_token: Option<String>,
    /// Only sends messages to HTTP stream sources.
    pub ingest_token: Option<String>,
}

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<Service>,
    pub auth: Auth,
}

const MAX_BODY: usize = 1024 * 1024;
/// The largest webhook request; each stream also caps it at its batch size in bytes.
const MAX_INGEST_BODY: usize = 64 * 1024 * 1024;
const MAX_WAIT_SECONDS: u64 = 3600;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/pipelines", get(pipelines))
        .route(
            "/v1/pipelines/{pipeline}/runs",
            post(create_run).get(pipeline_runs),
        )
        .route("/v1/runs", get(list_runs))
        .route("/v1/runs/{id}", get(run))
        .route("/v1/runs/{id}/logs", get(logs))
        .route("/v1/runs/{id}/output", get(output))
        .route("/v1/runs/{id}/cancel", post(cancel))
        .route("/v1/snapshot", get(snapshot))
        .route("/v1/models", get(models))
        .route("/v1/audit/{model}", get(audit_records))
        .route("/v1/audit/{model}/verify", get(audit_verify))
        .route("/v1/streams", get(streams))
        .route("/v1/streams/{pipeline}", get(stream_detail))
        .route("/v1/streams/{pipeline}/tap", get(stream_tap))
        .route(
            "/v1/streams/{pipeline}/messages",
            post(ingest).layer(DefaultBodyLimit::max(MAX_INGEST_BODY)),
        )
        .route("/v1/control", post(control))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(state)
}

pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

impl From<crate::ledger::LedgerError> for ApiError {
    fn from(error: crate::ledger::LedgerError) -> ApiError {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

type ApiResult<T> = Result<T, ApiError>;

#[derive(PartialEq)]
enum Access {
    Read,
    Write,
    /// Sending messages to a stream's HTTP source.
    Ingest,
}

fn authorize(state: &AppState, headers: &HeaderMap, access: Access) -> ApiResult<()> {
    let Some(token) = &state.auth.token else {
        return Ok(());
    };
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let Some(provided) = provided else {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "Send the service token: Authorization: Bearer <token>.".into(),
        ));
    };
    if constant_time_eq(provided, token) {
        return Ok(());
    }
    if access == Access::Read
        && let Some(read) = &state.auth.read_token
        && constant_time_eq(provided, read)
    {
        return Ok(());
    }
    if access == Access::Ingest
        && let Some(ingest) = &state.auth.ingest_token
        && constant_time_eq(provided, ingest)
    {
        return Ok(());
    }
    if state
        .auth
        .ingest_token
        .as_deref()
        .is_some_and(|ingest| constant_time_eq(provided, ingest))
    {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "The ingest token only sends messages to streams.".into(),
        ));
    }
    if state
        .auth
        .read_token
        .as_deref()
        .is_some_and(|read| constant_time_eq(provided, read))
    {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "The read token cannot start, cancel or change runs; use the service token.".into(),
        ));
    }
    Err(ApiError(
        StatusCode::UNAUTHORIZED,
        "The token is not valid for this service.".into(),
    ))
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let status = if state.service.shutting_down() {
        "stopping"
    } else {
        "ok"
    };
    let code = if state.service.shutting_down() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (
        code,
        Json(json!({
            "status": status,
            "service": state.service.manifest.service.name,
            "pipelines": state.service.manifest.pipelines.len(),
            "running_runs": state.service.running_runs(),
        })),
    )
}

async fn pipelines(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    let pipelines: Vec<Value> = state
        .service
        .manifest
        .pipelines
        .iter()
        .map(|p| {
            json!({
                "id": p.uuid,
                "name": p.name,
                "description": p.description,
                "blocks": p.blocks.iter().map(|b| json!({
                    "id": b.uuid,
                    "type": b.block_type.as_str(),
                    "language": b.language,
                    "upstream": b.upstream,
                })).collect::<Vec<_>>(),
                "variables": p.variables.keys().collect::<Vec<_>>(),
                "triggers": p.triggers.iter().map(|t| json!({
                    "name": t.name, "kind": t.kind, "schedule": t.describe(), "active": t.active,
                    "after_pipeline": t.after_pipeline, "after_status": t.after_status,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(Json(json!({"pipelines": pipelines})))
}

#[derive(Deserialize)]
struct WaitQuery {
    wait: Option<u64>,
    /// A block whose output the response carries once the run completes.
    output: Option<String>,
}

/// The run request in a body; empty is a run with the pipeline's own variables. Integers too
/// large for 64 bits are refused rather than rounded.
fn run_request(body: &[u8]) -> ApiResult<RunRequest> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(RunRequest::default());
    }
    let text = std::str::from_utf8(body).map_err(|_| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "the body is not UTF-8 JSON.".into(),
        )
    })?;
    if let Some(message) = mage_service_core::json::check_integers(text, "The request") {
        return Err(ApiError(StatusCode::BAD_REQUEST, message));
    }
    serde_json::from_str(text).map_err(|error| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("the body is not a valid run request: {error}"),
        )
    })
}

async fn create_run(
    State(state): State<AppState>,
    Path(pipeline): Path<String>,
    Query(query): Query<WaitQuery>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> ApiResult<Response> {
    authorize(&state, &headers, Access::Write)?;
    let request = run_request(&body)?;
    if let Some(key) = &request.idempotency_key
        && (key.is_empty() || key.len() > 200)
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "idempotency_key must have 1 to 200 characters.".into(),
        ));
    }
    let (id, created) = state
        .service
        .submit(
            &pipeline,
            "api",
            &request.variables,
            request.idempotency_key.as_deref(),
        )
        .map_err(|error| match error {
            AdmissionError::UnknownPipeline(_) => {
                ApiError(StatusCode::NOT_FOUND, error.to_string())
            }
            AdmissionError::ShuttingDown => {
                ApiError(StatusCode::SERVICE_UNAVAILABLE, error.to_string())
            }
            AdmissionError::Ledger(e) => e.into(),
        })?;
    let mut detail =
        state.service.ledger.run(&id)?.ok_or_else(|| {
            ApiError(StatusCode::INTERNAL_SERVER_ERROR, "the run vanished".into())
        })?;
    if let Some(seconds) = query.wait {
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(seconds.min(MAX_WAIT_SECONDS));
        let started = tokio::time::Instant::now();
        while !detail.run.status.finished() && tokio::time::Instant::now() < deadline {
            // Checks often at first, so a short run (an inference) answers without a
            // delay; a ledger read takes microseconds.
            let pause = if started.elapsed() < Duration::from_secs(1) {
                5
            } else {
                50
            };
            tokio::time::sleep(Duration::from_millis(pause)).await;
            if let Some(current) = state.service.ledger.run(&id)? {
                detail = current;
            }
        }
    }
    let code = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let mut body = serde_json::to_value(&detail)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if let Some(block) = &query.output
        && detail.run.status == RunStatus::Completed
    {
        inline_output(&state, &detail, block, &mut body).await?;
    }
    Ok((
        code,
        [(header::LOCATION, format!("/v1/runs/{id}"))],
        Json(body),
    )
        .into_response())
}

/// Largest output a run's response carries; larger ones are read from `output_url`.
const MAX_INLINE_OUTPUT: u64 = 8 * 1024 * 1024;

/// The block's JSON output in the response as `output`; other kinds as `output_url`.
async fn inline_output(
    state: &AppState,
    detail: &RunDetail,
    block: &str,
    body: &mut Value,
) -> ApiResult<()> {
    let record = output_record(detail, block, 0)?;
    let url = format!("/v1/runs/{}/output?block={block}", detail.run.id);
    let path = output_path(state, record)?;
    let small = std::fs::metadata(&path)
        .map(|m| m.len() <= MAX_INLINE_OUTPUT)
        .unwrap_or(false);
    let rows = if small && is_ipc_frame(record) {
        let path = path.clone();
        tokio::task::spawn_blocking(move || ipc_rows_json(&path))
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };
    if record.get("kind").and_then(Value::as_str) == Some("json") && small {
        let text = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        body["output"] = serde_json::from_str(&text)
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    } else if let Some(rows) = rows {
        body["output"] = rows;
    } else {
        body["output_url"] = json!(url);
    }
    Ok(())
}

/// A frame stored as an Arrow IPC file, as Python, R and Rust blocks write them.
fn is_ipc_frame(record: &Value) -> bool {
    record.get("kind").and_then(Value::as_str) == Some("frame")
        && record
            .get("format")
            .and_then(Value::as_str)
            .unwrap_or("ipc")
            == "ipc"
}

/// The rows of an Arrow IPC file as a JSON array of objects, one per row.
fn ipc_rows_json(path: &std::path::Path) -> Result<Value, String> {
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let reader = arrow_ipc::reader::FileReader::try_new(file, None).map_err(|e| e.to_string())?;
    // Every row has every column; a missing value is null.
    let mut writer = arrow_json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json::writer::JsonArray>(Vec::new());
    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;
    let bytes = writer.into_inner();
    if bytes.is_empty() {
        return Ok(json!([]));
    }
    // A float NaN or infinity has no JSON form; such a frame is served as Arrow.
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

/// A block's output record from a run, by position.
fn output_record<'a>(detail: &'a RunDetail, block: &str, index: usize) -> ApiResult<&'a Value> {
    let summary = detail
        .blocks
        .iter()
        .find(|b| b.block == block)
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                format!("run {} has no block {block}", detail.run.id),
            )
        })?;
    summary.outputs.get(index).ok_or_else(|| {
        ApiError(
            StatusCode::NOT_FOUND,
            format!(
                "block {block} of run {} has no output {index}; it has {}",
                detail.run.id,
                summary.outputs.len()
            ),
        )
    })
}

/// The output file of a record, which must be in the service's data directory.
fn output_path(state: &AppState, record: &Value) -> ApiResult<std::path::PathBuf> {
    let path = record
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "the output has no file".into()))?;
    let path = std::fs::canonicalize(path).map_err(|_| {
        ApiError(
            StatusCode::GONE,
            "the output file is gone; runs older than the retention period lose them".into(),
        )
    })?;
    let data_dir = std::fs::canonicalize(&state.service.data_dir)
        .unwrap_or_else(|_| state.service.data_dir.clone());
    if !path.starts_with(&data_dir) {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "the output is outside the service data".into(),
        ));
    }
    Ok(path)
}

/// The media type an output kind is served as; None for kinds only Python can read.
fn content_type(record: &Value) -> Option<&'static str> {
    match (
        record.get("kind").and_then(Value::as_str),
        record.get("format").and_then(Value::as_str),
    ) {
        (Some("json"), _) => Some("application/json"),
        (Some("frame"), Some("parquet")) => Some("application/vnd.apache.parquet"),
        (Some("frame"), _) => Some("application/vnd.apache.arrow.file"),
        (Some("numpy"), _) => Some("application/octet-stream"),
        _ => None,
    }
}

#[derive(Deserialize)]
struct OutputQuery {
    block: String,
    index: Option<usize>,
    /// `json` serves a frame as JSON rows instead of its Arrow file.
    format: Option<String>,
}

/// A block's stored output, as its file: JSON, an Arrow IPC file, Parquet or NumPy.
async fn output(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<OutputQuery>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    authorize(&state, &headers, Access::Read)?;
    let detail = state
        .service
        .ledger
        .run(&id)?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no run {id}")))?;
    let record = output_record(&detail, &query.block, query.index.unwrap_or(0))?;
    if let Some(value) = record.get("value") {
        return Ok(Json(value.clone()).into_response());
    }
    let media = content_type(record).ok_or_else(|| {
        ApiError(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!(
                "block {} returned a {} output, which only Python can read; return a \
                 DataFrame, a list or a dict to serve it",
                query.block,
                record
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("pickled")
            ),
        )
    })?;
    let path = output_path(&state, record)?;
    if query.format.as_deref() == Some("json") && is_ipc_frame(record) {
        let rows = tokio::task::spawn_blocking(move || ipc_rows_json(&path))
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(|e| {
                ApiError(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!("the frame cannot be written as JSON ({e}); read it as Arrow"),
                )
            })?;
        return Ok(Json(rows).into_response());
    }
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(([(header::CONTENT_TYPE, media)], bytes).into_response())
}

#[derive(Deserialize)]
struct ListQuery {
    pipeline: Option<String>,
    status: Option<String>,
    limit: Option<u32>,
}

async fn list_runs(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    runs_response(
        &state,
        query.pipeline.as_deref(),
        query.status.as_deref(),
        query.limit,
    )
}

async fn pipeline_runs(
    State(state): State<AppState>,
    Path(pipeline): Path<String>,
    Query(query): Query<ListQuery>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    if state.service.manifest.pipeline(&pipeline).is_none() {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("the service has no pipeline {pipeline}"),
        ));
    }
    runs_response(
        &state,
        Some(&pipeline),
        query.status.as_deref(),
        query.limit,
    )
}

fn runs_response(
    state: &AppState,
    pipeline: Option<&str>,
    status: Option<&str>,
    limit: Option<u32>,
) -> ApiResult<Json<Value>> {
    let status = match status {
        None => None,
        Some(text) => Some(RunStatus::parse(text).ok_or_else(|| {
            ApiError(
                StatusCode::BAD_REQUEST,
                "status is one of queued, running, completed, failed, cancelled.".into(),
            )
        })?),
    };
    let runs =
        state
            .service
            .ledger
            .list_runs(pipeline, status, limit.unwrap_or(50).clamp(1, 1000))?;
    Ok(Json(json!({"runs": runs})))
}

async fn run(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<RunDetail>> {
    authorize(&state, &headers, Access::Read)?;
    state
        .service
        .ledger
        .run(&id)?
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no run {id}")))
}

#[derive(Deserialize)]
struct LogQuery {
    block: Option<String>,
    tail: Option<usize>,
    format: Option<String>,
}

async fn logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<LogQuery>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    authorize(&state, &headers, Access::Read)?;
    if state.service.ledger.run(&id)?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("no run {id}")));
    }
    let path = state.service.run_dir(&id).join("run.log");
    let text = match tokio::fs::read_to_string(&path).await {
        Ok(text) => text,
        Err(_) => match state.service.ledger.other_owner(&id)? {
            // An empty log would read as a run that printed nothing.
            Some((instance, host)) => {
                return Err(ApiError(
                    StatusCode::CONFLICT,
                    format!(
                        "Run {id} ran on instance {instance}{}; its log is on that \
                         instance's data volume.",
                        if host.is_empty() {
                            String::new()
                        } else {
                            format!(" (host {host})")
                        }
                    ),
                ));
            }
            None => String::new(),
        },
    };
    let mut lines: Vec<Value> = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| match &query.block {
            Some(block) => line.get("block").and_then(Value::as_str) == Some(block.as_str()),
            None => true,
        })
        .collect();
    if let Some(tail) = query.tail
        && lines.len() > tail
    {
        lines.drain(..lines.len() - tail);
    }
    if query.format.as_deref() == Some("text") {
        let text: String = lines
            .iter()
            .map(|line| {
                let ts = line.get("ts").and_then(Value::as_str).unwrap_or("");
                let block = line.get("block").and_then(Value::as_str);
                let message = line.get("message").and_then(Value::as_str).unwrap_or("");
                match block {
                    Some(block) => format!("{ts} [{block}] {message}\n"),
                    None => format!("{ts} {message}\n"),
                }
            })
            .collect();
        return Ok(([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], text).into_response());
    }
    Ok(Json(json!({"lines": lines})).into_response())
}

async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Write)?;
    if state.service.ledger.run(&id)?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, format!("no run {id}")));
    }
    let cancelled = state.service.cancel(&id)?;
    Ok(Json(json!({"id": id, "cancelling": cancelled})))
}

/// The embedded models with the metadata recorded at export: version, run, flavors,
/// signature, params and metrics.
/// `GET /v1/streams`: each stream's state, counts, acknowledged positions and last error.
async fn streams(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    let streams = state
        .service
        .streams
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let list: Vec<Value> = streams
        .iter()
        .map(|(pipeline, status)| {
            let status = status.lock().map(|s| s.clone()).unwrap_or_default();
            let mut value = json!(status);
            value["pipeline"] = json!(pipeline);
            value
        })
        .collect();
    Ok(Json(json!({ "streams": list })))
}

/// `POST /v1/streams/{pipeline}/messages`: a webhook's messages for a stream with an HTTP
/// source (ADR-0031). The answer is 200 only once every required sink confirmed them; 503
/// with `Retry-After` when the stream is busy or stopped, 504 when they were not confirmed in
/// time. An `Idempotency-Key` header makes a retry safe.
async fn ingest(
    State(state): State<AppState>,
    Path(pipeline): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    match ingest_messages(&state, &pipeline, &headers, &body).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn ingest_messages(
    state: &AppState,
    pipeline: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> ApiResult<Response> {
    use mage_connector_http::{IngestError, IngestOutcome};

    authorize(state, headers, Access::Ingest)?;
    let ingest = state
        .service
        .ingest
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(pipeline)
        .cloned()
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                format!("The service has no stream {pipeline} with an HTTP source."),
            )
        })?;
    if body.len() > ingest.max_bytes {
        return Err(ApiError(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "The request has {} bytes; this stream takes at most {}.",
                body.len(),
                ingest.max_bytes
            ),
        ));
    }
    if let Some(check) = &ingest.signature {
        let value = headers
            .get(check.header.as_str())
            .and_then(|v| v.to_str().ok());
        if !check.verify(value, body) {
            return Err(ApiError(
                StatusCode::UNAUTHORIZED,
                format!("The {} header does not sign this request.", check.header),
            ));
        }
    }
    let stopped = state
        .service
        .streams
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(pipeline)
        .and_then(|s| s.lock().ok().map(|s| (s.state, s.last_error.clone())))
        .filter(|(stream_state, _)| *stream_state == crate::stream::StreamState::Failed);
    if let Some((_, error)) = stopped {
        return Ok(busy(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "The stream stopped: {}",
                error.unwrap_or_else(|| "see its log".into())
            ),
            Duration::from_secs(30),
        ));
    }
    let messages = parse_messages(headers, body)?;
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(str::to_string)
        .or_else(|| {
            // Senders that cannot set the header (Amplitude) identify a message in its body.
            ingest
                .key_field
                .as_deref()
                .and_then(|field| mage_connector_http::key_from_field(&messages, field))
        });
    if key.as_ref().is_some_and(|k| k.len() > 200) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "The Idempotency-Key has more than 200 characters.".into(),
        ));
    }
    match ingest.handle.submit(messages, key).await {
        Ok(IngestOutcome::Delivered { messages }) => {
            Ok(Json(json!({"accepted": messages})).into_response())
        }
        Ok(IngestOutcome::Duplicate { messages }) => {
            Ok(Json(json!({"accepted": messages, "duplicate": true})).into_response())
        }
        Err(error @ IngestError::Busy { retry_after }) => Ok(busy(
            StatusCode::SERVICE_UNAVAILABLE,
            error.to_string(),
            retry_after,
        )),
        Err(error @ IngestError::Unavailable(_)) => Ok(busy(
            StatusCode::SERVICE_UNAVAILABLE,
            error.to_string(),
            Duration::from_secs(5),
        )),
        Err(error @ IngestError::Timeout) => {
            Err(ApiError(StatusCode::GATEWAY_TIMEOUT, error.to_string()))
        }
        Err(error @ IngestError::Empty) => {
            Err(ApiError(StatusCode::BAD_REQUEST, error.to_string()))
        }
    }
}

fn busy(status: StatusCode, message: String, retry_after: Duration) -> Response {
    let seconds = retry_after.as_secs().max(1).to_string();
    (
        status,
        [(header::RETRY_AFTER, seconds)],
        Json(json!({"error": message})),
    )
        .into_response()
}

/// A JSON array of messages, one JSON message, or JSON lines (`application/x-ndjson`); each
/// message is kept as the JSON it is.
fn parse_messages(
    headers: &HeaderMap,
    body: &[u8],
) -> ApiResult<Vec<Box<serde_json::value::RawValue>>> {
    use serde_json::value::RawValue;

    let invalid = |error: String| {
        ApiError(
            StatusCode::BAD_REQUEST,
            format!("The body is not JSON messages: {error}"),
        )
    };
    let text = std::str::from_utf8(body).map_err(|_| invalid("it is not UTF-8".into()))?;
    let lines = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("ndjson") || v.contains("jsonlines"));
    if lines {
        return text
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim().is_empty())
            .map(|(number, line)| {
                RawValue::from_string(line.trim().to_string())
                    .map_err(|e| invalid(format!("line {}: {e}", number + 1)))
            })
            .collect();
    }
    if text.trim_start().starts_with('[') {
        let messages: Vec<Box<RawValue>> =
            serde_json::from_str(text).map_err(|e| invalid(e.to_string()))?;
        return Ok(messages);
    }
    let message: Box<RawValue> = serde_json::from_str(text).map_err(|e| invalid(e.to_string()))?;
    Ok(vec![message])
}

#[derive(Deserialize)]
struct TapQuery {
    point: Option<String>,
    limit: Option<usize>,
}

/// The last messages at a tap point of a stream (`source` by default), oldest first, and the
/// points there are. Only with MAGE_SERVICE_TAP=on, as development runs set it.
async fn stream_tap(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(pipeline): Path<String>,
    Query(query): Query<TapQuery>,
) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    let streams = state
        .service
        .streams
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let status = streams
        .get(&pipeline)
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                format!("The service has no stream {pipeline}."),
            )
        })?
        .lock()
        .map(|s| s.clone())
        .unwrap_or_default();
    if !status.tap_enabled {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "The tap is off: development runs turn it on (MAGE_SERVICE_TAP=on), since it \
             keeps message data."
                .into(),
        ));
    }
    let point = query.point.unwrap_or_else(|| "source".into());
    let limit = query
        .limit
        .unwrap_or(crate::stream::TAP_SIZE)
        .min(crate::stream::TAP_SIZE);
    let kept = status.taps.get(&point).cloned().unwrap_or_default();
    let messages: Vec<_> = kept.iter().skip(kept.len().saturating_sub(limit)).collect();
    Ok(Json(json!({
        "pipeline": pipeline,
        "point": point,
        "points": status.taps.keys().collect::<Vec<_>>(),
        "messages": messages,
    })))
}

async fn stream_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(pipeline): Path<String>,
) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    let streams = state
        .service
        .streams
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let status = streams
        .get(&pipeline)
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                format!("The service has no stream {pipeline}."),
            )
        })?
        .lock()
        .map(|s| s.clone())
        .unwrap_or_default();
    let mut value = json!(status);
    value["pipeline"] = json!(pipeline);
    Ok(Json(value))
}

#[derive(Deserialize)]
struct ModelsQuery {
    /// Days of shadow records the challengers' comparison covers.
    days: Option<u32>,
}

async fn models(
    State(state): State<AppState>,
    Query(query): Query<ModelsQuery>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    let days = query.days.unwrap_or(7).clamp(1, 366);
    let shadow_dir = state.service.data_dir.join("shadow");
    let models_list = state.service.manifest.models.clone();
    // The comparison reads files: off the async threads.
    let mut comparisons =
        tokio::task::spawn_blocking(move || crate::shadow::all(&shadow_dir, &models_list, days))
            .await
            .unwrap_or_default();
    let mut models = Vec::new();
    for model in &state.service.manifest.models {
        let metadata_path = state
            .service
            .service_dir
            .join(&model.path)
            .join("mage-model.json");
        let metadata = tokio::fs::read_to_string(&metadata_path)
            .await
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .unwrap_or(Value::Null);
        models.push(json!({
            "name": model.name,
            "uri": model.uri,
            "version": model.version,
            "run_id": model.run_id,
            "flavors": model.flavors,
            "sha256": model.sha256,
            "size_bytes": model.size_bytes,
            "metadata": metadata,
            "shadow_sample": model.shadow_sample,
            "challengers": comparisons.remove(&model.name).unwrap_or_default(),
        }));
    }
    Ok(Json(json!({"models": models})))
}

#[derive(Deserialize)]
struct AuditQuery {
    key: String,
    days: Option<u32>,
    limit: Option<usize>,
}

/// The audit records of one input key, newest first.
async fn audit_records(
    State(state): State<AppState>,
    Path(model): Path<String>,
    Query(query): Query<AuditQuery>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    known_model(&state, &model)?;
    let dir = state.service.data_dir.join("audit");
    let (days, limit) = (
        query.days.unwrap_or(30).clamp(1, 3660),
        query.limit.unwrap_or(100).clamp(1, 10_000),
    );
    let records = tokio::task::spawn_blocking(move || {
        crate::audit::find(&dir, &model, &query.key, days, limit)
    })
    .await
    .unwrap_or_default();
    Ok(Json(json!({ "records": records })))
}

/// Whether every audit file of the model is an unbroken chain.
async fn audit_verify(
    State(state): State<AppState>,
    Path(model): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    known_model(&state, &model)?;
    let dir = state.service.data_dir.join("audit");
    let chains = tokio::task::spawn_blocking(move || crate::audit::verify(&dir, &model))
        .await
        .unwrap_or_default();
    let intact = chains.iter().all(|c| c.broken_at.is_none());
    Ok(Json(json!({ "intact": intact, "files": chains })))
}

fn known_model(state: &AppState, model: &str) -> ApiResult<()> {
    if state
        .service
        .manifest
        .models
        .iter()
        .any(|m| m.name == model)
    {
        Ok(())
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, format!("no model {model}")))
    }
}

async fn snapshot(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
    Ok(Json(
        serde_json::to_value(state.service.snapshot()?).unwrap_or(Value::Null),
    ))
}

async fn control(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ControlRequest>,
) -> ApiResult<Json<ControlReceipt>> {
    authorize(&state, &headers, Access::Write)?;
    let pipeline = state
        .service
        .manifest
        .pipeline(&request.pipeline_id)
        .ok_or_else(|| {
            ApiError(
                StatusCode::NOT_FOUND,
                format!("no pipeline {}", request.pipeline_id),
            )
        })?;
    let (paused, max) = match request.command {
        ControlCommand::Pause {} => (Some(true), None),
        ControlCommand::Resume {} => (Some(false), None),
        ControlCommand::SetConcurrency { max_in_flight } => {
            if !(1..=64).contains(&max_in_flight) {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "max_in_flight must be between 1 and 64.".into(),
                ));
            }
            (None, Some(max_in_flight))
        }
    };
    // Settings exist before they change.
    state
        .service
        .ledger
        .settings(&pipeline.uuid, pipeline.max_concurrent_runs)?;
    let (_, applied) = state.service.ledger.update_settings(
        &pipeline.uuid,
        request.expected_config_revision,
        paused,
        max,
    )?;
    if !applied {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "The pipeline's settings changed since they were read; review them again.".into(),
        ));
    }
    if let Some(paused) = paused
        && let Some(switch) = state
            .service
            .stream_pause
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&pipeline.uuid)
    {
        switch.send_replace(paused);
    }
    state.service.wake();
    Ok(Json(ControlReceipt {
        request_id: request.request_id,
        pipeline: state.service.pipeline_snapshot(pipeline)?,
    }))
}

async fn metrics(State(state): State<AppState>) -> ApiResult<Response> {
    use std::fmt::Write;
    let mut out = String::new();
    let service = &state.service;
    let _ = writeln!(
        out,
        "# HELP mage_service_runs Runs by pipeline and status, in the run history."
    );
    let _ = writeln!(out, "# TYPE mage_service_runs gauge");
    for pipeline in &service.manifest.pipelines {
        for status in [
            RunStatus::Queued,
            RunStatus::Running,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ] {
            let count = service.ledger.count(&pipeline.uuid, status)?;
            let _ = writeln!(
                out,
                "mage_service_runs{{pipeline=\"{}\",status=\"{}\"}} {count}",
                escape(&pipeline.uuid),
                status.as_str()
            );
        }
    }
    let streams: Vec<(String, crate::stream::StreamStatus)> = service
        .streams
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .map(|(k, v)| (k.clone(), v.lock().map(|s| s.clone()).unwrap_or_default()))
        .collect();
    if !streams.is_empty() {
        for (name, help) in [
            (
                "messages_in",
                "Messages a stream read since the service started.",
            ),
            (
                "messages_out",
                "Messages a stream's blocks produced since the service started.",
            ),
            (
                "batches",
                "Batches a stream read since the service started.",
            ),
            (
                "retries",
                "Retried transforms and writes since the service started.",
            ),
            (
                "dead_lettered_batches",
                "Batches sent to the dead-letter sink.",
            ),
            (
                "alerts",
                "Alerts a stream raised, such as a sink failing repeatedly.",
            ),
        ] {
            let _ = writeln!(out, "# HELP mage_service_stream_{name}_total {help}");
            let _ = writeln!(out, "# TYPE mage_service_stream_{name}_total counter");
            for (pipeline, s) in &streams {
                let value = match name {
                    "messages_in" => s.messages_in,
                    "messages_out" => s.messages_out,
                    "batches" => s.batches,
                    "retries" => s.retries,
                    "dead_lettered_batches" => s.dead_lettered_batches,
                    _ => s.alerts,
                };
                let _ = writeln!(
                    out,
                    "mage_service_stream_{name}_total{{pipeline=\"{}\"}} {value}",
                    escape(pipeline)
                );
            }
        }
        let _ = writeln!(
            out,
            "# HELP mage_service_stream_lag_messages Messages behind the end of each partition \
             this instance reads."
        );
        let _ = writeln!(out, "# TYPE mage_service_stream_lag_messages gauge");
        for (pipeline, s) in &streams {
            for (partition, lag) in &s.lag {
                let _ = writeln!(
                    out,
                    "mage_service_stream_lag_messages{{pipeline=\"{}\",partition=\"{partition}\"}} {lag}",
                    escape(pipeline)
                );
            }
        }
        let _ = writeln!(out, "# HELP mage_service_stream_up 1 while a stream runs.");
        let _ = writeln!(out, "# TYPE mage_service_stream_up gauge");
        for (pipeline, s) in &streams {
            let up = matches!(s.state, crate::stream::StreamState::Running) as u8;
            let _ = writeln!(
                out,
                "mage_service_stream_up{{pipeline=\"{}\"}} {up}",
                escape(pipeline)
            );
        }
    }
    let _ = writeln!(
        out,
        "# HELP mage_service_block_seconds Block attempt durations since the service started."
    );
    let _ = writeln!(out, "# TYPE mage_service_block_seconds histogram");
    for ((pipeline, block), m) in service.block_metrics() {
        let labels = format!(
            "pipeline=\"{}\",block=\"{}\"",
            escape(&pipeline),
            escape(&block)
        );
        for (bound, count) in BUCKETS.iter().zip(m.buckets.iter()) {
            let _ = writeln!(
                out,
                "mage_service_block_seconds_bucket{{{labels},le=\"{bound}\"}} {count}"
            );
        }
        let total = m.succeeded + m.failed;
        let _ = writeln!(
            out,
            "mage_service_block_seconds_bucket{{{labels},le=\"+Inf\"}} {total}"
        );
        let _ = writeln!(
            out,
            "mage_service_block_seconds_sum{{{labels}}} {}",
            m.seconds_sum
        );
        let _ = writeln!(out, "mage_service_block_seconds_count{{{labels}}} {total}");
        let _ = writeln!(
            out,
            "mage_service_block_failures_total{{{labels}}} {}",
            m.failed
        );
    }
    let monitors = service.monitor_metrics();
    if !monitors.is_empty() {
        let _ = writeln!(
            out,
            "# HELP mage_service_monitor_result The last result of each monitor: 0 pass, 1 hold, \
             2 warn, 3 fail, 4 error."
        );
        let _ = writeln!(out, "# TYPE mage_service_monitor_result gauge");
        for ((pipeline, block, monitor), m) in &monitors {
            let _ = writeln!(
                out,
                "mage_service_monitor_result{{pipeline=\"{}\",block=\"{}\",monitor=\"{}\"}} {}",
                escape(pipeline),
                escape(block),
                escape(monitor),
                m.result
            );
        }
        let _ = writeln!(
            out,
            "# HELP mage_service_monitor_check The value of each check of the last run of each \
             monitor: drift, missing share, rows, age, metrics."
        );
        let _ = writeln!(out, "# TYPE mage_service_monitor_check gauge");
        for ((pipeline, block, monitor), m) in &monitors {
            for (check, column, segment, value) in &m.checks {
                let _ = writeln!(
                    out,
                    "mage_service_monitor_check{{pipeline=\"{}\",block=\"{}\",monitor=\"{}\",\
                     check=\"{}\",column=\"{}\",segment=\"{}\"}} {value}",
                    escape(pipeline),
                    escape(block),
                    escape(monitor),
                    escape(check),
                    escape(column),
                    escape(segment)
                );
            }
        }
    }
    let _ = writeln!(out, "# TYPE mage_service_up gauge");
    let _ = writeln!(out, "mage_service_up 1");
    let _ = writeln!(
        out,
        "mage_service_start_time_seconds {}",
        service.started_at.timestamp()
    );
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response())
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::{content_type, ipc_rows_json};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn a_frame_reads_as_json_rows_compressed_or_not() {
        use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
        use arrow_ipc::writer::{FileWriter, IpcWriteOptions};

        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "score",
                Arc::new(Float64Array::from(vec![Some(0.25), None])) as ArrayRef,
            ),
            (
                "segment",
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
        ])
        .unwrap();
        for compression in [None, Some(arrow_ipc::CompressionType::LZ4_FRAME)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("output_0.arrow");
            let options = IpcWriteOptions::default()
                .try_with_compression(compression)
                .unwrap();
            let file = std::fs::File::create(&path).unwrap();
            let mut writer =
                FileWriter::try_new_with_options(file, &batch.schema(), options).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();

            assert_eq!(
                ipc_rows_json(&path).unwrap(),
                json!([
                    {"id": 1, "score": 0.25, "segment": "a"},
                    {"id": 2, "score": null, "segment": "b"},
                ])
            );
        }
    }

    #[test]
    fn outputs_are_served_with_their_media_type() {
        let cases = [
            (json!({"kind": "json"}), Some("application/json")),
            (
                json!({"kind": "frame", "format": "ipc"}),
                Some("application/vnd.apache.arrow.file"),
            ),
            (
                json!({"kind": "frame", "format": "parquet"}),
                Some("application/vnd.apache.parquet"),
            ),
            (json!({"kind": "numpy"}), Some("application/octet-stream")),
            (json!({"kind": "pickle"}), None),
        ];
        for (record, media) in cases {
            assert_eq!(content_type(&record), media, "{record}");
        }
    }
}
