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
}

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<Service>,
    pub auth: Auth,
}

const MAX_BODY: usize = 1024 * 1024;
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

async fn create_run(
    State(state): State<AppState>,
    Path(pipeline): Path<String>,
    Query(query): Query<WaitQuery>,
    headers: HeaderMap,
    body: Option<Json<RunRequest>>,
) -> ApiResult<Response> {
    authorize(&state, &headers, Access::Write)?;
    let request = body.map(|Json(r)| r).unwrap_or_default();
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
    let text = tokio::fs::read_to_string(&path).await.unwrap_or_default();
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
async fn models(State(state): State<AppState>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    authorize(&state, &headers, Access::Read)?;
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
        }));
    }
    Ok(Json(json!({"models": models})))
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
