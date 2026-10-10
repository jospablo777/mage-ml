//! Runs a pipeline run: its blocks in dependency order, independent branches at the same
//! time, each block with its retries and timeout, as Mage runs them.
//!
//! Outputs pass between blocks as files in the run's directory: tables as Arrow IPC, other
//! values as JSON, NumPy or pickle files. When a block fails after its retries, no new block
//! starts; blocks already running finish, the blocks after the failed one are marked
//! upstream_failed and the others cancelled, and the run fails with the block's error.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use mage_service_core::manifest::{Block, Language, Pipeline};
use mage_service_core::protocol::{BlockStatus, RunStatus};
use serde_json::{Map, Value, json};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::log::RunLog;
use crate::python::{PythonPool, WorkerError};
use crate::service::Service;

/// One block's outcome after all its attempts.
struct BlockOutcome {
    block: String,
    result: Result<Vec<Value>, String>,
    /// The block stopped because the run was cancelled, not because it failed.
    cancelled: bool,
}

pub async fn execute_run(service: Arc<Service>, run_id: String, cancel: CancellationToken) {
    let detail = match service.ledger.run(&run_id) {
        Ok(Some(detail)) => detail,
        Ok(None) => return,
        Err(error) => {
            crate::log::error(&format!("run {run_id}: {error}"));
            return;
        }
    };
    let Some(pipeline) = service.manifest.pipeline(&detail.run.pipeline).cloned() else {
        let _ = service.ledger.finish_run(
            &run_id,
            RunStatus::Failed,
            Some("The service has no pipeline with this id."),
        );
        return;
    };
    let run_dir = service.run_dir(&run_id);
    let log = match RunLog::create(&run_dir.join("run.log"), &run_id) {
        Ok(log) => log,
        Err(error) => {
            let message = format!(
                "Could not create the run directory {}: {error}",
                run_dir.display()
            );
            let _ = service
                .ledger
                .finish_run(&run_id, RunStatus::Failed, Some(&message));
            crate::log::error(&message);
            return;
        }
    };
    log.info(
        None,
        &format!(
            "Run {run_id} of {} started ({}).",
            pipeline.uuid, detail.run.source
        ),
    );
    let kwargs = Arc::new(service.block_kwargs(&pipeline, &detail.run, &detail.variables));

    let run_future = run_blocks(
        &service, &pipeline, &run_id, &run_dir, &log, kwargs, &cancel,
    );
    let outcome = match pipeline.run_timeout_seconds {
        Some(seconds) => {
            match tokio::time::timeout(Duration::from_secs(seconds), run_future).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    cancel.cancel();
                    Err((
                        RunStatus::Failed,
                        format!(
                            "The run took longer than its {seconds} s timeout and was stopped."
                        ),
                    ))
                }
            }
        }
        None => run_future.await,
    };
    let (status, error) = match outcome {
        Ok(()) => (RunStatus::Completed, None),
        Err((status, message)) => (status, Some(message)),
    };
    // Blocks left pending when the run stopped did not run.
    if let Ok(Some(current)) = service.ledger.run(&run_id) {
        for block in current
            .blocks
            .iter()
            .filter(|b| matches!(b.status, BlockStatus::Pending | BlockStatus::Running))
        {
            let _ = service.ledger.finish_block(
                &run_id,
                &block.block,
                BlockStatus::Cancelled,
                None,
                &[],
            );
        }
    }
    if let Err(e) = service.ledger.finish_run(&run_id, status, error.as_deref()) {
        crate::log::error(&format!("run {run_id}: {e}"));
    }
    match &error {
        None => log.info(None, &format!("Run {run_id} completed.")),
        Some(message) => log.error(
            None,
            &format!("Run {run_id} {}: {message}", status.as_str()),
        ),
    }
    service.run_finished(&pipeline.uuid);
}

async fn run_blocks(
    service: &Arc<Service>,
    pipeline: &Pipeline,
    run_id: &str,
    run_dir: &Path,
    log: &RunLog,
    kwargs: Arc<Map<String, Value>>,
    cancel: &CancellationToken,
) -> Result<(), (RunStatus, String)> {
    let order: Vec<Block> = pipeline
        .topological_order()
        .ok_or((
            RunStatus::Failed,
            "The pipeline's blocks form a cycle.".to_string(),
        ))?
        .into_iter()
        .cloned()
        .collect();
    let mut outputs: HashMap<String, Vec<Value>> = HashMap::new();
    let mut started: HashSet<String> = HashSet::new();
    let mut tasks: JoinSet<BlockOutcome> = JoinSet::new();
    let mut failure: Option<(String, String)> = None;

    loop {
        if failure.is_none() && !cancel.is_cancelled() {
            for block in &order {
                if started.contains(&block.uuid)
                    || !block.upstream.iter().all(|u| outputs.contains_key(u))
                {
                    continue;
                }
                started.insert(block.uuid.clone());
                let inputs: Vec<Vec<Value>> =
                    block.upstream.iter().map(|u| outputs[u].clone()).collect();
                let task = BlockTask {
                    service: service.clone(),
                    block: block.clone(),
                    pipeline_uuid: pipeline.uuid.clone(),
                    run_id: run_id.to_string(),
                    output_dir: run_dir.join("blocks").join(sanitize(&block.uuid)),
                    log: log.clone(),
                    kwargs: kwargs.clone(),
                    inputs,
                    cancel: cancel.clone(),
                };
                tasks.spawn(task.run());
            }
        }
        let Some(joined) = tasks.join_next().await else {
            break;
        };
        let outcome = match joined {
            Ok(outcome) => outcome,
            Err(error) => {
                failure
                    .get_or_insert(("(executor)".into(), format!("a block task failed: {error}")));
                continue;
            }
        };
        match outcome.result {
            Ok(records) => {
                outputs.insert(outcome.block, records);
            }
            Err(_) if outcome.cancelled => {}
            Err(message) => {
                if failure.is_none() {
                    failure = Some((outcome.block.clone(), message));
                    mark_downstream_failed(service, pipeline, run_id, &outcome.block);
                }
            }
        }
    }

    if let Some((block, message)) = failure {
        return Err((
            RunStatus::Failed,
            format!("Block {block} failed: {message}"),
        ));
    }
    if cancel.is_cancelled() {
        return Err((RunStatus::Cancelled, "The run was cancelled.".into()));
    }
    Ok(())
}

fn mark_downstream_failed(service: &Service, pipeline: &Pipeline, run_id: &str, failed: &str) {
    let downstream = pipeline.downstream();
    let mut stack = vec![failed.to_string()];
    let mut seen = HashSet::new();
    while let Some(uuid) = stack.pop() {
        for child in downstream.get(uuid.as_str()).into_iter().flatten() {
            if seen.insert(child.to_string()) {
                let _ = service.ledger.finish_block(
                    run_id,
                    child,
                    BlockStatus::UpstreamFailed,
                    Some(&format!("Upstream block {failed} failed.")),
                    &[],
                );
                stack.push(child.to_string());
            }
        }
    }
}

pub fn sanitize(uuid: &str) -> String {
    uuid.replace(['/', '\\'], "__")
}

struct BlockTask {
    service: Arc<Service>,
    block: Block,
    pipeline_uuid: String,
    run_id: String,
    output_dir: PathBuf,
    log: RunLog,
    kwargs: Arc<Map<String, Value>>,
    inputs: Vec<Vec<Value>>,
    cancel: CancellationToken,
}

impl BlockTask {
    async fn run(self) -> BlockOutcome {
        let attempts = self.block.retry.retries + 1;
        let mut last_error = String::new();
        for attempt in 1..=attempts {
            if self.cancel.is_cancelled() {
                break;
            }
            let _ = self
                .service
                .ledger
                .start_block(&self.run_id, &self.block.uuid, attempt);
            if attempt > 1 {
                self.log.info(
                    Some(&self.block.uuid),
                    &format!("Attempt {attempt} of {attempts}."),
                );
            }
            let _ = std::fs::remove_dir_all(&self.output_dir);
            if let Err(error) = std::fs::create_dir_all(&self.output_dir) {
                last_error = format!("could not create {}: {error}", self.output_dir.display());
                break;
            }
            let started = std::time::Instant::now();
            match self.run_once().await {
                Ok(records) => {
                    let _ = self.service.ledger.finish_block(
                        &self.run_id,
                        &self.block.uuid,
                        BlockStatus::Completed,
                        None,
                        &records,
                    );
                    self.log.info(
                        Some(&self.block.uuid),
                        &format!(
                            "Completed in {:.2} s with {} output{}.",
                            started.elapsed().as_secs_f64(),
                            records.len(),
                            if records.len() == 1 { "" } else { "s" }
                        ),
                    );
                    self.service.observe_block(
                        &self.pipeline_uuid,
                        &self.block.uuid,
                        started.elapsed(),
                        true,
                    );
                    return BlockOutcome {
                        block: self.block.uuid.clone(),
                        result: Ok(records),
                        cancelled: false,
                    };
                }
                Err(BlockFailure {
                    message,
                    details,
                    retryable,
                }) => {
                    self.service.observe_block(
                        &self.pipeline_uuid,
                        &self.block.uuid,
                        started.elapsed(),
                        false,
                    );
                    let mut text = message.clone();
                    if !details.is_empty() {
                        text.push('\n');
                        text.push_str(details.trim_end());
                    }
                    self.log.error(Some(&self.block.uuid), &text);
                    last_error = message;
                    if !retryable || attempt == attempts || self.cancel.is_cancelled() {
                        break;
                    }
                    let delay = self.block.retry.delay(attempt + 1);
                    self.log.info(
                        Some(&self.block.uuid),
                        &format!("Retrying in {delay:.1} s."),
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs_f64(delay)) => {}
                        _ = self.cancel.cancelled() => break,
                    }
                }
            }
        }
        let cancelled = self.cancel.is_cancelled();
        let (status, error) = if cancelled {
            (BlockStatus::Cancelled, "the run was cancelled".to_string())
        } else {
            (BlockStatus::Failed, last_error)
        };
        let _ = self.service.ledger.finish_block(
            &self.run_id,
            &self.block.uuid,
            status,
            Some(&error),
            &[],
        );
        BlockOutcome {
            block: self.block.uuid.clone(),
            result: Err(error),
            cancelled,
        }
    }

    async fn run_once(&self) -> Result<Vec<Value>, BlockFailure> {
        let timeout = self.block.timeout_seconds.map(Duration::from_secs);
        match self.block.language {
            // The Python worker runs R and SQL blocks too.
            Language::Python | Language::R | Language::Sql => {
                let pool = self.service.python.as_ref().ok_or_else(|| {
                    BlockFailure::fatal(
                        "this image has no Python runtime for Python, R and SQL blocks",
                    )
                })?;
                self.run_python(pool, timeout).await
            }
            Language::Rust => self.run_rust(timeout).await,
        }
    }

    async fn run_python(
        &self,
        pool: &Arc<PythonPool>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Value>, BlockFailure> {
        let inputs: Vec<Value> = self
            .inputs
            .iter()
            .map(|records| python_input(records))
            .collect();
        let request = json!({
            "block": {
                "uuid": self.block.uuid,
                "type": self.block.block_type.as_str(),
                "file": self.service.project_dir().join(&self.block.file),
                "configuration": self.block.configuration,
                "language": match self.block.language {
                    Language::R => "r",
                    Language::Sql => "sql",
                    _ => "python",
                },
                "pipeline_uuid": self.pipeline_uuid,
                "sql": self.block.sql,
            },
            "inputs": inputs,
            "kwargs": *self.kwargs,
            "output_dir": self.output_dir,
            "run_tests": true,
        });
        let reply = pool
            .run(
                request,
                self.log.clone(),
                &self.block.uuid,
                timeout,
                &self.cancel,
            )
            .await
            .map_err(|error| match error {
                WorkerError::Cancelled => BlockFailure::fatal("the run was cancelled"),
                other => BlockFailure::retryable(other.to_string()),
            })?;
        if reply.get("ok").and_then(Value::as_bool) == Some(true) {
            return Ok(reply
                .get("outputs")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default());
        }
        let error = reply.get("error").cloned().unwrap_or(Value::Null);
        let phase = error.get("phase").and_then(Value::as_str).unwrap_or("run");
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("the block failed");
        let details = error
            .get("details")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        // Code that does not load fails the same way on every attempt.
        let retryable = phase != "load";
        Err(BlockFailure {
            message: message.to_string(),
            details,
            retryable,
        })
    }

    async fn run_rust(&self, timeout: Option<Duration>) -> Result<Vec<Value>, BlockFailure> {
        let binary = self
            .block
            .binary
            .as_ref()
            .map(|b| self.service.service_dir.join(b))
            .ok_or_else(|| BlockFailure::fatal("this Rust block has no binary"))?;
        let mut inputs = Vec::with_capacity(self.inputs.len());
        for (position, records) in self.inputs.iter().enumerate() {
            inputs.push(rust_input(records).map_err(|reason| {
                BlockFailure::fatal(format!(
                    "upstream {} of this Rust block {reason}",
                    self.block
                        .upstream
                        .get(position)
                        .map(String::as_str)
                        .unwrap_or("?")
                ))
            })?);
        }
        let job = json!({
            "api_version": 1,
            "block_uuid": self.block.uuid,
            "block_type": self.block.block_type.as_str(),
            "pipeline_uuid": self.pipeline_uuid,
            "inputs": inputs,
            "variables": *self.kwargs,
            "output_dir": ".",
        });
        std::fs::write(self.output_dir.join("job.json"), job.to_string())
            .map_err(|e| BlockFailure::retryable(format!("could not write the job: {e}")))?;

        let mut command = tokio::process::Command::new(&binary);
        command
            .arg(&self.output_dir)
            .current_dir(self.service.project_dir())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true)
            .envs(self.service.extra_env.iter().cloned());
        let mut child = command.spawn().map_err(|e| {
            BlockFailure::fatal(format!("could not start {}: {e}", binary.display()))
        })?;
        let pid = child.id().unwrap_or(0);
        let mut readers = Vec::new();
        for (stream, name) in [
            (
                child
                    .stdout
                    .take()
                    .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Unpin + Send>),
                "stdout",
            ),
            (
                child
                    .stderr
                    .take()
                    .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Unpin + Send>),
                "stderr",
            ),
        ] {
            if let Some(stream) = stream {
                let log = self.log.clone();
                let block = self.block.uuid.clone();
                readers.push(tokio::spawn(async move {
                    use tokio::io::AsyncBufReadExt;
                    let mut lines = tokio::io::BufReader::new(stream).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        log.write("info", Some(&block), Some(name), &line);
                    }
                }));
            }
        }
        let waited = async {
            match timeout {
                Some(limit) => tokio::time::timeout(limit, child.wait())
                    .await
                    .map_err(|_| limit),
                None => Ok(child.wait().await),
            }
        };
        let status = tokio::select! {
            status = waited => status,
            _ = self.cancel.cancelled() => {
                crate::python::kill_group(pid);
                return Err(BlockFailure::fatal("the run was cancelled"));
            }
        };
        let status = match status {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                return Err(BlockFailure::retryable(format!(
                    "waiting for the block failed: {error}"
                )));
            }
            Err(limit) => {
                crate::python::kill_group(pid);
                return Err(BlockFailure::retryable(format!(
                    "the block ran longer than {} seconds and was stopped",
                    limit.as_secs()
                )));
            }
        };
        for reader in readers {
            let _ = reader.await;
        }
        let result_path = self.output_dir.join("result.json");
        if status.success() && result_path.exists() {
            let text = std::fs::read_to_string(&result_path)
                .map_err(|e| BlockFailure::retryable(format!("could not read the result: {e}")))?;
            let result: Value = serde_json::from_str(&text)
                .map_err(|e| BlockFailure::retryable(format!("invalid result: {e}")))?;
            let tests = result
                .get("tests")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let failed: Vec<String> = tests
                .iter()
                .filter(|t| t.get("passed").and_then(Value::as_bool) == Some(false))
                .map(|t| {
                    format!(
                        "{}: {}",
                        t.get("name").and_then(Value::as_str).unwrap_or("test"),
                        t.get("message").and_then(Value::as_str).unwrap_or("failed")
                    )
                })
                .collect();
            if !failed.is_empty() {
                return Err(BlockFailure::retryable(format!(
                    "{} of {} tests failed: {}",
                    failed.len(),
                    tests.len(),
                    failed.join("; ")
                )));
            }
            let records = result
                .get("outputs")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|mut record| {
                    if let Some(path) = record.get("path").and_then(Value::as_str) {
                        record["path"] = json!(self.output_dir.join(path));
                    }
                    record
                })
                .collect();
            return Ok(records);
        }
        let error_path = self.output_dir.join("error.json");
        let message = std::fs::read_to_string(&error_path)
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .map(|error| {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                match error.get("location").and_then(Value::as_str) {
                    Some(location) => format!("{message} (at {location})"),
                    None => message,
                }
            })
            .unwrap_or_else(|| format!("the block exited with {status}"));
        Err(BlockFailure::retryable(message))
    }
}

struct BlockFailure {
    message: String,
    details: String,
    retryable: bool,
}

impl BlockFailure {
    fn fatal(message: impl Into<String>) -> BlockFailure {
        BlockFailure {
            message: message.into(),
            details: String::new(),
            retryable: false,
        }
    }

    fn retryable(message: impl Into<String>) -> BlockFailure {
        BlockFailure {
            message: message.into(),
            details: String::new(),
            retryable: true,
        }
    }
}

/// An upstream's outputs as one Python argument: its single output, or a list of them, as
/// Mage passes them.
fn python_input(records: &[Value]) -> Value {
    if records.len() == 1 {
        records[0].clone()
    } else {
        json!({"kind": "list", "items": records})
    }
}

/// An upstream's outputs as one Rust block input.
fn rust_input(records: &[Value]) -> Result<Value, String> {
    match records {
        [] => Ok(json!({"kind": "empty"})),
        [record] => match record.get("kind").and_then(Value::as_str) {
            Some("frame") => Ok(json!({
                "kind": "frame",
                "format": record.get("format").cloned().unwrap_or(json!("ipc")),
                "path": record["path"],
            })),
            Some("json") => Ok(json!({"kind": "json", "path": record["path"]})),
            Some(other) => Err(format!(
                "returned a {other} value, which Rust blocks cannot read; return a table or \
                 JSON values"
            )),
            None => Err("returned an output without a kind".into()),
        },
        _ => Err(format!(
            "returned {} outputs; a Rust block takes one value per upstream block",
            records.len()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_outputs_become_arguments_as_in_mage() {
        let one = json!({"kind": "json", "path": "/a"});
        assert_eq!(python_input(std::slice::from_ref(&one)), one);
        assert_eq!(python_input(&[]), json!({"kind": "list", "items": []}));
        assert_eq!(
            rust_input(&[
                json!({"kind": "frame", "format": "ipc", "path": "/f", "origin": "pandas"})
            ])
            .unwrap(),
            json!({"kind": "frame", "format": "ipc", "path": "/f"})
        );
        assert_eq!(rust_input(&[]).unwrap(), json!({"kind": "empty"}));
        assert!(
            rust_input(&[json!({"kind": "pickle", "path": "/p"})])
                .unwrap_err()
                .contains("pickle")
        );
    }
}
