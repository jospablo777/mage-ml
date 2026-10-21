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

use mage_service_core::io_config;
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
    /// A condition returned False, so the block did not run; its result is empty.
    skipped: bool,
}

/// What a block attempt did: ran and gave outputs, or was skipped by a condition.
enum Ran {
    Outputs(Vec<Value>),
    Skipped(String),
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
    match service.ledger.finish_run(&run_id, status, error.as_deref()) {
        Ok(true) => {}
        // Another instance failed it after this one's lease expired; its record stands.
        Ok(false) => log.warn(
            None,
            &format!(
                "Run {run_id} finished, but the run history no longer gives it to this \
                 instance (its lease expired); its recorded status stands."
            ),
        ),
        Err(e) => crate::log::error(&format!("run {run_id}: {e}")),
    }
    match &error {
        None => log.info(None, &format!("Run {run_id} completed.")),
        Some(message) => log.error(
            None,
            &format!("Run {run_id} {}: {message}", status.as_str()),
        ),
    }
    if status == RunStatus::Failed
        && let (Some(alerts), Some(message)) = (&service.alerts, &error)
    {
        let failed = service
            .ledger
            .run(&run_id)
            .ok()
            .flatten()
            .and_then(|detail| {
                detail
                    .blocks
                    .into_iter()
                    .find(|b| b.status == BlockStatus::Failed)
                    .map(|b| (b.block, b.error))
            });
        let (block, detail) = match failed {
            Some((block, Some(detail))) => (Some(block), detail),
            Some((block, None)) => (Some(block), message.clone()),
            None => (None, message.clone()),
        };
        alerts.run_failed(&pipeline.uuid, &run_id, block.as_deref(), &detail);
    }
    service.run_finished(&pipeline.uuid, &run_id, status);
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
    let chains = block_chains(pipeline, &order);
    let chained: HashSet<String> = chains
        .values()
        .flat_map(|(_, chain)| chain.iter().skip(1).map(|b| b.uuid.clone()))
        .collect();
    let mut outputs: HashMap<String, Vec<Value>> = HashMap::new();
    // Blocks a condition skipped; the blocks downstream of them are skipped too, as in Mage.
    let mut skipped: HashSet<String> = HashSet::new();
    let mut started: HashSet<String> = HashSet::new();
    let mut tasks: JoinSet<BlockOutcome> = JoinSet::new();
    let mut failure: Option<(String, String)> = None;

    loop {
        if failure.is_none() && !cancel.is_cancelled() {
            for block in &order {
                if started.contains(&block.uuid)
                    || chained.contains(&block.uuid)
                    || !block
                        .upstream
                        .iter()
                        .all(|u| outputs.contains_key(u) || skipped.contains(u))
                {
                    continue;
                }
                started.insert(block.uuid.clone());
                if let Some(upstream) = block.upstream.iter().find(|u| skipped.contains(*u)) {
                    let members = chains
                        .get(&block.uuid)
                        .map(|(_, chain)| chain.clone())
                        .unwrap_or_else(|| vec![block.clone()]);
                    // Each block names its own upstream block: a chain's later blocks follow
                    // the block before them.
                    let mut previous = upstream.clone();
                    for member in &members {
                        let reason =
                            format!("Upstream block {previous} was skipped by a condition.");
                        previous = member.uuid.clone();
                        started.insert(member.uuid.clone());
                        skipped.insert(member.uuid.clone());
                        let _ = service.ledger.finish_block(
                            run_id,
                            &member.uuid,
                            BlockStatus::ConditionFailed,
                            Some(&reason),
                            &[],
                        );
                        log.info(Some(&member.uuid), &format!("Skipped: {reason}"));
                    }
                    continue;
                }
                let inputs: Vec<Vec<Value>> =
                    block.upstream.iter().map(|u| outputs[u].clone()).collect();
                if let Some((kind, chain)) = chains.get(&block.uuid) {
                    for member in chain {
                        started.insert(member.uuid.clone());
                    }
                    let task = ChainTask {
                        service: service.clone(),
                        kind: *kind,
                        blocks: chain.clone(),
                        pipeline_uuid: pipeline.uuid.clone(),
                        run_id: run_id.to_string(),
                        output_dir: run_dir
                            .join("blocks")
                            .join(format!("{}__chain", sanitize(&block.uuid))),
                        log: log.clone(),
                        kwargs: kwargs.clone(),
                        inputs,
                        cancel: cancel.clone(),
                    };
                    tasks.spawn(task.run());
                    continue;
                }
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
            Ok(_) if outcome.skipped => {
                skipped.insert(outcome.block);
            }
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

/// How a chain of blocks runs as one unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainKind {
    /// In the service's Rust binary: each block's query extends the previous block's lazy
    /// plan, and Polars optimizes the chain as one.
    Rust,
    /// In one Python worker request: each block takes the previous block's outputs in
    /// memory, and consecutive R blocks share one R session.
    Worker,
}

fn chain_kind(block: &Block) -> Option<ChainKind> {
    match block.language {
        Language::Rust if block.entry.is_some() => Some(ChainKind::Rust),
        Language::Rust => None,
        Language::Python | Language::R | Language::Sql => Some(ChainKind::Worker),
        // Window blocks are stream blocks; the manifest check keeps them out of runs.
        Language::Window => None,
    }
}

/// Chains of blocks that run as one unit, keyed by their first block: blocks of one kind
/// where each is the only downstream block of the one before and has no other upstream
/// block, so nothing else needs the outputs in between. Only the last block's outputs
/// are written. MAGE_SERVICE_CHAINS=off runs every block on its own;
/// MAGE_SERVICE_RUST_CHAINS=off does that for Rust blocks only.
fn block_chains(pipeline: &Pipeline, order: &[Block]) -> HashMap<String, (ChainKind, Vec<Block>)> {
    let mut chains = HashMap::new();
    let off = |name: &str| std::env::var(name).is_ok_and(|v| v == "off");
    if off("MAGE_SERVICE_CHAINS") {
        return chains;
    }
    let rust_off = off("MAGE_SERVICE_RUST_CHAINS");
    let downstream = pipeline.downstream();
    let by_uuid: HashMap<&str, &Block> = order.iter().map(|b| (b.uuid.as_str(), b)).collect();
    let mut assigned: HashSet<String> = HashSet::new();
    for block in order {
        // A block with conditions runs on its own, so its conditions see its inputs; one with
        // a contract or monitors too, so its output is written and checked.
        let Some(kind) = chain_kind(block).filter(|_| {
            block.conditions.is_empty() && block.contract.is_none() && block.monitors.is_empty()
        }) else {
            continue;
        };
        if assigned.contains(&block.uuid) || (kind == ChainKind::Rust && rust_off) {
            continue;
        }
        let mut chain = vec![block.clone()];
        // One Rust process has one MAGE_IO_CONFIG: blocks with different profiles split.
        let mut profile = block.io_config.clone();
        loop {
            let last = chain.last().expect("a chain has a block");
            let next = match downstream.get(last.uuid.as_str()).map(Vec::as_slice) {
                Some([only]) => by_uuid.get(only).copied(),
                _ => None,
            };
            let Some(next) = next else { break };
            let compatible = kind != ChainKind::Rust
                || match (&profile, &next.io_config) {
                    (Some(a), Some(b)) => a == b,
                    _ => true,
                };
            if chain_kind(next) != Some(kind)
                || next.upstream.len() != 1
                || !next.conditions.is_empty()
                || next.contract.is_some()
                || !next.monitors.is_empty()
                || !compatible
            {
                break;
            }
            if profile.is_none() {
                profile = next.io_config.clone();
            }
            chain.push(next.clone());
        }
        if chain.len() > 1 {
            assigned.extend(chain.iter().map(|b| b.uuid.clone()));
            chains.insert(block.uuid.clone(), (kind, chain));
        }
    }
    chains
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
                Ok(Ran::Skipped(reason)) => {
                    let _ = self.service.ledger.finish_block(
                        &self.run_id,
                        &self.block.uuid,
                        BlockStatus::ConditionFailed,
                        Some(&reason),
                        &[],
                    );
                    self.log
                        .info(Some(&self.block.uuid), &format!("Skipped: {reason}"));
                    return BlockOutcome {
                        block: self.block.uuid.clone(),
                        result: Ok(Vec::new()),
                        cancelled: false,
                        skipped: true,
                    };
                }
                Ok(Ran::Outputs(records)) => {
                    let checked = match crate::contract::check_blocking(
                        &self.block,
                        &records,
                        &self.output_dir,
                        &self.log,
                    )
                    .await
                    {
                        Ok(()) => {
                            crate::monitor::check_blocking(
                                self.service.clone(),
                                &self.pipeline_uuid,
                                &self.block,
                                &records,
                                &self.output_dir,
                                &self.log,
                                &self.run_id,
                            )
                            .await
                        }
                        failed => failed,
                    };
                    if let Err(message) = checked {
                        // The same data breaks the same rules on every attempt.
                        self.service.observe_block(
                            &self.pipeline_uuid,
                            &self.block.uuid,
                            started.elapsed(),
                            false,
                        );
                        self.log.error(Some(&self.block.uuid), &message);
                        last_error = message;
                        break;
                    }
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
                        skipped: false,
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
            skipped: false,
        }
    }

    async fn run_once(&self) -> Result<Ran, BlockFailure> {
        let timeout = self.block.timeout_seconds.map(Duration::from_secs);
        let pool = || {
            self.service.python.as_ref().ok_or_else(|| {
                BlockFailure::fatal(
                    "this image has no Python runtime for Python, R and SQL blocks or for \
                     conditional blocks",
                )
            })
        };
        match self.block.language {
            // The Python worker runs R and SQL blocks too, and checks their conditions in
            // the same request.
            Language::Python | Language::R | Language::Sql => {
                self.run_python(pool()?, timeout, false).await
            }
            Language::Window => Err(BlockFailure::fatal(
                "a window block runs in a stream, not in a pipeline run",
            )),
            Language::Rust => {
                if !self.block.conditions.is_empty()
                    && let Ran::Skipped(reason) = self.run_python(pool()?, timeout, true).await?
                {
                    return Ok(Ran::Skipped(reason));
                }
                self.run_rust(timeout).await.map(Ran::Outputs)
            }
        }
    }

    /// Runs the block in the Python worker, or with conditions_only only checks its
    /// conditions.
    async fn run_python(
        &self,
        pool: &Arc<PythonPool>,
        timeout: Option<Duration>,
        conditions_only: bool,
    ) -> Result<Ran, BlockFailure> {
        let inputs: Vec<Value> = self
            .inputs
            .iter()
            .map(|records| python_input(records))
            .collect();
        let request = json!({
            "block": worker_block(&self.service, &self.block, &self.pipeline_uuid),
            "inputs": inputs,
            "kwargs": *self.kwargs,
            "output_dir": self.output_dir,
            "run_tests": true,
            "conditions_only": conditions_only,
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
            if let Some(reason) = reply.get("skipped").and_then(Value::as_str) {
                return Ok(Ran::Skipped(reason.to_string()));
            }
            return Ok(Ran::Outputs(
                reply
                    .get("outputs")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            ));
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
        let inputs = rust_inputs(&self.block, &self.inputs)?;
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
        let env: Vec<(String, String)> = self
            .block
            .entry
            .iter()
            .map(|entry| ("MAGE_BLOCK".to_string(), entry.clone()))
            .collect();
        let status = match (&self.service.residents, &self.block.entry) {
            (Some(residents), Some(entry)) => {
                residents
                    .run(
                        &self.service,
                        &binary,
                        &self.output_dir,
                        Some(entry),
                        rendered_io_config(&self.service, self.block.io_config.as_ref()),
                        &self.log,
                        &self.block.uuid,
                        timeout,
                        &self.cancel,
                    )
                    .await?
            }
            _ => {
                run_rust_binary(
                    &self.service,
                    &binary,
                    &self.output_dir,
                    env,
                    self.block.io_config.as_ref(),
                    &self.log,
                    &self.block.uuid,
                    timeout,
                    &self.cancel,
                )
                .await?
            }
        };
        let result_path = self.output_dir.join("result.json");
        if status.success() && result_path.exists() {
            let result = read_result(&result_path)?;
            test_failures(&result)?;
            return Ok(output_records(&result, &self.output_dir));
        }
        Err(rust_failure(&self.output_dir, status).0)
    }
}

/// One block's part of a chain's result.
#[derive(Debug, Clone, Default)]
struct ChainBlockResult {
    seconds: f64,
    /// Its output went to the next block as a plan (Rust chains).
    planned: bool,
    /// Its outputs, when the chain keeps them (MAGE_SERVICE_KEEP_OUTPUTS).
    outputs: Vec<Value>,
}

/// Whether a chain writes every block's outputs, for debugging; by default only the last
/// block's are written (MAGE_SERVICE_KEEP_OUTPUTS=on).
fn keep_outputs() -> bool {
    std::env::var("MAGE_SERVICE_KEEP_OUTPUTS").is_ok_and(|v| v == "on" || v == "1")
}

/// Each block's kept outputs in a chain's result (`blocks[].outputs`), with absolute
/// paths.
fn kept_outputs(result: &Value, output_dir: &Path) -> Vec<Vec<Value>> {
    result
        .get("blocks")
        .and_then(Value::as_array)
        .map(|blocks| {
            blocks
                .iter()
                .map(|b| output_records(&json!({"outputs": b.get("outputs")}), output_dir))
                .collect()
        })
        .unwrap_or_default()
}

/// A block as the Python worker takes it, with the files of its conditions.
fn worker_block(service: &Service, block: &Block, pipeline_uuid: &str) -> Value {
    let conditionals = service
        .manifest
        .pipeline(pipeline_uuid)
        .map(|p| p.conditionals.as_slice())
        .unwrap_or_default();
    let conditions: Vec<Value> = block
        .conditions
        .iter()
        .filter_map(|uuid| conditionals.iter().find(|c| &c.uuid == uuid))
        .map(|c| json!({"uuid": c.uuid, "file": service.project_dir().join(&c.file)}))
        .collect();
    json!({
        "conditions": conditions,
        "uuid": block.uuid,
        "type": block.block_type.as_str(),
        "file": service.project_dir().join(&block.file),
        "configuration": block.configuration,
        "language": match block.language {
            Language::R => "r",
            Language::Sql => "sql",
            _ => "python",
        },
        "pipeline_uuid": pipeline_uuid,
        "sql": block.sql,
        "sha256": block.sha256,
    })
}

/// A Rust block's inputs for its job.json: the upstream outputs' files.
fn rust_inputs(block: &Block, inputs: &[Vec<Value>]) -> Result<Vec<Value>, BlockFailure> {
    let mut items = Vec::with_capacity(inputs.len());
    for (position, records) in inputs.iter().enumerate() {
        items.push(rust_input(records).map_err(|reason| {
            BlockFailure::fatal(format!(
                "upstream {} of this Rust block {reason}",
                block
                    .upstream
                    .get(position)
                    .map(String::as_str)
                    .unwrap_or("?")
            ))
        })?);
    }
    Ok(items)
}

/// Runs a Rust binary on its job directory and returns its exit status. Its output goes
/// to the run's log under `tag`; a chain of blocks prints a marker before each block, and
/// the lines after it go under that block.
#[allow(clippy::too_many_arguments)]
async fn run_rust_binary(
    service: &Service,
    binary: &Path,
    job_dir: &Path,
    env: Vec<(String, String)>,
    io_config_profile: Option<&Value>,
    log: &RunLog,
    tag: &str,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
) -> Result<std::process::ExitStatus, BlockFailure> {
    let mut command = tokio::process::Command::new(binary);
    command
        .arg(job_dir)
        .current_dir(service.project_dir())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        // Only the block environment: the service's own credentials are not inherited.
        .env_clear()
        .envs(service.extra_env.iter().cloned())
        .envs(env);
    if let Some(profile) = rendered_io_config(service, io_config_profile) {
        command.env("MAGE_IO_CONFIG", profile);
    }
    let mut child = command
        .spawn()
        .map_err(|e| BlockFailure::fatal(format!("could not start {}: {e}", binary.display())))?;
    let pid = child.id().unwrap_or(0);
    let current = Arc::new(std::sync::Mutex::new(tag.to_string()));
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
            let log = log.clone();
            let current = current.clone();
            readers.push(tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut lines = tokio::io::BufReader::new(stream).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(block) = line.strip_prefix(BLOCK_MARKER) {
                        *current.lock().unwrap_or_else(|p| p.into_inner()) = block.to_string();
                        continue;
                    }
                    let block = current.lock().unwrap_or_else(|p| p.into_inner()).clone();
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
        _ = cancel.cancelled() => {
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
    Ok(status)
}

/// A block's connection profile with its variables filled in, as MAGE_IO_CONFIG holds it.
fn rendered_io_config(service: &Service, profile: Option<&Value>) -> Option<String> {
    let extra = &service.extra_env;
    let lookup = |name: &str| {
        extra
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .or_else(|| std::env::var(name).ok())
    };
    profile.map(|profile| io_config::render(profile, &lookup).to_string())
}

/// The marker a chain prints before each block (mage::runtime::BLOCK_MARKER).
const BLOCK_MARKER: &str = "\u{1e}mage-block ";

fn read_result(path: &Path) -> Result<Value, BlockFailure> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| BlockFailure::retryable(format!("could not read the result: {e}")))?;
    serde_json::from_str(&text).map_err(|e| BlockFailure::retryable(format!("invalid result: {e}")))
}

fn test_failures(result: &Value) -> Result<(), BlockFailure> {
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
    if failed.is_empty() {
        return Ok(());
    }
    Err(BlockFailure::retryable(format!(
        "{} of {} tests failed: {}",
        failed.len(),
        tests.len(),
        failed.join("; ")
    )))
}

/// The output records of a result, with paths made absolute.
fn output_records(result: &Value, output_dir: &Path) -> Vec<Value> {
    result
        .get("outputs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|mut record| {
            if let Some(path) = record.get("path").and_then(Value::as_str) {
                record["path"] = json!(output_dir.join(path));
            }
            record
        })
        .collect()
}

/// Why a Rust binary failed, and the block that failed when it ran a chain.
fn rust_failure(
    job_dir: &Path,
    status: std::process::ExitStatus,
) -> (BlockFailure, Option<String>) {
    let error = std::fs::read_to_string(job_dir.join("error.json"))
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok());
    let block = error
        .as_ref()
        .and_then(|e| e.get("block_uuid"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let message = error
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
        .unwrap_or_else(|| format!("the block exited{}", crate::python::describe_exit(status)));
    (BlockFailure::retryable(message), block)
}

/// A chain of Rust blocks (rust_chains) run as one process. Every block keeps its block
/// run: the chain's attempts start them all, and each finishes with its own time.
struct ChainTask {
    service: Arc<Service>,
    kind: ChainKind,
    blocks: Vec<Block>,
    pipeline_uuid: String,
    run_id: String,
    output_dir: PathBuf,
    log: RunLog,
    kwargs: Arc<Map<String, Value>>,
    inputs: Vec<Vec<Value>>,
    cancel: CancellationToken,
}

impl ChainTask {
    async fn run(self) -> BlockOutcome {
        let head = &self.blocks[0];
        let tail = &self.blocks[self.blocks.len() - 1];
        // A failed block retries with its own settings; the chain runs again from its
        // first block, since the outputs in between are not kept.
        let attempts: u32 = self.blocks.iter().map(|b| b.retry.retries + 1).sum();
        let mut failures: HashMap<String, u32> = HashMap::new();
        let timeout = self
            .blocks
            .iter()
            .map(|b| b.timeout_seconds)
            .sum::<Option<u64>>()
            .map(Duration::from_secs);
        self.log.info(
            Some(&head.uuid),
            &format!(
                "Runs with {} as one {} of {} blocks.",
                self.blocks[1..]
                    .iter()
                    .map(|b| b.uuid.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                match self.kind {
                    ChainKind::Rust => "compiled Rust chain",
                    ChainKind::Worker => "chain in one process",
                },
                self.blocks.len()
            ),
        );
        let mut failure: (String, Option<String>) = (String::new(), None);
        for attempt in 1..=attempts {
            if self.cancel.is_cancelled() {
                break;
            }
            for block in &self.blocks {
                let _ = self
                    .service
                    .ledger
                    .start_block(&self.run_id, &block.uuid, attempt);
            }
            if attempt > 1 {
                self.log.info(
                    Some(&head.uuid),
                    &format!("Attempt {attempt} of {attempts} of the chain."),
                );
            }
            let _ = std::fs::remove_dir_all(&self.output_dir);
            if let Err(error) = std::fs::create_dir_all(&self.output_dir) {
                failure = (
                    format!("could not create {}: {error}", self.output_dir.display()),
                    None,
                );
                break;
            }
            let chain_started = crate::ledger::now();
            match self.run_once(timeout).await {
                Ok((records, seconds)) => {
                    let chain_finished = crate::ledger::now();
                    // The spans follow one another with the times the chain measured,
                    // laid back from the chain's end; the first block starts with the
                    // chain, so it carries the worker's start-up and the input reads, and
                    // the spans add up to the chain's time.
                    let mut starts = vec![chain_started; self.blocks.len()];
                    let mut boundary = chain_finished;
                    for position in (1..self.blocks.len()).rev() {
                        let part = seconds.get(position).map_or(0.0, |p| p.seconds);
                        boundary = (boundary - chrono::Duration::microseconds((part * 1e6) as i64))
                            .max(chain_started);
                        starts[position] = boundary;
                    }
                    for (position, block) in self.blocks.iter().enumerate() {
                        let last = position + 1 == self.blocks.len();
                        let part = seconds.get(position).cloned().unwrap_or_default();
                        let (elapsed, planned) = (part.seconds, part.planned);
                        // The block whose computation ran this block's query too.
                        let computed_in = self.blocks[position..]
                            .iter()
                            .zip(&seconds[position.min(seconds.len())..])
                            .find(|(_, part)| !part.planned)
                            .map(|(b, _)| b.uuid.as_str())
                            .unwrap_or(tail.uuid.as_str());
                        let block_finished = if last {
                            chain_finished
                        } else {
                            starts[position + 1]
                        };
                        let _ = self.service.ledger.finish_chained_block(
                            &self.run_id,
                            &block.uuid,
                            starts[position],
                            block_finished,
                            if last { &records } else { &part.outputs },
                        );
                        let message = if planned {
                            format!(
                                "Planned in the compiled chain; its work ran with {computed_in}."
                            )
                        } else {
                            chain_block_message(
                                elapsed,
                                position,
                                (block_finished - starts[position]).num_microseconds(),
                            )
                        };
                        self.log.info(Some(&block.uuid), &message);
                        self.service.observe_block(
                            &self.pipeline_uuid,
                            &block.uuid,
                            Duration::from_secs_f64(elapsed),
                            true,
                        );
                    }
                    return BlockOutcome {
                        block: tail.uuid.clone(),
                        result: Ok(records),
                        cancelled: false,
                        skipped: false,
                    };
                }
                Err((
                    BlockFailure {
                        message,
                        details,
                        retryable,
                    },
                    block,
                )) => {
                    let failed = block.clone().unwrap_or_else(|| head.uuid.clone());
                    self.service
                        .observe_block(&self.pipeline_uuid, &failed, Duration::ZERO, false);
                    let mut text = message.clone();
                    if !details.is_empty() {
                        text.push('\n');
                        text.push_str(details.trim_end());
                    }
                    self.log.error(Some(&failed), &text);
                    let settings = self
                        .blocks
                        .iter()
                        .find(|b| b.uuid == failed)
                        .unwrap_or(head)
                        .retry
                        .clone();
                    let failed_times = failures.entry(failed.clone()).or_insert(0);
                    *failed_times += 1;
                    let failed_times = *failed_times;
                    failure = (message, block);
                    if !retryable
                        || failed_times > settings.retries
                        || attempt == attempts
                        || self.cancel.is_cancelled()
                    {
                        break;
                    }
                    let delay = settings.delay(failed_times + 1);
                    self.log.info(
                        Some(&failed),
                        &format!("Retrying the chain in {delay:.1} s."),
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs_f64(delay)) => {}
                        _ = self.cancel.cancelled() => break,
                    }
                }
            }
        }
        let cancelled = self.cancel.is_cancelled();
        let (message, block) = failure;
        if cancelled {
            for block in &self.blocks {
                let _ = self.service.ledger.finish_block(
                    &self.run_id,
                    &block.uuid,
                    BlockStatus::Cancelled,
                    Some("the run was cancelled"),
                    &[],
                );
            }
            return BlockOutcome {
                block: head.uuid.clone(),
                result: Err("the run was cancelled".into()),
                cancelled: true,
                skipped: false,
            };
        }
        // The blocks before the one that failed returned; the run marks the ones after it
        // upstream_failed.
        let failed = block.unwrap_or_else(|| head.uuid.clone());
        for block in &self.blocks {
            if block.uuid == failed {
                let _ = self.service.ledger.finish_block(
                    &self.run_id,
                    &block.uuid,
                    BlockStatus::Failed,
                    Some(&message),
                    &[],
                );
                break;
            }
            let _ = self.service.ledger.finish_block(
                &self.run_id,
                &block.uuid,
                BlockStatus::Completed,
                None,
                &[],
            );
        }
        BlockOutcome {
            block: failed,
            result: Err(message),
            cancelled: false,
            skipped: false,
        }
    }

    async fn run_once(
        &self,
        timeout: Option<Duration>,
    ) -> Result<(Vec<Value>, Vec<ChainBlockResult>), (BlockFailure, Option<String>)> {
        match self.kind {
            ChainKind::Rust => self.run_rust_chain(timeout).await,
            ChainKind::Worker => self.run_worker_chain(timeout).await,
        }
    }

    async fn run_worker_chain(
        &self,
        timeout: Option<Duration>,
    ) -> Result<(Vec<Value>, Vec<ChainBlockResult>), (BlockFailure, Option<String>)> {
        let head = &self.blocks[0];
        let pool = self.service.python.as_ref().ok_or_else(|| {
            (
                BlockFailure::fatal(
                    "this image has no Python runtime for Python, R and SQL blocks",
                ),
                None,
            )
        })?;
        let chain: Vec<Value> = self
            .blocks
            .iter()
            .map(|b| worker_block(&self.service, b, &self.pipeline_uuid))
            .collect();
        let request = json!({
            "block": chain[0],
            "chain": chain,
            "keep_outputs": keep_outputs(),
            "inputs": self.inputs.iter().map(|r| python_input(r)).collect::<Vec<_>>(),
            "kwargs": *self.kwargs,
            "output_dir": self.output_dir,
            "run_tests": true,
        });
        let reply = pool
            .run(request, self.log.clone(), &head.uuid, timeout, &self.cancel)
            .await
            .map_err(|error| match error {
                WorkerError::Cancelled => (BlockFailure::fatal("the run was cancelled"), None),
                other => (BlockFailure::retryable(other.to_string()), None),
            })?;
        if reply.get("ok").and_then(Value::as_bool) == Some(true) {
            let seconds = reply
                .get("blocks")
                .and_then(Value::as_array)
                .map(|blocks| {
                    blocks
                        .iter()
                        .map(|b| ChainBlockResult {
                            seconds: b.get("seconds").and_then(Value::as_f64).unwrap_or(0.0),
                            planned: false,
                            outputs: b
                                .get("outputs")
                                .and_then(Value::as_array)
                                .cloned()
                                .unwrap_or_default(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let outputs = reply
                .get("outputs")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            return Ok((outputs, seconds));
        }
        let error = reply.get("error").cloned().unwrap_or(Value::Null);
        let phase = error.get("phase").and_then(Value::as_str).unwrap_or("run");
        let block = error
            .get("block_uuid")
            .and_then(Value::as_str)
            .map(str::to_string);
        Err((
            BlockFailure {
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("the block failed")
                    .to_string(),
                details: error
                    .get("details")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                retryable: phase != "load",
            },
            block,
        ))
    }

    async fn run_rust_chain(
        &self,
        timeout: Option<Duration>,
    ) -> Result<(Vec<Value>, Vec<ChainBlockResult>), (BlockFailure, Option<String>)> {
        let head = &self.blocks[0];
        let binary = head
            .binary
            .as_ref()
            .map(|b| self.service.service_dir.join(b))
            .ok_or_else(|| (BlockFailure::fatal("this Rust block has no binary"), None))?;
        let inputs = rust_inputs(head, &self.inputs).map_err(|f| (f, None))?;
        let steps: Vec<Value> = self
            .blocks
            .iter()
            .map(|b| {
                json!({
                    "block_uuid": b.uuid,
                    "block_type": b.block_type.as_str(),
                    "entry": b.entry,
                    "keep_output": keep_outputs(),
                })
            })
            .collect();
        let job = json!({
            "api_version": 1,
            "block_uuid": head.uuid,
            "block_type": head.block_type.as_str(),
            "pipeline_uuid": self.pipeline_uuid,
            "inputs": inputs,
            "variables": *self.kwargs,
            "output_dir": ".",
            "chain": steps,
        });
        std::fs::write(self.output_dir.join("job.json"), job.to_string()).map_err(|e| {
            (
                BlockFailure::retryable(format!("could not write the job: {e}")),
                None,
            )
        })?;
        let profile = self.blocks.iter().find_map(|b| b.io_config.as_ref());
        let status = match &self.service.residents {
            Some(residents) => {
                residents
                    .run(
                        &self.service,
                        &binary,
                        &self.output_dir,
                        None,
                        rendered_io_config(&self.service, profile),
                        &self.log,
                        &head.uuid,
                        timeout,
                        &self.cancel,
                    )
                    .await
            }
            None => {
                run_rust_binary(
                    &self.service,
                    &binary,
                    &self.output_dir,
                    vec![("MAGE_CHAIN".to_string(), "1".to_string())],
                    profile,
                    &self.log,
                    &head.uuid,
                    timeout,
                    &self.cancel,
                )
                .await
            }
        }
        .map_err(|f| (f, None))?;
        let result_path = self.output_dir.join("result.json");
        if status.success() && result_path.exists() {
            let result = read_result(&result_path).map_err(|f| (f, None))?;
            test_failures(&result).map_err(|f| (f, None))?;
            let kept = kept_outputs(&result, &self.output_dir);
            let seconds = result
                .get("blocks")
                .and_then(Value::as_array)
                .map(|blocks| {
                    blocks
                        .iter()
                        .zip(kept.into_iter().chain(std::iter::repeat(Vec::new())))
                        .map(|(b, outputs)| ChainBlockResult {
                            seconds: b.get("seconds").and_then(Value::as_f64).unwrap_or(0.0),
                            planned: b.get("planned").and_then(Value::as_bool).unwrap_or(false),
                            outputs,
                        })
                        .collect()
                })
                .unwrap_or_default();
            return Ok((output_records(&result, &self.output_dir), seconds));
        }
        Err(rust_failure(&self.output_dir, status))
    }
}

pub(crate) struct BlockFailure {
    message: String,
    details: String,
    retryable: bool,
}

impl BlockFailure {
    pub(crate) fn fatal(message: impl Into<String>) -> BlockFailure {
        BlockFailure {
            message: message.into(),
            details: String::new(),
            retryable: false,
        }
    }

    pub(crate) fn retryable(message: impl Into<String>) -> BlockFailure {
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

/// The log line of a completed block in a chain. The first block's span also holds the
/// chain's start-up (worker start and input reads); the line says so, so it adds up with
/// the run summary.
fn chain_block_message(elapsed: f64, position: usize, span_micros: Option<i64>) -> String {
    let span = span_micros.unwrap_or(0) as f64 / 1e6;
    let start_up = span - elapsed;
    if position == 0 && start_up >= 0.01 {
        format!("Completed in {elapsed:.2} s in the chain, after {start_up:.2} s starting it.")
    } else {
        format!("Completed in {elapsed:.2} s in the chain.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_chained_block_names_the_start_up_it_carries() {
        assert_eq!(
            chain_block_message(0.03, 0, Some(503_000)),
            "Completed in 0.03 s in the chain, after 0.47 s starting it."
        );
        assert_eq!(
            chain_block_message(0.03, 0, Some(35_000)),
            "Completed in 0.03 s in the chain."
        );
        assert_eq!(
            chain_block_message(0.01, 2, Some(400_000)),
            "Completed in 0.01 s in the chain."
        );
    }

    fn pipeline(blocks: Value) -> Pipeline {
        serde_json::from_value(
            json!({"uuid": "p", "name": "p", "max_concurrent_runs": 1, "blocks": blocks}),
        )
        .unwrap()
    }

    fn rust(uuid: &str, upstream: &[&str]) -> Value {
        json!({"uuid": uuid, "type": "transformer", "language": "rust", "file": "x.rs",
               "sha256": "0", "upstream": upstream, "binary": "bin/mage-blocks",
               "entry": format!("block_{uuid}")})
    }

    fn chains_of(pipeline: &Pipeline) -> Vec<Vec<String>> {
        let order: Vec<Block> = pipeline
            .topological_order()
            .unwrap()
            .into_iter()
            .cloned()
            .collect();
        let mut chains: Vec<Vec<String>> = block_chains(pipeline, &order)
            .into_values()
            .map(|(_, c)| c.into_iter().map(|b| b.uuid).collect())
            .collect();
        chains.sort();
        chains
    }

    #[test]
    fn rust_chains_are_linear_runs_of_bundled_rust_blocks() {
        let python = json!({"uuid": "py", "type": "transformer", "language": "python",
                            "file": "py.py", "sha256": "0", "upstream": ["c"]});
        // a -> b -> c, and c feeds both py and f: the chain is a, b, c and ends at c.
        let fanned = pipeline(json!([
            rust("a", &[]),
            rust("b", &["a"]),
            rust("c", &["b"]),
            python,
            rust("f", &["c"]),
        ]));
        assert_eq!(chains_of(&fanned), vec![vec!["a", "b", "c"]]);

        // g has two upstream blocks, so it starts no chain with either.
        let joined = pipeline(json!([
            rust("d", &[]),
            rust("e", &[]),
            rust("g", &["d", "e"]),
            rust("h", &["g"]),
        ]));
        assert_eq!(chains_of(&joined), vec![vec!["g", "h"]]);

        // Different connection profiles cannot share one process.
        let mut x = rust("x", &[]);
        x["io_config"] = json!({"POSTGRES_HOST": "a"});
        let mut y = rust("y", &["x"]);
        y["io_config"] = json!({"POSTGRES_HOST": "b"});
        assert!(chains_of(&pipeline(json!([x, y]))).is_empty());

        // Python, R and SQL blocks chain in the worker; a Rust block between them splits.
        let worker = |uuid: &str, language: &str, upstream: &[&str]| {
            json!({"uuid": uuid, "type": "transformer", "language": language,
                   "file": "b", "sha256": "0", "upstream": upstream})
        };
        let mixed = pipeline(json!([
            worker("w1", "python", &[]),
            worker("w2", "r", &["w1"]),
            worker("w3", "sql", &["w2"]),
            rust("r1", &["w3"]),
            worker("w4", "python", &["r1"]),
        ]));
        assert_eq!(chains_of(&mixed), vec![vec!["w1", "w2", "w3"]]);

        // Blocks without the service's one binary run on their own.
        let mut own = rust("o", &[]);
        own.as_object_mut().unwrap().remove("entry");
        assert!(chains_of(&pipeline(json!([own, rust("p2", &["o"])]))).is_empty());
    }

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
