//! The state of a running service: its manifest, run history, workers and the dispatcher
//! that starts queued runs within each pipeline's concurrency.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use mage_service_core::manifest::{Manifest, Pipeline, TriggerKind};
use mage_service_core::protocol::{
    API_VERSION, PipelineSnapshot, RunStatus, RunSummary, Snapshot, TriggerSnapshot,
};
use serde_json::{Map, Value, json};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::executor::{execute_run, sanitize};
use crate::ledger::{Ledger, LedgerError, NewRun, timestamp};
use crate::python::PythonPool;
use crate::scheduler::Schedule;

pub struct Config {
    pub service_dir: PathBuf,
    pub data_dir: PathBuf,
    pub schedules_enabled: bool,
    /// Runs of all pipelines at the same time, across the service.
    pub max_total_runs: usize,
    pub retention_days: u32,
    pub environment: String,
    pub extra_env: Vec<(String, String)>,
}

pub struct Service {
    pub manifest: Manifest,
    pub service_dir: PathBuf,
    pub data_dir: PathBuf,
    pub ledger: Ledger,
    pub python: Option<Arc<PythonPool>>,
    pub schedules_enabled: bool,
    pub environment: String,
    pub started_at: chrono::DateTime<Utc>,
    pub retention_days: u32,
    /// Variables read from secret files, for block processes.
    pub extra_env: Vec<(String, String)>,
    max_total_runs: usize,
    wake: Notify,
    shutting_down: AtomicBool,
    running: Mutex<HashMap<String, (String, CancellationToken)>>,
    metrics: Mutex<Metrics>,
    idle: Notify,
}

#[derive(Default)]
pub struct Metrics {
    /// (pipeline, block) -> (successes, failures, seconds, histogram buckets).
    pub blocks: BTreeMap<(String, String), BlockMetrics>,
}

pub const BUCKETS: [f64; 12] = [
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 1800.0,
];

#[derive(Default, Clone)]
pub struct BlockMetrics {
    pub succeeded: u64,
    pub failed: u64,
    pub seconds_sum: f64,
    pub buckets: [u64; BUCKETS.len()],
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("the service has no pipeline {0}")]
    UnknownPipeline(String),
    #[error("the service is shutting down")]
    ShuttingDown,
    #[error(transparent)]
    Ledger(#[from] LedgerError),
}

impl Service {
    pub fn new(
        manifest: Manifest,
        config: Config,
        ledger: Ledger,
        python: Option<Arc<PythonPool>>,
    ) -> Arc<Service> {
        Arc::new(Service {
            manifest,
            service_dir: config.service_dir,
            data_dir: config.data_dir,
            ledger,
            python,
            schedules_enabled: config.schedules_enabled,
            environment: config.environment,
            started_at: Utc::now(),
            retention_days: config.retention_days,
            extra_env: config.extra_env,
            max_total_runs: config.max_total_runs.max(1),
            wake: Notify::new(),
            shutting_down: AtomicBool::new(false),
            running: Mutex::new(HashMap::new()),
            metrics: Mutex::new(Metrics::default()),
            idle: Notify::new(),
        })
    }

    /// The project keeps its name: blocks import shared code as `<project>.utils`.
    pub fn project_dir(&self) -> PathBuf {
        self.service_dir.join(&self.manifest.service.project)
    }

    pub fn run_dir(&self, run_id: &str) -> PathBuf {
        self.data_dir.join("runs").join(sanitize(run_id))
    }

    pub fn shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    pub fn wake(&self) {
        self.wake.notify_one();
    }

    /// Accepts a run: committed to the run history before this returns.
    pub fn submit(
        &self,
        pipeline_uuid: &str,
        source: &str,
        variables: &Map<String, Value>,
        idempotency_key: Option<&str>,
    ) -> Result<(String, bool), AdmissionError> {
        if self.shutting_down() {
            return Err(AdmissionError::ShuttingDown);
        }
        let pipeline = self
            .manifest
            .pipeline(pipeline_uuid)
            .ok_or_else(|| AdmissionError::UnknownPipeline(pipeline_uuid.to_string()))?;
        let blocks: Vec<String> = pipeline.blocks.iter().map(|b| b.uuid.clone()).collect();
        let result = self.ledger.create_run(NewRun {
            pipeline: pipeline_uuid,
            source,
            variables,
            idempotency_key,
            blocks: &blocks,
        })?;
        self.wake();
        Ok(result)
    }

    /// Cancels a queued or running run; false when it already finished or does not exist.
    pub fn cancel(&self, run_id: &str) -> Result<bool, LedgerError> {
        if let Some((_, token)) = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(run_id)
        {
            token.cancel();
            return Ok(true);
        }
        match self.ledger.run(run_id)? {
            Some(detail) if detail.run.status == RunStatus::Queued => {
                self.ledger.finish_run(
                    run_id,
                    RunStatus::Cancelled,
                    Some("Cancelled before it started."),
                )?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub fn run_finished(&self, _pipeline: &str) {
        self.wake();
    }

    pub fn running_runs(&self) -> usize {
        self.running.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Starts queued runs while each pipeline and the service have capacity; returns when the
    /// service shuts down and its runs have finished.
    pub async fn dispatch(self: Arc<Self>) {
        loop {
            if !self.shutting_down() {
                self.start_queued();
            }
            if self.shutting_down() && self.running_runs() == 0 {
                self.idle.notify_waiters();
                return;
            }
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    }

    fn start_queued(self: &Arc<Self>) {
        for pipeline in &self.manifest.pipelines {
            let settings = match self
                .ledger
                .settings(&pipeline.uuid, pipeline.max_concurrent_runs)
            {
                Ok(settings) => settings,
                Err(error) => {
                    crate::log::error(&error.to_string());
                    continue;
                }
            };
            if settings.paused {
                continue;
            }
            loop {
                let running_here = self
                    .running
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .values()
                    .filter(|(p, _)| p == &pipeline.uuid)
                    .count() as u32;
                if running_here >= settings.max_concurrent_runs
                    || self.running_runs() >= self.max_total_runs
                {
                    break;
                }
                let run_id = match self.ledger.claim_next(&pipeline.uuid) {
                    Ok(Some(id)) => id,
                    Ok(None) => break,
                    Err(error) => {
                        crate::log::error(&error.to_string());
                        break;
                    }
                };
                let token = CancellationToken::new();
                self.running
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(run_id.clone(), (pipeline.uuid.clone(), token.clone()));
                let service = self.clone();
                tokio::spawn(async move {
                    let id = run_id.clone();
                    execute_run(service.clone(), run_id, token).await;
                    service
                        .running
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&id);
                    service.wake();
                });
            }
        }
    }

    /// Stops admission, waits for running runs up to the deadline, then cancels them.
    pub async fn shutdown(&self, deadline: Duration) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.wake();
        let waiting = self.running_runs();
        if waiting > 0 {
            crate::log::info(&format!(
                "Waiting up to {} s for {waiting} running run{} to finish.",
                deadline.as_secs(),
                if waiting == 1 { "" } else { "s" }
            ));
        }
        let start = Instant::now();
        while self.running_runs() > 0 && start.elapsed() < deadline {
            tokio::select! {
                _ = self.idle.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
        let tokens: Vec<CancellationToken> = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .map(|(_, token)| token.clone())
            .collect();
        if !tokens.is_empty() {
            crate::log::warn(&format!(
                "Cancelling {} run(s) still running.",
                tokens.len()
            ));
            for token in tokens {
                token.cancel();
            }
            let start = Instant::now();
            while self.running_runs() > 0 && start.elapsed() < Duration::from_secs(10) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if let Some(python) = &self.python {
            python.shutdown().await;
        }
    }

    /// The keyword arguments of the run's blocks, as Mage passes them.
    pub fn block_kwargs(
        &self,
        pipeline: &Pipeline,
        run: &RunSummary,
        run_variables: &Map<String, Value>,
    ) -> Map<String, Value> {
        let mut kwargs = pipeline.variables.clone();
        for (key, value) in run_variables {
            kwargs.insert(key.clone(), value.clone());
        }
        kwargs
            .entry("execution_date")
            .or_insert_with(|| json!(run.created_at));
        kwargs.entry("event").or_insert_with(|| json!({}));
        kwargs.insert("env".into(), json!(self.environment));
        kwargs.insert("pipeline_uuid".into(), json!(pipeline.uuid));
        kwargs.insert("pipeline_run_id".into(), json!(run.id));
        kwargs
    }

    pub fn observe_block(&self, pipeline: &str, block: &str, elapsed: Duration, success: bool) {
        let seconds = elapsed.as_secs_f64();
        let mut metrics = self.metrics.lock().unwrap_or_else(|p| p.into_inner());
        let entry = metrics
            .blocks
            .entry((pipeline.to_string(), block.to_string()))
            .or_default();
        if success {
            entry.succeeded += 1;
        } else {
            entry.failed += 1;
        }
        entry.seconds_sum += seconds;
        for (index, bound) in BUCKETS.iter().enumerate() {
            if seconds <= *bound {
                entry.buckets[index] += 1;
            }
        }
    }

    pub fn block_metrics(&self) -> BTreeMap<(String, String), BlockMetrics> {
        self.metrics
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .blocks
            .clone()
    }

    pub fn pipeline_snapshot(&self, pipeline: &Pipeline) -> Result<PipelineSnapshot, LedgerError> {
        let settings = self
            .ledger
            .settings(&pipeline.uuid, pipeline.max_concurrent_runs)?;
        let recent = self.ledger.recent_finished(&pipeline.uuid, 120)?;
        let in_flight = self.ledger.running_count(&pipeline.uuid)?;
        let queued = self.ledger.count(&pipeline.uuid, RunStatus::Queued)? as u32;
        let window_seconds = 60;
        let throughput = throughput(&recent, window_seconds, 60);
        let status = if settings.paused {
            "paused"
        } else if in_flight > 0 {
            "running"
        } else {
            "idle"
        };
        let now = Utc::now();
        let triggers = pipeline
            .triggers
            .iter()
            .map(|trigger| {
                let next_run_at = if trigger.kind == TriggerKind::Time
                    && trigger.active
                    && self.schedules_enabled
                {
                    Schedule::new(pipeline, trigger)
                        .ok()
                        .and_then(|s| s.next_after(now))
                        .map(timestamp)
                } else {
                    None
                };
                TriggerSnapshot {
                    name: trigger.name.clone(),
                    kind: match trigger.kind {
                        TriggerKind::Time => "time".into(),
                        TriggerKind::Api => "api".into(),
                    },
                    schedule: trigger.schedule.clone(),
                    active: trigger.active,
                    next_run_at,
                }
            })
            .collect();
        Ok(PipelineSnapshot {
            id: pipeline.uuid.clone(),
            status: status.into(),
            runs_succeeded: self.ledger.count(&pipeline.uuid, RunStatus::Completed)?,
            runs_failed: self.ledger.count(&pipeline.uuid, RunStatus::Failed)?,
            in_flight,
            queued,
            latency_ms: recent.iter().map(|(_, ms)| *ms).collect(),
            throughput,
            last_error: self.ledger.last_error(&pipeline.uuid)?,
            config_revision: settings.revision,
            max_in_flight: settings.max_concurrent_runs,
            window_seconds: window_seconds as u64,
            recent_runs: self.ledger.list_runs(Some(&pipeline.uuid), None, 20)?,
            triggers,
            blocks: pipeline.blocks.iter().map(|b| b.uuid.clone()).collect(),
        })
    }

    pub fn snapshot(&self) -> Result<Snapshot, LedgerError> {
        let pipelines = self
            .manifest
            .pipelines
            .iter()
            .map(|p| self.pipeline_snapshot(p))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Snapshot {
            service_id: self.manifest.service.name.clone(),
            revision: self
                .manifest
                .service
                .source_sha256
                .chars()
                .take(12)
                .collect(),
            sampled_at_unix_ms: Utc::now().timestamp_millis().max(0) as u64,
            pipelines,
            api_version: API_VERSION,
            schedules_enabled: self.schedules_enabled,
            started_at_unix_ms: self.started_at.timestamp_millis().max(0) as u64,
        })
    }

    /// Deletes finished runs older than the retention period with their files.
    pub fn prune(&self) {
        if self.retention_days == 0 {
            return;
        }
        let cutoff = Utc::now() - chrono::Duration::days(self.retention_days as i64);
        match self.ledger.prune(cutoff) {
            Ok(ids) => {
                for id in &ids {
                    let _ = std::fs::remove_dir_all(self.run_dir(id));
                }
                if !ids.is_empty() {
                    crate::log::info(&format!(
                        "Removed {} run(s) older than {} days.",
                        ids.len(),
                        self.retention_days
                    ));
                }
            }
            Err(error) => crate::log::error(&format!("Pruning old runs failed: {error}")),
        }
    }
}

/// Finished runs per window over the last `windows` windows, oldest first.
fn throughput(recent: &[(String, u64)], window_seconds: i64, windows: usize) -> Vec<u64> {
    let now = Utc::now().timestamp();
    let mut counts = vec![0u64; windows];
    for (finished, _) in recent {
        let Ok(at) = chrono::DateTime::parse_from_rfc3339(finished) else {
            continue;
        };
        let age = now - at.timestamp();
        if age < 0 {
            continue;
        }
        let index = (age / window_seconds) as usize;
        if index < windows {
            counts[windows - 1 - index] += 1;
        }
    }
    counts
}

pub fn ensure_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}
