//! The HTTP API of a pipeline service, shared by the service and the console.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const API_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Queued => "queued",
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }

    pub fn parse(text: &str) -> Option<RunStatus> {
        Some(match text {
            "queued" => RunStatus::Queued,
            "running" => RunStatus::Running,
            "completed" => RunStatus::Completed,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            _ => return None,
        })
    }

    pub fn finished(self) -> bool {
        matches!(
            self,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BlockStatus {
    Pending,
    Running,
    Completed,
    Failed,
    /// An upstream block failed, so this block did not run.
    UpstreamFailed,
    Cancelled,
}

impl BlockStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockStatus::Pending => "pending",
            BlockStatus::Running => "running",
            BlockStatus::Completed => "completed",
            BlockStatus::Failed => "failed",
            BlockStatus::UpstreamFailed => "upstream_failed",
            BlockStatus::Cancelled => "cancelled",
        }
    }

    pub fn parse(text: &str) -> Option<BlockStatus> {
        Some(match text {
            "pending" => BlockStatus::Pending,
            "running" => BlockStatus::Running,
            "completed" => BlockStatus::Completed,
            "failed" => BlockStatus::Failed,
            "upstream_failed" => BlockStatus::UpstreamFailed,
            "cancelled" => BlockStatus::Cancelled,
            _ => return None,
        })
    }
}

/// `POST /v1/pipelines/{pipeline}/runs`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    #[serde(default)]
    pub variables: Map<String, Value>,
    /// The same key returns the run it created before, so a retried request runs once.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunSummary {
    pub id: String,
    pub pipeline: String,
    pub status: RunStatus,
    /// `api`, `schedule:<trigger>` or `once:<trigger>`.
    pub source: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockRunSummary {
    pub block: String,
    pub status: BlockStatus,
    pub attempts: u32,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
    #[serde(default)]
    pub outputs: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunDetail {
    #[serde(flatten)]
    pub run: RunSummary,
    pub variables: Map<String, Value>,
    pub blocks: Vec<BlockRunSummary>,
}

/// `GET /v1/snapshot`: everything the console draws, in one bounded response.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Snapshot {
    pub service_id: String,
    pub revision: String,
    pub sampled_at_unix_ms: u64,
    pub pipelines: Vec<PipelineSnapshot>,
    #[serde(default)]
    pub api_version: u32,
    #[serde(default)]
    pub schedules_enabled: bool,
    #[serde(default)]
    pub started_at_unix_ms: u64,
    #[serde(default)]
    pub models: Vec<ModelSnapshot>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ModelSnapshot {
    pub name: String,
    pub uri: String,
    pub version: Option<String>,
    pub flavors: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PipelineSnapshot {
    pub id: String,
    /// `idle`, `running` or `paused`.
    pub status: String,
    pub runs_succeeded: u64,
    pub runs_failed: u64,
    pub in_flight: u32,
    pub queued: u32,
    /// Durations of recent finished runs, oldest first.
    pub latency_ms: Vec<u64>,
    /// Finished runs per window, oldest first; `window_seconds` long.
    pub throughput: Vec<u64>,
    pub last_error: Option<String>,
    pub config_revision: u64,
    pub max_in_flight: u32,
    #[serde(default = "default_window")]
    pub window_seconds: u64,
    #[serde(default)]
    pub recent_runs: Vec<RunSummary>,
    #[serde(default)]
    pub triggers: Vec<TriggerSnapshot>,
    #[serde(default)]
    pub blocks: Vec<String>,
}

fn default_window() -> u64 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TriggerSnapshot {
    pub name: String,
    pub kind: String,
    pub schedule: Option<String>,
    pub active: bool,
    pub next_run_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ControlRequest {
    pub pipeline_id: String,
    pub request_id: String,
    pub expected_config_revision: u64,
    pub command: ControlCommand,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlCommand {
    Pause {},
    Resume {},
    SetConcurrency { max_in_flight: u32 },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ControlReceipt {
    pub request_id: String,
    pub pipeline: PipelineSnapshot,
}
