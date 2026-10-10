//! The files a block run exchanges with Mage, all in one job directory.
//!
//! Mage writes `job.json` and the input files, then runs the block's binary with the job
//! directory as its only argument. The binary writes its outputs, then `result.json`
//! when it succeeds or `error.json` when it fails, and exits with 0 or 1.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const API_VERSION: u32 = 1;
pub const JOB_FILE: &str = "job.json";
pub const RESULT_FILE: &str = "result.json";
pub const ERROR_FILE: &str = "error.json";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub api_version: u32,
    pub block_uuid: String,
    pub block_type: String,
    #[serde(default)]
    pub pipeline_uuid: Option<String>,
    #[serde(default)]
    pub execution_partition: Option<String>,
    pub inputs: Vec<Input>,
    #[serde(default)]
    pub variables: Map<String, Value>,
    /// Where outputs go; relative to the job directory.
    pub output_dir: PathBuf,
}

/// One upstream output, in the order of the block's upstream blocks.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Input {
    /// A table in an Arrow IPC or Parquet file.
    Frame { format: FrameFormat, path: PathBuf },
    /// Any other value, as JSON.
    Json { path: PathBuf },
    /// The upstream block returned nothing.
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameFormat {
    Ipc,
    Parquet,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputRecord {
    Frame {
        format: FrameFormat,
        path: String,
        rows: Option<u64>,
    },
    Json {
        path: String,
    },
    Decision {
        value: bool,
    },
}

#[derive(Debug, Serialize)]
pub struct TestRecord {
    pub name: String,
    pub passed: bool,
    pub message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RunResult {
    pub api_version: u32,
    pub outputs: Vec<OutputRecord>,
    pub tests: Vec<TestRecord>,
}

#[derive(Debug, Serialize)]
pub struct ErrorRecord {
    /// `error` for an Err returned by the block, `panic` for a panic.
    pub kind: &'static str,
    pub message: String,
    /// file:line:column of a panic.
    pub location: Option<String>,
}

impl Job {
    pub fn read(job_dir: &Path) -> anyhow::Result<Job> {
        let bytes = std::fs::read(job_dir.join(JOB_FILE))?;
        let job: Job = serde_json::from_slice(&bytes)?;
        if job.api_version != API_VERSION {
            anyhow::bail!(
                "This block was built for job version {API_VERSION}; Mage sent version {}. \
                 Run the block again to rebuild it.",
                job.api_version
            );
        }
        Ok(job)
    }
}

/// Writes JSON to a temporary file and renames it, so Mage never reads half a file.
pub fn write_json_atomic(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec(value)?)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}
