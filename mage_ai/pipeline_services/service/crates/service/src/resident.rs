//! Resident Rust block programs for runs: the service's `mage-blocks` program started once
//! with MAGE_RESIDENT=1 (mage::serve_jobs) serves the jobs of many runs, so what a block
//! loads once, such as a model session kept in a static, stays loaded. Starting the program
//! and its ONNX session on every run cost 21 ms of a 25 ms request.
//!
//! A job is a line on the program's stdin; the program runs it as it would on its own (the
//! result or error in the job directory) and then prints a marker on stdout and on stderr,
//! so every log line before the markers is that job's. Programs are kept per binary and
//! connection profile, one job at a time each, and replaced after `max_jobs` jobs. A job
//! that runs past its timeout or is cancelled kills its program; a program that panicked
//! exits after reporting it. MAGE_SERVICE_RUST_RESIDENT=off starts a program per run.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Mutex;
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio_util::sync::CancellationToken;

use crate::executor::BlockFailure;
use crate::log::RunLog;
use crate::service::Service;

/// mage::runtime::JOB_DONE.
const JOB_DONE: &str = "\u{1e}mage-job-done";
/// mage::runtime::BLOCK_MARKER.
const BLOCK_MARKER: &str = "\u{1e}mage-block ";

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    binary: PathBuf,
    /// The rendered connection profile (MAGE_IO_CONFIG), set when the program starts.
    io_config: Option<String>,
}

struct Program {
    child: Child,
    pid: u32,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    stderr: Lines<BufReader<ChildStderr>>,
    jobs: u32,
}

pub struct Residents {
    idle: Mutex<HashMap<Key, Vec<Program>>>,
    max_jobs: u32,
    max_idle: usize,
}

impl Residents {
    /// None with MAGE_SERVICE_RUST_RESIDENT=off. MAGE_SERVICE_RUST_MAX_JOBS (1000) jobs per
    /// program before it is replaced; `max_idle` programs kept per binary and profile.
    pub fn from_env(max_idle: usize) -> Option<Residents> {
        if std::env::var("MAGE_SERVICE_RUST_RESIDENT").is_ok_and(|v| v == "off") {
            return None;
        }
        let max_jobs = std::env::var("MAGE_SERVICE_RUST_MAX_JOBS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(1000);
        Some(Residents {
            idle: Mutex::new(HashMap::new()),
            max_jobs,
            max_idle: max_idle.max(1),
        })
    }

    fn take(&self, key: &Key) -> Option<Program> {
        let mut idle = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        let programs = idle.get_mut(key)?;
        // A program that exited while idle (killed, out of memory) is dropped here.
        while let Some(mut program) = programs.pop() {
            if matches!(program.child.try_wait(), Ok(None)) {
                return Some(program);
            }
        }
        None
    }

    fn give_back(&self, key: Key, program: Program) {
        if program.jobs >= self.max_jobs {
            return;
        }
        let mut idle = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        let programs = idle.entry(key).or_default();
        if programs.len() < self.max_idle {
            programs.push(program);
        }
    }

    fn start(service: &Service, key: &Key) -> Result<Program, BlockFailure> {
        let mut command = tokio::process::Command::new(&key.binary);
        command
            .current_dir(service.project_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true)
            // Only the block environment: the service's own credentials are not inherited.
            .env_clear()
            .envs(service.extra_env.iter().cloned())
            .env("MAGE_RESIDENT", "1");
        if let Some(profile) = &key.io_config {
            command.env("MAGE_IO_CONFIG", profile);
        }
        let mut child = command.spawn().map_err(|e| {
            BlockFailure::fatal(format!("could not start {}: {e}", key.binary.display()))
        })?;
        let pid = child.id().unwrap_or(0);
        let stdin = child.stdin.take().expect("piped");
        let stdout = BufReader::new(child.stdout.take().expect("piped")).lines();
        let stderr = BufReader::new(child.stderr.take().expect("piped")).lines();
        Ok(Program {
            child,
            pid,
            stdin,
            stdout,
            stderr,
            jobs: 0,
        })
    }

    /// Runs the job in `job_dir` (the block `block`, or the chain job.json lists) on a
    /// resident program; its exit status as a program run on its own would have.
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        &self,
        service: &Service,
        binary: &Path,
        job_dir: &Path,
        block: Option<&str>,
        io_config: Option<String>,
        log: &RunLog,
        tag: &str,
        timeout: Option<Duration>,
        cancel: &CancellationToken,
    ) -> Result<ExitStatus, BlockFailure> {
        use std::os::unix::process::ExitStatusExt;

        let key = Key {
            binary: binary.to_path_buf(),
            io_config,
        };
        let job_dir = std::path::absolute(job_dir)
            .map_err(|e| BlockFailure::retryable(format!("the job directory: {e}")))?;
        // A retry runs in the same directory: the last attempt's files would read as this
        // one's.
        let _ = std::fs::remove_file(job_dir.join("error.json"));
        let _ = std::fs::remove_file(job_dir.join("result.json"));
        let request = json!({"job_dir": job_dir, "block": block}).to_string() + "\n";
        let mut program = match self.take(&key) {
            Some(program) => program,
            None => Residents::start(service, &key)?,
        };
        if program.stdin.write_all(request.as_bytes()).await.is_err()
            || program.stdin.flush().await.is_err()
        {
            // It exited between the check and the write: a new one takes the job.
            program = Residents::start(service, &key)?;
            program
                .stdin
                .write_all(request.as_bytes())
                .await
                .map_err(|e| BlockFailure::retryable(format!("sending the job: {e}")))?;
            program
                .stdin
                .flush()
                .await
                .map_err(|e| BlockFailure::retryable(format!("sending the job: {e}")))?;
        }
        let pid = program.pid;
        let served = serve(&mut program, log, tag);
        let outcome = tokio::select! {
            outcome = served => outcome,
            _ = cancel.cancelled() => {
                crate::python::kill_group(pid);
                return Err(BlockFailure::fatal("the run was cancelled"));
            }
            _ = sleep_for(timeout) => {
                crate::python::kill_group(pid);
                return Err(BlockFailure::retryable(format!(
                    "the block ran longer than {} seconds and was stopped",
                    timeout.unwrap_or_default().as_secs()
                )));
            }
        };
        match outcome {
            Served::Done => {
                program.jobs += 1;
                let failed = job_dir.join("error.json").exists();
                self.give_back(key, program);
                Ok(ExitStatus::from_raw(if failed { 1 << 8 } else { 0 }))
            }
            // It exited: after a panic it wrote the error first.
            Served::Exited => {
                program.child.wait().await.map_err(|e| {
                    BlockFailure::retryable(format!("waiting for the block failed: {e}"))
                })
            }
        }
    }
}

enum Served {
    Done,
    Exited,
}

async fn sleep_for(timeout: Option<Duration>) {
    match timeout {
        Some(limit) => tokio::time::sleep(limit).await,
        None => std::future::pending().await,
    }
}

/// Writes the job's lines to the run's log until the program marks the job done on both
/// streams, or exits.
async fn serve(program: &mut Program, log: &RunLog, tag: &str) -> Served {
    let mut current = tag.to_string();
    let (mut out_done, mut err_done) = (false, false);
    while !(out_done && err_done) {
        tokio::select! {
            line = program.stdout.next_line(), if !out_done => match line {
                Ok(Some(line)) if line == JOB_DONE => out_done = true,
                Ok(Some(line)) => match line.strip_prefix(BLOCK_MARKER) {
                    Some(block) => current = block.to_string(),
                    None => log.write("info", Some(&current), Some("stdout"), &line),
                },
                _ => return Served::Exited,
            },
            line = program.stderr.next_line(), if !err_done => match line {
                Ok(Some(line)) if line == JOB_DONE => err_done = true,
                Ok(Some(line)) => log.write("info", Some(&current), Some("stderr"), &line),
                _ => return Served::Exited,
            },
        }
    }
    Served::Done
}
