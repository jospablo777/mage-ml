//! Python blocks run in long-lived worker processes (`mage_ai/pipeline_services/runtime/worker.py`).
//!
//! A worker serves one block at a time: the request goes to its stdin as one JSON line and
//! the reply comes back on file descriptor 3, so whatever the block prints on stdout and
//! stderr stays a log line of that block. Idle workers wait in a pool, so a block pays no
//! interpreter start-up or imports. A worker that times out, crashes or served its quota of
//! blocks is killed with its process group and replaced.

use std::io::{BufRead, BufReader};
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt as _;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Semaphore, mpsc};

use crate::log::RunLog;

#[derive(Clone, Debug)]
pub struct PythonConfig {
    pub executable: String,
    pub worker_script: PathBuf,
    pub project_dir: PathBuf,
    pub max_workers: usize,
    /// Blocks a worker serves before it is replaced, to bound leaked memory and state.
    pub max_blocks_per_worker: u32,
    pub env: Vec<(String, String)>,
}

/// Who receives the lines a worker prints: the run log of its current block.
type LogTarget = Arc<Mutex<Option<(RunLog, String)>>>;

struct Worker {
    child: Child,
    pid: u32,
    stdin: ChildStdin,
    replies: mpsc::Receiver<String>,
    log_target: LogTarget,
    blocks_served: u32,
    stderr_tail: Arc<Mutex<Vec<String>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("{0}")]
    Start(String),
    #[error("the block ran longer than {0} seconds and was stopped")]
    Timeout(u64),
    #[error("the Python process stopped while running the block{0}")]
    Crashed(String),
    #[error("the Python worker sent an invalid reply: {0}")]
    Protocol(String),
    #[error("the run was cancelled")]
    Cancelled,
}

pub struct PythonPool {
    config: PythonConfig,
    idle: Mutex<Vec<Worker>>,
    slots: Semaphore,
}

impl PythonPool {
    pub fn new(config: PythonConfig) -> Arc<PythonPool> {
        let slots = Semaphore::new(config.max_workers.max(1));
        Arc::new(PythonPool {
            config,
            idle: Mutex::new(Vec::new()),
            slots,
        })
    }

    /// Starts one worker ahead of the first block, so the first run starts at once.
    pub async fn warm(&self) -> Result<(), WorkerError> {
        let worker = self.spawn().await?;
        self.idle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(worker);
        Ok(())
    }

    async fn spawn(&self) -> Result<Worker, WorkerError> {
        let mut fds = [0; 2];
        // SAFETY: pipe writes two new descriptors into the array.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(WorkerError::Start(format!(
                "could not create the reply pipe: {}",
                std::io::Error::last_os_error()
            )));
        }
        // SAFETY: both descriptors were just created and are owned here.
        let (read_end, write_end) =
            unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        let write_raw = fds[1];
        let read_raw = fds[0];

        let mut command = std::process::Command::new(&self.config.executable);
        command
            .arg("-u")
            .arg(&self.config.worker_script)
            .current_dir(&self.config.project_dir)
            .env("MAGE_REPO_PATH", &self.config.project_dir)
            .env("PYTHONUNBUFFERED", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Its own process group: a timeout kills the block's child processes too.
            .process_group(0);
        for (key, value) in &self.config.env {
            command.env(key, value);
        }
        // SAFETY: only async-signal-safe calls between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::dup2(write_raw, 3) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                libc::close(read_raw);
                if write_raw != 3 {
                    libc::close(write_raw);
                }
                Ok(())
            });
        }
        let mut child = Command::from(command)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                WorkerError::Start(format!(
                    "could not start Python ({}): {e}",
                    self.config.executable
                ))
            })?;
        drop(write_end);
        let pid = child.id().unwrap_or(0);

        let (reply_tx, mut replies) = mpsc::channel::<String>(4);
        std::thread::Builder::new()
            .name(format!("python-replies-{pid}"))
            .spawn(move || {
                let reader = BufReader::new(std::fs::File::from(read_end));
                for line in reader.lines() {
                    let Ok(line) = line else { break };
                    if reply_tx.blocking_send(line).is_err() {
                        break;
                    }
                }
            })
            .map_err(|e| WorkerError::Start(format!("could not start a reader thread: {e}")))?;

        let log_target: LogTarget = Arc::new(Mutex::new(None));
        let stderr_tail = Arc::new(Mutex::new(Vec::new()));
        if let Some(stdout) = child.stdout.take() {
            forward(stdout, "stdout", log_target.clone(), None);
        }
        if let Some(stderr) = child.stderr.take() {
            forward(
                stderr,
                "stderr",
                log_target.clone(),
                Some(stderr_tail.clone()),
            );
        }
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| WorkerError::Start("the worker has no stdin".into()))?;

        let ready = tokio::time::timeout(Duration::from_secs(120), replies.recv()).await;
        match ready {
            Ok(Some(line)) if line.contains("\"ready\"") => {}
            Ok(Some(line)) => {
                return Err(WorkerError::Start(format!(
                    "the Python worker started with an unexpected message: {line}"
                )));
            }
            Ok(None) => {
                let status = child.wait().await.ok();
                let tail = stderr_tail
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .join("\n");
                return Err(WorkerError::Start(format!(
                    "the Python worker exited at start ({}):\n{tail}",
                    status.map(|s| s.to_string()).unwrap_or_default()
                )));
            }
            Err(_) => {
                kill_group(pid);
                return Err(WorkerError::Start(
                    "the Python worker did not start within 120 seconds".into(),
                ));
            }
        }
        Ok(Worker {
            child,
            pid,
            stdin,
            replies,
            log_target,
            blocks_served: 0,
            stderr_tail,
        })
    }

    /// Runs one block request on a worker; returns the worker's reply.
    pub async fn run(
        &self,
        mut request: Value,
        log: RunLog,
        block: &str,
        timeout: Option<Duration>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<Value, WorkerError> {
        let _slot = self
            .slots
            .acquire()
            .await
            .map_err(|_| WorkerError::Start("the worker pool is closed".into()))?;
        let pooled = self.idle.lock().unwrap_or_else(|p| p.into_inner()).pop();
        let mut worker = match pooled {
            Some(worker) => worker,
            None => self.spawn().await?,
        };
        let id = uuid::Uuid::new_v4().to_string();
        request["id"] = json!(id);
        request["op"] = json!("run");
        *worker.log_target.lock().unwrap_or_else(|p| p.into_inner()) =
            Some((log, block.to_string()));
        worker
            .stderr_tail
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();

        let outcome = async {
            let mut line = serde_json::to_string(&request)
                .map_err(|e| WorkerError::Protocol(e.to_string()))?;
            line.push('\n');
            if worker.stdin.write_all(line.as_bytes()).await.is_err() {
                return Err(WorkerError::Crashed(String::new()));
            }
            let _ = worker.stdin.flush().await;
            match worker.replies.recv().await {
                Some(reply) => serde_json::from_str::<Value>(&reply)
                    .map_err(|e| WorkerError::Protocol(e.to_string())),
                None => Err(WorkerError::Crashed(String::new())),
            }
        };
        let limited = async {
            match timeout {
                Some(limit) => match tokio::time::timeout(limit, outcome).await {
                    Ok(result) => result,
                    Err(_) => Err(WorkerError::Timeout(limit.as_secs())),
                },
                None => outcome.await,
            }
        };
        let result = tokio::select! {
            result = limited => result,
            _ = cancel.cancelled() => Err(WorkerError::Cancelled),
        };
        // The target stays the block's until the worker's next block: stdout and stderr are
        // other pipes than the reply, so a block's last lines can arrive after its reply.
        // A worker runs one block at a time, so they are still that block's.

        match result {
            Ok(reply) if reply.get("id").and_then(Value::as_str) == Some(id.as_str()) => {
                worker.blocks_served += 1;
                if worker.blocks_served >= self.config.max_blocks_per_worker {
                    retire(worker).await;
                } else {
                    self.idle
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .push(worker);
                }
                Ok(reply)
            }
            Ok(reply) => {
                retire(worker).await;
                Err(WorkerError::Protocol(format!(
                    "a reply to another request: {reply}"
                )))
            }
            Err(WorkerError::Crashed(_)) => {
                let status =
                    match tokio::time::timeout(Duration::from_secs(5), worker.child.wait()).await {
                        Ok(Ok(status)) => describe_exit(status),
                        _ => String::new(),
                    };
                let tail = worker
                    .stderr_tail
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .join("\n");
                kill_group(worker.pid);
                let mut detail = status;
                if !tail.is_empty() {
                    detail.push_str(&format!(":\n{tail}"));
                }
                Err(WorkerError::Crashed(detail))
            }
            Err(error) => {
                kill_group(worker.pid);
                let _ = worker.child.wait().await;
                Err(error)
            }
        }
    }

    pub async fn shutdown(&self) {
        let workers: Vec<Worker> =
            std::mem::take(&mut *self.idle.lock().unwrap_or_else(|p| p.into_inner()));
        for worker in workers {
            retire(worker).await;
        }
    }
}

async fn retire(mut worker: Worker) {
    let _ = worker.stdin.write_all(b"{\"op\":\"shutdown\"}\n").await;
    let _ = worker.stdin.flush().await;
    if tokio::time::timeout(Duration::from_secs(5), worker.child.wait())
        .await
        .is_err()
    {
        kill_group(worker.pid);
        let _ = worker.child.wait().await;
    }
}

pub fn kill_group(pid: u32) {
    if pid > 0 {
        // SAFETY: signals the worker's own process group.
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGKILL);
        }
    }
}

fn describe_exit(status: std::process::ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    match (status.code(), status.signal()) {
        (_, Some(libc::SIGKILL)) => " (killed, often by running out of memory)".into(),
        (_, Some(signal)) => format!(" (signal {signal})"),
        (Some(code), _) => format!(" (exit code {code})"),
        _ => String::new(),
    }
}

const STDERR_TAIL_LINES: usize = 40;

fn forward(
    stream: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    name: &'static str,
    target: LogTarget,
    tail: Option<Arc<Mutex<Vec<String>>>>,
) {
    tokio::spawn(async move {
        let mut lines = AsyncBufReader::new(stream).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(tail) = &tail {
                let mut tail = tail.lock().unwrap_or_else(|p| p.into_inner());
                tail.push(line.clone());
                if tail.len() > STDERR_TAIL_LINES {
                    tail.remove(0);
                }
            }
            let current = target.lock().unwrap_or_else(|p| p.into_inner()).clone();
            match current {
                Some((log, block)) => log.write("info", Some(&block), Some(name), &line),
                None => crate::log::info(&format!("python worker {name}: {line}")),
            }
        }
    });
}
