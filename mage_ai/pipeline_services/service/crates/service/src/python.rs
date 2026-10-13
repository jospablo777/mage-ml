//! Python blocks run in long-lived worker processes (`mage_ai/pipeline_services/runtime/worker.py`).
//!
//! A worker serves one block at a time: the request goes to file descriptor 4 as one JSON
//! line and the reply comes back on file descriptor 3, so whatever the block prints on
//! stdout and stderr stays a log line of that block. Its stdin is /dev/null: a block that
//! read stdin used to read the next requests. The worker marks both descriptors
//! non-inheritable, so programs a block starts cannot reach them. Idle workers wait in a pool, so a block pays no
//! interpreter start-up or imports. A worker that times out, crashes or served its quota of
//! blocks is killed with its process group and replaced.

use std::io::{BufRead, BufReader};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt as _;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::process::{Child, Command};
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
    /// Stream batches do not count toward `max_blocks_per_worker` (a stream would reload its
    /// blocks and models every few seconds); a worker that serves streams is replaced when
    /// its memory passes this many bytes, or, when 0, when it doubled since its first stream
    /// batch plus 256 MB.
    pub max_rss_bytes: u64,
    pub env: Vec<(String, String)>,
}

/// How a request counts toward replacing its worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recycle {
    /// A block run: one of `max_blocks_per_worker`.
    AfterBlocks,
    /// A stream batch: only memory growth replaces the worker.
    OnMemory,
}

const RSS_HEADROOM: u64 = 256 * 1024 * 1024;

/// Who receives the lines a worker prints: the run log of its current block.
type LogTarget = Arc<Mutex<Option<(RunLog, String)>>>;

struct Worker {
    child: Child,
    pid: u32,
    requests: tokio::fs::File,
    replies: mpsc::Receiver<String>,
    log_target: LogTarget,
    blocks_served: u32,
    /// Its memory after its first stream batch, which holds the blocks' models.
    baseline_rss: Option<u64>,
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
    /// A worker starting in the background (warm_in_background), which the first block
    /// that needs a worker takes instead of starting another.
    warming: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<Worker, WorkerError>>>>,
}

impl PythonPool {
    pub fn new(config: PythonConfig) -> Arc<PythonPool> {
        let slots = Semaphore::new(config.max_workers.max(1));
        Arc::new(PythonPool {
            config,
            idle: Mutex::new(Vec::new()),
            slots,
            warming: tokio::sync::Mutex::new(None),
        })
    }

    /// Starts one worker while the run's first blocks run: a run whose first blocks are
    /// Rust does not wait for Python to start. A worker that cannot start fails the first
    /// block that needs it, with the reason.
    pub async fn warm_in_background(self: &Arc<Self>) {
        let pool = self.clone();
        *self.warming.lock().await = Some(tokio::spawn(async move { pool.spawn().await }));
    }

    async fn take_warming(&self) -> Option<Result<Worker, WorkerError>> {
        let handle = self.warming.lock().await.take()?;
        Some(
            handle.await.unwrap_or_else(|e| {
                Err(WorkerError::Start(format!("the worker task failed: {e}")))
            }),
        )
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
        let (read_end, write_end) = pipe("reply")?;
        let (request_read, request_write) = pipe("request")?;
        let write_raw = write_end.as_raw_fd();
        let read_raw = read_end.as_raw_fd();
        let request_read_raw = request_read.as_raw_fd();
        let request_write_raw = request_write.as_raw_fd();

        let mut command = std::process::Command::new(&self.config.executable);
        // `config.env` is the worker's whole environment; nothing else is inherited, so the
        // service's own credentials never reach block code.
        command
            .env_clear()
            .arg("-u")
            .arg(&self.config.worker_script)
            .current_dir(&self.config.project_dir)
            .env("MAGE_REPO_PATH", &self.config.project_dir)
            .env("PYTHONUNBUFFERED", "1")
            .env("MAGE_WORKER_REQUEST_FD", "4")
            .stdin(Stdio::null())
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
                // Move both ends above 9 first, so placing one on 3 or 4 cannot overwrite the
                // other whatever numbers the pipes got.
                let reply = libc::fcntl(write_raw, libc::F_DUPFD, 10);
                let request = libc::fcntl(request_read_raw, libc::F_DUPFD, 10);
                if reply < 0 || request < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::dup2(reply, 3) < 0 || libc::dup2(request, 4) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                for fd in [
                    reply,
                    request,
                    write_raw,
                    read_raw,
                    request_read_raw,
                    request_write_raw,
                ] {
                    if fd != 3 && fd != 4 {
                        libc::close(fd);
                    }
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
        drop(request_read);
        let requests = tokio::fs::File::from_std(std::fs::File::from(request_write));
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
            requests,
            replies,
            log_target,
            blocks_served: 0,
            baseline_rss: None,
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
        let line = move |id: &str| {
            request["id"] = json!(id);
            // A batch run by default; streams send "stream".
            if request.get("op").is_none() {
                request["op"] = json!("run");
            }
            serde_json::to_string(&request)
        };
        let reply = self.run_raw(line, log, block, timeout, cancel).await?;
        serde_json::from_str(&reply).map_err(|e| WorkerError::Protocol(e.to_string()))
    }

    /// Runs one request whose JSON `line` builds for the given request id, and returns the
    /// reply as the worker wrote it. Streams use it to pass message values through without
    /// parsing them into `Value`, which would round integers past 64 bits.
    pub async fn run_raw<F>(
        &self,
        line: F,
        log: RunLog,
        block: &str,
        timeout: Option<Duration>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<String, WorkerError>
    where
        F: FnOnce(&str) -> Result<String, serde_json::Error>,
    {
        self.exchange(line, log, block, timeout, cancel, Recycle::AfterBlocks)
            .await
            .map(|(reply, _)| reply)
    }

    /// A stream batch: like `run_raw`, but only memory growth replaces the worker. Also
    /// returns whether the worker was replaced, so the stream can load its blocks into the
    /// next one before its next batch.
    pub async fn run_stream<F>(
        &self,
        line: F,
        log: RunLog,
        timeout: Option<Duration>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<(String, bool), WorkerError>
    where
        F: FnOnce(&str) -> Result<String, serde_json::Error>,
    {
        self.exchange(line, log, "stream", timeout, cancel, Recycle::OnMemory)
            .await
    }

    async fn exchange<F>(
        &self,
        line: F,
        log: RunLog,
        block: &str,
        timeout: Option<Duration>,
        cancel: &tokio_util::sync::CancellationToken,
        recycle: Recycle,
    ) -> Result<(String, bool), WorkerError>
    where
        F: FnOnce(&str) -> Result<String, serde_json::Error>,
    {
        let _slot = self
            .slots
            .acquire()
            .await
            .map_err(|_| WorkerError::Start("the worker pool is closed".into()))?;
        let pooled = self.idle.lock().unwrap_or_else(|p| p.into_inner()).pop();
        let mut worker = match pooled {
            Some(worker) => worker,
            None => match self.take_warming().await {
                Some(started) => started?,
                None => self.spawn().await?,
            },
        };
        let id = uuid::Uuid::new_v4().to_string();
        let request = line(&id).map_err(|e| WorkerError::Protocol(e.to_string()));
        *worker.log_target.lock().unwrap_or_else(|p| p.into_inner()) =
            Some((log, block.to_string()));
        worker
            .stderr_tail
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();

        let outcome = async {
            let mut line = request?;
            line.push('\n');
            if worker.requests.write_all(line.as_bytes()).await.is_err() {
                return Err(WorkerError::Crashed(String::new()));
            }
            let _ = worker.requests.flush().await;
            match worker.replies.recv().await {
                Some(reply) => Ok(reply),
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
            Ok(reply) if reply_id(&reply).as_deref() == Some(id.as_str()) => {
                let replace = match recycle {
                    Recycle::AfterBlocks => {
                        worker.blocks_served += 1;
                        worker.blocks_served >= self.config.max_blocks_per_worker
                    }
                    Recycle::OnMemory => match reply_rss(&reply) {
                        Some(rss) => {
                            let baseline = *worker.baseline_rss.get_or_insert(rss);
                            let limit = if self.config.max_rss_bytes > 0 {
                                self.config.max_rss_bytes
                            } else {
                                baseline.saturating_mul(2).saturating_add(RSS_HEADROOM)
                            };
                            if rss > limit {
                                crate::log::info(&format!(
                                    "A Python worker serving a stream uses {} MB, above its \
                                     limit of {} MB; it is replaced.",
                                    rss / 1_048_576,
                                    limit / 1_048_576
                                ));
                            }
                            rss > limit
                        }
                        None => false,
                    },
                };
                if replace {
                    retire(worker).await;
                } else {
                    self.idle
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .push(worker);
                }
                Ok((reply, replace))
            }
            Ok(reply) => {
                retire(worker).await;
                let shown: String = reply.chars().take(500).collect();
                Err(WorkerError::Protocol(format!(
                    "a reply to another request: {shown}"
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
        let mut workers: Vec<Worker> =
            std::mem::take(&mut *self.idle.lock().unwrap_or_else(|p| p.into_inner()));
        if let Some(Ok(worker)) = self.take_warming().await {
            workers.push(worker);
        }
        for worker in workers {
            retire(worker).await;
        }
    }
}

async fn retire(mut worker: Worker) {
    let _ = worker.requests.write_all(b"{\"op\":\"shutdown\"}\n").await;
    let _ = worker.requests.flush().await;
    if tokio::time::timeout(Duration::from_secs(5), worker.child.wait())
        .await
        .is_err()
    {
        kill_group(worker.pid);
        let _ = worker.child.wait().await;
    }
}

/// A pipe whose descriptors close on exec in the service: only the worker's copies, placed
/// on 3 and 4 before exec, survive.
fn pipe(name: &str) -> Result<(OwnedFd, OwnedFd), WorkerError> {
    let mut fds = [0; 2];
    // SAFETY: pipe writes two new descriptors into the array.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(WorkerError::Start(format!(
            "could not create the {name} pipe: {}",
            std::io::Error::last_os_error()
        )));
    }
    for fd in fds {
        // SAFETY: fd was just created and is valid.
        unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    }
    // SAFETY: both descriptors were just created and are owned here.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

pub fn kill_group(pid: u32) {
    if pid > 0 {
        // SAFETY: signals the worker's own process group.
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGKILL);
        }
    }
}

/// How a child process ended, as a suffix for "the block exited": signals name their usual
/// cause.
pub(crate) fn describe_exit(status: std::process::ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    match (status.code(), status.signal()) {
        (_, Some(libc::SIGKILL)) => " (killed, often by running out of memory)".into(),
        (_, Some(libc::SIGSEGV)) => " (crashed in native code: segmentation fault)".into(),
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
            // A chain of blocks names each block before its lines.
            if let Some(block) = line.strip_prefix("\u{1e}mage-block ") {
                if let Some((_, current)) =
                    target.lock().unwrap_or_else(|p| p.into_inner()).as_mut()
                {
                    *current = block.to_string();
                }
                continue;
            }
            let current = target.lock().unwrap_or_else(|p| p.into_inner()).clone();
            match current {
                Some((log, block)) => log.write("info", Some(&block), Some(name), &line),
                None => crate::log::info(&format!("python worker {name}: {line}")),
            }
        }
    });
}

#[derive(serde::Deserialize)]
struct ReplyHead {
    id: Option<String>,
    rss_bytes: Option<u64>,
}

/// The `id` of a worker reply, read without parsing the rest of it into values.
fn reply_id(reply: &str) -> Option<String> {
    serde_json::from_str::<ReplyHead>(reply).ok()?.id
}

/// The worker's memory when it replied.
fn reply_rss(reply: &str) -> Option<u64> {
    serde_json::from_str::<ReplyHead>(reply).ok()?.rss_bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(
        max_blocks_per_worker: u32,
        max_rss_bytes: u64,
    ) -> (Arc<PythonPool>, tempfile::TempDir) {
        let folder = tempfile::tempdir().unwrap();
        let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../runtime/worker.py");
        let pool = PythonPool::new(PythonConfig {
            executable: std::env::var("MAGE_SERVICE_TEST_PYTHON")
                .unwrap_or_else(|_| "python3".into()),
            worker_script: worker,
            project_dir: folder.path().to_path_buf(),
            max_workers: 1,
            max_blocks_per_worker,
            max_rss_bytes,
            env: Vec::new(),
        });
        (pool, folder)
    }

    async fn batch(pool: &PythonPool, folder: &std::path::Path) -> bool {
        let log = RunLog::create(&folder.join("log"), "stream").unwrap();
        let line = |id: &str| {
            serde_json::to_string(&json!({"id": id, "op": "stream", "chain": [], "messages": []}))
        };
        let (_, replaced) = pool
            .run_stream(line, log, None, &tokio_util::sync::CancellationToken::new())
            .await
            .unwrap();
        replaced
    }

    #[tokio::test]
    async fn stream_batches_replace_a_worker_on_memory_not_on_count() {
        // One block per worker, but stream batches do not count.
        let (counting, folder) = pool(1, u64::MAX);
        for _ in 0..3 {
            assert!(!batch(&counting, folder.path()).await);
        }
        // Past its memory limit, the worker is replaced.
        let (limited, folder) = pool(200, 1);
        assert!(batch(&limited, folder.path()).await);
    }
}

#[cfg(test)]
mod exit_tests {
    use super::describe_exit;
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;

    #[test]
    fn exits_name_their_usual_cause() {
        // A raw wait status: the low 7 bits hold the signal, the next byte the exit code.
        assert_eq!(
            describe_exit(ExitStatus::from_raw(libc::SIGKILL)),
            " (killed, often by running out of memory)"
        );
        assert_eq!(
            describe_exit(ExitStatus::from_raw(libc::SIGSEGV)),
            " (crashed in native code: segmentation fault)"
        );
        assert_eq!(
            describe_exit(ExitStatus::from_raw(libc::SIGABRT)),
            " (signal 6)"
        );
        assert_eq!(
            describe_exit(ExitStatus::from_raw(2 << 8)),
            " (exit code 2)"
        );
    }
}
