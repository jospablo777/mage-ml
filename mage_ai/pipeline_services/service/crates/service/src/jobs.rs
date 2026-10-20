//! `mage-service jobs`: warm Python workers for the jobs of MageML's scheduler.
//!
//! The scheduler starts this command as its child and talks to it with one JSON object per
//! line: requests on stdin, events on stdout. A job is a pickled call (the target and its
//! arguments, as MageML's process queue passes them) that a Python job worker
//! (`mage_ai.orchestration.queue.engine_worker`) runs. Workers are started ahead of the
//! jobs, with MageML imported, so a job pays no interpreter start-up or imports; this was
//! most of a scheduled run (docs/development/run-engine.md).
//!
//! Requests: `{"op":"run","job_id":..,"payload":..}`, `{"op":"kill","job_id":..}`,
//! `{"op":"shutdown"}`. Events: `{"event":"ready"}`, `{"event":"started","job_id":..,
//! "pid":..}`, `{"event":"alias","job_id":..,"alias":..}` (a stage registers the next block
//! run it runs), `{"event":"finished","job_id":..,"ok":..,"error":..}`. The command exits
//! when stdin closes, as when the scheduler dies, and its workers with it.
//!
//! A worker serves one job at a time. It is replaced after `max_jobs` jobs, when it asks to
//! retire (threads left running, memory above the limit), and when its job is killed or
//! crashes; a killed job's process group goes with it.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::process::CommandExt as _;
use std::process::{ExitCode, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::python::{describe_exit, kill_group, pipe};

#[derive(clap::Args, Clone, Debug)]
pub struct JobsArgs {
    /// The Python that runs the job workers: MageML's.
    #[arg(long, env = "MAGE_JOBS_PYTHON", default_value = "python3")]
    pub python: String,
    /// The job worker module.
    #[arg(
        long,
        env = "MAGE_JOBS_WORKER_MODULE",
        default_value = "mage_ai.orchestration.queue.engine_worker"
    )]
    pub module: String,
    /// Jobs that run at the same time, at most.
    #[arg(long, env = "MAGE_JOBS_WORKERS", default_value_t = 4)]
    pub workers: usize,
    /// Workers kept started and idle, ready for the next job.
    #[arg(long, env = "MAGE_JOBS_WARM", default_value_t = 1)]
    pub warm: usize,
    /// Jobs a worker runs before it is replaced.
    #[arg(long, env = "MAGE_JOBS_MAX_PER_WORKER", default_value_t = 50)]
    pub max_jobs: u32,
    /// Seconds an idle worker beyond the warm ones waits for a job before it ends: a burst
    /// of jobs does not leave its workers holding memory.
    #[arg(long, env = "MAGE_JOBS_IDLE_SECONDS", default_value_t = 60)]
    pub idle_seconds: u64,
}

/// How long a worker may take to import MageML and say it is ready.
const START_WITHIN: Duration = Duration::from_secs(180);

struct JobWorker {
    child: Child,
    pid: u32,
    requests: tokio::fs::File,
    replies: mpsc::Receiver<String>,
    jobs_served: u32,
}

/// What a job did, sent back to the main loop by the task that ran it.
struct Finished {
    job_id: String,
    ok: bool,
    error: Option<String>,
    /// The worker, when it can run another job.
    worker: Option<JobWorker>,
}

#[derive(Debug)]
enum Request {
    Run { job_id: String, payload: Value },
    Kill { job_id: String },
    Shutdown,
}

fn parse_request(line: &str) -> Result<Request, String> {
    let value: Value =
        serde_json::from_str(line).map_err(|e| format!("not a JSON request ({e}): {line}"))?;
    let job_id = || {
        value
            .get("job_id")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| format!("a request without job_id: {line}"))
    };
    match value.get("op").and_then(Value::as_str) {
        Some("run") => Ok(Request::Run {
            job_id: job_id()?,
            payload: value.get("payload").cloned().unwrap_or(Value::Null),
        }),
        Some("kill") => Ok(Request::Kill { job_id: job_id()? }),
        Some("shutdown") => Ok(Request::Shutdown),
        _ => Err(format!("an unknown request: {line}")),
    }
}

/// What a worker's reply line is: an event during its job, or the job's last word.
#[derive(Debug, PartialEq)]
enum Reply {
    Alias(String),
    Done {
        ok: bool,
        error: Option<String>,
        retire: bool,
    },
}

fn parse_reply(line: &str) -> Option<Reply> {
    let value: Value = serde_json::from_str(line).ok()?;
    if value.get("event").and_then(Value::as_str) == Some("alias") {
        return value
            .get("alias")
            .and_then(Value::as_str)
            .map(|a| Reply::Alias(a.to_string()));
    }
    let ok = value.get("ok")?.as_bool()?;
    Some(Reply::Done {
        ok,
        error: value.get("error").and_then(Value::as_str).map(String::from),
        retire: value
            .get("retire")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// Whether a worker that finished a job runs another.
fn keeps(worker: &JobWorker, retire: bool, max_jobs: u32) -> bool {
    !retire && worker.jobs_served < max_jobs
}

/// How many of the idle workers (idle since these instants, oldest first) end now: those
/// idle longer than `limit`, keeping `warm` of them.
fn idle_to_retire(
    idle_since: &[std::time::Instant],
    warm: usize,
    now: std::time::Instant,
    limit: Duration,
) -> usize {
    let spare = idle_since.len().saturating_sub(warm);
    idle_since
        .iter()
        .take(spare)
        .take_while(|since| now.duration_since(**since) >= limit)
        .count()
}

pub async fn run(args: JobsArgs) -> Result<ExitCode, String> {
    let workers = args.workers.max(1);
    let (events, mut event_lines) = mpsc::unbounded_channel::<Value>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(event) = event_lines.recv().await {
            let mut line = event.to_string();
            line.push('\n');
            if stdout.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            let _ = stdout.flush().await;
        }
    });

    let (requests_tx, mut requests) = mpsc::unbounded_channel::<Option<String>>();
    tokio::spawn(async move {
        let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if requests_tx.send(Some(line)).is_err() {
                return;
            }
        }
        // stdin closed: the scheduler is gone.
        let _ = requests_tx.send(None);
    });

    let (started_tx, mut started) = mpsc::unbounded_channel::<Result<JobWorker, String>>();
    let (finished_tx, mut finished) = mpsc::unbounded_channel::<Finished>();
    let mut queue: VecDeque<(String, Value)> = VecDeque::new();
    // Oldest first; a job takes the most recently used worker, so the others age out.
    let mut idle: Vec<(JobWorker, std::time::Instant)> = Vec::new();
    let idle_limit = Duration::from_secs(args.idle_seconds);
    let mut trim = tokio::time::interval(Duration::from_secs(5));
    trim.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Job id to the pid of the worker running it.
    let mut running: HashMap<String, u32> = HashMap::new();
    // Jobs killed on request, whose end is reported as that, not as a crash.
    let mut killed: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut starting = 0usize;
    let mut start_failures = 0u32;

    let spawn_worker = |starting: &mut usize| {
        *starting += 1;
        let tx = started_tx.clone();
        let (python, module) = (args.python.clone(), args.module.clone());
        tokio::spawn(async move {
            let _ = tx.send(start_worker(&python, &module).await);
        });
    };
    for _ in 0..args.warm.min(workers) {
        spawn_worker(&mut starting);
    }
    let _ = events.send(json!({"event": "ready", "workers": workers}));

    loop {
        // Jobs go to idle workers; more workers start while jobs wait and there is room,
        // and enough to keep `warm` of them idle.
        while let Some((job_id, payload)) = queue.pop_front() {
            match idle.pop().map(|(worker, _)| worker) {
                Some(worker) => {
                    running.insert(job_id.clone(), worker.pid);
                    let _ = events
                        .send(json!({"event": "started", "job_id": job_id, "pid": worker.pid}));
                    tokio::spawn(run_job(
                        worker,
                        job_id,
                        payload,
                        events.clone(),
                        finished_tx.clone(),
                    ));
                }
                None => {
                    queue.push_front((job_id, payload));
                    break;
                }
            }
        }
        let total = idle.len() + running.len() + starting;
        let wanted = (queue.len() + args.warm.min(workers)).saturating_sub(idle.len() + starting);
        for _ in 0..wanted.min(workers.saturating_sub(total)) {
            if start_failures >= 3 {
                break;
            }
            spawn_worker(&mut starting);
        }

        tokio::select! {
            _ = trim.tick() => {
                let since: Vec<_> = idle.iter().map(|(_, since)| *since).collect();
                let retiring = idle_to_retire(
                    &since,
                    args.warm.min(workers),
                    std::time::Instant::now(),
                    idle_limit,
                );
                for (worker, _) in idle.drain(..retiring) {
                    tokio::spawn(retire(worker));
                }
            },
            request = requests.recv() => match request.flatten() {
                None => break,
                Some(line) => match parse_request(&line) {
                    Ok(Request::Run { job_id, payload }) => {
                        if running.contains_key(&job_id)
                            || queue.iter().any(|(queued, _)| queued == &job_id)
                        {
                            continue;
                        }
                        queue.push_back((job_id, payload));
                    }
                    Ok(Request::Kill { job_id }) => {
                        if let Some(pid) = running.get(&job_id) {
                            killed.insert(job_id.clone());
                            kill_group(*pid);
                        } else if let Some(at) = queue.iter().position(|(q, _)| q == &job_id) {
                            queue.remove(at);
                            let _ = events.send(json!({
                                "event": "finished", "job_id": job_id, "ok": false,
                                "error": "killed before it started",
                            }));
                        }
                    }
                    Ok(Request::Shutdown) => break,
                    Err(message) => crate::log::warn(&message),
                },
            },
            Some(outcome) = started.recv() => {
                starting = starting.saturating_sub(1);
                match outcome {
                    Ok(worker) => {
                        start_failures = 0;
                        idle.push((worker, std::time::Instant::now()));
                    }
                    Err(message) => {
                        start_failures += 1;
                        crate::log::error(&message);
                        if start_failures >= 3 {
                            // Workers cannot start: fail the waiting jobs, so the scheduler
                            // sees them end, and keep failing new ones with the reason.
                            for (job_id, _) in queue.drain(..) {
                                let _ = events.send(json!({
                                    "event": "finished", "job_id": job_id, "ok": false,
                                    "error": message,
                                }));
                            }
                            let _ = events.send(json!({"event": "broken", "error": message}));
                        }
                    }
                }
            },
            Some(done) = finished.recv() => {
                running.remove(&done.job_id);
                let error = if killed.remove(&done.job_id) {
                    Some("killed on request".to_string())
                } else {
                    done.error
                };
                let _ = events.send(json!({
                    "event": "finished", "job_id": done.job_id, "ok": done.ok,
                    "error": error,
                }));
                if let Some(worker) = done.worker {
                    if keeps(&worker, false, args.max_jobs) {
                        idle.push((worker, std::time::Instant::now()));
                    } else {
                        tokio::spawn(retire(worker));
                    }
                }
            },
        }
        if start_failures >= 3 && !queue.is_empty() {
            for (job_id, _) in queue.drain(..) {
                let _ = events.send(json!({
                    "event": "finished", "job_id": job_id, "ok": false,
                    "error": "the job workers cannot start; see the scheduler log",
                }));
            }
        }
    }

    for pid in running.values() {
        kill_group(*pid);
    }
    for (worker, _) in idle {
        retire(worker).await;
    }
    drop(events);
    let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
    Ok(ExitCode::SUCCESS)
}

async fn run_job(
    mut worker: JobWorker,
    job_id: String,
    payload: Value,
    events: mpsc::UnboundedSender<Value>,
    finished: mpsc::UnboundedSender<Finished>,
) {
    let mut line = json!({"op": "run", "id": job_id, "payload": payload}).to_string();
    line.push('\n');
    let wrote = worker.requests.write_all(line.as_bytes()).await.is_ok()
        && worker.requests.flush().await.is_ok();
    let mut outcome = None;
    if wrote {
        while let Some(reply) = worker.replies.recv().await {
            match parse_reply(&reply) {
                Some(Reply::Alias(alias)) => {
                    let _ =
                        events.send(json!({"event": "alias", "job_id": job_id, "alias": alias}));
                }
                Some(Reply::Done { ok, error, retire }) => {
                    outcome = Some((ok, error, retire));
                    break;
                }
                None => crate::log::warn(&format!("A job worker sent an unexpected line: {reply}")),
            }
        }
    }
    let done = match outcome {
        Some((ok, error, retire)) => {
            worker.jobs_served += 1;
            let worker = if retire {
                tokio::spawn(self::retire(worker));
                None
            } else {
                Some(worker)
            };
            Finished {
                job_id,
                ok,
                error,
                worker,
            }
        }
        None => {
            // Killed, or the process ended during the job.
            kill_group(worker.pid);
            let status =
                match tokio::time::timeout(Duration::from_secs(5), worker.child.wait()).await {
                    Ok(Ok(status)) => describe_exit(status),
                    _ => String::new(),
                };
            Finished {
                job_id,
                ok: false,
                error: Some(format!("the job process stopped{status}")),
                worker: None,
            }
        }
    };
    let _ = finished.send(done);
}

async fn start_worker(python: &str, module: &str) -> Result<JobWorker, String> {
    let (reply_read, reply_write) = pipe("reply").map_err(|e| e.to_string())?;
    let (request_read, request_write) = pipe("request").map_err(|e| e.to_string())?;
    let (reply_w, reply_r) = (reply_write.as_raw_fd(), reply_read.as_raw_fd());
    let (request_r, request_w) = (request_read.as_raw_fd(), request_write.as_raw_fd());
    // The worker's prints go to this command's stderr: its stdout carries the events.
    let output = || {
        std::io::stderr()
            .as_fd()
            .try_clone_to_owned()
            .map(Stdio::from)
            .map_err(|e| format!("could not pass stderr to a job worker: {e}"))
    };
    let mut command = std::process::Command::new(python);
    command
        .arg("-u")
        .arg("-m")
        .arg(module)
        .env("PYTHONUNBUFFERED", "1")
        .env("MAGE_WORKER_REQUEST_FD", "4")
        .stdin(Stdio::null())
        .stdout(output()?)
        .stderr(output()?)
        // Its own process group: killing a job kills the processes it started.
        .process_group(0);
    // SAFETY: only async-signal-safe calls between fork and exec.
    unsafe {
        command.pre_exec(move || {
            let reply = libc::fcntl(reply_w, libc::F_DUPFD, 10);
            let request = libc::fcntl(request_r, libc::F_DUPFD, 10);
            if reply < 0 || request < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::dup2(reply, 3) < 0 || libc::dup2(request, 4) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            for fd in [reply, request, reply_w, reply_r, request_r, request_w] {
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
        .map_err(|e| format!("could not start a job worker with {python}: {e}"))?;
    drop(reply_write);
    drop(request_read);
    let requests = tokio::fs::File::from_std(std::fs::File::from(request_write));
    let pid = child.id().unwrap_or(0);
    let (reply_tx, mut replies) = mpsc::channel::<String>(16);
    std::thread::Builder::new()
        .name(format!("job-replies-{pid}"))
        .spawn(move || {
            for line in BufReader::new(std::fs::File::from(reply_read)).lines() {
                let Ok(line) = line else { break };
                if reply_tx.blocking_send(line).is_err() {
                    break;
                }
            }
        })
        .map_err(|e| format!("could not start a reader thread: {e}"))?;
    match tokio::time::timeout(START_WITHIN, replies.recv()).await {
        Ok(Some(line)) if line.contains("\"ready\"") => Ok(JobWorker {
            child,
            pid,
            requests,
            replies,
            jobs_served: 0,
        }),
        Ok(Some(line)) => {
            kill_group(pid);
            Err(format!(
                "a job worker started with an unexpected message: {line}"
            ))
        }
        Ok(None) => {
            let status = child.wait().await.map(describe_exit).unwrap_or_default();
            Err(format!(
                "a job worker ({python} -m {module}) exited at start{status}; its output is above"
            ))
        }
        Err(_) => {
            kill_group(pid);
            Err(format!(
                "a job worker did not start within {} seconds",
                START_WITHIN.as_secs()
            ))
        }
    }
}

async fn retire(mut worker: JobWorker) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_are_read_and_bad_ones_named() {
        match parse_request(r#"{"op":"run","job_id":"block_run_3","payload":{"a":1}}"#) {
            Ok(Request::Run { job_id, payload }) => {
                assert_eq!(job_id, "block_run_3");
                assert_eq!(payload, json!({"a": 1}));
            }
            _ => panic!("not a run request"),
        }
        assert!(matches!(
            parse_request(r#"{"op":"kill","job_id":"x"}"#),
            Ok(Request::Kill { .. })
        ));
        assert!(matches!(
            parse_request(r#"{"op":"shutdown"}"#),
            Ok(Request::Shutdown)
        ));
        assert!(
            parse_request(r#"{"op":"run"}"#)
                .unwrap_err()
                .contains("job_id")
        );
        assert!(
            parse_request("not json")
                .unwrap_err()
                .contains("not a JSON")
        );
    }

    #[test]
    fn idle_workers_past_the_limit_end_and_the_warm_ones_stay() {
        let now = std::time::Instant::now();
        let ago = |s| now - Duration::from_secs(s);
        let limit = Duration::from_secs(60);
        // Five idle, oldest first; two stay warm whatever their age.
        let idle = [ago(300), ago(120), ago(61), ago(10), ago(1)];
        assert_eq!(idle_to_retire(&idle, 2, now, limit), 3);
        assert_eq!(idle_to_retire(&idle, 4, now, limit), 1);
        assert_eq!(idle_to_retire(&idle, 5, now, limit), 0);
        // A recent one ahead of an old one stops the count: workers age in order.
        assert_eq!(idle_to_retire(&[ago(5), ago(300)], 0, now, limit), 0);
        assert_eq!(idle_to_retire(&[], 2, now, limit), 0);
    }

    #[test]
    fn replies_are_aliases_or_the_last_word() {
        assert_eq!(
            parse_reply(r#"{"event":"alias","alias":"block_run_4"}"#),
            Some(Reply::Alias("block_run_4".into()))
        );
        assert_eq!(
            parse_reply(r#"{"id":"j","ok":false,"error":"boom","retire":true}"#),
            Some(Reply::Done {
                ok: false,
                error: Some("boom".into()),
                retire: true
            })
        );
        assert_eq!(
            parse_reply(r#"{"id":"j","ok":true}"#),
            Some(Reply::Done {
                ok: true,
                error: None,
                retire: false
            })
        );
        assert_eq!(parse_reply("{}"), None);
    }
}
