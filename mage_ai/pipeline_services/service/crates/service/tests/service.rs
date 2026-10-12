//! The service binary end to end: a real process with Python workers, driven over HTTP.
//!
//! Blocks here use only Python's standard library, so the tests need `python3` and nothing
//! else. `MAGE_SERVICE_TEST_PYTHON` picks another interpreter.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const TOKEN: &str = "test-token-0123456789";
const READ_TOKEN: &str = "read-token-0123456789";

fn worker() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../runtime/worker.py")
}

fn python() -> String {
    std::env::var("MAGE_SERVICE_TEST_PYTHON").unwrap_or_else(|_| "python3".into())
}

/// The port in the service's "serves ... on http://host:port" line.
fn listening_port(log: &str) -> Option<u16> {
    let line = log.lines().find(|l| l.contains(" serves "))?;
    let address = line.split("on http://").nth(1)?.split_whitespace().next()?;
    address.rsplit(':').next()?.parse().ok()
}

struct Service {
    dir: tempfile::TempDir,
    port: u16,
    child: Option<Child>,
}

const BLOCKS: &[(&str, &str)] = &[
    (
        "data_loaders/load.py",
        "@data_loader\ndef load(*args, **kwargs):\n    print('loading', kwargs.get('n', 3))\n    return {'values': list(range(int(kwargs.get('n', 3))))}\n",
    ),
    (
        "transformers/total.py",
        "@transformer\ndef total(data, *args, **kwargs):\n    return {'total': sum(data['values'])}\n\n@test\ndef positive(output, *args):\n    assert output['total'] >= 0\n",
    ),
    (
        "transformers/flaky.py",
        "import os\n\n@transformer\ndef flaky(data, *args, **kwargs):\n    marker = os.path.join(kwargs['marker_dir'], 'attempts')\n    n = int(open(marker).read()) + 1 if os.path.exists(marker) else 1\n    open(marker, 'w').write(str(n))\n    if n < 3:\n        raise ValueError(f'attempt {n} fails')\n    return n\n",
    ),
    (
        "transformers/broken.py",
        "@transformer\ndef broken(data, *args, **kwargs):\n    raise RuntimeError('this block always fails')\n",
    ),
    (
        "transformers/after_broken.py",
        "@transformer\ndef after(data, *args, **kwargs):\n    return data\n",
    ),
    (
        "data_loaders/thing.py",
        "@data_loader\ndef thing(*args, **kwargs):\n    return {1, 2}\n",
    ),
    (
        "conditionals/gate.py",
        "@condition\ndef evaluate_condition(data, *args, **kwargs):\n    return kwargs.get('gate') == 'open'\n",
    ),
    (
        "transformers/slow.py",
        "import os, time\n\n@transformer\ndef slow(data, *args, **kwargs):\n    open(os.path.join(kwargs['marker_dir'], 'pid'), 'w').write(str(os.getpid()))\n    time.sleep(float(kwargs.get('seconds', 30)))\n    return 1\n",
    ),
];

fn manifest(once_trigger: bool) -> Value {
    let block = |uuid: &str, kind: &str, file: &str, upstream: &[&str]| {
        json!({"uuid": uuid, "type": kind, "language": "python", "file": file, "sha256": "0",
               "upstream": upstream})
    };
    let mut flaky = block("flaky", "transformer", "transformers/flaky.py", &["load"]);
    flaky["retry"] = json!({"retries": 2, "delay_seconds": 0.1, "max_delay_seconds": 1.0,
                            "exponential_backoff": false});
    let triggers = if once_trigger {
        json!([{"name": "first", "kind": "time", "schedule": "@once", "active": true}])
    } else {
        json!([])
    };
    json!({
        "schema_version": 1,
        "service": {"name": "test-service", "project": "proj", "exported_at": "2026-10-10T00:00:00Z",
                    "mage_version": "test", "source_sha256": "abc"},
        "pipelines": [
            {"uuid": "sum", "name": "sum", "max_concurrent_runs": 2, "variables": {"n": 4},
             "blocks": [block("load", "data_loader", "data_loaders/load.py", &[]),
                        block("total", "transformer", "transformers/total.py", &["load"])],
             "triggers": triggers},
            {"uuid": "retry", "name": "retry", "max_concurrent_runs": 1,
             "blocks": [block("load", "data_loader", "data_loaders/load.py", &[]), flaky]},
            {"uuid": "fails", "name": "fails", "max_concurrent_runs": 1,
             "blocks": [block("load", "data_loader", "data_loaders/load.py", &[]),
                        block("broken", "transformer", "transformers/broken.py", &["load"]),
                        block("after", "transformer", "transformers/after_broken.py", &["broken"])]},
            {"uuid": "thing", "name": "thing", "max_concurrent_runs": 1,
             "blocks": [block("thing", "data_loader", "data_loaders/thing.py", &[])]},
            {"uuid": "slow", "name": "slow", "max_concurrent_runs": 1,
             "blocks": [block("load", "data_loader", "data_loaders/load.py", &[]),
                        block("slow", "transformer", "transformers/slow.py", &["load"])]},
            {"uuid": "follow", "name": "follow", "max_concurrent_runs": 1,
             "blocks": [block("load", "data_loader", "data_loaders/load.py", &[])],
             "triggers": [{"name": "after_thing", "kind": "after_run", "active": true,
                           "after_pipeline": "thing", "variables": {"n": 2}}]},
            {"uuid": "gated", "name": "gated", "max_concurrent_runs": 1,
             "blocks": [block("load", "data_loader", "data_loaders/load.py", &[]),
                        {"uuid": "rusty", "type": "transformer", "language": "rust",
                         "file": "transformers/rusty.rs", "sha256": "0", "upstream": ["load"],
                         "binary": "bin/always-fails", "conditions": ["gate"]},
                        block("after", "transformer", "transformers/after_broken.py", &["rusty"])],
             "conditionals": [{"uuid": "gate", "file": "conditionals/gate.py", "sha256": "0"}]},
            {"uuid": "on_failure", "name": "on_failure", "max_concurrent_runs": 1,
             "blocks": [block("load", "data_loader", "data_loaders/load.py", &[])],
             "triggers": [{"name": "after_fails", "kind": "after_run", "active": true,
                           "after_pipeline": "fails", "after_status": "failed"}]}
        ]
    })
}

impl Service {
    fn create(once_trigger: bool) -> Service {
        let dir = tempfile::tempdir().unwrap();
        for (file, code) in BLOCKS {
            let path = dir.path().join("proj").join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, code).unwrap();
        }
        std::fs::write(
            dir.path().join("service.json"),
            manifest(once_trigger).to_string(),
        )
        .unwrap();
        // A Rust block's program that fails whenever it starts: a skipped block never does.
        std::fs::write(dir.path().join("proj/transformers/rusty.rs"), "").unwrap();
        let program = dir.path().join("bin/always-fails");
        std::fs::create_dir_all(program.parent().unwrap()).unwrap();
        std::fs::write(&program, "#!/bin/sh\necho 'the block ran' >&2\nexit 3\n").unwrap();
        std::fs::set_permissions(
            &program,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();
        Service {
            dir,
            port: 0,
            child: None,
        }
    }

    fn start(&mut self, extra: &[(&str, &str)]) {
        let log_path = self.dir.path().join("service.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_mage-service"));
        command
            .arg("serve")
            .env("MAGE_SERVICE_DIR", self.dir.path())
            .env("MAGE_SERVICE_WORKER", worker())
            .env("MAGE_SERVICE_PYTHON", python())
            // The service picks a free port and logs it: a port chosen here could be taken
            // before the service binds it.
            .env("MAGE_SERVICE_PORT", "0")
            .env("MAGE_SERVICE_TOKEN", TOKEN)
            .env("MAGE_SERVICE_READ_TOKEN", READ_TOKEN)
            .env("MAGE_SERVICE_SHUTDOWN_SECONDS", "2")
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        for (key, value) in extra {
            command.env(key, value);
        }
        self.child = Some(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let text = std::fs::read_to_string(&log_path).unwrap_or_default();
            if let Some(port) = listening_port(&text) {
                self.port = port;
                if let Ok((200, _)) = self.request("GET", "/healthz", None, None) {
                    return;
                }
            }
            if self.child.as_mut().unwrap().try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let exited = self.child.as_mut().unwrap().try_wait().unwrap();
        let text = std::fs::read_to_string(&log_path).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        let tail = lines[lines.len().saturating_sub(30)..].join("\n");
        panic!("the service did not start (exited: {exited:?}):\n{tail}");
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> std::io::Result<(u16, Value)> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port))?;
        stream.set_read_timeout(Some(Duration::from_secs(120)))?;
        let body = body.map(Value::to_string).unwrap_or_default();
        let auth = token
            .map(|t| format!("Authorization: Bearer {t}\r\n"))
            .unwrap_or_default();
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        let status = response[9..12].parse().unwrap_or(0);
        let text = response
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .unwrap_or("");
        Ok((
            status,
            serde_json::from_str(text).unwrap_or(Value::String(text.into())),
        ))
    }

    fn post(&self, path: &str, body: Value) -> (u16, Value) {
        self.request("POST", path, Some(TOKEN), Some(&body))
            .unwrap()
    }

    fn get(&self, path: &str) -> (u16, Value) {
        self.request("GET", path, Some(TOKEN), None).unwrap()
    }

    fn signal(&mut self, signal: &str) {
        let pid = self.child.as_ref().unwrap().id().to_string();
        Command::new("kill").args([signal, &pid]).status().unwrap();
        let child = self.child.as_mut().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                self.child = None;
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("the service did not stop after {signal}");
    }

    fn markers(&self) -> PathBuf {
        let path = self.dir.path().join("markers");
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_status(service: &Service, id: &str, wanted: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (_, run) = service.get(&format!("/v1/runs/{id}"));
        if run["status"] == wanted {
            return run;
        }
        assert!(
            Instant::now() < deadline,
            "run {id} never became {wanted}: {run}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn runs_complete_and_return_their_blocks_when_waited_for() {
    let mut service = Service::create(false);
    service.start(&[]);

    let (status, run) = service.post(
        "/v1/pipelines/sum/runs?wait=60",
        json!({"variables": {"n": 5}}),
    );
    assert_eq!(status, 201, "{run}");
    assert_eq!(run["status"], "completed", "{run}");
    let blocks: Vec<_> = run["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["status"].clone())
        .collect();
    assert_eq!(blocks, vec!["completed", "completed"]);
    let output = &run["blocks"][1]["outputs"][0];
    assert_eq!(output["kind"], "json");
    let total: Value =
        serde_json::from_str(&std::fs::read_to_string(output["path"].as_str().unwrap()).unwrap())
            .unwrap();
    assert_eq!(total, json!({"total": 10}));

    // Logs keep what the blocks printed, per block.
    let (_, logs) = service.get(&format!(
        "/v1/runs/{}/logs?block=load",
        run["id"].as_str().unwrap()
    ));
    let lines: Vec<_> = logs["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["message"].as_str().unwrap().to_string())
        .collect();
    assert!(lines.iter().any(|l| l == "loading 5"), "{lines:?}");

    // The same idempotency key returns the first run.
    let (first_status, first) = service.post(
        "/v1/pipelines/sum/runs?wait=60",
        json!({"idempotency_key": "k1"}),
    );
    let (again_status, again) =
        service.post("/v1/pipelines/sum/runs", json!({"idempotency_key": "k1"}));
    assert_eq!((first_status, again_status), (201, 200));
    assert_eq!(first["id"], again["id"]);

    let (status, _) = service.post("/v1/pipelines/missing/runs", json!({}));
    assert_eq!(status, 404);
}

#[test]
fn a_run_answers_with_a_blocks_output() {
    let mut service = Service::create(false);
    service.start(&[]);

    let (status, run) = service.post(
        "/v1/pipelines/sum/runs?wait=60&output=total",
        json!({"variables": {"n": 5}}),
    );
    assert_eq!(status, 201, "{run}");
    assert_eq!(run["output"], json!({"total": 10}), "{run}");
    assert!(run.get("output_url").is_none());

    let id = run["id"].as_str().unwrap();
    assert_eq!(
        service.get(&format!("/v1/runs/{id}/output?block=total")),
        (200, json!({"total": 10}))
    );
    let (status, error) = service.get(&format!("/v1/runs/{id}/output?block=nope"));
    assert_eq!(status, 404);
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("has no block nope"),
        "{error}"
    );
    let (status, error) = service.get(&format!("/v1/runs/{id}/output?block=total&index=3"));
    assert_eq!(status, 404);
    assert!(
        error["error"].as_str().unwrap().contains("has no output 3"),
        "{error}"
    );

    // A pickled output is not served: the response points at it and the URL says why.
    let (_, run) = service.post("/v1/pipelines/thing/runs?wait=60&output=thing", json!({}));
    assert!(run.get("output").is_none(), "{run}");
    let url = run["output_url"].as_str().unwrap().to_string();
    let (status, error) = service.get(&url);
    assert_eq!(status, 415);
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("only Python can read"),
        "{error}"
    );
}

#[test]
fn tokens_separate_reading_from_running() {
    let mut service = Service::create(false);
    service.start(&[]);

    let (status, _) = service.request("GET", "/v1/runs", None, None).unwrap();
    assert_eq!(status, 401);
    let (status, _) = service
        .request("GET", "/v1/runs", Some(READ_TOKEN), None)
        .unwrap();
    assert_eq!(status, 200);
    let (status, body) = service
        .request(
            "POST",
            "/v1/pipelines/sum/runs",
            Some(READ_TOKEN),
            Some(&json!({})),
        )
        .unwrap();
    assert_eq!(status, 403, "{body}");
    let (status, _) = service
        .request("GET", "/v1/runs", Some("wrong-token"), None)
        .unwrap();
    assert_eq!(status, 401);
    // Health and metrics carry no data and need no token.
    let (status, _) = service.request("GET", "/healthz", None, None).unwrap();
    assert_eq!(status, 200);
    let (status, metrics) = service.request("GET", "/metrics", None, None).unwrap();
    assert_eq!(status, 200);
    assert!(metrics.as_str().unwrap().contains("mage_service_up 1"));
}

#[test]
fn retries_recover_and_failures_stop_downstream_blocks() {
    let mut service = Service::create(false);
    service.start(&[]);
    let markers = service.markers();

    let (_, run) = service.post(
        "/v1/pipelines/retry/runs?wait=60",
        json!({"variables": {"marker_dir": markers}}),
    );
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(run["blocks"][1]["attempts"], 3);

    let (_, run) = service.post("/v1/pipelines/fails/runs?wait=60", json!({}));
    assert_eq!(run["status"], "failed");
    assert!(
        run["error"]
            .as_str()
            .unwrap()
            .contains("Block broken failed"),
        "{run}"
    );
    let statuses: Vec<_> = run["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["status"].clone())
        .collect();
    assert_eq!(statuses, vec!["completed", "failed", "upstream_failed"]);
    assert!(
        run["blocks"][1]["error"]
            .as_str()
            .unwrap()
            .contains("this block always fails")
    );

    let (_, metrics) = service.request("GET", "/metrics", None, None).unwrap();
    assert!(
        metrics
            .as_str()
            .unwrap()
            .contains("mage_service_runs{pipeline=\"fails\",status=\"failed\"} 1")
    );
}

#[test]
fn a_cancelled_run_stops_its_block_process() {
    let mut service = Service::create(false);
    service.start(&[]);
    let markers = service.markers();

    let (_, run) = service.post(
        "/v1/pipelines/slow/runs",
        json!({"variables": {"marker_dir": markers, "seconds": 60}}),
    );
    let id = run["id"].as_str().unwrap().to_string();
    let pid_file = markers.join("pid");
    let deadline = Instant::now() + Duration::from_secs(60);
    while !pid_file.exists() {
        assert!(Instant::now() < deadline, "the slow block never started");
        std::thread::sleep(Duration::from_millis(100));
    }
    let pid = std::fs::read_to_string(&pid_file).unwrap();

    let (status, body) = service.post(&format!("/v1/runs/{id}/cancel"), json!({}));
    assert_eq!((status, body["cancelling"].clone()), (200, json!(true)));
    let run = wait_status(&service, &id, "cancelled");
    assert_eq!(run["blocks"][1]["status"], "cancelled", "{run}");
    let alive = Command::new("kill")
        .args(["-0", pid.trim()])
        .status()
        .unwrap()
        .success();
    assert!(!alive, "the block's worker process is still running");

    // The service runs the next run normally.
    let (_, run) = service.post("/v1/pipelines/sum/runs?wait=60", json!({}));
    assert_eq!(run["status"], "completed");
}

#[test]
fn a_killed_service_marks_its_running_run_failed_when_it_starts_again() {
    let mut service = Service::create(false);
    service.start(&[]);
    let markers = service.markers();
    let (_, run) = service.post(
        "/v1/pipelines/slow/runs",
        json!({"variables": {"marker_dir": markers, "seconds": 60}}),
    );
    let id = run["id"].as_str().unwrap().to_string();
    wait_status(&service, &id, "running");

    service.signal("-KILL");
    service.start(&[]);

    let (_, run) = service.get(&format!("/v1/runs/{id}"));
    assert_eq!(run["status"], "failed");
    assert!(
        run["error"]
            .as_str()
            .unwrap()
            .contains("stopped while this run was running")
    );
}

#[test]
fn sigterm_lets_a_short_run_finish_and_stops_a_long_one() {
    let mut service = Service::create(false);
    service.start(&[]);
    let markers = service.markers();
    let (_, run) = service.post(
        "/v1/pipelines/slow/runs",
        json!({"variables": {"marker_dir": markers, "seconds": 0.5}}),
    );
    let id = run["id"].as_str().unwrap().to_string();
    wait_status(&service, &id, "running");
    service.signal("-TERM");
    service.start(&[]);
    let (_, run) = service.get(&format!("/v1/runs/{id}"));
    assert_eq!(run["status"], "completed", "{run}");

    let (_, run) = service.post(
        "/v1/pipelines/slow/runs",
        json!({"variables": {"marker_dir": markers, "seconds": 60}}),
    );
    let id = run["id"].as_str().unwrap().to_string();
    wait_status(&service, &id, "running");
    let started = Instant::now();
    // MAGE_SERVICE_SHUTDOWN_SECONDS is 2.
    service.signal("-TERM");
    assert!(started.elapsed() < Duration::from_secs(20));
    service.start(&[]);
    let (_, run) = service.get(&format!("/v1/runs/{id}"));
    assert_eq!(run["status"], "cancelled", "{run}");
}

#[test]
fn a_condition_skips_its_block_and_the_blocks_after_it() {
    let mut service = Service::create(false);
    service.start(&[]);

    let (_, run) = service.post(
        "/v1/pipelines/gated/runs?wait=60",
        json!({"variables": {"gate": "closed"}}),
    );
    assert_eq!(run["status"], "completed", "{run}");
    let statuses: Vec<_> = run["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| (b["block"].clone(), b["status"].clone(), b["error"].clone()))
        .collect();
    assert_eq!(
        statuses,
        vec![
            (json!("load"), json!("completed"), Value::Null),
            (
                json!("rusty"),
                json!("condition_failed"),
                json!("Condition gate returned False.")
            ),
            (
                json!("after"),
                json!("condition_failed"),
                json!("Upstream block rusty was skipped by a condition.")
            ),
        ]
    );

    // With the gate open the Rust block's program runs, and this one fails.
    let (_, run) = service.post(
        "/v1/pipelines/gated/runs?wait=60",
        json!({"variables": {"gate": "open"}}),
    );
    assert_eq!(run["status"], "failed", "{run}");
    assert_eq!(run["blocks"][1]["status"], "failed", "{run}");
}

fn runs_of(service: &Service, query: &str) -> Vec<Value> {
    let (_, runs) = service.get(&format!("/v1/runs?{query}"));
    runs["runs"].as_array().cloned().unwrap_or_default()
}

fn wait_for_runs(service: &Service, query: &str, count: usize) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let runs = runs_of(service, query);
        if runs.len() >= count {
            return runs;
        }
        assert!(
            Instant::now() < deadline,
            "no {count} run(s) for {query}: {runs:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn a_finished_run_starts_the_pipelines_that_follow_it() {
    let mut service = Service::create(false);
    service.start(&[]);

    let (status, run) = service.post("/v1/pipelines/thing/runs?wait=60", json!({}));
    assert_eq!(status, 201, "{run}");
    assert_eq!(run["status"], "completed");
    let followed = wait_for_runs(&service, "pipeline=follow&status=completed", 1);
    assert_eq!(followed[0]["source"], "after:thing");
    let (_, detail) = service.get(&format!("/v1/runs/{}", followed[0]["id"].as_str().unwrap()));
    assert_eq!(detail["variables"]["n"], 2, "{detail}");
    // As in MageML, the run knows which run started it.
    assert_eq!(
        detail["variables"]["upstream_pipeline"], "thing",
        "{detail}"
    );
    assert_eq!(
        detail["variables"]["upstream_pipeline_run_id"], run["id"],
        "{detail}"
    );
    assert_eq!(
        detail["variables"]["upstream_status"], "completed",
        "{detail}"
    );

    // A failed run starts only the pipeline that follows failures.
    let (_, failed) = service.post("/v1/pipelines/fails/runs?wait=60", json!({}));
    assert_eq!(failed["status"], "failed");
    let after_failure = wait_for_runs(&service, "pipeline=on_failure", 1);
    assert_eq!(after_failure[0]["source"], "after:fails");
    let (_, detail) = service.get(&format!(
        "/v1/runs/{}",
        after_failure[0]["id"].as_str().unwrap()
    ));
    assert_eq!(detail["variables"]["upstream_status"], "failed", "{detail}");
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(
        runs_of(&service, "pipeline=follow").len(),
        1,
        "a failure started follow"
    );

    let (_, pipelines) = service.get("/v1/pipelines");
    let follow = pipelines["pipelines"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == "follow")
        .unwrap()
        .clone();
    assert_eq!(follow["triggers"][0]["kind"], "after_run", "{follow}");
    assert_eq!(follow["triggers"][0]["schedule"], "after thing completes");
}

#[test]
fn a_once_trigger_runs_once_when_schedules_are_on() {
    let mut service = Service::create(true);
    service.start(&[]);
    std::thread::sleep(Duration::from_secs(2));
    let (_, runs) = service.get("/v1/runs?pipeline=sum");
    assert_eq!(
        runs["runs"].as_array().unwrap().len(),
        0,
        "schedules are off by default"
    );
    service.signal("-TERM");

    service.start(&[("MAGE_SERVICE_SCHEDULES", "on")]);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (_, runs) = service.get("/v1/runs?pipeline=sum&status=completed");
        if runs["runs"].as_array().unwrap().len() == 1 {
            assert_eq!(runs["runs"][0]["source"], "once:first");
            break;
        }
        assert!(Instant::now() < deadline, "the once trigger did not run");
        std::thread::sleep(Duration::from_millis(200));
    }
    service.signal("-TERM");
    service.start(&[("MAGE_SERVICE_SCHEDULES", "on")]);
    std::thread::sleep(Duration::from_secs(3));
    let (_, runs) = service.get("/v1/runs?pipeline=sum");
    assert_eq!(
        runs["runs"].as_array().unwrap().len(),
        1,
        "a once trigger ran twice"
    );
}
