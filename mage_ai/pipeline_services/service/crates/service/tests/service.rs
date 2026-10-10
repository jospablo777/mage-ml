//! The service binary end to end: a real process with Python workers, driven over HTTP.
//!
//! Blocks here use only Python's standard library, so the tests need `python3` and nothing
//! else. `MAGE_SERVICE_TEST_PYTHON` picks another interpreter.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
            {"uuid": "slow", "name": "slow", "max_concurrent_runs": 1,
             "blocks": [block("load", "data_loader", "data_loaders/load.py", &[]),
                        block("slow", "transformer", "transformers/slow.py", &["load"])]}
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
        Service {
            dir,
            port: free_port(),
            child: None,
        }
    }

    fn start(&mut self, extra: &[(&str, &str)]) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mage-service"));
        command
            .arg("serve")
            .env("MAGE_SERVICE_DIR", self.dir.path())
            .env("MAGE_SERVICE_WORKER", worker())
            .env("MAGE_SERVICE_PYTHON", python())
            .env("MAGE_SERVICE_PORT", self.port.to_string())
            .env("MAGE_SERVICE_TOKEN", TOKEN)
            .env("MAGE_SERVICE_READ_TOKEN", READ_TOKEN)
            .env("MAGE_SERVICE_SHUTDOWN_SECONDS", "2")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for (key, value) in extra {
            command.env(key, value);
        }
        self.child = Some(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if let Ok((200, _)) = self.request("GET", "/healthz", None, None) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("the service did not start");
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
