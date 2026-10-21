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
        "data_loaders/environ.py",
        "import os\n\n@data_loader\ndef environ(*args, **kwargs):\n    return {'names': sorted(os.environ)}\n",
    ),
    (
        "data_loaders/probe.py",
        "import os\n\n@data_loader\ndef probe(*args, **kwargs):\n    try:\n        input()\n        stdin = 'read'\n    except EOFError:\n        stdin = 'eof'\n    status = os.system('test -e /dev/fd/3 && exit 3; test -e /dev/fd/4 && exit 4; exit 0')\n    return {'stdin': stdin, 'child': os.waitstatus_to_exitcode(status)}\n",
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
            {"uuid": "environ", "name": "environ", "max_concurrent_runs": 1,
             "blocks": [block("environ", "data_loader", "data_loaders/environ.py", &[])]},
            {"uuid": "probe", "name": "probe", "max_concurrent_runs": 1,
             "blocks": [block("probe", "data_loader", "data_loaders/probe.py", &[])]},
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
    /// A service with its own manifest and block files.
    fn with_manifest(manifest: &Value, files: &[(&str, &str)]) -> Service {
        let dir = tempfile::tempdir().unwrap();
        for (file, code) in files {
            let path = dir.path().join("proj").join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, code).unwrap();
        }
        std::fs::write(dir.path().join("service.json"), manifest.to_string()).unwrap();
        Service {
            dir,
            port: 0,
            child: None,
        }
    }

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
        self.request_text(
            method,
            path,
            token,
            &body.map(Value::to_string).unwrap_or_default(),
        )
    }

    /// A request with the body as written, for JSON a `Value` cannot hold.
    fn request_text(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: &str,
    ) -> std::io::Result<(u16, Value)> {
        self.request_with(method, path, token, body, &[])
    }

    fn request_with(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: &str,
        headers: &[(&str, &str)],
    ) -> std::io::Result<(u16, Value)> {
        let extra: String = headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect();
        let mut stream = TcpStream::connect(("127.0.0.1", self.port))?;
        stream.set_read_timeout(Some(Duration::from_secs(120)))?;
        let auth = token
            .map(|t| format!("Authorization: Bearer {t}\r\n"))
            .unwrap_or_default()
            + &extra;
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
fn integers_too_large_for_64_bits_are_refused_not_rounded() {
    let mut service = Service::create(false);
    service.start(&[]);

    let (status, body) = service
        .request_text(
            "POST",
            "/v1/pipelines/sum/runs",
            Some(TOKEN),
            r#"{"variables": {"customer": 123456789012345678901234567890}}"#,
        )
        .unwrap();

    assert_eq!(status, 400, "{body}");
    assert!(
        body.to_string().contains("123456789012345678901234567890"),
        "{body}"
    );
    // The largest 64-bit integers pass.
    let (status, _) = service
        .request_text(
            "POST",
            "/v1/pipelines/sum/runs",
            Some(TOKEN),
            r#"{"variables": {"customer": 18446744073709551615}}"#,
        )
        .unwrap();
    assert_eq!(status, 201);
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

fn block_environment(service: &Service) -> Vec<String> {
    let (status, run) = service.post(
        "/v1/pipelines/environ/runs?wait=60&output=environ",
        json!({}),
    );
    assert_eq!(status, 201, "{run}");
    serde_json::from_value(run["output"]["names"].clone()).expect("a list of names")
}

#[test]
fn block_code_never_sees_the_services_credentials() {
    let mut service = Service::create(false);
    service.start(&[
        ("IBM_CLOUD_API_KEY", "ibm-secret"),
        ("USER_SETTING", "kept"),
    ]);

    let names = block_environment(&service);

    for hidden in [
        "MAGE_SERVICE_TOKEN",
        "MAGE_SERVICE_READ_TOKEN",
        "IBM_CLOUD_API_KEY",
        "MAGE_SERVICE_DIR",
    ] {
        assert!(
            !names.iter().any(|n| n == hidden),
            "{hidden} reached the block: {names:?}"
        );
    }
    for kept in [
        "USER_SETTING",
        "PATH",
        "MAGE_SERVICE_MODELS_DIR",
        "MAGE_REPO_PATH",
    ] {
        assert!(
            names.iter().any(|n| n == kept),
            "{kept} is missing: {names:?}"
        );
    }
}

#[test]
fn strict_mode_passes_only_system_declared_and_listed_variables() {
    let mut service = Service::create(false);
    service.start(&[
        ("MAGE_SERVICE_WORKER_ENV", "strict"),
        ("MAGE_SERVICE_PASS_ENV", "USER_*"),
        ("USER_SETTING", "kept"),
        ("UNLISTED_SETTING", "dropped"),
    ]);

    let names = block_environment(&service);

    assert!(names.iter().any(|n| n == "USER_SETTING"), "{names:?}");
    assert!(names.iter().any(|n| n == "PATH"), "{names:?}");
    assert!(!names.iter().any(|n| n == "UNLISTED_SETTING"), "{names:?}");
    assert!(
        !names.iter().any(|n| n == "MAGE_SERVICE_TOKEN"),
        "{names:?}"
    );
}

#[test]
fn a_block_cannot_read_or_reach_the_worker_protocol() {
    // Requests came on stdin, so a block calling input() read the next request; the reply
    // descriptor reached programs the block started.
    let mut service = Service::create(false);
    service.start(&[]);

    let (status, run) = service.post("/v1/pipelines/probe/runs?wait=60&output=probe", json!({}));

    assert_eq!(status, 201, "{run}");
    assert_eq!(run["output"], json!({"stdin": "eof", "child": 0}), "{run}");
    // The worker still serves the next run.
    let (status, run) = service.post("/v1/pipelines/sum/runs?wait=60&output=total", json!({}));
    assert_eq!(status, 201, "{run}");
}

const TAG_BLOCK: &str = "@transformer\ndef tag(messages, *args, **kwargs):\n    return [dict(m, tagged=True) for m in messages]\n";

fn webhook_manifest(source_config: Value, max_bytes: usize) -> Value {
    json!({
        "schema_version": 2,
        "service": {"name": "hooks", "project": "proj", "exported_at": "2026-10-13T00:00:00Z",
                    "mage_version": "test", "source_sha256": "abc"},
        "pipelines": [{
            "uuid": "hooks", "name": "hooks", "max_concurrent_runs": 1, "variables": {},
            "blocks": [{"uuid": "tag", "type": "transformer", "language": "python",
                        "file": "transformers/tag.py", "sha256": "0", "upstream": []}],
            "triggers": [],
            "stream": {
                "source": {"name": "hook", "kind": "http", "config": source_config},
                "sinks": [{"name": "out", "kind": "stdout", "after": "tag"}],
                "batch": {"max_records": 100, "max_wait_ms": 100, "max_bytes": max_bytes}
            }
        }]
    })
}

fn wait_for_stream(service: &Service, pipeline: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let (_, streams) = service.get("/v1/streams");
        if streams["streams"].as_array().is_some_and(|list| {
            list.iter()
                .any(|s| s["pipeline"] == pipeline && s["state"] == "running")
        }) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("stream {pipeline} did not start");
}

#[test]
fn a_webhook_is_answered_once_its_messages_are_delivered_and_a_retry_writes_nothing() {
    let mut service = Service::with_manifest(
        &webhook_manifest(json!({}), 1 << 20),
        &[("transformers/tag.py", TAG_BLOCK)],
    );
    service.start(&[("MAGE_SERVICE_INGEST_TOKEN", "ingest-secret")]);
    wait_for_stream(&service, "hooks");
    let send = |token: Option<&str>, key: &str| {
        service
            .request_with(
                "POST",
                "/v1/streams/hooks/messages",
                token,
                r#"[{"id": 1, "big": 123456789012345678901234567890}, {"id": 2}]"#,
                &[("Idempotency-Key", key)],
            )
            .unwrap()
    };

    let (status, body) = send(Some("ingest-secret"), "k1");
    assert_eq!((status, &body["accepted"]), (200, &json!(2)), "{body}");
    let (status, body) = send(Some("ingest-secret"), "k1");
    assert_eq!((status, &body["duplicate"]), (200, &json!(true)), "{body}");

    // The sink wrote each message once, the big integer whole.
    let log = std::fs::read_to_string(service.dir.path().join("service.log")).unwrap();
    assert_eq!(
        log.matches(r#""tagged": true"#).count() + log.matches(r#""tagged":true"#).count(),
        2,
        "{log}"
    );
    assert!(log.contains("123456789012345678901234567890"), "{log}");

    // The ingest token only sends messages; nothing without a token.
    assert_eq!(send(None, "k2").0, 401);
    let (status, _) = service
        .request_text(
            "POST",
            "/v1/pipelines/hooks/runs",
            Some("ingest-secret"),
            "{}",
        )
        .unwrap();
    assert_eq!(status, 403);
}

#[test]
fn a_webhook_checks_its_signature_and_size() {
    let config = json!({"signature": {
        "header": "X-Hub-Signature-256",
        "prefix": "sha256=",
        "secret": {"$parts": [{"env": "WEBHOOK_SECRET"}]}
    }});
    let mut service = Service::with_manifest(
        &webhook_manifest(config, 2048),
        &[("transformers/tag.py", TAG_BLOCK)],
    );
    service.start(&[("WEBHOOK_SECRET", "shh")]);
    wait_for_stream(&service, "hooks");
    let body = r#"{"event": "opened"}"#;
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"shh");
    let signature: String = ring::hmac::sign(&key, body.as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let post = |body: &str, signature: &str| {
        service
            .request_with(
                "POST",
                "/v1/streams/hooks/messages",
                Some(TOKEN),
                body,
                &[("X-Hub-Signature-256", signature)],
            )
            .unwrap()
    };

    assert_eq!(post(body, &format!("sha256={signature}")).0, 200);
    let (status, error) = post(body, "sha256=00");
    assert_eq!(status, 401, "{error}");
    let large = format!("[{}]", vec![r#"{"x": 1}"#; 400].join(","));
    assert_eq!(post(&large, &format!("sha256={signature}")).0, 413);
}

/// Two services on one PostgreSQL run history (`MAGE_TEST_POSTGRES_URL`): each sees the
/// other's runs, a cancel sent to either reaches the run, and the run of an instance that
/// was killed is failed by the other one.
#[test]
fn two_services_share_a_postgresql_run_history() {
    let Ok(url) = std::env::var("MAGE_TEST_POSTGRES_URL") else {
        eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
        return;
    };
    let schema = format!(
        "service_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let env = [
        ("MAGE_SERVICE_LEDGER_URL", url.as_str()),
        ("MAGE_SERVICE_LEDGER_SCHEMA", schema.as_str()),
    ];
    let mut a = Service::create(false);
    a.start(&env);
    let mut b = Service::create(false);
    b.start(&env);

    let (_, run) = a.post("/v1/pipelines/sum/runs?wait=60", json!({}));
    assert_eq!(run["status"], "completed", "{run}");
    let (_, seen) = b.get(&format!("/v1/runs/{}", run["id"].as_str().unwrap()));
    assert_eq!(seen["status"], "completed", "{seen}");

    // Whichever instance claimed it, a cancel sent to b stops it.
    let markers = a.markers();
    let (_, run) = a.post(
        "/v1/pipelines/slow/runs",
        json!({"variables": {"marker_dir": markers, "seconds": 60}}),
    );
    let id = run["id"].as_str().unwrap().to_string();
    wait_status(&a, &id, "running");
    let (status, body) = b.post(&format!("/v1/runs/{id}/cancel"), json!({}));
    assert_eq!((status, body["cancelling"].clone()), (200, json!(true)));
    wait_status(&a, &id, "cancelled");

    // A run on a, which is then killed: b fails it once it sees a's process gone.
    b.signal("-TERM");
    let (_, run) = a.post(
        "/v1/pipelines/slow/runs",
        json!({"variables": {"marker_dir": markers, "seconds": 60}}),
    );
    let id = run["id"].as_str().unwrap().to_string();
    wait_status(&a, &id, "running");
    b.start(&env);
    a.signal("-KILL");
    let run = wait_status(&b, &id, "failed");
    let (status, body) = b.get(&format!("/v1/runs/{id}/logs"));
    assert_eq!(status, 409, "{body}");
    assert!(
        body.to_string()
            .contains("its log is on that instance's data volume"),
        "{body}"
    );
    assert!(
        run["error"]
            .as_str()
            .unwrap()
            .contains("stopped while this run was running"),
        "{run}"
    );
}

/// `mage-service ledger migrate` and `ledger import`: a service moves its SQLite run
/// history into PostgreSQL (`MAGE_TEST_POSTGRES_URL`).
#[test]
fn the_ledger_command_migrates_postgresql_and_imports_a_sqlite_history() {
    let Ok(url) = std::env::var("MAGE_TEST_POSTGRES_URL") else {
        eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
        return;
    };
    let schema = format!(
        "ledger_command_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let mut service = Service::create(false);
    service.start(&[]);
    let (_, run) = service.post("/v1/pipelines/sum/runs?wait=60", json!({}));
    assert_eq!(run["status"], "completed");
    service.signal("-TERM");

    let ledger = |args: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_mage-service"))
            .arg("ledger")
            .args(args)
            .env("MAGE_SERVICE_DIR", service.dir.path())
            .env("MAGE_SERVICE_LEDGER_URL", &url)
            .env("MAGE_SERVICE_LEDGER_SCHEMA", &schema)
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&output.stdout).to_string()
            + &String::from_utf8_lossy(&output.stderr);
        (output.status.success(), text)
    };
    let (ok, text) = ledger(&["migrate"]);
    assert!(ok && text.contains("from version 0 to 1"), "{text}");
    let (ok, text) = ledger(&["migrate"]);
    assert!(ok && text.contains("is at version 1"), "{text}");
    let history = service.dir.path().join("data/runs.db");
    let history = history.to_str().unwrap();
    let (ok, text) = ledger(&["import", history]);
    assert!(ok && text.contains("Copied 1 run(s)"), "{text}");
    let (ok, text) = ledger(&["import", history]);
    assert!(ok && text.contains("Copied 0 run(s)"), "{text}");

    // Served from PostgreSQL, the service has the run it made on SQLite.
    service.start(&[
        ("MAGE_SERVICE_LEDGER_URL", url.as_str()),
        ("MAGE_SERVICE_LEDGER_SCHEMA", schema.as_str()),
    ]);
    let (_, seen) = service.get(&format!("/v1/runs/{}", run["id"].as_str().unwrap()));
    assert_eq!(seen["status"], "completed", "{seen}");
}

/// `mage-service check-connections`: each connector of a stream, read-only; exit 1 when one
/// failed. A PostgreSQL sink with a wrong password fails (with `MAGE_TEST_POSTGRES_URL`).
#[test]
fn check_connections_reports_each_connector_of_a_stream() {
    let mut manifest = webhook_manifest(json!({}), 1 << 20);
    let sinks = &mut manifest["pipelines"][0]["stream"]["sinks"];
    sinks
        .as_array_mut()
        .unwrap()
        .push(json!({"name": "search", "kind": "search", "after": "tag",
                     "config": {"url": "http://localhost:1"}}));
    let service = Service::with_manifest(&manifest, &[("transformers/tag.py", TAG_BLOCK)]);
    let check = |manifest_dir: &Path| {
        let output = Command::new(env!("CARGO_BIN_EXE_mage-service"))
            .args(["check-connections", "--json"])
            .env("MAGE_SERVICE_DIR", manifest_dir)
            .output()
            .unwrap();
        let rows: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
            panic!("{}", String::from_utf8_lossy(&output.stderr));
        });
        (output.status.success(), rows)
    };
    let (ok, rows) = check(service.dir.path());
    assert!(ok, "{rows}");
    let results: Vec<(String, String, String)> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["role"].as_str().unwrap().to_string(),
                r["kind"].as_str().unwrap().to_string(),
                r["result"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        results,
        vec![
            ("source".into(), "http".into(), "passed".into()),
            ("sink 1".into(), "stdout".into(), "passed".into()),
            ("sink 2".into(), "search".into(), "not_checked".into()),
        ]
    );

    let Ok(url) = std::env::var("MAGE_TEST_POSTGRES_URL") else {
        eprintln!("MAGE_TEST_POSTGRES_URL is not set: the PostgreSQL part is skipped");
        return;
    };
    let config: tokio_postgres::Config = url.split('?').next().unwrap().parse().unwrap();
    let host = match &config.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(host) => host.clone(),
        tokio_postgres::config::Host::Unix(path) => path.display().to_string(),
    };
    manifest["pipelines"][0]["stream"]["sinks"]
        .as_array_mut()
        .unwrap()
        .push(
            json!({"name": "pg", "kind": "postgres", "after": "tag", "config": {
            "host": host, "port": config.get_ports()[0], "database": config.get_dbname(),
            "username": config.get_user(), "password": "wrong", "schema": "public",
            "table": "orders", "sslmode": "disable"}}),
        );
    let service = Service::with_manifest(&manifest, &[("transformers/tag.py", TAG_BLOCK)]);
    let (ok, rows) = check(service.dir.path());
    assert!(!ok, "{rows}");
    let pg = &rows[3];
    assert_eq!(
        (pg["kind"].as_str(), pg["result"].as_str()),
        (Some("postgres"), Some("failed"))
    );
    assert!(
        pg["summary"]
            .as_str()
            .unwrap()
            .contains("Could not connect"),
        "{pg}"
    );
}

/// A program that speaks the resident protocol (mage::serve_jobs) as the SDK does: it
/// counts the jobs it served, and fails, panics or sleeps when the run asks.
const RESIDENT_PROGRAM: &str = r#"#!/usr/bin/env python3
import json, os, sys, time

served = 0

def run(job_dir):
    global served
    served += 1
    job = json.load(open(os.path.join(job_dir, 'job.json')))
    variables = job.get('variables', {})
    print(f'job {served} in {os.getpid()}', flush=True)
    if variables.get('sleep'):
        time.sleep(float(variables['sleep']))
    if variables.get('fail') or variables.get('panic'):
        kind = 'panic' if variables.get('panic') else 'error'
        json.dump({'kind': kind, 'message': f'asked to {kind}'},
                  open(os.path.join(job_dir, 'error.json'), 'w'))
        return kind
    json.dump({'served': served, 'pid': os.getpid()}, open(os.path.join(job_dir, 'out.json'), 'w'))
    json.dump({'api_version': 1, 'outputs': [{'kind': 'json', 'path': 'out.json'}], 'tests': []},
              open(os.path.join(job_dir, 'result.json'), 'w'))
    return 'ok'

if len(sys.argv) > 1:
    sys.exit(0 if run(sys.argv[1]) == 'ok' else 1)
for line in sys.stdin:
    outcome = run(json.loads(line)['job_dir'])
    print('\x1emage-job-done', flush=True)
    print('\x1emage-job-done', file=sys.stderr, flush=True)
    if outcome == 'panic':
        sys.exit(1)
"#;

fn resident_service() -> Service {
    let manifest = json!({
        "schema_version": 1,
        "service": {"name": "resident", "project": "proj", "exported_at": "2026-10-21T00:00:00Z",
                    "mage_version": "test", "source_sha256": "abc"},
        "pipelines": [{
            "uuid": "score", "name": "score", "max_concurrent_runs": 2, "variables": {},
            "blocks": [{"uuid": "score", "type": "transformer", "language": "rust",
                        "file": "transformers/score.rs", "sha256": "0", "upstream": [],
                        "binary": "bin/mage-blocks", "entry": "score"}],
            "triggers": []
        }]
    });
    let service = Service::with_manifest(&manifest, &[("transformers/score.rs", "")]);
    let program = service.dir.path().join("bin/mage-blocks");
    std::fs::create_dir_all(program.parent().unwrap()).unwrap();
    std::fs::write(&program, RESIDENT_PROGRAM.replace("python3", &python())).unwrap();
    std::fs::set_permissions(
        &program,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    service
}

fn score(service: &Service, variables: Value) -> Value {
    let (_, run) = service.post(
        "/v1/pipelines/score/runs?wait=60&output=score",
        json!({"variables": variables}),
    );
    run
}

#[test]
fn a_rust_program_stays_resident_between_runs_until_it_panics_or_is_stopped() {
    let mut service = resident_service();
    service.start(&[]);

    let first = score(&service, json!({}));
    let second = score(&service, json!({}));
    assert_eq!(first["status"], "completed", "{first}");
    let pid = first["output"]["pid"].clone();
    assert_eq!(
        (
            second["output"]["served"].clone(),
            second["output"]["pid"].clone()
        ),
        (json!(2), pid.clone())
    );

    // An error fails the run and keeps the program; a panic ends it.
    let failed = score(&service, json!({"fail": true}));
    assert_eq!(failed["status"], "failed");
    assert!(
        failed["error"].as_str().unwrap().contains("asked to error"),
        "{failed}"
    );
    assert_eq!(score(&service, json!({}))["output"]["pid"], pid);
    let panicked = score(&service, json!({"panic": true}));
    assert!(
        panicked["error"]
            .as_str()
            .unwrap()
            .contains("asked to panic"),
        "{panicked}"
    );
    let after = score(&service, json!({}));
    assert_ne!(after["output"]["pid"], pid);
    assert_eq!(after["output"]["served"], 1);

    // Each job's lines are in its own run's log.
    let (_, logs) = service.get(&format!("/v1/runs/{}/logs", after["id"].as_str().unwrap()));
    let text = logs.to_string();
    assert!(text.contains("job 1 in"), "{text}");
    assert!(!text.contains("job 2 in"), "{text}");

    // A cancelled job takes its program with it.
    let (_, slow) = service.post(
        "/v1/pipelines/score/runs",
        json!({"variables": {"sleep": 30}}),
    );
    let id = slow["id"].as_str().unwrap().to_string();
    wait_status(&service, &id, "running");
    std::thread::sleep(Duration::from_millis(300));
    service.post(&format!("/v1/runs/{id}/cancel"), json!({}));
    wait_status(&service, &id, "cancelled");
    let fresh = score(&service, json!({}));
    assert_eq!(fresh["output"]["served"], 1, "{fresh}");
}

#[test]
fn without_resident_programs_each_run_starts_its_own() {
    let mut service = resident_service();
    service.start(&[("MAGE_SERVICE_RUST_RESIDENT", "off")]);
    let first = score(&service, json!({}));
    let second = score(&service, json!({}));
    assert_eq!(first["status"], "completed", "{first}");
    assert_ne!(first["output"]["pid"], second["output"]["pid"]);
    assert_eq!(second["output"]["served"], 1);
}

/// A webhook that records each request's body; answers 200.
fn webhook_receiver() -> (u16, std::sync::Arc<std::sync::Mutex<Vec<Value>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = received.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut request = Vec::new();
            let mut buffer = [0u8; 8192];
            while let Ok(n) = stream.read(&mut buffer) {
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..n]);
                let text = String::from_utf8_lossy(&request);
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if body.len() >= length {
                        if let Ok(value) = serde_json::from_str(&body[..length]) {
                            store.lock().unwrap().push(value);
                        }
                        let _ = stream.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        );
                        break;
                    }
                }
            }
        }
    });
    (port, received)
}

fn wait_for_alerts(received: &std::sync::Mutex<Vec<Value>>, count: usize) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let alerts = received.lock().unwrap().clone();
        if alerts.len() >= count {
            return alerts;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    received.lock().unwrap().clone()
}

#[test]
fn a_failed_run_and_a_late_one_send_alerts_once_per_cooldown() {
    let (port, received) = webhook_receiver();
    let mut manifest = manifest(false);
    manifest["alerts"] = json!({
        "on": ["run_failed", "run_late"],
        "channels": [{"kind": "webhook", "url_env": "TEST_ALERT_URL"}],
        "cooldown_minutes": 10,
        "run_minutes": 0.02,
    });
    let mut service = Service::create(false);
    std::fs::write(
        service.dir.path().join("service.json"),
        manifest.to_string(),
    )
    .unwrap();
    let url = format!("http://127.0.0.1:{port}/hook");
    service.start(&[
        ("TEST_ALERT_URL", url.as_str()),
        ("MAGE_SERVICE_PUBLIC_URL", "https://scoring.example.org"),
        ("MAGE_SERVICE_ALERT_CHECK_SECONDS", "0.2"),
    ]);

    let (_, run) = service.post("/v1/pipelines/fails/runs?wait=60", json!({}));
    assert_eq!(run["status"], "failed");
    let alerts = wait_for_alerts(&received, 1);
    let alert = &alerts[0];
    assert_eq!(
        (
            alert["kind"].as_str(),
            alert["pipeline"].as_str(),
            alert["block"].as_str()
        ),
        (Some("run_failed"), Some("fails"), Some("broken")),
        "{alert}"
    );
    let text = alert["text"].as_str().unwrap();
    assert!(text.contains("this block always fails"), "{text}");
    let link = format!(
        "https://scoring.example.org/v1/runs/{}",
        run["id"].as_str().unwrap()
    );
    assert_eq!(alert["link"], json!(link));
    assert!(text.contains("What to do:"), "{text}");

    // The same failure again within the cooldown is held.
    service.post("/v1/pipelines/fails/runs?wait=60", json!({}));
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a["kind"] == "run_failed")
            .count(),
        1
    );

    // A run past run_minutes (1.2 s) is reported late, once.
    let markers = service.markers();
    let (_, slow) = service.post(
        "/v1/pipelines/slow/runs",
        json!({"variables": {"marker_dir": markers, "seconds": 6}}),
    );
    let alerts = wait_for_alerts(&received, 2);
    let late = alerts
        .iter()
        .find(|a| a["kind"] == "run_late")
        .expect("a late alert");
    assert_eq!(late["run_id"], slow["id"]);
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a["kind"] == "run_late")
            .count(),
        1
    );
}

#[test]
fn a_stream_with_event_time_counts_late_messages_and_shows_its_watermark() {
    let mut manifest = webhook_manifest(json!({}), 1 << 20);
    manifest["pipelines"][0]["stream"]["time"] =
        json!({"field": "at", "format": "epoch_s", "lateness_ms": 1000, "on_late": "pass"});
    let mut service = Service::with_manifest(&manifest, &[("transformers/tag.py", TAG_BLOCK)]);
    service.start(&[("MAGE_SERVICE_INGEST_TOKEN", "ingest-secret")]);
    wait_for_stream(&service, "hooks");
    for (key, body) in [
        ("a", r#"[{"at": 100}, {"at": 105}]"#),
        ("b", r#"[{"at": 101}, {"x": 1}]"#),
    ] {
        let (status, reply) = service
            .request_with(
                "POST",
                "/v1/streams/hooks/messages",
                Some("ingest-secret"),
                body,
                &[("Idempotency-Key", key)],
            )
            .unwrap();
        assert_eq!(status, 200, "{reply}");
    }
    let (_, detail) = service.get("/v1/streams/hooks");
    assert_eq!(
        (detail["late"].clone(), detail["untimed"].clone()),
        (json!(1), json!(1)),
        "{detail}"
    );
    // With pass, every message went on to the sink.
    let log = std::fs::read_to_string(service.dir.path().join("service.log")).unwrap();
    assert_eq!(log.matches("tagged").count(), 4, "{log}");
    let (_, metrics) = service
        .request("GET", "/metrics", Some(TOKEN), None)
        .unwrap();
    let text = metrics.as_str().unwrap_or_default().to_string();
    assert!(
        text.contains(r#"mage_service_stream_late_total{pipeline="hooks"} 1"#),
        "{text}"
    );
    assert!(
        text.contains(
            r#"mage_service_stream_watermark_seconds{pipeline="hooks",partition="0"} 104.000"#
        ),
        "{text}"
    );
}
