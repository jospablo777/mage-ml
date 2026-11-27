//! OpenLineage events of runs and block attempts, so catalogs (Marquez, DataHub, OpenMetadata)
//! show the service's pipelines and the data between their blocks.
//!
//! - A run is a job named by its pipeline; each block attempt is a job `<pipeline>.<block>`
//!   whose run has the pipeline run as its parent facet. Each gets START, then COMPLETE,
//!   FAIL or ABORT.
//! - A block's output is the dataset `<pipeline>.<block>` in the service's namespace, with
//!   its columns (a schema facet) when it is a frame; its inputs are its upstream blocks'.
//!
//! The standard variables configure it: `OPENLINEAGE_URL` (with `OPENLINEAGE_ENDPOINT`,
//! `api/v1/lineage` by default), `OPENLINEAGE_API_KEY`, `OPENLINEAGE_NAMESPACE` (the
//! service's name by default), `OPENLINEAGE_DISABLED`. Without a URL nothing starts. Events
//! go out in order from one thread; a slow or absent backend drops them (counted on /metrics),
//! never delays a run.

use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::traces::Counts;

const QUEUE: usize = 4096;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
const PARENT_URL: &str =
    "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet";
const SCHEMA_FACET_URL: &str =
    "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet";
const ERROR_FACET_URL: &str = "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet";

fn producer() -> String {
    format!(
        "https://github.com/jospablo777/mage-ml/tree/engine-{}",
        env!("CARGO_PKG_VERSION")
    )
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub url: String,
    pub api_key: Option<String>,
    pub namespace: String,
}

impl Config {
    pub fn from_env(service_name: &str) -> Option<Config> {
        Config::from_vars(service_name, |name| {
            std::env::var(name).ok().filter(|v| !v.trim().is_empty())
        })
    }

    fn from_vars(service_name: &str, var: impl Fn(&str) -> Option<String>) -> Option<Config> {
        if var("OPENLINEAGE_DISABLED").is_some_and(|v| v.trim().eq_ignore_ascii_case("true")) {
            return None;
        }
        let base = var("OPENLINEAGE_URL")?;
        let endpoint = var("OPENLINEAGE_ENDPOINT").unwrap_or_else(|| "api/v1/lineage".into());
        Some(Config {
            url: format!(
                "{}/{}",
                base.trim().trim_end_matches('/'),
                endpoint.trim().trim_start_matches('/')
            ),
            api_key: var("OPENLINEAGE_API_KEY"),
            namespace: var("OPENLINEAGE_NAMESPACE").unwrap_or_else(|| service_name.to_string()),
        })
    }
}

pub struct Lineage {
    pub config: Config,
    queue: SyncSender<Job>,
    pub counts: std::sync::Arc<Counts>,
}

enum Job {
    Event(Value),
    Flush(std::sync::mpsc::Sender<()>),
}

static LINEAGE: OnceLock<Option<Lineage>> = OnceLock::new();

/// Starts sending events when the environment configures it; once per process.
pub fn init(service_name: &str) {
    LINEAGE.get_or_init(|| {
        let config = Config::from_env(service_name)?;
        crate::log::info(&format!(
            "Lineage events go to {} (OpenLineage, namespace {}).",
            config.url, config.namespace
        ));
        Some(Lineage::start(config))
    });
}

pub fn lineage() -> Option<&'static Lineage> {
    LINEAGE.get().and_then(Option::as_ref)
}

/// Sends the events waiting, for at most `wait`: at the end of the process.
pub fn flush(wait: Duration) {
    if let Some(lineage) = lineage() {
        lineage.flush(wait);
    }
}

impl Lineage {
    pub fn start(config: Config) -> Lineage {
        let (queue, jobs) = sync_channel(QUEUE);
        let counts = std::sync::Arc::new(Counts::default());
        let lineage = Lineage {
            config: config.clone(),
            queue,
            counts: counts.clone(),
        };
        std::thread::Builder::new()
            .name("mage-lineage".into())
            .spawn(move || send_loop(config, jobs, counts))
            .expect("the lineage thread starts");
        lineage
    }

    pub fn emit(&self, event: Value) {
        if self.queue.try_send(Job::Event(event)).is_err() {
            self.counts.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn flush(&self, wait: Duration) {
        let (done, flushed) = std::sync::mpsc::channel();
        if self.queue.send(Job::Flush(done)).is_ok() {
            let _ = flushed.recv_timeout(wait);
        }
    }
}

fn send_loop(config: Config, jobs: Receiver<Job>, counts: std::sync::Arc<Counts>) {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(REQUEST_TIMEOUT))
        .http_status_as_error(false)
        .build()
        .into();
    let mut last_warning: Option<Instant> = None;
    for job in jobs {
        let event = match job {
            Job::Event(event) => event,
            Job::Flush(done) => {
                let _ = done.send(());
                continue;
            }
        };
        let mut request = agent
            .post(&config.url)
            .header("Content-Type", "application/json");
        if let Some(key) = &config.api_key {
            request = request.header("Authorization", format!("Bearer {key}"));
        }
        let outcome = match serde_json::to_vec(&event) {
            Ok(body) => match request.send(&body[..]) {
                Ok(response) if response.status().is_success() => Ok(()),
                Ok(response) => Err(format!("the backend answered HTTP {}", response.status())),
                Err(error) => Err(error.to_string()),
            },
            Err(error) => Err(error.to_string()),
        };
        match outcome {
            Ok(()) => {
                counts.sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                counts.failed.fetch_add(1, Ordering::Relaxed);
                if last_warning.is_none_or(|at| at.elapsed() > Duration::from_secs(60)) {
                    last_warning = Some(Instant::now());
                    crate::log::warn(&format!(
                        "Lineage: an event was not sent to {}: {error}",
                        config.url
                    ));
                }
            }
        }
    }
}

/// How a run or an attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Complete,
    Fail,
    Abort,
}

/// The run of a job: the pipeline run, or one attempt of a block.
pub struct JobRun<'a> {
    pub run_id: &'a str,
    pub job: String,
    /// The pipeline run of a block attempt.
    pub parent: Option<(&'a str, &'a str)>,
}

/// A dataset between blocks: a block's output in the service's namespace.
pub fn block_dataset(namespace: &str, pipeline: &str, block: &str) -> Value {
    json!({"namespace": namespace, "name": format!("{pipeline}.{block}")})
}

/// `dataset` with the columns of a frame output (its Arrow IPC file's schema).
pub fn with_schema(mut dataset: Value, records: &[Value], output_dir: &Path) -> Value {
    let Some(path) = records
        .first()
        .and_then(|record| frame_path(record, output_dir))
    else {
        return dataset;
    };
    let Some(fields) = arrow_fields(&path) else {
        return dataset;
    };
    dataset["facets"] = json!({"schema": {
        "_producer": producer(),
        "_schemaURL": SCHEMA_FACET_URL,
        "fields": fields,
    }});
    dataset
}

fn frame_path(record: &Value, output_dir: &Path) -> Option<std::path::PathBuf> {
    let frame = record.get("kind").and_then(Value::as_str) == Some("frame")
        && record
            .get("format")
            .and_then(Value::as_str)
            .unwrap_or("ipc")
            == "ipc";
    let path = record.get("path").and_then(Value::as_str)?;
    frame.then(|| output_dir.join(path))
}

fn arrow_fields(path: &Path) -> Option<Vec<Value>> {
    let file = std::fs::File::open(path).ok()?;
    let reader = arrow_ipc::reader::FileReader::try_new(file, None).ok()?;
    Some(
        reader
            .schema()
            .fields()
            .iter()
            .map(|field| json!({"name": field.name(), "type": field.data_type().to_string()}))
            .collect(),
    )
}

/// An event of `run`: START when `outcome` is None; `error` is the message and the language
/// of the code that failed.
pub fn event(
    namespace: &str,
    run: &JobRun,
    outcome: Option<Outcome>,
    error: Option<(&str, &str)>,
    inputs: Vec<Value>,
    outputs: Vec<Value>,
) -> Value {
    let mut facets = serde_json::Map::new();
    if let Some((parent_run, parent_job)) = run.parent {
        facets.insert(
            "parent".into(),
            json!({
                "_producer": producer(),
                "_schemaURL": PARENT_URL,
                "run": {"runId": parent_run},
                "job": {"namespace": namespace, "name": parent_job},
            }),
        );
    }
    if let Some((message, language)) = error {
        facets.insert(
            "errorMessage".into(),
            json!({
                "_producer": producer(),
                "_schemaURL": ERROR_FACET_URL,
                "message": message.chars().take(4000).collect::<String>(),
                "programmingLanguage": language,
            }),
        );
    }
    json!({
        "eventType": match outcome {
            None => "START",
            Some(Outcome::Complete) => "COMPLETE",
            Some(Outcome::Fail) => "FAIL",
            Some(Outcome::Abort) => "ABORT",
        },
        "eventTime": crate::ledger::timestamp(crate::ledger::now()),
        "producer": producer(),
        "schemaURL": SCHEMA_URL,
        "run": {"runId": run.run_id, "facets": facets},
        "job": {"namespace": namespace, "name": run.job},
        "inputs": inputs,
        "outputs": outputs,
    })
}

/// The events of one block attempt, or none without a backend.
pub struct Attempt {
    attempt_id: String,
    pipeline: String,
    run_id: String,
    block: String,
    language: &'static str,
    inputs: Vec<Value>,
}

impl Attempt {
    /// Sends the attempt's START; None when lineage is off.
    pub fn start(
        pipeline: &str,
        run_id: &str,
        block: &str,
        language: &'static str,
        upstream: &[String],
    ) -> Option<Attempt> {
        let lineage = lineage()?;
        let namespace = &lineage.config.namespace;
        let attempt = Attempt {
            attempt_id: uuid::Uuid::now_v7().to_string(),
            pipeline: pipeline.to_string(),
            run_id: run_id.to_string(),
            block: block.to_string(),
            language,
            inputs: upstream
                .iter()
                .map(|u| block_dataset(namespace, pipeline, u))
                .collect(),
        };
        lineage.emit(event(
            namespace,
            &attempt.job_run(),
            None,
            None,
            attempt.inputs.clone(),
            vec![block_dataset(namespace, pipeline, block)],
        ));
        Some(attempt)
    }

    fn job_run(&self) -> JobRun<'_> {
        JobRun {
            run_id: &self.attempt_id,
            job: format!("{}.{}", self.pipeline, self.block),
            parent: Some((&self.run_id, &self.pipeline)),
        }
    }

    /// The attempt's end: its output's columns when it completed with a frame.
    pub fn finish(
        self,
        outcome: Outcome,
        error: Option<&str>,
        records: &[Value],
        output_dir: &Path,
    ) {
        let Some(lineage) = lineage() else {
            return;
        };
        let namespace = &lineage.config.namespace;
        let output = block_dataset(namespace, &self.pipeline, &self.block);
        let outputs = match outcome {
            Outcome::Complete => vec![with_schema(output, records, output_dir)],
            _ => vec![output],
        };
        lineage.emit(event(
            namespace,
            &self.job_run(),
            Some(outcome),
            error.map(|message| (message, self.language)),
            self.inputs.clone(),
            outputs,
        ));
    }
}

/// A pipeline run's START, or its end with `outcome`.
pub fn pipeline_run(pipeline: &str, run_id: &str, outcome: Option<Outcome>) {
    if let Some(lineage) = lineage() {
        let run = JobRun {
            run_id,
            job: pipeline.to_string(),
            parent: None,
        };
        lineage.emit(event(
            &lineage.config.namespace,
            &run,
            outcome,
            None,
            vec![],
            vec![],
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    #[test]
    fn the_standard_variables_configure_it() {
        assert_eq!(Config::from_vars("svc", vars(&[])), None);
        assert_eq!(
            Config::from_vars("svc", vars(&[("OPENLINEAGE_URL", "http://marquez:5000/")])),
            Some(Config {
                url: "http://marquez:5000/api/v1/lineage".into(),
                api_key: None,
                namespace: "svc".into(),
            })
        );
        let custom = Config::from_vars(
            "svc",
            vars(&[
                ("OPENLINEAGE_URL", "https://catalog"),
                ("OPENLINEAGE_ENDPOINT", "/events"),
                ("OPENLINEAGE_API_KEY", "k"),
                ("OPENLINEAGE_NAMESPACE", "prod"),
            ]),
        )
        .unwrap();
        assert_eq!(custom.url, "https://catalog/events");
        assert_eq!(custom.api_key.as_deref(), Some("k"));
        assert_eq!(custom.namespace, "prod");
        assert_eq!(
            Config::from_vars(
                "svc",
                vars(&[
                    ("OPENLINEAGE_URL", "http://m"),
                    ("OPENLINEAGE_DISABLED", "true")
                ]),
            ),
            None
        );
    }

    #[test]
    fn a_block_attempt_names_its_parent_run_its_inputs_and_its_columns() {
        let dir = tempfile::tempdir().unwrap();
        let batch = arrow_array::RecordBatch::try_from_iter([
            (
                "id",
                std::sync::Arc::new(arrow_array::Int64Array::from(vec![1, 2]))
                    as arrow_array::ArrayRef,
            ),
            (
                "name",
                std::sync::Arc::new(arrow_array::StringArray::from(vec!["a", "b"])),
            ),
        ])
        .unwrap();
        let file = std::fs::File::create(dir.path().join("output_0.arrow")).unwrap();
        let mut writer = arrow_ipc::writer::FileWriter::try_new(file, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let records = [json!({"kind": "frame", "format": "ipc", "path": "output_0.arrow"})];
        let run = JobRun {
            run_id: "01a21a4f-07f9-7165-8022-9e996fdf2006",
            job: "etl.clean".into(),
            parent: Some(("01a21a4f-0805-70d9-bdf0-b12c7b200613", "etl")),
        };

        let done = event(
            "svc",
            &run,
            Some(Outcome::Complete),
            None,
            vec![block_dataset("svc", "etl", "load")],
            vec![with_schema(
                block_dataset("svc", "etl", "clean"),
                &records,
                dir.path(),
            )],
        );

        assert_eq!(done["eventType"], "COMPLETE");
        assert_eq!(
            done["job"],
            json!({"namespace": "svc", "name": "etl.clean"})
        );
        assert_eq!(
            done["run"]["facets"]["parent"]["run"]["runId"],
            "01a21a4f-0805-70d9-bdf0-b12c7b200613"
        );
        assert_eq!(done["inputs"][0]["name"], "etl.load");
        assert_eq!(
            done["outputs"][0]["facets"]["schema"]["fields"],
            json!([{"name": "id", "type": "Int64"}, {"name": "name", "type": "Utf8"}])
        );
        let failed = event(
            "svc",
            &run,
            Some(Outcome::Fail),
            Some(("boom", "python")),
            vec![],
            vec![],
        );
        assert_eq!(failed["run"]["facets"]["errorMessage"]["message"], "boom");
    }

    #[test]
    fn events_reach_the_backend_in_order() {
        let (url, received) = crate::traces::testing::collector();
        let base = url.trim_end_matches("/v1/traces").to_string();
        let lineage = Lineage::start(Config {
            url: format!("{base}/api/v1/lineage"),
            api_key: Some("k".into()),
            namespace: "svc".into(),
        });
        let run = JobRun {
            run_id: "01a21a4f-07f9-7165-8022-9e996fdf2006",
            job: "etl".into(),
            parent: None,
        };
        lineage.emit(event("svc", &run, None, None, vec![], vec![]));
        lineage.emit(event(
            "svc",
            &run,
            Some(Outcome::Complete),
            None,
            vec![],
            vec![],
        ));
        let first = received.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = received.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            (first["eventType"].as_str(), second["eventType"].as_str()),
            (Some("START"), Some("COMPLETE"))
        );
    }
}
