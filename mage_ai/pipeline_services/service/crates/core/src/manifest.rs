//! `service.json`: what `mage export service` captured from a Mage project.
//!
//! The manifest is the only input of a pipeline service besides the project files. It is
//! strict: an unknown field is an error, so a manifest from a newer exporter fails loudly
//! instead of losing a setting.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The version the exporter writes. Version 2 adds streams; the service reads 1 and 2, so
/// services exported before keep working (principle 8: read the previous shape).
pub const SCHEMA_VERSION: u32 = 2;
pub const OLDEST_SCHEMA_VERSION: u32 = 1;
pub const MANIFEST_FILE: &str = "service.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema_version: u32,
    pub service: ServiceInfo,
    pub pipelines: Vec<Pipeline>,
    /// Environment variables the exported code reads; values are never exported.
    #[serde(default)]
    pub environment: Vec<EnvironmentVariable>,
    /// ML models embedded in the image, from MLflow.
    #[serde(default)]
    pub models: Vec<Model>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Model {
    pub name: String,
    pub uri: String,
    /// Relative to the service directory; holds artifacts/ and mage-model.json.
    pub path: String,
    pub sha256: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub flavors: Vec<String>,
    #[serde(default)]
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentVariable {
    pub name: String,
    /// Its value is redacted from logs and errors.
    #[serde(default)]
    pub secret: bool,
    /// The code fails without it (`os.environ["X"]`); others are read with a default.
    #[serde(default)]
    pub required: bool,
    /// Where it is read: io_config.yaml or block and module files. A variable read here
    /// reaches block processes.
    #[serde(default)]
    pub used_by: Vec<String>,
    /// The stream connectors whose settings read it. Only the engine reads those; a
    /// variable no block reads never reaches block processes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connectors: Vec<String>,
    /// Where the service fetches it at start-up when the environment does not set it, as
    /// MAGE_SERVICE_SECRETS writes a reference: `ibm:<secret id>#field` for io_config.yaml's
    /// `ibm_secret_var`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServiceInfo {
    pub name: String,
    /// The project directory name; blocks import shared code as `<project>.utils`.
    pub project: String,
    pub exported_at: String,
    pub mage_version: String,
    /// SHA-256 of every exported file, in path order, so a service can say what it runs.
    pub source_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    pub uuid: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub max_concurrent_runs: u32,
    #[serde(default)]
    pub run_timeout_seconds: Option<u64>,
    /// Pipeline variables; secrets stay references such as `{{ env_var('TOKEN') }}`.
    #[serde(default)]
    pub variables: Map<String, Value>,
    pub blocks: Vec<Block>,
    #[serde(default)]
    pub triggers: Vec<Trigger>,
    /// Conditional blocks: Python files whose @condition functions decide whether the
    /// blocks they are attached to run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditionals: Vec<Conditional>,
    /// A streaming pipeline: it reads its source without end and its blocks, transformers,
    /// run on each batch of messages. It has no triggers; the service runs it from start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<Stream>,
}

/// The source, the sinks and the policies of a streaming pipeline. Connector settings
/// hold secrets as `$parts` environment references, as io_config profiles do.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stream {
    pub source: Connector,
    pub sinks: Vec<Connector>,
    #[serde(default)]
    pub batch: BatchPolicy,
    #[serde(default)]
    pub errors: ErrorPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Connector {
    /// The source or sink block's uuid in the Mage pipeline.
    pub name: String,
    pub kind: ConnectorKind,
    /// For a sink: the block whose output it writes. Mage streams branch, and sinks attach
    /// after different transformers. A stream without blocks writes the source's messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(default)]
    pub config: Map<String, Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorKind {
    /// Source: `POST /v1/streams/{pipeline}/messages`. Sink: an HTTP endpoint.
    Http,
    /// Sink: one JSON line per message on the service's output, for development.
    Stdout,
    Kafka,
    /// Sink: a Feast feature server's push endpoint.
    Feast,
    Rabbitmq,
    IbmMq,
    /// NATS JetStream: a durable pull consumer, and publishes JetStream acknowledges.
    Nats,
    /// MongoDB: a change stream with saved resume tokens, and a collection.
    Mongodb,
    /// Sink: one object per batch, Parquet or JSON lines, on S3, GCS, Azure Blob or disk.
    ObjectStorage,
    /// Source: changes by logical replication (pgoutput), confirmed in the slot. Sink: a
    /// PostgreSQL table, one transaction per batch.
    Postgres,
    /// Sink: a ClickHouse table, one deduplicated insert per batch.
    Clickhouse,
    /// Source: an Amazon SQS queue, messages deleted after the sinks.
    Sqs,
    /// Google Cloud Pub/Sub: a subscription, acknowledged after the sinks, and a topic.
    Pubsub,
    /// ActiveMQ over STOMP: a queue, acknowledged after the sinks, and persistent sends.
    Activemq,
    /// Sink: Elasticsearch or OpenSearch through the bulk API.
    Search,
    /// Sink: InfluxDB 2 as line protocol.
    Influxdb,
}

impl ConnectorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ConnectorKind::Http => "http",
            ConnectorKind::Stdout => "stdout",
            ConnectorKind::Kafka => "kafka",
            ConnectorKind::Feast => "feast",
            ConnectorKind::Rabbitmq => "rabbitmq",
            ConnectorKind::IbmMq => "ibm_mq",
            ConnectorKind::Nats => "nats",
            ConnectorKind::Mongodb => "mongodb",
            ConnectorKind::ObjectStorage => "object_storage",
            ConnectorKind::Postgres => "postgres",
            ConnectorKind::Clickhouse => "clickhouse",
            ConnectorKind::Sqs => "sqs",
            ConnectorKind::Pubsub => "pubsub",
            ConnectorKind::Activemq => "activemq",
            ConnectorKind::Search => "search",
            ConnectorKind::Influxdb => "influxdb",
        }
    }

    pub fn can_read(self) -> bool {
        matches!(
            self,
            ConnectorKind::Http
                | ConnectorKind::Kafka
                | ConnectorKind::Rabbitmq
                | ConnectorKind::IbmMq
                | ConnectorKind::Nats
                | ConnectorKind::Mongodb
                | ConnectorKind::Postgres
                | ConnectorKind::Sqs
                | ConnectorKind::Pubsub
                | ConnectorKind::Activemq
        )
    }

    pub fn can_write(self) -> bool {
        matches!(
            self,
            ConnectorKind::Http
                | ConnectorKind::Stdout
                | ConnectorKind::Kafka
                | ConnectorKind::Feast
                | ConnectorKind::Rabbitmq
                | ConnectorKind::IbmMq
                | ConnectorKind::Nats
                | ConnectorKind::Mongodb
                | ConnectorKind::ObjectStorage
                | ConnectorKind::Postgres
                | ConnectorKind::Clickhouse
                | ConnectorKind::Pubsub
                | ConnectorKind::Activemq
                | ConnectorKind::Search
                | ConnectorKind::Influxdb
        )
    }
}

/// How many messages a batch holds before the blocks run on it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct BatchPolicy {
    pub max_records: usize,
    /// A batch with fewer records runs after this long, so a quiet stream has no delay
    /// beyond it.
    pub max_wait_ms: u64,
    /// The source waits when a batch reaches this size, whatever its record count.
    pub max_bytes: usize,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        // Mage's streaming defaults: 100 messages, 500 ms.
        BatchPolicy {
            max_records: 100,
            max_wait_ms: 500,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

/// What happens to a batch whose blocks or sinks fail.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ErrorPolicy {
    pub retries: u32,
    pub delay_seconds: f64,
    pub max_delay_seconds: f64,
    /// Where the records of a batch go after its last retry. Without one the stream stops
    /// with the error, so no message is dropped quietly.
    pub dead_letter: Option<Connector>,
}

impl Default for ErrorPolicy {
    fn default() -> Self {
        ErrorPolicy {
            retries: 3,
            delay_seconds: 1.0,
            max_delay_seconds: 30.0,
            dead_letter: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Conditional {
    pub uuid: String,
    /// Relative to the project directory.
    pub file: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BlockType {
    DataLoader,
    Transformer,
    DataExporter,
    Custom,
}

impl BlockType {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockType::DataLoader => "data_loader",
            BlockType::Transformer => "transformer",
            BlockType::DataExporter => "data_exporter",
            BlockType::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    Python,
    Rust,
    R,
    Sql,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Block {
    pub uuid: String,
    #[serde(rename = "type")]
    pub block_type: BlockType,
    pub language: Language,
    /// Relative to the project directory.
    pub file: String,
    pub sha256: String,
    /// In the order of the function's arguments.
    #[serde(default)]
    pub upstream: Vec<String>,
    #[serde(default)]
    pub retry: Retry,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub configuration: Map<String, Value>,
    /// A Rust block's prebuilt binary, relative to the service directory.
    #[serde(default)]
    pub binary: Option<String>,
    /// The block in a binary that holds several (MAGE_BLOCK); None for a block's own
    /// binary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<String>,
    /// A SQL block's table name and its upstream blocks' language, type and
    /// configuration, which the SQL runner needs to name and reuse their tables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<Value>,
    /// A Rust block's io_config profile; see `io_config`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub io_config: Option<Value>,
    /// The conditional blocks that decide whether this block runs: it runs when every
    /// one returns True.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Retry {
    pub retries: u32,
    pub delay_seconds: f64,
    pub max_delay_seconds: f64,
    pub exponential_backoff: bool,
}

impl Default for Retry {
    fn default() -> Self {
        Retry {
            retries: 0,
            delay_seconds: 5.0,
            max_delay_seconds: 60.0,
            exponential_backoff: true,
        }
    }
}

impl Retry {
    /// The wait before attempt `attempt` (the first retry is attempt 2).
    pub fn delay(&self, attempt: u32) -> f64 {
        let retries_done = attempt.saturating_sub(2) as i32;
        let delay = if self.exponential_backoff {
            self.delay_seconds * 2f64.powi(retries_done)
        } else {
            self.delay_seconds
        };
        delay.min(self.max_delay_seconds).max(0.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    Time,
    Api,
    /// Starts a run when a run of another pipeline of the service ends.
    AfterRun,
}

impl TriggerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TriggerKind::Time => "time",
            TriggerKind::Api => "api",
            TriggerKind::AfterRun => "after_run",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Trigger {
    pub name: String,
    pub kind: TriggerKind,
    /// A cron expression or `@once`, `@hourly`, `@daily`, `@weekly`, `@monthly`.
    #[serde(default)]
    pub schedule: Option<String>,
    /// RFC 3339; no occurrence before it.
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub timezone: Option<String>,
    pub active: bool,
    #[serde(default)]
    pub variables: Map<String, Value>,
    #[serde(default)]
    pub skip_if_previous_running: bool,
    /// After-run triggers: the pipeline whose runs start this one.
    #[serde(default)]
    pub after_pipeline: Option<String>,
    /// After-run triggers: which endings start a run: completed (the default), failed or any.
    #[serde(default)]
    pub after_status: Option<String>,
}

impl Trigger {
    /// Whether a run of `pipeline` that ended with `status` starts this after-run trigger.
    pub fn follows(&self, pipeline: &str, status: &str) -> bool {
        self.kind == TriggerKind::AfterRun
            && self.active
            && self.after_pipeline.as_deref() == Some(pipeline)
            && match self.after_status.as_deref().unwrap_or("completed") {
                "any" => matches!(status, "completed" | "failed"),
                wanted => wanted == status,
            }
    }

    /// How the trigger starts runs, for people: a schedule, or the run it follows.
    pub fn describe(&self) -> Option<String> {
        match self.kind {
            TriggerKind::AfterRun => self.after_pipeline.as_ref().map(|after| {
                let ending = match self.after_status.as_deref().unwrap_or("completed") {
                    "failed" => "fails",
                    "any" => "ends",
                    _ => "completes",
                };
                format!("after {after} {ending}")
            }),
            _ => self.schedule.clone(),
        }
    }
}

/// The cron expression of a Mage schedule interval; `@once` has none.
pub fn cron_expression(schedule: &str) -> Option<String> {
    let expression = match schedule.trim() {
        "@once" => return None,
        "@hourly" => "0 * * * *",
        "@daily" => "0 0 * * *",
        "@weekly" => "0 0 * * 0",
        "@monthly" => "0 0 1 * *",
        other => other,
    };
    Some(expression.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ManifestError(pub String);

impl Manifest {
    pub fn parse(text: &str) -> Result<Manifest, ManifestError> {
        if let Some(message) = crate::json::check_integers(text, "service.json") {
            return Err(ManifestError(message));
        }
        let manifest: Manifest = serde_json::from_str(text)
            .map_err(|error| ManifestError(format!("service.json is invalid: {error}")))?;
        if manifest.schema_version > SCHEMA_VERSION {
            return Err(ManifestError(format!(
                "service.json has schema version {}, newer than this engine reads \
                 ({OLDEST_SCHEMA_VERSION} to {SCHEMA_VERSION}): update the engine to the \
                 MageML version that exported the service.",
                manifest.schema_version
            )));
        }
        if manifest.schema_version < OLDEST_SCHEMA_VERSION {
            return Err(ManifestError(format!(
                "service.json has schema version {}, older than this engine reads \
                 ({OLDEST_SCHEMA_VERSION} to {SCHEMA_VERSION}): export the pipelines again.",
                manifest.schema_version
            )));
        }
        if manifest.schema_version < 2
            && let Some(pipeline) = manifest.pipelines.iter().find(|p| p.stream.is_some())
        {
            return Err(ManifestError(format!(
                "service.json has schema version {} but pipeline {} is a stream, which needs \
                 version 2.",
                manifest.schema_version, pipeline.uuid
            )));
        }
        Ok(manifest)
    }

    pub fn pipeline(&self, uuid: &str) -> Option<&Pipeline> {
        self.pipelines.iter().find(|p| p.uuid == uuid)
    }

    /// Every problem at once, so an export is fixed in one pass.
    pub fn validate(&self, project_dir: Option<&Path>) -> Vec<String> {
        let mut problems = Vec::new();
        let mut pipeline_ids = BTreeSet::new();
        if !safe_relative(&self.service.project) || self.service.project.contains('/') {
            problems.push(format!(
                "The project name {:?} cannot be a directory name.",
                self.service.project
            ));
        }
        if self.pipelines.is_empty() {
            problems.push("The service has no pipelines.".into());
        }
        let mut model_names = BTreeSet::new();
        for model in &self.models {
            if !model_names.insert(model.name.as_str()) {
                problems.push(format!("Model {} appears twice.", model.name));
            }
            if !safe_relative(&model.path) {
                problems.push(format!(
                    "Model {} has path {:?} outside the service.",
                    model.name, model.path
                ));
            } else if let Some(service_dir) = project_dir.and_then(Path::parent)
                && !service_dir.join(&model.path).join("artifacts").is_dir()
            {
                problems.push(format!(
                    "Model {} needs {}/artifacts, which is not in the service.",
                    model.name, model.path
                ));
            }
        }
        for pipeline in &self.pipelines {
            if !pipeline_ids.insert(pipeline.uuid.as_str()) {
                problems.push(format!("Pipeline {} appears twice.", pipeline.uuid));
            }
            if !valid_id(&pipeline.uuid) {
                problems.push(format!(
                    "Pipeline id {:?} must use letters, digits, '_', '-' and '.'.",
                    pipeline.uuid
                ));
            }
            if pipeline.max_concurrent_runs == 0 {
                problems.push(format!(
                    "Pipeline {}: max_concurrent_runs must be at least 1.",
                    pipeline.uuid
                ));
            }
            problems.extend(pipeline.validate(project_dir));
            for trigger in &pipeline.triggers {
                if let Some(after) = &trigger.after_pipeline
                    && trigger.kind == TriggerKind::AfterRun
                    && self.pipeline(after).is_none()
                {
                    problems.push(format!(
                        "Pipeline {}: trigger {} follows pipeline {after}, which is not in \
                         the service.",
                        pipeline.uuid, trigger.name
                    ));
                }
            }
        }
        if let Some(cycle) = self.chain_cycle() {
            problems.push(format!(
                "Pipelines start each other after their runs in a loop: {}. A run would start \
                 the next without end; remove one of these after-run triggers.",
                cycle.join(" -> ")
            ));
        }
        problems
    }

    /// The pipelines each pipeline's runs start, from the after-run triggers.
    pub fn followers(&self, pipeline: &str) -> Vec<(&Pipeline, &Trigger)> {
        self.pipelines
            .iter()
            .flat_map(|p| p.triggers.iter().map(move |t| (p, t)))
            .filter(|(_, t)| {
                t.kind == TriggerKind::AfterRun && t.after_pipeline.as_deref() == Some(pipeline)
            })
            .collect()
    }

    /// A loop of after-run triggers, as the pipelines in it, first one repeated at the end.
    fn chain_cycle(&self) -> Option<Vec<String>> {
        fn visit<'a>(
            manifest: &'a Manifest,
            uuid: &'a str,
            path: &mut Vec<&'a str>,
            done: &mut BTreeSet<&'a str>,
        ) -> Option<Vec<String>> {
            if let Some(start) = path.iter().position(|p| *p == uuid) {
                let mut cycle: Vec<String> = path[start..].iter().map(|p| p.to_string()).collect();
                cycle.push(uuid.to_string());
                return Some(cycle);
            }
            if done.contains(uuid) {
                return None;
            }
            path.push(uuid);
            for (follower, _) in manifest.followers(uuid) {
                if let Some(cycle) = visit(manifest, &follower.uuid, path, done) {
                    return Some(cycle);
                }
            }
            path.pop();
            done.insert(uuid);
            None
        }
        let mut done = BTreeSet::new();
        for pipeline in &self.pipelines {
            if let Some(cycle) = visit(self, &pipeline.uuid, &mut Vec::new(), &mut done) {
                return Some(cycle);
            }
        }
        None
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 200
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
        && !id.starts_with('/')
        && !id.contains("..")
}

fn safe_relative(path: &str) -> bool {
    let path = Path::new(path);
    !path.as_os_str().is_empty() && path.components().all(|c| matches!(c, Component::Normal(_)))
}

impl Pipeline {
    fn validate_stream(&self, stream: &Stream) -> Vec<String> {
        let name = &self.uuid;
        let mut problems = Vec::new();
        if !self.triggers.is_empty() {
            problems.push(format!(
                "Pipeline {name} is a stream, which runs from the service's start; it cannot \
                 have triggers."
            ));
        }
        if !stream.source.kind.can_read() {
            problems.push(format!(
                "Pipeline {name}: {} cannot be a stream source.",
                stream.source.kind.as_str()
            ));
        }
        if stream.sinks.is_empty() {
            problems.push(format!("Pipeline {name} is a stream with no sink."));
        }
        let mut names = BTreeSet::from([stream.source.name.as_str()]);
        let sinks = stream.sinks.iter().chain(stream.errors.dead_letter.iter());
        for sink in sinks {
            if !sink.kind.can_write() {
                problems.push(format!(
                    "Pipeline {name}: {} cannot be a stream sink.",
                    sink.kind.as_str()
                ));
            }
            if !names.insert(sink.name.as_str()) {
                problems.push(format!(
                    "Pipeline {name}: connector {} appears twice.",
                    sink.name
                ));
            }
        }
        // The blocks form a DAG whose roots read the source (ADR-0032); each sink names the
        // block it writes. The generic checks cover unknown upstreams and cycles.
        for block in &self.blocks {
            if block.block_type != BlockType::Transformer {
                problems.push(format!(
                    "Pipeline {name}: block {} is a {:?}; a stream's blocks are transformers.",
                    block.uuid, block.block_type
                ));
            }
            if !matches!(
                block.language,
                Language::Python | Language::Rust | Language::R
            ) {
                problems.push(format!(
                    "Pipeline {name}: block {} is in {:?}; stream blocks run in Python, Rust or R.",
                    block.uuid, block.language
                ));
            }
        }
        if stream.source.after.is_some() {
            problems.push(format!(
                "Pipeline {name}: the source {} cannot follow a block.",
                stream.source.name
            ));
        }
        for sink in &stream.sinks {
            match (&sink.after, self.blocks.is_empty()) {
                (Some(after), _) if self.block(after).is_none() => problems.push(format!(
                    "Pipeline {name}: sink {} follows {after}, which is not a block of the \
                     stream.",
                    sink.name
                )),
                (None, false) => problems.push(format!(
                    "Pipeline {name}: sink {} must name the block whose output it writes.",
                    sink.name
                )),
                _ => {}
            }
        }
        if let Some(dead_letter) = &stream.errors.dead_letter
            && dead_letter.after.is_some()
        {
            problems.push(format!(
                "Pipeline {name}: the dead-letter sink {} receives failed messages of any \
                 block; it cannot follow one.",
                dead_letter.name
            ));
        }
        let batch = &stream.batch;
        if batch.max_records == 0 || batch.max_bytes < 1024 {
            problems.push(format!(
                "Pipeline {name}: a batch needs at least 1 record and 1024 bytes."
            ));
        }
        let errors = &stream.errors;
        let delays = [errors.delay_seconds, errors.max_delay_seconds];
        if delays.iter().any(|d| !d.is_finite() || *d < 0.0) {
            problems.push(format!(
                "Pipeline {name}: the stream's retry delays must be numbers of 0 or more."
            ));
        }
        problems
    }

    pub fn block(&self, uuid: &str) -> Option<&Block> {
        self.blocks.iter().find(|b| b.uuid == uuid)
    }

    pub fn downstream(&self) -> HashMap<&str, Vec<&str>> {
        let mut downstream: HashMap<&str, Vec<&str>> = HashMap::new();
        for block in &self.blocks {
            for upstream in &block.upstream {
                downstream
                    .entry(upstream.as_str())
                    .or_default()
                    .push(block.uuid.as_str());
            }
        }
        downstream
    }

    /// Blocks in an order where each comes after its upstream blocks; None with a cycle.
    pub fn topological_order(&self) -> Option<Vec<&Block>> {
        let index: BTreeMap<&str, &Block> =
            self.blocks.iter().map(|b| (b.uuid.as_str(), b)).collect();
        let mut remaining: BTreeMap<&str, usize> = self
            .blocks
            .iter()
            .map(|b| {
                let known = b
                    .upstream
                    .iter()
                    .filter(|u| index.contains_key(u.as_str()))
                    .count();
                (b.uuid.as_str(), known)
            })
            .collect();
        let downstream = self.downstream();
        // Keeps the manifest's block order among blocks that are ready together.
        let mut ready: Vec<&str> = self
            .blocks
            .iter()
            .filter(|b| remaining[b.uuid.as_str()] == 0)
            .map(|b| b.uuid.as_str())
            .collect();
        let mut order = Vec::with_capacity(self.blocks.len());
        let mut position = 0;
        while position < ready.len() {
            let uuid = ready[position];
            position += 1;
            order.push(index[uuid]);
            for child in downstream.get(uuid).into_iter().flatten() {
                let count = remaining.get_mut(child)?;
                *count -= 1;
                if *count == 0 {
                    ready.push(child);
                }
            }
        }
        (order.len() == self.blocks.len()).then_some(order)
    }

    fn validate(&self, project_dir: Option<&Path>) -> Vec<String> {
        let mut problems = Vec::new();
        let name = &self.uuid;
        // A stream may send its source's messages to its sinks with no block between.
        if self.blocks.is_empty() && self.stream.is_none() {
            problems.push(format!("Pipeline {name} has no blocks."));
        }
        if let Some(stream) = &self.stream {
            problems.extend(self.validate_stream(stream));
        }
        let mut seen = BTreeSet::new();
        for block in &self.blocks {
            if !seen.insert(block.uuid.as_str()) {
                problems.push(format!(
                    "Pipeline {name}: block {} appears twice.",
                    block.uuid
                ));
            }
            if !valid_id(&block.uuid) {
                problems.push(format!(
                    "Pipeline {name}: block id {:?} has characters a file name cannot hold.",
                    block.uuid
                ));
            }
            if !safe_relative(&block.file) {
                problems.push(format!(
                    "Pipeline {name}: block {} has file {:?}, which is not a path inside the \
                     project.",
                    block.uuid, block.file
                ));
            } else if let Some(dir) = project_dir
                && !dir.join(&block.file).is_file()
            {
                problems.push(format!(
                    "Pipeline {name}: block {} needs {}, which is not in the service.",
                    block.uuid, block.file
                ));
            }
            if block.language == Language::Rust && block.binary.is_none() {
                problems.push(format!(
                    "Pipeline {name}: Rust block {} has no binary; build the image with the \
                     generated Dockerfile.",
                    block.uuid
                ));
            }
            if !(block.retry.delay_seconds.is_finite() && block.retry.max_delay_seconds.is_finite())
            {
                problems.push(format!(
                    "Pipeline {name}: block {} has a retry delay that is not a number.",
                    block.uuid
                ));
            }
            for condition in &block.conditions {
                if !self.conditionals.iter().any(|c| &c.uuid == condition) {
                    problems.push(format!(
                        "Pipeline {name}: block {} has condition {condition}, which is not \
                         among the pipeline's conditional blocks.",
                        block.uuid
                    ));
                }
            }
            let mut upstream_seen = BTreeSet::new();
            for upstream in &block.upstream {
                if !upstream_seen.insert(upstream.as_str()) {
                    problems.push(format!(
                        "Pipeline {name}: block {} lists upstream {upstream} twice.",
                        block.uuid
                    ));
                }
                if self.block(upstream).is_none() {
                    problems.push(format!(
                        "Pipeline {name}: block {} depends on {upstream}, which is not in the \
                         pipeline.",
                        block.uuid
                    ));
                }
            }
        }
        for conditional in &self.conditionals {
            if !safe_relative(&conditional.file) {
                problems.push(format!(
                    "Pipeline {name}: conditional block {} has file {:?}, which is not a path \
                     inside the project.",
                    conditional.uuid, conditional.file
                ));
            } else if let Some(dir) = project_dir
                && !dir.join(&conditional.file).is_file()
            {
                problems.push(format!(
                    "Pipeline {name}: conditional block {} needs {}, which is not in the \
                     service.",
                    conditional.uuid, conditional.file
                ));
            }
        }
        if problems.is_empty() && self.topological_order().is_none() {
            problems.push(format!(
                "Pipeline {name}: its blocks depend on each other in a cycle."
            ));
        }
        let mut trigger_names = BTreeSet::new();
        for trigger in &self.triggers {
            if !trigger_names.insert(trigger.name.as_str()) {
                problems.push(format!(
                    "Pipeline {name}: trigger {} appears twice.",
                    trigger.name
                ));
            }
            if trigger.kind == TriggerKind::Time && trigger.schedule.is_none() {
                problems.push(format!(
                    "Pipeline {name}: time trigger {} has no schedule.",
                    trigger.name
                ));
            }
            if trigger.kind == TriggerKind::AfterRun {
                if trigger.after_pipeline.is_none() {
                    problems.push(format!(
                        "Pipeline {name}: after-run trigger {} names no pipeline to follow.",
                        trigger.name
                    ));
                }
                if !matches!(
                    trigger.after_status.as_deref().unwrap_or("completed"),
                    "completed" | "failed" | "any"
                ) {
                    problems.push(format!(
                        "Pipeline {name}: after-run trigger {} has after_status {:?}; use \
                         completed, failed or any.",
                        trigger.name,
                        trigger.after_status.as_deref().unwrap_or_default()
                    ));
                }
            }
        }
        problems
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub fn manifest(blocks: Value) -> Manifest {
        serde_json::from_value(json!({
            "schema_version": 1,
            "service": {
                "name": "s", "project": "p", "exported_at": "2026-10-10T00:00:00Z",
                "mage_version": "0.9.79", "source_sha256": "00"
            },
            "pipelines": [{
                "uuid": "etl", "name": "etl", "max_concurrent_runs": 1,
                "blocks": blocks
            }]
        }))
        .unwrap()
    }

    fn block(uuid: &str, upstream: &[&str]) -> Value {
        json!({
            "uuid": uuid, "type": "transformer", "language": "python",
            "file": format!("transformers/{uuid}.py"), "sha256": "00", "upstream": upstream
        })
    }

    #[test]
    fn a_valid_manifest_has_no_problems_and_a_topological_order() {
        let m = manifest(json!([
            block("c", &["a", "b"]),
            block("a", &[]),
            block("b", &["a"])
        ]));
        assert_eq!(m.validate(None), Vec::<String>::new());
        let order: Vec<_> = m.pipelines[0]
            .topological_order()
            .unwrap()
            .iter()
            .map(|b| b.uuid.clone())
            .collect();
        assert_eq!(order, ["a", "b", "c"]);
    }

    #[test]
    fn every_problem_is_reported() {
        let m = manifest(json!([
            block("a", &["missing"]),
            block("a", &[]),
            {"uuid": "b", "type": "transformer", "language": "python",
             "file": "../outside.py", "sha256": "00"}
        ]));
        let problems = m.validate(None).join("\n");
        assert!(problems.contains("depends on missing"), "{problems}");
        assert!(problems.contains("block a appears twice"), "{problems}");
        assert!(
            problems.contains("not a path inside the project"),
            "{problems}"
        );
    }

    #[test]
    fn conditions_name_conditional_blocks_with_files_in_the_project() {
        let mut guarded = block("b", &["a"]);
        guarded["conditions"] = json!(["has_rows", "missing"]);
        let mut m = manifest(json!([block("a", &[]), guarded]));
        m.pipelines[0].conditionals = vec![Conditional {
            uuid: "has_rows".into(),
            file: "../has_rows.py".into(),
            sha256: "00".into(),
        }];
        let problems = m.validate(None).join("\n");
        assert!(
            problems.contains(
                "block b has condition missing, which is not among the pipeline's \
                 conditional blocks"
            ),
            "{problems}"
        );
        assert!(
            problems.contains("conditional block has_rows has file \"../has_rows.py\""),
            "{problems}"
        );
    }

    #[test]
    fn a_cycle_is_rejected() {
        let m = manifest(json!([block("a", &["b"]), block("b", &["a"])]));
        assert!(m.validate(None)[0].contains("cycle"));
        assert!(m.pipelines[0].topological_order().is_none());
    }

    #[test]
    fn unknown_fields_and_other_versions_fail() {
        let text = serde_json::to_string(&manifest(json!([block("a", &[])]))).unwrap();
        assert!(Manifest::parse(&text).is_ok());
        let unknown = text.replacen("\"name\":\"s\"", "\"name\":\"s\",\"surprise\":1", 1);
        assert!(
            Manifest::parse(&unknown)
                .unwrap_err()
                .0
                .contains("surprise")
        );
        let newer = text.replacen("\"schema_version\":1", "\"schema_version\":3", 1);
        assert!(Manifest::parse(&newer).unwrap_err().0.contains("version 3"));
    }

    #[test]
    fn retry_delays_grow_and_stop_at_the_maximum() {
        let retry = Retry {
            retries: 5,
            delay_seconds: 2.0,
            max_delay_seconds: 10.0,
            exponential_backoff: true,
        };
        assert_eq!([2, 3, 4, 5].map(|a| retry.delay(a)), [2.0, 4.0, 8.0, 10.0]);
    }

    #[test]
    fn named_intervals_become_cron_expressions() {
        assert_eq!(cron_expression("@daily").as_deref(), Some("0 0 * * *"));
        assert_eq!(
            cron_expression("*/5 * * * *").as_deref(),
            Some("*/5 * * * *")
        );
        assert_eq!(cron_expression("@once"), None);
    }

    fn chained(triggers: &[(&str, Value)]) -> Manifest {
        let pipelines: Vec<Value> = ["a", "b", "c"]
            .iter()
            .map(|uuid| {
                let mine: Vec<Value> = triggers
                    .iter()
                    .filter(|(p, _)| p == uuid)
                    .map(|(_, t)| t.clone())
                    .collect();
                json!({"uuid": uuid, "name": uuid, "max_concurrent_runs": 1,
                       "blocks": [block("x", &[])], "triggers": mine})
            })
            .collect();
        serde_json::from_value(json!({
            "schema_version": 1,
            "service": {"name": "s", "project": "p", "exported_at": "2026-10-10T00:00:00Z",
                        "mage_version": "0.9.79", "source_sha256": "00"},
            "pipelines": pipelines,
        }))
        .unwrap()
    }

    fn after(name: &str, pipeline: &str, status: Option<&str>) -> Value {
        json!({"name": name, "kind": "after_run", "active": true,
               "after_pipeline": pipeline, "after_status": status})
    }

    #[test]
    fn after_run_triggers_follow_the_endings_they_name() {
        let m = chained(&[
            ("b", after("t", "a", None)),
            ("c", after("u", "b", Some("any"))),
        ]);
        assert!(m.validate(None).is_empty(), "{:?}", m.validate(None));
        let b = &m.pipeline("b").unwrap().triggers[0];
        assert!(b.follows("a", "completed"));
        assert!(!b.follows("a", "failed"));
        assert!(!b.follows("c", "completed"));
        let c = &m.pipeline("c").unwrap().triggers[0];
        assert!(c.follows("b", "failed") && c.follows("b", "completed"));
        assert!(!c.follows("b", "cancelled"));
        assert_eq!(b.describe().as_deref(), Some("after a completes"));
        assert_eq!(c.describe().as_deref(), Some("after b ends"));
        assert_eq!(m.followers("a").len(), 1);
    }

    #[test]
    fn chains_that_loop_or_follow_missing_pipelines_are_problems() {
        let looped = chained(&[
            ("a", after("t", "c", None)),
            ("b", after("t", "a", None)),
            ("c", after("t", "b", None)),
        ]);
        let problems = looped.validate(None).join("\n");
        assert!(problems.contains("in a loop"), "{problems}");

        let missing = chained(&[
            ("b", after("t", "nope", None)),
            ("c", after("u", "a", Some("sometimes"))),
            (
                "a",
                json!({"name": "v", "kind": "after_run", "active": true}),
            ),
        ]);
        let problems = missing.validate(None).join("\n");
        assert!(problems.contains("follows pipeline nope"), "{problems}");
        assert!(
            problems.contains("after_status \"sometimes\""),
            "{problems}"
        );
        assert!(
            problems.contains("names no pipeline to follow"),
            "{problems}"
        );
    }

    fn stream_manifest(blocks: Value, stream: Value) -> Manifest {
        let mut manifest = manifest(blocks);
        manifest.pipelines[0].stream = Some(serde_json::from_value(stream).unwrap());
        manifest
    }

    fn kafka_to_http() -> Value {
        json!({
            "source": {"name": "orders", "kind": "kafka", "config": {
                "bootstrap_server": "kafka:9092", "topic": "orders",
                "sasl_password": {"$parts": [{"env": "KAFKA_PASSWORD"}]}
            }},
            "sinks": [{"name": "scores", "kind": "http", "after": "score",
                       "config": {"url": "http://x/y"}}]
        })
    }

    #[test]
    fn a_stream_parses_with_its_defaults_and_secret_references() {
        let blocks = json!([block("clean", &[]), block("score", &["clean"])]);
        let manifest = stream_manifest(blocks, kafka_to_http());

        assert!(
            manifest.validate(None).is_empty(),
            "{:?}",
            manifest.validate(None)
        );
        let stream = manifest.pipelines[0].stream.as_ref().unwrap();
        assert_eq!(stream.batch, BatchPolicy::default());
        assert_eq!(stream.batch.max_records, 100);
        assert_eq!(stream.errors.retries, 3);
        assert!(stream.errors.dead_letter.is_none());
        let password = &stream.source.config["sasl_password"];
        assert_eq!(
            crate::io_config::variables(password),
            vec!["KAFKA_PASSWORD"]
        );
    }

    #[test]
    fn a_stream_may_have_no_blocks_but_needs_a_sink() {
        let mut stream = kafka_to_http();
        stream["sinks"][0].as_object_mut().unwrap().remove("after");
        assert!(
            stream_manifest(json!([]), stream.clone())
                .validate(None)
                .is_empty()
        );

        stream["sinks"] = json!([]);
        let problems = stream_manifest(json!([]), stream).validate(None).join("\n");
        assert!(problems.contains("is a stream with no sink"), "{problems}");
    }

    #[test]
    fn a_stream_branches_and_sinks_follow_any_block() {
        let blocks = json!([
            block("clean", &[]),
            block("score", &["clean"]),
            block("audit", &["clean"])
        ]);
        let mut stream = kafka_to_http();
        stream["sinks"] = json!([
            {"name": "scores", "kind": "kafka", "after": "score"},
            {"name": "audit_log", "kind": "stdout", "after": "audit"}
        ]);

        let manifest = stream_manifest(blocks, stream);

        assert!(
            manifest.validate(None).is_empty(),
            "{:?}",
            manifest.validate(None)
        );
    }

    #[test]
    fn stream_blocks_are_transformers_and_sinks_name_a_block() {
        let mut loader = block("load", &[]);
        loader["type"] = json!("data_loader");
        let mut stream = kafka_to_http();
        stream["sinks"] = json!([
            {"name": "a", "kind": "kafka", "after": "missing"},
            {"name": "b", "kind": "kafka"}
        ]);
        stream["errors"] = json!({"dead_letter": {"name": "dl", "kind": "kafka", "after": "load"}});

        let problems = stream_manifest(json!([loader]), stream)
            .validate(None)
            .join("\n");

        assert!(
            problems.contains("a stream's blocks are transformers"),
            "{problems}"
        );
        assert!(problems.contains("sink a follows missing"), "{problems}");
        assert!(
            problems.contains("sink b must name the block"),
            "{problems}"
        );
        assert!(problems.contains("dead-letter sink dl"), "{problems}");
    }

    #[test]
    fn a_stream_has_no_triggers_and_connectors_in_their_roles() {
        let mut manifest = stream_manifest(
            json!([]),
            json!({
                "source": {"name": "out", "kind": "stdout"},
                "sinks": [{"name": "out", "kind": "kafka"}],
                "batch": {"max_records": 0}
            }),
        );
        manifest.pipelines[0].triggers = serde_json::from_value(json!([
            {"name": "daily", "kind": "time", "schedule": "@daily", "active": true}
        ]))
        .unwrap();

        let problems = manifest.validate(None).join("\n");

        assert!(problems.contains("cannot have triggers"), "{problems}");
        assert!(
            problems.contains("stdout cannot be a stream source"),
            "{problems}"
        );
        assert!(
            problems.contains("connector out appears twice"),
            "{problems}"
        );
        assert!(problems.contains("at least 1 record"), "{problems}");
    }

    #[test]
    fn an_unknown_connector_kind_is_an_error() {
        let stream = json!({"source": {"name": "s", "kind": "carrier_pigeon"}, "sinks": []});
        assert!(serde_json::from_value::<Stream>(stream).is_err());
    }

    #[test]
    fn versions_1_and_2_are_read_and_streams_need_2() {
        let mut value = serde_json::to_value(manifest(json!([block("a", &[])]))).unwrap();
        for version in [1, 2] {
            value["schema_version"] = json!(version);
            assert!(
                Manifest::parse(&value.to_string()).is_ok(),
                "version {version}"
            );
        }
        value["schema_version"] = json!(3);
        let error = Manifest::parse(&value.to_string()).unwrap_err().0;
        assert!(
            error.contains("newer than this engine reads (1 to 2)"),
            "{error}"
        );
        assert!(error.contains("update the engine"), "{error}");
        value["schema_version"] = json!(0);
        let error = Manifest::parse(&value.to_string()).unwrap_err().0;
        assert!(error.contains("export the pipelines again"), "{error}");

        let mut stream = serde_json::to_value(stream_manifest(json!([]), kafka_to_http())).unwrap();
        stream["schema_version"] = json!(1);
        let error = Manifest::parse(&stream.to_string()).unwrap_err().0;
        assert!(
            error.contains("is a stream, which needs version 2"),
            "{error}"
        );
        stream["schema_version"] = json!(2);
        assert!(Manifest::parse(&stream.to_string()).is_ok());
    }
}
