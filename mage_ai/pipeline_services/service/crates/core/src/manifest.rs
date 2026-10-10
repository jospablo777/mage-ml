//! `service.json`: what `mage export service` captured from a Mage project.
//!
//! The manifest is the only input of a pipeline service besides the project files. It is
//! strict: an unknown field is an error, so a manifest from a newer exporter fails loudly
//! instead of losing a setting.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const SCHEMA_VERSION: u32 = 1;
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
    /// Where it is read: io_config.yaml or block and module files.
    #[serde(default)]
    pub used_by: Vec<String>,
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
        let manifest: Manifest = serde_json::from_str(text)
            .map_err(|error| ManifestError(format!("service.json is invalid: {error}")))?;
        if manifest.schema_version != SCHEMA_VERSION {
            return Err(ManifestError(format!(
                "service.json has schema version {}; this service reads version {SCHEMA_VERSION}. \
                 Export the pipelines again with the matching Mage version.",
                manifest.schema_version
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
        }
        problems
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
        if self.blocks.is_empty() {
            problems.push(format!("Pipeline {name} has no blocks."));
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
        let newer = text.replacen("\"schema_version\":1", "\"schema_version\":2", 1);
        assert!(Manifest::parse(&newer).unwrap_err().0.contains("version 2"));
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
}
