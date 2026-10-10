use std::path::PathBuf;

use anyhow::{Context as _, anyhow};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

/// The pipeline variables and runtime arguments of the run, such as `execution_date`.
#[derive(Debug, Clone, Default)]
pub struct Vars(pub(crate) Map<String, Value>);

impl Vars {
    /// The variable converted to `T`; an error names a missing or mistyped variable.
    pub fn get<T: DeserializeOwned>(&self, name: &str) -> anyhow::Result<T> {
        let value = self
            .0
            .get(name)
            .ok_or_else(|| anyhow!("The pipeline has no variable {name}"))?;
        serde_json::from_value(value.clone())
            .with_context(|| format!("Variable {name} is {value}, of another type"))
    }

    /// The variable converted to `T`, or `default` when the pipeline has no such variable.
    pub fn get_or<T: DeserializeOwned>(&self, name: &str, default: T) -> anyhow::Result<T> {
        if self.0.contains_key(name) {
            self.get(name)
        } else {
            Ok(default)
        }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    pub fn raw(&self) -> &Map<String, Value> {
        &self.0
    }
}

/// Where and as what the block runs.
#[derive(Debug, Clone)]
pub struct BlockContext {
    pub block_uuid: String,
    pub block_type: String,
    pub pipeline_uuid: Option<String>,
    pub execution_partition: Option<String>,
    pub variables: Vars,
    /// A directory the block can use for scratch files; Mage removes it after the run.
    pub scratch_dir: PathBuf,
}
