//! ONNX models in Rust blocks, run by ONNX Runtime. Behind the `onnx` feature: enable it in
//! the project's rust/Cargo.toml with
//! `mage-block = { path = ".mage/sdk", features = ["onnx"] }`.
//!
//! ```ignore
//! use mage::prelude::*;
//!
//! fn transform(customers: DataFrame) -> Result<DataFrame> {
//!     let model = mage::onnx::model("models/churn.onnx")?;
//!     let scores = model.predict(&customers, &["tenure", "monthly_spend", "tickets"])?;
//!     Ok(customers.hstack(scores.columns())?)
//! }
//! ```
//!
//! A model is loaded once per process and reused: a chain of blocks or an exported service
//! that runs a block many times loads it once. Paths are relative to the project.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, Result, anyhow, bail, ensure};
use ort::session::Session;
use ort::session::builder::GraphOptimizationLevel;
use ort::value::{DynValue, Tensor, TensorElementType, ValueType};
use polars::prelude::*;

/// A loaded model: its session and the names and types of its inputs and outputs.
pub struct Model {
    path: PathBuf,
    session: Mutex<Session>,
    inputs: Vec<(String, Option<TensorElementType>)>,
    outputs: Vec<String>,
}

static MODELS: OnceLock<Mutex<HashMap<PathBuf, Arc<Model>>>> = OnceLock::new();

/// The model at `path`, loaded on the first call and shared by later ones.
pub fn model(path: impl AsRef<Path>) -> Result<Arc<Model>> {
    let path = path.as_ref().to_path_buf();
    let models = MODELS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(loaded) = models
        .lock()
        .map_err(|_| anyhow!("model cache poisoned"))?
        .get(&path)
    {
        return Ok(loaded.clone());
    }
    let loaded = Arc::new(Model::load(&path)?);
    models
        .lock()
        .map_err(|_| anyhow!("model cache poisoned"))?
        .insert(path, loaded.clone());
    Ok(loaded)
}

/// Threads for one inference: the block's share of the machine when MageML set it.
fn threads() -> Option<usize> {
    [
        "MAGE_BLOCK_THREADS",
        "RAYON_NUM_THREADS",
        "POLARS_MAX_THREADS",
    ]
    .iter()
    .find_map(|name| std::env::var(name).ok()?.parse::<usize>().ok())
    .filter(|threads| *threads > 0)
}

impl Model {
    /// Loads a model without the process cache; [`model`] is the usual way.
    pub fn load(path: &Path) -> Result<Self> {
        ensure!(
            path.is_file(),
            "No ONNX model at {}: paths are relative to the project, and an exported \
             service includes the files its blocks name.",
            path.display()
        );
        let mut builder = Session::builder()
            .map_err(|e| anyhow!("{e}"))?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(|e| anyhow!("{e}"))?;
        if let Some(threads) = threads() {
            builder = builder
                .with_intra_threads(threads)
                .map_err(|e| anyhow!("{e}"))?;
        }
        let session = builder
            .commit_from_file(path)
            .map_err(|e| anyhow!("{e}"))
            .with_context(|| format!("ONNX Runtime could not load {}", path.display()))?;
        let inputs = session
            .inputs()
            .iter()
            .map(|input| (input.name().to_string(), input.dtype().tensor_type()))
            .collect();
        let outputs = session
            .outputs()
            .iter()
            .map(|output| output.name().to_string())
            .collect();
        Ok(Self {
            path: path.to_path_buf(),
            session: Mutex::new(session),
            inputs,
            outputs,
        })
    }

    /// The model's input names, in order.
    pub fn inputs(&self) -> Vec<&str> {
        self.inputs.iter().map(|(name, _)| name.as_str()).collect()
    }

    /// The model's output names, in order.
    pub fn outputs(&self) -> Vec<&str> {
        self.outputs.iter().map(String::as_str).collect()
    }

    /// Runs the model on the `features` columns of `frame` and returns its outputs, one
    /// column per output (`label`), or one per class or dimension (`probabilities_0`, ...).
    ///
    /// A model with one input gets the features as a matrix, one row per row of the
    /// frame, in the order given. A model with several inputs gets each feature as the
    /// input of the same name, which is how scikit-learn pipelines with encoders export.
    pub fn predict(&self, frame: &DataFrame, features: &[&str]) -> Result<DataFrame> {
        ensure!(
            !features.is_empty(),
            "predict needs at least one feature column"
        );
        let rows = frame.height();
        let mut inputs: Vec<(String, DynValue)> = Vec::new();
        if self.inputs.len() == 1 {
            let (name, ty) = &self.inputs[0];
            inputs.push((name.clone(), matrix(frame, features, *ty, &self.path)?));
        } else {
            for (name, ty) in &self.inputs {
                ensure!(
                    features.contains(&name.as_str()),
                    "{} has inputs {}; pass each as a feature column of the same name",
                    self.path.display(),
                    self.inputs().join(", ")
                );
                inputs.push((
                    name.clone(),
                    matrix(frame, &[name.as_str()], *ty, &self.path)?,
                ));
            }
        }
        let mut session = self
            .session
            .lock()
            .map_err(|_| anyhow!("model session poisoned"))?;
        let results = session
            .run(inputs)
            .map_err(|e| anyhow!("{e}"))
            .with_context(|| format!("{} failed on {rows} rows", self.path.display()))?;
        let mut columns: Vec<Column> = Vec::new();
        for name in &self.outputs {
            let value = &results[name.as_str()];
            columns.extend(output_columns(name, value, rows, &self.path)?);
        }
        Ok(DataFrame::new(rows, columns)?)
    }
}

/// The feature columns as one tensor of the input's element type, shaped [rows, columns].
fn matrix(
    frame: &DataFrame,
    features: &[&str],
    ty: Option<TensorElementType>,
    path: &Path,
) -> Result<DynValue> {
    let rows = frame.height();
    let shape = vec![rows as i64, features.len() as i64];
    let columns = features
        .iter()
        .map(|name| {
            frame
                .column(name)
                .map_err(|_| anyhow!("no feature column {name}"))
                .map(|c| c.as_materialized_series().clone())
        })
        .collect::<Result<Vec<Series>>>()?;
    Ok(match ty {
        Some(TensorElementType::Float32) | None => {
            let data = row_major(&columns, rows, |s| {
                floats(s).map(|v| v.into_iter().map(|x| x as f32).collect())
            })?;
            Tensor::from_array((shape, data))
                .map_err(|e| anyhow!("{e}"))?
                .into_dyn()
        }
        Some(TensorElementType::Float64) => {
            let data = row_major(&columns, rows, floats)?;
            Tensor::from_array((shape, data))
                .map_err(|e| anyhow!("{e}"))?
                .into_dyn()
        }
        Some(TensorElementType::Int64) => {
            let data = row_major(&columns, rows, integers)?;
            Tensor::from_array((shape, data))
                .map_err(|e| anyhow!("{e}"))?
                .into_dyn()
        }
        Some(TensorElementType::String) => {
            let data = row_major(&columns, rows, strings)?;
            Tensor::from_string_array((shape, data.as_slice()))
                .map_err(|e| anyhow!("{e}"))?
                .into_dyn()
        }
        Some(other) => bail!(
            "{} takes {other:?} input; predict feeds float, double, int64 and string inputs",
            path.display()
        ),
    })
}

fn row_major<T: Clone>(
    columns: &[Series],
    rows: usize,
    values: impl Fn(&Series) -> Result<Vec<T>>,
) -> Result<Vec<T>> {
    let values = columns
        .iter()
        .map(&values)
        .collect::<Result<Vec<Vec<T>>>>()?;
    let mut data = Vec::with_capacity(rows * values.len());
    for row in 0..rows {
        for column in &values {
            data.push(column[row].clone());
        }
    }
    Ok(data)
}

/// Floats, with missing values as NaN, which tree models read as missing.
fn floats(series: &Series) -> Result<Vec<f64>> {
    let cast = series.cast(&DataType::Float64)?;
    Ok(cast.f64()?.iter().map(|v| v.unwrap_or(f64::NAN)).collect())
}

fn integers(series: &Series) -> Result<Vec<i64>> {
    ensure!(
        series.null_count() == 0,
        "feature {} has missing values; the model takes integers, which have no missing value",
        series.name()
    );
    let cast = series.cast(&DataType::Int64)?;
    Ok(cast.i64()?.iter().map(|v| v.unwrap_or_default()).collect())
}

fn strings(series: &Series) -> Result<Vec<String>> {
    let cast = series.cast(&DataType::String)?;
    Ok(cast
        .str()?
        .iter()
        .map(|v| v.unwrap_or_default().to_string())
        .collect())
}

/// One output as columns: [rows] or [rows, 1] is one column, [rows, k] is k columns.
fn output_columns(name: &str, value: &DynValue, rows: usize, path: &Path) -> Result<Vec<Column>> {
    let dtype = value.dtype();
    let Some(ty) = dtype.tensor_type() else {
        bail!(
            "{} output {name} is a {}, not a tensor. A scikit-learn classifier exported with \
             skl2onnx returns its probabilities as a list of maps unless it is converted with \
             options={{id(model): {{'zipmap': False}}}}.",
            path.display(),
            match dtype {
                ValueType::Sequence(_) => "sequence",
                ValueType::Map { .. } => "map",
                _ => "non-tensor value",
            }
        );
    };
    let shape: Vec<i64> = value.shape().iter().copied().collect();
    ensure!(
        shape.first().copied() == Some(rows as i64),
        "{} output {name} has shape {shape:?}, not one row per input row ({rows})",
        path.display()
    );
    let width = shape.iter().skip(1).product::<i64>().max(1) as usize;
    let names: Vec<String> = if width == 1 {
        vec![name.to_string()]
    } else {
        (0..width).map(|k| format!("{name}_{k}")).collect()
    };
    macro_rules! split {
        ($t:ty, $build:expr) => {{
            let (_, data) = value
                .try_extract_tensor::<$t>()
                .map_err(|e| anyhow!("{e}"))?;
            names
                .iter()
                .enumerate()
                .map(|(k, column)| {
                    let values: Vec<_> = (0..rows).map(|row| data[row * width + k]).collect();
                    $build(column.as_str(), values)
                })
                .collect()
        }};
    }
    Ok(match ty {
        TensorElementType::Float32 => split!(f32, |n: &str, v: Vec<f32>| Column::new(n.into(), v)),
        TensorElementType::Float64 => split!(f64, |n: &str, v: Vec<f64>| Column::new(n.into(), v)),
        TensorElementType::Int64 => split!(i64, |n: &str, v: Vec<i64>| Column::new(n.into(), v)),
        TensorElementType::Int32 => split!(i32, |n: &str, v: Vec<i32>| Column::new(n.into(), v)),
        TensorElementType::Bool => split!(bool, |n: &str, v: Vec<bool>| Column::new(n.into(), v)),
        TensorElementType::String => {
            let (_, data) = value.try_extract_strings().map_err(|e| anyhow!("{e}"))?;
            names
                .iter()
                .enumerate()
                .map(|(k, column)| {
                    let values: Vec<&str> = (0..rows)
                        .map(|row| data[row * width + k].as_str())
                        .collect();
                    Column::new(column.as_str().into(), values)
                })
                .collect()
        }
        other => bail!(
            "{} output {name} is {other:?}, which predict does not read",
            path.display()
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_floats_become_nan_and_missing_integers_are_an_error() {
        let series = Series::new("x".into(), [Some(1.5f64), None]);
        let values = floats(&series).unwrap();
        assert_eq!(values[0], 1.5);
        assert!(values[1].is_nan());

        let counts = Series::new("n".into(), [Some(1i64), None]);
        let error = integers(&counts).err().unwrap().to_string();
        assert!(error.contains("feature n has missing values"), "{error}");
    }

    #[test]
    fn features_are_laid_out_one_row_after_another() {
        let a = Series::new("a".into(), [1.0f64, 2.0]);
        let b = Series::new("b".into(), [10.0f64, 20.0]);
        assert_eq!(
            row_major(&[a, b], 2, floats).unwrap(),
            vec![1.0, 10.0, 2.0, 20.0]
        );
    }
}
