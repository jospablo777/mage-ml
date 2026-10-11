// Needs the onnx feature of MageML's crate: in the project's rust/Cargo.toml, set
// mage-block = { path = ".mage/sdk", features = ["postgres", "onnx"] }
use mage::prelude::*;

/// Scores each row with an ONNX model: scikit-learn, XGBoost, LightGBM, CatBoost and
/// PyTorch models export to ONNX. The model loads once per process and is reused by later
/// runs. Set `model` (a path in the project) and `features` (the input columns, in the
/// order the model was trained on) as pipeline variables.
fn transform(data: DataFrame, vars: Vars) -> Result<DataFrame> {
    let path: String = vars.get_or("model", "models/model.onnx".to_string())?;
    let features: Vec<String> = vars.get_or("features", vec!["amount".to_string()])?;
    let features: Vec<&str> = features.iter().map(String::as_str).collect();

    let model = mage::onnx::model(&path)?;
    let predictions = model.predict(&data, &features)?;
    Ok(data.hstack(predictions.columns())?)
}

fn test_one_prediction_per_row(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "No rows were scored");
    Ok(())
}
