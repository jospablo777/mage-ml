//! mage::onnx on small scikit-learn models exported with skl2onnx (tests/fixtures).
#![cfg(feature = "onnx")]

use mage::polars::prelude::*;

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn a_regressor_predicts_from_a_feature_matrix() {
    let frame =
        df!("x0" => [0.0f64, 1.0, 2.0], "x1" => [0.0f64, 0.0, 3.0], "id" => [1, 2, 3]).unwrap();
    let model = mage::onnx::model(fixture("regressor.onnx")).unwrap();
    assert_eq!(model.inputs().len(), 1);

    let predictions = model.predict(&frame, &["x0", "x1"]).unwrap();

    let values: Vec<f32> = predictions
        .column("variable")
        .unwrap()
        .f32()
        .unwrap()
        .into_no_null_iter()
        .collect();
    for (got, expected) in values.iter().zip([1.0f32, 4.0, 1.0]) {
        assert!((got - expected).abs() < 1e-4, "{values:?}");
    }
}

#[test]
fn a_classifier_gives_labels_and_one_column_per_class() {
    let frame = df!("a" => [0.0f64, 5.0], "b" => [5.0f64, 0.0]).unwrap();
    let model = mage::onnx::model(fixture("classifier.onnx")).unwrap();

    let predictions = model.predict(&frame, &["a", "b"]).unwrap();

    let labels: Vec<i64> = predictions
        .column("label")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(labels, vec![0, 1]);
    let positive: Vec<f32> = predictions
        .column("probabilities_1")
        .unwrap()
        .f32()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert!(positive[0] < 0.5 && positive[1] > 0.5, "{positive:?}");
    assert!(predictions.column("probabilities_0").is_ok());
}

#[test]
fn a_pipeline_takes_each_column_as_its_own_input_text_included() {
    let frame = df!("city" => ["paris", "lima"], "amount" => [1.0f64, 4.0]).unwrap();
    let model = mage::onnx::model(fixture("pipeline.onnx")).unwrap();
    assert_eq!(model.inputs(), vec!["city", "amount"]);

    let predictions = model.predict(&frame, &["city", "amount"]).unwrap();

    let values: Vec<f32> = predictions
        .column("variable")
        .unwrap()
        .f32()
        .unwrap()
        .into_no_null_iter()
        .collect();
    for (got, expected) in values.iter().zip([10.0f32, 40.0]) {
        assert!((got - expected).abs() < 1e-3, "{values:?}");
    }
}

#[test]
fn errors_name_the_problem() {
    let missing = mage::onnx::model("no/such/model.onnx")
        .err()
        .unwrap()
        .to_string();
    assert!(
        missing.contains("No ONNX model at no/such/model.onnx"),
        "{missing}"
    );

    let frame = df!("a" => [0.0f64], "b" => [5.0f64]).unwrap();
    let zipmap = mage::onnx::model(fixture("classifier_zipmap.onnx")).unwrap();
    let error = zipmap
        .predict(&frame, &["a", "b"])
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("'zipmap': False"), "{error}");

    let model = mage::onnx::model(fixture("pipeline.onnx")).unwrap();
    let error = model.predict(&frame, &["a"]).err().unwrap().to_string();
    assert!(error.contains("has inputs city, amount"), "{error}");
}

#[test]
fn a_model_is_loaded_once_per_process() {
    let first = mage::onnx::model(fixture("regressor.onnx")).unwrap();
    let second = mage::onnx::model(fixture("regressor.onnx")).unwrap();
    assert!(std::sync::Arc::ptr_eq(&first, &second));
}
