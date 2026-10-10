use std::path::Path;

use mage::prelude::*;
use mage::{run_job, test};
use serde_json::json;

fn job(dir: &Path, block_type: &str, inputs: serde_json::Value, variables: serde_json::Value) {
    std::fs::write(
        dir.join("job.json"),
        serde_json::to_vec(&json!({
            "api_version": 1,
            "block_uuid": "block",
            "block_type": block_type,
            "inputs": inputs,
            "variables": variables,
            "output_dir": "out",
        }))
        .unwrap(),
    )
    .unwrap();
}

fn write_ipc(dir: &Path, name: &str, mut frame: DataFrame) {
    let file = std::fs::File::create(dir.join(name)).unwrap();
    IpcWriter::new(file).finish(&mut frame).unwrap();
}

fn result(dir: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(dir.join("result.json")).unwrap()).unwrap()
}

fn read_output(dir: &Path, index: usize) -> DataFrame {
    LazyFrame::scan_ipc(
        PlRefPath::try_from_path(&dir.join(format!("out/output_{index}.arrow"))).unwrap(),
        Default::default(),
        Default::default(),
    )
    .unwrap()
    .collect()
    .unwrap()
}

fn orders() -> DataFrame {
    df!("id" => [1i64, 2, 3, 4], "amount" => [10.0f64, -2.0, 7.5, 0.0]).unwrap()
}

fn positive(orders: LazyFrame, vars: Vars) -> Result<LazyFrame> {
    let minimum: f64 = vars.get_or("minimum", 0.0)?;
    Ok(orders.filter(col("amount").gt(lit(minimum))))
}

fn test_has_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "No rows");
    Ok(())
}

fn test_too_strict(output: &DataFrame) -> bool {
    output.height() > 100
}

#[test]
fn a_lazy_transformer_reads_vars_writes_its_output_and_runs_tests() {
    let dir = tempfile::tempdir().unwrap();
    write_ipc(dir.path(), "in.arrow", orders());
    job(
        dir.path(),
        "transformer",
        json!([{"kind": "frame", "format": "ipc", "path": "in.arrow"}]),
        json!({"minimum": 5.0}),
    );
    run_job(
        positive,
        vec![
            test("test_has_rows", test_has_rows),
            test("test_too_strict", test_too_strict),
        ],
        dir.path(),
    )
    .unwrap();

    let output = read_output(dir.path(), 0);
    assert_eq!(
        output.column("id").unwrap().i64().unwrap().to_vec(),
        [Some(1), Some(3)]
    );
    let result = result(dir.path());
    assert_eq!(result["outputs"][0]["kind"], "frame");
    assert_eq!(result["tests"][0]["passed"], true);
    assert_eq!(result["tests"][1]["passed"], false);
    assert_eq!(result["tests"][1]["message"], "The test returned false");
}

#[test]
fn several_inputs_outputs_and_json() {
    let dir = tempfile::tempdir().unwrap();
    write_ipc(dir.path(), "a.arrow", orders());
    std::fs::write(dir.path().join("b.json"), br#"{"rate": 2}"#).unwrap();
    job(
        dir.path(),
        "transformer",
        json!([
            {"kind": "frame", "format": "ipc", "path": "a.arrow"},
            {"kind": "json", "path": "b.json"},
            {"kind": "empty"},
        ]),
        json!({}),
    );
    fn block(
        orders: DataFrame,
        settings: Value,
        missing: Option<LazyFrame>,
    ) -> Result<(DataFrame, Value)> {
        ensure!(missing.is_none());
        let rate = settings["rate"].as_f64().context("rate")?;
        let doubled = orders
            .lazy()
            .with_column((col("amount") * lit(rate)).alias("amount"))
            .collect()?;
        Ok((doubled, json!({"rows": 4})))
    }
    run_job(block, vec![], dir.path()).unwrap();
    let output = read_output(dir.path(), 0);
    assert_eq!(
        output.column("amount").unwrap().f64().unwrap().get(0),
        Some(20.0)
    );
    let result = result(dir.path());
    assert_eq!(result["outputs"][0]["rows"], 4);
    assert_eq!(result["outputs"][1]["kind"], "json");
}

#[test]
fn a_conditional_returns_a_decision() {
    let dir = tempfile::tempdir().unwrap();
    job(
        dir.path(),
        "conditional",
        json!([]),
        json!({"enabled": true}),
    );
    run_job(|vars: Vars| vars.get::<bool>("enabled"), vec![], dir.path()).unwrap();
    assert_eq!(
        result(dir.path())["outputs"][0],
        json!({"kind": "decision", "value": true})
    );

    let dir = tempfile::tempdir().unwrap();
    job(dir.path(), "conditional", json!([]), json!({}));
    let error = run_job(|| json!({"not": "a bool"}), vec![], dir.path()).unwrap_err();
    assert!(error.to_string().contains("returns a bool"), "{error}");
}

#[test]
fn errors_name_the_problem() {
    let dir = tempfile::tempdir().unwrap();
    job(dir.path(), "transformer", json!([]), json!({}));
    let error = run_job(|orders: LazyFrame| orders, vec![], dir.path()).unwrap_err();
    assert!(
        error.to_string().contains("has 0 upstream outputs"),
        "{error}"
    );

    let dir = tempfile::tempdir().unwrap();
    job(
        dir.path(),
        "transformer",
        json!([]),
        json!({"minimum": "five"}),
    );
    let error = run_job(
        |vars: Vars| vars.get::<f64>("minimum").map(|_| ()),
        vec![],
        dir.path(),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("Variable minimum is \"five\""),
        "{error:#}"
    );

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("x.json"), b"[1]").unwrap();
    job(
        dir.path(),
        "transformer",
        json!([{"kind": "json", "path": "x.json"}]),
        json!({}),
    );
    let error = run_job(|frame: LazyFrame| frame, vec![], dir.path()).unwrap_err();
    assert!(error.to_string().contains("is not a table"), "{error}");
}

#[test]
fn context_works_on_polars_results() {
    // The prelude makes anyhow's context the only one in scope.
    fn read() -> Result<DataFrame> {
        LazyFrame::scan_parquet(PlRefPath::from("/missing.parquet"), Default::default())
            .context("Opening the file")?
            .collect()
            .context("Reading the file")
    }
    let message = format!("{:#}", read().unwrap_err());
    assert!(
        message.starts_with("Opening the file: ") || message.starts_with("Reading the file: "),
        "{message}"
    );
}

#[test]
fn an_error_reports_each_cause_once() {
    let dir = tempfile::tempdir().unwrap();
    write_ipc(dir.path(), "in.arrow", orders());
    job(
        dir.path(),
        "transformer",
        json!([{"kind": "frame", "format": "ipc", "path": "in.arrow"}]),
        json!({}),
    );
    let error = run_job(
        |orders: LazyFrame| {
            orders
                .select([col("missing")])
                .collect()
                .context("Selecting")
        },
        vec![],
        dir.path(),
    )
    .unwrap_err();
    let text = mage::runtime::describe(&error);
    assert!(text.starts_with("Selecting\n\nCaused by:"), "{text}");
    assert_eq!(
        text.matches("unable to find column \"missing\"").count(),
        1,
        "{text}"
    );
}
