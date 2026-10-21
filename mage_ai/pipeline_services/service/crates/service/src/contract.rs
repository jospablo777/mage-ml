//! Data contracts on block outputs: after a block ran, its output is checked against the
//! contract MageML exported with it, by mage-checks, the checker of MageML runs (feature
//! `checks`). The report is written next to the output as `contract.json`.
//!
//! A violation with enforcement `fail` fails the block, without retries: the same data
//! breaks the same rules on every attempt. With `warn`, the run log has the violations and
//! the run goes on. An engine built without the feature logs that the contract is not
//! checked.
use std::path::Path;

use mage_service_core::manifest::{Block, ContractEnforcement};
use serde_json::Value;

use crate::log::RunLog;

/// Examples of breaking rows kept per rule, as in MageML runs.
#[cfg(feature = "checks")]
const EXAMPLES: usize = 10;

/// [`check`] on a blocking thread: it reads the whole output, which must not hold up the
/// runtime's threads.
pub async fn check_blocking(
    block: &Block,
    outputs: &[Value],
    output_dir: &Path,
    log: &RunLog,
) -> Result<(), String> {
    if block.contract.is_none() {
        return Ok(());
    }
    let (block, outputs) = (block.clone(), outputs.to_vec());
    let (output_dir, log) = (output_dir.to_path_buf(), log.clone());
    tokio::task::spawn_blocking(move || check(&block, &outputs, &output_dir, &log))
        .await
        .unwrap_or_else(|error| Err(format!("The contract check stopped: {error}")))
}

/// Checks the block's output; Err with the violations when they fail the block.
pub fn check(
    block: &Block,
    outputs: &[Value],
    output_dir: &Path,
    log: &RunLog,
) -> Result<(), String> {
    let Some(contract) = &block.contract else {
        return Ok(());
    };
    let Some(output) = outputs.get(contract.output) else {
        return violated(
            block,
            contract.enforcement,
            log,
            format!(
                "Contract {}: the block has no output_{} to check.",
                contract.name, contract.output
            ),
        );
    };
    let Some(path) = output.get("path").and_then(Value::as_str) else {
        return violated(
            block,
            contract.enforcement,
            log,
            format!(
                "Contract {}: output_{} is not a table, which a contract checks.",
                contract.name, contract.output
            ),
        );
    };
    validate(block, path, output_dir, log)
}

#[cfg(feature = "checks")]
fn validate(block: &Block, path: &str, output_dir: &Path, log: &RunLog) -> Result<(), String> {
    let contract = block.contract.as_ref().expect("checked by the caller");
    let report = mage_checks::contract::validate(path, &contract.document.to_string(), EXAMPLES)
        .map_err(|error| format!("Contract {}: the check failed: {error}", contract.name))?;
    let text = serde_json::to_string_pretty(&report).unwrap_or_default();
    let _ = std::fs::write(output_dir.join("contract.json"), text);
    if report.passed {
        log.info(
            Some(&block.uuid),
            &format!(
                "Contract {}: {} rows meet every rule.",
                contract.name, report.rows
            ),
        );
        return Ok(());
    }
    let broken: Vec<String> = report
        .violations
        .iter()
        .map(|v| {
            let column = v.column.as_deref().unwrap_or("table");
            format!("{column} {} ({} rows)", v.rule, v.rows)
        })
        .collect();
    violated(
        block,
        contract.enforcement,
        log,
        format!(
            "Contract {}: {} of {} rows break {} rule{}: {}.",
            contract.name,
            report
                .violations
                .iter()
                .map(|v| v.rows)
                .max()
                .unwrap_or_default(),
            report.rows,
            broken.len(),
            if broken.len() == 1 { "" } else { "s" },
            broken.join("; ")
        ),
    )
}

#[cfg(not(feature = "checks"))]
fn validate(block: &Block, _path: &str, _output_dir: &Path, log: &RunLog) -> Result<(), String> {
    let contract = block.contract.as_ref().expect("checked by the caller");
    log.warn(
        Some(&block.uuid),
        &format!(
            "Contract {} is not checked: this engine was built without contracts. An \
             exported service's image has them.",
            contract.name
        ),
    );
    Ok(())
}

fn violated(
    block: &Block,
    enforcement: ContractEnforcement,
    log: &RunLog,
    message: String,
) -> Result<(), String> {
    match enforcement {
        ContractEnforcement::Fail => Err(message),
        ContractEnforcement::Warn => {
            log.warn(Some(&block.uuid), &message);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;

    fn block(enforcement: &str) -> Block {
        serde_json::from_value(json!({
            "uuid": "score",
            "type": "transformer",
            "language": "python",
            "file": "transformers/score.py",
            "sha256": "x",
            "contract": {
                "name": "customer_scores",
                "enforcement": enforcement,
                "output": 0,
                "document": {
                    "name": "customer_scores",
                    "columns": [
                        {"name": "customer_id", "type": "integer", "unique": true},
                        {"name": "score", "type": "float", "nullable": false, "max": 1},
                    ],
                },
            },
        }))
        .unwrap()
    }

    /// An output as blocks write it: uncompressed Arrow IPC, or LZ4.
    fn output(dir: &Path, rows: (Vec<i64>, Vec<Option<f64>>), lz4: bool) -> Value {
        use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
        use arrow_ipc::writer::{FileWriter, IpcWriteOptions};

        let batch = RecordBatch::try_from_iter([
            (
                "customer_id",
                Arc::new(Int64Array::from(rows.0)) as ArrayRef,
            ),
            ("score", Arc::new(Float64Array::from(rows.1)) as ArrayRef),
        ])
        .unwrap();
        let path = dir.join("output_0.arrow");
        let options = IpcWriteOptions::default()
            .try_with_compression(lz4.then_some(arrow_ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer = FileWriter::try_new_with_options(
            std::fs::File::create(&path).unwrap(),
            &batch.schema(),
            options,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        json!({"kind": "frame", "format": "ipc", "path": path})
    }

    fn log(dir: &Path) -> RunLog {
        RunLog::create(&dir.join("run.log"), "r1").unwrap()
    }

    #[test]
    fn a_missing_or_unreadable_output_breaks_a_failing_contract() {
        let dir = tempfile::tempdir().unwrap();
        let error = check(&block("fail"), &[], dir.path(), &log(dir.path())).unwrap_err();
        assert!(error.contains("no output_0"), "{error}");
        let object = json!({"kind": "object", "value": {"a": 1}});
        let error = check(
            &block("fail"),
            std::slice::from_ref(&object),
            dir.path(),
            &log(dir.path()),
        )
        .unwrap_err();
        assert!(error.contains("not a table"), "{error}");
        assert!(check(&block("warn"), &[object], dir.path(), &log(dir.path())).is_ok());
        let mut plain = block("fail");
        plain.contract = None;
        assert!(check(&plain, &[], dir.path(), &log(dir.path())).is_ok());
    }

    #[cfg(feature = "checks")]
    #[test]
    fn violations_fail_the_block_or_warn_and_a_report_is_kept() {
        for lz4 in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let bad = output(
                dir.path(),
                (vec![1, 2, 2], vec![Some(0.5), None, Some(1.5)]),
                lz4,
            );
            let error = check(
                &block("fail"),
                std::slice::from_ref(&bad),
                dir.path(),
                &log(dir.path()),
            )
            .unwrap_err();
            assert!(error.contains("Contract customer_scores"), "{error}");
            assert!(error.contains("customer_id unique (2 rows)"), "{error}");
            assert!(error.contains("score nullable (1 rows)"), "{error}");
            let report: Value = serde_json::from_str(
                &std::fs::read_to_string(dir.path().join("contract.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(report["passed"], false);
            assert_eq!(report["rows"], 3);

            assert!(check(&block("warn"), &[bad], dir.path(), &log(dir.path())).is_ok());
            let warned = std::fs::read_to_string(dir.path().join("run.log")).unwrap();
            assert!(warned.contains("\"warning\""), "{warned}");

            let good = output(dir.path(), (vec![1, 2], vec![Some(0.5), Some(1.0)]), lz4);
            assert!(check(&block("fail"), &[good], dir.path(), &log(dir.path())).is_ok());
        }
    }

    #[cfg(not(feature = "checks"))]
    #[test]
    fn an_engine_without_contracts_says_the_contract_is_not_checked() {
        let dir = tempfile::tempdir().unwrap();
        let out = output(dir.path(), (vec![1, 1], vec![None, None]), false);
        assert!(check(&block("fail"), &[out], dir.path(), &log(dir.path())).is_ok());
        let text = std::fs::read_to_string(dir.path().join("run.log")).unwrap();
        assert!(text.contains("built without contracts"), "{text}");
    }
}
