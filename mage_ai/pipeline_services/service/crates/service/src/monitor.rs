//! Monitors of block outputs: after a block ran, each monitor that watches it profiles the
//! output and compares it with a baseline, by mage-checks, the code MageML runs monitors
//! with (feature `checks`). A `previous` baseline is the service's own earlier runs (the
//! last `rolling` of them), kept as one record per run under
//! `<data>/monitors/<pipeline>/<monitor>/`; a `run:<id>` baseline is MageML's record of that
//! run, exported with the service.
//!
//! The run log has a summary of each monitor, the block's output folder `monitors.json`,
//! and `/metrics` the last result of each monitor and the value of each check. A failed
//! monitor fails the block only with `fail_block`, without retries: the same data fails
//! the same checks. Evidently and NannyML, which run in MageML's Python, are not run.
use std::path::Path;

use mage_service_core::manifest::Block;
use serde_json::Value;
#[cfg(feature = "checks")]
use serde_json::json;

use crate::log::RunLog;
use crate::service::Service;

/// Records kept per monitor; the oldest go first. A rolling baseline reads the last ones,
/// and the first record's bin edges stay as the reference.
#[cfg(feature = "checks")]
const KEPT_RECORDS: usize = 500;

/// Where a monitor's records are kept.
pub fn records_dir(service: &Service, pipeline: &str, monitor: &str) -> std::path::PathBuf {
    service
        .data_dir
        .join("monitors")
        .join(crate::executor::sanitize(pipeline))
        .join(crate::executor::sanitize(monitor))
}

/// Runs the block's monitors on a blocking thread: they read the whole output. Err with the
/// summaries of the failed monitors that fail the block.
pub async fn check_blocking(
    service: std::sync::Arc<Service>,
    pipeline: &str,
    block: &Block,
    outputs: &[Value],
    output_dir: &Path,
    log: &RunLog,
    run_id: &str,
) -> Result<(), String> {
    if block.monitors.is_empty() {
        return Ok(());
    }
    let (pipeline, block, outputs) = (pipeline.to_string(), block.clone(), outputs.to_vec());
    let (output_dir, log, run_id) = (output_dir.to_path_buf(), log.clone(), run_id.to_string());
    tokio::task::spawn_blocking(move || {
        check(
            &service,
            &pipeline,
            &block,
            &outputs,
            &output_dir,
            &log,
            &run_id,
        )
    })
    .await
    .unwrap_or_else(|error| Err(format!("The monitors stopped: {error}")))
}

#[cfg(not(feature = "checks"))]
fn check(
    _service: &Service,
    _pipeline: &str,
    block: &Block,
    _outputs: &[Value],
    _output_dir: &Path,
    log: &RunLog,
    _run_id: &str,
) -> Result<(), String> {
    for monitor in &block.monitors {
        log.warn(
            Some(&block.uuid),
            &format!(
                "Monitor {} is not run: this engine was built without checks. An exported \
                 service's image has them.",
                monitor.name
            ),
        );
    }
    Ok(())
}

#[cfg(feature = "checks")]
fn check(
    service: &Service,
    pipeline: &str,
    block: &Block,
    outputs: &[Value],
    output_dir: &Path,
    log: &RunLog,
    run_id: &str,
) -> Result<(), String> {
    let mut records = Vec::new();
    let mut failing = Vec::new();
    for monitor in &block.monitors {
        let record = match run_one(service, pipeline, monitor, outputs, run_id) {
            Ok(record) => record,
            Err(error) => json!({
                "monitor": monitor.name, "result": "error", "error": error, "checks": [],
                "run_id": run_id,
            }),
        };
        let text = summary(&record);
        match record["result"].as_str() {
            Some("pass") | Some("hold") => log.info(Some(&block.uuid), &text),
            _ => log.warn(Some(&block.uuid), &text),
        }
        service.observe_monitor(pipeline, &block.uuid, &monitor.name, &record);
        if record["result"] == "fail" && record["fail_block"] == true {
            failing.push(text);
        }
        let mut shown = record;
        if let Some(object) = shown.as_object_mut() {
            object.remove("profile");
        }
        records.push(shown);
    }
    let text = serde_json::to_string_pretty(&records).unwrap_or_default();
    let _ = std::fs::write(output_dir.join("monitors.json"), text);
    if failing.is_empty() {
        Ok(())
    } else {
        Err(failing.join("\n"))
    }
}

#[cfg(feature = "checks")]
fn saved(directory: &Path) -> Vec<(std::path::PathBuf, Value)> {
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(directory)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "json"))
                .collect()
        })
        .unwrap_or_default();
    // Named <milliseconds, zero-padded>-<run>.json: by name is by time.
    files.sort();
    files
        .into_iter()
        .filter_map(|path| {
            let text = std::fs::read_to_string(&path).ok()?;
            Some((path, serde_json::from_str(&text).ok()?))
        })
        .collect()
}

#[cfg(feature = "checks")]
fn edges(record: Option<&Value>) -> Value {
    let mut edges = serde_json::Map::new();
    for column in record
        .and_then(|r| r["profile"]["columns"].as_array())
        .into_iter()
        .flatten()
    {
        if let (Some(name), Some(values)) = (column["name"].as_str(), column.get("edges"))
            && values.as_array().is_some_and(|v| !v.is_empty())
        {
            edges.insert(name.to_string(), values.clone());
        }
    }
    Value::Object(edges)
}

#[cfg(feature = "checks")]
fn run_one(
    service: &Service,
    pipeline: &str,
    check: &mage_service_core::manifest::MonitorCheck,
    outputs: &[Value],
    run_id: &str,
) -> Result<Value, String> {
    use mage_checks::monitor;

    let settings =
        monitor::Monitor::parse(&check.settings.to_string()).map_err(|error| error.to_string())?;
    let path = outputs
        .get(check.output)
        .and_then(|o| o.get("path"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!(
                "Monitor {}: output_{} of the block is not a table, which a monitor profiles.",
                check.name, check.output
            )
        })?;
    let directory = records_dir(service, pipeline, &check.name);
    let history = saved(&directory);
    let earlier: Vec<Value> = history.iter().map(|(_, r)| r.clone()).collect();
    let (baseline, reference) = match &check.baseline {
        Some(fixed) => (Some(fixed.clone()), Some(fixed.clone())),
        None => {
            let window = &earlier[settings.baseline_window(earlier.len())];
            let baseline = (!window.is_empty()).then(|| monitor::merge(window));
            (baseline, earlier.first().cloned())
        }
    };
    let request = settings.profile_request(&edges(reference.as_ref()));
    let mut profile = mage_checks::profile::profile(path, &request.to_string())
        .map_err(|error| format!("Monitor {}: {error}", check.name))?;
    if settings.has_performance() {
        profile["performance"] = monitor::performance(&settings.performance, path);
    }
    let result = monitor::compare(
        &settings,
        &profile,
        baseline.as_ref().map(|b| &b["profile"]),
        Some(chrono::Utc::now()),
    );
    let now = chrono::Utc::now();
    let record = json!({
        "monitor": check.name,
        "pipeline_run_id": run_id,
        "run_id": run_id,
        "baseline_run_id": baseline.as_ref().map(|b| b["pipeline_run_id"].clone()),
        "baseline_runs": baseline.as_ref().map(|b| b["baseline_runs"].clone()),
        "checked_at": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "result": result["result"],
        "checks": result["checks"],
        "fail_block": settings.fail_block,
        "profile": profile,
    });
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("Monitor {}: {error}", check.name))?;
    let name = format!(
        "{:020}-{}.json",
        now.timestamp_millis().max(0),
        crate::executor::sanitize(run_id)
    );
    std::fs::write(
        directory.join(name),
        serde_json::to_vec(&record).unwrap_or_default(),
    )
    .map_err(|error| format!("Monitor {}: {error}", check.name))?;
    // The first record stays: its bin edges are every later run's.
    if history.len() + 1 > KEPT_RECORDS {
        for (path, _) in history
            .iter()
            .skip(1)
            .take(history.len() + 1 - KEPT_RECORDS)
        {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(record)
}

/// The monitor's result and its checks that did not pass, as MageML prints them.
#[cfg_attr(not(feature = "checks"), allow(dead_code))]
pub fn summary(record: &Value) -> String {
    let name = record["monitor"].as_str().unwrap_or("");
    if let Some(error) = record["error"].as_str() {
        return format!("Monitor {name}: the check failed: {error}");
    }
    let against = match &record["baseline_run_id"] {
        Value::Null => "no baseline".to_string(),
        Value::String(run) => format!("run {run}"),
        other => format!("run {other}"),
    };
    let mut lines = vec![format!(
        "Monitor {name}: {} (against {against}).",
        record["result"].as_str().unwrap_or("")
    )];
    let mut passed = Vec::new();
    for check in record["checks"].as_array().into_iter().flatten() {
        let mut label = check["check"].as_str().unwrap_or("").to_string();
        if let Some(column) = check["column"].as_str() {
            label.push(' ');
            label.push_str(column);
        }
        if check["result"] == "pass" {
            if let Some(segment) = check["segment"].as_str() {
                label.push_str(&format!(" [{segment}]"));
            }
            passed.push(label);
        } else {
            lines.push(format!(
                "- {label}: {}; {}",
                check["result"].as_str().unwrap_or(""),
                check["detail"].as_str().unwrap_or("")
            ));
        }
    }
    if !passed.is_empty() {
        lines.push(format!("- passed: {}", passed.join(", ")));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn the_summary_names_the_checks_that_passed() {
        let record = json!({"monitor": "m", "result": "warn", "baseline_run_id": 3, "checks": [
            {"check": "rows", "column": null, "result": "pass", "detail": "48 rows"},
            {"check": "missing", "column": "revenue", "result": "pass", "detail": "0%"},
            {"check": "drift", "column": "orders", "result": "warn", "detail": "0.12"},
        ]});
        assert_eq!(
            summary(&record),
            "Monitor m: warn (against run 3).\n- drift orders: warn; 0.12\n- passed: rows, \
             missing revenue"
        );
    }
}
