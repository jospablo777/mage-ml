//! Monitors: column profiles and their comparison across runs, by mage-checks, the crate
//! exported services check them with, so MageML runs and services compute the same.
use serde_json::Value;

use crate::error::AtlasError;

/// Column profiles of a Parquet or Arrow IPC file, for monitoring.
pub fn profile(path: &str, request: &str) -> Result<Value, AtlasError> {
    Ok(mage_checks::profile::profile(path, request)?)
}

/// Each check of a monitor (its settings as JSON) for a profile against a baseline's.
pub fn compare(
    monitor: &str,
    current: &str,
    baseline: Option<&str>,
    execution_date: Option<&str>,
) -> Result<Value, AtlasError> {
    let monitor = mage_checks::monitor::Monitor::parse(monitor)?;
    let parse = |text: &str, what: &str| -> Result<Value, AtlasError> {
        serde_json::from_str(text)
            .map_err(|error| AtlasError::Invalid(format!("Invalid {what}: {error}")))
    };
    let current = parse(current, "profile")?;
    let baseline = baseline.map(|b| parse(b, "baseline profile")).transpose()?;
    let execution_date = execution_date
        .map(|text| {
            chrono::DateTime::parse_from_rfc3339(text)
                .map(|time| time.with_timezone(&chrono::Utc))
                .map_err(|error| AtlasError::Invalid(format!("Invalid execution date: {error}")))
        })
        .transpose()?;
    Ok(mage_checks::monitor::compare(
        &monitor,
        &current,
        baseline.as_ref(),
        execution_date,
    ))
}

/// One baseline record from several earlier runs' records (JSON list, oldest first).
pub fn merge(records: &str) -> Result<Value, AtlasError> {
    let records: Vec<Value> = serde_json::from_str(records)
        .map_err(|error| AtlasError::Invalid(format!("Invalid monitor records: {error}")))?;
    Ok(mage_checks::monitor::merge(&records))
}

/// The performance metrics of the monitor's labeled rows in a file.
pub fn performance(monitor: &str, path: &str) -> Result<Value, AtlasError> {
    let monitor = mage_checks::monitor::Monitor::parse(monitor)?;
    Ok(mage_checks::monitor::performance(
        &monitor.performance,
        path,
    ))
}
