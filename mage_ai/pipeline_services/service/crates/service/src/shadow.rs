//! Champion and challengers: the comparison of a model's challengers with it, from the
//! shadow records the Python workers write (mage_ai/pipeline_services/models.py, `predict`)
//! under `<data>/shadow/<model>/<challenger>/<day>.jsonl`. Per challenger: the calls and rows
//! compared, errors and dropped calls, how often it agreed with the champion, how far apart
//! numeric scores were and their distributions, and each model's time per call.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

/// Bins of each score distribution.
const BINS: usize = 10;

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct Comparison {
    pub challenger: String,
    pub version: Option<String>,
    pub calls: u64,
    pub rows: u64,
    pub errors: u64,
    pub dropped: u64,
    pub last_error: Option<String>,
    /// Rows where the challenger's output equals the champion's.
    pub agreement: Option<f64>,
    /// For numeric outputs: the mean absolute difference and each model's mean.
    pub mean_abs_diff: Option<f64>,
    pub champion_mean: Option<f64>,
    pub challenger_mean: Option<f64>,
    /// Shared bin edges and each model's counts, for numeric outputs.
    pub edges: Vec<f64>,
    pub champion_counts: Vec<u64>,
    pub challenger_counts: Vec<u64>,
    pub champion_ms: Latency,
    pub challenger_ms: Latency,
}

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct Latency {
    pub p50: Option<f64>,
    pub p95: Option<f64>,
}

fn latency(mut values: Vec<f64>) -> Latency {
    if values.is_empty() {
        return Latency::default();
    }
    values.sort_by(f64::total_cmp);
    let at = |q: f64| values[((values.len() - 1) as f64 * q).round() as usize];
    Latency {
        p50: Some(at(0.5)),
        p95: Some(at(0.95)),
    }
}

/// The comparison of each challenger of `model`, over the records of the last `days` days
/// (by file name), up to `max_rows` rows each, newest days first.
pub fn compare(shadow_dir: &Path, model: &str, days: u32, max_rows: u64) -> Vec<Comparison> {
    let folder = shadow_dir.join(model);
    let Ok(entries) = std::fs::read_dir(&folder) else {
        return Vec::new();
    };
    let mut challengers: Vec<String> = entries
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    challengers.sort();
    let since = chrono::Utc::now().date_naive() - chrono::Days::new(u64::from(days.max(1)) - 1);
    challengers
        .into_iter()
        .map(|challenger| {
            let mut files: Vec<_> = std::fs::read_dir(folder.join(&challenger))
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| {
                    p.file_stem()
                        .and_then(|s| s.to_str())
                        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                        .is_some_and(|day| day >= since)
                })
                .collect();
            files.sort();
            files.reverse();
            summarize(challenger, &files, max_rows)
        })
        .collect()
}

fn summarize(challenger: String, files: &[std::path::PathBuf], max_rows: u64) -> Comparison {
    let mut comparison = Comparison {
        challenger,
        ..Comparison::default()
    };
    let (mut champion_ms, mut challenger_ms) = (Vec::new(), Vec::new());
    let (mut agreed, mut pairs) = (0u64, Vec::<(f64, f64)>::new());
    let mut numeric = true;
    'files: for path in files {
        let Ok(file) = std::fs::File::open(path) else {
            continue;
        };
        for line in std::io::BufReader::new(file).lines() {
            let Ok(line) = line else { break };
            let Ok(record) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            match record["kind"].as_str() {
                Some("call") => {
                    comparison.calls += 1;
                    if comparison.version.is_none() {
                        comparison.version =
                            record["challenger_version"].as_str().map(str::to_string);
                    }
                    if record["dropped"] == true {
                        comparison.dropped += 1;
                    } else if let Some(error) = record["error"].as_str() {
                        comparison.errors += 1;
                        comparison
                            .last_error
                            .get_or_insert_with(|| error.to_string());
                    }
                    if let Some(ms) = record["champion_ms"].as_f64() {
                        champion_ms.push(ms);
                    }
                    if let Some(ms) = record["challenger_ms"].as_f64() {
                        challenger_ms.push(ms);
                    }
                }
                Some("row") => {
                    if comparison.rows >= max_rows {
                        break 'files;
                    }
                    comparison.rows += 1;
                    let (mine, theirs) = (&record["champion"], &record["challenger"]);
                    if mine == theirs {
                        agreed += 1;
                    }
                    match (mine.as_f64(), theirs.as_f64()) {
                        (Some(a), Some(b)) if numeric => pairs.push((a, b)),
                        _ => numeric = false,
                    }
                }
                _ => {}
            }
        }
    }
    if comparison.rows > 0 {
        comparison.agreement = Some(agreed as f64 / comparison.rows as f64);
    }
    if numeric && !pairs.is_empty() {
        let n = pairs.len() as f64;
        comparison.mean_abs_diff = Some(pairs.iter().map(|(a, b)| (a - b).abs()).sum::<f64>() / n);
        comparison.champion_mean = Some(pairs.iter().map(|(a, _)| a).sum::<f64>() / n);
        comparison.challenger_mean = Some(pairs.iter().map(|(_, b)| b).sum::<f64>() / n);
        let low = pairs
            .iter()
            .map(|(a, b)| a.min(*b))
            .fold(f64::INFINITY, f64::min);
        let high = pairs
            .iter()
            .map(|(a, b)| a.max(*b))
            .fold(f64::NEG_INFINITY, f64::max);
        let width = if high > low {
            (high - low) / BINS as f64
        } else {
            1.0
        };
        comparison.edges = (0..=BINS).map(|i| low + width * i as f64).collect();
        let bin = |v: f64| (((v - low) / width) as usize).min(BINS - 1);
        comparison.champion_counts = vec![0; BINS];
        comparison.challenger_counts = vec![0; BINS];
        for (a, b) in &pairs {
            comparison.champion_counts[bin(*a)] += 1;
            comparison.challenger_counts[bin(*b)] += 1;
        }
    }
    comparison.champion_ms = latency(champion_ms);
    comparison.challenger_ms = latency(challenger_ms);
    comparison
}

/// The models with challengers, by name: their comparisons, for `/v1/models`.
pub fn all(
    shadow_dir: &Path,
    models: &[mage_service_core::manifest::Model],
    days: u32,
) -> BTreeMap<String, Vec<Comparison>> {
    models
        .iter()
        .filter(|m| !m.challengers.is_empty())
        .map(|m| {
            (
                m.name.clone(),
                compare(shadow_dir, &m.name, days, 1_000_000),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, challenger: &str, lines: &[Value]) {
        let folder = dir.join("churn").join(challenger);
        std::fs::create_dir_all(&folder).unwrap();
        let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(folder.join(format!("{day}.jsonl")), text).unwrap();
        // An old day is outside the window.
        std::fs::write(
            folder.join("2000-01-01.jsonl"),
            serde_json::json!({"kind": "row", "champion": 1, "challenger": 9}).to_string(),
        )
        .unwrap();
    }

    #[test]
    fn a_challenger_is_compared_with_the_champion_row_by_row() {
        let dir = tempfile::tempdir().unwrap();
        use serde_json::json;
        write(
            dir.path(),
            "v4",
            &[
                json!({"kind": "call", "rows": 4, "champion_ms": 2.0, "challenger_ms": 5.0,
                       "challenger_version": "4", "error": null}),
                json!({"kind": "row", "key": 1, "champion": 0.0, "challenger": 0.0}),
                json!({"kind": "row", "key": 2, "champion": 1.0, "challenger": 1.0}),
                json!({"kind": "row", "key": 3, "champion": 0.5, "challenger": 1.0}),
                json!({"kind": "row", "key": 4, "champion": 1.0, "challenger": 0.0}),
                json!({"kind": "call", "rows": 0, "error": "RuntimeError: broken"}),
                json!({"kind": "call", "rows": 3, "dropped": true, "error": "dropped"}),
            ],
        );
        let [v4] = compare(dir.path(), "churn", 7, 100).try_into().unwrap();
        assert_eq!(
            (v4.calls, v4.rows, v4.errors, v4.dropped),
            (3, 4, 1, 1),
            "{v4:?}"
        );
        assert_eq!(v4.version.as_deref(), Some("4"));
        assert_eq!(v4.last_error.as_deref(), Some("RuntimeError: broken"));
        assert_eq!(v4.agreement, Some(0.5));
        assert_eq!(v4.mean_abs_diff, Some(0.375));
        assert_eq!(
            (v4.champion_mean, v4.challenger_mean),
            (Some(0.625), Some(0.5))
        );
        assert_eq!(v4.champion_counts.iter().sum::<u64>(), 4);
        assert_eq!(v4.edges.len(), BINS + 1);
        assert_eq!(v4.champion_ms.p50, Some(2.0));
        assert_eq!(v4.challenger_ms.p95, Some(5.0));
    }

    #[test]
    fn labels_are_compared_by_agreement_only() {
        let dir = tempfile::tempdir().unwrap();
        use serde_json::json;
        write(
            dir.path(),
            "v5",
            &[
                json!({"kind": "row", "champion": "churn", "challenger": "churn"}),
                json!({"kind": "row", "champion": "stay", "challenger": "churn"}),
                json!({"kind": "row", "champion": "stay", "challenger": "stay"}),
            ],
        );
        let [v5] = compare(dir.path(), "churn", 1, 100).try_into().unwrap();
        assert_eq!(v5.agreement, Some(2.0 / 3.0));
        assert_eq!(v5.mean_abs_diff, None);
        assert!(v5.edges.is_empty());
        assert!(compare(dir.path(), "other", 7, 100).is_empty());
    }
}
