//! Audit records of a service's predictions: one per scored input, written by the Python
//! workers (mage_ai/pipeline_services/models.py, `audit`) under
//! `<data>/audit/<model>/<day>-<pid>.jsonl`. Each record holds the SHA-256 of the line before
//! it in its file (`previous`), so the files are chains: a removed or changed record breaks
//! its chain at that line. Here: the records of an input key, the check of every chain, and
//! the removal of days past the retention.

use std::io::BufRead;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

/// The audit files of a model, newest day first.
fn files(audit_dir: &Path, model: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(audit_dir.join(model))
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .collect();
    files.sort();
    files.reverse();
    files
}

fn day_of(path: &Path) -> Option<chrono::NaiveDate> {
    let stem = path.file_stem()?.to_str()?;
    chrono::NaiveDate::parse_from_str(stem.get(..10)?, "%Y-%m-%d").ok()
}

/// The records of `key` (compared as JSON, and as text, so `42` finds `"42"`), newest
/// days first, at most `limit`.
pub fn find(audit_dir: &Path, model: &str, key: &str, days: u32, limit: usize) -> Vec<Value> {
    let since = chrono::Utc::now().date_naive() - chrono::Days::new(u64::from(days.max(1)) - 1);
    let wanted: Option<Value> = serde_json::from_str(key).ok();
    let mut found = Vec::new();
    for path in files(audit_dir, model) {
        if day_of(&path).is_some_and(|day| day < since) {
            continue;
        }
        let Ok(file) = std::fs::File::open(&path) else {
            continue;
        };
        for line in std::io::BufReader::new(file).lines() {
            let Ok(line) = line else { break };
            // Most lines are another key's: parse only the ones that can match.
            if !line.contains(key) {
                continue;
            }
            let Ok(record) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            let matches = match &record["key"] {
                Value::String(text) => text == key,
                other => Some(other) == wanted.as_ref(),
            };
            if matches {
                found.push(record);
                if found.len() >= limit {
                    return found;
                }
            }
        }
    }
    found
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Chain {
    pub file: String,
    pub records: u64,
    /// The first line (1-based) whose `previous` is not the hash of the line before it.
    pub broken_at: Option<u64>,
}

/// Checks the chain of every file of the model.
pub fn verify(audit_dir: &Path, model: &str) -> Vec<Chain> {
    use sha2_lite::sha256_hex;

    let mut chains = Vec::new();
    for path in files(audit_dir, model).into_iter().rev() {
        let mut chain = Chain {
            file: path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
            records: 0,
            broken_at: None,
        };
        let Ok(file) = std::fs::File::open(&path) else {
            continue;
        };
        let mut previous: Option<String> = None;
        for line in std::io::BufReader::new(file).lines() {
            let Ok(line) = line else { break };
            chain.records += 1;
            let record: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
            let claimed = record["previous"].as_str().map(str::to_string);
            // The first record of a file starts its chain.
            if chain.records > 1 && claimed != previous && chain.broken_at.is_none() {
                chain.broken_at = Some(chain.records);
            }
            previous = Some(sha256_hex(line.as_bytes()));
        }
        chains.push(chain);
    }
    chains
}

/// Removes the files of days older than `days` days; returns how many.
pub fn prune(audit_dir: &Path, days: u32) -> usize {
    if days == 0 {
        return 0;
    }
    let cutoff = chrono::Utc::now().date_naive() - chrono::Days::new(u64::from(days));
    let mut removed = 0;
    for model in std::fs::read_dir(audit_dir)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
    {
        for path in files(audit_dir, &model.file_name().to_string_lossy()) {
            if day_of(&path).is_some_and(|day| day < cutoff) && std::fs::remove_file(&path).is_ok()
            {
                removed += 1;
            }
        }
    }
    removed
}

/// SHA-256 with ring, which the engine has for SigV4.
mod sha2_lite {
    pub fn sha256_hex(bytes: &[u8]) -> String {
        let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
        digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Lines chained as models.audit writes them.
    fn chained(records: &[Value]) -> String {
        let mut previous: Option<String> = None;
        let mut text = String::new();
        for record in records {
            let mut record = record.clone();
            record["previous"] = json!(previous);
            let line = record.to_string();
            previous = Some(sha2_lite::sha256_hex(line.as_bytes()));
            text.push_str(&line);
            text.push('\n');
        }
        text
    }

    fn write(dir: &Path, name: &str, text: &str) {
        std::fs::create_dir_all(dir.join("churn")).unwrap();
        std::fs::write(dir.join("churn").join(name), text).unwrap();
    }

    #[test]
    fn records_are_found_by_key_and_their_chains_checked() {
        let dir = tempfile::tempdir().unwrap();
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let records = [
            json!({"key": 42, "output": 0.9}),
            json!({"key": "a-1", "output": 0.1}),
            json!({"key": 42, "output": 0.8}),
        ];
        write(
            dir.path(),
            &format!("{today}-100.jsonl"),
            &chained(&records),
        );
        write(
            dir.path(),
            "2000-01-01-7.jsonl",
            &chained(&[json!({"key": 42, "output": 0.0})]),
        );

        let found = find(dir.path(), "churn", "42", 30, 10);
        assert_eq!(
            found
                .iter()
                .map(|r| r["output"].clone())
                .collect::<Vec<_>>(),
            vec![json!(0.9), json!(0.8)]
        );
        assert_eq!(find(dir.path(), "churn", "a-1", 30, 10).len(), 1);
        assert_eq!(find(dir.path(), "churn", "42", 30, 1).len(), 1);

        let chains = verify(dir.path(), "churn");
        assert!(chains.iter().all(|c| c.broken_at.is_none()), "{chains:?}");

        // A record removed from the middle breaks the chain where it was.
        let text = chained(&records);
        let lines: Vec<&str> = text.lines().collect();
        write(
            dir.path(),
            &format!("{today}-100.jsonl"),
            &format!("{}\n{}\n", lines[0], lines[2]),
        );
        let chains = verify(dir.path(), "churn");
        let today_chain = chains.iter().find(|c| c.file.starts_with(&today)).unwrap();
        assert_eq!(today_chain.broken_at, Some(2));

        assert_eq!(prune(dir.path(), 365), 1);
        // The day of 2000 is gone; today's file keeps its two records of 42.
        assert_eq!(find(dir.path(), "churn", "42", 100_000, 10).len(), 2);
    }
}
