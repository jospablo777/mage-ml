//! Environment variables and secrets for the blocks.
//!
//! Every platform can put a secret in a container's environment: Docker `-e` and env files,
//! Kubernetes `secretKeyRef`, AWS ECS `secrets` from Secrets Manager or Parameter Store,
//! IBM Code Engine `--env-from-secret`. Secrets mounted as files work too:
//!
//! - `NAME_FILE=/path` sets `NAME` to the file's content (Docker and Kubernetes secrets,
//!   Vault agent, the AWS and IBM CSI secret drivers);
//! - `MAGE_SERVICE_SECRETS_DIR=/dir` sets one variable per file, named after the file.
//!
//! A variable set directly wins over a file. Values marked secret, and every value read
//! from a file, are replaced by `***` in logs and stored errors.

use std::collections::BTreeMap;
use std::path::Path;

use mage_service_core::manifest::Manifest;

const MAX_SECRET_BYTES: u64 = 1024 * 1024;
const SECRET_HINTS: [&str; 7] = [
    "PASS",
    "SECRET",
    "TOKEN",
    "KEY",
    "CREDENTIAL",
    "PRIVATE",
    "AUTH",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Environment,
    File,
    Directory,
}

#[derive(Debug, Default)]
pub struct Secrets {
    /// Variables to add to block processes, read from files.
    pub additions: BTreeMap<String, String>,
    sources: BTreeMap<String, Source>,
    redacted: Vec<String>,
    pub problems: Vec<String>,
}

pub fn looks_secret(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    SECRET_HINTS.iter().any(|hint| upper.contains(hint))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}

fn read_secret(path: &Path) -> Result<String, String> {
    let metadata = std::fs::metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{} is not a file", path.display()));
    }
    if metadata.len() > MAX_SECRET_BYTES {
        return Err(format!("{} is larger than 1 MiB", path.display()));
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    // Files written by editors and `echo` end with a newline that is not part of the value.
    Ok(text
        .strip_suffix('\n')
        .map(|t| t.strip_suffix('\r').unwrap_or(t))
        .unwrap_or(&text)
        .to_string())
}

impl Secrets {
    pub fn load(manifest: &Manifest, environment: &BTreeMap<String, String>) -> Secrets {
        let mut secrets = Secrets::default();
        for (name, value) in environment {
            if !value.is_empty() {
                secrets.sources.insert(name.clone(), Source::Environment);
            }
        }
        if let Some(directory) = environment.get("MAGE_SERVICE_SECRETS_DIR") {
            match std::fs::read_dir(directory) {
                Ok(entries) => {
                    let mut entries: Vec<_> = entries.flatten().collect();
                    entries.sort_by_key(|e| e.file_name());
                    for entry in entries {
                        let name = entry.file_name().to_string_lossy().to_string();
                        // Kubernetes mounts secrets through hidden ..data links.
                        if name.starts_with('.')
                            || !valid_name(&name)
                            || secrets.sources.contains_key(&name)
                        {
                            continue;
                        }
                        match read_secret(&entry.path()) {
                            Ok(value) => secrets.add(&name, value, Source::Directory),
                            Err(error) => secrets.problems.push(format!("Secret {name}: {error}")),
                        }
                    }
                }
                Err(error) => secrets
                    .problems
                    .push(format!("MAGE_SERVICE_SECRETS_DIR {directory}: {error}")),
            }
        }
        for (name, path) in environment {
            let Some(base) = name.strip_suffix("_FILE") else {
                continue;
            };
            if !valid_name(base) || environment.get(base).is_some_and(|v| !v.is_empty()) {
                continue;
            }
            match read_secret(Path::new(path)) {
                Ok(value) => secrets.add(base, value, Source::File),
                Err(error) => secrets.problems.push(format!("{name}: {error}")),
            }
        }
        for variable in &manifest.environment {
            if (variable.secret || looks_secret(&variable.name))
                && let Some(value) = environment.get(&variable.name)
            {
                secrets.redact(value);
            }
        }
        for (name, value) in environment {
            if name.starts_with("MAGE_SECRET_")
                || name == "MAGE_SERVICE_TOKEN"
                || name == "MAGE_SERVICE_READ_TOKEN"
            {
                secrets.redact(value);
            }
        }
        secrets.redacted.sort_by_key(|v| std::cmp::Reverse(v.len()));
        secrets.redacted.dedup();
        secrets
    }

    fn add(&mut self, name: &str, value: String, source: Source) {
        self.redact(&value);
        self.sources.insert(name.to_string(), source);
        self.additions.insert(name.to_string(), value);
    }

    fn redact(&mut self, value: &str) {
        // Short values would hide ordinary words in the logs.
        if value.chars().count() >= 4 {
            self.redacted.push(value.to_string());
        }
    }

    pub fn source(&self, name: &str) -> Option<Source> {
        self.sources.get(name).copied()
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.additions.get(name).map(String::as_str)
    }

    pub fn redacted_values(&self) -> Vec<String> {
        self.redacted.clone()
    }

    /// Required variables that are not set, and optional ones; for the start-up report.
    pub fn missing(&self, manifest: &Manifest) -> (Vec<String>, Vec<String>) {
        let mut required = Vec::new();
        let mut optional = Vec::new();
        for variable in &manifest.environment {
            if self.source(&variable.name).is_some() {
                continue;
            }
            if variable.required {
                required.push(variable.name.clone());
            } else {
                optional.push(variable.name.clone());
            }
        }
        (required, optional)
    }
}

pub fn redact(text: &str, values: &[String]) -> String {
    let mut out = text.to_string();
    for value in values {
        if out.contains(value.as_str()) {
            out = out.replace(value.as_str(), "***");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn manifest() -> Manifest {
        serde_json::from_value(json!({
            "schema_version": 1,
            "service": {"name": "s", "project": "p", "exported_at": "x", "mage_version": "x",
                        "source_sha256": "0"},
            "pipelines": [],
            "environment": [
                {"name": "PGPASSWORD", "secret": true, "required": true},
                {"name": "PGHOST", "required": true},
                {"name": "API_URL"},
                {"name": "DB_TOKEN"}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn files_and_directories_fill_variables_and_are_redacted() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("token.txt"), "s3cr3t-value\n").unwrap();
        let secrets_dir = dir.path().join("secrets");
        std::fs::create_dir(&secrets_dir).unwrap();
        std::fs::write(secrets_dir.join("PGPASSWORD"), "from-dir-pass").unwrap();
        std::fs::write(secrets_dir.join(".hidden"), "x").unwrap();
        let env: BTreeMap<String, String> = [
            (
                "DB_TOKEN_FILE".to_string(),
                dir.path().join("token.txt").display().to_string(),
            ),
            (
                "MAGE_SERVICE_SECRETS_DIR".to_string(),
                secrets_dir.display().to_string(),
            ),
            ("PGHOST".to_string(), "db".to_string()),
        ]
        .into();

        let secrets = Secrets::load(&manifest(), &env);

        assert_eq!(secrets.get("DB_TOKEN"), Some("s3cr3t-value"));
        assert_eq!(secrets.get("PGPASSWORD"), Some("from-dir-pass"));
        assert_eq!(secrets.source("PGHOST"), Some(Source::Environment));
        assert_eq!(secrets.get(".hidden"), None);
        let text = redact(
            "password=from-dir-pass token=s3cr3t-value host=db",
            &secrets.redacted_values(),
        );
        assert_eq!(text, "password=*** token=*** host=db");
        let (required, optional) = secrets.missing(&manifest());
        assert!(required.is_empty());
        assert_eq!(optional, vec!["API_URL".to_string()]);
    }

    #[test]
    fn a_variable_set_directly_wins_over_its_file_and_missing_ones_are_listed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("p"), "file-value").unwrap();
        let env: BTreeMap<String, String> = [
            ("PGPASSWORD".to_string(), "direct-value".to_string()),
            (
                "PGPASSWORD_FILE".to_string(),
                dir.path().join("p").display().to_string(),
            ),
        ]
        .into();
        let secrets = Secrets::load(&manifest(), &env);
        assert_eq!(secrets.get("PGPASSWORD"), None);
        assert_eq!(redact("direct-value", &secrets.redacted_values()), "***");
        let (required, _) = secrets.missing(&manifest());
        assert_eq!(required, vec!["PGHOST".to_string()]);
    }

    #[test]
    fn unreadable_files_are_reported() {
        let env: BTreeMap<String, String> = [(
            "API_TOKEN_FILE".to_string(),
            "/nonexistent/token".to_string(),
        )]
        .into();
        let secrets = Secrets::load(&manifest(), &env);
        assert!(
            secrets.problems[0].contains("API_TOKEN_FILE"),
            "{:?}",
            secrets.problems
        );
        assert!(looks_secret("aws_secret_access_key"));
        assert!(!looks_secret("PGHOST"));
    }
}
