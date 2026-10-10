//! Environment variables and secrets for the blocks.
//!
//! Every platform can put a secret in a container's environment: Docker `-e` and env files,
//! Kubernetes `secretKeyRef`, AWS ECS `secrets` from Secrets Manager or Parameter Store,
//! IBM Code Engine `--env-from-secret`. Secrets mounted as files work too:
//!
//! - `NAME_FILE=/path` sets `NAME` to the file's content (Docker and Kubernetes secrets,
//!   Vault agent, the AWS and IBM CSI secret drivers), for the variables the code reads and
//!   the service's own credentials. Other `*_FILE` variables, such as `SSL_CERT_FILE` or
//!   `AWS_WEB_IDENTITY_TOKEN_FILE`, mean what their tools say and are left alone;
//! - `MAGE_SERVICE_SECRETS_DIR=/dir` sets one variable per file, named after the file.
//!
//! - `MAGE_SERVICE_SECRETS` fetches secrets from a provider at start-up, one reference per
//!   comma or line: `[NAME=]provider:reference[#field]`. Providers: `file` (a path), `ibm`
//!   (IBM Cloud Secrets Manager, see `ibm.rs`), `aws` (AWS Secrets Manager) and `aws-ssm`
//!   (AWS Systems Manager Parameter Store, see `aws.rs`). A provider runs only when a
//!   reference uses it.
//!
//! A variable set directly wins over every other source. Values marked secret, and every
//! value read from a file or a provider, are replaced by `***` in logs and stored errors.

use std::collections::BTreeMap;
use std::path::Path;

use mage_service_core::manifest::Manifest;

// Linux refuses to start a process with an environment string over 128 KiB.
const MAX_SECRET_BYTES: u64 = 128 * 1024 - 256;
/// Variables besides the manifest's whose `NAME_FILE` the service reads.
const FILE_VARIABLES: [&str; 6] = [
    "MAGE_SERVICE_TOKEN",
    "MAGE_SERVICE_READ_TOKEN",
    "IBM_CLOUD_API_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
];
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
    Provider,
}

#[derive(Debug, Default)]
pub struct Secrets {
    /// Variables to add to block processes, read from files.
    pub additions: BTreeMap<String, String>,
    sources: BTreeMap<String, Source>,
    redacted: Vec<String>,
    pub problems: Vec<String>,
}

pub const SECRETS: &str = "MAGE_SERVICE_SECRETS";

/// One entry of `MAGE_SERVICE_SECRETS`: `[NAME=]provider:reference[#field]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    /// None when the secret holds several values, each becoming a variable.
    pub variable: Option<String>,
    pub provider: String,
    pub id: String,
    pub field: Option<String>,
}

pub fn parse_references(text: &str) -> Result<Vec<Reference>, String> {
    let mut references = Vec::new();
    for item in text
        .split([',', '\n'])
        .map(str::trim)
        .filter(|i| !i.is_empty())
    {
        let (variable, rest) = match item.split_once('=') {
            Some((variable, rest)) => (Some(variable.trim().to_string()), rest.trim()),
            None => (None, item),
        };
        if let Some(variable) = &variable
            && !valid_name(variable)
        {
            return Err(format!("{SECRETS}: {variable:?} is not a variable name"));
        }
        let (provider, reference) = rest.split_once(':').ok_or_else(|| {
            format!("{SECRETS}: {item:?} needs a provider, such as ibm:<secret id> or file:<path>")
        })?;
        let (id, field) = match reference.rsplit_once('#') {
            Some((id, field)) => (id.trim().to_string(), Some(field.trim().to_string())),
            None => (reference.trim().to_string(), None),
        };
        references.push(Reference {
            variable,
            provider: provider.trim().to_ascii_lowercase(),
            id,
            field,
        });
    }
    Ok(references)
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
        return Err(format!(
            "{} is larger than 128 KiB, too large for an environment variable; mount it and \
             read the file from the block instead",
            path.display()
        ));
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
            let wanted = FILE_VARIABLES.contains(&base)
                || base.starts_with("MAGE_SECRET_")
                || manifest.environment.iter().any(|v| v.name == base);
            if !wanted {
                continue;
            }
            match read_secret(Path::new(path)) {
                Ok(value) => secrets.add(base, value, Source::File),
                Err(error) => secrets.problems.push(format!("{name}: {error}")),
            }
        }
        if let Some(text) = environment.get(SECRETS) {
            secrets.load_references(text, environment);
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

    fn load_references(&mut self, text: &str, environment: &BTreeMap<String, String>) {
        let references = match parse_references(text) {
            Ok(references) => references,
            Err(error) => {
                self.problems.push(error);
                return;
            }
        };
        let mut ibm = Vec::new();
        let mut aws = Vec::new();
        let mut values = Vec::new();
        for reference in references {
            match reference.provider.as_str() {
                "file" => match &reference.variable {
                    Some(name) => match read_secret(Path::new(&reference.id)) {
                        Ok(value) => values.push((name.clone(), value)),
                        Err(error) => self.problems.push(format!("{SECRETS} {name}: {error}")),
                    },
                    None => self.problems.push(format!(
                        "{SECRETS}: file:{} needs a variable name, NAME=file:<path>",
                        reference.id
                    )),
                },
                "ibm" => match crate::ibm::Mapping::new(
                    reference.variable,
                    &reference.id,
                    reference.field,
                ) {
                    Ok(mapping) => ibm.push(mapping),
                    Err(error) => self.problems.push(error),
                },
                "aws" | "aws-ssm" => match crate::aws::Mapping::new(
                    if reference.provider == "aws" {
                        crate::aws::Store::SecretsManager
                    } else {
                        crate::aws::Store::ParameterStore
                    },
                    reference.variable,
                    &reference.id,
                    reference.field,
                ) {
                    Ok(mapping) => aws.push(mapping),
                    Err(error) => self.problems.push(format!("{SECRETS}: {error}")),
                },
                other => self.problems.push(format!(
                    "{SECRETS}: unknown provider {other:?}; the providers are file, ibm, aws \
                     and aws-ssm"
                )),
            }
        }
        if !ibm.is_empty() {
            let api_key = environment
                .get("IBM_CLOUD_API_KEY")
                .filter(|k| !k.is_empty())
                .cloned()
                .or_else(|| self.additions.get("IBM_CLOUD_API_KEY").cloned());
            if let Some(key) = &api_key {
                self.redact(key);
            }
            match crate::ibm::fetch(environment, api_key.as_deref(), &ibm) {
                Ok(fetched) => values.extend(fetched),
                Err(error) => self
                    .problems
                    .push(format!("IBM Cloud Secrets Manager: {error}")),
            }
        }
        if !aws.is_empty() {
            for name in ["AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"] {
                if let Some(value) = environment.get(name) {
                    self.redact(value);
                }
            }
            match crate::aws::fetch(environment, &aws) {
                Ok(fetched) => values.extend(fetched),
                Err(error) => self.problems.push(format!("AWS: {error}")),
            }
        }
        for (name, value) in values {
            if environment.get(&name).is_some_and(|v| !v.is_empty()) {
                continue;
            }
            self.add(&name, value, Source::Provider);
        }
    }

    fn add(&mut self, name: &str, value: String, source: Source) {
        if value.len() as u64 > MAX_SECRET_BYTES {
            self.problems.push(format!(
                "{name} is larger than 128 KiB, too large for an environment variable"
            ));
            return;
        }
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

    /// SSL_CERT_FILE holds a CA bundle of over 200 KiB; read as a variable it made every
    /// block process fail to start on Linux (E2BIG).
    #[test]
    fn other_tools_file_variables_are_left_alone_and_large_files_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = dir.path().join("ca.crt");
        std::fs::write(&bundle, "x".repeat(224 * 1024)).unwrap();
        std::fs::write(dir.path().join("token"), "web-identity").unwrap();
        let env: BTreeMap<String, String> = [
            ("SSL_CERT_FILE".to_string(), bundle.display().to_string()),
            (
                "AWS_WEB_IDENTITY_TOKEN_FILE".to_string(),
                dir.path().join("token").display().to_string(),
            ),
            ("DB_TOKEN_FILE".to_string(), bundle.display().to_string()),
            (
                "MAGE_SERVICE_TOKEN_FILE".to_string(),
                dir.path().join("token").display().to_string(),
            ),
        ]
        .into();

        let secrets = Secrets::load(&manifest(), &env);

        assert_eq!(secrets.get("SSL_CERT"), None);
        assert_eq!(secrets.get("AWS_WEB_IDENTITY_TOKEN"), None);
        assert_eq!(secrets.get("MAGE_SERVICE_TOKEN"), Some("web-identity"));
        assert_eq!(secrets.get("DB_TOKEN"), None);
        assert!(
            secrets
                .problems
                .iter()
                .any(|p| p.contains("DB_TOKEN_FILE") && p.contains("128 KiB")),
            "{:?}",
            secrets.problems
        );
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
    fn references_name_a_provider_and_unknown_ones_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("key"), "api-key-value").unwrap();
        let parsed = parse_references("A=ibm:abc-1#password, ibm:kv-2").unwrap();
        assert_eq!(parsed[0].provider, "ibm");
        assert_eq!(parsed[0].field.as_deref(), Some("password"));
        assert_eq!(parsed[1].variable, None);
        assert!(
            parse_references("A=abc")
                .unwrap_err()
                .contains("needs a provider")
        );

        let env: BTreeMap<String, String> = [(
            SECRETS.to_string(),
            format!(
                "API_KEY=file:{}, X=vault:secret/x",
                dir.path().join("key").display()
            ),
        )]
        .into();
        let secrets = Secrets::load(&manifest(), &env);
        assert_eq!(secrets.get("API_KEY"), Some("api-key-value"));
        assert_eq!(secrets.source("API_KEY"), Some(Source::Provider));
        assert!(
            secrets.problems[0].contains("unknown provider \"vault\""),
            "{:?}",
            secrets.problems
        );
        assert_eq!(
            redact("key api-key-value", &secrets.redacted_values()),
            "key ***"
        );
    }

    #[test]
    fn unreadable_files_are_reported() {
        let env: BTreeMap<String, String> = [(
            "DB_TOKEN_FILE".to_string(),
            "/nonexistent/token".to_string(),
        )]
        .into();
        let secrets = Secrets::load(&manifest(), &env);
        assert!(
            secrets.problems[0].contains("DB_TOKEN_FILE"),
            "{:?}",
            secrets.problems
        );
        assert!(looks_secret("aws_secret_access_key"));
        assert!(!looks_secret("PGHOST"));
    }
}
