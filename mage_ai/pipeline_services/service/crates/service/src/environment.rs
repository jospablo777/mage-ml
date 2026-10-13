//! The environment of block processes (Python workers, Rust programs).
//!
//! Block processes used to inherit the service's whole environment, so user code could read
//! the service's own credentials: the API tokens, the references to secret managers and the
//! IBM Cloud key the service reads secrets with. Blocks now start from an empty environment
//! filled from this module.
//!
//! By default every variable passes except those the service owns. Libraries read many
//! variables without naming them in the block's code (`MLFLOW_TRACKING_URI`, cloud SDK
//! credentials, proxies), so a list of what blocks need cannot be complete; the service's own
//! credentials are what must never reach them. `MAGE_SERVICE_WORKER_ENV=strict` passes only
//! system variables, the variables the manifest lists, and the names or `PREFIX_*` patterns in
//! `MAGE_SERVICE_PASS_ENV`.

use std::collections::BTreeSet;

/// Service variables blocks never see, with their `_FILE` forms.
const SERVICE_SECRETS: [&str; 4] = [
    "MAGE_SERVICE_TOKEN",
    "MAGE_SERVICE_READ_TOKEN",
    "MAGE_SERVICE_SECRETS",
    "IBM_CLOUD_API_KEY",
];

/// Service settings blocks may read; every other `MAGE_SERVICE_` variable stays with the
/// service.
const SERVICE_SETTINGS_FOR_BLOCKS: [&str; 2] = ["MAGE_SERVICE_LOG_LEVEL", "MAGE_SERVICE_ENV"];

/// What a process needs to run at all, passed in strict mode too.
const SYSTEM: [&str; 22] = [
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "LANG",
    "TZ",
    "TMPDIR",
    "TERM",
    "VIRTUAL_ENV",
    "LD_LIBRARY_PATH",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "HOSTNAME",
];
const SYSTEM_PREFIXES: [&str; 8] = [
    "LC_",
    "PYTHON",
    "R_",
    "OMP_",
    "MKL_",
    "OPENBLAS_",
    "POLARS_",
    "RAYON_",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Everything except the service's own variables.
    Default,
    /// System variables, the manifest's variables and `MAGE_SERVICE_PASS_ENV`.
    Strict,
}

impl Mode {
    pub fn from_env() -> Mode {
        match std::env::var("MAGE_SERVICE_WORKER_ENV").as_deref() {
            Ok("strict") => Mode::Strict,
            _ => Mode::Default,
        }
    }
}

fn service_owned(name: &str) -> bool {
    if SERVICE_SECRETS
        .iter()
        .any(|s| name == *s || name.strip_suffix("_FILE") == Some(*s))
    {
        return true;
    }
    name.starts_with("MAGE_SERVICE_") && !SERVICE_SETTINGS_FOR_BLOCKS.contains(&name)
}

fn matches(name: &str, pattern: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => name == pattern,
    }
}

/// The variables a block process gets from `inherited`, the service's environment.
/// `declared` holds the variables the manifest lists; `pass` the `MAGE_SERVICE_PASS_ENV`
/// entries.
pub fn block_environment(
    inherited: impl IntoIterator<Item = (String, String)>,
    declared: &BTreeSet<String>,
    pass: &[String],
    mode: Mode,
) -> Vec<(String, String)> {
    inherited
        .into_iter()
        .filter(|(name, _)| {
            if service_owned(name) {
                return false;
            }
            match mode {
                Mode::Default => true,
                Mode::Strict => {
                    SYSTEM.contains(&name.as_str())
                        || SYSTEM_PREFIXES.iter().any(|p| name.starts_with(p))
                        || declared.contains(name)
                        || pass.iter().any(|p| matches(name, p))
                }
            }
        })
        .collect()
}

/// `MAGE_SERVICE_PASS_ENV`: names or `PREFIX_*` patterns, separated by commas or spaces.
pub fn pass_list() -> Vec<String> {
    std::env::var("MAGE_SERVICE_PASS_ENV")
        .unwrap_or_default()
        .split([',', ' '])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(names: &[&str]) -> Vec<(String, String)> {
        names
            .iter()
            .map(|n| (n.to_string(), "value".to_string()))
            .collect()
    }

    fn names(vars: Vec<(String, String)>) -> Vec<String> {
        vars.into_iter().map(|(n, _)| n).collect()
    }

    #[test]
    fn the_services_credentials_never_reach_blocks() {
        let inherited = env(&[
            "MAGE_SERVICE_TOKEN",
            "MAGE_SERVICE_TOKEN_FILE",
            "MAGE_SERVICE_READ_TOKEN",
            "MAGE_SERVICE_SECRETS",
            "IBM_CLOUD_API_KEY",
            "IBM_CLOUD_API_KEY_FILE",
            "MAGE_SERVICE_IBM_TRUSTED_PROFILE",
            "MAGE_SERVICE_PORT",
            "MAGE_SERVICE_LOG_LEVEL",
            "MAGE_SERVICE_ENV",
            "MLFLOW_TRACKING_URI",
            "AWS_ACCESS_KEY_ID",
            "PATH",
        ]);

        let passed = names(block_environment(
            inherited,
            &BTreeSet::new(),
            &[],
            Mode::Default,
        ));

        assert_eq!(
            passed,
            [
                "MAGE_SERVICE_LOG_LEVEL",
                "MAGE_SERVICE_ENV",
                "MLFLOW_TRACKING_URI",
                "AWS_ACCESS_KEY_ID",
                "PATH"
            ]
        );
    }

    #[test]
    fn strict_mode_passes_system_declared_and_listed_variables_only() {
        let inherited = env(&[
            "PATH",
            "LC_ALL",
            "PYTHONHASHSEED",
            "PGPASSWORD",
            "OPENAI_API_KEY",
            "WANDB_API_KEY",
            "WANDB_PROJECT",
            "UNRELATED",
            "MAGE_SERVICE_TOKEN",
        ]);
        let declared = BTreeSet::from(["PGPASSWORD".to_string()]);

        let passed = names(block_environment(
            inherited,
            &declared,
            &["OPENAI_API_KEY".to_string(), "WANDB_*".to_string()],
            Mode::Strict,
        ));

        assert_eq!(
            passed,
            [
                "PATH",
                "LC_ALL",
                "PYTHONHASHSEED",
                "PGPASSWORD",
                "OPENAI_API_KEY",
                "WANDB_API_KEY",
                "WANDB_PROJECT"
            ]
        );
    }
}
