//! Secrets from Azure Key Vault, read once at start-up, over its REST API; no Azure SDK.
//!
//! References come from `MAGE_SERVICE_SECRETS` (see `secrets.rs`) or the manifest
//! (`azure_secret_var` in io_config.yaml):
//!
//! - `PGPASSWORD=azure:<secret name>`: the secret's value, in the vault `AZURE_KEY_VAULT_URL`
//!   (`https://<vault>.vault.azure.net`), as MageML's `azure_secret_var` reads it;
//! - `PGUSER=azure:<secret name>#username`: one key of a JSON value;
//! - `azure:<vault url>/secrets/<name>[/<version>]`: a secret of another vault, or a version.
//!
//! Credentials come from the first of these that is set up, in the order of the Azure SDKs'
//! `DefaultAzureCredential`:
//!
//! 1. a client secret: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`;
//! 2. a workload identity (AKS): `AZURE_FEDERATED_TOKEN_FILE` with `AZURE_TENANT_ID` and
//!    `AZURE_CLIENT_ID`;
//! 3. a managed identity: `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` (App Service, Container
//!    Apps, Functions), else the instance metadata service (VMs, AKS node identities);
//!    `AZURE_CLIENT_ID` picks a user-assigned identity.
//!
//! `AZURE_AUTHORITY_HOST` changes the sign-in host, for sovereign clouds.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::Value;

const VAULT_VARIABLE: &str = "AZURE_KEY_VAULT_URL";
const DEFAULT_AUTHORITY: &str = "https://login.microsoftonline.com";
const IMDS: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
const API_VERSION: &str = "7.4";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapping {
    /// None: the value is a JSON object whose keys become variables.
    pub variable: Option<String>,
    pub vault: Option<String>,
    pub name: String,
    pub version: Option<String>,
    pub field: Option<String>,
}

impl Mapping {
    pub fn new(
        variable: Option<String>,
        reference: &str,
        field: Option<String>,
    ) -> Result<Mapping, String> {
        let (vault, rest) = match reference.find("/secrets/") {
            Some(index) => (
                Some(reference[..index].trim_end_matches('/').to_string()),
                &reference[index + "/secrets/".len()..],
            ),
            None => (None, reference),
        };
        let mut parts = rest.split('/');
        let name = parts.next().unwrap_or_default().to_string();
        let version = parts.next().map(str::to_string).filter(|v| !v.is_empty());
        if name.is_empty() || parts.next().is_some() {
            return Err(format!(
                "azure:{reference} is not a secret: azure:<name>, or \
                 azure:https://<vault>.vault.azure.net/secrets/<name>[/<version>]"
            ));
        }
        if let Some(vault) = &vault
            && !vault.starts_with("https://")
            && !vault.starts_with("http://127.0.0.1")
            && !vault.starts_with("http://localhost")
        {
            return Err(format!(
                "azure:{reference}: the vault must be an https:// address"
            ));
        }
        Ok(Mapping {
            variable,
            vault,
            name,
            version,
            field,
        })
    }
}

fn agent() -> ureq::Agent {
    agent_within(Duration::from_secs(30))
}

/// The metadata service answers at once where it exists; elsewhere a probe must not hang.
fn agent_within(timeout: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .build()
        .into()
}

fn setting<'a>(environment: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    environment
        .get(name)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
}

fn token_from(
    response: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    source: &str,
) -> Result<String, String> {
    let response = response.map_err(|e| format!("{source} could not be reached: {e}"))?;
    let status = response.status().as_u16();
    let body: Value = response
        .into_body()
        .read_json()
        .map_err(|e| format!("{source} sent an invalid response: {e}"))?;
    if status != 200 {
        let reason = body
            .get("error_description")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("no reason given");
        return Err(format!(
            "{source} refused the credentials ({status}): {reason}"
        ));
    }
    body.get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("{source} sent no access token"))
}

/// An access token for Key Vault.
pub fn token(environment: &BTreeMap<String, String>) -> Result<String, String> {
    let agent = agent();
    let authority = setting(environment, "AZURE_AUTHORITY_HOST")
        .unwrap_or(DEFAULT_AUTHORITY)
        .trim_end_matches('/')
        .to_string();
    let tenant = setting(environment, "AZURE_TENANT_ID");
    let client = setting(environment, "AZURE_CLIENT_ID");
    let scope = "https://vault.azure.net/.default";
    if let (Some(tenant), Some(client), Some(secret)) =
        (tenant, client, setting(environment, "AZURE_CLIENT_SECRET"))
    {
        let response = agent
            .post(format!("{authority}/{tenant}/oauth2/v2.0/token"))
            .send_form([
                ("grant_type", "client_credentials"),
                ("client_id", client),
                ("client_secret", secret),
                ("scope", scope),
            ]);
        return token_from(response, "Microsoft Entra ID");
    }
    if let (Some(tenant), Some(client), Some(file)) = (
        tenant,
        client,
        setting(environment, "AZURE_FEDERATED_TOKEN_FILE"),
    ) {
        let assertion = std::fs::read_to_string(file)
            .map_err(|e| format!("AZURE_FEDERATED_TOKEN_FILE {file}: {e}"))?;
        let response = agent
            .post(format!("{authority}/{tenant}/oauth2/v2.0/token"))
            .send_form([
                ("grant_type", "client_credentials"),
                ("client_id", client),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", assertion.trim()),
                ("scope", scope),
            ]);
        return token_from(response, "Microsoft Entra ID (workload identity)");
    }
    let resource = "https://vault.azure.net";
    if let (Some(endpoint), Some(header)) = (
        setting(environment, "IDENTITY_ENDPOINT"),
        setting(environment, "IDENTITY_HEADER"),
    ) {
        let mut request = agent
            .get(endpoint)
            .query("resource", resource)
            .query("api-version", "2019-08-01")
            .header("X-IDENTITY-HEADER", header);
        if let Some(client) = client {
            request = request.query("client_id", client);
        }
        return token_from(request.call(), "the managed identity endpoint");
    }
    let imds = setting(environment, "MAGE_SERVICE_AZURE_IMDS_URL").unwrap_or(IMDS);
    let mut request = agent_within(Duration::from_secs(3))
        .get(imds)
        .query("resource", resource)
        .query("api-version", "2018-02-01")
        .header("Metadata", "true");
    if let Some(client) = client {
        request = request.query("client_id", client);
    }
    token_from(request.call(), "the instance metadata service").map_err(|e| {
        format!(
            "no Azure credentials: set AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET, \
             a workload identity, or run with a managed identity ({e})"
        )
    })
}

fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The variables a secret's value gives under a mapping.
pub fn values(mapping: &Mapping, value: &str) -> Result<Vec<(String, String)>, String> {
    match (&mapping.variable, &mapping.field) {
        (Some(variable), None) => Ok(vec![(variable.clone(), value.to_string())]),
        (variable, field) => {
            let json: Value = serde_json::from_str(value).map_err(|_| {
                format!(
                    "secret {} is not JSON, so it has no keys; map it to a variable without #",
                    mapping.name
                )
            })?;
            match (variable, field) {
                (Some(variable), Some(field)) => json
                    .get(field)
                    .map(|v| vec![(variable.clone(), text(v))])
                    .ok_or_else(|| format!("secret {} has no key {field}", mapping.name)),
                _ => json
                    .as_object()
                    .map(|o| o.iter().map(|(k, v)| (k.clone(), text(v))).collect())
                    .ok_or_else(|| format!("secret {} is not a JSON object", mapping.name)),
            }
        }
    }
}

/// Every mapped secret's values; an error names the secret that failed.
pub fn fetch(
    environment: &BTreeMap<String, String>,
    mappings: &[Mapping],
) -> Result<Vec<(String, String)>, String> {
    let default_vault = setting(environment, VAULT_VARIABLE);
    let token = token(environment)?;
    let agent = agent();
    let mut out = Vec::new();
    for mapping in mappings {
        let vault = mapping
            .vault
            .as_deref()
            .or(default_vault)
            .ok_or_else(|| {
                format!(
                    "secret {}: set {VAULT_VARIABLE} (https://<vault>.vault.azure.net)",
                    mapping.name
                )
            })?
            .trim_end_matches('/');
        let url = match &mapping.version {
            Some(version) => format!("{vault}/secrets/{}/{version}", mapping.name),
            None => format!("{vault}/secrets/{}", mapping.name),
        };
        let response = agent
            .get(&url)
            .query("api-version", API_VERSION)
            .header("Authorization", &format!("Bearer {token}"))
            .call()
            .map_err(|e| format!("Key Vault could not be reached: {e}"))?;
        let status = response.status().as_u16();
        let body: Value = response.into_body().read_json().unwrap_or(Value::Null);
        match status {
            200 => {}
            401 | 403 => {
                return Err(format!(
                    "Key Vault denied secret {} ({status}); give the identity the Key Vault \
                     Secrets User role, or a get permission on secrets",
                    mapping.name
                ));
            }
            404 => return Err(format!("Key Vault has no secret {}", mapping.name)),
            _ => {
                let reason = body
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("no reason given");
                return Err(format!(
                    "Key Vault failed for secret {} ({status}): {reason}",
                    mapping.name
                ));
            }
        }
        let value = body
            .get("value")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("Key Vault sent no value for secret {}", mapping.name))?;
        out.extend(values(mapping, value)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    #[test]
    fn references_name_a_secret_a_vault_and_a_version() {
        let plain = Mapping::new(Some("PG".into()), "pg-password", None).unwrap();
        assert_eq!(
            (plain.vault, plain.name, plain.version),
            (None, "pg-password".into(), None)
        );
        let full = Mapping::new(
            Some("PG".into()),
            "https://shop.vault.azure.net/secrets/pg/3f1c",
            None,
        )
        .unwrap();
        assert_eq!(full.vault.as_deref(), Some("https://shop.vault.azure.net"));
        assert_eq!(full.version.as_deref(), Some("3f1c"));
        assert!(Mapping::new(None, "", None).is_err());
        assert!(Mapping::new(None, "http://evil.example/secrets/x", None).is_err());
    }

    #[test]
    fn json_values_give_keys() {
        let mapping = Mapping::new(Some("U".into()), "db", Some("user".into())).unwrap();
        assert_eq!(
            values(&mapping, r#"{"user": "shop", "port": 5432}"#).unwrap(),
            vec![("U".to_string(), "shop".to_string())]
        );
        let all = Mapping::new(None, "db", None).unwrap();
        assert_eq!(
            values(&all, r#"{"a": 1}"#).unwrap(),
            vec![("a".into(), "1".into())]
        );
        assert!(values(&all, "plain").is_err());
    }

    /// Entra ID and a vault on one local port: tokens for a client secret, secrets by name.
    fn fake_azure() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut length = 0;
                let mut authorization = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                    if lower.starts_with("authorization:") {
                        authorization = line[14..].trim().to_string();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let body = String::from_utf8_lossy(&body);
                let (status, reply) = if request_line.contains("/oauth2/v2.0/token") {
                    if body.contains("client_secret=good") {
                        ("200 OK", r#"{"access_token": "tok"}"#.to_string())
                    } else {
                        (
                            "401 Unauthorized",
                            r#"{"error_description": "AADSTS7000215: Invalid client secret"}"#
                                .to_string(),
                        )
                    }
                } else if authorization != "Bearer tok" {
                    ("401 Unauthorized", "{}".to_string())
                } else if request_line.contains("/secrets/pg-password") {
                    ("200 OK", r#"{"value": "s3cret"}"#.to_string())
                } else if request_line.contains("/secrets/db") {
                    ("200 OK", r#"{"value": "{\"user\": \"shop\"}"}"#.to_string())
                } else {
                    (
                        "404 Not Found",
                        r#"{"error": {"message": "no"}}"#.to_string(),
                    )
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: \
                     {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
            }
        });
        address
    }

    fn environment(address: &str, secret: &str) -> BTreeMap<String, String> {
        [
            ("AZURE_AUTHORITY_HOST", address),
            ("AZURE_TENANT_ID", "tenant"),
            ("AZURE_CLIENT_ID", "client"),
            ("AZURE_CLIENT_SECRET", secret),
            (VAULT_VARIABLE, address),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    #[test]
    fn secrets_are_fetched_with_a_client_secret() {
        let address = fake_azure();
        let mappings = vec![
            Mapping::new(Some("PGPASSWORD".into()), "pg-password", None).unwrap(),
            Mapping::new(Some("PGUSER".into()), "db", Some("user".into())).unwrap(),
        ];
        let values = fetch(&environment(&address, "good"), &mappings).unwrap();
        assert_eq!(
            values,
            vec![
                ("PGPASSWORD".to_string(), "s3cret".to_string()),
                ("PGUSER".to_string(), "shop".to_string()),
            ]
        );
    }

    #[test]
    fn a_wrong_secret_and_a_missing_secret_are_explained() {
        let address = fake_azure();
        let mapping = vec![Mapping::new(Some("X".into()), "pg-password", None).unwrap()];
        let error = fetch(&environment(&address, "bad"), &mapping).unwrap_err();
        assert!(error.contains("Invalid client secret"), "{error}");
        let missing = vec![Mapping::new(Some("X".into()), "nope", None).unwrap()];
        let error = fetch(&environment(&address, "good"), &missing).unwrap_err();
        assert!(error.contains("has no secret nope"), "{error}");
    }
}
