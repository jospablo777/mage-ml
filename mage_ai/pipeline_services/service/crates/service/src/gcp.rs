//! Secrets from Google Cloud Secret Manager, read once at start-up, over its REST API; no
//! Google SDK.
//!
//! References come from `MAGE_SERVICE_SECRETS` (see `secrets.rs`) or the manifest
//! (`gcp_secret_var` in io_config.yaml):
//!
//! - `PGPASSWORD=gcp:<secret>`: the latest version, in the project `GOOGLE_CLOUD_PROJECT`
//!   (or the credentials' or the metadata server's project);
//! - `gcp:projects/<project>/secrets/<secret>[/versions/<version>]`: any project and version;
//! - `PGUSER=gcp:<secret>#username`: one key of a JSON payload; `gcp:<secret>` without a
//!   variable sets each key of a JSON payload.
//!
//! Credentials, in the order of Google's application default credentials:
//!
//! 1. `GOOGLE_APPLICATION_CREDENTIALS`: a service account key (signed JWT) or an authorized
//!    user file (refresh token);
//! 2. the metadata server (GKE workload identity, Cloud Run, Compute Engine);
//!    `GCE_METADATA_HOST` points elsewhere.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde_json::{Value, json};

const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const SECRET_MANAGER: &str = "https://secretmanager.googleapis.com";
const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const METADATA_HOST: &str = "metadata.google.internal";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapping {
    pub variable: Option<String>,
    pub project: Option<String>,
    pub secret: String,
    pub version: String,
    pub field: Option<String>,
}

impl Mapping {
    pub fn new(
        variable: Option<String>,
        reference: &str,
        field: Option<String>,
    ) -> Result<Mapping, String> {
        let invalid = || {
            format!(
                "gcp:{reference} is not a secret: gcp:<secret>, or \
                 gcp:projects/<project>/secrets/<secret>[/versions/<version>]"
            )
        };
        let parts: Vec<&str> = reference.split('/').collect();
        let (project, secret, version) = match parts.as_slice() {
            [secret] if !secret.is_empty() => (None, *secret, "latest"),
            ["projects", project, "secrets", secret] => (Some(*project), *secret, "latest"),
            ["projects", project, "secrets", secret, "versions", version] => {
                (Some(*project), *secret, *version)
            }
            _ => return Err(invalid()),
        };
        let valid = |s: &str| {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        };
        if !valid(secret) || !valid(version) || project.is_some_and(|p| !valid(p)) {
            return Err(invalid());
        }
        Ok(Mapping {
            variable,
            project: project.map(str::to_string),
            secret: secret.to_string(),
            version: version.to_string(),
            field,
        })
    }
}

fn http(timeout: Duration) -> ureq::Agent {
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

fn read_json(
    response: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    source: &str,
) -> Result<(u16, Value), String> {
    let response = response.map_err(|e| format!("{source} could not be reached: {e}"))?;
    let status = response.status().as_u16();
    let body = response.into_body().read_json().unwrap_or(Value::Null);
    Ok((status, body))
}

fn access_token(status: u16, body: &Value, source: &str) -> Result<String, String> {
    if status != 200 {
        let reason = body
            .get("error_description")
            .or_else(|| body.get("error"))
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| v.to_string())
            })
            .unwrap_or_else(|| "no reason given".into());
        return Err(format!(
            "{source} refused the credentials ({status}): {reason}"
        ));
    }
    body.get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("{source} sent no access token"))
}

fn base64url(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

/// A JWT for a service account, signed with its key (RS256).
pub fn service_account_assertion(
    client_email: &str,
    private_key_pem: &str,
    token_uri: &str,
    now: u64,
) -> Result<String, String> {
    let body: String = private_key_pem
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .map_err(|_| "the service account's private_key is not a PEM key".to_string())?;
    let key = ring::signature::RsaKeyPair::from_pkcs8(&der)
        .map_err(|e| format!("the service account's private_key cannot sign: {e}"))?;
    let header = base64url(br#"{"alg":"RS256","typ":"JWT"}"#);
    let claims = base64url(
        json!({"iss": client_email, "scope": SCOPE, "aud": token_uri, "iat": now,
               "exp": now + 3600})
        .to_string()
        .as_bytes(),
    );
    let message = format!("{header}.{claims}");
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        message.as_bytes(),
        &mut signature,
    )
    .map_err(|_| "signing the service account's token request failed".to_string())?;
    Ok(format!("{message}.{}", base64url(&signature)))
}

/// An access token, and the project the credentials belong to when they say.
pub fn token(environment: &BTreeMap<String, String>) -> Result<(String, Option<String>), String> {
    let agent = http(Duration::from_secs(30));
    if let Some(path) = setting(environment, "GOOGLE_APPLICATION_CREDENTIALS") {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("GOOGLE_APPLICATION_CREDENTIALS {path}: {e}"))?;
        let credentials: Value = serde_json::from_str(&text)
            .map_err(|_| format!("GOOGLE_APPLICATION_CREDENTIALS {path} is not JSON"))?;
        let field = |name: &str| credentials.get(name).and_then(Value::as_str);
        match field("type") {
            Some("service_account") => {
                let token_uri = field("token_uri").unwrap_or(DEFAULT_TOKEN_URI);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let assertion = service_account_assertion(
                    field("client_email").ok_or("the service account has no client_email")?,
                    field("private_key").ok_or("the service account has no private_key")?,
                    token_uri,
                    now,
                )?;
                let (status, body) = read_json(
                    agent.post(token_uri).send_form([
                        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                        ("assertion", assertion.as_str()),
                    ]),
                    "Google's token endpoint",
                )?;
                let token = access_token(status, &body, "Google's token endpoint")?;
                return Ok((token, field("project_id").map(str::to_string)));
            }
            Some("authorized_user") => {
                let token_uri = field("token_uri").unwrap_or(DEFAULT_TOKEN_URI);
                let (status, body) = read_json(
                    agent.post(token_uri).send_form([
                        ("grant_type", "refresh_token"),
                        ("client_id", field("client_id").unwrap_or_default()),
                        ("client_secret", field("client_secret").unwrap_or_default()),
                        ("refresh_token", field("refresh_token").unwrap_or_default()),
                    ]),
                    "Google's token endpoint",
                )?;
                let token = access_token(status, &body, "Google's token endpoint")?;
                return Ok((token, field("quota_project_id").map(str::to_string)));
            }
            other => {
                return Err(format!(
                    "GOOGLE_APPLICATION_CREDENTIALS {path} is a {} credential; services take \
                     service account keys and authorized user files, or the metadata server",
                    other.unwrap_or("unknown")
                ));
            }
        }
    }
    let host = setting(environment, "GCE_METADATA_HOST").unwrap_or(METADATA_HOST);
    let base = if host.starts_with("http") {
        host.to_string()
    } else {
        format!("http://{host}")
    };
    // Where there is no metadata server, the probe must not hang.
    let probe = http(Duration::from_secs(3));
    let (status, body) = read_json(
        probe
            .get(format!(
                "{base}/computeMetadata/v1/instance/service-accounts/default/token"
            ))
            .header("Metadata-Flavor", "Google")
            .call(),
        "the metadata server",
    )
    .map_err(|e| {
        format!(
            "no Google Cloud credentials: set GOOGLE_APPLICATION_CREDENTIALS, or run on GKE, \
             Cloud Run or Compute Engine ({e})"
        )
    })?;
    let token = access_token(status, &body, "the metadata server")?;
    let project = probe
        .get(format!("{base}/computeMetadata/v1/project/project-id"))
        .header("Metadata-Flavor", "Google")
        .call()
        .ok()
        .and_then(|r| r.into_body().read_to_string().ok())
        .filter(|p| !p.is_empty());
    Ok((token, project))
}

fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The variables a payload gives under a mapping.
pub fn values(mapping: &Mapping, payload: &str) -> Result<Vec<(String, String)>, String> {
    match (&mapping.variable, &mapping.field) {
        (Some(variable), None) => Ok(vec![(variable.clone(), payload.to_string())]),
        (variable, field) => {
            let json: Value = serde_json::from_str(payload).map_err(|_| {
                format!(
                    "secret {} is not JSON, so it has no keys; map it to a variable without #",
                    mapping.secret
                )
            })?;
            match (variable, field) {
                (Some(variable), Some(field)) => json
                    .get(field)
                    .map(|v| vec![(variable.clone(), text(v))])
                    .ok_or_else(|| format!("secret {} has no key {field}", mapping.secret)),
                _ => json
                    .as_object()
                    .map(|o| o.iter().map(|(k, v)| (k.clone(), text(v))).collect())
                    .ok_or_else(|| format!("secret {} is not a JSON object", mapping.secret)),
            }
        }
    }
}

/// Every mapped secret's values; an error names the secret that failed.
pub fn fetch(
    environment: &BTreeMap<String, String>,
    mappings: &[Mapping],
) -> Result<Vec<(String, String)>, String> {
    let (token, credentials_project) = token(environment)?;
    let default_project = setting(environment, "GOOGLE_CLOUD_PROJECT")
        .or_else(|| setting(environment, "GCLOUD_PROJECT"))
        .map(str::to_string)
        .or(credentials_project);
    let base = setting(environment, "MAGE_SERVICE_GCP_SECRET_MANAGER_URL")
        .unwrap_or(SECRET_MANAGER)
        .trim_end_matches('/');
    let agent = http(Duration::from_secs(30));
    let mut out = Vec::new();
    for mapping in mappings {
        let project = mapping
            .project
            .clone()
            .or_else(|| default_project.clone())
            .ok_or_else(|| {
                format!(
                    "secret {}: set GOOGLE_CLOUD_PROJECT, or name the project: \
                     gcp:projects/<project>/secrets/{}",
                    mapping.secret, mapping.secret
                )
            })?;
        let (status, body) = read_json(
            agent
                .get(format!(
                    "{base}/v1/projects/{project}/secrets/{}/versions/{}:access",
                    mapping.secret, mapping.version
                ))
                .header("Authorization", &format!("Bearer {token}"))
                .call(),
            "Secret Manager",
        )?;
        match status {
            200 => {}
            401 | 403 => {
                return Err(format!(
                    "Secret Manager denied secret {} ({status}); give the service account \
                     the Secret Manager Secret Accessor role",
                    mapping.secret
                ));
            }
            404 => {
                return Err(format!(
                    "Secret Manager has no secret {} (version {}) in project {project}",
                    mapping.secret, mapping.version
                ));
            }
            _ => {
                let reason = body
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("no reason given");
                return Err(format!(
                    "Secret Manager failed for secret {} ({status}): {reason}",
                    mapping.secret
                ));
            }
        }
        let data = body
            .pointer("/payload/data")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("Secret Manager sent no payload for {}", mapping.secret))?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data)
            .map_err(|_| format!("secret {}'s payload is not base64", mapping.secret))?;
        let payload = String::from_utf8(bytes)
            .map_err(|_| format!("secret {} is binary, not text", mapping.secret))?;
        out.extend(values(mapping, &payload)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    #[test]
    fn references_name_a_secret_a_project_and_a_version() {
        let short = Mapping::new(Some("A".into()), "pg-password", None).unwrap();
        assert_eq!((short.project, short.version.as_str()), (None, "latest"));
        let full = Mapping::new(None, "projects/shop-1/secrets/db/versions/3", None).unwrap();
        assert_eq!(
            (full.project.as_deref(), full.version.as_str()),
            (Some("shop-1"), "3")
        );
        for bad in [
            "",
            "projects/p/secrets",
            "a/b",
            "projects/p/secrets/x/versions/../y",
        ] {
            assert!(Mapping::new(None, bad, None).is_err(), "{bad}");
        }
    }

    /// A metadata server and Secret Manager on one local port.
    fn fake_google() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut authorization = String::new();
                let mut flavor = false;
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if lower.starts_with("authorization:") {
                        authorization = line[14..].trim().to_string();
                    }
                    if lower.starts_with("metadata-flavor: google") {
                        flavor = true;
                    }
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let payload = |text: &str| {
                    json!({"payload": {"data": base64::engine::general_purpose::STANDARD
                        .encode(text)}})
                    .to_string()
                };
                let (status, reply) = if request_line.contains("/service-accounts/default/token") {
                    if flavor {
                        ("200 OK", r#"{"access_token": "tok"}"#.to_string())
                    } else {
                        ("403 Forbidden", "{}".to_string())
                    }
                } else if request_line.contains("/project/project-id") {
                    ("200 OK", "shop-1".to_string())
                } else if authorization != "Bearer tok" {
                    ("401 Unauthorized", "{}".to_string())
                } else if request_line.contains("/projects/shop-1/secrets/pg/versions/latest") {
                    ("200 OK", payload("s3cret ñ"))
                } else if request_line.contains("/projects/shop-1/secrets/db/versions/2") {
                    ("200 OK", payload(r#"{"user": "shop", "port": 5432}"#))
                } else {
                    ("404 Not Found", "{}".to_string())
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
            }
        });
        address
    }

    #[test]
    fn secrets_are_fetched_with_the_metadata_server_and_its_project() {
        let address = fake_google();
        let environment: BTreeMap<String, String> = [
            ("GCE_METADATA_HOST".to_string(), address.clone()),
            ("MAGE_SERVICE_GCP_SECRET_MANAGER_URL".to_string(), address),
        ]
        .into();
        let mappings = vec![
            Mapping::new(Some("PGPASSWORD".into()), "pg", None).unwrap(),
            Mapping::new(
                Some("PGUSER".into()),
                "projects/shop-1/secrets/db/versions/2",
                Some("user".into()),
            )
            .unwrap(),
            Mapping::new(None, "projects/shop-1/secrets/db/versions/2", None).unwrap(),
        ];
        let values = fetch(&environment, &mappings).unwrap();
        assert_eq!(
            values,
            vec![
                ("PGPASSWORD".to_string(), "s3cret ñ".to_string()),
                ("PGUSER".to_string(), "shop".to_string()),
                ("port".to_string(), "5432".to_string()),
                ("user".to_string(), "shop".to_string()),
            ]
        );
        let missing = vec![Mapping::new(Some("X".into()), "nope", None).unwrap()];
        let error = fetch(
            &[
                ("GCE_METADATA_HOST".to_string(), fake_google()),
                (
                    "MAGE_SERVICE_GCP_SECRET_MANAGER_URL".to_string(),
                    fake_google(),
                ),
            ]
            .into(),
            &missing,
        )
        .unwrap_err();
        assert!(error.contains("no secret nope"), "{error}");
    }

    #[test]
    fn a_service_account_key_signs_a_verifiable_assertion() {
        // An RSA key from openssl, which ring cannot generate; skipped without openssl.
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key.pem");
        let made = std::process::Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
            ])
            .arg(&key)
            .output();
        if !made.is_ok_and(|o| o.status.success()) {
            eprintln!("openssl is not available; skipped");
            return;
        }
        let pem = std::fs::read_to_string(&key).unwrap();
        let jwt = service_account_assertion(
            "svc@shop-1.iam.gserviceaccount.com",
            &pem,
            DEFAULT_TOKEN_URI,
            1_800_000_000,
        )
        .unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims: Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts[1])
                .unwrap(),
        )
        .unwrap();
        assert_eq!(claims["iss"], "svc@shop-1.iam.gserviceaccount.com");
        assert_eq!(claims["exp"], 1_800_003_600u64);
        // The signature checks out against the key's public half.
        let der: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
        let der = base64::engine::general_purpose::STANDARD
            .decode(der)
            .unwrap();
        let pair = ring::signature::RsaKeyPair::from_pkcs8(&der).unwrap();
        let public = ring::signature::UnparsedPublicKey::new(
            &ring::signature::RSA_PKCS1_2048_8192_SHA256,
            ring::signature::KeyPair::public_key(&pair).as_ref(),
        );
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[2])
            .unwrap();
        public
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
            .unwrap();
    }
}
