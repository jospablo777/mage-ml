//! Secrets from IBM Cloud Secrets Manager, read once at start-up.
//!
//! Settings:
//!
//! - `MAGE_SERVICE_IBM_SECRETS_MANAGER_URL`: the instance endpoint,
//!   `https://<instance id>.<region>.secrets-manager.appdomain.cloud` (or `.private.`).
//! - Authentication, one of:
//!   - `IBM_CLOUD_API_KEY` (or `IBM_CLOUD_API_KEY_FILE`): a service ID's API key;
//!   - `MAGE_SERVICE_IBM_TRUSTED_PROFILE`: a trusted profile id or name, with the compute
//!     resource token that Code Engine and IKS mount, so no key is stored at all.
//!
//! References come from `MAGE_SERVICE_SECRETS` (see `secrets.rs`) with the `ibm:` provider:
//!
//! - `PGPASSWORD=ibm:<secret id>`: an arbitrary secret's payload, a username_password
//!   secret's password, an iam_credentials secret's API key;
//! - `PGUSER=ibm:<secret id>#username`: one field of the secret;
//! - `ibm:<secret id>`: a key-value secret; each key becomes a variable.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::Value;

pub const URL: &str = "MAGE_SERVICE_IBM_SECRETS_MANAGER_URL";
const DEFAULT_IAM: &str = "https://iam.cloud.ibm.com";
const CR_TOKEN_PATHS: [&str; 2] = [
    "/var/run/secrets/codeengine.cloud.ibm.com/compute-resource-token/token",
    "/var/run/secrets/tokens/vault-token",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapping {
    /// None for a key-value secret, whose keys become variables.
    pub variable: Option<String>,
    pub secret_id: String,
    pub field: Option<String>,
}

impl Mapping {
    pub fn new(
        variable: Option<String>,
        id: &str,
        field: Option<String>,
    ) -> Result<Mapping, String> {
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err(format!(
                "ibm:{id} does not name a Secrets Manager secret id"
            ));
        }
        Ok(Mapping {
            variable,
            secret_id: id.to_string(),
            field,
        })
    }
}

/// The values a secret gives to its mapping.
pub fn values(mapping: &Mapping, secret: &Value) -> Result<Vec<(String, String)>, String> {
    let kind = secret
        .get("secret_type")
        .and_then(Value::as_str)
        .unwrap_or("arbitrary");
    let name = secret
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or(&mapping.secret_id);
    let text = |value: &Value| match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    match (&mapping.variable, kind) {
        (None, "kv") => {
            let data = secret
                .get("data")
                .and_then(Value::as_object)
                .ok_or_else(|| format!("secret {name} has no data"))?;
            Ok(data.iter().map(|(k, v)| (k.clone(), text(v))).collect())
        }
        (None, other) => Err(format!(
            "secret {name} is a {other} secret; map it to a variable: NAME={}",
            mapping.secret_id
        )),
        (Some(variable), _) => {
            let field = match (&mapping.field, kind) {
                (Some(field), _) => field.as_str(),
                (None, "arbitrary") => "payload",
                (None, "username_password") => "password",
                (None, "iam_credentials") => "api_key",
                (None, "kv") => {
                    return Err(format!(
                        "secret {name} is a key-value secret; pick a key with #key, or list it \
                         without a variable to set every key"
                    ));
                }
                (None, other) => {
                    return Err(format!(
                        "secret {name} is a {other} secret; pick a field with #field"
                    ));
                }
            };
            let value = secret
                .get(field)
                .or_else(|| secret.get("data").and_then(|d| d.get(field)))
                .ok_or_else(|| format!("secret {name} has no field {field}"))?;
            Ok(vec![(variable.clone(), text(value))])
        }
    }
}

pub struct Client {
    agent: ureq::Agent,
    url: String,
    token: String,
}

impl Client {
    pub fn connect(
        environment: &BTreeMap<String, String>,
        api_key: Option<&str>,
    ) -> Result<Client, String> {
        let url = environment
            .get(URL)
            .map(|u| u.trim_end_matches('/').to_string())
            .ok_or_else(|| format!("{URL} is not set"))?;
        if !url.starts_with("https://")
            && !url.starts_with("http://127.0.0.1")
            && !url.starts_with("http://localhost")
        {
            return Err(format!("{URL} must be an https:// address"));
        }
        let iam_url = environment
            .get("MAGE_SERVICE_IBM_IAM_URL")
            .cloned()
            .unwrap_or_else(|| DEFAULT_IAM.to_string());
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(30)))
            .http_status_as_error(false)
            .build()
            .into();
        let form: Vec<(&str, String)> = if let Some(key) = api_key {
            vec![
                (
                    "grant_type",
                    "urn:ibm:params:oauth:grant-type:apikey".into(),
                ),
                ("apikey", key.to_string()),
            ]
        } else if let Some(profile) = environment.get("MAGE_SERVICE_IBM_TRUSTED_PROFILE") {
            let token_path = environment
                .get("MAGE_SERVICE_IBM_CR_TOKEN_FILE")
                .cloned()
                .or_else(|| {
                    CR_TOKEN_PATHS
                        .iter()
                        .find(|p| std::path::Path::new(p).exists())
                        .map(|p| p.to_string())
                })
                .ok_or("no compute resource token is mounted; enable the trusted profile on the Code Engine app, or set MAGE_SERVICE_IBM_CR_TOKEN_FILE")?;
            let cr_token =
                std::fs::read_to_string(&token_path).map_err(|e| format!("{token_path}: {e}"))?;
            let profile_key = if profile.starts_with("Profile-") {
                "profile_id"
            } else {
                "profile_name"
            };
            vec![
                (
                    "grant_type",
                    "urn:ibm:params:oauth:grant-type:cr-token".into(),
                ),
                ("cr_token", cr_token.trim().to_string()),
                (profile_key, profile.clone()),
            ]
        } else {
            return Err(
                "set IBM_CLOUD_API_KEY (or IBM_CLOUD_API_KEY_FILE) or MAGE_SERVICE_IBM_TRUSTED_PROFILE"
                    .into(),
            );
        };
        let response = agent
            .post(format!("{}/identity/token", iam_url.trim_end_matches('/')))
            .header("Accept", "application/json")
            .send_form(form.iter().map(|(k, v)| (*k, v.as_str())))
            .map_err(|e| format!("IBM Cloud IAM could not be reached: {e}"))?;
        let status = response.status().as_u16();
        let body: Value = response
            .into_body()
            .read_json()
            .map_err(|e| format!("IBM Cloud IAM sent an invalid response: {e}"))?;
        if status != 200 {
            let reason = body
                .get("errorMessage")
                .or_else(|| body.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("no reason given");
            return Err(format!(
                "IBM Cloud IAM refused the credentials ({status}): {reason}"
            ));
        }
        let token = body
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or("IBM Cloud IAM sent no access token")?
            .to_string();
        Ok(Client { agent, url, token })
    }

    pub fn secret(&self, id: &str) -> Result<Value, String> {
        let response = self
            .agent
            .get(format!("{}/api/v2/secrets/{id}", self.url))
            .header("Authorization", &format!("Bearer {}", self.token))
            .header("Accept", "application/json")
            .call()
            .map_err(|e| format!("Secrets Manager could not be reached: {e}"))?;
        let status = response.status().as_u16();
        let body: Value = response
            .into_body()
            .read_json()
            .map_err(|e| format!("Secrets Manager sent an invalid response for {id}: {e}"))?;
        match status {
            200 => Ok(body),
            401 | 403 => Err(format!(
                "Secrets Manager denied secret {id} ({status}); grant the service ID or trusted \
                 profile the SecretsReader role"
            )),
            404 => Err(format!("Secrets Manager has no secret {id}")),
            _ => Err(format!(
                "Secrets Manager failed for secret {id} ({status}): {}",
                body.get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("no reason given")
            )),
        }
    }
}

/// Every mapped secret's values; an error names the secret that failed.
pub fn fetch(
    environment: &BTreeMap<String, String>,
    api_key: Option<&str>,
    mappings: &[Mapping],
) -> Result<Vec<(String, String)>, String> {
    if mappings.is_empty() {
        return Ok(Vec::new());
    }
    let client = Client::connect(environment, api_key)?;
    let mut fetched = BTreeMap::new();
    let mut out = Vec::new();
    for mapping in mappings {
        if !fetched.contains_key(&mapping.secret_id) {
            fetched.insert(
                mapping.secret_id.clone(),
                client.secret(&mapping.secret_id)?,
            );
        }
        out.extend(values(mapping, &fetched[&mapping.secret_id])?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse_mappings(text: &str) -> Result<Vec<Mapping>, String> {
        crate::secrets::parse_references(text)?
            .into_iter()
            .map(|r| Mapping::new(r.variable, &r.id, r.field))
            .collect()
    }

    #[test]
    fn mappings_name_variables_secrets_and_fields() {
        let parsed =
            parse_mappings("PGPASSWORD=ibm:abc-123, PGUSER=ibm:def-456#username\nibm:kv-789")
                .unwrap();
        assert_eq!(
            parsed,
            vec![
                Mapping {
                    variable: Some("PGPASSWORD".into()),
                    secret_id: "abc-123".into(),
                    field: None
                },
                Mapping {
                    variable: Some("PGUSER".into()),
                    secret_id: "def-456".into(),
                    field: Some("username".into())
                },
                Mapping {
                    variable: None,
                    secret_id: "kv-789".into(),
                    field: None
                },
            ]
        );
        assert!(parse_mappings("BAD NAME=ibm:abc").is_err());
        assert!(parse_mappings("X=ibm:../etc").is_err());
    }

    /// Answers IAM and Secrets Manager requests like IBM Cloud does.
    fn fake_ibm_cloud() -> String {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            for stream in listener.incoming().take(3) {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut length = 0;
                let mut authorization = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                    if lower.starts_with("authorization:") {
                        authorization = line.trim().to_string();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let body = String::from_utf8(body).unwrap();
                let (status, reply) = if request_line.starts_with("POST /identity/token") {
                    if body.contains("apikey=good-key") {
                        ("200 OK", json!({"access_token": "tok", "expires_in": 3600}))
                    } else {
                        (
                            "400 Bad Request",
                            json!({"errorMessage": "Provided API key could not be found."}),
                        )
                    }
                } else if !authorization.ends_with("Bearer tok") {
                    ("401 Unauthorized", json!({"message": "no"}))
                } else if request_line.contains("/api/v2/secrets/db-pass") {
                    (
                        "200 OK",
                        json!({"secret_type": "username_password", "name": "db", "username": "mage", "password": "s3cret"}),
                    )
                } else if request_line.contains("/api/v2/secrets/conn") {
                    (
                        "200 OK",
                        json!({"secret_type": "kv", "name": "conn", "data": {"PGHOST": "db.internal"}}),
                    )
                } else {
                    ("404 Not Found", json!({"message": "not found"}))
                };
                let text = reply.to_string();
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                    text.len()
                )
                .unwrap();
            }
        });
        address
    }

    #[test]
    fn secrets_are_fetched_with_an_api_key() {
        let address = fake_ibm_cloud();
        let environment: BTreeMap<String, String> = [
            (URL.to_string(), address.clone()),
            ("MAGE_SERVICE_IBM_IAM_URL".to_string(), address),
        ]
        .into();
        let mappings =
            parse_mappings("PGPASSWORD=ibm:db-pass, PGUSER=ibm:db-pass#username, ibm:conn")
                .unwrap();
        let values = fetch(&environment, Some("good-key"), &mappings).unwrap();
        assert_eq!(
            values,
            vec![
                ("PGPASSWORD".to_string(), "s3cret".to_string()),
                ("PGUSER".to_string(), "mage".to_string()),
                ("PGHOST".to_string(), "db.internal".to_string()),
            ]
        );
    }

    #[test]
    fn a_rejected_key_is_explained() {
        let address = fake_ibm_cloud();
        let environment: BTreeMap<String, String> = [
            (URL.to_string(), address.clone()),
            ("MAGE_SERVICE_IBM_IAM_URL".to_string(), address),
        ]
        .into();
        let mappings = parse_mappings("X=ibm:db-pass").unwrap();
        let error = fetch(&environment, Some("wrong"), &mappings).err().unwrap();
        assert!(error.contains("refused the credentials (400)"), "{error}");
        assert!(error.contains("could not be found"), "{error}");
    }

    #[test]
    fn each_secret_type_gives_its_value() {
        let arbitrary = json!({"secret_type": "arbitrary", "name": "a", "payload": "p@ss"});
        let user = json!({"secret_type": "username_password", "username": "u", "password": "pw"});
        let iam = json!({"secret_type": "iam_credentials", "api_key": "key"});
        let kv = json!({"secret_type": "kv", "data": {"HOST": "db", "PORT": 5432}});
        let map = |text: &str| {
            parse_mappings(&format!(
                "{}ibm:{}",
                text.split_once('=')
                    .map(|(v, _)| format!("{v}="))
                    .unwrap_or_default(),
                text.split_once('=').map(|(_, r)| r).unwrap_or(text)
            ))
            .unwrap()
            .remove(0)
        };
        assert_eq!(
            values(&map("X=a"), &arbitrary).unwrap(),
            vec![("X".into(), "p@ss".into())]
        );
        assert_eq!(
            values(&map("X=a"), &user).unwrap(),
            vec![("X".into(), "pw".into())]
        );
        assert_eq!(
            values(&map("X=a#username"), &user).unwrap(),
            vec![("X".into(), "u".into())]
        );
        assert_eq!(
            values(&map("X=a"), &iam).unwrap(),
            vec![("X".into(), "key".into())]
        );
        assert_eq!(
            values(&map("a"), &kv).unwrap(),
            vec![("HOST".into(), "db".into()), ("PORT".into(), "5432".into())]
        );
        assert_eq!(
            values(&map("P=a#PORT"), &kv).unwrap(),
            vec![("P".into(), "5432".into())]
        );
        assert!(values(&map("X=a"), &kv).unwrap_err().contains("key-value"));
        assert!(
            values(&map("a"), &arbitrary)
                .unwrap_err()
                .contains("map it to a variable")
        );
    }
}
