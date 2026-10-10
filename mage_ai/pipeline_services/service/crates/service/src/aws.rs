//! Secrets from AWS Secrets Manager and Systems Manager Parameter Store, read once at
//! start-up. Requests are signed with Signature Version 4; no AWS SDK is needed.
//!
//! References come from `MAGE_SERVICE_SECRETS` (see `secrets.rs`):
//!
//! - `PGPASSWORD=aws:<secret name or ARN>`: the secret's string;
//! - `PGUSER=aws:<secret>#username`: one key of a JSON secret, such as an RDS secret;
//! - `aws:<secret>`: a JSON secret; each key becomes a variable;
//! - `API_KEY=aws-ssm:/path/to/parameter`: a parameter, decrypted when it is a SecureString.
//!
//! The region comes from the ARN, or `AWS_REGION` or `AWS_DEFAULT_REGION`. Credentials come
//! from the first of these that is set up, as the AWS SDKs do:
//!
//! 1. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`;
//! 2. a web identity: `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` (EKS IAM roles for
//!    service accounts);
//! 3. container credentials: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` (ECS task roles) or
//!    `AWS_CONTAINER_CREDENTIALS_FULL_URI` with `AWS_CONTAINER_AUTHORIZATION_TOKEN[_FILE]`
//!    (EKS Pod Identity);
//! 4. the EC2 instance role, through IMDSv2, unless `AWS_EC2_METADATA_DISABLED=true`.
//!
//! `AWS_ENDPOINT_URL_SECRETS_MANAGER`, `AWS_ENDPOINT_URL_SSM`, `AWS_ENDPOINT_URL_STS` and
//! `AWS_ENDPOINT_URL` point the requests elsewhere, such as a VPC endpoint.

use std::collections::BTreeMap;
use std::time::Duration;

use ring::{digest, hmac};
use serde_json::{Value, json};

const ECS_HOST: &str = "http://169.254.170.2";
const IMDS: &str = "http://169.254.169.254";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Store {
    SecretsManager,
    ParameterStore,
}

impl Store {
    fn service(self) -> &'static str {
        match self {
            Store::SecretsManager => "secretsmanager",
            Store::ParameterStore => "ssm",
        }
    }

    fn endpoint_variable(self) -> &'static str {
        match self {
            Store::SecretsManager => "AWS_ENDPOINT_URL_SECRETS_MANAGER",
            Store::ParameterStore => "AWS_ENDPOINT_URL_SSM",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Store::SecretsManager => "aws",
            Store::ParameterStore => "aws-ssm",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapping {
    pub store: Store,
    /// None for a JSON secret whose keys become variables.
    pub variable: Option<String>,
    pub id: String,
    pub field: Option<String>,
}

impl Mapping {
    pub fn new(
        store: Store,
        variable: Option<String>,
        id: &str,
        field: Option<String>,
    ) -> Result<Mapping, String> {
        let prefix = store.prefix();
        if id.is_empty() || id.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(format!(
                "{prefix}:{id:?} does not name a secret or parameter"
            ));
        }
        if store == Store::ParameterStore && variable.is_none() {
            return Err(format!(
                "{prefix}:{id} needs a variable name, NAME={prefix}:<parameter>"
            ));
        }
        Ok(Mapping {
            store,
            variable,
            id: id.to_string(),
            field,
        })
    }

    /// The region of an ARN, such as arn:aws:secretsmanager:eu-west-1:123:secret:db.
    fn region(&self) -> Option<&str> {
        let mut parts = self.id.strip_prefix("arn:")?.split(':');
        parts.nth(2).filter(|r| !r.is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub token: Option<String>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, data).as_ref())
}

fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data.as_bytes())
        .as_ref()
        .to_vec()
}

/// The headers that sign a JSON POST to `/` of an AWS service.
pub fn sign(
    credentials: &Credentials,
    region: &str,
    service: &str,
    host: &str,
    target: &str,
    body: &str,
    amz_date: &str,
) -> Vec<(String, String)> {
    let date = &amz_date[..8];
    let mut headers = vec![
        ("content-type", "application/x-amz-json-1.1".to_string()),
        ("host", host.to_string()),
        ("x-amz-date", amz_date.to_string()),
    ];
    if let Some(token) = &credentials.token {
        headers.push(("x-amz-security-token", token.clone()));
    }
    headers.push(("x-amz-target", target.to_string()));
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed_headers = headers
        .iter()
        .map(|(k, _)| *k)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "POST\n/\n\n{canonical_headers}\n{signed_headers}\n{}",
        sha256_hex(body.as_bytes())
    );
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let mut key = hmac_sha256(format!("AWS4{}", credentials.secret_key).as_bytes(), date);
    for part in [region, service, "aws4_request"] {
        key = hmac_sha256(&key, part);
    }
    let signature = hex(&hmac_sha256(&key, &string_to_sign));
    let mut out: Vec<(String, String)> = headers
        .into_iter()
        .filter(|(k, _)| *k != "host")
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    out.push((
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, \
             Signature={signature}",
            credentials.access_key
        ),
    ));
    out
}

fn agent(timeout: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .build()
        .into()
}

fn setting<'a>(environment: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    environment
        .get(name)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
}

fn read_token(path: &str) -> Result<String, String> {
    std::fs::read_to_string(path)
        .map(|t| t.trim().to_string())
        .map_err(|e| format!("{path}: {e}"))
}

/// The text between <name> and </name>, for STS's XML answers.
fn xml_value<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let start = xml.find(&format!("<{name}>"))? + name.len() + 2;
    let end = xml[start..].find(&format!("</{name}>"))? + start;
    Some(&xml[start..end])
}

fn query_escape(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn from_json(body: &Value, source: &str) -> Result<Credentials, String> {
    let field = |name: &str| body.get(name).and_then(Value::as_str).map(str::to_string);
    Ok(Credentials {
        access_key: field("AccessKeyId").ok_or(format!("{source} sent no AccessKeyId"))?,
        secret_key: field("SecretAccessKey").ok_or(format!("{source} sent no SecretAccessKey"))?,
        token: field("Token"),
    })
}

/// Credentials from the first source that is set up; see the module documentation.
pub fn credentials(
    environment: &BTreeMap<String, String>,
    region: &str,
) -> Result<Credentials, String> {
    if let (Some(access_key), Some(secret_key)) = (
        setting(environment, "AWS_ACCESS_KEY_ID"),
        setting(environment, "AWS_SECRET_ACCESS_KEY"),
    ) {
        return Ok(Credentials {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            token: setting(environment, "AWS_SESSION_TOKEN").map(str::to_string),
        });
    }
    if let (Some(role), Some(token_file)) = (
        setting(environment, "AWS_ROLE_ARN"),
        setting(environment, "AWS_WEB_IDENTITY_TOKEN_FILE"),
    ) {
        let token = read_token(token_file)?;
        let session = setting(environment, "AWS_ROLE_SESSION_NAME").unwrap_or("mage-service");
        let endpoint = setting(environment, "AWS_ENDPOINT_URL_STS")
            .or_else(|| setting(environment, "AWS_ENDPOINT_URL"))
            .map(|e| e.trim_end_matches('/').to_string())
            .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com"));
        let url = format!(
            "{endpoint}/?Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={}\
             &RoleSessionName={}&WebIdentityToken={}",
            query_escape(role),
            query_escape(session),
            query_escape(&token)
        );
        let response = agent(Duration::from_secs(30))
            .get(&url)
            .call()
            .map_err(|e| format!("AWS STS could not be reached: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .into_body()
            .read_to_string()
            .map_err(|e| format!("AWS STS sent an invalid response: {e}"))?;
        if status != 200 {
            let reason = xml_value(&body, "Message").unwrap_or("no reason given");
            return Err(format!(
                "AWS STS refused the web identity for {role} ({status}): {reason}"
            ));
        }
        let field = |name| {
            xml_value(&body, name)
                .map(str::to_string)
                .ok_or(format!("AWS STS sent no {name}"))
        };
        return Ok(Credentials {
            access_key: field("AccessKeyId")?,
            secret_key: field("SecretAccessKey")?,
            token: Some(field("SessionToken")?),
        });
    }
    let container_url = setting(environment, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
        .map(|uri| format!("{ECS_HOST}{uri}"))
        .or_else(|| setting(environment, "AWS_CONTAINER_CREDENTIALS_FULL_URI").map(str::to_string));
    if let Some(url) = container_url {
        let authorization = match (
            setting(environment, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"),
            setting(environment, "AWS_CONTAINER_AUTHORIZATION_TOKEN"),
        ) {
            (Some(path), _) => Some(read_token(path)?),
            (None, Some(token)) => Some(token.to_string()),
            (None, None) => None,
        };
        let mut request = agent(Duration::from_secs(10)).get(&url);
        if let Some(token) = &authorization {
            request = request.header("Authorization", token);
        }
        let response = request
            .call()
            .map_err(|e| format!("the container credentials endpoint could not be reached: {e}"))?;
        let status = response.status().as_u16();
        let body: Value = response.into_body().read_json().map_err(|e| {
            format!("the container credentials endpoint sent an invalid response: {e}")
        })?;
        if status != 200 {
            return Err(format!(
                "the container credentials endpoint refused ({status}); check the task or pod role"
            ));
        }
        return from_json(&body, "the container credentials endpoint");
    }
    if setting(environment, "AWS_EC2_METADATA_DISABLED")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        return Err(no_credentials());
    }
    let imds = setting(environment, "AWS_EC2_METADATA_SERVICE_ENDPOINT")
        .map(|e| e.trim_end_matches('/').to_string())
        .unwrap_or_else(|| IMDS.to_string());
    // Off EC2 nothing answers, so the timeout is short.
    let agent = agent(Duration::from_secs(2));
    let token = agent
        .put(format!("{imds}/latest/api/token"))
        .header("X-aws-ec2-metadata-token-ttl-seconds", "300")
        .send_empty()
        .ok()
        .filter(|r| r.status().as_u16() == 200)
        .and_then(|r| r.into_body().read_to_string().ok())
        .ok_or_else(no_credentials)?;
    let get = |path: &str| -> Result<String, String> {
        let response = agent
            .get(format!(
                "{imds}/latest/meta-data/iam/security-credentials/{path}"
            ))
            .header("X-aws-ec2-metadata-token", &token)
            .call()
            .map_err(|e| format!("EC2 instance metadata: {e}"))?;
        if response.status().as_u16() != 200 {
            return Err("the EC2 instance has no IAM role".to_string());
        }
        response
            .into_body()
            .read_to_string()
            .map_err(|e| format!("EC2 instance metadata: {e}"))
    };
    let role = get("")?
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    let body: Value = serde_json::from_str(&get(&role)?)
        .map_err(|e| format!("EC2 instance metadata sent invalid credentials: {e}"))?;
    from_json(&body, "EC2 instance metadata")
}

fn no_credentials() -> String {
    "no AWS credentials: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or run with an IAM \
     role (ECS task role, EKS service account or Pod Identity, EC2 instance profile)"
        .to_string()
}

fn now() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}

/// Calls one action of a service and returns its JSON answer.
fn call(
    environment: &BTreeMap<String, String>,
    credentials: &Credentials,
    store: Store,
    region: &str,
    target: &str,
    body: &Value,
    what: &str,
) -> Result<Value, String> {
    let endpoint = setting(environment, store.endpoint_variable())
        .or_else(|| setting(environment, "AWS_ENDPOINT_URL"))
        .map(|e| e.trim_end_matches('/').to_string())
        .unwrap_or_else(|| format!("https://{}.{region}.amazonaws.com", store.service()));
    if !endpoint.starts_with("https://")
        && !endpoint.starts_with("http://127.0.0.1")
        && !endpoint.starts_with("http://localhost")
    {
        return Err(format!("{endpoint} must be an https:// address"));
    }
    let host = endpoint
        .split_once("://")
        .map(|(_, rest)| rest.split('/').next().unwrap_or(rest))
        .unwrap_or(&endpoint)
        .to_string();
    let text = body.to_string();
    let mut request = agent(Duration::from_secs(30)).post(format!("{endpoint}/"));
    for (name, value) in sign(
        credentials,
        region,
        store.service(),
        &host,
        target,
        &text,
        &now(),
    ) {
        request = request.header(&name, &value);
    }
    let response = request
        .send(text.as_bytes())
        .map_err(|e| format!("{} could not be reached: {e}", store.service()))?;
    let status = response.status().as_u16();
    let answer: Value = response
        .into_body()
        .read_json()
        .map_err(|e| format!("invalid response for {what}: {e}"))?;
    if status == 200 {
        return Ok(answer);
    }
    let kind = answer
        .get("__type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .rsplit('#')
        .next()
        .unwrap_or("")
        .to_string();
    let message = answer
        .get("message")
        .or_else(|| answer.get("Message"))
        .and_then(Value::as_str)
        .unwrap_or("no reason given");
    Err(match kind.as_str() {
        "ResourceNotFoundException" | "ParameterNotFound" => format!("{what} does not exist"),
        "AccessDeniedException" => format!(
            "access to {what} was denied ({message}); allow {} for the role, and kms:Decrypt \
             when a customer managed key encrypts it",
            match store {
                Store::SecretsManager => "secretsmanager:GetSecretValue",
                Store::ParameterStore => "ssm:GetParameter",
            }
        ),
        "UnrecognizedClientException" | "InvalidSignatureException" | "ExpiredTokenException" => {
            format!("the AWS credentials were refused ({kind}): {message}")
        }
        _ => format!("{what} could not be read ({status} {kind}): {message}"),
    })
}

fn text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The values a secret's string gives to its mapping.
pub fn values(mapping: &Mapping, secret: &str) -> Result<Vec<(String, String)>, String> {
    let what = format!("{}:{}", mapping.store.prefix(), mapping.id);
    let object = || -> Result<serde_json::Map<String, Value>, String> {
        match serde_json::from_str::<Value>(secret) {
            Ok(Value::Object(map)) => Ok(map),
            _ => Err(format!("{what} is not a JSON object")),
        }
    };
    match (&mapping.variable, &mapping.field) {
        (Some(variable), None) => Ok(vec![(variable.clone(), secret.to_string())]),
        (Some(variable), Some(field)) => {
            let map = object()?;
            let value = map
                .get(field)
                .ok_or_else(|| format!("{what} has no key {field}"))?;
            Ok(vec![(variable.clone(), text(value))])
        }
        (None, field) => {
            if field.is_some() {
                return Err(format!("{what}#... needs a variable name, NAME={what}#key"));
            }
            Ok(object()?
                .iter()
                .map(|(k, v)| (k.clone(), text(v)))
                .collect())
        }
    }
}

/// Every mapped secret's values; an error names the secret that failed.
pub fn fetch(
    environment: &BTreeMap<String, String>,
    mappings: &[Mapping],
) -> Result<Vec<(String, String)>, String> {
    if mappings.is_empty() {
        return Ok(Vec::new());
    }
    let default_region =
        setting(environment, "AWS_REGION").or_else(|| setting(environment, "AWS_DEFAULT_REGION"));
    let region_of = |mapping: &Mapping| -> Result<String, String> {
        mapping
            .region()
            .or(default_region)
            .map(str::to_string)
            .ok_or_else(|| {
                format!(
                    "{}:{} needs a region: set AWS_REGION, or use the secret's ARN",
                    mapping.store.prefix(),
                    mapping.id
                )
            })
    };
    let first_region = region_of(&mappings[0])?;
    let credentials = credentials(environment, &first_region)?;
    let mut fetched: BTreeMap<(Store, String), String> = BTreeMap::new();
    let mut out = Vec::new();
    for mapping in mappings {
        let key = (mapping.store, mapping.id.clone());
        if !fetched.contains_key(&key) {
            let region = region_of(mapping)?;
            let what = format!("{}:{}", mapping.store.prefix(), mapping.id);
            let secret = match mapping.store {
                Store::SecretsManager => {
                    let answer = call(
                        environment,
                        &credentials,
                        mapping.store,
                        &region,
                        "secretsmanager.GetSecretValue",
                        &json!({"SecretId": mapping.id}),
                        &what,
                    )?;
                    secret_string(&answer, &what)?
                }
                Store::ParameterStore => {
                    let answer = call(
                        environment,
                        &credentials,
                        mapping.store,
                        &region,
                        "AmazonSSM.GetParameter",
                        &json!({"Name": mapping.id, "WithDecryption": true}),
                        &what,
                    )?;
                    answer
                        .pointer("/Parameter/Value")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .ok_or(format!("{what} has no value"))?
                }
            };
            fetched.insert(key.clone(), secret);
        }
        out.extend(values(mapping, &fetched[&key])?);
    }
    Ok(out)
}

fn secret_string(answer: &Value, what: &str) -> Result<String, String> {
    if let Some(text) = answer.get("SecretString").and_then(Value::as_str) {
        return Ok(text.to_string());
    }
    let binary = answer
        .get("SecretBinary")
        .and_then(Value::as_str)
        .ok_or(format!("{what} has no value"))?;
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(binary)
        .map_err(|e| format!("{what} has an invalid binary value: {e}"))?;
    String::from_utf8(bytes).map_err(|_| format!("{what} is binary, not text"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials(token: Option<&str>) -> Credentials {
        Credentials {
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            token: token.map(str::to_string),
        }
    }

    fn authorization(token: Option<&str>) -> String {
        sign(
            &credentials(token),
            "us-east-1",
            "secretsmanager",
            "secretsmanager.us-east-1.amazonaws.com",
            "secretsmanager.GetSecretValue",
            r#"{"SecretId":"prod/db"}"#,
            "20261010T092146Z",
        )
        .into_iter()
        .find(|(k, _)| k == "authorization")
        .unwrap()
        .1
    }

    /// The expected values come from botocore's SigV4Auth for the same request.
    #[test]
    fn requests_are_signed_as_the_aws_sdks_sign_them() {
        assert_eq!(
            authorization(None),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261010/us-east-1/secretsmanager/\
             aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, \
             Signature=3c23d8912aae62798c197218c9efb407d6f4f7f283bff4a4c4371f653bd027c4"
        );
        assert_eq!(
            authorization(Some("session-token")),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261010/us-east-1/secretsmanager/\
             aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-security-token;\
             x-amz-target, \
             Signature=5719fe0bc3cac4884442ad94debcb09353263f6e4cf8edddc8a5cd055ee8bd8f"
        );
    }

    fn mapping(store: Store, text: &str) -> Mapping {
        let reference = crate::secrets::parse_references(text).unwrap().remove(0);
        Mapping::new(store, reference.variable, &reference.id, reference.field).unwrap()
    }

    #[test]
    fn arns_name_their_region_and_json_secrets_give_keys() {
        let arn = mapping(
            Store::SecretsManager,
            "PGUSER=aws:arn:aws:secretsmanager:eu-west-1:123456789012:secret:prod/db-AbCdEf#username",
        );
        assert_eq!(arn.region(), Some("eu-west-1"));
        assert_eq!(arn.field.as_deref(), Some("username"));
        let rds = r#"{"username": "mage", "password": "pw", "port": 5432}"#;
        assert_eq!(
            values(&arn, rds).unwrap(),
            vec![("PGUSER".into(), "mage".into())]
        );
        let all = mapping(Store::SecretsManager, "aws:prod/db");
        assert_eq!(all.region(), None);
        assert_eq!(values(&all, rds).unwrap().len(), 3);
        assert!(
            values(&all, "plain")
                .unwrap_err()
                .contains("not a JSON object")
        );
        let whole = mapping(Store::SecretsManager, "TOKEN=aws:api");
        assert_eq!(
            values(&whole, "abc").unwrap(),
            vec![("TOKEN".into(), "abc".into())]
        );
        let reference = crate::secrets::parse_references("aws-ssm:/app/key")
            .unwrap()
            .remove(0);
        assert!(
            Mapping::new(
                Store::ParameterStore,
                reference.variable,
                &reference.id,
                None
            )
            .unwrap_err()
            .contains("needs a variable name")
        );
    }

    #[test]
    fn web_identity_answers_are_read_from_xml() {
        let xml = "<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIA1\
                   </AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t\
                   </SessionToken></Credentials></AssumeRoleWithWebIdentityResponse>";
        assert_eq!(xml_value(xml, "SecretAccessKey"), Some("s"));
        assert_eq!(xml_value(xml, "Missing"), None);
        assert_eq!(query_escape("a b/c=+"), "a%20b%2Fc%3D%2B");
    }

    /// Answers container credentials, Secrets Manager and Parameter Store requests as AWS
    /// does, and checks that each request carries the session token and a signature.
    fn fake_aws() -> String {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut headers = BTreeMap::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':') {
                        headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
                    }
                }
                let length = headers
                    .get("content-length")
                    .map(|l| l.parse().unwrap())
                    .unwrap_or(0);
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let target = headers.get("x-amz-target").cloned().unwrap_or_default();
                let signed = headers.get("authorization").is_some_and(|a| {
                    a.starts_with("AWS4-HMAC-SHA256 Credential=ASIATEST/")
                        && a.contains("x-amz-security-token")
                }) && headers.get("x-amz-security-token").map(String::as_str)
                    == Some("session");
                let (status, reply) = if request_line.starts_with("GET /creds") {
                    if headers.get("authorization").map(String::as_str) == Some("pod-token") {
                        (
                            "200 OK",
                            json!({"AccessKeyId": "ASIATEST", "SecretAccessKey": "secret", "Token": "session"}),
                        )
                    } else {
                        ("401 Unauthorized", json!({}))
                    }
                } else if !signed {
                    (
                        "400 Bad Request",
                        json!({"__type": "InvalidSignatureException", "message": "unsigned"}),
                    )
                } else if target == "secretsmanager.GetSecretValue" {
                    match body["SecretId"].as_str() {
                        Some("prod/db") => (
                            "200 OK",
                            json!({"SecretString": r#"{"username":"mage","password":"pw"}"#}),
                        ),
                        _ => (
                            "400 Bad Request",
                            json!({"__type": "AccessDeniedException", "message": "not allowed"}),
                        ),
                    }
                } else if target == "AmazonSSM.GetParameter" && body["WithDecryption"] == true {
                    (
                        "200 OK",
                        json!({"Parameter": {"Name": body["Name"], "Value": "ssm-value"}}),
                    )
                } else {
                    (
                        "400 Bad Request",
                        json!({"__type": "Unknown", "message": target}),
                    )
                };
                let text = reply.to_string();
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                    text.len()
                );
            }
        });
        address
    }

    fn pod_environment(address: &str) -> BTreeMap<String, String> {
        [
            ("AWS_REGION", "us-east-1".to_string()),
            ("AWS_ENDPOINT_URL", address.to_string()),
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                format!("{address}/creds"),
            ),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "pod-token".to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    #[test]
    fn secrets_and_parameters_are_read_with_container_credentials() {
        let address = fake_aws();
        let environment = pod_environment(&address);
        let references = "PGUSER=aws:prod/db#username, aws:prod/db, KEY=aws-ssm:/app/key";
        let mappings: Vec<Mapping> = crate::secrets::parse_references(references)
            .unwrap()
            .into_iter()
            .map(|r| {
                let store = if r.provider == "aws" {
                    Store::SecretsManager
                } else {
                    Store::ParameterStore
                };
                Mapping::new(store, r.variable, &r.id, r.field).unwrap()
            })
            .collect();
        assert_eq!(
            fetch(&environment, &mappings).unwrap(),
            vec![
                ("PGUSER".into(), "mage".into()),
                ("password".into(), "pw".into()),
                ("username".into(), "mage".into()),
                ("KEY".into(), "ssm-value".into()),
            ]
        );
    }

    #[test]
    fn denied_secrets_and_missing_credentials_are_explained() {
        let address = fake_aws();
        let environment = pod_environment(&address);
        let denied = mapping(Store::SecretsManager, "X=aws:other");
        let error = fetch(&environment, std::slice::from_ref(&denied)).unwrap_err();
        assert!(error.contains("secretsmanager:GetSecretValue"), "{error}");
        let mut wrong_token = environment.clone();
        wrong_token.insert("AWS_CONTAINER_AUTHORIZATION_TOKEN".into(), "bad".into());
        let error = fetch(&wrong_token, std::slice::from_ref(&denied)).unwrap_err();
        assert!(error.contains("refused (401)"), "{error}");
        let nothing: BTreeMap<String, String> = [
            ("AWS_REGION", "us-east-1"),
            ("AWS_EC2_METADATA_DISABLED", "true"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        assert!(
            fetch(&nothing, std::slice::from_ref(&denied))
                .unwrap_err()
                .contains("no AWS credentials")
        );
        let no_region: BTreeMap<String, String> = BTreeMap::new();
        assert!(
            fetch(&no_region, &[denied])
                .unwrap_err()
                .contains("needs a region")
        );
    }
}
