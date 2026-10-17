//! Elasticsearch and OpenSearch sink for streams, with MageML's settings.
//!
//! Each batch goes to the cluster's bulk API, in requests of up to 5,000 messages or 8 MB,
//! and is confirmed once every message was written. Each message is the document; with
//! `_id`, the named message field is the document's id, so a batch written again replaces
//! its documents. `_op_type` is `index` (the default), `create` (data streams; a document
//! that already exists counts as written) or `update` (the message's fields update the
//! document). `_source` names a key to wrap each message under.
//!
//! Settings: `engine` (elasticsearch or opensearch, from MageML's connector_type), `host`
//! (a URL; without a scheme, https), `index_name`, `verify_certs` (true), `ca_cert` (a file
//! or PEM), and for Elasticsearch `api_key`; for OpenSearch `http_auth`, `user:password` or
//! `@awsauth` (MageML's default: SigV4 with the AWS credentials the S3 sink uses, service
//! `aws_service`, es by default, aoss for OpenSearch Serverless, region `region` or
//! AWS_REGION).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use mage_stream_ports::{
    BatchMeta, BoxFuture, ConnectorError, RecordBatch, Sink, SinkCapabilities, read_envelope,
};
use object_store::ClientOptions;
use object_store::aws::{AmazonS3Builder, AwsAuthorizer, AwsCredential};
use object_store::client::{
    Certificate, CredentialProvider, HttpClient, HttpConnector, HttpRequestBody, ReqwestConnector,
};
use serde_json::value::RawValue;
use serde_json::{Value, json};

const MAX_DOCUMENTS: usize = 5_000;
const MAX_BYTES: usize = 8 * 1024 * 1024;

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

enum Auth {
    None,
    Header(String),
    Aws {
        credentials: Arc<dyn CredentialProvider<Credential = AwsCredential>>,
        region: String,
        service: String,
    },
}

pub struct SearchSink {
    url: String,
    index: String,
    id_field: Option<String>,
    op_type: String,
    source_key: Option<String>,
    auth: Auth,
    http: HttpClient,
    label: String,
}

impl SearchSink {
    pub fn new(config: &Value) -> Result<SearchSink, ConnectorError> {
        let engine = text(config, "engine").unwrap_or_else(|| "elasticsearch".into());
        let label = if engine == "opensearch" {
            "OpenSearch"
        } else {
            "Elasticsearch"
        };
        let host = text(config, "host")
            .ok_or_else(|| ConnectorError::fatal(format!("{label} settings need host.")))?;
        let url = if host.contains("://") {
            host
        } else {
            format!("https://{host}")
        };
        let url = url.trim_end_matches('/').to_string();
        let index = text(config, "index_name")
            .ok_or_else(|| ConnectorError::fatal(format!("{label} settings need index_name.")))?;
        let op_type = text(config, "_op_type").unwrap_or_else(|| "index".into());
        if !matches!(op_type.as_str(), "index" | "create" | "update") {
            return Err(ConnectorError::fatal(format!(
                "{label} _op_type is index, create or update, not {op_type}."
            )));
        }
        let id_field = text(config, "_id");
        if op_type == "update" && id_field.is_none() {
            return Err(ConnectorError::fatal(format!(
                "{label} updates need _id, the message field that names each document."
            )));
        }
        let mut options = ClientOptions::new()
            .with_timeout(Duration::from_secs(120))
            .with_connect_timeout(Duration::from_secs(10))
            .with_allow_http(url.starts_with("http://"));
        if config.get("verify_certs") == Some(&Value::Bool(false)) {
            options = options.with_allow_invalid_certificates(true);
        }
        if let Some(ca) = text(config, "ca_cert") {
            let pem = if ca.contains("-----BEGIN") {
                ca.into_bytes()
            } else {
                std::fs::read(&ca)
                    .map_err(|e| ConnectorError::fatal(format!("{label} ca_cert {ca}: {e}")))?
            };
            let certificate = Certificate::from_pem(&pem)
                .map_err(|e| ConnectorError::fatal(format!("{label} ca_cert: {e}")))?;
            options = options.with_root_certificate(certificate);
        }
        let auth = if engine == "opensearch" {
            match text(config, "http_auth").as_deref().unwrap_or("@awsauth") {
                "@awsauth" => {
                    let region = text(config, "region")
                        .or_else(|| std::env::var("AWS_REGION").ok())
                        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
                        .unwrap_or_else(|| "us-east-1".into());
                    let store = AmazonS3Builder::from_env()
                        .with_bucket_name("mage-credentials")
                        .with_region(&region)
                        .build()
                        .map_err(|e| ConnectorError::fatal(format!("{label} AWS: {e}")))?;
                    Auth::Aws {
                        credentials: store.credentials().clone(),
                        region,
                        service: text(config, "aws_service").unwrap_or_else(|| "es".into()),
                    }
                }
                user_password => Auth::Header(format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(user_password)
                )),
            }
        } else {
            match text(config, "api_key") {
                Some(key) => Auth::Header(format!("ApiKey {key}")),
                None => Auth::None,
            }
        };
        let http = ReqwestConnector::default()
            .connect(&options)
            .map_err(|e| ConnectorError::fatal(format!("{label} client: {e}")))?;
        Ok(SearchSink {
            url,
            index,
            id_field,
            op_type,
            source_key: text(config, "_source").filter(|key| key != "_source"),
            auth,
            http,
            label: label.to_string(),
        })
    }

    /// The bulk request bodies of a batch.
    fn bodies(&self, values: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, ConnectorError> {
        let mut bodies = Vec::new();
        let mut body = Vec::new();
        let mut count = 0;
        for value in values {
            let mut target = json!({"_index": self.index});
            if let Some(field) = &self.id_field {
                target["_id"] = json!(document_id(value, field).ok_or_else(|| {
                    ConnectorError::fatal(format!(
                        "{}: a message has no {field} field for its document id: {}",
                        self.label,
                        String::from_utf8_lossy(&value[..value.len().min(200)])
                    ))
                })?);
            }
            let action = serde_json::to_vec(&json!({self.op_type.as_str(): target})).expect("JSON");
            let document = match (&self.source_key, self.op_type.as_str()) {
                (Some(key), _) => wrap(key, value),
                (None, "update") => wrap("doc", value),
                (None, _) => value.clone(),
            };
            if count > 0 && (count >= MAX_DOCUMENTS || body.len() + document.len() > MAX_BYTES) {
                bodies.push(std::mem::take(&mut body));
                count = 0;
            }
            body.extend_from_slice(&action);
            body.push(b'\n');
            body.extend_from_slice(&document);
            body.push(b'\n');
            count += 1;
        }
        if !body.is_empty() {
            bodies.push(body);
        }
        Ok(bodies)
    }

    async fn bulk(&self, body: Vec<u8>) -> Result<(), ConnectorError> {
        let mut request = http::Request::builder()
            .method("POST")
            .uri(format!("{}/_bulk", self.url))
            .header("content-type", "application/x-ndjson");
        if let Auth::Header(value) = &self.auth {
            request = request.header("authorization", value);
        }
        let mut request = request
            .body(HttpRequestBody::from(body))
            .map_err(|e| ConnectorError::fatal(format!("{}: {e}", self.label)))?;
        if let Auth::Aws {
            credentials,
            region,
            service,
        } = &self.auth
        {
            let credential = credentials.get_credential().await.map_err(|e| {
                ConnectorError::retryable(format!("{} AWS credentials: {e}", self.label))
            })?;
            AwsAuthorizer::new(&credential, service, region)
                .try_authorize(&mut request, None)
                .map_err(|e| ConnectorError::fatal(format!("{} signing: {e}", self.label)))?;
        }
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|e| ConnectorError::retryable(format!("{}: {e}", self.label)))?;
        let status = response.status().as_u16();
        let bytes = response
            .into_body()
            .bytes()
            .await
            .map_err(|e| ConnectorError::retryable(format!("{}: {e}", self.label)))?;
        if !(200..300).contains(&status) {
            let text = String::from_utf8_lossy(&bytes[..bytes.len().min(2000)]).to_string();
            let message = format!("{} answered {status}: {text}", self.label);
            return Err(if status == 429 || status >= 500 {
                ConnectorError::retryable(message)
            } else {
                ConnectorError::fatal(message)
            });
        }
        let answer: Value = serde_json::from_slice(&bytes).map_err(|e| {
            ConnectorError::retryable(format!("{}: the bulk answer is not JSON: {e}", self.label))
        })?;
        match first_failure(&answer, &self.op_type) {
            None => Ok(()),
            Some((status, reason)) => {
                let message = format!("{} refused a document ({status}): {reason}", self.label);
                Err(if status == 429 || status >= 500 {
                    ConnectorError::retryable(message)
                } else {
                    ConnectorError::fatal(message)
                })
            }
        }
    }
}

/// A message's field as a document id: a string as it is, anything else as its JSON.
fn document_id(value: &[u8], field: &str) -> Option<String> {
    let fields: HashMap<String, Box<RawValue>> = serde_json::from_slice(value).ok()?;
    let raw = fields.get(field)?;
    match serde_json::from_str::<Value>(raw.get()).ok()? {
        Value::Null => None,
        Value::String(text) => Some(text),
        _ => Some(raw.get().to_string()),
    }
}

fn wrap(key: &str, value: &[u8]) -> Vec<u8> {
    let mut out = serde_json::to_vec(&json!({ key: null })).expect("JSON");
    // {"key":null} becomes {"key":<value>}, the value's bytes as they are.
    out.truncate(out.len() - 5);
    out.extend_from_slice(value);
    out.push(b'}');
    out
}

/// The first item the cluster did not write, as (status, reason). A create of a document
/// that already exists counts as written: a replay of a data stream's batch.
fn first_failure(answer: &Value, op_type: &str) -> Option<(u16, String)> {
    if answer.get("errors") != Some(&Value::Bool(true)) {
        return None;
    }
    for item in answer.get("items")?.as_array()? {
        let result = item.as_object()?.values().next()?;
        let status = result.get("status")?.as_u64()? as u16;
        if status < 300 || (status == 409 && op_type == "create") {
            continue;
        }
        let error = result.get("error").cloned().unwrap_or(Value::Null);
        let reason = match (&error.get("type"), &error.get("reason")) {
            (Some(kind), Some(reason)) => format!(
                "{} {}",
                kind.as_str().unwrap_or(""),
                reason.as_str().unwrap_or("")
            ),
            _ => error.to_string(),
        };
        return Some((status, reason));
    }
    None
}

impl Sink for SearchSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        _meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let envelopes = read_envelope(batch)
                .map_err(|e| ConnectorError::fatal(format!("{}: {e}", self.label)))?;
            let values: Vec<Vec<u8>> = envelopes.into_iter().map(|e| e.value).collect();
            for body in self.bodies(&values)? {
                self.bulk(body).await?;
            }
            Ok(())
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: self.id_field.is_some(),
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink(config: Value) -> SearchSink {
        SearchSink::new(&config).unwrap()
    }

    #[test]
    fn bulk_bodies_name_the_index_and_ids() {
        let s = sink(
            json!({"host": "http://localhost:9200", "index_name": "orders",
                            "_id": "order"}),
        );
        let bodies = s
            .bodies(&[br#"{"order": 7, "n": 12345678901234567890}"#.to_vec()])
            .unwrap();
        let text = String::from_utf8(bodies[0].clone()).unwrap();
        assert_eq!(
            text,
            "{\"index\":{\"_id\":\"7\",\"_index\":\"orders\"}}\n{\"order\": 7, \"n\": 12345678901234567890}\n"
        );
    }

    #[test]
    fn updates_and_named_sources_wrap_the_message() {
        let s = sink(json!({"host": "h", "index_name": "i", "_id": "k", "_op_type": "update"}));
        let text =
            String::from_utf8(s.bodies(&[br#"{"k":"a"}"#.to_vec()]).unwrap()[0].clone()).unwrap();
        assert!(text.ends_with("{\"doc\":{\"k\":\"a\"}}\n"), "{text}");
        assert_eq!(s.url, "https://h");
        let s = sink(json!({"host": "h", "index_name": "i", "_source": "payload"}));
        let text =
            String::from_utf8(s.bodies(&[br#"{"k":1}"#.to_vec()]).unwrap()[0].clone()).unwrap();
        assert!(text.ends_with("{\"payload\":{\"k\":1}}\n"), "{text}");
        assert!(
            SearchSink::new(&json!({"host": "h", "index_name": "i", "_op_type": "update"}))
                .is_err()
        );
    }

    #[test]
    fn large_batches_split_into_several_requests() {
        let s = sink(json!({"host": "h", "index_name": "i"}));
        let values = vec![br#"{"n":1}"#.to_vec(); MAX_DOCUMENTS + 1];
        assert_eq!(s.bodies(&values).unwrap().len(), 2);
    }

    #[test]
    fn item_failures_are_found_and_existing_creates_count_as_written() {
        let answer = json!({"errors": true, "items": [
            {"create": {"status": 409, "error": {"type": "version_conflict_engine_exception"}}},
            {"create": {"status": 400, "error": {"type": "mapper_parsing_exception",
                                                  "reason": "failed to parse field [n]"}}},
        ]});
        assert_eq!(
            first_failure(&answer, "create"),
            Some((
                400,
                "mapper_parsing_exception failed to parse field [n]".into()
            ))
        );
        assert_eq!(
            first_failure(&json!({"errors": false, "items": []}), "index"),
            None
        );
    }

    #[test]
    fn ids_read_strings_and_numbers() {
        assert_eq!(document_id(br#"{"id": "a-1"}"#, "id"), Some("a-1".into()));
        assert_eq!(
            document_id(br#"{"id": 12345678901234567890}"#, "id"),
            Some("12345678901234567890".into())
        );
        assert_eq!(document_id(br#"{"id": null}"#, "id"), None);
        assert_eq!(document_id(br#"[1]"#, "id"), None);
    }
}
