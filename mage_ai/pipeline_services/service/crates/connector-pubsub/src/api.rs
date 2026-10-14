//! Pub/Sub's REST API, with a bearer token from object_store's Google credentials, or none
//! for the emulator.

use std::sync::Arc;
use std::time::Duration;

use mage_stream_ports::ConnectorError;
use object_store::ClientOptions;
use object_store::client::{
    CredentialProvider, HttpClient, HttpConnector, HttpRequestBody, ReqwestConnector,
};
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
use serde_json::Value;

pub struct Api {
    base: String,
    http: HttpClient,
    credentials: Option<Arc<dyn CredentialProvider<Credential = GcpCredential>>>,
}

impl Api {
    /// `emulator`: the emulator's host:port, which takes no credentials.
    pub fn new(emulator: Option<&str>, timeout: Duration) -> Result<Api, ConnectorError> {
        let (base, credentials) = match emulator {
            Some(host) => {
                let host = host.trim_end_matches('/');
                let base = if host.contains("://") {
                    format!("{host}/v1")
                } else {
                    format!("http://{host}/v1")
                };
                (base, None)
            }
            None => {
                // The store is never used: it resolves the credentials as the GCS sink does.
                let store = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name("mage-credentials")
                    .build()
                    .map_err(|e| ConnectorError::fatal(format!("Pub/Sub credentials: {e}")))?;
                (
                    "https://pubsub.googleapis.com/v1".to_string(),
                    Some(store.credentials().clone()),
                )
            }
        };
        let options = ClientOptions::new()
            .with_timeout(timeout)
            .with_connect_timeout(Duration::from_secs(10))
            .with_allow_http(base.starts_with("http://"));
        let http = ReqwestConnector::default()
            .connect(&options)
            .map_err(|e| ConnectorError::fatal(format!("Pub/Sub client: {e}")))?;
        Ok(Api {
            base,
            http,
            credentials,
        })
    }

    /// `path` follows /v1, such as `projects/p/subscriptions/s:pull`.
    pub async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, ConnectorError> {
        let mut request = http::Request::builder()
            .method(method)
            .uri(format!("{}/{path}", self.base))
            .header("content-type", "application/json");
        if let Some(credentials) = &self.credentials {
            let credential = credentials
                .get_credential()
                .await
                .map_err(|e| ConnectorError::retryable(format!("Pub/Sub credentials: {e}")))?;
            request = request.header("authorization", format!("Bearer {}", credential.bearer));
        }
        let payload = body.map_or_else(Vec::new, |b| serde_json::to_vec(b).expect("JSON"));
        let request = request
            .body(HttpRequestBody::from(payload))
            .map_err(|e| ConnectorError::fatal(format!("Pub/Sub {path}: {e}")))?;
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|e| ConnectorError::retryable(format!("Pub/Sub {path}: {e}")))?;
        let status = response.status();
        let bytes = response
            .into_body()
            .bytes()
            .await
            .map_err(|e| ConnectorError::retryable(format!("Pub/Sub {path}: {e}")))?;
        if status.is_success() {
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_slice(&bytes).map_err(|e| {
                ConnectorError::retryable(format!("Pub/Sub {path}: the answer is not JSON: {e}"))
            });
        }
        Err(error(path, status.as_u16(), &bytes))
    }
}

/// An error answer, retryable when trying again can succeed.
pub fn error(path: &str, status: u16, body: &[u8]) -> ConnectorError {
    let answer: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let kind = answer
        .pointer("/error/status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let message = answer
        .pointer("/error/message")
        .and_then(Value::as_str)
        .map_or_else(|| String::from_utf8_lossy(body).to_string(), str::to_string);
    let text = format!("Pub/Sub {path}: {kind} {message} (HTTP {status})");
    let retryable = status >= 500
        || status == 429
        || matches!(
            kind.as_str(),
            "UNAVAILABLE" | "DEADLINE_EXCEEDED" | "RESOURCE_EXHAUSTED" | "ABORTED" | "INTERNAL"
        );
    if retryable {
        ConnectorError::retryable(text)
    } else {
        ConnectorError::fatal(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_name_their_status_and_say_whether_to_try_again() {
        let missing = error(
            "projects/p/subscriptions/s:pull",
            404,
            br#"{"error":{"code":404,"message":"Subscription does not exist","status":"NOT_FOUND"}}"#,
        );
        assert!(!missing.retryable);
        assert_eq!(
            missing.message,
            "Pub/Sub projects/p/subscriptions/s:pull: NOT_FOUND Subscription does not exist \
             (HTTP 404)"
        );
        assert!(error("x", 429, b"").retryable);
        assert!(error("x", 400, br#"{"error":{"status":"UNAVAILABLE"}}"#).retryable);
    }

    #[test]
    fn the_emulator_takes_a_host_or_a_url() {
        let api = Api::new(Some("localhost:8085"), Duration::from_secs(5)).unwrap();
        assert_eq!(api.base, "http://localhost:8085/v1");
        assert!(api.credentials.is_none());
        let api = Api::new(Some("http://emulator:8085/"), Duration::from_secs(5)).unwrap();
        assert_eq!(api.base, "http://emulator:8085/v1");
    }
}
