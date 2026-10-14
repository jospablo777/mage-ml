//! SQS's JSON protocol: a POST to the endpoint with the action in `X-Amz-Target`, signed with
//! SigV4 by object_store's signer and credentials.

use std::sync::Arc;
use std::time::Duration;

use mage_stream_ports::ConnectorError;
use object_store::ClientOptions;
use object_store::aws::{AmazonS3Builder, AwsAuthorizer, AwsCredential};
use object_store::client::{
    CredentialProvider, HttpClient, HttpConnector, HttpRequestBody, ReqwestConnector,
};
use serde_json::Value;

pub struct Api {
    endpoint: String,
    region: String,
    http: HttpClient,
    credentials: Arc<dyn CredentialProvider<Credential = AwsCredential>>,
}

impl Api {
    /// `timeout` bounds one request, so it must exceed the long poll's wait.
    pub fn new(
        endpoint: Option<&str>,
        region: &str,
        timeout: Duration,
    ) -> Result<Api, ConnectorError> {
        let endpoint = endpoint
            .map(|e| e.trim_end_matches('/').to_string())
            .unwrap_or_else(|| format!("https://sqs.{region}.amazonaws.com"));
        // The store is never used: it resolves the credentials as the S3 sink does
        // (AWS_ACCESS_KEY_ID, AWS_WEB_IDENTITY_TOKEN_FILE, the ECS and instance roles).
        let store = AmazonS3Builder::from_env()
            .with_bucket_name("mage-credentials")
            .with_region(region)
            .build()
            .map_err(|e| ConnectorError::fatal(format!("SQS credentials: {e}")))?;
        let credentials = store.credentials().clone();
        let options = ClientOptions::new()
            .with_timeout(timeout)
            .with_connect_timeout(Duration::from_secs(10))
            .with_allow_http(endpoint.starts_with("http://"));
        let http = ReqwestConnector::default()
            .connect(&options)
            .map_err(|e| ConnectorError::fatal(format!("SQS client: {e}")))?;
        Ok(Api {
            endpoint,
            region: region.to_string(),
            http,
            credentials,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn call(&self, action: &str, body: &Value) -> Result<Value, ConnectorError> {
        let credential = self
            .credentials
            .get_credential()
            .await
            .map_err(|e| ConnectorError::retryable(format!("SQS credentials: {e}")))?;
        let payload = serde_json::to_vec(body).expect("JSON");
        let mut request = http::Request::builder()
            .method("POST")
            .uri(&self.endpoint)
            .header("content-type", "application/x-amz-json-1.0")
            .header("x-amz-target", format!("AmazonSQS.{action}"))
            .body(HttpRequestBody::from(payload))
            .map_err(|e| ConnectorError::fatal(format!("SQS {action}: {e}")))?;
        AwsAuthorizer::new(&credential, "sqs", &self.region)
            .try_authorize(&mut request, None)
            .map_err(|e| ConnectorError::fatal(format!("SQS {action}: signing: {e}")))?;
        let response = self
            .http
            .execute(request)
            .await
            .map_err(|e| ConnectorError::retryable(format!("SQS {action}: {e}")))?;
        let status = response.status();
        let bytes = response
            .into_body()
            .bytes()
            .await
            .map_err(|e| ConnectorError::retryable(format!("SQS {action}: {e}")))?;
        if status.is_success() {
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_slice(&bytes).map_err(|e| {
                ConnectorError::retryable(format!("SQS {action}: the answer is not JSON: {e}"))
            });
        }
        Err(error(action, status.as_u16(), &bytes))
    }
}

/// An error answer, retryable when trying again can succeed.
pub fn error(action: &str, status: u16, body: &[u8]) -> ConnectorError {
    let answer: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
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
        .map_or_else(|| String::from_utf8_lossy(body).to_string(), str::to_string);
    let text = format!("SQS {action}: {kind} {message} (HTTP {status})");
    let retryable = status >= 500
        || status == 429
        || matches!(
            kind.as_str(),
            "ThrottlingException"
                | "RequestThrottled"
                | "ExpiredToken"
                | "ExpiredTokenException"
                | "ServiceUnavailable"
                | "InternalError"
                | "KmsThrottled"
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
    fn errors_name_their_kind_and_say_whether_to_try_again() {
        let missing = error(
            "GetQueueUrl",
            400,
            br#"{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"no queue"}"#,
        );
        assert!(!missing.retryable);
        assert_eq!(
            missing.message,
            "SQS GetQueueUrl: QueueDoesNotExist no queue (HTTP 400)"
        );
        assert!(
            error(
                "ReceiveMessage",
                400,
                br#"{"__type":"ThrottlingException"}"#
            )
            .retryable
        );
        assert!(error("ReceiveMessage", 503, b"down").retryable);
    }
}
