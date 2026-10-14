//! Amazon SQS source for streams, with MageML's settings.
//!
//! Settings: `queue_name` (or `queue_url`), `wait_time_seconds` (the long poll; 20 by
//! default), `receivers` (long polls at once, each up to 10 messages; 4 by default),
//! `visibility_timeout` (seconds; the queue's by default), `region` (or `AWS_REGION`),
//! `endpoint_url` (or `AWS_ENDPOINT_URL_SQS`, `AWS_ENDPOINT_URL`). Credentials are the
//! AWS chain the S3 sink uses: the environment, a web identity (EKS), the ECS task role or
//! the instance role.
//!
//! Each message's body reaches the blocks. A message is deleted only once every sink took
//! its batch; MageML deletes it when it is received, which loses it if the pipeline fails.
//! While a batch runs, the source keeps its messages invisible to other consumers, so a
//! slow batch is not read twice; a message not deleted, because the service stopped or died,
//! comes back after its visibility timeout. Several instances read one queue as competing
//! consumers.

mod api;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, ConnectorError,
    Envelope, EnvelopeBuilder, PartitionId, Position, Source, SourceCapabilities, SourceEvent,
};
use serde_json::{Value, json};
use tokio::sync::mpsc;

pub use api::Api;

const PARTITION: PartitionId = PartitionId(0);
/// SQS returns at most 10 messages a call, and takes 10 entries per batch call.
const PER_CALL: usize = 10;
/// Batch calls at once when deleting or extending.
const CALLS_AT_ONCE: usize = 16;

/// How a source groups messages into batches.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_wait: Duration,
}

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn number(config: &Value, key: &str) -> Result<Option<u64>, ConnectorError> {
    match text(config, key) {
        None => Ok(None),
        Some(value) => value
            .parse::<f64>()
            .ok()
            .filter(|v| *v >= 0.0)
            .map(|v| Some(v as u64))
            .ok_or_else(|| ConnectorError::fatal(format!("SQS {key} is a number of seconds."))),
    }
}

struct Received {
    body: Vec<u8>,
    receipt: String,
    sent_ms: Option<i64>,
}

struct Pending {
    builder: EnvelopeBuilder,
    first: u64,
    last: u64,
    started: Instant,
}

pub struct SqsSource {
    queue_name: Option<String>,
    queue_url: Option<String>,
    wait_seconds: u64,
    receivers: usize,
    visibility: Option<u64>,
    limits: BatchLimits,
    api: Arc<Api>,
    connected: Option<Connected>,
    epoch: u64,
    seq: u64,
    delivered: u64,
    /// The receipt of each delivered message not yet deleted, and when it was last made
    /// invisible.
    handles: BTreeMap<u64, (String, Instant)>,
    pending: Option<Pending>,
    paused: Arc<AtomicBool>,
    last_extend: Instant,
}

struct Connected {
    url: String,
    visibility: Duration,
    messages: mpsc::Receiver<Result<Vec<Received>, ConnectorError>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Connected {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl SqsSource {
    pub fn new(config: &Value, limits: BatchLimits) -> Result<SqsSource, ConnectorError> {
        let queue_name = text(config, "queue_name");
        let queue_url = text(config, "queue_url");
        if queue_name.is_none() && queue_url.is_none() {
            return Err(ConnectorError::fatal(
                "SQS source settings need queue_name or queue_url.",
            ));
        }
        let wait_seconds = number(config, "wait_time_seconds")?.unwrap_or(20).min(20);
        let receivers = number(config, "receivers")?.unwrap_or(4).clamp(1, 64) as usize;
        let region = text(config, "region")
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .unwrap_or_else(|| "us-east-1".into());
        let endpoint = text(config, "endpoint_url")
            .or_else(|| std::env::var("AWS_ENDPOINT_URL_SQS").ok())
            .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok());
        let api = Api::new(
            endpoint.as_deref(),
            &region,
            Duration::from_secs(wait_seconds + 15),
        )?;
        Ok(SqsSource {
            queue_name,
            queue_url,
            wait_seconds,
            receivers,
            visibility: number(config, "visibility_timeout")?,
            limits,
            api: Arc::new(api),
            connected: None,
            epoch: 0,
            seq: 0,
            delivered: 0,
            handles: BTreeMap::new(),
            pending: None,
            paused: Arc::new(AtomicBool::new(false)),
            last_extend: Instant::now(),
        })
    }

    async fn connect(&mut self) -> Result<SourceEvent, ConnectorError> {
        let url = match &self.queue_url {
            Some(url) => url.clone(),
            None => {
                let name = self.queue_name.as_deref().expect("checked");
                let answer = self
                    .api
                    .call("GetQueueUrl", &json!({"QueueName": name}))
                    .await?;
                answer
                    .get("QueueUrl")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ConnectorError::retryable("SQS GetQueueUrl: no QueueUrl"))?
                    .to_string()
            }
        };
        let visibility = match self.visibility {
            Some(seconds) => seconds,
            None => {
                let answer = self
                    .api
                    .call(
                        "GetQueueAttributes",
                        &json!({"QueueUrl": url, "AttributeNames": ["VisibilityTimeout"]}),
                    )
                    .await?;
                answer
                    .pointer("/Attributes/VisibilityTimeout")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(30)
            }
        };
        let (sender, messages) = mpsc::channel(self.receivers * 2);
        let tasks = (0..self.receivers)
            .map(|_| {
                tokio::spawn(receive(
                    self.api.clone(),
                    url.clone(),
                    self.wait_seconds,
                    self.visibility,
                    sender.clone(),
                    self.paused.clone(),
                ))
            })
            .collect();
        self.connected = Some(Connected {
            url,
            visibility: Duration::from_secs(visibility.max(1)),
            messages,
            tasks,
        });
        self.epoch += 1;
        self.seq = 0;
        Ok(SourceEvent::Assigned {
            partition: PARTITION,
            epoch: AssignmentEpoch(self.epoch),
        })
    }

    fn take(&mut self) -> Result<Option<SourceEvent>, ConnectorError> {
        let Some(mut pending) = self.pending.take() else {
            return Ok(None);
        };
        let records = pending.builder.len() as u32;
        let bytes = pending.builder.bytes() as u64;
        let batch = pending
            .builder
            .finish()
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let meta = BatchMeta {
            id: BatchId {
                partition: PARTITION,
                epoch: AssignmentEpoch(self.epoch),
                seq: self.seq,
            },
            first: Position::Offset(pending.first as i64),
            last: Position::Offset(pending.last as i64),
            records,
            bytes,
        };
        self.seq += 1;
        Ok(Some(SourceEvent::Batch { batch, meta }))
    }

    fn full(&self) -> bool {
        self.pending.as_ref().is_some_and(|p| {
            p.builder.len() >= self.limits.max_records || p.builder.bytes() >= self.limits.max_bytes
        })
    }

    fn wait(&self) -> Duration {
        self.pending
            .as_ref()
            .map_or(self.limits.max_wait, |p| {
                (p.started + self.limits.max_wait).saturating_duration_since(Instant::now())
            })
            .max(Duration::from_millis(10))
    }

    fn add(&mut self, message: Received) {
        self.delivered += 1;
        let position = self.delivered;
        self.handles
            .insert(position, (message.receipt, Instant::now()));
        let envelope = Envelope {
            key: None,
            value: message.body,
            headers: Vec::new(),
            partition: PARTITION.0,
            position: Some(position as i64),
            timestamp: message.sent_ms,
        };
        let pending = self.pending.get_or_insert_with(|| Pending {
            builder: EnvelopeBuilder::new(),
            first: position,
            last: position,
            started: Instant::now(),
        });
        pending.builder.push(&envelope);
        pending.last = position;
    }

    /// Keeps the messages of running batches invisible: those past half their visibility get
    /// it again.
    async fn extend(&mut self) -> Result<(), ConnectorError> {
        let Some(connected) = &self.connected else {
            return Ok(());
        };
        if self.last_extend.elapsed() < Duration::from_secs(1) {
            return Ok(());
        }
        self.last_extend = Instant::now();
        let half = connected.visibility / 2;
        let now = Instant::now();
        let due: Vec<(u64, String)> = self
            .handles
            .iter()
            .filter(|(_, (_, at))| now.duration_since(*at) >= half)
            .map(|(position, (receipt, _))| (*position, receipt.clone()))
            .collect();
        if due.is_empty() {
            return Ok(());
        }
        let seconds = connected.visibility.as_secs();
        let entries = due
            .iter()
            .map(|(_, receipt)| json!({"ReceiptHandle": receipt, "VisibilityTimeout": seconds}))
            .collect();
        let failed = batch_calls(
            self.api.clone(),
            connected.url.clone(),
            "ChangeMessageVisibilityBatch",
            entries,
        )
        .await?;
        if let Some(error) = failed {
            return Err(ConnectorError::retryable(error));
        }
        for (position, _) in due {
            if let Some(entry) = self.handles.get_mut(&position) {
                entry.1 = now;
            }
        }
        Ok(())
    }
}

/// One long poll after another, until the source goes.
async fn receive(
    api: Arc<Api>,
    url: String,
    wait_seconds: u64,
    visibility: Option<u64>,
    sender: mpsc::Sender<Result<Vec<Received>, ConnectorError>>,
    paused: Arc<AtomicBool>,
) {
    let mut backoff = Duration::from_millis(100);
    let mut request = json!({
        "QueueUrl": url,
        "MaxNumberOfMessages": PER_CALL,
        "WaitTimeSeconds": wait_seconds,
        "MessageSystemAttributeNames": ["SentTimestamp"],
    });
    if let Some(seconds) = visibility {
        request["VisibilityTimeout"] = json!(seconds);
    }
    while !sender.is_closed() {
        if paused.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        match api.call("ReceiveMessage", &request).await {
            Ok(answer) => {
                backoff = Duration::from_millis(100);
                let messages: Vec<Received> = answer
                    .get("Messages")
                    .and_then(Value::as_array)
                    .map(|list| list.iter().filter_map(received).collect())
                    .unwrap_or_default();
                if !messages.is_empty() && sender.send(Ok(messages)).await.is_err() {
                    return;
                }
            }
            Err(error) => {
                if sender.send(Err(error)).await.is_err() {
                    return;
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }
    }
}

fn received(message: &Value) -> Option<Received> {
    Some(Received {
        body: message.get("Body")?.as_str()?.as_bytes().to_vec(),
        receipt: message.get("ReceiptHandle")?.as_str()?.to_string(),
        sent_ms: message
            .pointer("/Attributes/SentTimestamp")
            .and_then(Value::as_str)
            .and_then(|t| t.parse().ok()),
    })
}

/// Sends `action` with the entries, 10 a call and several calls at once: the first failure
/// that is not a message gone or already back in the queue, if any.
async fn batch_calls(
    api: Arc<Api>,
    url: String,
    action: &'static str,
    entries: Vec<Value>,
) -> Result<Option<String>, ConnectorError> {
    let bodies: Vec<Value> = entries
        .chunks(PER_CALL)
        .map(|chunk| {
            let entries: Vec<Value> = chunk
                .iter()
                .enumerate()
                .map(|(i, entry)| {
                    let mut entry = entry.clone();
                    entry["Id"] = json!(i.to_string());
                    entry
                })
                .collect();
            json!({"QueueUrl": url, "Entries": entries})
        })
        .collect();
    let answers: Vec<Result<Value, ConnectorError>> =
        futures_util::stream::iter(bodies.into_iter().map(|body| {
            let api = api.clone();
            async move { api.call(action, &body).await }
        }))
        .buffer_unordered(CALLS_AT_ONCE)
        .collect()
        .await;
    for answer in answers {
        let answer = answer?;
        for failure in answer
            .get("Failed")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let code = failure.get("Code").and_then(Value::as_str).unwrap_or("");
            // The message came back after its visibility timeout and was received again, or
            // is gone: its next receipt deletes it.
            if code.contains("ReceiptHandleIsInvalid") || code.contains("MessageNotInflight") {
                continue;
            }
            let message = failure.get("Message").and_then(Value::as_str).unwrap_or("");
            return Ok(Some(format!("SQS {action}: {code} {message}")));
        }
    }
    Ok(None)
}

impl Source for SqsSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if self.connected.is_none() {
                return self.connect().await;
            }
            loop {
                if self.full() {
                    return Ok(self.take()?.expect("full"));
                }
                self.extend().await?;
                if self.paused.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return Ok(self.take()?.unwrap_or(SourceEvent::Idle));
                }
                let wait = self.wait();
                let connected = self.connected.as_mut().expect("connected");
                match tokio::time::timeout(wait, connected.messages.recv()).await {
                    Ok(Some(Ok(messages))) => {
                        for message in messages {
                            self.add(message);
                        }
                    }
                    Ok(Some(Err(error))) => return Err(error),
                    Ok(None) => {
                        self.connected = None;
                        self.pending = None;
                        self.handles.clear();
                        return Ok(SourceEvent::Revoking {
                            partition: PARTITION,
                            epoch: AssignmentEpoch(self.epoch),
                            within: Duration::from_millis(0),
                        });
                    }
                    Err(_) => return Ok(self.take()?.unwrap_or(SourceEvent::Idle)),
                }
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            let mut done = Vec::new();
            for commit in commits {
                if commit.epoch != AssignmentEpoch(self.epoch) || self.connected.is_none() {
                    return CommitOutcome::Stale;
                }
                let Position::Offset(last) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "SQS acknowledges message positions",
                    ));
                };
                let later = self.handles.split_off(&(last as u64 + 1));
                done.extend(std::mem::replace(&mut self.handles, later).into_values());
            }
            if done.is_empty() {
                return CommitOutcome::Committed;
            }
            let url = self.connected.as_ref().expect("checked").url.clone();
            let entries = done
                .into_iter()
                .map(|(receipt, _)| json!({"ReceiptHandle": receipt}))
                .collect();
            let deleted = batch_calls(self.api.clone(), url, "DeleteMessageBatch", entries).await;
            match deleted {
                Ok(None) => CommitOutcome::Committed,
                // Not deleted: the messages come back after their visibility timeout.
                Ok(Some(error)) => CommitOutcome::Failed(ConnectorError::retryable(error)),
                Err(error) => CommitOutcome::Failed(error),
            }
        })
    }

    fn pause(&mut self, _partition: PartitionId) {
        self.paused.store(true, Ordering::Relaxed);
    }

    fn resume(&mut self, _partition: PartitionId) {
        self.paused.store(false, Ordering::Relaxed);
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            replayable: true,
            ordered: false,
            ack_kind: AckKind::Message,
            max_instances: None,
            needs_keepalive: false,
            durable_positions: false,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.connected = None;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> BatchLimits {
        BatchLimits {
            max_records: 100,
            max_bytes: 1 << 20,
            max_wait: Duration::from_millis(100),
        }
    }

    #[test]
    fn settings_need_a_queue_and_take_seconds() {
        assert!(SqsSource::new(&json!({}), limits()).is_err());
        let source = SqsSource::new(
            &json!({"queue_name": "orders", "region": "eu-west-1", "wait_time_seconds": 30,
                    "receivers": 8, "visibility_timeout": "45"}),
            limits(),
        )
        .unwrap();
        // SQS waits 20 seconds at most.
        assert_eq!(source.wait_seconds, 20);
        assert_eq!(source.receivers, 8);
        assert_eq!(source.visibility, Some(45));
        assert_eq!(source.api.endpoint(), "https://sqs.eu-west-1.amazonaws.com");
        assert!(
            SqsSource::new(
                &json!({"queue_name": "q", "region": "x", "visibility_timeout": "soon"}),
                limits()
            )
            .is_err()
        );
    }

    #[test]
    fn received_messages_keep_their_body_receipt_and_time() {
        let message = received(&json!({
            "Body": "{\"n\": 1}", "ReceiptHandle": "r1",
            "Attributes": {"SentTimestamp": "1760000000000"},
        }))
        .unwrap();
        assert_eq!(message.body, b"{\"n\": 1}");
        assert_eq!(message.receipt, "r1");
        assert_eq!(message.sent_ms, Some(1_760_000_000_000));
        assert!(received(&json!({"Body": "x"})).is_none());
    }
}
