//! A stream's Python blocks as a [`TransformSession`]: each batch goes to a pooled worker as
//! one `stream` request (the JSON protocol of ADR-0018; the worker keeps the blocks compiled),
//! and each block's messages come back as an envelope batch.
//!
//! A message's value goes to the worker as the JSON it is (the worker parses it, so numbers
//! keep every digit), or as a JSON string when it is text that is not JSON, as Mage's
//! streaming transformers receive it; a value that is neither is an error, never rewritten.
//! Output messages are written as the worker wrote them, byte for byte, in the input batch's
//! partition; output rows do not map one to one to input rows, so they have no position.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mage_service_core::manifest::{Block, Pipeline};
use mage_stream_ports::{
    BoxFuture, Envelope, EnvelopeBuilder, RecordBatch, TransformError, TransformOutput,
    TransformSession, read_envelope,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::log::RunLog;
use crate::python::{PythonPool, WorkerError};

pub struct PythonStreamSession {
    pool: Arc<PythonPool>,
    chain: Vec<Value>,
    outputs: Vec<String>,
    kwargs: Value,
    log: RunLog,
    timeout: Option<Duration>,
    cancel: CancellationToken,
    /// Ask the worker for the messages that fail on their own (row-level dead letters).
    isolate: bool,
    shape: MessageShape,
}

/// How a stream's Python session runs, besides its blocks.
pub struct SessionOptions {
    /// The blocks whose messages the sinks write.
    pub outputs: Vec<String>,
    pub log: RunLog,
    pub cancel: CancellationToken,
    /// Find the messages that fail on their own (the stream has a dead-letter sink).
    pub isolate: bool,
    /// How blocks receive messages: their values, or for a RabbitMQ source Mage's
    /// `Payload(method, properties, body)`, so its blocks run unchanged.
    pub shape: MessageShape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageShape {
    Values,
    RabbitmqPayload,
}

impl PythonStreamSession {
    pub fn new(
        pool: Arc<PythonPool>,
        project_dir: &std::path::Path,
        pipeline: &Pipeline,
        order: &[Block],
        options: SessionOptions,
    ) -> PythonStreamSession {
        let SessionOptions {
            outputs,
            log,
            cancel,
            isolate,
            shape,
        } = options;
        let chain = order
            .iter()
            .map(|block| {
                json!({
                    "uuid": block.uuid,
                    "type": block.block_type.as_str(),
                    "language": "python",
                    "file": project_dir.join(&block.file),
                    "sha256": block.sha256,
                    "configuration": block.configuration,
                    "upstream": block.upstream,
                    "pipeline_uuid": pipeline.uuid,
                })
            })
            .collect();
        let timeout = order
            .iter()
            .filter_map(|b| b.timeout_seconds)
            .max()
            .map(Duration::from_secs);
        PythonStreamSession {
            pool,
            chain,
            outputs,
            kwargs: json!(pipeline.variables),
            log,
            timeout,
            cancel,
            isolate,
            shape,
        }
    }
}

/// The message's value as JSON for the worker, unparsed.
fn message_value(envelope: &Envelope) -> Result<Box<RawValue>, TransformError> {
    if let Ok(raw) = serde_json::from_slice::<&RawValue>(&envelope.value) {
        return Ok(raw.to_owned());
    }
    match std::str::from_utf8(&envelope.value) {
        Ok(text) => serde_json::value::to_raw_value(text)
            .map_err(|e| TransformError::Protocol(e.to_string())),
        Err(_) => Err(TransformError::User(format!(
            "the message at position {} of partition {} is neither JSON nor UTF-8 text; \
             Python stream blocks get JSON or text messages, and binary data is not rewritten",
            envelope
                .position
                .map_or_else(|| "?".to_string(), |p| p.to_string()),
            envelope.partition
        ))),
    }
}

/// A RabbitMQ delivery for the worker, which makes Mage's `Payload` of it: the body as text
/// (or base64 when it is not UTF-8), with its headers, routing key and delivery tag.
fn rabbitmq_message(envelope: &Envelope) -> Result<Box<RawValue>, TransformError> {
    use base64::Engine as _;

    let headers: serde_json::Map<String, Value> = envelope
        .headers
        .iter()
        .map(|h| {
            (
                h.key.clone(),
                Value::String(String::from_utf8_lossy(&h.value).into_owned()),
            )
        })
        .collect();
    let mut message = json!({
        "headers": headers,
        "routing_key": envelope.key.as_ref().map(|k| String::from_utf8_lossy(k).into_owned()),
        "delivery_tag": envelope.position,
    });
    match std::str::from_utf8(&envelope.value) {
        Ok(text) => message["body"] = Value::String(text.to_string()),
        Err(_) => {
            message["body_base64"] =
                Value::String(base64::engine::general_purpose::STANDARD.encode(&envelope.value))
        }
    }
    serde_json::value::to_raw_value(&message).map_err(|e| TransformError::Protocol(e.to_string()))
}

fn to_batch(messages: &[Box<RawValue>], partition: u32) -> Result<RecordBatch, TransformError> {
    let mut builder = EnvelopeBuilder::new();
    for message in messages {
        builder.push(&Envelope {
            value: message.get().as_bytes().to_vec(),
            partition,
            ..Envelope::default()
        });
    }
    builder
        .finish()
        .map_err(|e| TransformError::Protocol(e.to_string()))
}

#[derive(Serialize)]
struct StreamRequest<'a> {
    id: &'a str,
    op: &'static str,
    chain: &'a [Value],
    messages: &'a [Box<RawValue>],
    outputs: &'a [String],
    kwargs: &'a Value,
    isolate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_shape: Option<&'static str>,
}

#[derive(Deserialize)]
struct StreamReply {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    error: Value,
    #[serde(default)]
    outputs: BTreeMap<String, Vec<Box<RawValue>>>,
    /// Messages that failed on their own, by their row in the batch.
    #[serde(default)]
    failures: Vec<Failure>,
}

#[derive(Deserialize)]
struct Failure {
    row: usize,
    #[serde(default)]
    block_uuid: Option<String>,
    #[serde(default)]
    message: String,
}

impl PythonStreamSession {
    /// Loads the stream's blocks (their top-level code, such as a model) into a worker in the
    /// background, after the one serving the stream was replaced, so the next batch does not
    /// wait for it.
    fn preload(&self) {
        let pool = self.pool.clone();
        let chain = self.chain.clone();
        let log = self.log.clone();
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let request = json!({"op": "stream_load", "chain": chain});
            let _ = pool.run(request, log, "stream", None, &cancel).await;
        });
    }
}

impl TransformSession for PythonStreamSession {
    fn transform(
        &mut self,
        batch: RecordBatch,
    ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
        Box::pin(async move {
            let envelopes =
                read_envelope(&batch).map_err(|e| TransformError::Protocol(e.to_string()))?;
            let partition = envelopes.first().map_or(0, |e| e.partition);
            let messages = match self.shape {
                MessageShape::Values => envelopes
                    .iter()
                    .map(message_value)
                    .collect::<Result<Vec<_>, _>>()?,
                MessageShape::RabbitmqPayload => envelopes
                    .iter()
                    .map(rabbitmq_message)
                    .collect::<Result<Vec<_>, _>>()?,
            };
            let shape = self.shape;
            let (chain, outputs, kwargs, isolate) =
                (&self.chain, &self.outputs, &self.kwargs, self.isolate);
            let line = |id: &str| {
                serde_json::to_string(&StreamRequest {
                    id,
                    op: "stream",
                    chain,
                    messages: &messages,
                    outputs,
                    kwargs,
                    isolate,
                    message_shape: (shape == MessageShape::RabbitmqPayload).then_some("rabbitmq"),
                })
            };
            let (reply, replaced) = self
                .pool
                .run_stream(line, self.log.clone(), self.timeout, &self.cancel)
                .await
                .map_err(|error| match error {
                    WorkerError::Timeout(_) => TransformError::Timeout(error.to_string()),
                    WorkerError::Crashed(_) | WorkerError::Start(_) | WorkerError::Cancelled => {
                        TransformError::Crash(error.to_string())
                    }
                    WorkerError::Protocol(_) => TransformError::Protocol(error.to_string()),
                })?;
            if replaced {
                self.preload();
            }
            let reply: StreamReply = serde_json::from_str(&reply)
                .map_err(|e| TransformError::Protocol(format!("the worker's reply: {e}")))?;
            if !reply.ok {
                let error = &reply.error;
                let block = error["block_uuid"].as_str().unwrap_or("a block");
                let message = error["message"].as_str().unwrap_or("failed");
                let details = error["details"].as_str().unwrap_or("");
                if !details.is_empty() {
                    self.log.error(Some(block), details);
                }
                return Err(TransformError::User(format!("{block}: {message}")));
            }
            let mut outputs = BTreeMap::new();
            for (block, messages) in &reply.outputs {
                outputs.insert(block.clone(), to_batch(messages, partition)?);
            }
            let row_errors = reply
                .failures
                .iter()
                .map(|failure| mage_stream_ports::RowError {
                    row: failure.row,
                    error: format!(
                        "{}: {}",
                        failure.block_uuid.as_deref().unwrap_or("a block"),
                        failure.message
                    ),
                })
                .collect();
            Ok(TransformOutput {
                outputs,
                row_errors,
            })
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
