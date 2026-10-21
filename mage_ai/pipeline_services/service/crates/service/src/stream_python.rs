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
    /// Whether the first blocks take a DataFrame, as the worker says when the stream starts:
    /// then a batch goes as a JSON lines file Polars reads in one pass.
    frames: Option<bool>,
    file: std::path::PathBuf,
    /// Blocks with keyed state (configuration `state_key`).
    state: crate::keyed_state::BlockStates,
}

static FILES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
    /// Where blocks with keyed state keep it (`<data>/state/<pipeline>`).
    pub state_dir: std::path::PathBuf,
    pub stable_positions: bool,
    /// The stream's event time, which timers of keyed state follow.
    pub time: Option<mage_service_core::manifest::EventTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageShape {
    Values,
    RabbitmqPayload,
    /// Mage's RAW_VALUE: blocks get each message's bytes (sent as base64).
    RawBytes,
    /// Mage's Azure Event Hubs source: blocks get `{"data": <the body as text>}`.
    EventHubsData,
    /// Mage's SQS RAW_VALUE: blocks get each body as text.
    RawText,
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
            state_dir,
            stable_positions,
            time,
        } = options;
        let state = crate::keyed_state::BlockStates::new(order, &state_dir, stable_positions, time);
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
            frames: None,
            state,
            file: std::env::temp_dir().join(format!(
                "mage-stream-{}-{}.ndjson",
                std::process::id(),
                FILES.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            )),
        }
    }

    /// Asks the worker what the stream's blocks take (stream_load, which also loads them).
    async fn learn_frames(&mut self) -> bool {
        if self.shape != MessageShape::Values {
            return false;
        }
        let request = json!({"op": "stream_load", "chain": self.chain});
        let Ok(reply) = self
            .pool
            .run(
                request,
                self.log.clone(),
                "stream",
                self.timeout,
                &self.cancel,
            )
            .await
        else {
            return false;
        };
        let roots: Vec<&str> = self
            .chain
            .iter()
            .filter(|b| b["upstream"].as_array().is_none_or(|u| u.is_empty()))
            .filter_map(|b| b["uuid"].as_str())
            .collect();
        !roots.is_empty()
            && roots.iter().all(|uuid| {
                matches!(
                    reply["inputs"][uuid].as_str(),
                    Some("polars") | Some("pandas")
                )
            })
    }
}

/// The message's value as JSON for the worker, unparsed.
/// Why the source could not decode the message (Avro), from its mage-decode-error header:
/// its bytes are not the message, whatever they are.
pub(crate) fn decode_error(envelope: &Envelope) -> Option<String> {
    envelope
        .headers
        .iter()
        .find(|h| h.key == "mage-decode-error")
        .map(|reason| {
            format!(
                "the message could not be read: {}",
                String::from_utf8_lossy(&reason.value)
            )
        })
}

fn message_value(envelope: &Envelope) -> Result<Box<RawValue>, TransformError> {
    let position = || {
        envelope
            .position
            .map_or_else(|| "?".to_string(), |p| p.to_string())
    };
    if let Ok(raw) = serde_json::from_slice::<&RawValue>(&envelope.value) {
        return Ok(raw.to_owned());
    }
    match std::str::from_utf8(&envelope.value) {
        Ok(text) => serde_json::value::to_raw_value(text)
            .map_err(|e| TransformError::Protocol(e.to_string())),
        Err(_) => Err(TransformError::User(format!(
            "the message at position {} of partition {} is neither JSON nor UTF-8 text; \
             Python stream blocks get JSON or text messages, and binary data is not rewritten",
            position(),
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

/// A message's body as text, unparsed (Mage's Event Hubs source hands blocks the text).
fn text_message(envelope: &Envelope) -> Result<Box<RawValue>, TransformError> {
    serde_json::value::to_raw_value(&String::from_utf8_lossy(&envelope.value))
        .map_err(|e| TransformError::Protocol(e.to_string()))
}

/// A message's bytes as base64 text, which the worker turns back into bytes.
fn raw_message(envelope: &Envelope) -> Result<Box<RawValue>, TransformError> {
    use base64::Engine as _;

    serde_json::value::to_raw_value(
        &base64::engine::general_purpose::STANDARD.encode(&envelope.value),
    )
    .map_err(|e| TransformError::Protocol(e.to_string()))
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
    #[serde(skip_serializing_if = "Option::is_none")]
    messages_file: Option<&'a std::path::Path>,
    count: usize,
    outputs: &'a [String],
    kwargs: &'a Value,
    isolate: bool,
    /// Per block with keyed state: the values and timers of the batch's keys, the timers
    /// that fire, and the time they are judged by.
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timers: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fired: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    watermark: Option<i64>,
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
    /// Each block's time and messages in and out.
    #[serde(default)]
    blocks: Vec<ReplyBlock>,
    /// Per block: the keys whose state changed, null for a removed one.
    #[serde(default)]
    state_updates: BTreeMap<String, crate::keyed_state::Changes>,
    /// Per block: the keys whose timer changed, null for a removed one.
    #[serde(default)]
    timer_updates: BTreeMap<String, crate::keyed_state::TimerChanges>,
}

#[derive(Deserialize)]
struct ReplyBlock {
    block_uuid: String,
    #[serde(default)]
    seconds: f64,
    #[serde(default)]
    messages_in: u64,
    #[serde(default)]
    messages_out: u64,
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
    /// Per block with keyed state, its field and what the service API reads of it.
    pub fn state_views(&self) -> Vec<(String, String, crate::keyed_state::StateView)> {
        self.state.views()
    }

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
            // Messages the source could not decode fail on their own, before any block; the
            // others go to the worker, which counts its rows among them.
            let mut undecoded = Vec::new();
            let mut rows = Vec::with_capacity(envelopes.len());
            let mut messages = Vec::with_capacity(envelopes.len());
            // With keyed state and offsets: messages the saved state already holds (read
            // again after a restart) are done; they are not run again.
            let held = self.state.held(partition).map_err(TransformError::Crash)?;
            let mut batch_keys = self.state.batch();
            for (row, envelope) in envelopes.iter().enumerate() {
                if let (Some(held), Some(position)) = (held, envelope.position)
                    && position <= held
                {
                    continue;
                }
                self.state.note(&mut batch_keys, envelope);
                if let Some(reason) = decode_error(envelope) {
                    undecoded.push(mage_stream_ports::RowError { row, error: reason });
                    continue;
                }
                rows.push(row);
                messages.push(match self.shape {
                    MessageShape::Values => message_value(envelope)?,
                    MessageShape::RabbitmqPayload => rabbitmq_message(envelope)?,
                    MessageShape::RawBytes => raw_message(envelope)?,
                    MessageShape::EventHubsData | MessageShape::RawText => text_message(envelope)?,
                });
            }
            if self.frames.is_none() {
                self.frames = Some(self.learn_frames().await);
            }
            let file = if self.frames == Some(true) {
                let mut lines =
                    Vec::with_capacity(messages.iter().map(|m| m.get().len() + 1).sum());
                for message in &messages {
                    lines.extend_from_slice(message.get().as_bytes());
                    lines.push(b'\n');
                }
                std::fs::write(&self.file, lines).map_err(|e| {
                    TransformError::Crash(format!("could not write the batch's file: {e}"))
                })?;
                Some(self.file.as_path())
            } else {
                None
            };
            let count = messages.len();
            if count == 0 && !self.state.is_empty() && undecoded.is_empty() {
                // Every message was held already.
                return Ok(TransformOutput::default());
            }
            self.state
                .fire(partition, &mut batch_keys)
                .map_err(TransformError::Crash)?;
            let outgoing = self
                .state
                .outgoing(partition, &batch_keys)
                .map_err(TransformError::Crash)?;
            let sent: &[Box<RawValue>] = if file.is_some() { &[] } else { &messages };
            let shape = self.shape;
            let (chain, outputs, kwargs, isolate) =
                (&self.chain, &self.outputs, &self.kwargs, self.isolate);
            let state_sent = outgoing.as_ref();
            let line = |id: &str| {
                serde_json::to_string(&StreamRequest {
                    id,
                    op: "stream",
                    chain,
                    messages: sent,
                    messages_file: file,
                    count,
                    outputs,
                    kwargs,
                    isolate,
                    state: state_sent.map(|o| &o.state),
                    timers: state_sent.map(|o| &o.timers),
                    fired: state_sent.map(|o| &o.fired),
                    watermark: state_sent.and_then(|o| o.watermark),
                    message_shape: match shape {
                        MessageShape::Values => None,
                        MessageShape::RabbitmqPayload => Some("rabbitmq"),
                        MessageShape::RawBytes => Some("raw"),
                        MessageShape::EventHubsData => Some("event_hub"),
                        MessageShape::RawText => Some("text"),
                    },
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
            self.state
                .apply(
                    partition,
                    &batch_keys,
                    &reply.state_updates,
                    &reply.timer_updates,
                )
                .map_err(TransformError::Crash)?;
            let mut outputs = BTreeMap::new();
            for (block, messages) in &reply.outputs {
                outputs.insert(block.clone(), to_batch(messages, partition)?);
            }
            let mut row_errors: Vec<_> = reply
                .failures
                .iter()
                .map(|failure| mage_stream_ports::RowError {
                    row: rows.get(failure.row).copied().unwrap_or(failure.row),
                    error: format!(
                        "{}: {}",
                        failure.block_uuid.as_deref().unwrap_or("a block"),
                        failure.message
                    ),
                })
                .collect();
            row_errors.extend(undecoded);
            row_errors.sort_by_key(|e| e.row);
            let blocks = reply
                .blocks
                .iter()
                .map(|b| mage_stream_ports::BlockStats {
                    block: b.block_uuid.clone(),
                    messages_in: b.messages_in,
                    messages_out: b.messages_out,
                    micros: (b.seconds * 1_000_000.0).round() as u64,
                })
                .collect();
            Ok(TransformOutput {
                outputs,
                row_errors,
                blocks,
            })
        })
    }

    fn committed<'a>(
        &'a mut self,
        commits: &'a [mage_stream_ports::Commit],
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move { self.state.save(commits) })
    }

    fn revoked(&mut self, partition: mage_stream_ports::PartitionId) {
        self.state.revoked(partition.0);
    }

    fn keeps_state(&self) -> bool {
        !self.state.is_empty()
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let _ = std::fs::remove_file(&self.file);
        })
    }
}
