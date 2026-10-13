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
}

impl PythonStreamSession {
    /// `outputs`: the blocks whose messages the sinks write.
    pub fn new(
        pool: Arc<PythonPool>,
        project_dir: &std::path::Path,
        pipeline: &Pipeline,
        order: &[Block],
        outputs: Vec<String>,
        log: RunLog,
        cancel: CancellationToken,
    ) -> PythonStreamSession {
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
}

#[derive(Deserialize)]
struct StreamReply {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    error: Value,
    #[serde(default)]
    outputs: BTreeMap<String, Vec<Box<RawValue>>>,
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
            let messages = envelopes
                .iter()
                .map(message_value)
                .collect::<Result<Vec<_>, _>>()?;
            let (chain, outputs, kwargs) = (&self.chain, &self.outputs, &self.kwargs);
            let line = |id: &str| {
                serde_json::to_string(&StreamRequest {
                    id,
                    op: "stream",
                    chain,
                    messages: &messages,
                    outputs,
                    kwargs,
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
            Ok(TransformOutput {
                outputs,
                row_errors: Vec::new(),
            })
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
