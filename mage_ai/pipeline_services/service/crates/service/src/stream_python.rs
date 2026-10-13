//! A stream's Python blocks as a [`TransformSession`]: each batch goes to a pooled worker as
//! one `stream` request (the JSON protocol of ADR-0018; the worker keeps the blocks compiled),
//! and each block's messages come back as an envelope batch.
//!
//! A message's value is parsed as JSON when it is JSON and passed as text otherwise, as
//! Mage's streaming transformers receive it. Output messages are written as JSON values in
//! the input batch's partition; output rows do not map one to one to input rows, so they have
//! no position.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mage_service_core::manifest::{Block, Pipeline};
use mage_stream_ports::{
    BoxFuture, Envelope, EnvelopeBuilder, RecordBatch, TransformError, TransformOutput,
    TransformSession, read_envelope,
};
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

fn message_value(envelope: &Envelope) -> Value {
    serde_json::from_slice(&envelope.value)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&envelope.value).into_owned()))
}

fn to_batch(messages: &[Value], partition: u32) -> Result<RecordBatch, TransformError> {
    let mut builder = EnvelopeBuilder::new();
    for message in messages {
        let value =
            serde_json::to_vec(message).map_err(|e| TransformError::Protocol(e.to_string()))?;
        builder.push(&Envelope {
            value,
            partition,
            ..Envelope::default()
        });
    }
    builder
        .finish()
        .map_err(|e| TransformError::Protocol(e.to_string()))
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
            let messages: Vec<Value> = envelopes.iter().map(message_value).collect();
            let request = json!({
                "op": "stream",
                "chain": self.chain,
                "messages": messages,
                "outputs": self.outputs,
                "kwargs": self.kwargs,
            });
            let reply = self
                .pool
                .run(
                    request,
                    self.log.clone(),
                    "stream",
                    self.timeout,
                    &self.cancel,
                )
                .await
                .map_err(|error| match error {
                    WorkerError::Timeout(_) => TransformError::Timeout(error.to_string()),
                    WorkerError::Crashed(_) | WorkerError::Start(_) | WorkerError::Cancelled => {
                        TransformError::Crash(error.to_string())
                    }
                    WorkerError::Protocol(_) => TransformError::Protocol(error.to_string()),
                })?;
            if reply.get("ok").and_then(Value::as_bool) != Some(true) {
                let error = &reply["error"];
                let block = error["block_uuid"].as_str().unwrap_or("a block");
                let message = error["message"].as_str().unwrap_or("failed");
                let details = error["details"].as_str().unwrap_or("");
                if !details.is_empty() {
                    self.log.error(Some(block), details);
                }
                return Err(TransformError::User(format!("{block}: {message}")));
            }
            let mut outputs = BTreeMap::new();
            for (block, messages) in reply["outputs"].as_object().into_iter().flatten() {
                let messages = messages.as_array().cloned().unwrap_or_default();
                outputs.insert(block.clone(), to_batch(&messages, partition)?);
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
