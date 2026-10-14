//! A stream's Rust blocks as a [`TransformSession`]: one resident program (the service's
//! `mage-blocks` with MAGE_STREAM=1, mage::serve_stream) runs them in order on each batch.
//! User code stays out of the engine process (ADR-0020); the program is started on the first
//! batch and again after it stops.
//!
//! The program connects to a Unix socket the session listens on and says it is ready. Each
//! batch goes as a header frame and one frame per message value, unchanged; the answer is a
//! header frame and the messages of each block a sink writes. A frame is a little-endian u32
//! length and its bytes. The block's stdout and stderr go to the stream's log.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mage_service_core::manifest::{Block, Pipeline};
use mage_stream_ports::{
    BoxFuture, Envelope, EnvelopeBuilder, RecordBatch, RowError, TransformError, TransformOutput,
    TransformSession, read_envelope,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixListener;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::sync::CancellationToken;

use crate::log::RunLog;
use crate::service::Service;

/// The frames' version; mage::protocol::STREAM_PROTOCOL in the block SDK.
const PROTOCOL: u64 = 1;
/// How long a started program has to connect and say it is ready.
const START_WITHIN: Duration = Duration::from_secs(60);
/// The marker the SDK prints before each block's output (mage::runtime::BLOCK_MARKER).
const BLOCK_MARKER: &str = "\u{1e}mage-block ";

static SOCKETS: AtomicU64 = AtomicU64::new(0);

pub struct RustStreamSession {
    service: Arc<Service>,
    binary: PathBuf,
    directory: PathBuf,
    /// stream.json without the socket, which each start adds.
    job: Value,
    io_config: Option<Value>,
    log: RunLog,
    first_block: String,
    timeout: Option<Duration>,
    cancel: CancellationToken,
    live: Option<Live>,
    next_id: u64,
}

struct Live {
    child: tokio::process::Child,
    pid: u32,
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

/// How a stream's Rust session runs, besides its blocks.
pub struct RustSessionOptions {
    /// The blocks whose messages the sinks write.
    pub outputs: Vec<String>,
    pub log: RunLog,
    pub cancel: CancellationToken,
    /// Find the messages that fail on their own (the stream has a dead-letter sink).
    pub isolate: bool,
}

impl RustStreamSession {
    /// The blocks must form one chain (each reads the one before) and share one io_config
    /// profile, as the program runs them in order with one environment.
    pub fn new(
        service: Arc<Service>,
        pipeline: &Pipeline,
        order: &[Block],
        options: RustSessionOptions,
    ) -> Result<RustStreamSession, String> {
        let first = order.first().ok_or("the stream has no Rust blocks")?;
        let binary = first
            .binary
            .as_ref()
            .map(|b| service.service_dir.join(b))
            .ok_or_else(|| format!("Rust block {} has no binary", first.uuid))?;
        let mut chain = Vec::with_capacity(order.len());
        for (position, block) in order.iter().enumerate() {
            let expected: Vec<&str> = order[..position]
                .last()
                .map(|b| vec![b.uuid.as_str()])
                .unwrap_or_default();
            if block
                .upstream
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                != expected
            {
                return Err(format!(
                    "Rust stream block {} does not read the block before it; Rust stream \
                     blocks run as one chain",
                    block.uuid
                ));
            }
            if block.io_config != first.io_config {
                return Err(format!(
                    "Rust stream blocks {} and {} use different io_config profiles; the \
                     blocks of a stream share one",
                    first.uuid, block.uuid
                ));
            }
            let entry = block
                .entry
                .as_ref()
                .ok_or_else(|| format!("Rust block {} has no entry", block.uuid))?;
            chain.push(json!({
                "block_uuid": block.uuid,
                "block_type": block.block_type.as_str(),
                "entry": entry,
                "send": options.outputs.contains(&block.uuid),
            }));
        }
        let timeout = order
            .iter()
            .filter_map(|b| b.timeout_seconds)
            .max()
            .map(Duration::from_secs);
        let directory = service
            .data_dir
            .join("streams")
            .join(crate::executor::sanitize(&pipeline.uuid))
            .join("rust");
        Ok(RustStreamSession {
            binary,
            directory,
            job: json!({
                "api_version": 1,
                "pipeline_uuid": pipeline.uuid,
                "variables": pipeline.variables,
                "chain": chain,
                "isolate": options.isolate,
            }),
            io_config: first.io_config.clone(),
            first_block: first.uuid.clone(),
            log: options.log,
            timeout,
            cancel: options.cancel,
            service,
            live: None,
            next_id: 0,
        })
    }

    async fn start(&mut self) -> Result<Live, String> {
        std::fs::create_dir_all(&self.directory)
            .map_err(|e| format!("could not make {}: {e}", self.directory.display()))?;
        // A short path: Unix socket paths hold about 100 bytes.
        let socket = std::env::temp_dir().join(format!(
            "mage-{}-{}.sock",
            std::process::id(),
            SOCKETS.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket)
            .map_err(|e| format!("could not listen on {}: {e}", socket.display()))?;
        let mut job = self.job.clone();
        job["socket"] = json!(socket);
        std::fs::write(self.directory.join("stream.json"), job.to_string())
            .map_err(|e| format!("could not write stream.json: {e}"))?;
        let started = self.spawn().await;
        let outcome = match started {
            Ok(mut child) => {
                let pid = child.id().unwrap_or(0);
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted.map_err(|e| e.to_string()),
                    status = child.wait() => Err(match status {
                        Ok(status) => format!("it exited at start ({})", crate::python::describe_exit(status)),
                        Err(e) => e.to_string(),
                    }),
                    _ = tokio::time::sleep(START_WITHIN) => {
                        Err(format!("it did not connect within {} seconds", START_WITHIN.as_secs()))
                    }
                    _ = self.cancel.cancelled() => Err("the stream stopped".to_string()),
                };
                match accepted {
                    Ok((stream, _)) => {
                        let (read, write) = stream.into_split();
                        let mut live = Live {
                            child,
                            pid,
                            reader: BufReader::with_capacity(1 << 20, read),
                            writer: BufWriter::with_capacity(1 << 20, write),
                        };
                        match tokio::time::timeout(START_WITHIN, read_frame(&mut live.reader)).await
                        {
                            Ok(Ok(Some(frame))) => check_ready(&frame).map(|()| live),
                            Ok(Ok(None)) => Err("it closed the connection at start".into()),
                            Ok(Err(e)) => Err(e.to_string()),
                            Err(_) => Err("it did not say it was ready".into()),
                        }
                    }
                    Err(error) => {
                        crate::python::kill_group(pid);
                        Err(error)
                    }
                }
            }
            Err(error) => Err(error),
        };
        let _ = std::fs::remove_file(&socket);
        outcome.map_err(|e| format!("the stream's Rust blocks did not start: {e}"))
    }

    async fn spawn(&self) -> Result<tokio::process::Child, String> {
        let mut command = tokio::process::Command::new(&self.binary);
        command
            .arg(&self.directory)
            .current_dir(self.service.project_dir())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true)
            // Only the block environment: the service's own credentials are not inherited.
            .env_clear()
            .envs(self.service.extra_env.iter().cloned())
            .env("MAGE_STREAM", "1");
        if let Some(profile) = &self.io_config {
            let extra = &self.service.extra_env;
            let lookup = |name: &str| {
                extra
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .or_else(|| std::env::var(name).ok())
            };
            command.env(
                "MAGE_IO_CONFIG",
                mage_service_core::io_config::render(profile, &lookup).to_string(),
            );
        }
        let mut child = command
            .spawn()
            .map_err(|e| format!("could not start {}: {e}", self.binary.display()))?;
        let current = Arc::new(std::sync::Mutex::new(self.first_block.clone()));
        if let Some(stdout) = child.stdout.take() {
            forward(stdout, "stdout", self.log.clone(), current.clone());
        }
        if let Some(stderr) = child.stderr.take() {
            forward(stderr, "stderr", self.log.clone(), current);
        }
        Ok(child)
    }

    /// Stops the program after a failed exchange; the next batch starts it again.
    async fn stop(&mut self) -> String {
        let Some(mut live) = self.live.take() else {
            return "it stopped".into();
        };
        drop(live.writer);
        match tokio::time::timeout(Duration::from_millis(500), live.child.wait()).await {
            Ok(Ok(status)) => format!("it exited ({})", crate::python::describe_exit(status)),
            _ => {
                crate::python::kill_group(live.pid);
                let _ = live.child.wait().await;
                "it was stopped".into()
            }
        }
    }

    async fn exchange(&mut self, id: u64, values: &[Vec<u8>]) -> std::io::Result<Reply> {
        let live = self.live.as_mut().expect("started");
        let header = json!({"id": id, "count": values.len()}).to_string();
        write_frame(&mut live.writer, header.as_bytes()).await?;
        for value in values {
            write_frame(&mut live.writer, value).await?;
        }
        live.writer.flush().await?;
        let header = read_frame(&mut live.reader)
            .await?
            .ok_or_else(|| std::io::Error::other("the connection closed"))?;
        let mut reply: Reply = serde_json::from_slice(&header)
            .map_err(|e| std::io::Error::other(format!("a reply's header: {e}")))?;
        if reply.id != id {
            return Err(std::io::Error::other(format!(
                "the reply is for batch {}, not {id}",
                reply.id
            )));
        }
        for output in &reply.outputs {
            let mut messages = Vec::with_capacity(output.count);
            for _ in 0..output.count {
                messages.push(
                    read_frame(&mut live.reader)
                        .await?
                        .ok_or_else(|| std::io::Error::other("a reply ended early"))?,
                );
            }
            reply.messages.push(messages);
        }
        Ok(reply)
    }
}

#[derive(Deserialize)]
struct Reply {
    id: u64,
    ok: bool,
    #[serde(default)]
    outputs: Vec<OutputCount>,
    #[serde(default)]
    failures: Vec<Failure>,
    #[serde(default)]
    error: Option<Failure>,
    #[serde(skip)]
    messages: Vec<Vec<Vec<u8>>>,
}

#[derive(Deserialize)]
struct OutputCount {
    block_uuid: String,
    count: usize,
}

#[derive(Deserialize)]
struct Failure {
    #[serde(default)]
    row: Option<usize>,
    block_uuid: String,
    message: String,
}

fn check_ready(frame: &[u8]) -> Result<(), String> {
    let ready: Value =
        serde_json::from_slice(frame).map_err(|e| format!("its first message: {e}"))?;
    match ready["protocol"].as_u64() {
        Some(PROTOCOL) => Ok(()),
        other => Err(format!(
            "it speaks stream protocol {}, this engine {PROTOCOL}; export the service again \
             to rebuild its Rust blocks",
            other.map_or_else(|| "?".to_string(), |v| v.to_string())
        )),
    }
}

fn forward(
    stream: impl AsyncRead + Unpin + Send + 'static,
    name: &'static str,
    log: RunLog,
    current: Arc<std::sync::Mutex<String>>,
) {
    tokio::spawn(async move {
        use tokio::io::AsyncBufReadExt;
        let mut lines = tokio::io::BufReader::new(stream).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(block) = line.strip_prefix(BLOCK_MARKER) {
                *current.lock().unwrap_or_else(|p| p.into_inner()) = block.to_string();
                continue;
            }
            let block = current.lock().unwrap_or_else(|p| p.into_inner()).clone();
            log.write("info", Some(&block), Some(name), &line);
        }
    });
}

async fn write_frame(writer: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> std::io::Result<()> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| std::io::Error::other("a message is larger than 4 GiB"))?;
    writer.write_all(&length.to_le_bytes()).await?;
    writer.write_all(bytes).await
}

/// The next frame, or None when the program closed the connection between frames.
async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Option<Vec<u8>>> {
    let mut length = [0u8; 4];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let mut bytes = vec![0u8; u32::from_le_bytes(length) as usize];
    reader.read_exact(&mut bytes).await?;
    Ok(Some(bytes))
}

fn to_batch(messages: Vec<Vec<u8>>, partition: u32) -> Result<RecordBatch, TransformError> {
    let mut builder = EnvelopeBuilder::new();
    for value in messages {
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

impl TransformSession for RustStreamSession {
    fn transform(
        &mut self,
        batch: RecordBatch,
    ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
        Box::pin(async move {
            let envelopes =
                read_envelope(&batch).map_err(|e| TransformError::Protocol(e.to_string()))?;
            let partition = envelopes.first().map_or(0, |e| e.partition);
            // Messages the source could not decode fail on their own, before any block.
            let mut undecoded = Vec::new();
            let mut rows = Vec::with_capacity(envelopes.len());
            let mut values: Vec<Vec<u8>> = Vec::with_capacity(envelopes.len());
            for (row, envelope) in envelopes.into_iter().enumerate() {
                match crate::stream_python::decode_error(&envelope) {
                    Some(error) => undecoded.push(RowError { row, error }),
                    None => {
                        rows.push(row);
                        values.push(envelope.value);
                    }
                }
            }
            if self.live.is_none() {
                self.live = Some(self.start().await.map_err(TransformError::Crash)?);
            }
            self.next_id += 1;
            let id = self.next_id;
            let limit = self.timeout;
            let cancel = self.cancel.clone();
            let exchanged = tokio::select! {
                result = async {
                    match limit {
                        Some(limit) => tokio::time::timeout(limit, self.exchange(id, &values))
                            .await
                            .map_err(|_| limit),
                        None => Ok(self.exchange(id, &values).await),
                    }
                } => result,
                _ = cancel.cancelled() => {
                    self.stop().await;
                    return Err(TransformError::Crash("the stream stopped".into()));
                }
            };
            let reply = match exchanged {
                Ok(Ok(reply)) => reply,
                Ok(Err(error)) => {
                    let how = self.stop().await;
                    return Err(TransformError::Crash(format!(
                        "the stream's Rust blocks failed: {error}; {how}"
                    )));
                }
                Err(limit) => {
                    self.stop().await;
                    return Err(TransformError::Timeout(format!(
                        "the stream's Rust blocks ran longer than {} seconds on a batch and \
                         were stopped",
                        limit.as_secs()
                    )));
                }
            };
            if !reply.ok {
                let error = reply.error.unwrap_or(Failure {
                    row: None,
                    block_uuid: "a block".into(),
                    message: "failed".into(),
                });
                self.log.error(Some(&error.block_uuid), &error.message);
                return Err(TransformError::User(format!(
                    "{}: {}",
                    error.block_uuid, error.message
                )));
            }
            let mut outputs = BTreeMap::new();
            for (output, messages) in reply.outputs.iter().zip(reply.messages) {
                outputs.insert(output.block_uuid.clone(), to_batch(messages, partition)?);
            }
            let mut row_errors: Vec<RowError> = reply
                .failures
                .into_iter()
                .filter_map(|failure| {
                    failure.row.map(|row| RowError {
                        row: rows.get(row).copied().unwrap_or(row),
                        error: format!("{}: {}", failure.block_uuid, failure.message),
                    })
                })
                .collect();
            row_errors.extend(undecoded);
            row_errors.sort_by_key(|e| e.row);
            Ok(TransformOutput {
                outputs,
                row_errors,
            })
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if self.live.is_some() {
                self.stop().await;
            }
        })
    }
}

#[cfg(test)]
pub(crate) fn sample_ready() -> Vec<u8> {
    json!({"type": "ready", "protocol": PROTOCOL})
        .to_string()
        .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_program_of_another_protocol_is_refused_with_what_to_do() {
        assert!(check_ready(&sample_ready()).is_ok());
        let error = check_ready(br#"{"type":"ready","protocol":2}"#).unwrap_err();
        assert!(error.contains("export the service again"), "{error}");
    }

    #[tokio::test]
    async fn frames_round_trip() {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, b"one").await.unwrap();
        write_frame(&mut buffer, b"").await.unwrap();
        let mut reader = std::io::Cursor::new(buffer);
        assert_eq!(
            read_frame(&mut reader).await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(read_frame(&mut reader).await.unwrap(), Some(Vec::new()));
        assert_eq!(read_frame(&mut reader).await.unwrap(), None);
    }
}
