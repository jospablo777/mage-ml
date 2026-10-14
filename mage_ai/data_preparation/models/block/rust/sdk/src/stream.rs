//! Stream blocks: a stream's Rust transformers run in one resident program, on each batch
//! of messages the service engine sends, with no Python and no JSON written again.
//!
//! A stream transformer takes the batch's messages and returns messages:
//!
//! ```ignore
//! use mage::prelude::*;
//!
//! fn transform(messages: Vec<Value>, vars: Vars) -> Result<Vec<Value>> {
//!     let rate: f64 = vars.get_or("rate", 1.0)?;
//!     Ok(messages
//!         .into_iter()
//!         .map(|mut m| {
//!             m["total"] = json!(m["price"].as_f64().unwrap_or(0.0) * rate);
//!             m
//!         })
//!         .collect())
//! }
//! ```
//!
//! Messages come as `Vec<Value>` (JSON; text that is not JSON becomes a string, as Mage's
//! Python stream blocks get it), `DataFrame` or `LazyFrame` (one row per JSON object),
//! `Vec<String>` (each message's text) or `Vec<Vec<u8>>` (its bytes), followed by `Vars` or
//! `BlockContext` if the block needs them. The block returns `Vec<Value>`, one `Value` (an
//! array is its messages, null is none), a `DataFrame` or `LazyFrame` (one JSON object per
//! row), `Vec<String>` or `Vec<Vec<u8>>` (each written as it is), `()`, or an `Option` or
//! `Result` of these. Code at the top of the file, such as a model in a `LazyLock`, loads
//! once: the program stays up for the stream.
//!
//! The program connects to the Unix socket `stream.json` names and says it is ready. For each
//! batch the engine sends a header frame and one frame per message; the program answers
//! with a header frame, then the messages of each block the engine asked for, in order. A
//! frame is a little-endian u32 length and its bytes.

use std::io::{BufReader, BufWriter, Read, Write};
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::context::{BlockContext, Vars};
use crate::prelude::*;
use crate::protocol::{STREAM_PROTOCOL, StreamJob};
use crate::runtime::{
    BLOCK_MARKER, describe, exit_with_parent, install_panic_hook, set_current_block,
    take_panic_message,
};

/// A batch's messages as they travel: each message's bytes.
pub type Raw = Vec<Vec<u8>>;

/// What a stream block takes as its messages.
pub trait FromMessages: Sized {
    fn from_messages(messages: &[Vec<u8>]) -> anyhow::Result<Self>;
}

impl FromMessages for Vec<Value> {
    fn from_messages(messages: &[Vec<u8>]) -> anyhow::Result<Self> {
        messages
            .iter()
            .enumerate()
            .map(|(index, bytes)| match serde_json::from_slice(bytes) {
                Ok(value) => Ok(value),
                Err(_) => match std::str::from_utf8(bytes) {
                    Ok(text) => Ok(Value::String(text.to_string())),
                    Err(_) => Err(anyhow!(
                        "Message {index} is neither JSON nor UTF-8 text; a block that takes \
                         Vec<Vec<u8>> gets binary messages"
                    )),
                },
            })
            .collect()
    }
}

impl FromMessages for Vec<String> {
    fn from_messages(messages: &[Vec<u8>]) -> anyhow::Result<Self> {
        messages
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                String::from_utf8(bytes.clone()).map_err(|_| {
                    anyhow!(
                        "Message {index} is not UTF-8 text; a block that takes Vec<Vec<u8>> \
                         gets binary messages"
                    )
                })
            })
            .collect()
    }
}

impl FromMessages for Vec<Vec<u8>> {
    fn from_messages(messages: &[Vec<u8>]) -> anyhow::Result<Self> {
        Ok(messages.to_vec())
    }
}

impl FromMessages for DataFrame {
    fn from_messages(messages: &[Vec<u8>]) -> anyhow::Result<Self> {
        if messages.is_empty() {
            return Ok(DataFrame::empty());
        }
        let mut lines = Vec::with_capacity(messages.iter().map(|m| m.len() + 1).sum());
        for (index, bytes) in messages.iter().enumerate() {
            if bytes.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{') {
                bail!(
                    "Message {index} is not a JSON object; a block that takes a DataFrame gets \
                     one row per object (take Vec<Value> for other messages)"
                );
            }
            lines.extend_from_slice(bytes);
            lines.push(b'\n');
        }
        JsonReader::new(std::io::Cursor::new(lines))
            .with_json_format(JsonFormat::JsonLines)
            .infer_schema_len(None)
            .finish()
            .context("Reading the messages as a table")
    }
}

impl FromMessages for LazyFrame {
    fn from_messages(messages: &[Vec<u8>]) -> anyhow::Result<Self> {
        Ok(DataFrame::from_messages(messages)?.lazy())
    }
}

/// What a stream block takes after its messages.
pub trait FromContext: Sized {
    fn from_context(context: &BlockContext) -> Self;
}

impl FromContext for Vars {
    fn from_context(context: &BlockContext) -> Self {
        context.variables.clone()
    }
}

impl FromContext for BlockContext {
    fn from_context(context: &BlockContext) -> Self {
        context.clone()
    }
}

/// What a stream block returns.
pub trait IntoMessages {
    fn into_messages(self) -> anyhow::Result<Raw>;
}

impl IntoMessages for Vec<Value> {
    fn into_messages(self) -> anyhow::Result<Raw> {
        self.iter()
            .map(|value| serde_json::to_vec(value).map_err(Into::into))
            .collect()
    }
}

impl IntoMessages for Value {
    fn into_messages(self) -> anyhow::Result<Raw> {
        match self {
            Value::Null => Ok(Vec::new()),
            Value::Array(values) => values.into_messages(),
            value => Ok(vec![serde_json::to_vec(&value)?]),
        }
    }
}

impl IntoMessages for DataFrame {
    fn into_messages(mut self) -> anyhow::Result<Raw> {
        let mut buffer = Vec::new();
        JsonWriter::new(&mut buffer)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut self)
            .context("Writing the table's rows as messages")?;
        Ok(buffer
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(<[u8]>::to_vec)
            .collect())
    }
}

impl IntoMessages for LazyFrame {
    fn into_messages(self) -> anyhow::Result<Raw> {
        self.collect()
            .context("Computing the block's table")?
            .into_messages()
    }
}

impl IntoMessages for Vec<String> {
    fn into_messages(self) -> anyhow::Result<Raw> {
        Ok(self.into_iter().map(String::into_bytes).collect())
    }
}

impl IntoMessages for Vec<Vec<u8>> {
    fn into_messages(self) -> anyhow::Result<Raw> {
        Ok(self)
    }
}

impl IntoMessages for () {
    fn into_messages(self) -> anyhow::Result<Raw> {
        Ok(Vec::new())
    }
}

impl<T: IntoMessages> IntoMessages for Option<T> {
    fn into_messages(self) -> anyhow::Result<Raw> {
        self.map_or_else(|| Ok(Vec::new()), IntoMessages::into_messages)
    }
}

impl<T: IntoMessages, E: Into<anyhow::Error>> IntoMessages for Result<T, E> {
    fn into_messages(self) -> anyhow::Result<Raw> {
        self.map_err(Into::into)?.into_messages()
    }
}

/// A stream block function: its messages, then up to two of `Vars` and `BlockContext`.
#[diagnostic::on_unimplemented(
    message = "this function cannot be a Mage stream block function",
    label = "check its parameters and return type",
    note = "it takes Vec<Value>, DataFrame, LazyFrame, Vec<String> or Vec<Vec<u8>>, then optionally Vars or BlockContext",
    note = "it returns Vec<Value>, Value, DataFrame, LazyFrame, Vec<String>, Vec<Vec<u8>>, (), or an Option or Result of these"
)]
pub trait StreamFn<Args> {
    fn call(&mut self, messages: &[Vec<u8>], context: &BlockContext) -> anyhow::Result<Raw>;
}

impl<F, R, M> StreamFn<(M,)> for F
where
    F: FnMut(M) -> R,
    M: FromMessages,
    R: IntoMessages,
{
    fn call(&mut self, messages: &[Vec<u8>], _context: &BlockContext) -> anyhow::Result<Raw> {
        (self)(M::from_messages(messages)?).into_messages()
    }
}

impl<F, R, M, C1> StreamFn<(M, C1)> for F
where
    F: FnMut(M, C1) -> R,
    M: FromMessages,
    C1: FromContext,
    R: IntoMessages,
{
    fn call(&mut self, messages: &[Vec<u8>], context: &BlockContext) -> anyhow::Result<Raw> {
        (self)(M::from_messages(messages)?, C1::from_context(context)).into_messages()
    }
}

impl<F, R, M, C1, C2> StreamFn<(M, C1, C2)> for F
where
    F: FnMut(M, C1, C2) -> R,
    M: FromMessages,
    C1: FromContext,
    C2: FromContext,
    R: IntoMessages,
{
    fn call(&mut self, messages: &[Vec<u8>], context: &BlockContext) -> anyhow::Result<Raw> {
        (self)(
            M::from_messages(messages)?,
            C1::from_context(context),
            C2::from_context(context),
        )
        .into_messages()
    }
}

type StreamCall = Box<dyn FnMut(&[Vec<u8>], &BlockContext) -> anyhow::Result<Raw>>;

/// A stream block's function, for [`serve_stream`]: what a block's generated library
/// returns from `mage_stream_block`.
pub struct StreamBlock {
    call: StreamCall,
}

/// Wraps a stream block function for [`serve_stream`].
pub fn stream_block<Args, F>(mut function: F) -> StreamBlock
where
    F: StreamFn<Args> + 'static,
{
    StreamBlock {
        call: Box::new(move |messages, context| function.call(messages, context)),
    }
}

/// The stream blocks a program holds: each block's name and the function that returns it.
pub type StreamEntries = Vec<(&'static str, fn() -> StreamBlock)>;

struct Step {
    block: StreamBlock,
    context: BlockContext,
    send: bool,
}

#[derive(Serialize)]
struct Ready<'a> {
    r#type: &'static str,
    protocol: u32,
    sdk: &'static str,
    pid: u32,
    blocks: Vec<&'a str>,
}

#[derive(Deserialize)]
struct Request {
    id: u64,
    count: usize,
}

#[derive(Serialize)]
struct Reply {
    id: u64,
    ok: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    outputs: Vec<OutputCount>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    blocks: Vec<Timing>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    failures: Vec<Failure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Failure>,
}

#[derive(Serialize)]
struct OutputCount {
    block_uuid: String,
    count: usize,
}

#[derive(Serialize)]
struct Timing {
    block_uuid: String,
    seconds: f64,
}

#[derive(Serialize, Clone)]
struct Failure {
    /// The message's row in the batch, for a message that failed on its own.
    #[serde(skip_serializing_if = "Option::is_none")]
    row: Option<usize>,
    block_uuid: String,
    message: String,
}

/// The entry point of a program that serves a stream's Rust blocks: `binary <directory>`,
/// with `stream.json` in the directory. It runs until the engine closes the connection.
pub fn serve_stream(entries: StreamEntries) -> ! {
    let Some(directory) = std::env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("This runs Mage stream blocks; the service runs it with a directory.");
        std::process::exit(2);
    };
    install_panic_hook();
    exit_with_parent();
    match serve(&entries, &directory) {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("The stream's Rust blocks stopped: {}", describe(&error));
            std::process::exit(1);
        }
    }
}

fn serve(entries: &StreamEntries, directory: &std::path::Path) -> anyhow::Result<()> {
    let job = StreamJob::read(directory).context("Reading stream.json")?;
    let scratch_dir = directory.join("scratch");
    std::fs::create_dir_all(&scratch_dir)?;
    let mut steps = Vec::with_capacity(job.chain.len());
    for step in &job.chain {
        // A block's own program holds one block, named `block`.
        let make = entries
            .iter()
            .find(|(name, _)| *name == step.entry)
            .or_else(|| match entries.as_slice() {
                [only] if only.0 == "block" && job.chain.len() == 1 => Some(only),
                _ => None,
            })
            .map(|(_, make)| *make)
            .ok_or_else(|| anyhow!("This program has no stream block {}", step.entry))?;
        steps.push(Step {
            block: make(),
            context: BlockContext {
                block_uuid: step.block_uuid.clone(),
                block_type: step.block_type.clone(),
                pipeline_uuid: job.pipeline_uuid.clone(),
                execution_partition: None,
                variables: Vars(job.variables.clone()),
                scratch_dir: scratch_dir.clone(),
            },
            send: step.send,
        });
    }
    let socket = std::os::unix::net::UnixStream::connect(&job.socket)
        .with_context(|| format!("Connecting to the service at {}", job.socket.display()))?;
    let mut reader = BufReader::with_capacity(1 << 20, socket.try_clone()?);
    let mut writer = BufWriter::with_capacity(1 << 20, socket);
    let ready = Ready {
        r#type: "ready",
        protocol: STREAM_PROTOCOL,
        sdk: env!("CARGO_PKG_VERSION"),
        pid: std::process::id(),
        blocks: job.chain.iter().map(|s| s.block_uuid.as_str()).collect(),
    };
    write_frame(&mut writer, &serde_json::to_vec(&ready)?)?;
    writer.flush()?;

    loop {
        let Some(header) = read_frame(&mut reader)? else {
            return Ok(());
        };
        let request: Request = serde_json::from_slice(&header).context("A batch's header")?;
        let mut messages = Vec::with_capacity(request.count);
        for _ in 0..request.count {
            messages.push(read_frame(&mut reader)?.ok_or_else(|| anyhow!("A batch ended early"))?);
        }
        let (reply, outputs) = run_batch(&mut steps, request.id, messages, job.isolate);
        std::io::stdout().flush().ok();
        write_frame(&mut writer, &serde_json::to_vec(&reply)?)?;
        for output in &outputs {
            for message in output {
                write_frame(&mut writer, message)?;
            }
        }
        writer.flush()?;
    }
}

fn run_batch(steps: &mut [Step], id: u64, messages: Raw, isolate: bool) -> (Reply, Vec<Raw>) {
    let sent: Vec<String> = steps
        .iter()
        .filter(|s| s.send)
        .map(|s| s.context.block_uuid.clone())
        .collect();
    let (outputs, blocks, failures) = match run_chain(steps, &messages) {
        Ok((outputs, blocks)) => (outputs, blocks, Vec::new()),
        Err(error) if !isolate || messages.len() <= 1 => {
            return (failed(id, error), Vec::new());
        }
        Err(error) => {
            let (outputs, failures) = isolated(steps, &messages, 0);
            if failures.len() == messages.len() {
                // Every message failing is a problem of the code, not of the messages.
                return (failed(id, error), Vec::new());
            }
            (outputs, Vec::new(), failures)
        }
    };
    let reply = Reply {
        id,
        ok: true,
        outputs: sent
            .into_iter()
            .zip(&outputs)
            .map(|(block_uuid, output)| OutputCount {
                block_uuid,
                count: output.len(),
            })
            .collect(),
        blocks,
        failures,
        error: None,
    };
    (reply, outputs)
}

fn failed(id: u64, error: Failure) -> Reply {
    Reply {
        id,
        ok: false,
        outputs: Vec::new(),
        blocks: Vec::new(),
        failures: Vec::new(),
        error: Some(error),
    }
}

/// Runs the blocks in order on the messages: the messages of each block the engine wants,
/// and each block's time; or the failing block's error.
fn run_chain(steps: &mut [Step], messages: &Raw) -> Result<(Vec<Raw>, Vec<Timing>), Failure> {
    let mut outputs = Vec::new();
    let mut timings = Vec::with_capacity(steps.len());
    let mut current: Option<Raw> = None;
    for step in steps.iter_mut() {
        let input = current.as_deref().unwrap_or(messages);
        let output = if input.is_empty() && current.is_some() {
            // The block before kept nothing of this batch.
            Vec::new()
        } else {
            let uuid = &step.context.block_uuid;
            set_current_block(uuid);
            println!("{BLOCK_MARKER}{uuid}");
            let started = Instant::now();
            let outcome =
                panic::catch_unwind(AssertUnwindSafe(|| (step.block.call)(input, &step.context)));
            let output = match outcome {
                Ok(Ok(output)) => output,
                Ok(Err(error)) => {
                    return Err(Failure {
                        row: None,
                        block_uuid: uuid.clone(),
                        message: describe(&error),
                    });
                }
                Err(_) => {
                    return Err(Failure {
                        row: None,
                        block_uuid: uuid.clone(),
                        message: format!("The block panicked: {}", take_panic_message()),
                    });
                }
            };
            timings.push(Timing {
                block_uuid: uuid.clone(),
                seconds: started.elapsed().as_secs_f64(),
            });
            output
        };
        if step.send {
            outputs.push(output.clone());
        }
        current = Some(output);
    }
    Ok((outputs, timings))
}

/// The outputs of the messages that run, and the rows of those that fail on their own:
/// a failing batch is split in halves until each failure is one message.
fn isolated(steps: &mut [Step], messages: &[Vec<u8>], offset: usize) -> (Vec<Raw>, Vec<Failure>) {
    let batch = messages.to_vec();
    match run_chain(steps, &batch) {
        Ok((outputs, _)) => (outputs, Vec::new()),
        Err(mut failure) if messages.len() == 1 => {
            failure.row = Some(offset);
            (
                vec![Vec::new(); steps.iter().filter(|s| s.send).count()],
                vec![failure],
            )
        }
        Err(_) => {
            let middle = messages.len() / 2;
            let (mut left, mut failures) = isolated(steps, &messages[..middle], offset);
            let (right, right_failures) = isolated(steps, &messages[middle..], offset + middle);
            for (into, more) in left.iter_mut().zip(right) {
                into.extend(more);
            }
            failures.extend(right_failures);
            (left, failures)
        }
    }
}

fn write_frame(writer: &mut impl Write, bytes: &[u8]) -> std::io::Result<()> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| std::io::Error::other("a message is larger than 4 GiB"))?;
    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(bytes)
}

/// The next frame, or None when the engine closed the connection between frames.
fn read_frame(reader: &mut impl Read) -> std::io::Result<Option<Vec<u8>>> {
    let mut length = [0u8; 4];
    match reader.read_exact(&mut length) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let mut bytes = vec![0u8; u32::from_le_bytes(length) as usize];
    reader.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(values: &[&str]) -> Raw {
        values.iter().map(|v| v.as_bytes().to_vec()).collect()
    }

    fn step(block: StreamBlock, uuid: &str, send: bool) -> Step {
        Step {
            block,
            context: BlockContext {
                block_uuid: uuid.into(),
                block_type: "transformer".into(),
                pipeline_uuid: None,
                execution_partition: None,
                variables: Vars(serde_json::Map::from_iter([("rate".into(), json!(2.0))])),
                scratch_dir: std::env::temp_dir(),
            },
            send,
        }
    }

    fn double(messages: Vec<Value>, vars: Vars) -> Result<Vec<Value>> {
        let rate: f64 = vars.get("rate")?;
        messages
            .into_iter()
            .map(|m| {
                let n = m["n"].as_f64().ok_or_else(|| anyhow!("n is missing"))?;
                Ok(json!({"n": n * rate}))
            })
            .collect()
    }

    #[test]
    fn values_text_frames_and_raw_bytes_cross_as_they_are() {
        let values = Vec::<Value>::from_messages(&raw(&[r#"{"n":1}"#, "plain text"])).unwrap();
        assert_eq!(values, vec![json!({"n": 1}), json!("plain text")]);
        let frame = DataFrame::from_messages(&raw(&[r#"{"a":1,"b":"x"}"#, r#"{"a":2}"#])).unwrap();
        assert_eq!(frame.shape(), (2, 2));
        let rows = frame.into_messages().unwrap();
        assert_eq!(rows, raw(&[r#"{"a":1,"b":"x"}"#, r#"{"a":2,"b":null}"#]));
        assert!(DataFrame::from_messages(&raw(&["[1]"])).is_err());
        let bytes = vec![vec![0xff, 0x00]];
        assert_eq!(Vec::<Vec<u8>>::from_messages(&bytes).unwrap(), bytes);
        assert!(Vec::<Value>::from_messages(&bytes).is_err());
        assert_eq!(json!([1, 2]).into_messages().unwrap(), raw(&["1", "2"]));
        assert!(Value::Null.into_messages().unwrap().is_empty());
    }

    #[test]
    fn a_chain_sends_the_blocks_the_engine_wants() {
        let keep_big = |messages: Vec<Value>| -> Vec<Value> {
            messages
                .into_iter()
                .filter(|m| m["n"].as_f64() > Some(2.0))
                .collect()
        };
        let mut steps = vec![
            step(stream_block(double), "double", true),
            step(stream_block(keep_big), "keep_big", true),
        ];
        let (reply, outputs) = run_batch(&mut steps, 7, raw(&[r#"{"n":1}"#, r#"{"n":3}"#]), false);
        assert!(reply.ok);
        assert_eq!(outputs[0], raw(&[r#"{"n":2.0}"#, r#"{"n":6.0}"#]));
        assert_eq!(outputs[1], raw(&[r#"{"n":6.0}"#]));
        assert_eq!(reply.outputs[1].count, 1);
    }

    #[test]
    fn failing_messages_are_isolated_and_a_failing_batch_names_its_block() {
        let mut steps = vec![step(stream_block(double), "double", true)];
        let batch = raw(&[r#"{"n":1}"#, r#"{"x":0}"#, r#"{"n":3}"#]);
        let (reply, outputs) = run_batch(&mut steps, 1, batch.clone(), true);
        assert!(reply.ok);
        assert_eq!(outputs[0], raw(&[r#"{"n":2.0}"#, r#"{"n":6.0}"#]));
        assert_eq!(reply.failures.len(), 1);
        assert_eq!(reply.failures[0].row, Some(1));

        let (reply, _) = run_batch(&mut steps, 2, batch, false);
        let error = reply.error.unwrap();
        assert_eq!(error.block_uuid, "double");
        assert!(error.message.contains("n is missing"), "{}", error.message);

        // Every message failing is the code's problem: the batch fails.
        let (reply, _) = run_batch(&mut steps, 3, raw(&[r#"{"x":0}"#, r#"{"y":0}"#]), true);
        assert!(!reply.ok);
    }

    #[test]
    fn a_panic_is_the_blocks_error_and_the_program_goes_on() {
        install_panic_hook();
        let panics = |_: Vec<Value>| -> Vec<Value> { panic!("bad model") };
        let mut steps = vec![step(stream_block(panics), "panics", true)];
        let (reply, _) = run_batch(&mut steps, 1, raw(&["{}"]), false);
        assert!(reply.error.unwrap().message.contains("bad model"));
        let mut steps = vec![step(stream_block(double), "double", true)];
        assert!(run_batch(&mut steps, 2, raw(&[r#"{"n":1}"#]), false).0.ok);
    }

    #[test]
    fn frames_round_trip_and_end_cleanly() {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, b"hello").unwrap();
        write_frame(&mut buffer, b"").unwrap();
        let mut reader = std::io::Cursor::new(buffer);
        assert_eq!(read_frame(&mut reader).unwrap(), Some(b"hello".to_vec()));
        assert_eq!(read_frame(&mut reader).unwrap(), Some(Vec::new()));
        assert_eq!(read_frame(&mut reader).unwrap(), None);
    }
}
