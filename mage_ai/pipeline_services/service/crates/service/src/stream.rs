//! Runs a stream: the tokio driver around `mage_stream_core::RunnerMachine`.
//!
//! The driver reads from the source, sends batches to the transform session (one lane, so a
//! partition's batches are transformed in order), writes them to every sink through one
//! writer task per sink (so a sink receives batches in order, and a failed write holds its
//! queue until the retry the machine schedules), and acknowledges positions when the machine
//! says so. Every outcome goes back to the machine as an event; the driver only performs
//! effects.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mage_stream_core::{
    BatchId, BatchMeta, Effect, Event, FailureKind, Operation, RunnerConfig, RunnerMachine, SinkId,
    StopReason,
};
use mage_stream_ports::{
    AckKind, CommitOutcome, ConnectorError, EnvelopeBuilder, Header, PartitionId, RecordBatch,
    RowError, Sink, Source, SourceEvent, TransformError, TransformOutput, TransformSession,
    read_envelope,
};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// What a stream reports through `/v1/streams` and the metrics.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StreamStatus {
    pub state: StreamState,
    pub batches: u64,
    pub messages_in: u64,
    pub messages_out: u64,
    pub retries: u64,
    pub dead_lettered_batches: u64,
    /// Messages that failed on their own and went to the dead-letter sink.
    pub dead_lettered_messages: u64,
    /// Messages behind their partition's watermark, and messages without an event time.
    pub late: u64,
    pub untimed: u64,
    /// Per partition this instance reads: its watermark, when the stream has event time.
    pub watermarks: BTreeMap<u32, String>,
    pub alerts: u64,
    pub last_error: Option<String>,
    /// Per partition: the last acknowledged position, as text.
    pub acknowledged: BTreeMap<u32, String>,
    /// Per partition this instance reads: messages behind the end, where the source knows.
    pub lag: BTreeMap<u32, i64>,
    /// Partitions this instance reads now.
    pub assigned: usize,
    /// The last minute, one value per second, oldest first: messages read, messages the
    /// blocks made, and the total lag.
    pub rate_in: VecDeque<u64>,
    pub rate_out: VecDeque<u64>,
    pub lag_history: VecDeque<i64>,
    /// Each block's numbers, in the order the blocks first ran.
    pub blocks: Vec<BlockStatus>,
    /// The last messages at each tap point (`source`, and each block whose output the stream
    /// sees), when MAGE_SERVICE_TAP=on: development runs only, since they hold message data.
    #[serde(skip)]
    pub taps: BTreeMap<String, VecDeque<TapMessage>>,
    #[serde(skip)]
    pub tap_enabled: bool,
    /// The last error written to the log, and when: a repeated error is written again only
    /// every [`ERROR_LOG_EVERY`].
    #[serde(skip)]
    pub error_logged: Option<(String, std::time::Instant)>,
}

/// How often an error that keeps coming back is written to the log again.
const ERROR_LOG_EVERY: Duration = Duration::from_secs(30);

/// A message seen at a tap point.
#[derive(Debug, Clone, Serialize)]
pub struct TapMessage {
    pub partition: u32,
    pub position: Option<i64>,
    pub value: serde_json::Value,
}

/// Messages kept per tap point.
pub const TAP_SIZE: usize = 50;

/// Keeps the last messages of a batch at a tap point; only those are decoded.
fn tap(status: &SharedStatus, point: &str, batch: &RecordBatch) {
    let Ok(messages) = read_envelope(batch) else {
        return;
    };
    let start = messages.len().saturating_sub(TAP_SIZE);
    if let Ok(mut s) = status.lock() {
        let kept = s.taps.entry(point.to_string()).or_default();
        for message in &messages[start..] {
            let value = serde_json::from_slice(&message.value).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&message.value).into_owned())
            });
            if kept.len() == TAP_SIZE {
                kept.pop_front();
            }
            kept.push_back(TapMessage {
                partition: message.partition,
                position: message.position,
                value,
            });
        }
    }
}

/// What one block did since the stream started: messages in and out, batches, batch errors,
/// and the median and 95th percentile of its time on a batch over the last batches.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BlockStatus {
    pub block: String,
    pub messages_in: u64,
    pub messages_out: u64,
    pub batches: u64,
    pub errors: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    #[serde(skip)]
    recent_micros: VecDeque<u64>,
}

/// Batches each block's percentiles cover.
const BLOCK_WINDOW: usize = 200;

impl BlockStatus {
    fn record(&mut self, stats: &mage_stream_ports::BlockStats) {
        self.messages_in += stats.messages_in;
        self.messages_out += stats.messages_out;
        self.batches += 1;
        if self.recent_micros.len() == BLOCK_WINDOW {
            self.recent_micros.pop_front();
        }
        self.recent_micros.push_back(stats.micros);
        let mut sorted: Vec<u64> = self.recent_micros.iter().copied().collect();
        sorted.sort_unstable();
        let at = |q: f64| {
            let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
            sorted[index.min(sorted.len() - 1)] as f64 / 1000.0
        };
        self.p50_ms = at(0.5);
        self.p95_ms = at(0.95);
    }
}

fn block_status<'a>(status: &'a mut StreamStatus, block: &str) -> &'a mut BlockStatus {
    let index = match status.blocks.iter().position(|b| b.block == block) {
        Some(index) => index,
        None => {
            status.blocks.push(BlockStatus {
                block: block.to_string(),
                ..BlockStatus::default()
            });
            status.blocks.len() - 1
        }
    };
    &mut status.blocks[index]
}

/// Seconds of history a stream's status keeps.
pub const HISTORY_SECONDS: usize = 60;

fn push_bounded<T>(series: &mut VecDeque<T>, value: T) {
    if series.len() == HISTORY_SECONDS {
        series.pop_front();
    }
    series.push_back(value);
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamState {
    #[default]
    Starting,
    Running,
    /// Paused from the API or the console: nothing new is read, work in flight finishes.
    Paused,
    Draining,
    /// Stopped by an error that needs a person: a poison batch or a rejecting sink.
    Failed,
    Finished,
}

pub type SharedStatus = Arc<Mutex<StreamStatus>>;

pub struct StreamParts {
    pub name: String,
    pub source: Box<dyn Source>,
    /// Each sink with the block whose output it writes; None writes the source's messages.
    pub sinks: Vec<(Arc<dyn Sink>, Option<String>)>,
    pub dead_letter: Option<Arc<dyn Sink>>,
    /// None sends the source's messages to the sinks unchanged.
    pub transform: Option<Box<dyn TransformSession>>,
    pub config: RunnerConfig,
    pub shutdown_deadline: Duration,
    /// Paused from the API or the console: the source stops fetching while batches in flight
    /// finish and commit. None for a stream nobody pauses.
    pub pause: Option<tokio::sync::watch::Receiver<bool>>,
    /// Keep the last messages at each tap point (MAGE_SERVICE_TAP=on, development runs).
    pub tap: bool,
    /// Event time and late messages (manifest `time`).
    pub time: Option<crate::stream_time::Lateness>,
}

/// Work for the transform lane, in order.
enum LaneJob {
    Transform(BatchId, RecordBatch),
    /// Save the state as of these positions before the source commits them.
    Committed(
        Vec<mage_stream_core::Commit>,
        tokio::sync::oneshot::Sender<Result<(), String>>,
    ),
    Revoked(PartitionId),
}

enum Internal {
    Event(Event),
    /// A transform finished: each block's output, or the error.
    Transformed(BatchId, Result<TransformOutput, TransformError>),
    RetryNow(Operation),
}

struct WriteJob {
    id: BatchId,
    batch: RecordBatch,
    meta: BatchMeta,
    /// The batch's rejected messages for the dead-letter sink, confirmed as
    /// [`SinkId::REJECTED_ROWS`], not the whole batch.
    rows: bool,
}

/// Batches a writer has handed to its sink and awaits the confirmation of.
const WRITES_IN_FLIGHT: usize = 4;

/// What the stream tells a writer about its batches.
#[derive(Debug, Clone, Copy)]
enum WriterControl {
    /// Write the failed batch again.
    Retry(BatchId),
    /// The batch was dropped (its partition went to another owner): not written again.
    Cancel(BatchId),
}

/// A sink's writer: hands batches to the sink in order, up to [`WRITES_IN_FLIGHT`] awaiting
/// their confirmation (a sink that does not split the two confirms each at once), and
/// reports receipts in order. After a failure it hands over nothing new until the batches in
/// flight are confirmed, then waits for each failed batch's retry, in order, before going on;
/// a batch the stream dropped is skipped. It waited for the retry of a dropped batch, which
/// never comes, and every later batch waited behind it (I-202).
fn spawn_writer(
    sink: Arc<dyn Sink>,
    sink_id: SinkId,
    dead_letter: bool,
    events: mpsc::UnboundedSender<Internal>,
) -> (
    mpsc::UnboundedSender<WriteJob>,
    mpsc::UnboundedSender<WriterControl>,
) {
    let (jobs_tx, mut jobs) = mpsc::unbounded_channel::<WriteJob>();
    let (control_tx, mut control) = mpsc::unbounded_channel::<WriterControl>();
    tokio::spawn(async move {
        let report = |job: &WriteJob, result: &Result<(), ConnectorError>| {
            let sink = if job.rows {
                SinkId::REJECTED_ROWS
            } else {
                sink_id
            };
            let event = match (result, dead_letter && !job.rows) {
                (Ok(()), false) => Event::SinkReceipt { id: job.id, sink },
                (Ok(()), true) => Event::DeadLetterReceipt(job.id),
                (Err(error), false) => Event::SinkFailed {
                    id: job.id,
                    sink,
                    retryable: error.retryable,
                    error: error.message.clone(),
                },
                (Err(error), true) => Event::DeadLetterFailed {
                    id: job.id,
                    error: error.message.clone(),
                },
            };
            events.send(Internal::Event(event)).is_ok()
        };
        let mut in_flight: std::collections::VecDeque<(
            WriteJob,
            tokio::task::JoinHandle<Result<(), ConnectorError>>,
        )> = Default::default();
        let mut failed: Vec<WriteJob> = Vec::new();
        let mut scheduled: std::collections::HashSet<BatchId> = Default::default();
        let mut cancelled: std::collections::HashSet<BatchId> = Default::default();
        let note =
            |message: WriterControl,
             scheduled: &mut std::collections::HashSet<BatchId>,
             cancelled: &mut std::collections::HashSet<BatchId>| match message {
                WriterControl::Retry(id) => {
                    scheduled.insert(id);
                }
                WriterControl::Cancel(id) => {
                    cancelled.insert(id);
                }
            };
        let mut open = true;
        loop {
            if failed.is_empty() && open && in_flight.len() < WRITES_IN_FLIGHT {
                let next = if in_flight.is_empty() {
                    jobs.recv().await
                } else {
                    tokio::select! {
                        biased;
                        done = &mut in_flight.front_mut().expect("in flight").1 => {
                            let (job, _) = in_flight.pop_front().expect("in flight");
                            let result = done.unwrap_or_else(|e| {
                                Err(ConnectorError::retryable(format!("the sink's write stopped: {e}")))
                            });
                            if !report(&job, &result) {
                                return;
                            }
                            if result.is_err() {
                                failed.push(job);
                            }
                            continue;
                        }
                        job = jobs.recv() => job,
                    }
                };
                let Some(job) = next else {
                    open = false;
                    continue;
                };
                while let Ok(message) = control.try_recv() {
                    note(message, &mut scheduled, &mut cancelled);
                }
                // A dropped batch is not written: its next owner reads it again.
                if cancelled.remove(&job.id) {
                    continue;
                }
                match sink.queue(&job.batch, &job.meta).await {
                    Ok(confirmation) => {
                        let handle = tokio::spawn(confirmation);
                        in_flight.push_back((job, handle));
                    }
                    Err(error) => {
                        if !report(&job, &Err(error)) {
                            return;
                        }
                        failed.push(job);
                    }
                }
                continue;
            }
            if let Some((job, handle)) = in_flight.pop_front() {
                let result = handle.await.unwrap_or_else(|e| {
                    Err(ConnectorError::retryable(format!(
                        "the sink's write stopped: {e}"
                    )))
                });
                if !report(&job, &result) {
                    return;
                }
                if result.is_err() {
                    failed.push(job);
                }
                continue;
            }
            if failed.is_empty() {
                // Nothing in flight, nothing failed, and no more jobs.
                return;
            }
            // Each failed batch again, in order, once the machine schedules its retry; a
            // retry writes the batch whole, as before.
            'jobs: for job in std::mem::take(&mut failed) {
                loop {
                    // A later batch's retry can come first: it is kept for its turn.
                    while !scheduled.remove(&job.id) {
                        if cancelled.remove(&job.id) {
                            continue 'jobs;
                        }
                        match control.recv().await {
                            Some(message) => note(message, &mut scheduled, &mut cancelled),
                            None => return,
                        }
                    }
                    let result = sink.write(&job.batch, &job.meta).await;
                    if !report(&job, &result) {
                        return;
                    }
                    if result.is_ok() {
                        break;
                    }
                }
            }
        }
    });
    (jobs_tx, control_tx)
}

/// The dead-letter copy of a batch's rejected messages, each with its own error.
fn rejected_rows(
    batch: &RecordBatch,
    stream: &str,
    errors: &[RowError],
) -> Result<RecordBatch, ConnectorError> {
    let messages = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
    let mut builder = EnvelopeBuilder::new();
    for error in errors {
        let Some(message) = messages.get(error.row) else {
            continue;
        };
        let mut message = message.clone();
        message.headers.push(Header {
            key: "mage-stream".into(),
            value: stream.as_bytes().to_vec(),
        });
        message.headers.push(Header {
            key: "mage-error".into(),
            value: error.error.as_bytes().to_vec(),
        });
        builder.push(&message);
    }
    builder
        .finish()
        .map_err(|e| ConnectorError::fatal(e.to_string()))
}

/// The dead-letter copy of a batch: its messages with the error in headers.
fn dead_letter_batch(
    batch: &RecordBatch,
    stream: &str,
    error: &str,
) -> Result<RecordBatch, ConnectorError> {
    let mut messages = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
    let mut builder = EnvelopeBuilder::new();
    for message in &mut messages {
        message.headers.push(Header {
            key: "mage-stream".into(),
            value: stream.as_bytes().to_vec(),
        });
        message.headers.push(Header {
            key: "mage-error".into(),
            value: error.as_bytes().to_vec(),
        });
        builder.push(message);
    }
    builder
        .finish()
        .map_err(|e| ConnectorError::fatal(e.to_string()))
}

/// How long an instance may read no partition before it says so.
const IDLE_WARNING_AFTER: Duration = Duration::from_secs(60);

/// Runs the stream until it finishes (a finite source, a shutdown) or stops on an error.
pub async fn run(mut parts: StreamParts, status: SharedStatus, shutdown: CancellationToken) {
    let tapping = parts.tap;
    count(&status, |s| s.tap_enabled = tapping);
    let (tx, mut rx) = mpsc::unbounded_channel::<Internal>();
    let mut machine = RunnerMachine::new(parts.config.clone());

    // One writer per sink, plus the dead-letter sink.
    let mut writers = Vec::new();
    let sink_after: Vec<Option<String>> = parts.sinks.iter().map(|(_, a)| a.clone()).collect();
    let sink_fed: std::collections::BTreeSet<String> =
        sink_after.iter().flatten().cloned().collect();
    for (index, (sink, _)) in parts.sinks.iter().enumerate() {
        writers.push(spawn_writer(
            sink.clone(),
            SinkId(index as u16),
            false,
            tx.clone(),
        ));
    }
    let dead_letter_writer = parts
        .dead_letter
        .clone()
        .map(|sink| spawn_writer(sink, SinkId::DEAD_LETTER, true, tx.clone()));

    // The transform lane.
    let (lane_tx, mut lane_rx) = mpsc::unbounded_channel::<LaneJob>();
    let mut transform = parts.transform.take();
    let lane_events = tx.clone();
    let lane = tokio::spawn(async move {
        while let Some(job) = lane_rx.recv().await {
            let (id, batch) = match job {
                LaneJob::Transform(id, batch) => (id, batch),
                LaneJob::Committed(commits, reply) => {
                    let saved = match transform.as_mut() {
                        Some(session) => session.committed(&commits).await,
                        None => Ok(()),
                    };
                    let _ = reply.send(saved);
                    continue;
                }
                LaneJob::Revoked(partition) => {
                    if let Some(session) = transform.as_mut() {
                        session.revoked(partition);
                    }
                    continue;
                }
            };
            let result = match transform.as_mut() {
                Some(session) => session.transform(batch).await,
                None => Ok(TransformOutput {
                    outputs: Default::default(),
                    row_errors: Vec::new(),
                    blocks: Vec::new(),
                }),
            };
            if lane_events.send(Internal::Transformed(id, result)).is_err() {
                break;
            }
        }
        if let Some(session) = transform.as_mut() {
            session.close().await;
        }
    });

    let mut inputs: HashMap<BatchId, (RecordBatch, BatchMeta)> = HashMap::new();
    // With event time: a batch's on-time messages, when some were late, with the row in
    // the batch of each, and the late ones for the dead-letter sink.
    let mut on_time: HashMap<BatchId, (RecordBatch, Vec<usize>, Vec<RowError>)> = HashMap::new();
    let mut time = parts.time.take();
    let mut outputs: HashMap<BatchId, std::collections::BTreeMap<String, RecordBatch>> =
        HashMap::new();
    // Messages of a batch that failed on their own, by their row in the batch.
    let mut rejected: HashMap<BatchId, Vec<RowError>> = HashMap::new();
    let mut reading = true;
    let mut assigned: std::collections::BTreeSet<PartitionId> = Default::default();
    let mut idle_warned = false;
    let (mut last_in, mut last_out) = (0u64, 0u64);
    let started = std::time::Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pause = parts.pause.take();
    let mut user_paused = pause.as_ref().is_some_and(|p| *p.borrow());
    set_state(
        &status,
        if user_paused {
            StreamState::Paused
        } else {
            StreamState::Running
        },
    );

    loop {
        let mut events: Vec<Event> = Vec::new();
        tokio::select! {
            biased;
            _ = shutdown.cancelled(), if reading => {
                events.push(Event::Shutdown);
                let deadline = parts.shutdown_deadline;
                let tx = tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(deadline).await;
                    let _ = tx.send(Internal::Event(Event::DeadlinePassed));
                });
            }
            Some(message) = rx.recv() => match message {
                Internal::Event(event) => events.push(event),
                Internal::Transformed(id, Ok(output)) => {
                    if tapping {
                        for (block, batch) in &output.outputs {
                            tap(&status, block, batch);
                        }
                    }
                    // The messages of the blocks sinks follow: a tapped run returns every
                    // block's output, and counts the same as one that is not.
                    let rows: usize = output
                        .outputs
                        .iter()
                        .filter(|(block, _)| sink_fed.contains(block.as_str()))
                        .map(|(_, batch)| batch.num_rows())
                        .sum();
                    count(&status, |s| {
                        s.messages_out += rows as u64;
                        for stats in &output.blocks {
                            block_status(s, &stats.block).record(stats);
                        }
                    });
                    outputs.insert(id, output.outputs);
                    let mut output_errors = output.row_errors;
                    if let Some((_, kept, late)) = on_time.get(&id) {
                        // The blocks saw the on-time messages: their rows, back to the batch's.
                        for error in &mut output_errors {
                            error.row = kept.get(error.row).copied().unwrap_or(error.row);
                        }
                        output_errors.extend(late.iter().cloned());
                        output_errors.sort_by_key(|e| e.row);
                    }
                    let output = TransformOutput { row_errors: output_errors, ..TransformOutput::default() };
                    if !output.row_errors.is_empty() {
                        let first = output.row_errors[0].error.clone();
                        events.push(Event::RowsRejected {
                            id,
                            count: output.row_errors.len(),
                            error: first,
                        });
                        rejected.insert(id, output.row_errors);
                    }
                    events.push(Event::TransformDone(id));
                }
                Internal::Transformed(id, Err(error)) => {
                    let kind = match error {
                        TransformError::User(_) | TransformError::Protocol(_) => FailureKind::User,
                        TransformError::Crash(_) => FailureKind::Crash,
                        TransformError::Timeout(_) => FailureKind::Timeout,
                    };
                    record_error(&status, &parts.name, &error.to_string());
                    // A block's error reads "<block>: <message>".
                    if let TransformError::User(message) = &error
                        && let Some((block, _)) = message.split_once(": ")
                    {
                        count(&status, |s| {
                            if s.blocks.iter().any(|b| b.block == block) {
                                block_status(s, block).errors += 1;
                            }
                        });
                    }
                    events.push(Event::TransformFailed { id, kind, error: error.to_string() });
                }
                Internal::RetryNow(operation) => {
                    count(&status, |s| s.retries += 1);
                    match operation {
                        Operation::Transform(id) => {
                            if let Some((batch, _)) = inputs.get(&id) {
                                let batch = on_time.get(&id).map_or(batch, |(kept, _, _)| kept);
                                let _ = lane_tx.send(LaneJob::Transform(id, batch.clone()));
                            }
                        }
                        Operation::Write(id, SinkId::REJECTED_ROWS) => {
                            if let Some((_, retry)) = &dead_letter_writer {
                                let _ = retry.send(WriterControl::Retry(id));
                            }
                        }
                        Operation::Write(id, sink) => {
                            if let Some((_, retry)) = writers.get(sink.0 as usize) {
                                let _ = retry.send(WriterControl::Retry(id));
                            }
                        }
                        Operation::DeadLetter(id) => {
                            if let Some((_, retry)) = &dead_letter_writer {
                                let _ = retry.send(WriterControl::Retry(id));
                            }
                        }
                    }
                }
            },
            // Before the source: a source that answers at once (Idle) must not starve the
            // pause switch or the ticker.
            changed = async {
                match pause.as_mut() {
                    Some(receiver) => receiver.changed().await.map(|_| *receiver.borrow()),
                    None => std::future::pending().await,
                }
            } => match changed {
                Ok(now) if now != user_paused && reading => {
                    user_paused = now;
                    for partition in &assigned {
                        if now {
                            parts.source.pause(*partition);
                        } else if !machine.is_paused(*partition) {
                            parts.source.resume(*partition);
                        }
                    }
                    set_state(&status, if now { StreamState::Paused } else { StreamState::Running });
                    crate::log::info(&format!(
                        "Stream {} {}.",
                        parts.name,
                        if now { "paused: nothing new is read" } else { "resumed" }
                    ));
                }
                Ok(_) => {}
                Err(_) => pause = None,
            },
            _ = ticker.tick() => {
                events.push(Event::Tick);
                let lag = parts.source.lag();
                count(&status, |s| {
                    let total_lag: i64 = lag.values().sum();
                    push_bounded(&mut s.rate_in, s.messages_in.saturating_sub(last_in));
                    push_bounded(&mut s.rate_out, s.messages_out.saturating_sub(last_out));
                    push_bounded(&mut s.lag_history, total_lag);
                    last_in = s.messages_in;
                    last_out = s.messages_out;
                    s.lag = lag;
                    s.assigned = assigned.len();
                });
                // More instances than partitions leave some idle: say so once (I-38).
                if assigned.is_empty()
                    && !idle_warned
                    && reading
                    && started.elapsed() >= IDLE_WARNING_AFTER
                    && parts.source.capabilities().ack_kind == AckKind::Offset
                {
                    idle_warned = true;
                    count(&status, |s| s.alerts += 1);
                    crate::log::warn(&format!(
                        "Stream {}: this instance has read no partition for {} seconds. When \
                         the stream runs more instances than its topic has partitions, the \
                         extra ones stay idle until a partition is free.",
                        parts.name,
                        IDLE_WARNING_AFTER.as_secs()
                    ));
                }
            }
            event = parts.source.next_event(), if reading => match event {
                Ok(SourceEvent::Batch { batch, meta }) => {
                    count(&status, |s| {
                        s.batches += 1;
                        s.messages_in += meta.records as u64;
                    });
                    if tapping {
                        tap(&status, "source", &batch);
                    }
                    if let Some(lateness) = time.as_mut() {
                        match lateness.split(&batch) {
                            Ok(split) => {
                                let watermarks = lateness.watermarks();
                                count(&status, |s| {
                                    s.late += split.late;
                                    s.untimed += split.untimed;
                                    for (partition, watermark) in watermarks {
                                        s.watermarks.insert(
                                            partition,
                                            chrono::DateTime::from_timestamp_millis(watermark)
                                                .map(|t| {
                                                    t.to_rfc3339_opts(
                                                        chrono::SecondsFormat::Millis,
                                                        true,
                                                    )
                                                })
                                                .unwrap_or_default(),
                                        );
                                    }
                                });
                                if let Some(kept) = split.on_time {
                                    on_time.insert(meta.id, (kept, split.kept, split.errors));
                                }
                            }
                            Err(error) => record_error(
                                &status,
                                &parts.name,
                                &format!("reading the event times: {error}"),
                            ),
                        }
                    }
                    inputs.insert(meta.id, (batch, meta.clone()));
                    events.push(Event::BatchRead(meta));
                }
                Ok(SourceEvent::Assigned { partition, epoch }) => {
                    assigned.insert(partition);
                    if user_paused {
                        parts.source.pause(partition);
                    }
                    events.push(Event::Assigned { partition, epoch });
                }
                Ok(SourceEvent::Revoking { partition, epoch, within }) => {
                    assigned.remove(&partition);
                    if let Some(lateness) = time.as_mut() {
                        lateness.forget(partition);
                    }
                    count(&status, |s| {
                        s.watermarks.remove(&partition.0);
                    });
                    events.push(Event::Revoking { partition, epoch });
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(within).await;
                        let _ = tx.send(Internal::Event(Event::RevokeDeadline { partition, epoch }));
                    });
                }
                Ok(SourceEvent::Idle) => {
                    events.push(Event::Tick);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(SourceEvent::End) => {
                    events.push(Event::Shutdown);
                }
                Err(error) => {
                    record_error(&status, &parts.name, &format!("the source failed: {error}"));
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            },
        }

        for event in events {
            // Effects in order; applying one can step the machine again (a confirmed
            // commit), and the effects of that step follow.
            let mut effects: VecDeque<Effect> = machine.step(event).into();
            while let Some(effect) = effects.pop_front() {
                match effect {
                    Effect::Transform(id) => {
                        if let Some((batch, _)) = inputs.get(&id) {
                            match on_time.get(&id) {
                                // Every message was late: nothing for the blocks.
                                Some((kept, _, _)) if kept.num_rows() == 0 => {
                                    let _ = tx.send(Internal::Transformed(
                                        id,
                                        Ok(TransformOutput::default()),
                                    ));
                                }
                                Some((kept, _, _)) => {
                                    let _ = lane_tx.send(LaneJob::Transform(id, kept.clone()));
                                }
                                None => {
                                    let _ = lane_tx.send(LaneJob::Transform(id, batch.clone()));
                                }
                            }
                        }
                    }
                    Effect::Write { id, sink } => {
                        let index = sink.0 as usize;
                        if let (Some((input, meta)), Some((jobs, _))) =
                            (inputs.get(&id), writers.get(index))
                        {
                            // The output of the block the sink follows; a block that made
                            // nothing of this batch writes an empty batch, which confirms it.
                            let batch = match sink_after.get(index).cloned().flatten() {
                                None => on_time.get(&id).map_or(input, |(kept, _, _)| kept).clone(),
                                Some(block) => outputs
                                    .get(&id)
                                    .and_then(|o| o.get(&block))
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        RecordBatch::new_empty(mage_stream_ports::envelope_schema())
                                    }),
                            };
                            let _ = jobs.send(WriteJob {
                                id,
                                batch,
                                meta: meta.clone(),
                                rows: false,
                            });
                        }
                    }
                    Effect::Retry { operation, after } => {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(after).await;
                            let _ = tx.send(Internal::RetryNow(operation));
                        });
                    }
                    Effect::DeadLetter { id, error } => {
                        count(&status, |s| s.dead_lettered_batches += 1);
                        if let (Some((batch, meta)), Some((jobs, _))) =
                            (inputs.get(&id), &dead_letter_writer)
                        {
                            match dead_letter_batch(batch, &parts.name, &error) {
                                Ok(copy) => {
                                    let _ = jobs.send(WriteJob {
                                        id,
                                        batch: copy,
                                        meta: meta.clone(),
                                        rows: false,
                                    });
                                }
                                Err(e) => record_error(&status, &parts.name, &e.message),
                            }
                        }
                    }
                    Effect::DeadLetterRows(id) => {
                        if let (Some((batch, meta)), Some(errors), Some((jobs, _))) =
                            (inputs.get(&id), rejected.get(&id), &dead_letter_writer)
                        {
                            count(&status, |s| s.dead_lettered_messages += errors.len() as u64);
                            match rejected_rows(batch, &parts.name, errors) {
                                Ok(copy) => {
                                    let _ = jobs.send(WriteJob {
                                        id,
                                        batch: copy,
                                        meta: meta.clone(),
                                        rows: true,
                                    });
                                }
                                Err(e) => record_error(&status, &parts.name, &e.message),
                            }
                        }
                    }
                    Effect::Commit(commits) => {
                        // State as of these positions is saved first: a restart then finds
                        // both at the same point, never positions past their state.
                        let (reply, saved) = tokio::sync::oneshot::channel();
                        let _ = lane_tx.send(LaneJob::Committed(commits.clone(), reply));
                        if let Ok(Err(error)) = saved.await {
                            record_error(
                                &status,
                                &parts.name,
                                &format!(
                                    "saving the stream's state failed; the positions are not acknowledged: {error}"
                                ),
                            );
                            continue;
                        }
                        match parts.source.commit(&commits).await {
                            CommitOutcome::Committed => {
                                for commit in commits {
                                    let id_partition = commit.partition.0;
                                    let position = commit.position.to_string();
                                    // Forget batches through the committed position.
                                    inputs.retain(|id, (_, meta)| {
                                        !(id.partition == commit.partition
                                            && id.epoch == commit.epoch
                                            && meta.last <= commit.position)
                                    });
                                    outputs.retain(|id, _| inputs.contains_key(id));
                                    on_time.retain(|id, _| inputs.contains_key(id));
                                    rejected.retain(|id, _| inputs.contains_key(id));
                                    count(&status, |s| {
                                        s.acknowledged.insert(id_partition, position);
                                    });
                                    // A partition paused for unconfirmed batches resumes.
                                    effects.extend(machine.step(Event::Committed(commit)));
                                }
                            }
                            CommitOutcome::Stale => {}
                            CommitOutcome::Failed(error) => {
                                record_error(
                                    &status,
                                    &parts.name,
                                    &format!("acknowledging failed: {error}"),
                                );
                            }
                        }
                    }
                    Effect::Revoked { partition, epoch } => {
                        let _ = lane_tx.send(LaneJob::Revoked(partition));
                        parts.source.revoked(partition, epoch);
                    }
                    Effect::Pause(partition) => parts.source.pause(partition),
                    // A paused stream stays paused when backpressure lifts.
                    Effect::Resume(_) if user_paused => {}
                    Effect::Resume(partition) => parts.source.resume(partition),
                    Effect::Cancel(ids) => {
                        for id in ids {
                            inputs.remove(&id);
                            outputs.remove(&id);
                            rejected.remove(&id);
                            for (_, control) in writers.iter().chain(&dead_letter_writer) {
                                let _ = control.send(WriterControl::Cancel(id));
                            }
                        }
                    }
                    Effect::StopReading => {
                        reading = false;
                        set_state(&status, StreamState::Draining);
                    }
                    Effect::Alert(message) => {
                        count(&status, |s| s.alerts += 1);
                        crate::log::warn(&format!("Stream {}: {message}", parts.name));
                        // A sink retrying without end says why in the stream's status.
                        keep_error(&status, &message);
                    }
                    Effect::Stop(reason) => {
                        let message = match reason {
                            StopReason::Poison { id, error } => format!(
                                "stopped: {id} failed in every attempt and the stream has no \
                                 dead-letter sink (or stops on poison messages): {error}"
                            ),
                            StopReason::SinkRejected { id, sink, error } => {
                                format!("stopped: sink {} rejected {id}: {error}", sink.0)
                            }
                        };
                        crate::log::error(&format!("Stream {}: {message}", parts.name));
                        keep_error(&status, &message);
                        set_state(&status, StreamState::Failed);
                        parts.source.failed(&message);
                    }
                    Effect::Finished { unfinished } => {
                        reading = false;
                        if !unfinished.is_empty() {
                            crate::log::warn(&format!(
                                "Stream {}: {} batch(es) were in flight at the shutdown \
                                 deadline; they are read again at the next start.",
                                parts.name,
                                unfinished.len()
                            ));
                        }
                        if status.lock().map(|s| s.state).unwrap_or_default() != StreamState::Failed
                        {
                            set_state(&status, StreamState::Finished);
                        }
                    }
                }
            }
        }
        let state = status.lock().map(|s| s.state).unwrap_or_default();
        if machine.is_finished() || state == StreamState::Failed {
            break;
        }
    }
    drop(lane_tx);
    let _ = lane.await;
    for (sink, _) in &parts.sinks {
        sink.flush().await;
    }
    parts.source.close().await;
}

fn set_state(status: &SharedStatus, state: StreamState) {
    if let Ok(mut s) = status.lock() {
        s.state = state;
    }
}

fn count(status: &SharedStatus, update: impl FnOnce(&mut StreamStatus)) {
    if let Ok(mut s) = status.lock() {
        update(&mut s);
    }
}

/// Keeps the error in the stream's status and writes it to the log: once when it changes,
/// then every [`ERROR_LOG_EVERY`] while it repeats. Only the status had it, so a source
/// reconnecting for minutes left nothing in the log (I-201).
fn record_error(status: &SharedStatus, name: &str, message: &str) {
    let write = match status.lock() {
        Ok(mut s) => {
            s.last_error = Some(message.to_string());
            let due = match &s.error_logged {
                Some((last, at)) => last != message || at.elapsed() >= ERROR_LOG_EVERY,
                None => true,
            };
            if due {
                s.error_logged = Some((message.to_string(), std::time::Instant::now()));
            }
            due
        }
        Err(_) => false,
    };
    if write {
        crate::log::warn(&format!("Stream {name}: {message}"));
    }
}

/// Keeps the error in the stream's status only, for one the caller already logged.
fn keep_error(status: &SharedStatus, message: &str) {
    if let Ok(mut s) = status.lock() {
        s.last_error = Some(message.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mage_connector_local::{MemorySink, MemorySource};
    use mage_stream_core::{PoisonPolicy, RetryPolicy, TrackerConfig};
    use mage_stream_ports::{BoxFuture, Envelope, TransformOutput};

    #[test]
    fn a_repeated_error_is_logged_once_per_interval_and_a_new_one_at_once() {
        let status: SharedStatus = Default::default();
        let logged_at = |status: &SharedStatus| {
            status
                .lock()
                .unwrap()
                .error_logged
                .as_ref()
                .map(|(m, at)| (m.clone(), *at))
        };

        record_error(&status, "orders", "the source failed: broker down");
        let (first, at) = logged_at(&status).unwrap();
        record_error(&status, "orders", "the source failed: broker down");
        assert_eq!(
            logged_at(&status).unwrap(),
            (first.clone(), at),
            "logged again too soon"
        );

        record_error(&status, "orders", "acknowledging failed: no coordinator");
        let (second, _) = logged_at(&status).unwrap();
        assert_eq!(second, "acknowledging failed: no coordinator");
        assert_eq!(
            status.lock().unwrap().last_error.as_deref(),
            Some(second.as_str())
        );

        keep_error(&status, "stopped: sink 0 rejected the batch");
        assert_eq!(
            logged_at(&status).unwrap().0,
            second,
            "keep_error does not log"
        );
    }

    fn messages(count: usize) -> Vec<Envelope> {
        (0..count)
            .map(|n| Envelope {
                value: format!("{{\"n\": {n}}}").into_bytes(),
                ..Envelope::default()
            })
            .collect()
    }

    fn config(sinks: u16, has_dead_letter: bool, poison: PoisonPolicy) -> RunnerConfig {
        let all: Vec<SinkId> = (0..sinks).map(SinkId).collect();
        RunnerConfig {
            tracker: TrackerConfig {
                required_sinks: all.clone(),
                max_in_flight_bytes: 1 << 20,
                max_in_flight_batches: 8,
            },
            sinks: all,
            transform_retry: RetryPolicy {
                retries: 1,
                delay: Duration::from_millis(5),
                max_delay: Duration::from_millis(20),
            },
            sink_retry: RetryPolicy {
                retries: 3,
                delay: Duration::from_millis(5),
                max_delay: Duration::from_millis(20),
            },
            poison,
            has_dead_letter,
            alert_after: 3,
        }
    }

    async fn drive(parts: StreamParts) -> StreamStatus {
        let status = SharedStatus::default();
        tokio::time::timeout(
            Duration::from_secs(20),
            run(parts, status.clone(), CancellationToken::new()),
        )
        .await
        .expect("the stream finishes");
        status.lock().unwrap().clone()
    }

    fn values(sink: &MemorySink) -> Vec<String> {
        sink.received()
            .iter()
            .map(|m| String::from_utf8(m.value.clone()).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn every_message_reaches_every_sink_in_order_and_is_acknowledged() {
        let source = MemorySource::new(vec![messages(25), messages(7)], 4, true);
        let handle = source.handle();
        let (a, b) = (MemorySink::new(), MemorySink::new());
        b.fail_next(3, true, "connection refused");

        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(a.clone()), None), (Arc::new(b.clone()), None)],
            dead_letter: None,
            transform: None,
            config: config(2, false, PoisonPolicy::Stop),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.state, StreamState::Finished, "{status:?}");
        assert_eq!(handle.committed(), vec![25, 7]);
        for sink in [&a, &b] {
            let received = sink.received();
            assert_eq!(received.len(), 32);
            // In order per partition.
            for partition in [0, 1] {
                let positions: Vec<i64> = received
                    .iter()
                    .filter(|m| m.partition == partition)
                    .map(|m| m.position.unwrap())
                    .collect();
                assert!(positions.windows(2).all(|w| w[0] < w[1]), "{positions:?}");
            }
        }
        assert_eq!(status.messages_in, 32);
        assert!(status.retries >= 3);
    }

    #[tokio::test]
    async fn late_messages_go_to_dead_letters_and_the_sinks_get_the_rest() {
        let times = [
            r#"{"at": 100}"#,
            r#"{"at": 101}"#,
            r#"{"at": 98}"#,
            r#"{"at": 102}"#,
            r#"{"x": 1}"#,
        ];
        let partition: Vec<Envelope> = times
            .iter()
            .map(|v| Envelope {
                value: v.as_bytes().to_vec(),
                ..Envelope::default()
            })
            .collect();
        let source = MemorySource::new(vec![partition], 2, true);
        let handle = source.handle();
        let (sink, dead) = (MemorySink::new(), MemorySink::new());
        let status = drive(StreamParts {
            name: "events".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(sink.clone()), None)],
            dead_letter: Some(Arc::new(dead.clone())),
            transform: None,
            config: config(1, true, PoisonPolicy::DeadLetter),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: Some(crate::stream_time::Lateness::new(
                mage_service_core::manifest::EventTime {
                    field: Some("at".into()),
                    format: mage_service_core::manifest::TimeFormat::EpochS,
                    lateness_ms: 1_000,
                    on_late: mage_service_core::manifest::LatePolicy::DeadLetter,
                },
            )),
        })
        .await;

        assert_eq!(status.state, StreamState::Finished, "{status:?}");
        assert_eq!(values(&sink), [times[0], times[1], times[3]]);
        let late = dead.received();
        let mut dead_values: Vec<String> = late
            .iter()
            .map(|m| String::from_utf8(m.value.clone()).unwrap())
            .collect();
        dead_values.sort();
        assert_eq!(dead_values, [times[2], times[4]]);
        let reasons: Vec<String> = late
            .iter()
            .flat_map(|m| m.headers.iter())
            .map(|h| String::from_utf8_lossy(&h.value).into_owned())
            .collect();
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("before partition 0's watermark")),
            "{reasons:?}"
        );
        assert!(
            reasons.iter().any(|r| r.contains("no event time in at")),
            "{reasons:?}"
        );
        assert_eq!((status.late, status.untimed), (1, 1));
        assert_eq!(status.watermarks[&0], "1970-01-01T00:01:41.000Z");
        assert_eq!(handle.committed(), vec![5]);
    }

    #[tokio::test]
    async fn a_paused_stream_reads_nothing_new_until_it_resumes() {
        let source = MemorySource::new(vec![messages(20)], 4, true);
        let handle = source.handle();
        let sink = MemorySink::new();
        let (switch, receiver) = tokio::sync::watch::channel(true);
        let status = SharedStatus::default();
        let running = tokio::spawn(run(
            StreamParts {
                name: "orders".into(),
                source: Box::new(source),
                sinks: vec![(Arc::new(sink.clone()), None)],
                dead_letter: None,
                transform: None,
                config: config(1, false, PoisonPolicy::Stop),
                shutdown_deadline: Duration::from_secs(5),
                pause: Some(receiver),
                tap: false,
                time: None,
            },
            status.clone(),
            CancellationToken::new(),
        ));

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(status.lock().unwrap().state, StreamState::Paused);
        assert!(sink.received().is_empty(), "a paused stream wrote messages");
        assert_eq!(handle.paused(), vec![0]);

        switch.send_replace(false);
        tokio::time::timeout(Duration::from_secs(20), running)
            .await
            .expect("the stream finishes")
            .unwrap();

        assert_eq!(sink.received().len(), 20);
        assert_eq!(handle.committed(), vec![20]);
        assert_eq!(status.lock().unwrap().state, StreamState::Finished);
    }

    struct Failing(usize);

    impl TransformSession for Failing {
        fn transform(
            &mut self,
            batch: RecordBatch,
        ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
            // Fails batches that hold message n == self.0.
            let poison = format!("{{\"n\": {}}}", self.0);
            let has = read_envelope(&batch)
                .unwrap()
                .iter()
                .any(|m| m.value == poison.as_bytes());
            Box::pin(async move {
                if has {
                    Err(TransformError::User("bad message".into()))
                } else {
                    Ok(TransformOutput {
                        outputs: [("t".to_string(), batch)].into(),
                        row_errors: vec![],
                        blocks: Vec::new(),
                    })
                }
            })
        }

        fn close(&mut self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    /// Two timed blocks: `parse` passes every message, `keep` drops the odd ones.
    struct Timed;

    impl TransformSession for Timed {
        fn transform(
            &mut self,
            batch: RecordBatch,
        ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
            let messages = read_envelope(&batch).unwrap();
            let mut kept = EnvelopeBuilder::new();
            for message in &messages {
                let value: serde_json::Value = serde_json::from_slice(&message.value).unwrap();
                if value["n"].as_u64().unwrap().is_multiple_of(2) {
                    kept.push(message);
                }
            }
            let kept = kept.finish().unwrap();
            let count = messages.len() as u64;
            Box::pin(async move {
                let kept_rows = kept.num_rows() as u64;
                Ok(TransformOutput {
                    outputs: [("keep".to_string(), kept)].into(),
                    row_errors: vec![],
                    blocks: vec![
                        mage_stream_ports::BlockStats {
                            block: "parse".into(),
                            messages_in: count,
                            messages_out: count,
                            micros: 1_000,
                        },
                        mage_stream_ports::BlockStats {
                            block: "keep".into(),
                            messages_in: count,
                            messages_out: kept_rows,
                            micros: 3_000,
                        },
                    ],
                })
            })
        }

        fn close(&mut self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn each_block_reports_its_messages_and_times() {
        let source = MemorySource::new(vec![messages(20)], 4, true);
        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(MemorySink::new()), Some("keep".into()))],
            dead_letter: None,
            transform: Some(Box::new(Timed)),
            config: config(1, false, PoisonPolicy::Stop),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        let blocks: Vec<(&str, u64, u64)> = status
            .blocks
            .iter()
            .map(|b| (b.block.as_str(), b.messages_in, b.messages_out))
            .collect();
        assert_eq!(blocks, vec![("parse", 20, 20), ("keep", 20, 10)]);
        assert!(status.blocks.iter().all(|b| b.batches >= 1));
        assert_eq!(
            (status.blocks[0].p50_ms, status.blocks[1].p95_ms),
            (1.0, 3.0)
        );
    }

    #[tokio::test]
    async fn the_tap_keeps_the_last_messages_at_the_source_and_each_output() {
        let source = MemorySource::new(vec![messages(80)], 4, true);
        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(MemorySink::new()), Some("keep".into()))],
            dead_letter: None,
            transform: Some(Box::new(Timed)),
            config: config(1, false, PoisonPolicy::Stop),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: true,
            time: None,
        })
        .await;

        assert!(status.tap_enabled);
        let source = &status.taps["source"];
        assert_eq!(source.len(), TAP_SIZE);
        assert_eq!(source.back().unwrap().value["n"], 79);
        let kept = &status.taps["keep"];
        assert!(kept.iter().all(|m| m.value["n"].as_u64().unwrap() % 2 == 0));
    }

    #[tokio::test]
    async fn without_the_tap_no_message_is_kept() {
        let source = MemorySource::new(vec![messages(10)], 4, true);
        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(MemorySink::new()), None)],
            dead_letter: None,
            transform: None,
            config: config(1, false, PoisonPolicy::Stop),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert!(status.taps.is_empty());
    }

    /// Rejects messages whose n is a multiple of self.0, on their own; passes the others.
    struct RejectsSome(usize);

    impl TransformSession for RejectsSome {
        fn transform(
            &mut self,
            batch: RecordBatch,
        ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
            let every = self.0;
            let messages = read_envelope(&batch).unwrap();
            let mut kept = EnvelopeBuilder::new();
            let mut row_errors = Vec::new();
            for (row, message) in messages.iter().enumerate() {
                let value: serde_json::Value = serde_json::from_slice(&message.value).unwrap();
                let n = value["n"].as_u64().unwrap() as usize;
                if n.is_multiple_of(every) {
                    row_errors.push(RowError {
                        row,
                        error: format!("n {n} is not allowed"),
                    });
                } else {
                    kept.push(message);
                }
            }
            let kept = kept.finish().unwrap();
            Box::pin(async move {
                Ok(TransformOutput {
                    outputs: [("t".to_string(), kept)].into(),
                    row_errors,
                    blocks: Vec::new(),
                })
            })
        }

        fn close(&mut self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn messages_that_fail_on_their_own_go_to_dead_letters_and_their_batch_flows() {
        let source = MemorySource::new(vec![messages(12)], 4, true);
        let handle = source.handle();
        let (sink, dead) = (MemorySink::new(), MemorySink::new());
        // The dead-letter sink fails twice: the batch waits for it, nothing is dropped.
        dead.fail_next(2, true, "dead letters unavailable");

        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(sink.clone()), Some("t".to_string()))],
            dead_letter: Some(Arc::new(dead.clone())),
            transform: Some(Box::new(RejectsSome(5))),
            config: config(1, true, PoisonPolicy::DeadLetter),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.state, StreamState::Finished, "{status:?}");
        assert_eq!(handle.committed(), vec![12]);
        assert_eq!(values(&dead), ["{\"n\": 0}", "{\"n\": 5}", "{\"n\": 10}"]);
        let error = dead.received()[1]
            .headers
            .iter()
            .find(|h| h.key == "mage-error")
            .map(|h| String::from_utf8_lossy(&h.value).into_owned());
        assert_eq!(error.as_deref(), Some("n 5 is not allowed"));
        assert_eq!(sink.received().len(), 9);
        assert_eq!(status.dead_lettered_messages, 3);
        assert_eq!(status.dead_lettered_batches, 0);
    }

    #[tokio::test]
    async fn a_writer_skips_a_failed_batch_the_stream_dropped() {
        use mage_stream_core::{AssignmentEpoch, BatchId, PartitionId, Position};

        let sink: Arc<dyn Sink> = Arc::new(FlakySink {
            failures: std::sync::atomic::AtomicUsize::new(1),
        });
        let (events_tx, mut events) = mpsc::unbounded_channel();
        let (jobs, control) = spawn_writer(sink, SinkId(0), false, events_tx);
        let job = |seq| {
            let id = BatchId {
                partition: PartitionId(0),
                epoch: AssignmentEpoch(1),
                seq,
            };
            WriteJob {
                id,
                batch: RecordBatch::new_empty(mage_stream_ports::envelope_schema()),
                meta: BatchMeta {
                    id,
                    first: Position::Offset(0),
                    last: Position::Offset(0),
                    records: 0,
                    bytes: 0,
                },
                rows: false,
            }
        };
        let (first, second) = (job(0), job(1));
        let (first_id, second_id) = (first.id, second.id);
        jobs.send(first).unwrap();
        async fn next(events: &mut mpsc::UnboundedReceiver<Internal>) -> Event {
            match tokio::time::timeout(Duration::from_secs(5), events.recv()).await {
                Ok(Some(Internal::Event(event))) => event,
                _ => panic!("the writer reported nothing within 5 seconds"),
            }
        }
        assert!(matches!(next(&mut events).await, Event::SinkFailed { id, .. } if id == first_id));
        jobs.send(second).unwrap();

        // The partition went to another owner: the failed batch is dropped, not retried.
        control.send(WriterControl::Cancel(first_id)).unwrap();
        assert!(
            matches!(next(&mut events).await, Event::SinkReceipt { id, .. } if id == second_id)
        );
    }

    /// Fails with a retryable error a number of times, then writes.
    struct FlakySink {
        failures: std::sync::atomic::AtomicUsize,
    }

    impl Sink for FlakySink {
        fn write<'a>(
            &'a self,
            _batch: &'a RecordBatch,
            _meta: &'a BatchMeta,
        ) -> BoxFuture<'a, Result<(), ConnectorError>> {
            Box::pin(async move {
                let left = self.failures.load(std::sync::atomic::Ordering::SeqCst);
                if left > 0 {
                    self.failures
                        .store(left - 1, std::sync::atomic::Ordering::SeqCst);
                    return Err(ConnectorError::retryable("the database is unreachable"));
                }
                Ok(())
            })
        }

        fn flush(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    /// Takes batches at once and confirms each later, as a Kafka producer does; the batch at
    /// `fail_at` (counting from 0) is refused once when confirmed.
    struct PipelinedSink {
        queued: std::sync::Mutex<Vec<String>>,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        most_in_flight: Arc<std::sync::atomic::AtomicUsize>,
        batches: std::sync::atomic::AtomicUsize,
        fail_at: Option<usize>,
    }

    impl PipelinedSink {
        fn new(fail_at: Option<usize>) -> PipelinedSink {
            PipelinedSink {
                queued: Default::default(),
                in_flight: Default::default(),
                most_in_flight: Default::default(),
                batches: Default::default(),
                fail_at,
            }
        }

        fn take(&self, batch: &RecordBatch) {
            let mut queued = self.queued.lock().unwrap();
            for message in read_envelope(batch).unwrap() {
                queued.push(String::from_utf8(message.value).unwrap());
            }
        }
    }

    impl Sink for PipelinedSink {
        fn write<'a>(
            &'a self,
            batch: &'a RecordBatch,
            _meta: &'a BatchMeta,
        ) -> BoxFuture<'a, Result<(), ConnectorError>> {
            Box::pin(async move {
                self.take(batch);
                Ok(())
            })
        }

        fn queue<'a>(
            &'a self,
            batch: &'a RecordBatch,
            _meta: &'a BatchMeta,
        ) -> BoxFuture<'a, Result<mage_stream_ports::Confirmation, ConnectorError>> {
            use std::sync::atomic::Ordering::SeqCst;
            Box::pin(async move {
                self.take(batch);
                let number = self.batches.fetch_add(1, SeqCst);
                let now = self.in_flight.fetch_add(1, SeqCst) + 1;
                self.most_in_flight.fetch_max(now, SeqCst);
                let in_flight = self.in_flight.clone();
                let refuse = self.fail_at == Some(number);
                Ok(Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    in_flight.fetch_sub(1, SeqCst);
                    if refuse {
                        return Err(ConnectorError::retryable("the broker refused the batch"));
                    }
                    Ok(())
                }) as mage_stream_ports::Confirmation)
            })
        }

        fn flush(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn a_sink_that_confirms_later_takes_the_next_batches_meanwhile() {
        let source = MemorySource::new(vec![messages(24)], 4, true);
        let handle = source.handle();
        let sink = Arc::new(PipelinedSink::new(None));

        let started = std::time::Instant::now();
        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(sink.clone(), None)],
            dead_letter: None,
            transform: None,
            config: config(1, false, PoisonPolicy::Stop),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.state, StreamState::Finished, "{status:?}");
        assert_eq!(handle.committed(), vec![24]);
        let expected: Vec<String> = (0..24).map(|n| format!("{{\"n\": {n}}}")).collect();
        assert_eq!(*sink.queued.lock().unwrap(), expected);
        // Six batches confirmed in 30 ms each, overlapping: well under 180 ms.
        assert!(
            sink.most_in_flight
                .load(std::sync::atomic::Ordering::SeqCst)
                > 1
        );
        assert!(
            started.elapsed() < Duration::from_millis(170),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_refused_confirmation_is_retried_and_nothing_is_lost() {
        let source = MemorySource::new(vec![messages(24)], 4, true);
        let handle = source.handle();
        let sink = Arc::new(PipelinedSink::new(Some(1)));

        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(sink.clone(), None)],
            dead_letter: None,
            transform: None,
            config: config(1, false, PoisonPolicy::Stop),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.state, StreamState::Finished, "{status:?}");
        assert_eq!(handle.committed(), vec![24]);
        assert!(status.retries >= 1, "{status:?}");
        let queued = sink.queued.lock().unwrap().clone();
        for n in 0..24 {
            assert!(
                queued.contains(&format!("{{\"n\": {n}}}")),
                "message {n} lost: {queued:?}"
            );
        }
        // The refused batch (messages 4 to 7) was written again, after the others in flight.
        assert_eq!(queued.len(), 28, "{queued:?}");
    }

    #[tokio::test]
    async fn a_sink_that_keeps_failing_says_why_in_the_status() {
        let source = MemorySource::new(vec![messages(3)], 4, true);
        let sink = Arc::new(FlakySink {
            failures: std::sync::atomic::AtomicUsize::new(4),
        });

        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(sink, None)],
            dead_letter: None,
            transform: None,
            config: config(1, false, PoisonPolicy::DeadLetter),
            shutdown_deadline: Duration::from_millis(200),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.alerts, 1, "{status:?}");
        let error = status.last_error.unwrap_or_default();
        assert!(error.contains("the database is unreachable"), "{error}");
    }

    #[tokio::test]
    async fn messages_that_fail_without_a_dead_letter_sink_stop_the_stream() {
        let source = MemorySource::new(vec![messages(12)], 4, true);
        let handle = source.handle();
        let sink = MemorySink::new();

        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(sink.clone()), Some("t".to_string()))],
            dead_letter: None,
            transform: Some(Box::new(RejectsSome(5))),
            config: config(1, false, PoisonPolicy::DeadLetter),
            shutdown_deadline: Duration::from_millis(200),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.state, StreamState::Failed, "{status:?}");
        assert!(status.last_error.unwrap().contains("not allowed"));
        assert!(handle.committed().iter().sum::<usize>() < 12);
    }

    #[tokio::test]
    async fn a_poison_batch_goes_to_dead_letters_with_its_error_and_the_rest_flows() {
        let source = MemorySource::new(vec![messages(12)], 3, true);
        let handle = source.handle();
        let (sink, dead) = (MemorySink::new(), MemorySink::new());

        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(sink.clone()), Some("t".to_string()))],
            dead_letter: Some(Arc::new(dead.clone())),
            transform: Some(Box::new(Failing(4))),
            config: config(1, true, PoisonPolicy::DeadLetter),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.state, StreamState::Finished, "{status:?}");
        assert_eq!(handle.committed(), vec![12]);
        assert_eq!(sink.received().len(), 9);
        assert_eq!(values(&dead), ["{\"n\": 3}", "{\"n\": 4}", "{\"n\": 5}"]);
        let headers = &dead.received()[0].headers;
        assert!(
            headers.iter().any(|h| h.key == "mage-error"
                && String::from_utf8_lossy(&h.value).contains("bad message"))
        );
        assert_eq!(status.dead_lettered_batches, 1);
    }

    #[tokio::test]
    async fn without_dead_letters_a_poison_batch_stops_the_stream_before_it() {
        let source = MemorySource::new(vec![messages(12)], 3, true);
        let handle = source.handle();
        let sink = MemorySink::new();

        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(sink.clone()), Some("t".to_string()))],
            dead_letter: None,
            transform: Some(Box::new(Failing(4))),
            config: config(1, false, PoisonPolicy::Stop),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.state, StreamState::Failed);
        assert!(status.last_error.unwrap().contains("bad message"));
        // Nothing at or after the poison batch is acknowledged.
        assert!(handle.committed()[0] <= 3, "{:?}", handle.committed());
    }

    #[tokio::test]
    async fn a_rejecting_sink_stops_the_stream_with_its_error() {
        let source = MemorySource::new(vec![messages(6)], 3, true);
        let sink = MemorySink::new();
        sink.fail_next(1, false, "column score is missing");

        let status = drive(StreamParts {
            name: "orders".into(),
            source: Box::new(source),
            sinks: vec![(Arc::new(sink.clone()), None)],
            dead_letter: None,
            transform: None,
            config: config(1, false, PoisonPolicy::Stop),
            shutdown_deadline: Duration::from_secs(5),
            pause: None,
            tap: false,
            time: None,
        })
        .await;

        assert_eq!(status.state, StreamState::Failed);
        assert!(
            status
                .last_error
                .unwrap()
                .contains("column score is missing")
        );
    }

    #[tokio::test]
    async fn after_a_crash_the_stream_reads_again_from_the_last_acknowledged_position() {
        let source = MemorySource::new(vec![messages(10)], 2, false);
        let handle = source.handle();
        let sink = MemorySink::new();
        // Two batches get through, then the sink is down: later batches stay in flight.
        sink.outage_after(2);
        let shutdown = CancellationToken::new();
        let status = SharedStatus::default();
        let running = tokio::spawn(run(
            StreamParts {
                name: "orders".into(),
                source: Box::new(source),
                sinks: vec![(Arc::new(sink.clone()), None)],
                dead_letter: None,
                transform: None,
                config: config(1, false, PoisonPolicy::Stop),
                shutdown_deadline: Duration::from_millis(50),
                pause: None,
                tap: false,
                time: None,
            },
            status.clone(),
            shutdown.clone(),
        ));
        // Let some batches through, then drop the stream as a crash would.
        tokio::time::sleep(Duration::from_millis(200)).await;
        running.abort();
        let acknowledged = handle.committed()[0];
        assert_eq!(acknowledged, 4, "two batches of two were acknowledged");

        let restarted = handle.restart();
        let again = MemorySink::new();
        let shutdown = CancellationToken::new();
        let stopper = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            stopper.cancel();
        });
        run(
            StreamParts {
                name: "orders".into(),
                source: Box::new(restarted),
                sinks: vec![(Arc::new(again.clone()), None)],
                dead_letter: None,
                transform: None,
                config: config(1, false, PoisonPolicy::Stop),
                shutdown_deadline: Duration::from_secs(5),
                pause: None,
                tap: false,
                time: None,
            },
            SharedStatus::default(),
            shutdown,
        )
        .await;

        // Nothing is lost: the second run starts at the first unacknowledged message, and
        // the two runs together hold every message.
        let first: Vec<i64> = again
            .received()
            .iter()
            .map(|m| m.position.unwrap())
            .collect();
        assert_eq!(first.first().copied(), Some(acknowledged as i64));
        let mut all: Vec<i64> = sink
            .received()
            .iter()
            .map(|m| m.position.unwrap())
            .collect();
        all.extend(first);
        all.sort();
        all.dedup();
        assert_eq!(all, (0..10).collect::<Vec<_>>());
    }
}
