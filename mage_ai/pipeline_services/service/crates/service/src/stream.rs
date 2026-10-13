//! Runs a stream: the tokio driver around `mage_stream_core::RunnerMachine`.
//!
//! The driver reads from the source, sends batches to the transform session (one lane, so a
//! partition's batches are transformed in order), writes them to every sink through one
//! writer task per sink (so a sink receives batches in order, and a failed write holds its
//! queue until the retry the machine schedules), and acknowledges positions when the machine
//! says so. Every outcome goes back to the machine as an event; the driver only performs
//! effects.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mage_stream_core::{
    BatchId, BatchMeta, Effect, Event, FailureKind, Operation, RunnerConfig, RunnerMachine, SinkId,
    StopReason,
};
use mage_stream_ports::{
    AckKind, CommitOutcome, ConnectorError, EnvelopeBuilder, Header, PartitionId, RecordBatch,
    Sink, Source, SourceEvent, TransformError, TransformSession, read_envelope,
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
    pub alerts: u64,
    pub last_error: Option<String>,
    /// Per partition: the last acknowledged position, as text.
    pub acknowledged: BTreeMap<u32, String>,
    /// Per partition this instance reads: messages behind the end, where the source knows.
    pub lag: BTreeMap<u32, i64>,
    /// Partitions this instance reads now.
    pub assigned: usize,
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamState {
    #[default]
    Starting,
    Running,
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
}

enum Internal {
    Event(Event),
    /// A transform finished: each block's output, or the error.
    Transformed(
        BatchId,
        Result<std::collections::BTreeMap<String, RecordBatch>, TransformError>,
    ),
    RetryNow(Operation),
}

struct WriteJob {
    id: BatchId,
    batch: RecordBatch,
    meta: BatchMeta,
}

/// A sink's writer: writes in order; after a failure, waits for the retry of that batch
/// before taking the next one.
fn spawn_writer(
    sink: Arc<dyn Sink>,
    sink_id: SinkId,
    dead_letter: bool,
    events: mpsc::UnboundedSender<Internal>,
) -> (
    mpsc::UnboundedSender<WriteJob>,
    mpsc::UnboundedSender<BatchId>,
) {
    let (jobs_tx, mut jobs) = mpsc::unbounded_channel::<WriteJob>();
    let (retry_tx, mut retries) = mpsc::unbounded_channel::<BatchId>();
    tokio::spawn(async move {
        while let Some(job) = jobs.recv().await {
            loop {
                let result = sink.write(&job.batch, &job.meta).await;
                let event = match (&result, dead_letter) {
                    (Ok(()), false) => Event::SinkReceipt {
                        id: job.id,
                        sink: sink_id,
                    },
                    (Ok(()), true) => Event::DeadLetterReceipt(job.id),
                    (Err(error), false) => Event::SinkFailed {
                        id: job.id,
                        sink: sink_id,
                        retryable: error.retryable,
                        error: error.message.clone(),
                    },
                    (Err(error), true) => Event::DeadLetterFailed {
                        id: job.id,
                        error: error.message.clone(),
                    },
                };
                if events.send(Internal::Event(event)).is_err() {
                    return;
                }
                if result.is_ok() {
                    break;
                }
                // Hold the queue until the machine schedules this batch's retry, or the
                // stream ends (a fatal error stops it).
                loop {
                    match retries.recv().await {
                        Some(id) if id == job.id => break,
                        Some(_) => continue,
                        None => return,
                    }
                }
            }
        }
    });
    (jobs_tx, retry_tx)
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
    let (tx, mut rx) = mpsc::unbounded_channel::<Internal>();
    let mut machine = RunnerMachine::new(parts.config.clone());

    // One writer per sink, plus the dead-letter sink.
    let mut writers = Vec::new();
    let sink_after: Vec<Option<String>> = parts.sinks.iter().map(|(_, a)| a.clone()).collect();
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
        .map(|sink| spawn_writer(sink, SinkId(u16::MAX), true, tx.clone()));

    // The transform lane.
    let (lane_tx, mut lane_rx) = mpsc::unbounded_channel::<(BatchId, RecordBatch)>();
    let mut transform = parts.transform.take();
    let lane_events = tx.clone();
    let lane = tokio::spawn(async move {
        while let Some((id, batch)) = lane_rx.recv().await {
            let result = match transform.as_mut() {
                Some(session) => session.transform(batch).await.map(|o| o.outputs),
                None => Ok(Default::default()),
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
    let mut outputs: HashMap<BatchId, std::collections::BTreeMap<String, RecordBatch>> =
        HashMap::new();
    let mut reading = true;
    let mut assigned: std::collections::BTreeSet<PartitionId> = Default::default();
    let mut idle_warned = false;
    let started = std::time::Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    set_state(&status, StreamState::Running);

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
                Internal::Transformed(id, Ok(by_block)) => {
                    let rows: usize = by_block.values().map(RecordBatch::num_rows).sum();
                    count(&status, |s| s.messages_out += rows as u64);
                    outputs.insert(id, by_block);
                    events.push(Event::TransformDone(id));
                }
                Internal::Transformed(id, Err(error)) => {
                    let kind = match error {
                        TransformError::User(_) | TransformError::Protocol(_) => FailureKind::User,
                        TransformError::Crash(_) => FailureKind::Crash,
                        TransformError::Timeout(_) => FailureKind::Timeout,
                    };
                    record_error(&status, &error.to_string());
                    events.push(Event::TransformFailed { id, kind, error: error.to_string() });
                }
                Internal::RetryNow(operation) => {
                    count(&status, |s| s.retries += 1);
                    match operation {
                        Operation::Transform(id) => {
                            if let Some((batch, _)) = inputs.get(&id) {
                                let _ = lane_tx.send((id, batch.clone()));
                            }
                        }
                        Operation::Write(id, sink) => {
                            if let Some((_, retry)) = writers.get(sink.0 as usize) {
                                let _ = retry.send(id);
                            }
                        }
                        Operation::DeadLetter(id) => {
                            if let Some((_, retry)) = &dead_letter_writer {
                                let _ = retry.send(id);
                            }
                        }
                    }
                }
            },
            event = parts.source.next_event(), if reading => match event {
                Ok(SourceEvent::Batch { batch, meta }) => {
                    count(&status, |s| {
                        s.batches += 1;
                        s.messages_in += meta.records as u64;
                    });
                    inputs.insert(meta.id, (batch, meta.clone()));
                    events.push(Event::BatchRead(meta));
                }
                Ok(SourceEvent::Assigned { partition, epoch }) => {
                    assigned.insert(partition);
                    events.push(Event::Assigned { partition, epoch });
                }
                Ok(SourceEvent::Revoking { partition, epoch, within }) => {
                    assigned.remove(&partition);
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
                    record_error(&status, &format!("the source failed: {error}"));
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            },
            _ = ticker.tick() => {
                events.push(Event::Tick);
                let lag = parts.source.lag();
                count(&status, |s| {
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
        }

        for event in events {
            let effects = machine.step(event);
            for effect in effects {
                match effect {
                    Effect::Transform(id) => {
                        if let Some((batch, _)) = inputs.get(&id) {
                            let _ = lane_tx.send((id, batch.clone()));
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
                                None => input.clone(),
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
                                    });
                                }
                                Err(e) => record_error(&status, &e.message),
                            }
                        }
                    }
                    Effect::Commit(commits) => {
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
                                    count(&status, |s| {
                                        s.acknowledged.insert(id_partition, position);
                                    });
                                    for effect in machine.step(Event::Committed(commit)) {
                                        if let Effect::Finished { .. } = effect {
                                            reading = false;
                                            set_state(&status, StreamState::Finished);
                                        }
                                    }
                                }
                            }
                            CommitOutcome::Stale => {}
                            CommitOutcome::Failed(error) => {
                                record_error(&status, &format!("acknowledging failed: {error}"));
                            }
                        }
                    }
                    Effect::Revoked { partition, epoch } => parts.source.revoked(partition, epoch),
                    Effect::Pause(partition) => parts.source.pause(partition),
                    Effect::Resume(partition) => parts.source.resume(partition),
                    Effect::Cancel(ids) => {
                        for id in ids {
                            inputs.remove(&id);
                            outputs.remove(&id);
                        }
                    }
                    Effect::StopReading => {
                        reading = false;
                        set_state(&status, StreamState::Draining);
                    }
                    Effect::Alert(message) => {
                        count(&status, |s| s.alerts += 1);
                        crate::log::warn(&format!("Stream {}: {message}", parts.name));
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
                        record_error(&status, &message);
                        set_state(&status, StreamState::Failed);
                    }
                    Effect::Finished { unfinished } => {
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

fn record_error(status: &SharedStatus, message: &str) {
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
                    })
                }
            })
        }

        fn close(&mut self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
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
