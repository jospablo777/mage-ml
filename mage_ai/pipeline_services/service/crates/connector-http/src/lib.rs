//! HTTP stream connectors (ADR-0031).
//!
//! Source: webhooks. The service's API hands each request's messages to an [`IngestHandle`];
//! the request becomes one batch, and its caller is answered only when the stream acknowledged
//! that batch, after every required sink confirmed it (or the dead-letter sink took it). A
//! caller that gets a 2xx knows its messages are delivered; any other answer means retry. An
//! `Idempotency-Key` makes the retry safe: a request with a key already delivered is answered
//! at once, and one still in flight waits for the same result. While the stream is paused for
//! backpressure, requests are refused as busy (503 with `Retry-After`) instead of queued.
//!
//! The source cannot replay: a request in flight when the service stops was not answered, so
//! its caller sends it again. Keys are remembered for `dedupe_window` in memory; after a
//! restart, a retried request is written again.

pub mod feast;
pub mod signature;
pub mod sink;

pub use feast::FeastSink;
pub use sink::HttpSink;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, ConnectorError,
    Envelope, EnvelopeBuilder, PartitionId, Position, Source, SourceCapabilities, SourceEvent,
};
use serde_json::value::RawValue;
use tokio::sync::{mpsc, oneshot};

const PARTITION: PartitionId = PartitionId(0);

/// How the webhook source behaves.
#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    /// Requests waiting for the stream before callers are told to retry later.
    pub queue: usize,
    /// How long a caller waits for its messages to be delivered.
    pub answer_within: Duration,
    /// How long an idempotency key is remembered after its request was delivered.
    pub dedupe_window: Duration,
    /// Keys remembered at most; the oldest go first.
    pub dedupe_keys: usize,
    /// How long the source waits for a request before it lets the stream tick.
    pub idle_after: Duration,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        HttpSourceConfig {
            queue: 64,
            answer_within: Duration::from_secs(30),
            dedupe_window: Duration::from_secs(24 * 3600),
            dedupe_keys: 100_000,
            idle_after: Duration::from_millis(500),
        }
    }
}

/// What a request's caller is told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestOutcome {
    /// The messages are delivered.
    Delivered { messages: usize },
    /// A request with this key was delivered before; nothing was written again.
    Duplicate { messages: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    /// The stream is behind or paused: retry after the given time.
    Busy { retry_after: Duration },
    /// Not delivered within `answer_within`; retry with the same key.
    Timeout,
    /// The stream is not reading (stopped or shutting down), with the reason.
    Unavailable(String),
    /// Nothing to deliver.
    Empty,
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::Busy { retry_after } => write!(
                f,
                "the stream is busy; retry in {} s",
                retry_after.as_secs().max(1)
            ),
            IngestError::Timeout => write!(
                f,
                "the messages were not confirmed in time; retry with the same Idempotency-Key"
            ),
            IngestError::Unavailable(reason) => write!(f, "the stream is not reading: {reason}"),
            IngestError::Empty => write!(f, "the request holds no messages"),
        }
    }
}

type Answer = oneshot::Sender<Result<usize, IngestError>>;

struct Submission {
    messages: Vec<Box<RawValue>>,
    key: Option<String>,
    answer: Answer,
}

enum KeyState {
    /// In flight: callers waiting for the same request.
    Pending(Vec<Answer>),
    Delivered {
        messages: usize,
        at: Instant,
    },
}

struct Shared {
    paused: AtomicBool,
    closed: Mutex<Option<String>>,
    keys: Mutex<Keys>,
}

#[derive(Default)]
struct Keys {
    states: HashMap<String, KeyState>,
    /// Delivered keys in the order they were delivered, for expiry.
    order: VecDeque<(Instant, String)>,
}

impl Keys {
    fn expire(&mut self, window: Duration, limit: usize) {
        let now = Instant::now();
        while let Some((at, key)) = self.order.front() {
            if now.duration_since(*at) < window && self.order.len() <= limit {
                break;
            }
            if let Some(KeyState::Delivered { at: delivered, .. }) = self.states.get(key)
                && delivered == at
            {
                self.states.remove(key);
            }
            self.order.pop_front();
        }
    }

    fn deliver(&mut self, key: String, messages: usize) {
        let at = Instant::now();
        if let Some(KeyState::Pending(waiters)) = self
            .states
            .insert(key.clone(), KeyState::Delivered { messages, at })
        {
            for waiter in waiters {
                let _ = waiter.send(Ok(messages));
            }
        }
        self.order.push_back((at, key));
    }

    fn fail(&mut self, key: &str, error: &IngestError) {
        if let Some(KeyState::Pending(waiters)) = self.states.remove(key) {
            for waiter in waiters {
                let _ = waiter.send(Err(error.clone()));
            }
        }
    }
}

/// The API's side of the webhook source.
#[derive(Clone)]
pub struct IngestHandle {
    submissions: mpsc::Sender<Submission>,
    shared: Arc<Shared>,
    config: HttpSourceConfig,
}

impl IngestHandle {
    /// Hands the messages to the stream and waits until they are delivered.
    pub async fn submit(
        &self,
        messages: Vec<Box<RawValue>>,
        key: Option<String>,
    ) -> Result<IngestOutcome, IngestError> {
        if messages.is_empty() {
            return Err(IngestError::Empty);
        }
        if let Some(reason) = self
            .shared
            .closed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            return Err(IngestError::Unavailable(reason));
        }
        let (answer, answered) = oneshot::channel();
        enum Seen {
            New(Answer),
            Delivered(usize),
            Waiting,
        }
        let seen = match &key {
            None => Seen::New(answer),
            Some(key) => {
                let mut keys = self.shared.keys.lock().unwrap_or_else(|p| p.into_inner());
                keys.expire(self.config.dedupe_window, self.config.dedupe_keys);
                match keys.states.get_mut(key) {
                    Some(KeyState::Delivered { messages, .. }) => Seen::Delivered(*messages),
                    Some(KeyState::Pending(waiters)) => {
                        waiters.push(answer);
                        Seen::Waiting
                    }
                    None => {
                        keys.states
                            .insert(key.clone(), KeyState::Pending(Vec::new()));
                        Seen::New(answer)
                    }
                }
            }
        };
        let answer = match seen {
            Seen::Delivered(messages) => return Ok(IngestOutcome::Duplicate { messages }),
            Seen::Waiting => return self.wait(answered, true).await,
            Seen::New(answer) => answer,
        };
        if self.shared.paused.load(Ordering::Relaxed) {
            self.forget(&key);
            return Err(IngestError::Busy {
                retry_after: Duration::from_secs(1),
            });
        }
        let submission = Submission {
            messages,
            key: key.clone(),
            answer,
        };
        if self.submissions.try_send(submission).is_err() {
            self.forget(&key);
            return Err(IngestError::Busy {
                retry_after: Duration::from_secs(1),
            });
        }
        self.wait(answered, false).await
    }

    async fn wait(
        &self,
        answered: oneshot::Receiver<Result<usize, IngestError>>,
        duplicate: bool,
    ) -> Result<IngestOutcome, IngestError> {
        match tokio::time::timeout(self.config.answer_within, answered).await {
            Ok(Ok(Ok(messages))) if duplicate => Ok(IngestOutcome::Duplicate { messages }),
            Ok(Ok(Ok(messages))) => Ok(IngestOutcome::Delivered { messages }),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(IngestError::Unavailable("the stream stopped".into())),
            // The request stays in flight; a retry with its key waits for the same result.
            Err(_) => Err(IngestError::Timeout),
        }
    }

    fn forget(&self, key: &Option<String>) {
        if let Some(key) = key {
            let mut keys = self.shared.keys.lock().unwrap_or_else(|p| p.into_inner());
            if matches!(keys.states.get(key), Some(KeyState::Pending(w)) if w.is_empty()) {
                keys.states.remove(key);
            }
        }
    }

    /// Whether the stream is refusing requests for backpressure.
    pub fn busy(&self) -> bool {
        self.shared.paused.load(Ordering::Relaxed)
    }
}

struct InFlight {
    last: i64,
    messages: usize,
    key: Option<String>,
    answer: Answer,
}

/// The stream's side of the webhook source: one partition, one batch per request.
pub struct HttpSource {
    submissions: mpsc::Receiver<Submission>,
    shared: Arc<Shared>,
    config: HttpSourceConfig,
    epoch: AssignmentEpoch,
    assigned: bool,
    next_position: i64,
    seq: u64,
    in_flight: BTreeMap<i64, InFlight>,
}

impl HttpSource {
    pub fn new(config: HttpSourceConfig) -> (HttpSource, IngestHandle) {
        let (tx, rx) = mpsc::channel(config.queue.max(1));
        let shared = Arc::new(Shared {
            paused: AtomicBool::new(false),
            closed: Mutex::new(None),
            keys: Mutex::new(Keys::default()),
        });
        let handle = IngestHandle {
            submissions: tx,
            shared: shared.clone(),
            config: config.clone(),
        };
        let source = HttpSource {
            submissions: rx,
            shared,
            config,
            epoch: AssignmentEpoch(1),
            assigned: false,
            next_position: 0,
            seq: 0,
            in_flight: BTreeMap::new(),
        };
        (source, handle)
    }

    fn batch(&mut self, submission: Submission) -> Result<SourceEvent, ConnectorError> {
        let mut builder = EnvelopeBuilder::new();
        let first = self.next_position;
        for message in &submission.messages {
            builder.push(&Envelope {
                value: message.get().as_bytes().to_vec(),
                partition: PARTITION.0,
                position: Some(self.next_position),
                ..Envelope::default()
            });
            self.next_position += 1;
        }
        let last = self.next_position - 1;
        let bytes = builder.bytes() as u64;
        let batch = builder
            .finish()
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let meta = BatchMeta {
            id: BatchId {
                partition: PARTITION,
                epoch: self.epoch,
                seq: self.seq,
            },
            first: Position::Offset(first),
            last: Position::Offset(last),
            records: submission.messages.len() as u32,
            bytes,
        };
        self.seq += 1;
        self.in_flight.insert(
            last,
            InFlight {
                last,
                messages: submission.messages.len(),
                key: submission.key,
                answer: submission.answer,
            },
        );
        Ok(SourceEvent::Batch { batch, meta })
    }

    /// Answers every request still waiting, as the stream will not deliver them.
    fn fail_all(&mut self, reason: &str) {
        let error = IngestError::Unavailable(reason.to_string());
        let mut keys = self.shared.keys.lock().unwrap_or_else(|p| p.into_inner());
        for (_, request) in std::mem::take(&mut self.in_flight) {
            if let Some(key) = &request.key {
                keys.fail(key, &error);
            }
            let _ = request.answer.send(Err(error.clone()));
        }
    }
}

impl Source for HttpSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            if !self.assigned {
                self.assigned = true;
                return Ok(SourceEvent::Assigned {
                    partition: PARTITION,
                    epoch: self.epoch,
                });
            }
            match tokio::time::timeout(self.config.idle_after, self.submissions.recv()).await {
                Ok(Some(submission)) => self.batch(submission),
                Ok(None) => Ok(SourceEvent::End),
                Err(_) => Ok(SourceEvent::Idle),
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            for commit in commits {
                if commit.partition != PARTITION || commit.epoch != self.epoch {
                    return CommitOutcome::Stale;
                }
                let Position::Offset(through) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "the HTTP source acknowledges positions",
                    ));
                };
                let done: Vec<i64> = self.in_flight.range(..=through).map(|(k, _)| *k).collect();
                let mut keys = self.shared.keys.lock().unwrap_or_else(|p| p.into_inner());
                for last in done {
                    let request = self.in_flight.remove(&last).expect("listed above");
                    debug_assert_eq!(request.last, last);
                    if let Some(key) = request.key {
                        keys.deliver(key, request.messages);
                    }
                    let _ = request.answer.send(Ok(request.messages));
                }
            }
            CommitOutcome::Committed
        })
    }

    fn pause(&mut self, _partition: PartitionId) {
        self.shared.paused.store(true, Ordering::Relaxed);
    }

    fn resume(&mut self, _partition: PartitionId) {
        self.shared.paused.store(false, Ordering::Relaxed);
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            replayable: false,
            ordered: false,
            ack_kind: AckKind::Response,
            max_instances: None,
            needs_keepalive: false,
        }
    }

    fn failed(&mut self, reason: &str) {
        let reason = format!("the stream stopped: {reason}");
        *self.shared.closed.lock().unwrap_or_else(|p| p.into_inner()) = Some(reason.clone());
        while let Ok(submission) = self.submissions.try_recv() {
            let _ = submission
                .answer
                .send(Err(IngestError::Unavailable(reason.clone())));
        }
        self.fail_all(&reason);
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            *self.shared.closed.lock().unwrap_or_else(|p| p.into_inner()) =
                Some("the service is stopping".into());
            self.submissions.close();
            while let Ok(submission) = self.submissions.try_recv() {
                let _ = submission.answer.send(Err(IngestError::Unavailable(
                    "the service is stopping".into(),
                )));
            }
            self.fail_all("the service is stopping");
        })
    }
}

impl Drop for HttpSource {
    fn drop(&mut self) {
        self.fail_all("the stream stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(text: &str) -> Box<RawValue> {
        RawValue::from_string(text.to_string()).unwrap()
    }

    async fn next_batch(source: &mut HttpSource) -> BatchMeta {
        loop {
            if let SourceEvent::Batch { meta, .. } = source.next_event().await.unwrap() {
                return meta;
            }
        }
    }

    fn commit_through(meta: &BatchMeta) -> Commit {
        Commit {
            partition: meta.id.partition,
            epoch: meta.id.epoch,
            position: meta.last.clone(),
        }
    }

    #[tokio::test]
    async fn a_caller_is_answered_only_after_its_batch_is_acknowledged() {
        let (mut source, handle) = HttpSource::new(HttpSourceConfig::default());
        let caller =
            tokio::spawn(
                async move { handle.submit(vec![raw(r#"{"a":1}"#), raw("2")], None).await },
            );
        let meta = next_batch(&mut source).await;
        assert_eq!(meta.records, 2);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!caller.is_finished(), "answered before the sinks confirmed");

        source.commit(&[commit_through(&meta)]).await;

        assert_eq!(
            caller.await.unwrap(),
            Ok(IngestOutcome::Delivered { messages: 2 })
        );
    }

    #[tokio::test]
    async fn a_retry_with_the_same_key_writes_nothing_again() {
        let (mut source, handle) = HttpSource::new(HttpSourceConfig::default());
        let first = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(vec![raw("1")], Some("k".into())).await })
        };
        let meta = next_batch(&mut source).await;
        // A retry while the first is in flight waits for the same result.
        let retry = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(vec![raw("1")], Some("k".into())).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        source.commit(&[commit_through(&meta)]).await;

        assert_eq!(
            first.await.unwrap(),
            Ok(IngestOutcome::Delivered { messages: 1 })
        );
        assert_eq!(
            retry.await.unwrap(),
            Ok(IngestOutcome::Duplicate { messages: 1 })
        );
        // Later, the key is answered at once and nothing reaches the stream.
        assert_eq!(
            handle.submit(vec![raw("1")], Some("k".into())).await,
            Ok(IngestOutcome::Duplicate { messages: 1 })
        );
        assert!(matches!(
            source.next_event().await.unwrap(),
            SourceEvent::Idle
        ));
    }

    #[tokio::test]
    async fn backpressure_refuses_requests_instead_of_queueing_them() {
        let (mut source, handle) = HttpSource::new(HttpSourceConfig::default());
        source.pause(PARTITION);
        assert!(matches!(
            handle.submit(vec![raw("1")], Some("k".into())).await,
            Err(IngestError::Busy { .. })
        ));
        source.resume(PARTITION);
        // The refused key can be sent again.
        let handle2 = handle.clone();
        let caller =
            tokio::spawn(async move { handle2.submit(vec![raw("1")], Some("k".into())).await });
        let meta = next_batch(&mut source).await;
        source.commit(&[commit_through(&meta)]).await;
        assert_eq!(
            caller.await.unwrap(),
            Ok(IngestOutcome::Delivered { messages: 1 })
        );
    }

    #[tokio::test]
    async fn a_full_queue_is_busy_and_a_stopping_service_answers_everyone() {
        let config = HttpSourceConfig {
            queue: 1,
            ..HttpSourceConfig::default()
        };
        let (mut source, handle) = HttpSource::new(config);
        let waiting = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(vec![raw("1")], None).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(matches!(
            handle.submit(vec![raw("2")], None).await,
            Err(IngestError::Busy { .. })
        ));

        source.close().await;

        assert!(matches!(
            waiting.await.unwrap(),
            Err(IngestError::Unavailable(_))
        ));
        assert!(matches!(
            handle.submit(vec![raw("3")], None).await,
            Err(IngestError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn a_stopped_stream_answers_its_callers_at_once_with_the_reason() {
        let (mut source, handle) = HttpSource::new(HttpSourceConfig::default());
        let waiting = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(vec![raw("1")], Some("k".into())).await })
        };
        next_batch(&mut source).await;

        source.failed("sink 0 rejected the batch");

        match waiting.await.unwrap() {
            Err(IngestError::Unavailable(reason)) => {
                assert!(reason.contains("rejected"), "{reason}")
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            handle.submit(vec![raw("2")], None).await,
            Err(IngestError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn an_unconfirmed_request_times_out_and_its_key_still_resolves() {
        let config = HttpSourceConfig {
            answer_within: Duration::from_millis(50),
            ..HttpSourceConfig::default()
        };
        let (mut source, handle) = HttpSource::new(config);
        let first = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(vec![raw("1")], Some("k".into())).await })
        };
        let meta = next_batch(&mut source).await;
        assert_eq!(first.await.unwrap(), Err(IngestError::Timeout));

        source.commit(&[commit_through(&meta)]).await;

        assert_eq!(
            handle.submit(vec![raw("1")], Some("k".into())).await,
            Ok(IngestOutcome::Duplicate { messages: 1 })
        );
    }
}
