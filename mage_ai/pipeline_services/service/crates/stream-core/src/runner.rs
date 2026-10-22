//! What a stream does next, as a pure function of events.
//!
//! The driver (a tokio task in the engine) reads batches, runs transforms, writes to sinks
//! and talks to the source; it reports each outcome as an [`Event`] and performs the
//! [`Effect`]s the machine returns. Failure rules (ADR-0030):
//!
//! - A transform failure (user code, a crashed or stuck worker) is retried with backoff;
//!   after the last retry the batch goes to the dead-letter sink, or the stream stops when
//!   there is none or the poison policy says so. A message is never dropped.
//! - A sink outage is retried with backoff for as long as it lasts, with an alert; valid data
//!   never goes to dead letters because a sink is down. A sink that rejects a batch as invalid
//!   (not retryable) stops the stream with that sink's error.
//! - A retry goes only to the sinks that have not confirmed the batch.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::ids::{AssignmentEpoch, BatchId, BatchMeta, PartitionId, SinkId};
use crate::tracker::{AckTracker, Admission, Commit, ReceiptOutcome, TrackerConfig};

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub retries: u32,
    pub delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    /// The wait before attempt `attempt + 1`, doubling from `delay` up to `max_delay`.
    pub fn delay(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
        self.delay.saturating_mul(factor).min(self.max_delay)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoisonPolicy {
    /// After the last retry, the batch's rows go to the dead-letter sink.
    DeadLetter,
    /// After the last retry, the stream stops: the default for CDC and keyed streams, where
    /// skipping a message breaks per-key order.
    Stop,
}

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub tracker: TrackerConfig,
    /// Every sink, required and best effort.
    pub sinks: Vec<SinkId>,
    pub transform_retry: RetryPolicy,
    /// Delays for sink retries. Retryable failures of required sinks retry without limit;
    /// best-effort sinks give up after `retries`.
    pub sink_retry: RetryPolicy,
    pub poison: PoisonPolicy,
    pub has_dead_letter: bool,
    /// Consecutive failures of one write before an alert.
    pub alert_after: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// The block raised an error.
    User,
    /// The worker process died.
    Crash,
    /// The transform ran past its deadline.
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Transform(BatchId),
    Write(BatchId, SinkId),
    DeadLetter(BatchId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Assigned {
        partition: PartitionId,
        epoch: AssignmentEpoch,
    },
    /// The source is giving the partition up: finish its batches in flight, acknowledge
    /// them, then report [`Effect::Revoked`], so the next owner does not repeat them.
    Revoking {
        partition: PartitionId,
        epoch: AssignmentEpoch,
    },
    /// The time the source allows for a revoke passed: what is not done is dropped.
    RevokeDeadline {
        partition: PartitionId,
        epoch: AssignmentEpoch,
    },
    BatchRead(BatchMeta),
    /// The driver made a timer batch (no message, no position; `BatchId::is_timer`) for an
    /// idle partition, so its due timers fire without waiting for a message.
    TimerBatch(BatchMeta),
    /// Some messages of the batch failed on their own; the others' output is coming
    /// (`TransformDone` follows). They go to the dead-letter sink, or the stream stops.
    RowsRejected {
        id: BatchId,
        count: usize,
        error: String,
    },
    TransformDone(BatchId),
    TransformFailed {
        id: BatchId,
        kind: FailureKind,
        error: String,
    },
    SinkReceipt {
        id: BatchId,
        sink: SinkId,
    },
    SinkFailed {
        id: BatchId,
        sink: SinkId,
        retryable: bool,
        error: String,
    },
    DeadLetterReceipt(BatchId),
    DeadLetterFailed {
        id: BatchId,
        error: String,
    },
    Committed(Commit),
    /// Periodic: re-emits commits the source has not confirmed.
    Tick,
    /// Stop reading; finish the batches in flight.
    Shutdown,
    /// The shutdown deadline passed.
    DeadlinePassed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// A batch failed its transform after every retry and the policy is to stop.
    Poison { id: BatchId, error: String },
    /// A required sink rejected a batch as invalid.
    SinkRejected {
        id: BatchId,
        sink: SinkId,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Transform(BatchId),
    Write {
        id: BatchId,
        sink: SinkId,
    },
    Retry {
        operation: Operation,
        after: Duration,
    },
    DeadLetter {
        id: BatchId,
        error: String,
    },
    /// Write the batch's rejected messages to the dead-letter sink; its receipt comes as a
    /// `SinkReceipt` from [`SinkId::REJECTED_ROWS`].
    DeadLetterRows(BatchId),
    /// Acknowledge these positions to the source, then report [`Event::Committed`].
    Commit(Vec<Commit>),
    /// These timer batches are done: forget them, and save the state of the sessions at
    /// these positions (each partition's last done one), which the source has already.
    TimersDone {
        ids: Vec<BatchId>,
        saves: Vec<Commit>,
    },
    Pause(PartitionId),
    Resume(PartitionId),
    /// Work for these batches is stale (their partition was revoked or reassigned): stop it.
    Cancel(Vec<BatchId>),
    /// The partition's revoke is complete: what was done is acknowledged (the commit comes
    /// before this effect); the source may hand the partition over.
    Revoked {
        partition: PartitionId,
        epoch: AssignmentEpoch,
    },
    StopReading,
    Alert(String),
    Stop(StopReason),
    /// Shutdown is complete. `unfinished` were in flight at the deadline and are not
    /// acknowledged; a restart reads them again.
    Finished {
        unfinished: Vec<BatchId>,
    },
}

#[derive(Debug)]
pub struct RunnerMachine {
    config: RunnerConfig,
    tracker: AckTracker,
    transform_attempts: BTreeMap<BatchId, u32>,
    write_attempts: BTreeMap<(BatchId, SinkId), u32>,
    dead_letter_attempts: BTreeMap<BatchId, u32>,
    best_effort_pending: BTreeSet<(BatchId, SinkId)>,
    paused: BTreeSet<PartitionId>,
    /// Partitions being revoked, waiting for their batches in flight.
    draining: BTreeMap<PartitionId, AssignmentEpoch>,
    stopping: bool,
    finished: bool,
    stopped: bool,
}

impl RunnerMachine {
    pub fn new(config: RunnerConfig) -> RunnerMachine {
        RunnerMachine {
            tracker: AckTracker::new(config.tracker.clone()),
            config,
            transform_attempts: BTreeMap::new(),
            write_attempts: BTreeMap::new(),
            dead_letter_attempts: BTreeMap::new(),
            best_effort_pending: BTreeSet::new(),
            paused: BTreeSet::new(),
            draining: BTreeMap::new(),
            stopping: false,
            finished: false,
            stopped: false,
        }
    }

    pub fn tracker(&self) -> &AckTracker {
        &self.tracker
    }

    /// Whether the runner paused the partition for backpressure.
    pub fn is_paused(&self, partition: PartitionId) -> bool {
        self.paused.contains(&partition)
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn step(&mut self, event: Event) -> Vec<Effect> {
        if self.finished || (self.stopped && event != Event::DeadlinePassed) {
            return Vec::new();
        }
        let mut effects = Vec::new();
        match event {
            Event::Assigned { partition, epoch } => match self.tracker.assign(partition, epoch) {
                Ok(dropped) => {
                    // Assigned again before the revoke finished: that revoke is over.
                    if let Some(old) = self.draining.remove(&partition) {
                        effects.push(Effect::Revoked {
                            partition,
                            epoch: old,
                        });
                    }
                    self.paused.remove(&partition);
                    self.forget(&dropped);
                    if !dropped.is_empty() {
                        effects.push(Effect::Cancel(dropped));
                    }
                }
                Err(stale) => effects.push(Effect::Alert(format!(
                    "the source assigned partition {} in epoch {}, not above epoch {}; the \
                     assignment is ignored",
                    stale.partition.0, stale.epoch.0, stale.highest.0
                ))),
            },
            Event::Revoking { partition, epoch } => {
                if self.tracker.epoch(partition) == Some(epoch) {
                    // Finished below once nothing of the partition is in flight.
                    self.draining.insert(partition, epoch);
                } else {
                    effects.push(Effect::Revoked { partition, epoch });
                }
            }
            Event::RevokeDeadline { partition, epoch } => {
                if self.draining.get(&partition) == Some(&epoch) {
                    self.release(partition, epoch, &mut effects);
                }
            }
            Event::BatchRead(meta) => {
                let id = meta.id;
                if self.draining.get(&id.partition) == Some(&id.epoch) {
                    effects.push(Effect::Cancel(vec![id]));
                    return effects;
                }
                match self.tracker.admit(meta) {
                    Admission::Stale => effects.push(Effect::Cancel(vec![id])),
                    Admission::Admitted { pause } => {
                        effects.push(Effect::Transform(id));
                        if pause && self.paused.insert(id.partition) {
                            effects.push(Effect::Pause(id.partition));
                        }
                    }
                }
            }
            Event::TimerBatch(meta) => {
                let id = meta.id;
                if self.stopping || self.draining.contains_key(&id.partition) {
                    effects.push(Effect::Cancel(vec![id]));
                    return effects;
                }
                match self.tracker.admit_timer(meta) {
                    Admission::Stale => effects.push(Effect::Cancel(vec![id])),
                    Admission::Admitted { .. } => effects.push(Effect::Transform(id)),
                }
            }
            Event::RowsRejected { id, count, error } => {
                if self.tracker.epoch(id.partition) == Some(id.epoch) {
                    if self.config.poison == PoisonPolicy::DeadLetter && self.config.has_dead_letter
                    {
                        if self.tracker.require(id, SinkId::REJECTED_ROWS) {
                            effects.push(Effect::DeadLetterRows(id));
                        }
                    } else {
                        self.stopped = true;
                        effects.push(Effect::StopReading);
                        effects.push(Effect::Stop(StopReason::Poison {
                            id,
                            error: format!("{count} message(s) of {id} failed: {error}"),
                        }));
                    }
                }
            }
            Event::TransformDone(id) => {
                if self.tracker.epoch(id.partition) == Some(id.epoch) {
                    self.transform_attempts.remove(&id);
                    for sink in &self.config.sinks {
                        if !self.config.tracker.required_sinks.contains(sink) {
                            self.best_effort_pending.insert((id, *sink));
                        }
                        effects.push(Effect::Write { id, sink: *sink });
                    }
                }
            }
            Event::TransformFailed { id, kind, error } => {
                if self.tracker.epoch(id.partition) == Some(id.epoch) {
                    let attempts = self.transform_attempts.entry(id).or_insert(0);
                    *attempts += 1;
                    let attempts = *attempts;
                    if attempts <= self.config.transform_retry.retries {
                        effects.push(Effect::Retry {
                            operation: Operation::Transform(id),
                            after: self.config.transform_retry.delay(attempts),
                        });
                    } else {
                        let error = format!("{} ({kind:?}, {attempts} attempts): {error}", id);
                        if self.config.poison == PoisonPolicy::DeadLetter
                            && self.config.has_dead_letter
                        {
                            effects.push(Effect::DeadLetter { id, error });
                        } else {
                            self.stopped = true;
                            effects.push(Effect::StopReading);
                            effects.push(Effect::Stop(StopReason::Poison { id, error }));
                        }
                    }
                }
            }
            Event::SinkReceipt { id, sink } => {
                self.write_attempts.remove(&(id, sink));
                self.best_effort_pending.remove(&(id, sink));
                if self.tracker.on_receipt(id, sink) == ReceiptOutcome::Recorded {
                    self.commits(&mut effects);
                }
            }
            Event::SinkFailed {
                id,
                sink,
                retryable,
                error,
            } => {
                if sink == SinkId::REJECTED_ROWS
                    && self.tracker.epoch(id.partition) == Some(id.epoch)
                {
                    // Rejected messages are never dropped: retry for as long as it takes.
                    let attempts = self.write_attempts.entry((id, sink)).or_insert(0);
                    *attempts += 1;
                    if *attempts == self.config.alert_after {
                        effects.push(Effect::Alert(format!(
                            "the dead-letter sink failed {attempts} times on {id}'s rejected \
                             messages: {error}"
                        )));
                    }
                    effects.push(Effect::Retry {
                        operation: Operation::Write(id, sink),
                        after: self.config.sink_retry.delay(*attempts),
                    });
                } else if self.tracker.epoch(id.partition) == Some(id.epoch) {
                    let required = self.config.tracker.required_sinks.contains(&sink);
                    let attempts = self.write_attempts.entry((id, sink)).or_insert(0);
                    *attempts += 1;
                    let attempts = *attempts;
                    if attempts == self.config.alert_after {
                        effects.push(Effect::Alert(format!(
                            "sink {} failed {attempts} times on {id}: {error}",
                            sink.0
                        )));
                    }
                    if required && !retryable {
                        self.stopped = true;
                        effects.push(Effect::StopReading);
                        effects.push(Effect::Stop(StopReason::SinkRejected { id, sink, error }));
                    } else if !required && attempts > self.config.sink_retry.retries {
                        // A best-effort sink gives up on this batch.
                        self.write_attempts.remove(&(id, sink));
                        self.best_effort_pending.remove(&(id, sink));
                        effects.push(Effect::Alert(format!(
                            "best-effort sink {} skipped {id}: {error}",
                            sink.0
                        )));
                    } else {
                        effects.push(Effect::Retry {
                            operation: Operation::Write(id, sink),
                            after: self.config.sink_retry.delay(attempts),
                        });
                    }
                }
            }
            Event::DeadLetterReceipt(id) => {
                self.dead_letter_attempts.remove(&id);
                if self.tracker.on_dead_lettered(id) == ReceiptOutcome::Recorded {
                    self.commits(&mut effects);
                }
            }
            Event::DeadLetterFailed { id, error } => {
                if self.tracker.epoch(id.partition) == Some(id.epoch) {
                    let attempts = self.dead_letter_attempts.entry(id).or_insert(0);
                    *attempts += 1;
                    if *attempts == self.config.alert_after {
                        effects.push(Effect::Alert(format!(
                            "the dead-letter sink failed {attempts} times on {id}: {error}"
                        )));
                    }
                    // Rejected rows are never dropped: retry for as long as it takes.
                    effects.push(Effect::Retry {
                        operation: Operation::DeadLetter(id),
                        after: self.config.sink_retry.delay(*attempts),
                    });
                }
            }
            Event::Committed(commit) => {
                self.tracker.confirm(&commit);
                // Unconfirmed batches hold partitions paused; confirmed, they can read on.
                self.resumes(&mut effects);
            }
            Event::Tick => self.commits(&mut effects),
            Event::Shutdown => {
                if !self.stopping {
                    self.stopping = true;
                    effects.push(Effect::StopReading);
                }
            }
            Event::DeadlinePassed => {
                self.finished = true;
                effects.push(Effect::Finished {
                    unfinished: self.tracker.in_flight(),
                });
                return effects;
            }
        }
        let drained: Vec<(PartitionId, AssignmentEpoch)> = self
            .draining
            .iter()
            .filter(|(p, _)| !self.tracker.has_unfinished(**p))
            .map(|(p, e)| (*p, *e))
            .collect();
        for (partition, epoch) in drained {
            self.release(partition, epoch, &mut effects);
        }
        if self.stopping && !self.stopped && self.tracker.is_settled() {
            self.finished = true;
            effects.push(Effect::Finished {
                unfinished: Vec::new(),
            });
        }
        effects
    }

    fn commits(&mut self, effects: &mut Vec<Effect>) {
        let commits = self.tracker.committable();
        if !commits.is_empty() {
            effects.push(Effect::Commit(commits));
        }
        self.timers_done(effects);
        self.resumes(effects);
    }

    fn timers_done(&mut self, effects: &mut Vec<Effect>) {
        let (ids, saves) = self.tracker.take_timers_done();
        if !ids.is_empty() {
            effects.push(Effect::TimersDone { ids, saves });
        }
    }

    /// Partitions read now with nothing in flight, which can take a timer batch.
    pub fn idle_partitions(&self) -> Vec<(PartitionId, AssignmentEpoch)> {
        if self.stopping || self.stopped {
            return Vec::new();
        }
        self.tracker
            .assigned()
            .into_iter()
            .filter(|(partition, _)| {
                !self.draining.contains_key(partition) && !self.tracker.has_unfinished(*partition)
            })
            .collect()
    }

    fn resumes(&mut self, effects: &mut Vec<Effect>) {
        let resumable: Vec<PartitionId> = self
            .paused
            .iter()
            .copied()
            .filter(|p| self.tracker.can_resume(*p))
            .collect();
        for partition in resumable {
            self.paused.remove(&partition);
            effects.push(Effect::Resume(partition));
        }
    }

    /// Ends a revoke: acknowledges what is done, drops the rest, and reports it.
    fn release(
        &mut self,
        partition: PartitionId,
        epoch: AssignmentEpoch,
        effects: &mut Vec<Effect>,
    ) {
        self.draining.remove(&partition);
        let commits: Vec<Commit> = self
            .tracker
            .committable()
            .into_iter()
            .filter(|c| c.partition == partition && c.epoch == epoch)
            .collect();
        if !commits.is_empty() {
            effects.push(Effect::Commit(commits));
        }
        self.timers_done(effects);
        let dropped = self.tracker.revoke(partition, epoch);
        self.paused.remove(&partition);
        self.forget(&dropped);
        if !dropped.is_empty() {
            effects.push(Effect::Cancel(dropped));
        }
        effects.push(Effect::Revoked { partition, epoch });
    }

    fn forget(&mut self, ids: &[BatchId]) {
        for id in ids {
            self.transform_attempts.remove(id);
            self.dead_letter_attempts.remove(id);
            self.write_attempts.retain(|(b, _), _| b != id);
            self.best_effort_pending.retain(|(b, _)| b != id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::Position;
    use proptest::prelude::*;

    const P: PartitionId = PartitionId(0);
    const E: AssignmentEpoch = AssignmentEpoch(1);
    const REQUIRED: SinkId = SinkId(0);
    const BEST_EFFORT: SinkId = SinkId(1);

    fn config(poison: PoisonPolicy, has_dead_letter: bool) -> RunnerConfig {
        RunnerConfig {
            tracker: TrackerConfig {
                required_sinks: vec![REQUIRED],
                max_in_flight_bytes: 10_000,
                max_in_flight_batches: 8,
            },
            sinks: vec![REQUIRED, BEST_EFFORT],
            transform_retry: RetryPolicy {
                retries: 2,
                delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(1),
            },
            sink_retry: RetryPolicy {
                retries: 2,
                delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(30),
            },
            poison,
            has_dead_letter,
            alert_after: 3,
        }
    }

    fn meta(seq: u64) -> BatchMeta {
        BatchMeta {
            id: BatchId {
                partition: P,
                epoch: E,
                seq,
            },
            first: Position::Offset(seq as i64 * 10),
            last: Position::Offset(seq as i64 * 10 + 9),
            records: 10,
            bytes: 100,
        }
    }

    fn started(poison: PoisonPolicy, dead_letter: bool) -> RunnerMachine {
        let mut m = RunnerMachine::new(config(poison, dead_letter));
        m.step(Event::Assigned {
            partition: P,
            epoch: E,
        });
        m
    }

    fn timer_meta(n: u64) -> BatchMeta {
        BatchMeta {
            id: BatchId {
                partition: P,
                epoch: E,
                seq: crate::ids::TIMER_SEQ | n,
            },
            first: Position::Opaque(n.to_be_bytes().to_vec()),
            last: Position::Opaque(n.to_be_bytes().to_vec()),
            records: 0,
            bytes: 0,
        }
    }

    fn finish(m: &mut RunnerMachine, id: BatchId) -> Vec<Effect> {
        let mut effects = m.step(Event::TransformDone(id));
        effects.extend(m.step(Event::SinkReceipt { id, sink: REQUIRED }));
        effects
    }

    #[test]
    fn a_timer_batch_runs_and_is_never_committed() {
        let mut m = started(PoisonPolicy::Stop, false);
        assert_eq!(m.idle_partitions(), [(P, E)]);
        let timer = timer_meta(1);
        assert_eq!(
            m.step(Event::TimerBatch(timer.clone())),
            [Effect::Transform(timer.id)]
        );
        assert!(m.idle_partitions().is_empty());
        let effects = finish(&mut m, timer.id);
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Commit(_))),
            "{effects:?}"
        );
        // No batch was ever done: nothing to save the state at yet.
        assert!(
            effects.contains(&Effect::TimersDone {
                ids: vec![timer.id],
                saves: vec![]
            }),
            "{effects:?}"
        );
        assert_eq!(m.idle_partitions(), [(P, E)]);

        // A source batch after it keeps its own numbering, and commits as usual.
        assert_eq!(
            m.step(Event::BatchRead(meta(0))),
            [Effect::Transform(meta(0).id)]
        );
        let effects = finish(&mut m, meta(0).id);
        let commit = Commit {
            partition: P,
            epoch: E,
            position: Position::Offset(9),
        };
        assert!(
            effects.contains(&Effect::Commit(vec![commit.clone()])),
            "{effects:?}"
        );
        m.step(Event::Committed(commit.clone()));

        // The next timer batch saves the state at the last done position, commits nothing.
        let effects = {
            m.step(Event::TimerBatch(timer_meta(2)));
            finish(&mut m, timer_meta(2).id)
        };
        assert!(
            effects.contains(&Effect::TimersDone {
                ids: vec![timer_meta(2).id],
                saves: vec![commit]
            }),
            "{effects:?}"
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Commit(_))),
            "{effects:?}"
        );
    }

    #[test]
    fn a_timer_batch_of_an_old_epoch_or_without_the_timer_bit_is_cancelled() {
        let mut m = started(PoisonPolicy::Stop, false);
        let mut old = timer_meta(1);
        old.id.epoch = AssignmentEpoch(0);
        assert_eq!(
            m.step(Event::TimerBatch(old.clone())),
            [Effect::Cancel(vec![old.id])]
        );
        assert_eq!(
            m.step(Event::TimerBatch(meta(5))),
            [Effect::Cancel(vec![meta(5).id])]
        );
    }

    #[test]
    fn rejected_messages_go_to_the_dead_letter_sink_and_the_batch_waits_for_both() {
        let mut m = started(PoisonPolicy::DeadLetter, true);
        let id = meta(0).id;
        m.step(Event::BatchRead(meta(0)));

        assert_eq!(
            m.step(Event::RowsRejected {
                id,
                count: 2,
                error: "bad".into()
            }),
            [Effect::DeadLetterRows(id)]
        );
        m.step(Event::TransformDone(id));
        // The sinks took the other messages; the rejected ones are not confirmed yet.
        let effects = m.step(Event::SinkReceipt { id, sink: REQUIRED });
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::Commit(_))),
            "{effects:?}"
        );
        // A failing dead-letter sink is retried past every sink retry limit.
        for _ in 0..5 {
            let effects = m.step(Event::SinkFailed {
                id,
                sink: SinkId::REJECTED_ROWS,
                retryable: false,
                error: "down".into(),
            });
            assert!(effects.iter().any(|e| matches!(e,
                Effect::Retry { operation: Operation::Write(_, s), .. } if *s == SinkId::REJECTED_ROWS)),
                "{effects:?}");
        }
        let effects = m.step(Event::SinkReceipt {
            id,
            sink: SinkId::REJECTED_ROWS,
        });
        assert!(matches!(&effects[0], Effect::Commit(c) if c[0].position == Position::Offset(9)));
    }

    #[test]
    fn rejected_messages_without_a_dead_letter_sink_stop_the_stream() {
        let mut m = started(PoisonPolicy::DeadLetter, false);
        let id = meta(0).id;
        m.step(Event::BatchRead(meta(0)));

        let effects = m.step(Event::RowsRejected {
            id,
            count: 1,
            error: "ValueError: x".into(),
        });

        assert_eq!(effects[0], Effect::StopReading);
        assert!(
            matches!(&effects[1], Effect::Stop(StopReason::Poison { error, .. })
            if error.contains("1 message(s)") && error.contains("ValueError"))
        );
    }

    #[test]
    fn a_batch_is_transformed_written_and_committed() {
        let mut m = started(PoisonPolicy::Stop, false);
        let id = meta(0).id;
        assert_eq!(m.step(Event::BatchRead(meta(0))), [Effect::Transform(id)]);
        assert_eq!(
            m.step(Event::TransformDone(id)),
            [
                Effect::Write { id, sink: REQUIRED },
                Effect::Write {
                    id,
                    sink: BEST_EFFORT
                }
            ]
        );
        // The best-effort sink is not awaited.
        let effects = m.step(Event::SinkReceipt { id, sink: REQUIRED });
        assert!(
            matches!(&effects[..], [Effect::Commit(c)] if c[0].position == Position::Offset(9))
        );
    }

    #[test]
    fn transform_failures_retry_then_dead_letter_or_stop() {
        let mut m = started(PoisonPolicy::DeadLetter, true);
        let id = meta(0).id;
        m.step(Event::BatchRead(meta(0)));
        let fail = || Event::TransformFailed {
            id,
            kind: FailureKind::User,
            error: "bad row".into(),
        };
        assert!(
            matches!(m.step(fail())[..], [Effect::Retry { after, .. }] if after == Duration::from_millis(100))
        );
        assert!(
            matches!(m.step(fail())[..], [Effect::Retry { after, .. }] if after == Duration::from_millis(200))
        );
        assert!(
            matches!(&m.step(fail())[..], [Effect::DeadLetter { error, .. }] if error.contains("bad row"))
        );
        // Not committed until the dead letter is confirmed.
        assert!(m.step(Event::Tick).is_empty());
        assert!(matches!(
            m.step(Event::DeadLetterReceipt(id))[..],
            [Effect::Commit(_)]
        ));

        let mut m = started(PoisonPolicy::Stop, true);
        m.step(Event::BatchRead(meta(0)));
        m.step(fail());
        m.step(fail());
        let effects = m.step(fail());
        assert!(matches!(
            &effects[..],
            [Effect::StopReading, Effect::Stop(StopReason::Poison { .. })]
        ));
        // A stopped stream does nothing more.
        assert!(m.step(Event::SinkReceipt { id, sink: REQUIRED }).is_empty());
    }

    #[test]
    fn a_sink_outage_retries_without_dead_letters_and_alerts() {
        let mut m = started(PoisonPolicy::DeadLetter, true);
        let id = meta(0).id;
        m.step(Event::BatchRead(meta(0)));
        m.step(Event::TransformDone(id));
        let mut alerts = 0;
        for _ in 0..10 {
            for effect in m.step(Event::SinkFailed {
                id,
                sink: REQUIRED,
                retryable: true,
                error: "connection refused".into(),
            }) {
                match effect {
                    Effect::Retry {
                        operation: Operation::Write(_, REQUIRED),
                        ..
                    } => {}
                    Effect::Alert(_) => alerts += 1,
                    other => panic!("unexpected {other:?}"),
                }
            }
        }
        assert_eq!(alerts, 1);
        assert!(matches!(
            m.step(Event::SinkReceipt { id, sink: REQUIRED })[..],
            [Effect::Commit(_)]
        ));
    }

    #[test]
    fn a_rejecting_required_sink_stops_and_a_best_effort_sink_gives_up() {
        let mut m = started(PoisonPolicy::DeadLetter, true);
        let id = meta(0).id;
        m.step(Event::BatchRead(meta(0)));
        m.step(Event::TransformDone(id));
        for attempt in 1..=3 {
            let effects = m.step(Event::SinkFailed {
                id,
                sink: BEST_EFFORT,
                retryable: true,
                error: "timeout".into(),
            });
            if attempt < 3 {
                assert!(matches!(effects[..], [Effect::Retry { .. }]));
            } else {
                assert!(
                    effects
                        .iter()
                        .any(|e| matches!(e, Effect::Alert(a) if a.contains("skipped")))
                );
            }
        }
        let effects = m.step(Event::SinkFailed {
            id,
            sink: REQUIRED,
            retryable: false,
            error: "column score is missing".into(),
        });
        assert!(matches!(
            &effects[..],
            [
                Effect::StopReading,
                Effect::Stop(StopReason::SinkRejected { .. })
            ]
        ));
    }

    #[test]
    fn a_revoke_waits_for_work_in_flight_then_commits_and_hands_over() {
        let mut m = started(PoisonPolicy::Stop, false);
        m.step(Event::BatchRead(meta(0)));
        m.step(Event::BatchRead(meta(1)));
        m.step(Event::TransformDone(meta(0).id));
        m.step(Event::SinkReceipt {
            id: meta(0).id,
            sink: REQUIRED,
        });

        // Batch 1 is still in flight: the partition stays until it is done.
        assert!(
            m.step(Event::Revoking {
                partition: P,
                epoch: E
            })
            .is_empty()
        );
        // The source read more before it stopped; that is not started.
        assert_eq!(
            m.step(Event::BatchRead(meta(2))),
            [Effect::Cancel(vec![meta(2).id])]
        );
        m.step(Event::TransformDone(meta(1).id));
        let effects = m.step(Event::SinkReceipt {
            id: meta(1).id,
            sink: REQUIRED,
        });

        assert!(matches!(&effects[0], Effect::Commit(c) if c[0].position == Position::Offset(19)));
        assert_eq!(
            effects.last(),
            Some(&Effect::Revoked {
                partition: P,
                epoch: E
            })
        );
        // Late work of the revoked epoch changes nothing.
        assert!(m.step(Event::TransformDone(meta(2).id)).is_empty());
    }

    #[test]
    fn a_revoke_deadline_commits_what_is_done_and_drops_the_rest() {
        let mut m = started(PoisonPolicy::Stop, false);
        m.step(Event::BatchRead(meta(0)));
        m.step(Event::BatchRead(meta(1)));
        m.step(Event::TransformDone(meta(0).id));
        m.step(Event::SinkReceipt {
            id: meta(0).id,
            sink: REQUIRED,
        });
        m.step(Event::Revoking {
            partition: P,
            epoch: E,
        });

        let effects = m.step(Event::RevokeDeadline {
            partition: P,
            epoch: E,
        });

        assert!(matches!(&effects[0], Effect::Commit(c) if c[0].position == Position::Offset(9)));
        assert_eq!(effects[1], Effect::Cancel(vec![meta(1).id]));
        assert_eq!(
            effects[2],
            Effect::Revoked {
                partition: P,
                epoch: E
            }
        );
        // A deadline after the revoke finished changes nothing.
        assert!(
            m.step(Event::RevokeDeadline {
                partition: P,
                epoch: E
            })
            .is_empty()
        );
    }

    #[test]
    fn a_revoke_with_nothing_in_flight_or_of_an_old_epoch_hands_over_at_once() {
        let mut m = started(PoisonPolicy::Stop, false);
        assert_eq!(
            m.step(Event::Revoking {
                partition: P,
                epoch: E
            }),
            [Effect::Revoked {
                partition: P,
                epoch: E
            }]
        );
        // The partition is gone now: a second revoke of the same epoch is stale.
        assert_eq!(
            m.step(Event::Revoking {
                partition: P,
                epoch: E
            }),
            [Effect::Revoked {
                partition: P,
                epoch: E
            }]
        );
    }

    #[test]
    fn shutdown_finishes_after_in_flight_work_or_at_the_deadline() {
        let mut m = started(PoisonPolicy::Stop, false);
        let id = meta(0).id;
        m.step(Event::BatchRead(meta(0)));
        assert_eq!(m.step(Event::Shutdown), [Effect::StopReading]);
        m.step(Event::TransformDone(id));
        let effects = m.step(Event::SinkReceipt { id, sink: REQUIRED });
        let Effect::Commit(commits) = &effects[0] else {
            panic!("{effects:?}")
        };
        let effects = m.step(Event::Committed(commits[0].clone()));
        assert_eq!(effects, [Effect::Finished { unfinished: vec![] }]);

        let mut m = started(PoisonPolicy::Stop, false);
        m.step(Event::BatchRead(meta(0)));
        m.step(Event::Shutdown);
        assert_eq!(
            m.step(Event::DeadlinePassed),
            [Effect::Finished {
                unfinished: vec![id]
            }]
        );
    }

    #[test]
    fn backpressure_pauses_and_resumes_the_partition() {
        let mut cfg = config(PoisonPolicy::Stop, false);
        cfg.tracker.max_in_flight_batches = 2;
        let mut m = RunnerMachine::new(cfg);
        m.step(Event::Assigned {
            partition: P,
            epoch: E,
        });
        let mut paused = false;
        for seq in 0..3 {
            paused |= m
                .step(Event::BatchRead(meta(seq)))
                .contains(&Effect::Pause(P));
        }
        assert!(paused);
        for seq in 0..3 {
            m.step(Event::TransformDone(meta(seq).id));
        }
        let mut commits = Vec::new();
        for seq in 0..3 {
            for effect in m.step(Event::SinkReceipt {
                id: meta(seq).id,
                sink: REQUIRED,
            }) {
                assert_ne!(effect, Effect::Resume(P), "resumed before the commit");
                if let Effect::Commit(c) = effect {
                    commits = c;
                }
            }
        }
        // The partition reads on once the source confirms the acknowledgement.
        assert!(
            m.step(Event::Committed(commits[0].clone()))
                .contains(&Effect::Resume(P))
        );
    }

    // Simulation: a driver performs effects with random outcomes and order; random
    // rebalances interleave. Invariants: a commit is never stale, never passes a batch of its
    // epoch that lacks its required receipt or a confirmed dead letter, and is monotonic per
    // epoch.

    #[derive(Debug, Clone)]
    struct Choice {
        pick: usize,
        outcome: u8,
    }

    fn choice() -> impl Strategy<Value = Choice> {
        (0usize..64, 0u8..100).prop_map(|(pick, outcome)| Choice { pick, outcome })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        #[test]
        fn simulated_streams_keep_the_delivery_invariants(
            choices in proptest::collection::vec(choice(), 50..400),
            dead_letter in proptest::bool::ANY,
        ) {
            let poison = if dead_letter { PoisonPolicy::DeadLetter } else { PoisonPolicy::Stop };
            let mut m = RunnerMachine::new(config(poison, dead_letter));
            let mut epoch = 1u64;
            m.step(Event::Assigned { partition: P, epoch: AssignmentEpoch(epoch) });
            let mut seq = 0u64;
            let mut queue: Vec<Effect> = Vec::new();
            let mut read: BTreeSet<BatchId> = BTreeSet::new();
            let mut done: BTreeSet<BatchId> = BTreeSet::new();
            let mut last_commit: BTreeMap<u64, i64> = BTreeMap::new();
            let mut stopped = false;
            let mut revoking: Option<u64> = None;
            let mut rejected: BTreeSet<BatchId> = BTreeSet::new();
            let mut rows_done: BTreeSet<BatchId> = BTreeSet::new();
            let mut revoked: BTreeSet<u64> = BTreeSet::new();
            for c in &choices {
                let mut events = Vec::new();
                if c.outcome < 15 && !stopped && revoking.is_none() {
                    let id = BatchId { partition: P, epoch: AssignmentEpoch(epoch), seq };
                    read.insert(id);
                    events.push(Event::BatchRead(BatchMeta {
                        id, first: Position::Offset(seq as i64 * 10),
                        last: Position::Offset(seq as i64 * 10 + 9), records: 10, bytes: 100,
                    }));
                    seq += 1;
                } else if c.outcome < 17 && revoking.is_none() {
                    // A revoke stays open while work completes, until a deadline or a new
                    // assignment.
                    revoking = Some(epoch);
                    events.push(Event::Revoking { partition: P, epoch: AssignmentEpoch(epoch) });
                } else if c.outcome < 20 && revoking.is_some() {
                    let old = revoking.take().unwrap();
                    if c.outcome % 2 == 0 {
                        events.push(Event::RevokeDeadline { partition: P, epoch: AssignmentEpoch(old) });
                    }
                    events.push(Event::Assigned { partition: P, epoch: AssignmentEpoch(epoch + 1) });
                } else if !queue.is_empty() {
                    let effect = queue.remove(c.pick % queue.len());
                    let fail = c.outcome % 7 == 0;
                    match effect {
                        Effect::Transform(id) | Effect::Retry { operation: Operation::Transform(id), .. } => {
                            if fail {
                                events.push(Event::TransformFailed { id, kind: FailureKind::Crash, error: "x".into() });
                            } else {
                                if c.outcome % 5 == 0 {
                                    // Some messages fail on their own.
                                    rejected.insert(id);
                                    events.push(Event::RowsRejected { id, count: 1, error: "row".into() });
                                }
                                events.push(Event::TransformDone(id));
                            }
                        }
                        Effect::DeadLetterRows(id) | Effect::Retry { operation: Operation::Write(id, SinkId::REJECTED_ROWS), .. } => {
                            if fail {
                                events.push(Event::SinkFailed { id, sink: SinkId::REJECTED_ROWS, retryable: true, error: "down".into() });
                            } else {
                                rows_done.insert(id);
                                events.push(Event::SinkReceipt { id, sink: SinkId::REJECTED_ROWS });
                            }
                        }
                        Effect::Write { id, sink } | Effect::Retry { operation: Operation::Write(id, sink), .. } => {
                            if fail {
                                events.push(Event::SinkFailed { id, sink, retryable: true, error: "down".into() });
                            } else {
                                if sink == REQUIRED { done.insert(id); }
                                events.push(Event::SinkReceipt { id, sink });
                            }
                        }
                        Effect::DeadLetter { id, .. } | Effect::Retry { operation: Operation::DeadLetter(id), .. } => {
                            if fail {
                                events.push(Event::DeadLetterFailed { id, error: "down".into() });
                            } else {
                                done.insert(id);
                                events.push(Event::DeadLetterReceipt(id));
                            }
                        }
                        Effect::Commit(commits) if !fail => {
                            events.extend(commits.into_iter().map(Event::Committed));
                        }
                        _ => {}
                    }
                } else {
                    events.push(Event::Tick);
                }
                for event in events {
                    if let Event::Assigned { epoch: e, .. } = &event {
                        epoch = e.0;
                    }
                    for effect in m.step(event) {
                        if let Effect::Commit(commits) = &effect {
                            for commit in commits {
                                prop_assert_eq!(commit.epoch.0, epoch, "stale commit");
                                prop_assert!(!revoked.contains(&commit.epoch.0),
                                    "commit of epoch {} after it was handed over", commit.epoch.0);
                                let Position::Offset(offset) = commit.position else { unreachable!() };
                                let through = (offset as u64 - 9) / 10;
                                for id in read.iter().filter(|id| id.epoch == commit.epoch && id.seq <= through) {
                                    prop_assert!(done.contains(id),
                                        "commit through batch {} passes batch {} without a receipt", through, id.seq);
                                    prop_assert!(!rejected.contains(id) || rows_done.contains(id),
                                        "commit through batch {} passes batch {} before its rejected messages were written", through, id.seq);
                                }
                                if let Some(previous) = last_commit.get(&commit.epoch.0) {
                                    prop_assert!(offset >= *previous, "commit moved back");
                                }
                                last_commit.insert(commit.epoch.0, offset);
                            }
                        }
                        if matches!(effect, Effect::Stop(_)) { stopped = true; }
                        if let Effect::Revoked { epoch: e, .. } = &effect {
                            revoked.insert(e.0);
                        }
                        queue.push(effect);
                    }
                }
            }
        }
    }
}
