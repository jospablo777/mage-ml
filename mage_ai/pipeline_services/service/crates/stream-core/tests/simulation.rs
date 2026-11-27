//! Deterministic simulation of a stream end to end (CANDIDATES_2026-11 #5): a Kafka-like
//! log of two partitions read in batches, transforms and sink writes finishing in any order
//! and failing at times, rebalances, and crashes that start a new runner from the committed
//! offsets. The driver here does what the engine's does with each effect; the sink is
//! transactional under exactly-once (writes visible when their commit is), and immediate
//! otherwise.
//!
//! - At least once: once everything is committed, every message's output is visible.
//! - Exactly once: it is visible once, at every step, through every crash.
//!
//! `STREAM_SIM_SEEDS` runs more seeds; `STREAM_SIM_SEED` replays one, printing each step.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mage_stream_core::{
    AssignmentEpoch, BatchId, BatchMeta, Commit, Effect, Event, FailureKind, Operation,
    PartitionId, PoisonPolicy, Position, RetryPolicy, RunnerConfig, RunnerMachine, SinkId,
    TrackerConfig,
};

const SINK: SinkId = SinkId(0);
const PARTITIONS: u32 = 2;
const MESSAGES: i64 = 120;

/// splitmix64: the same seed, the same schedule.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

fn config(exactly_once: bool) -> RunnerConfig {
    let retry = RetryPolicy {
        retries: 3,
        delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(10),
    };
    RunnerConfig {
        tracker: TrackerConfig {
            required_sinks: vec![SINK],
            max_in_flight_bytes: 1_000_000,
            max_in_flight_batches: 4,
        },
        sinks: vec![SINK],
        transform_retry: retry.clone(),
        sink_retry: retry,
        poison: PoisonPolicy::Stop,
        has_dead_letter: false,
        alert_after: 3,
        exactly_once,
    }
}

/// Work the driver started and that finishes later, in any order.
#[derive(Debug, Clone)]
enum Work {
    Transform(BatchId),
    Write(BatchId),
    /// A source acknowledgement on its way.
    Commit(Vec<Commit>),
}

struct Sim {
    exactly_once: bool,
    machine: RunnerMachine,
    epochs: BTreeMap<PartitionId, AssignmentEpoch>,
    /// The next offset to read in each partition, and the offset the source has committed
    /// (the next one a new reader starts at).
    next_read: BTreeMap<PartitionId, i64>,
    committed: BTreeMap<PartitionId, i64>,
    seq: u64,
    batches: BTreeMap<BatchId, (PartitionId, i64, i64)>,
    work: Vec<Work>,
    paused: BTreeSet<PartitionId>,
    revoking: BTreeSet<PartitionId>,
    reading: bool,
    /// The outputs readers see (read_committed under exactly-once), and those in the open
    /// transaction.
    visible: Vec<(PartitionId, i64)>,
    transaction: Vec<(PartitionId, i64)>,
    restarts: u32,
    log: Vec<String>,
}

impl Sim {
    fn new(exactly_once: bool) -> Sim {
        let partitions: Vec<PartitionId> = (0..PARTITIONS).map(PartitionId).collect();
        let mut sim = Sim {
            exactly_once,
            machine: RunnerMachine::new(config(exactly_once)),
            epochs: BTreeMap::new(),
            next_read: partitions.iter().map(|p| (*p, 0)).collect(),
            committed: partitions.iter().map(|p| (*p, 0)).collect(),
            seq: 0,
            batches: BTreeMap::new(),
            work: Vec::new(),
            paused: BTreeSet::new(),
            revoking: BTreeSet::new(),
            reading: true,
            visible: Vec::new(),
            transaction: Vec::new(),
            restarts: 0,
            log: Vec::new(),
        };
        for partition in partitions {
            sim.assign(partition);
        }
        sim
    }

    fn assign(&mut self, partition: PartitionId) {
        let epoch = AssignmentEpoch(self.epochs.get(&partition).map_or(1, |e| e.0 + 1));
        self.epochs.insert(partition, epoch);
        // A new assignment reads from what the source has committed.
        self.next_read.insert(partition, self.committed[&partition]);
        self.paused.remove(&partition);
        self.revoking.remove(&partition);
        self.event(Event::Assigned { partition, epoch });
    }

    /// The process dies: work in flight is lost, an open transaction aborts, and a new
    /// runner starts from the committed offsets.
    fn crash(&mut self, why: &str) {
        self.log.push(format!("crash: {why}"));
        self.restarts += 1;
        self.transaction.clear();
        self.work.clear();
        self.batches.clear();
        self.reading = true;
        self.machine = RunnerMachine::new(config(self.exactly_once));
        for partition in (0..PARTITIONS).map(PartitionId) {
            self.assign(partition);
        }
    }

    fn event(&mut self, event: Event) {
        self.log.push(format!("event {event:?}"));
        let effects = self.machine.step(event);
        for effect in effects {
            self.effect(effect);
        }
    }

    fn effect(&mut self, effect: Effect) {
        match effect {
            Effect::Transform(id)
            | Effect::Retry {
                operation: Operation::Transform(id),
                ..
            } => self.work.push(Work::Transform(id)),
            Effect::Write { id, .. }
            | Effect::Retry {
                operation: Operation::Write(id, _),
                ..
            } => self.work.push(Work::Write(id)),
            Effect::Commit(commits) => {
                // The driver awaits the source's commit before the next effect, as the
                // engine's does; a commit of an epoch no longer assigned is refused, as a
                // Kafka group refuses one from an old generation. Only the Committed events
                // come later.
                let current: Vec<Commit> = commits
                    .into_iter()
                    .filter(|c| self.epochs.get(&c.partition) == Some(&c.epoch))
                    .collect();
                if current.is_empty() {
                    return;
                }
                if self.exactly_once {
                    // The open transaction commits with these offsets, at once and whole.
                    self.visible.append(&mut self.transaction);
                }
                self.acknowledge(&current);
                self.work.push(Work::Commit(current));
            }
            Effect::Pause(partition) => {
                self.paused.insert(partition);
            }
            Effect::Resume(partition) => {
                self.paused.remove(&partition);
            }
            Effect::Cancel(ids) => self.work.retain(|w| match w {
                Work::Transform(id) | Work::Write(id) => !ids.contains(id),
                Work::Commit(_) => true,
            }),
            Effect::Revoked { partition, .. } => {
                // The partition is handed over, here to this same reader again.
                self.log.push(format!("revoked {partition:?}"));
                self.assign(partition);
            }
            Effect::StopReading => self.reading = false,
            Effect::Stop(reason) => self.crash(&format!("{reason:?}")),
            Effect::Finished { .. } | Effect::Alert(_) | Effect::TimersDone { .. } => {}
            Effect::DeadLetter { .. }
            | Effect::DeadLetterRows(_)
            | Effect::Retry {
                operation: Operation::DeadLetter(_),
                ..
            } => {
                panic!("no dead-letter sink: {effect:?}")
            }
        }
    }

    /// The source records the offsets: a new reader starts after them.
    fn acknowledge(&mut self, commits: &[Commit]) {
        for commit in commits {
            let Position::Offset(last) = commit.position else {
                unreachable!()
            };
            let next = self.committed.get_mut(&commit.partition).unwrap();
            assert!(last + 1 >= *next, "offset moved back: {commit:?}");
            *next = last + 1;
        }
    }

    fn read(&mut self, rng: &mut Rng) -> bool {
        let ready: Vec<PartitionId> = self
            .next_read
            .iter()
            .filter(|(p, next)| {
                **next < MESSAGES && !self.paused.contains(p) && !self.revoking.contains(p)
            })
            .map(|(p, _)| *p)
            .collect();
        if !self.reading || ready.is_empty() {
            return false;
        }
        let partition = ready[rng.below(ready.len())];
        let first = self.next_read[&partition];
        let last = (first + 1 + rng.below(12) as i64).min(MESSAGES) - 1;
        self.next_read.insert(partition, last + 1);
        self.seq += 1;
        let id = BatchId {
            partition,
            epoch: self.epochs[&partition],
            seq: self.seq,
        };
        self.batches.insert(id, (partition, first, last));
        self.event(Event::BatchRead(BatchMeta {
            id,
            first: Position::Offset(first),
            last: Position::Offset(last),
            records: (last - first + 1) as u32,
            bytes: 100,
        }));
        true
    }

    fn finish_work(&mut self, rng: &mut Rng, faults: bool) {
        let work = self.work.remove(rng.below(self.work.len()));
        match work {
            Work::Transform(id) => {
                if faults && rng.chance(8) {
                    self.event(Event::TransformFailed {
                        id,
                        kind: FailureKind::Crash,
                        error: "worker died".into(),
                    });
                } else {
                    self.event(Event::TransformDone(id));
                }
            }
            Work::Write(id) => {
                if faults && rng.chance(5) {
                    self.event(Event::SinkFailed {
                        id,
                        sink: SINK,
                        retryable: true,
                        error: "broker down".into(),
                    });
                    return;
                }
                let Some((partition, first, last)) = self.batches.get(&id).copied() else {
                    return;
                };
                let rows = (first..=last).map(|o| (partition, o));
                if self.exactly_once {
                    self.transaction.extend(rows);
                } else {
                    self.visible.extend(rows);
                }
                self.event(Event::SinkReceipt { id, sink: SINK });
            }
            Work::Commit(commits) => {
                for commit in commits {
                    self.event(Event::Committed(commit));
                }
            }
        }
    }

    fn step(&mut self, rng: &mut Rng, faults: bool) {
        let roll = rng.below(100);
        if faults && roll < 2 {
            self.crash("killed");
        } else if faults && roll < 4 {
            let partition = PartitionId(rng.below(PARTITIONS as usize) as u32);
            if !self.revoking.contains(&partition) {
                self.revoking.insert(partition);
                let epoch = self.epochs[&partition];
                self.event(Event::Revoking { partition, epoch });
            }
        } else if roll < 30 {
            if !self.read(rng) && !self.work.is_empty() {
                self.finish_work(rng, faults);
            }
        } else if roll < 38 {
            self.event(Event::Tick);
        } else if !self.work.is_empty() {
            self.finish_work(rng, faults);
        } else {
            self.read(rng);
        }
    }

    fn all_committed(&self) -> bool {
        self.committed.values().all(|next| *next == MESSAGES)
    }

    fn check(&self) -> Result<(), String> {
        if self.exactly_once {
            let mut seen = BTreeSet::new();
            for row in &self.visible {
                if !seen.insert(row) {
                    return Err(format!("{row:?} is visible twice"));
                }
            }
        }
        Ok(())
    }
}

fn simulate(seed: u64, exactly_once: bool) -> Result<u32, (String, Vec<String>)> {
    let mut rng = Rng(seed ^ if exactly_once { 0xE05 } else { 0 });
    let mut sim = Sim::new(exactly_once);
    for _ in 0..1500 {
        sim.step(&mut rng, true);
        if let Err(error) = sim.check() {
            return Err((error, sim.log));
        }
    }
    // The faults stop: the stream must catch up and commit everything.
    for _ in 0..20_000 {
        if sim.all_committed() {
            break;
        }
        sim.step(&mut rng, false);
        if let Err(error) = sim.check() {
            return Err((error, sim.log));
        }
    }
    if !sim.all_committed() {
        return Err((
            format!("not everything committed: {:?}", sim.committed),
            sim.log,
        ));
    }
    let visible: BTreeSet<_> = sim.visible.iter().copied().collect();
    for partition in (0..PARTITIONS).map(PartitionId) {
        for offset in 0..MESSAGES {
            if !visible.contains(&(partition, offset)) {
                return Err((format!("{partition:?} offset {offset} lost"), sim.log));
            }
        }
    }
    Ok(sim.restarts)
}

fn run(exactly_once: bool) {
    let report = |seed: u64, error: String, log: Vec<String>, all: bool| {
        let shown = if all {
            &log[..]
        } else {
            &log[log.len().saturating_sub(25)..]
        };
        panic!(
            "seed {seed} (exactly_once {exactly_once}): {error}\nlast steps:\n{}\n(replay: \
             STREAM_SIM_SEED={seed})",
            shown.join("\n")
        );
    };
    if let Some(seed) = std::env::var("STREAM_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        if let Err((error, log)) = simulate(seed, exactly_once) {
            report(seed, error, log, true);
        }
        return;
    }
    let seeds: u64 = std::env::var("STREAM_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let mut restarts = 0;
    for seed in 0..seeds {
        match simulate(seed, exactly_once) {
            Ok(n) => restarts += n,
            Err((error, log)) => report(seed, error, log, false),
        }
    }
    // The schedules did crash: a simulation without them proves little.
    assert!(
        restarts as u64 >= seeds,
        "{restarts} restarts in {seeds} seeds"
    );
}

#[test]
fn at_least_once_loses_nothing_through_crashes_rebalances_and_failures() {
    run(false);
}

#[test]
fn exactly_once_shows_each_output_once_through_crashes_rebalances_and_failures() {
    run(true);
}
