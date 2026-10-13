//! Which source positions may be acknowledged.
//!
//! A partition's batches stay in flight, in read order, until each is done: every required
//! sink confirmed it, or its rows went to the dead-letter sink and that write was confirmed.
//! The position to acknowledge is the last position of the longest run of done batches at
//! the front. A restart reads again from there, so a crash repeats at most the batches that
//! were in flight.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::ids::{AssignmentEpoch, BatchId, BatchMeta, PartitionId, Position, SinkId};

#[derive(Debug, Clone)]
pub struct TrackerConfig {
    /// Sinks whose receipt a batch needs before its positions count. Best-effort sinks are
    /// not listed.
    pub required_sinks: Vec<SinkId>,
    /// In-flight bytes of one partition above which the source should pause it.
    pub max_in_flight_bytes: u64,
    /// In-flight batches of one partition above which the source should pause it.
    pub max_in_flight_batches: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// The batch is tracked; `pause` asks the source to stop reading this partition until
    /// [`AckTracker::can_resume`] says so.
    Admitted { pause: bool },
    /// The batch belongs to an epoch that is no longer assigned; drop it.
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptOutcome {
    Recorded,
    /// The sink had already confirmed this batch (a retried write).
    Duplicate,
    /// The batch belongs to a revoked or replaced epoch; nothing changes.
    Stale,
    /// No batch with this id is in flight, and its epoch is current: it was already done.
    Unknown,
}

/// A position the source may acknowledge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub partition: PartitionId,
    pub epoch: AssignmentEpoch,
    pub position: Position,
}

#[derive(Debug)]
struct InFlight {
    meta: BatchMeta,
    pending: BTreeSet<SinkId>,
    dead_lettered: bool,
}

impl InFlight {
    fn done(&self) -> bool {
        self.dead_lettered || self.pending.is_empty()
    }
}

#[derive(Debug)]
struct PartitionState {
    epoch: AssignmentEpoch,
    last_seq: Option<u64>,
    in_flight: VecDeque<InFlight>,
    bytes: u64,
    /// The last position of the run of done batches already removed from the front.
    done_through: Option<Position>,
    /// `done_through` advanced and the source has not confirmed acknowledging it.
    unconfirmed: bool,
}

#[derive(Debug)]
pub struct AckTracker {
    config: TrackerConfig,
    partitions: BTreeMap<PartitionId, PartitionState>,
    /// The highest epoch ever assigned per partition, kept after a revoke: an assignment
    /// must move forward, so a source that reused an epoch could not commit stale work.
    highest_epoch: BTreeMap<PartitionId, AssignmentEpoch>,
}

/// An assignment whose epoch is not above every earlier epoch of its partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleAssignment {
    pub partition: PartitionId,
    pub epoch: AssignmentEpoch,
    pub highest: AssignmentEpoch,
}

impl AckTracker {
    pub fn new(config: TrackerConfig) -> AckTracker {
        AckTracker {
            config,
            partitions: BTreeMap::new(),
            highest_epoch: BTreeMap::new(),
        }
    }

    /// A partition is assigned in a new epoch; the batches of its previous epoch are dropped
    /// and returned, so their work can be cancelled. An epoch that does not move forward is
    /// refused.
    pub fn assign(
        &mut self,
        partition: PartitionId,
        epoch: AssignmentEpoch,
    ) -> Result<Vec<BatchId>, StaleAssignment> {
        if let Some(&highest) = self.highest_epoch.get(&partition)
            && epoch <= highest
        {
            return Err(StaleAssignment {
                partition,
                epoch,
                highest,
            });
        }
        self.highest_epoch.insert(partition, epoch);
        let dropped = self
            .partitions
            .remove(&partition)
            .map(|state| state.in_flight.iter().map(|b| b.meta.id).collect())
            .unwrap_or_default();
        self.partitions.insert(
            partition,
            PartitionState {
                epoch,
                last_seq: None,
                in_flight: VecDeque::new(),
                bytes: 0,
                done_through: None,
                unconfirmed: false,
            },
        );
        Ok(dropped)
    }

    /// The partition is revoked; returns the batches that were not done. A revoke of an
    /// epoch that is no longer current changes nothing.
    pub fn revoke(&mut self, partition: PartitionId, epoch: AssignmentEpoch) -> Vec<BatchId> {
        match self.partitions.get(&partition) {
            Some(state) if state.epoch == epoch => {}
            _ => return Vec::new(),
        }
        let state = self.partitions.remove(&partition).expect("checked above");
        state
            .in_flight
            .iter()
            .filter(|b| !b.done())
            .map(|b| b.meta.id)
            .collect()
    }

    pub fn admit(&mut self, meta: BatchMeta) -> Admission {
        let Some(state) = self.partitions.get_mut(&meta.id.partition) else {
            return Admission::Stale;
        };
        if state.epoch != meta.id.epoch || state.last_seq.is_some_and(|s| meta.id.seq <= s) {
            return Admission::Stale;
        }
        state.last_seq = Some(meta.id.seq);
        state.bytes += meta.bytes;
        let pending = self.config.required_sinks.iter().copied().collect();
        state.in_flight.push_back(InFlight {
            meta,
            pending,
            dead_lettered: false,
        });
        Admission::Admitted {
            pause: state.bytes > self.config.max_in_flight_bytes
                || state.in_flight.len() > self.config.max_in_flight_batches,
        }
    }

    fn batch_mut(&mut self, id: BatchId) -> Result<&mut InFlight, ReceiptOutcome> {
        let state = match self.partitions.get_mut(&id.partition) {
            Some(state) if state.epoch == id.epoch => state,
            _ => return Err(ReceiptOutcome::Stale),
        };
        state
            .in_flight
            .iter_mut()
            .find(|b| b.meta.id == id)
            .ok_or(ReceiptOutcome::Unknown)
    }

    /// A sink confirmed a batch.
    pub fn on_receipt(&mut self, id: BatchId, sink: SinkId) -> ReceiptOutcome {
        match self.batch_mut(id) {
            Ok(batch) => {
                if batch.pending.remove(&sink) {
                    ReceiptOutcome::Recorded
                } else {
                    ReceiptOutcome::Duplicate
                }
            }
            Err(outcome) => outcome,
        }
    }

    /// The batch's rejected rows reached the dead-letter sink, which confirmed the write.
    pub fn on_dead_lettered(&mut self, id: BatchId) -> ReceiptOutcome {
        match self.batch_mut(id) {
            Ok(batch) if !batch.dead_lettered => {
                batch.dead_lettered = true;
                ReceiptOutcome::Recorded
            }
            Ok(_) => ReceiptOutcome::Duplicate,
            Err(outcome) => outcome,
        }
    }

    /// The positions the source may acknowledge now: per partition, the last position of
    /// the run of done batches at the front. Repeated until [`AckTracker::confirm`].
    pub fn committable(&mut self) -> Vec<Commit> {
        let mut commits = Vec::new();
        for (partition, state) in &mut self.partitions {
            while state.in_flight.front().is_some_and(InFlight::done) {
                let batch = state.in_flight.pop_front().expect("checked above");
                state.bytes -= batch.meta.bytes;
                state.done_through = Some(batch.meta.last);
                state.unconfirmed = true;
            }
            if state.unconfirmed
                && let Some(position) = &state.done_through
            {
                commits.push(Commit {
                    partition: *partition,
                    epoch: state.epoch,
                    position: position.clone(),
                });
            }
        }
        commits
    }

    /// The source acknowledged a commit. False when the commit is stale or superseded.
    pub fn confirm(&mut self, commit: &Commit) -> bool {
        match self.partitions.get_mut(&commit.partition) {
            Some(state)
                if state.epoch == commit.epoch
                    && state.done_through.as_ref() == Some(&commit.position) =>
            {
                state.unconfirmed = false;
                true
            }
            _ => false,
        }
    }

    /// Whether a paused partition is back under half its limits.
    pub fn can_resume(&self, partition: PartitionId) -> bool {
        self.partitions.get(&partition).is_some_and(|state| {
            state.bytes <= self.config.max_in_flight_bytes / 2
                && state.in_flight.len() <= self.config.max_in_flight_batches / 2
        })
    }

    pub fn epoch(&self, partition: PartitionId) -> Option<AssignmentEpoch> {
        self.partitions.get(&partition).map(|s| s.epoch)
    }

    /// Batches not yet done, in read order per partition.
    pub fn in_flight(&self) -> Vec<BatchId> {
        self.partitions
            .values()
            .flat_map(|s| s.in_flight.iter().filter(|b| !b.done()).map(|b| b.meta.id))
            .collect()
    }

    pub fn in_flight_bytes(&self, partition: PartitionId) -> u64 {
        self.partitions.get(&partition).map_or(0, |s| s.bytes)
    }

    /// The sinks that have not confirmed a batch yet.
    pub fn pending_sinks(&self, id: BatchId) -> Vec<SinkId> {
        self.partitions
            .get(&id.partition)
            .filter(|s| s.epoch == id.epoch)
            .and_then(|s| s.in_flight.iter().find(|b| b.meta.id == id))
            .map(|b| b.pending.iter().copied().collect())
            .unwrap_or_default()
    }

    /// True when nothing is in flight and every done position was confirmed.
    pub fn is_settled(&self) -> bool {
        self.partitions
            .values()
            .all(|s| s.in_flight.is_empty() && !s.unconfirmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const P: PartitionId = PartitionId(0);
    const E1: AssignmentEpoch = AssignmentEpoch(1);
    const E2: AssignmentEpoch = AssignmentEpoch(2);
    const A: SinkId = SinkId(0);
    const B: SinkId = SinkId(1);

    fn tracker(sinks: &[SinkId]) -> AckTracker {
        AckTracker::new(TrackerConfig {
            required_sinks: sinks.to_vec(),
            max_in_flight_bytes: 1000,
            max_in_flight_batches: 4,
        })
    }

    fn meta(partition: PartitionId, epoch: AssignmentEpoch, seq: u64) -> BatchMeta {
        let first = (seq * 10) as i64;
        BatchMeta {
            id: BatchId {
                partition,
                epoch,
                seq,
            },
            first: Position::Offset(first),
            last: Position::Offset(first + 9),
            records: 10,
            bytes: 100,
        }
    }

    fn offset(commits: &[Commit]) -> Option<i64> {
        match commits.first().map(|c| &c.position) {
            Some(Position::Offset(o)) => Some(*o),
            _ => None,
        }
    }

    #[test]
    fn a_position_advances_only_past_batches_every_required_sink_confirmed() {
        let mut t = tracker(&[A, B]);
        t.assign(P, E1).unwrap();
        for seq in 0..3 {
            t.admit(meta(P, E1, seq));
        }
        let id = |seq| meta(P, E1, seq).id;

        t.on_receipt(id(1), A);
        t.on_receipt(id(1), B);
        assert!(t.committable().is_empty(), "batch 0 is not done");

        t.on_receipt(id(0), A);
        assert!(
            t.committable().is_empty(),
            "sink B has not confirmed batch 0"
        );

        t.on_receipt(id(0), B);
        let commits = t.committable();
        assert_eq!(offset(&commits), Some(19), "batches 0 and 1 are done");
        assert!(t.confirm(&commits[0]));
        assert!(t.committable().is_empty());
    }

    #[test]
    fn a_receipt_of_a_replaced_epoch_changes_nothing() {
        let mut t = tracker(&[A]);
        t.assign(P, E1).unwrap();
        t.admit(meta(P, E1, 0));
        let dropped = t.assign(P, E2).unwrap();
        assert_eq!(dropped, vec![meta(P, E1, 0).id]);

        assert_eq!(t.on_receipt(meta(P, E1, 0).id, A), ReceiptOutcome::Stale);
        assert!(t.committable().is_empty());
        assert_eq!(t.admit(meta(P, E1, 1)), Admission::Stale);
    }

    #[test]
    fn an_epoch_must_move_forward_even_after_a_revoke() {
        let mut t = tracker(&[A]);
        t.assign(P, E2).unwrap();
        t.revoke(P, E2);

        assert!(t.assign(P, E2).is_err());
        assert!(t.assign(P, E1).is_err());
        assert!(t.assign(P, AssignmentEpoch(3)).is_ok());
    }

    #[test]
    fn a_dead_letter_counts_only_once_confirmed() {
        let mut t = tracker(&[A]);
        t.assign(P, E1).unwrap();
        t.admit(meta(P, E1, 0));
        assert!(t.committable().is_empty());

        assert_eq!(
            t.on_dead_lettered(meta(P, E1, 0).id),
            ReceiptOutcome::Recorded
        );

        assert_eq!(offset(&t.committable()), Some(9));
    }

    #[test]
    fn the_source_pauses_over_its_limits_and_resumes_under_half() {
        let mut t = tracker(&[A]);
        t.assign(P, E1).unwrap();
        let mut pauses = Vec::new();
        for seq in 0..6 {
            if let Admission::Admitted { pause } = t.admit(meta(P, E1, seq)) {
                pauses.push(pause);
            }
        }
        assert_eq!(pauses, [false, false, false, false, true, true]);
        for seq in 0..4 {
            t.on_receipt(meta(P, E1, seq).id, A);
        }
        t.committable();
        assert!(t.can_resume(P));
    }

    #[test]
    fn a_revoke_returns_unfinished_batches_and_a_stale_revoke_is_ignored() {
        let mut t = tracker(&[A]);
        t.assign(P, E2).unwrap();
        t.admit(meta(P, E2, 0));
        t.admit(meta(P, E2, 1));
        t.on_receipt(meta(P, E2, 0).id, A);

        assert!(t.revoke(P, E1).is_empty());
        assert_eq!(t.revoke(P, E2), vec![meta(P, E2, 1).id]);
        assert_eq!(t.epoch(P), None);
    }

    // Property tests: random interleavings of admissions, receipts, dead letters,
    // reassignments, revokes, commits and confirmations.

    #[derive(Debug, Clone)]
    enum Op {
        Admit(u32),
        Receipt(u32, u64, u16),
        DeadLetter(u32, u64),
        Reassign(u32),
        Revoke(u32),
        Commit,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (0u32..3).prop_map(Op::Admit),
            6 => (0u32..3, 0u64..12, 0u16..2).prop_map(|(p, s, k)| Op::Receipt(p, s, k)),
            1 => (0u32..3, 0u64..12).prop_map(|(p, s)| Op::DeadLetter(p, s)),
            1 => (0u32..3).prop_map(Op::Reassign),
            1 => (0u32..3).prop_map(Op::Revoke),
            3 => Just(Op::Commit),
        ]
    }

    /// A model of what may be committed: per partition and epoch, the batches admitted and
    /// which of them are done, written independently of the tracker.
    /// (sinks that have not confirmed, dead lettered)
    type ModelBatch = (BTreeSet<u16>, bool);

    #[derive(Default)]
    struct Model {
        epochs: BTreeMap<u32, u64>,
        highest: BTreeMap<u32, u64>,
        next_seq: BTreeMap<(u32, u64), u64>,
        /// (partition, epoch) -> seq -> (pending sinks, dead lettered)
        batches: BTreeMap<(u32, u64), BTreeMap<u64, ModelBatch>>,
    }

    impl Model {
        /// The highest seq whose batch and all earlier batches of the epoch are done.
        fn done_prefix(&self, partition: u32, epoch: u64) -> Option<u64> {
            let mut last = None;
            for (seq, (pending, dead)) in self.batches.get(&(partition, epoch))? {
                if pending.is_empty() || *dead {
                    last = Some(*seq);
                } else {
                    break;
                }
            }
            last
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        #[test]
        fn commits_are_monotonic_never_pass_unfinished_work_and_never_stale(
            ops in proptest::collection::vec(op(), 1..200)
        ) {
            let sinks = [A, B];
            let mut t = tracker(&sinks);
            let mut m = Model::default();
            let mut last_commit: BTreeMap<(u32, u64), i64> = BTreeMap::new();
            for p in 0..3 {
                t.assign(PartitionId(p), AssignmentEpoch(1)).unwrap();
                m.epochs.insert(p, 1);
                m.highest.insert(p, 1);
            }
            for op in ops {
                match op {
                    Op::Admit(p) => {
                        let Some(&epoch) = m.epochs.get(&p) else { continue };
                        let seq = m.next_seq.entry((p, epoch)).or_insert(0);
                        let admitted = t.admit(meta(PartitionId(p), AssignmentEpoch(epoch), *seq));
                        let is_admitted = matches!(admitted, Admission::Admitted { .. });
                        prop_assert!(is_admitted, "batch not admitted");
                        m.batches.entry((p, epoch)).or_default()
                            .insert(*seq, (sinks.iter().map(|s| s.0).collect(), false));
                        *seq += 1;
                    }
                    Op::Receipt(p, seq, sink) => {
                        // Receipts may name any epoch, current or old.
                        for epoch in 1..=m.epochs.get(&p).copied().unwrap_or(3) {
                            let id = BatchId { partition: PartitionId(p), epoch: AssignmentEpoch(epoch), seq };
                            let outcome = t.on_receipt(id, SinkId(sink));
                            let current = m.epochs.get(&p) == Some(&epoch);
                            if !current {
                                prop_assert_eq!(outcome, ReceiptOutcome::Stale);
                            } else if let Some((pending, _)) = m.batches
                                .get_mut(&(p, epoch)).and_then(|b| b.get_mut(&seq)) {
                                pending.remove(&sink);
                            }
                        }
                    }
                    Op::DeadLetter(p, seq) => {
                        let Some(&epoch) = m.epochs.get(&p) else { continue };
                        let id = BatchId { partition: PartitionId(p), epoch: AssignmentEpoch(epoch), seq };
                        t.on_dead_lettered(id);
                        if let Some(batch) = m.batches.get_mut(&(p, epoch)).and_then(|b| b.get_mut(&seq)) {
                            batch.1 = true;
                        }
                    }
                    Op::Reassign(p) => {
                        let highest = m.highest.get(&p).copied().unwrap_or(0);
                        // An old epoch is refused; the next one is accepted.
                        prop_assert!(t.assign(PartitionId(p), AssignmentEpoch(highest)).is_err());
                        let epoch = highest + 1;
                        prop_assert!(t.assign(PartitionId(p), AssignmentEpoch(epoch)).is_ok());
                        m.epochs.insert(p, epoch);
                        m.highest.insert(p, epoch);
                    }
                    Op::Revoke(p) => {
                        if let Some(epoch) = m.epochs.remove(&p) {
                            t.revoke(PartitionId(p), AssignmentEpoch(epoch));
                        }
                    }
                    Op::Commit => {
                        for commit in t.committable() {
                            let p = commit.partition.0;
                            let epoch = commit.epoch.0;
                            // Never stale.
                            prop_assert_eq!(m.epochs.get(&p), Some(&epoch));
                            let Position::Offset(offset) = commit.position else { unreachable!() };
                            let seq = (offset as u64 - 9) / 10;
                            // Never past unfinished work: every batch up to it is done.
                            let prefix = m.done_prefix(p, epoch);
                            prop_assert!(prefix.is_some_and(|d| seq <= d),
                                "commit of batch {} with done prefix {:?}", seq, prefix);
                            // Monotonic per (partition, epoch).
                            if let Some(previous) = last_commit.get(&(p, epoch)) {
                                prop_assert!(offset >= *previous);
                            }
                            last_commit.insert((p, epoch), offset);
                            prop_assert!(t.confirm(&commit));
                        }
                    }
                }
            }
        }
    }
}
