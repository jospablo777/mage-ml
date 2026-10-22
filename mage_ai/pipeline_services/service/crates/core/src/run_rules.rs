//! The run history's state rules: which changes a run and its block runs may make, seen
//! from one instance. The ledger applies each rule as the guard of a single SQL statement,
//! so two instances never both win a change; its conformance test runs every state through
//! that SQL and compares the outcome with these functions.

use crate::protocol::{BlockStatus, RunStatus};

/// Who holds a run, seen from the instance asking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Holder {
    Me,
    /// Another instance whose lease is current.
    Other,
    /// Another instance whose lease expired: killed, or stalled past its lease.
    Gone,
    /// No owner recorded: a queued run, or a run from before owners were recorded.
    Nobody,
}

impl Holder {
    pub const ALL: [Holder; 4] = [Holder::Me, Holder::Other, Holder::Gone, Holder::Nobody];

    fn alive(self) -> bool {
        matches!(self, Holder::Me | Holder::Other)
    }
}

pub const RUN_STATUSES: [RunStatus; 5] = [
    RunStatus::Queued,
    RunStatus::Running,
    RunStatus::Completed,
    RunStatus::Failed,
    RunStatus::Cancelled,
];

pub const BLOCK_STATUSES: [BlockStatus; 7] = [
    BlockStatus::Pending,
    BlockStatus::Running,
    BlockStatus::Completed,
    BlockStatus::Failed,
    BlockStatus::UpstreamFailed,
    BlockStatus::ConditionFailed,
    BlockStatus::Cancelled,
];

/// What one instance knows when it would start a queued run of a pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admission {
    pub shutting_down: bool,
    pub paused: bool,
    /// The pipeline's runs this instance runs, and how many it may run at once.
    pub pipeline_running: u32,
    pub pipeline_max: u32,
    /// All runs this instance runs, and its limit (MAGE_SERVICE_MAX_RUNS).
    pub service_running: u32,
    pub service_max: u32,
}

/// Why a queued run waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    ShuttingDown,
    Paused,
    PipelineFull,
    ServiceFull,
}

/// Whether the instance may try to claim a queued run: None when it may, and the database
/// then decides across instances ([`may_claim`]); otherwise why the run waits.
pub fn admit(state: Admission) -> Option<Hold> {
    if state.shutting_down {
        Some(Hold::ShuttingDown)
    } else if state.paused {
        Some(Hold::Paused)
    } else if state.pipeline_running >= state.pipeline_max {
        Some(Hold::PipelineFull)
    } else if state.service_running >= state.service_max {
        Some(Hold::ServiceFull)
    } else {
        None
    }
}

/// Claiming the oldest queued run of a pipeline: while fewer than `max` of its runs are
/// running on any instance.
pub fn may_claim(running: u32, max: u32) -> bool {
    running < max
}

/// Finishing a run: a queued run (cancelled before it started), or a running run this
/// instance holds. A finished run stays as it is, and so does another instance's run.
pub fn may_finish(status: RunStatus, holder: Holder) -> bool {
    status == RunStatus::Queued || (status == RunStatus::Running && holder == Holder::Me)
}

/// Asking the instance that runs a run to cancel it.
pub fn may_request_cancel(status: RunStatus) -> bool {
    status == RunStatus::Running
}

/// Recording a block run's start or end: only for a running run this instance holds, so a
/// run that recovery failed or a cancel finished keeps what it says.
pub fn may_record_block(status: RunStatus, holder: Holder) -> bool {
    status == RunStatus::Running && holder == Holder::Me
}

/// A new run with an idempotency key the earlier run used takes the key over when the
/// earlier run failed or was cancelled; otherwise the caller gets the earlier run.
pub fn key_passes_on(earlier: RunStatus) -> bool {
    matches!(earlier, RunStatus::Failed | RunStatus::Cancelled)
}

/// What recovery makes of a run: a running run whose holder is not alive failed.
pub fn recovered_run(status: RunStatus, holder: Holder) -> Option<RunStatus> {
    (status == RunStatus::Running && !holder.alive()).then_some(RunStatus::Failed)
}

/// What recovery makes of a block run of a recovered run: the running block failed, and
/// the blocks that had not started are cancelled.
pub fn recovered_block(status: BlockStatus) -> Option<BlockStatus> {
    match status {
        BlockStatus::Running => Some(BlockStatus::Failed),
        BlockStatus::Pending => Some(BlockStatus::Cancelled),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_finished_run_never_changes() {
        for status in RUN_STATUSES.into_iter().filter(|s| s.finished()) {
            for holder in Holder::ALL {
                assert!(!may_finish(status, holder));
                assert!(!may_record_block(status, holder));
                assert_eq!(recovered_run(status, holder), None);
            }
            assert!(!may_request_cancel(status));
        }
    }

    #[test]
    fn a_running_run_changes_only_through_its_holder_or_recovery() {
        let running = RunStatus::Running;
        assert!(may_finish(running, Holder::Me));
        assert!(may_record_block(running, Holder::Me));
        for holder in [Holder::Other, Holder::Gone, Holder::Nobody] {
            assert!(!may_finish(running, holder));
            assert!(!may_record_block(running, holder));
        }
        assert_eq!(recovered_run(running, Holder::Me), None);
        assert_eq!(recovered_run(running, Holder::Other), None);
        assert_eq!(
            recovered_run(running, Holder::Gone),
            Some(RunStatus::Failed)
        );
        assert_eq!(
            recovered_run(running, Holder::Nobody),
            Some(RunStatus::Failed)
        );
    }

    #[test]
    fn every_recovered_status_is_finished() {
        for status in RUN_STATUSES {
            for holder in Holder::ALL {
                if let Some(after) = recovered_run(status, holder) {
                    assert!(after.finished());
                }
            }
        }
        for status in BLOCK_STATUSES {
            if let Some(after) = recovered_block(status) {
                assert!(!matches!(
                    after,
                    BlockStatus::Pending | BlockStatus::Running
                ));
            }
        }
    }

    #[test]
    fn only_a_failed_or_cancelled_run_gives_up_its_key() {
        let passing: Vec<_> = RUN_STATUSES
            .into_iter()
            .filter(|s| key_passes_on(*s))
            .collect();
        assert_eq!(passing, [RunStatus::Failed, RunStatus::Cancelled]);
    }

    #[test]
    fn admission_names_the_first_reason_a_run_waits() {
        let open = Admission {
            shutting_down: false,
            paused: false,
            pipeline_running: 0,
            pipeline_max: 2,
            service_running: 3,
            service_max: 4,
        };
        assert_eq!(admit(open), None);
        let full = Admission {
            pipeline_running: 2,
            service_running: 4,
            ..open
        };
        assert_eq!(admit(full), Some(Hold::PipelineFull));
        assert_eq!(
            admit(Admission {
                service_running: 4,
                ..open
            }),
            Some(Hold::ServiceFull)
        );
        assert_eq!(
            admit(Admission {
                paused: true,
                ..full
            }),
            Some(Hold::Paused)
        );
        assert_eq!(
            admit(Admission {
                shutting_down: true,
                paused: true,
                ..full
            }),
            Some(Hold::ShuttingDown)
        );
        assert_eq!(
            admit(Admission {
                pipeline_max: 0,
                ..open
            }),
            Some(Hold::PipelineFull)
        );
    }

    #[test]
    fn a_claim_stays_under_the_limit() {
        assert!(may_claim(0, 1));
        assert!(!may_claim(1, 1));
        assert!(!may_claim(0, 0));
    }
}
