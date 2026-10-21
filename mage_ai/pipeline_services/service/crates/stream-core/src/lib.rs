//! Stream delivery without I/O: what a stream reads, which batches are in flight, when a
//! source position may be acknowledged, and what the runner does next.
//!
//! The rules (NEXT_RELEASE.md section 4.3, ADR-0029, ADR-0030):
//!
//! - Every assignment of a partition has an epoch; work of an older epoch never commits.
//! - A batch is done when every required sink confirmed it, or when its rows went to the
//!   dead-letter sink and that write was confirmed.
//! - A partition's position advances only past a run of done batches, in order.
//! - Sink outages retry; only rows user code rejected go to dead letters.
//!
//! The tokio driver, the sources, the sinks and the transforms live in other crates and talk
//! to this one through events and effects.

mod ids;
mod runner;
mod time;
mod tracker;
pub mod window;

pub use ids::{AssignmentEpoch, BatchId, BatchMeta, PartitionId, Position, SinkId};
pub use runner::{
    Effect, Event, FailureKind, Operation, PoisonPolicy, RetryPolicy, RunnerConfig, RunnerMachine,
    StopReason,
};
pub use time::EventClock;
pub use tracker::{AckTracker, Admission, Commit, ReceiptOutcome, StaleAssignment, TrackerConfig};
