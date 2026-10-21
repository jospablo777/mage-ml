//! Event time: when a message's event happened, not when the stream read it. Each partition
//! has a watermark, the latest event time it has seen less the allowed lateness: the stream
//! takes it that no event before the watermark is still to come, and a message older than
//! it is late. Watermarks are partition-local, as streams are (ADR-0037): a partition that
//! moves to another instance starts its watermark again there.
//!
//! Times are milliseconds since the Unix epoch.

use std::collections::BTreeMap;

use crate::ids::PartitionId;

#[derive(Debug, Clone, Default)]
pub struct EventClock {
    /// How far behind the latest event time an event may arrive and still be on time.
    lateness_ms: i64,
    /// Per partition, the latest event time seen.
    latest: BTreeMap<PartitionId, i64>,
}

impl EventClock {
    pub fn new(lateness_ms: i64) -> EventClock {
        EventClock {
            lateness_ms: lateness_ms.max(0),
            latest: BTreeMap::new(),
        }
    }

    /// The partition's watermark; None before its first event.
    pub fn watermark(&self, partition: PartitionId) -> Option<i64> {
        self.latest
            .get(&partition)
            .map(|latest| latest.saturating_sub(self.lateness_ms))
    }

    /// Whether an event at `time` arrives after the partition's watermark passed it.
    pub fn is_late(&self, partition: PartitionId, time: i64) -> bool {
        self.watermark(partition)
            .is_some_and(|watermark| time < watermark)
    }

    /// Takes the events of a batch, in order: each is judged against the watermark as it
    /// stands when the event arrives, then moves it. Returns, per event, the watermark it was
    /// judged against when it was late, None when it was on time. A late event never moves
    /// the watermark back.
    pub fn observe(&mut self, partition: PartitionId, times: &[Option<i64>]) -> Vec<Option<i64>> {
        times
            .iter()
            .map(|time| match time {
                Some(time) => {
                    let late = self
                        .watermark(partition)
                        .filter(|watermark| *time < *watermark);
                    let latest = self.latest.entry(partition).or_insert(*time);
                    if *time > *latest {
                        *latest = *time;
                    }
                    late
                }
                None => None,
            })
            .collect()
    }

    /// Forgets a partition this instance no longer reads.
    pub fn forget(&mut self, partition: PartitionId) {
        self.latest.remove(&partition);
    }

    /// Each partition's watermark.
    pub fn watermarks(&self) -> BTreeMap<PartitionId, i64> {
        self.latest
            .keys()
            .filter_map(|p| Some((*p, self.watermark(*p)?)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const P: PartitionId = PartitionId(0);

    #[test]
    fn an_event_older_than_the_lateness_allows_is_late() {
        let mut clock = EventClock::new(1_000);
        assert_eq!(clock.watermark(P), None);
        // 5,000 moves the watermark to 4,000: 4,500 is on time, 3,999 is late.
        assert_eq!(
            clock.observe(P, &[Some(5_000), Some(4_500), Some(3_999), Some(6_000)]),
            [None, None, Some(4_000), None]
        );
        assert_eq!(clock.watermark(P), Some(5_000));
        // A partition has its own watermark.
        assert_eq!(clock.observe(PartitionId(1), &[Some(10)]), [None]);
        assert_eq!(clock.watermarks().len(), 2);
        clock.forget(PartitionId(1));
        assert_eq!(clock.watermark(PartitionId(1)), None);
        // An event without a time is never late and moves nothing.
        assert_eq!(clock.observe(P, &[None]), [None]);
        assert_eq!(clock.watermark(P), Some(5_000));
    }

    proptest! {
        /// The watermark never goes back, and an event is late exactly when it is older
        /// than the latest event before it, less the lateness.
        #[test]
        fn watermarks_only_move_forward(
            lateness in 0i64..10_000,
            times in proptest::collection::vec(proptest::option::of(0i64..1_000_000), 0..200),
        ) {
            let mut clock = EventClock::new(lateness);
            let mut latest: Option<i64> = None;
            let mut last_watermark: Option<i64> = None;
            for time in &times {
                let [judged] = clock.observe(P, &[*time])[..] else { unreachable!() };
                let late = judged.is_some();
                let expected = match (time, latest) {
                    (Some(t), Some(l)) => *t < l - lateness,
                    _ => false,
                };
                prop_assert_eq!(late, expected);
                if let Some(t) = time {
                    latest = Some(latest.map_or(*t, |l| l.max(*t)));
                }
                let watermark = clock.watermark(P);
                prop_assert!(watermark >= last_watermark);
                last_watermark = watermark;
            }
        }
    }
}
