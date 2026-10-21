//! Windows over event time: tumbling, hopping and session windows per key, each holding
//! the aggregates a stream asks for (count, sum, min, max, average, distinct count). A window
//! closes when its partition's watermark passes its end; closing it gives its result once.
//! No I/O: the engine feeds events and watermarks and keeps snapshots.
//!
//! Times are milliseconds since the Unix epoch; a window covers [start, end).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum WindowKind {
    Tumbling {
        size_ms: i64,
    },
    /// Windows of `size_ms` starting every `slide_ms`; an event is in each that covers it.
    Hopping {
        size_ms: i64,
        slide_ms: i64,
    },
    /// A key's events no more than `gap_ms` apart, in one window that ends `gap_ms` after
    /// its last event.
    Session {
        gap_ms: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregate {
    Count,
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
    Distinct(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowSpec {
    /// The field that keys the windows (`a.b` for a nested one); none keeps one set of
    /// windows for all events.
    #[serde(default)]
    pub key: Option<String>,
    pub kind: WindowKind,
    /// Output field and its aggregate, in order.
    pub aggregates: Vec<(String, Aggregate)>,
}

impl WindowSpec {
    pub fn check(&self) -> Result<(), String> {
        match self.kind {
            WindowKind::Tumbling { size_ms } if size_ms <= 0 => {
                return Err("a tumbling window's size is above 0".into());
            }
            WindowKind::Hopping { size_ms, slide_ms } if size_ms <= 0 || slide_ms <= 0 => {
                return Err("a hopping window's size and slide are above 0".into());
            }
            WindowKind::Hopping { size_ms, slide_ms } if size_ms % slide_ms != 0 => {
                return Err("a hopping window's size is a whole number of slides".into());
            }
            WindowKind::Session { gap_ms } if gap_ms <= 0 => {
                return Err("a session window's gap is above 0".into());
            }
            _ => {}
        }
        if self.aggregates.is_empty() {
            return Err("a window has at least one aggregate".into());
        }
        let names: BTreeSet<&str> = self.aggregates.iter().map(|(n, _)| n.as_str()).collect();
        if names.len() != self.aggregates.len() {
            return Err("two aggregates have the same name".into());
        }
        for reserved in ["key", "window_start", "window_end"] {
            if names.contains(reserved) {
                return Err(format!("{reserved} is a field of every window's result"));
            }
        }
        Ok(())
    }
}

/// One window's running aggregates; one slot per aggregate of the spec.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Accumulator {
    pub count: u64,
    /// Per aggregate: values seen, their sum, min and max.
    pub seen: Vec<u64>,
    pub sum: Vec<f64>,
    pub min: Vec<Option<f64>>,
    pub max: Vec<Option<f64>>,
    pub distinct: Vec<BTreeSet<String>>,
}

impl Accumulator {
    fn new(slots: usize) -> Accumulator {
        Accumulator {
            count: 0,
            seen: vec![0; slots],
            sum: vec![0.0; slots],
            min: vec![None; slots],
            max: vec![None; slots],
            distinct: vec![BTreeSet::new(); slots],
        }
    }

    fn add(&mut self, spec: &WindowSpec, event: &Value) {
        self.count += 1;
        for (slot, (_, aggregate)) in spec.aggregates.iter().enumerate() {
            match aggregate {
                Aggregate::Count => {}
                Aggregate::Distinct(field) => {
                    if let Some(value) = lookup(event, field).filter(|v| !v.is_null()) {
                        self.distinct[slot].insert(value.to_string());
                    }
                }
                Aggregate::Sum(field)
                | Aggregate::Min(field)
                | Aggregate::Max(field)
                | Aggregate::Avg(field) => {
                    if let Some(number) = lookup(event, field).and_then(number) {
                        self.seen[slot] += 1;
                        self.sum[slot] += number;
                        self.min[slot] = Some(self.min[slot].map_or(number, |m| m.min(number)));
                        self.max[slot] = Some(self.max[slot].map_or(number, |m| m.max(number)));
                    }
                }
            }
        }
    }

    fn merge(&mut self, other: &Accumulator) {
        self.count += other.count;
        for slot in 0..self.seen.len() {
            self.seen[slot] += other.seen[slot];
            self.sum[slot] += other.sum[slot];
            self.min[slot] = match (self.min[slot], other.min[slot]) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            self.max[slot] = match (self.max[slot], other.max[slot]) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
            let theirs = other.distinct[slot].clone();
            self.distinct[slot].extend(theirs);
        }
    }

    fn result(&self, spec: &WindowSpec) -> Map<String, Value> {
        let mut out = Map::new();
        for (slot, (name, aggregate)) in spec.aggregates.iter().enumerate() {
            let value = match aggregate {
                Aggregate::Count => Value::from(self.count),
                Aggregate::Distinct(_) => Value::from(self.distinct[slot].len()),
                Aggregate::Sum(_) => Value::from(self.sum[slot]),
                Aggregate::Min(_) => self.min[slot].map_or(Value::Null, Value::from),
                Aggregate::Max(_) => self.max[slot].map_or(Value::Null, Value::from),
                Aggregate::Avg(_) if self.seen[slot] > 0 => {
                    Value::from(self.sum[slot] / self.seen[slot] as f64)
                }
                Aggregate::Avg(_) => Value::Null,
            };
            out.insert(name.clone(), value);
        }
        out
    }
}

/// A field of a JSON object, by a dotted path.
pub fn lookup<'a>(event: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(event, |value, part| value.get(part))
}

fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

/// A key as text: a string as it is, anything else as its JSON; missing is null.
pub fn key_of(event: &Value, field: Option<&str>) -> String {
    match field.and_then(|f| lookup(event, f)) {
        None => {
            if field.is_some() {
                "null".into()
            } else {
                String::new()
            }
        }
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

/// A closed window's result.
#[derive(Debug, Clone, PartialEq)]
pub struct Closed {
    pub key: String,
    pub start: i64,
    pub end: i64,
    pub fields: Map<String, Value>,
}

/// The open windows of one partition.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Windows {
    /// key -> start -> (end, aggregates).
    open: BTreeMap<String, BTreeMap<i64, (i64, Accumulator)>>,
}

impl Windows {
    pub fn len(&self) -> usize {
        self.open.values().map(BTreeMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.open.is_empty()
    }

    fn add_to(&mut self, spec: &WindowSpec, key: &str, start: i64, end: i64, event: &Value) {
        let slots = spec.aggregates.len();
        let (_, accumulator) = self
            .open
            .entry(key.to_string())
            .or_default()
            .entry(start)
            .or_insert_with(|| (end, Accumulator::new(slots)));
        accumulator.add(spec, event);
    }

    /// Adds an event at `time` to the windows that cover it. A window that already closed
    /// (its end at or before `watermark`) takes nothing: returns false.
    pub fn add(
        &mut self,
        spec: &WindowSpec,
        key: &str,
        time: i64,
        event: &Value,
        watermark: Option<i64>,
    ) -> bool {
        let closed = |end: i64| watermark.is_some_and(|w| end <= w);
        match spec.kind {
            WindowKind::Tumbling { size_ms } => {
                let start = time.div_euclid(size_ms) * size_ms;
                if closed(start + size_ms) {
                    return false;
                }
                self.add_to(spec, key, start, start + size_ms, event);
                true
            }
            WindowKind::Hopping { size_ms, slide_ms } => {
                let mut added = false;
                let mut start = time.div_euclid(slide_ms) * slide_ms;
                while start > time - size_ms {
                    if !closed(start + size_ms) {
                        self.add_to(spec, key, start, start + size_ms, event);
                        added = true;
                    }
                    start -= slide_ms;
                }
                added
            }
            WindowKind::Session { gap_ms } => {
                if closed(time + gap_ms) {
                    return false;
                }
                let mut accumulator = Accumulator::new(spec.aggregates.len());
                accumulator.add(spec, event);
                let (mut start, mut end) = (time, time + gap_ms);
                let sessions = self.open.entry(key.to_string()).or_default();
                // Sessions the event's [time, time + gap) overlaps: within the gap of their
                // events. They merge into one.
                let touching: Vec<i64> = sessions
                    .iter()
                    .filter(|(s, (e, _))| time < *e && **s < time + gap_ms)
                    .map(|(s, _)| *s)
                    .collect();
                for found in touching {
                    let (other_end, other) = sessions.remove(&found).expect("present");
                    start = start.min(found);
                    end = end.max(other_end);
                    accumulator.merge(&other);
                }
                sessions.insert(start, (end, accumulator));
                true
            }
        }
    }

    /// Closes the windows that end at or before the watermark, in end order.
    pub fn close(&mut self, spec: &WindowSpec, watermark: i64) -> Vec<Closed> {
        let mut closed = Vec::new();
        for (key, windows) in self.open.iter_mut() {
            let done: Vec<i64> = windows
                .iter()
                .filter(|(_, (end, _))| *end <= watermark)
                .map(|(start, _)| *start)
                .collect();
            for start in done {
                let (end, accumulator) = windows.remove(&start).expect("present");
                closed.push(Closed {
                    key: key.clone(),
                    start,
                    end,
                    fields: accumulator.result(spec),
                });
            }
        }
        self.open.retain(|_, windows| !windows.is_empty());
        closed.sort_by(|a, b| (a.end, &a.key, a.start).cmp(&(b.end, &b.key, b.start)));
        closed
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    fn spec(kind: WindowKind) -> WindowSpec {
        WindowSpec {
            key: Some("card".into()),
            kind,
            aggregates: vec![
                ("n".into(), Aggregate::Count),
                ("total".into(), Aggregate::Sum("amount".into())),
                ("low".into(), Aggregate::Min("amount".into())),
                ("high".into(), Aggregate::Max("amount".into())),
                ("mean".into(), Aggregate::Avg("amount".into())),
                ("shops".into(), Aggregate::Distinct("shop".into())),
            ],
        }
    }

    #[test]
    fn a_tumbling_window_closes_once_with_its_aggregates() {
        let spec = spec(WindowKind::Tumbling { size_ms: 1_000 });
        let mut windows = Windows::default();
        for (time, amount, shop) in [
            (100, 10.0, "a"),
            (900, 30.0, "b"),
            (999, 20.0, "a"),
            (1_000, 5.0, "a"),
        ] {
            let event = json!({"card": "c1", "amount": amount, "shop": shop});
            assert!(windows.add(&spec, &key_of(&event, Some("card")), time, &event, None));
        }
        assert!(windows.close(&spec, 999).is_empty());
        let [first] = &windows.close(&spec, 1_000)[..] else {
            panic!()
        };
        assert_eq!(
            (first.key.as_str(), first.start, first.end),
            ("c1", 0, 1_000)
        );
        assert_eq!(
            Value::Object(first.fields.clone()),
            json!({"n": 3, "total": 60.0, "low": 10.0, "high": 30.0, "mean": 20.0, "shops": 2})
        );
        assert!(
            windows.close(&spec, 1_000).is_empty(),
            "a window closes once"
        );
        // An event for a closed window is refused.
        let late = json!({"card": "c1", "amount": 1});
        assert!(!windows.add(&spec, "c1", 500, &late, Some(1_000)));
        assert_eq!(windows.len(), 1);
    }

    #[test]
    fn hopping_windows_each_take_the_events_they_cover() {
        let spec = spec(WindowKind::Hopping {
            size_ms: 1_000,
            slide_ms: 500,
        });
        let mut windows = Windows::default();
        let event = json!({"card": "c", "amount": 1});
        windows.add(&spec, "c", 700, &event, None);
        // 700 is in [0, 1000) and [500, 1500).
        let closed = windows.close(&spec, 10_000);
        assert_eq!(
            closed.iter().map(|c| (c.start, c.end)).collect::<Vec<_>>(),
            [(0, 1_000), (500, 1_500)]
        );
    }

    #[test]
    fn sessions_merge_events_within_the_gap_and_split_after_it() {
        let spec = spec(WindowKind::Session { gap_ms: 100 });
        let mut windows = Windows::default();
        let event = json!({"card": "c", "amount": 1});
        for time in [0, 50, 140, 400] {
            windows.add(&spec, "c", time, &event, None);
        }
        // 0, 50 and 140 are one session (each within 100 of the last); 400 is another.
        let closed = windows.close(&spec, 1_000);
        assert_eq!(
            closed
                .iter()
                .map(|c| (c.start, c.end, c.fields["n"].clone()))
                .collect::<Vec<_>>(),
            [(0, 240, json!(3)), (400, 500, json!(1))]
        );
        // An event between two sessions joins them.
        let mut windows = Windows::default();
        for time in [0, 180, 90] {
            windows.add(&spec, "c", time, &event, None);
        }
        let closed = windows.close(&spec, 1_000);
        assert_eq!(
            closed.iter().map(|c| (c.start, c.end)).collect::<Vec<_>>(),
            [(0, 280)]
        );
    }

    #[test]
    fn a_spec_says_what_is_wrong() {
        let mut bad = spec(WindowKind::Hopping {
            size_ms: 1_000,
            slide_ms: 300,
        });
        assert!(bad.check().unwrap_err().contains("whole number of slides"));
        bad.kind = WindowKind::Tumbling { size_ms: 10 };
        bad.aggregates.push(("key".into(), Aggregate::Count));
        assert!(bad.check().unwrap_err().contains("key is a field"));
    }

    proptest! {
        /// Tumbling windows count every event once, in the window of its time.
        #[test]
        fn tumbling_counts_add_up(times in proptest::collection::vec(0i64..100_000, 0..300)) {
            let spec = WindowSpec {
                key: None,
                kind: WindowKind::Tumbling { size_ms: 1_000 },
                aggregates: vec![("n".into(), Aggregate::Count)],
            };
            let mut windows = Windows::default();
            for time in &times {
                windows.add(&spec, "", *time, &json!({}), None);
            }
            let closed = windows.close(&spec, i64::MAX);
            let total: u64 = closed.iter().map(|c| c.fields["n"].as_u64().unwrap()).sum();
            prop_assert_eq!(total, times.len() as u64);
            for window in &closed {
                let expected = times.iter().filter(|t| **t >= window.start && **t < window.end).count();
                prop_assert_eq!(window.fields["n"].as_u64().unwrap(), expected as u64);
            }
        }

        /// Sessions: every event is in exactly one, and sessions of a key are more than
        /// the gap apart.
        #[test]
        fn sessions_partition_the_events(times in proptest::collection::vec(0i64..10_000, 1..100)) {
            let gap = 100;
            let spec = WindowSpec {
                key: None,
                kind: WindowKind::Session { gap_ms: gap },
                aggregates: vec![("n".into(), Aggregate::Count)],
            };
            let mut windows = Windows::default();
            for time in &times {
                windows.add(&spec, "", *time, &json!({}), None);
            }
            let closed = windows.close(&spec, i64::MAX);
            let total: u64 = closed.iter().map(|c| c.fields["n"].as_u64().unwrap()).sum();
            prop_assert_eq!(total, times.len() as u64);
            let mut sorted = times.clone();
            sorted.sort_unstable();
            let mut expected = 1;
            for pair in sorted.windows(2) {
                if pair[1] - pair[0] >= gap {
                    expected += 1;
                }
            }
            prop_assert_eq!(closed.len(), expected);
        }
    }
}
