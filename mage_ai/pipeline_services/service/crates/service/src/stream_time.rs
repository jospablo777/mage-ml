//! Event time in a stream (manifest `time`): each message's event time, from a field of the
//! message or the source's timestamp, judged against its partition's watermark
//! (mage_stream_core::EventClock). With `on_late: dead_letter`, late messages, and messages
//! without an event time, leave the batch before the blocks and every sink, and go to the
//! dead-letter sink with the reason; with `pass` they stay and are counted. Either way none
//! is dropped without a trace.

use mage_service_core::manifest::{EventTime, LatePolicy, TimeFormat};
use mage_stream_core::{EventClock, PartitionId};
use mage_stream_ports::{RecordBatch, RowError, read_envelope};
use serde_json::Value;

pub struct Lateness {
    settings: EventTime,
    clock: EventClock,
}

/// What a batch holds once its late messages are out.
#[derive(Debug)]
pub struct Split {
    /// The on-time messages, when some left the batch; None when all stay.
    pub on_time: Option<RecordBatch>,
    /// For each row of `on_time`, its row in the batch.
    pub kept: Vec<usize>,
    /// The messages that left, by their row in the batch, with the reason.
    pub errors: Vec<RowError>,
    pub late: u64,
    pub untimed: u64,
}

impl Lateness {
    pub fn new(settings: EventTime) -> Lateness {
        let clock = EventClock::new(settings.lateness_ms as i64);
        Lateness { settings, clock }
    }

    pub fn forget(&mut self, partition: PartitionId) {
        self.clock.forget(partition);
    }

    pub fn watermarks(&self) -> std::collections::BTreeMap<u32, i64> {
        self.clock
            .watermarks()
            .into_iter()
            .map(|(p, w)| (p.0, w))
            .collect()
    }

    fn time_of(&self, value: &[u8], timestamp: Option<i64>) -> Option<i64> {
        let Some(field) = &self.settings.field else {
            return timestamp;
        };
        let message: Value = serde_json::from_slice(value).ok()?;
        let found = field
            .split('.')
            .try_fold(&message, |value, part| value.get(part))?;
        parse_time(found, self.settings.format)
    }

    /// Judges the batch's messages, in order, and moves the watermarks.
    pub fn split(&mut self, batch: &RecordBatch) -> Result<Split, String> {
        let messages = read_envelope(batch).map_err(|e| e.to_string())?;
        let times: Vec<Option<i64>> = messages
            .iter()
            .map(|m| self.time_of(&m.value, m.timestamp))
            .collect();
        // Per message, the watermark it was late against.
        let mut late: Vec<Option<i64>> = vec![None; messages.len()];
        // A batch is one partition's; grouped anyway, in order.
        let mut partitions: Vec<u32> = messages.iter().map(|m| m.partition).collect();
        partitions.sort_unstable();
        partitions.dedup();
        for partition in partitions {
            let rows: Vec<usize> = (0..messages.len())
                .filter(|&i| messages[i].partition == partition)
                .collect();
            let judged = self.clock.observe(
                PartitionId(partition),
                &rows.iter().map(|&i| times[i]).collect::<Vec<_>>(),
            );
            for (row, judged) in rows.into_iter().zip(judged) {
                late[row] = judged;
            }
        }
        let mut split = Split {
            on_time: None,
            kept: Vec::new(),
            errors: Vec::new(),
            late: late.iter().filter(|l| l.is_some()).count() as u64,
            untimed: times.iter().filter(|t| t.is_none()).count() as u64,
        };
        if self.settings.on_late == LatePolicy::Pass || (split.late == 0 && split.untimed == 0) {
            return Ok(split);
        }
        let mut keep = Vec::with_capacity(messages.len());
        for (row, message) in messages.iter().enumerate() {
            let reason = match (times[row], late[row]) {
                (None, _) => Some(match &self.settings.field {
                    Some(field) => format!("late: the message has no event time in {field}"),
                    None => "late: the message has no source timestamp for its event time".into(),
                }),
                (Some(time), Some(watermark)) => Some(format!(
                    "late: event time {} is before partition {}'s watermark {}",
                    iso(time),
                    message.partition,
                    iso(watermark)
                )),
                _ => None,
            };
            match reason {
                Some(error) => {
                    split.errors.push(RowError { row, error });
                    keep.push(false);
                }
                None => {
                    split.kept.push(row);
                    keep.push(true);
                }
            }
        }
        let mask = arrow_array::BooleanArray::from(keep);
        split.on_time = Some(
            arrow_select::filter::filter_record_batch(batch, &mask).map_err(|e| e.to_string())?,
        );
        Ok(split)
    }
}

fn iso(millis: i64) -> String {
    chrono::DateTime::from_timestamp_millis(millis)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| millis.to_string())
}

/// An event time in milliseconds from a field's value.
fn parse_time(value: &Value, format: TimeFormat) -> Option<i64> {
    match (format, value) {
        (TimeFormat::EpochMs, Value::Number(n)) => n.as_f64().map(|v| v as i64),
        (TimeFormat::EpochS, Value::Number(n)) => n.as_f64().map(|v| (v * 1000.0) as i64),
        (TimeFormat::EpochMs | TimeFormat::EpochS, Value::String(text)) => text
            .trim()
            .parse::<f64>()
            .ok()
            .and_then(|v| parse_time(&Value::from(v), format)),
        (TimeFormat::Rfc3339, Value::String(text)) => chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|t| t.timestamp_millis())
            .or_else(|| {
                // A time without an offset is UTC.
                chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
                    .ok()
                    .map(|t| t.and_utc().timestamp_millis())
            }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use mage_stream_ports::{Envelope, EnvelopeBuilder};

    use super::*;

    fn batch(values: &[&str]) -> RecordBatch {
        let mut builder = EnvelopeBuilder::new();
        for (i, value) in values.iter().enumerate() {
            builder.push(&Envelope {
                key: None,
                value: value.as_bytes().to_vec(),
                headers: vec![],
                partition: 0,
                position: Some(i as i64),
                timestamp: Some(1_000_000 + i as i64),
            });
        }
        builder.finish().unwrap()
    }

    fn settings(field: Option<&str>, format: TimeFormat, on_late: LatePolicy) -> EventTime {
        EventTime {
            field: field.map(str::to_string),
            format,
            lateness_ms: 1_000,
            on_late,
        }
    }

    #[test]
    fn late_and_untimed_messages_leave_the_batch_with_the_reason() {
        let mut lateness = Lateness::new(settings(
            Some("at"),
            TimeFormat::EpochS,
            LatePolicy::DeadLetter,
        ));
        let split = lateness
            .split(&batch(&[
                r#"{"at": 100}"#,
                r#"{"at": 98.5}"#,
                r#"{"at": "99.5"}"#,
                r#"{"other": 1}"#,
                r#"{"at": 101}"#,
            ]))
            .unwrap();
        assert_eq!((split.late, split.untimed), (1, 1));
        assert_eq!(split.kept, [0, 2, 4]);
        assert_eq!(split.on_time.unwrap().num_rows(), 3);
        let rows: Vec<usize> = split.errors.iter().map(|e| e.row).collect();
        assert_eq!(rows, [1, 3]);
        assert!(
            split.errors[0]
                .error
                .contains("before partition 0's watermark 1970-01-01T00:01:39.000Z"),
            "{}",
            split.errors[0].error
        );
        assert!(split.errors[1].error.contains("no event time in at"));
        assert_eq!(lateness.watermarks()[&0], 100_000);
    }

    #[test]
    fn with_pass_every_message_stays_and_is_counted() {
        let mut lateness = Lateness::new(settings(None, TimeFormat::EpochMs, LatePolicy::Pass));
        lateness.split(&batch(&["a", "b"])).unwrap();
        // The source timestamps were 1,000,000 and 1,000,001: a new batch from an older time.
        let mut older = batch(&["c"]);
        let mut builder = EnvelopeBuilder::new();
        builder.push(&Envelope {
            key: None,
            value: b"c".to_vec(),
            headers: vec![],
            partition: 0,
            position: Some(9),
            timestamp: Some(500),
        });
        older = builder.finish().unwrap_or(older);
        let split = lateness.split(&older).unwrap();
        assert_eq!(
            (split.late, split.on_time.is_none(), split.errors.len()),
            (1, true, 0)
        );
    }

    #[test]
    fn times_are_read_in_their_format() {
        use serde_json::json;
        assert_eq!(
            parse_time(&json!(1_700_000_000_123i64), TimeFormat::EpochMs),
            Some(1_700_000_000_123)
        );
        assert_eq!(
            parse_time(&json!(1_700_000_000.5), TimeFormat::EpochS),
            Some(1_700_000_000_500)
        );
        assert_eq!(
            parse_time(&json!("2026-10-21T10:00:00Z"), TimeFormat::Rfc3339),
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-10-21T10:00:00Z")
                    .unwrap()
                    .timestamp_millis()
            )
        );
        assert_eq!(
            parse_time(&json!("2026-10-21T10:00:00.250"), TimeFormat::Rfc3339),
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-10-21T10:00:00.250Z")
                    .unwrap()
                    .timestamp_millis()
            )
        );
        assert_eq!(parse_time(&json!("soon"), TimeFormat::Rfc3339), None);
        assert_eq!(parse_time(&json!(true), TimeFormat::EpochMs), None);
    }
}
