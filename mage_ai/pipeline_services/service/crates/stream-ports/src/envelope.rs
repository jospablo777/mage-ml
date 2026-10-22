//! The envelope every stream batch has, whatever the source (ADR-0019):
//!
//! | column | type | |
//! | --- | --- | --- |
//! | `key` | binary, nullable | the message key |
//! | `value` | binary | the payload, decoded by the blocks or a declared format |
//! | `headers` | list of struct (key utf8, value binary) | ordered, duplicates kept, as Kafka has them |
//! | `partition` | uint32 | an index into the stream's partition table |
//! | `position` | int64, nullable | offset, LSN or tag of the message in its partition |
//! | `timestamp` | int64, nullable | milliseconds since the epoch, from the source |
//!
//! With the stream's name, (`partition`, `position`) is the dedupe id sinks use to absorb a
//! batch written twice after a crash.

use std::sync::{Arc, OnceLock};

use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    UInt32Builder,
};
use arrow_array::{
    Array, BinaryArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray, UInt32Array,
};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Vec<u8>,
}

/// One message of a batch, read out of the envelope.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Envelope {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<Header>,
    pub partition: u32,
    pub position: Option<i64>,
    pub timestamp: Option<i64>,
}

/// The header of the one envelope of a timer batch: no message, only the partition whose
/// due timers fire (see mage_stream_core::BatchId::is_timer).
pub const TIMER_TICK_HEADER: &str = "mage-timer-tick";

/// The envelope of a timer batch for a partition.
pub fn timer_tick(partition: u32) -> Envelope {
    Envelope {
        headers: vec![Header {
            key: TIMER_TICK_HEADER.to_string(),
            value: Vec::new(),
        }],
        partition,
        ..Envelope::default()
    }
}

/// Whether an envelope is a timer batch's, not a message.
pub fn is_timer_tick(envelope: &Envelope) -> bool {
    envelope.position.is_none()
        && envelope.value.is_empty()
        && envelope.headers.len() == 1
        && envelope.headers[0].key == TIMER_TICK_HEADER
}

fn header_fields() -> Fields {
    Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Binary, false),
    ])
}

fn header_item() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Struct(header_fields()), true))
}

/// The envelope's schema, shared by every batch.
pub fn envelope_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Binary, true),
                Field::new("value", DataType::Binary, false),
                Field::new("headers", DataType::List(header_item()), false),
                Field::new("partition", DataType::UInt32, false),
                Field::new("position", DataType::Int64, true),
                Field::new("timestamp", DataType::Int64, true),
            ]))
        })
        .clone()
}

/// Builds a batch message by message.
pub struct EnvelopeBuilder {
    keys: BinaryBuilder,
    values: BinaryBuilder,
    headers: ListBuilder<StructBuilder>,
    partitions: UInt32Builder,
    positions: Int64Builder,
    timestamps: Int64Builder,
    bytes: usize,
}

impl Default for EnvelopeBuilder {
    fn default() -> Self {
        EnvelopeBuilder::new()
    }
}

impl EnvelopeBuilder {
    pub fn new() -> EnvelopeBuilder {
        let header = StructBuilder::new(
            header_fields(),
            vec![
                Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                Box::new(BinaryBuilder::new()),
            ],
        );
        EnvelopeBuilder {
            keys: BinaryBuilder::new(),
            values: BinaryBuilder::new(),
            headers: ListBuilder::new(header).with_field(header_item()),
            partitions: UInt32Builder::new(),
            positions: Int64Builder::new(),
            timestamps: Int64Builder::new(),
            bytes: 0,
        }
    }

    pub fn push(&mut self, message: &Envelope) {
        self.keys.append_option(message.key.as_deref());
        self.values.append_value(&message.value);
        let header = self.headers.values();
        for h in &message.headers {
            header
                .field_builder::<StringBuilder>(0)
                .expect("key builder")
                .append_value(&h.key);
            header
                .field_builder::<BinaryBuilder>(1)
                .expect("value builder")
                .append_value(&h.value);
            header.append(true);
        }
        self.headers.append(true);
        self.partitions.append_value(message.partition);
        self.positions.append_option(message.position);
        self.timestamps.append_option(message.timestamp);
        self.bytes += message.value.len()
            + message.key.as_ref().map_or(0, Vec::len)
            + message
                .headers
                .iter()
                .map(|h| h.key.len() + h.value.len())
                .sum::<usize>();
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Payload bytes pushed so far, for batch limits.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        self.bytes = 0;
        RecordBatch::try_new(
            envelope_schema(),
            vec![
                Arc::new(self.keys.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.headers.finish()),
                Arc::new(self.partitions.finish()),
                Arc::new(self.positions.finish()),
                Arc::new(self.timestamps.finish()),
            ],
        )
    }
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, ArrowError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            ArrowError::SchemaError(format!("the batch has no {name} column of the envelope"))
        })
}

/// The messages of an envelope batch.
pub fn read_envelope(batch: &RecordBatch) -> Result<Vec<Envelope>, ArrowError> {
    let keys = column::<BinaryArray>(batch, "key")?;
    let values = column::<BinaryArray>(batch, "value")?;
    let headers = column::<ListArray>(batch, "headers")?;
    let partitions = column::<UInt32Array>(batch, "partition")?;
    let positions = column::<Int64Array>(batch, "position")?;
    let timestamps = column::<Int64Array>(batch, "timestamp")?;
    let mut messages = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut row_headers = Vec::new();
        if headers.is_valid(row) {
            let list = headers.value(row);
            let list = list
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ArrowError::SchemaError("headers are not structs".into()))?;
            let hk = list
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ArrowError::SchemaError("header keys are not text".into()))?;
            let hv = list
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ArrowError::SchemaError("header values are not binary".into()))?;
            for i in 0..list.len() {
                row_headers.push(Header {
                    key: hk.value(i).to_string(),
                    value: hv.value(i).to_vec(),
                });
            }
        }
        messages.push(Envelope {
            key: keys.is_valid(row).then(|| keys.value(row).to_vec()),
            value: values.value(row).to_vec(),
            headers: row_headers,
            partition: partitions.value(row),
            position: positions.is_valid(row).then(|| positions.value(row)),
            timestamp: timestamps.is_valid(row).then(|| timestamps.value(row)),
        });
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip_with_ordered_duplicate_headers() {
        let messages = vec![
            Envelope {
                key: Some(b"order-1".to_vec()),
                value: br#"{"total": 3}"#.to_vec(),
                headers: vec![
                    Header {
                        key: "trace".into(),
                        value: b"a".to_vec(),
                    },
                    Header {
                        key: "trace".into(),
                        value: b"b".to_vec(),
                    },
                ],
                partition: 2,
                position: Some(41),
                timestamp: Some(1_791_849_600_000),
            },
            Envelope {
                key: None,
                value: b"plain text".to_vec(),
                headers: vec![],
                partition: 0,
                position: None,
                timestamp: None,
            },
        ];
        let mut builder = EnvelopeBuilder::new();
        for m in &messages {
            builder.push(m);
        }
        assert_eq!(builder.len(), 2);
        assert_eq!(builder.bytes(), 7 + 12 + 2 * 6 + 10);

        let batch = builder.finish().unwrap();

        assert_eq!(batch.schema(), envelope_schema());
        assert_eq!(read_envelope(&batch).unwrap(), messages);
        assert!(builder.is_empty());
    }

    #[test]
    fn a_batch_without_the_envelope_is_an_error() {
        let other = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Int64,
            false,
        )])));
        let error = read_envelope(&other).unwrap_err().to_string();
        assert!(error.contains("no key column of the envelope"), "{error}");
    }
}
