//! Spans of a stream's batches (traces.rs): one per batch read, from its read until its
//! position is committed (or it is cancelled), with a child for each transform attempt and
//! each sink write. Timer batches carry no messages and are not traced. With tracing off
//! nothing is kept.

use std::collections::HashMap;

use mage_stream_core::{BatchId, BatchMeta, Commit, Position, SinkId};

use crate::traces::{Kind, Span, Tracer};

struct Traced {
    batch: Span,
    last: Position,
    transform: Option<Span>,
    writes: HashMap<SinkId, Span>,
}

pub struct BatchTraces {
    pipeline: String,
    /// The block each sink writes the output of; None writes the source's messages.
    sinks: Vec<Option<String>>,
    tracer: Option<&'static Tracer>,
    batches: HashMap<BatchId, Traced>,
}

impl BatchTraces {
    pub fn new(pipeline: &str, sinks: Vec<Option<String>>) -> BatchTraces {
        BatchTraces::with_tracer(crate::traces::tracer(), pipeline, sinks)
    }

    fn with_tracer(
        tracer: Option<&'static Tracer>,
        pipeline: &str,
        sinks: Vec<Option<String>>,
    ) -> BatchTraces {
        BatchTraces {
            pipeline: pipeline.to_string(),
            sinks,
            tracer,
            batches: HashMap::new(),
        }
    }

    pub fn read(&mut self, meta: &BatchMeta) {
        if self.tracer.is_none() || meta.id.is_timer() {
            return;
        }
        let mut batch = self.span(format!("stream {} batch", self.pipeline), None);
        batch.set("mage.pipeline", self.pipeline.clone());
        batch.set("mage.partition", meta.id.partition.0);
        batch.set("mage.position.first", meta.first.to_string());
        batch.set("mage.position.last", meta.last.to_string());
        batch.set("messaging.batch.message_count", meta.records);
        self.batches.insert(
            meta.id,
            Traced {
                batch,
                last: meta.last.clone(),
                transform: None,
                writes: HashMap::new(),
            },
        );
    }

    fn span(&self, name: String, parent: Option<&Span>) -> Span {
        Span::start_in(
            self.tracer,
            name,
            Kind::Internal,
            parent.and_then(Span::context),
        )
    }

    pub fn transform_started(&mut self, id: BatchId) {
        let tracer = self.tracer;
        if let Some(traced) = self.batches.get_mut(&id) {
            let span = Span::start_in(tracer, "transform", Kind::Internal, traced.batch.context());
            traced.transform = Some(span);
        }
    }

    /// The versions of the broadcast values the batch's blocks ran with, on the batch's span,
    /// so an output can be traced to the values it was made with.
    pub fn broadcast(&mut self, id: BatchId, versions: &str) {
        if let Some(traced) = self.batches.get_mut(&id) {
            traced.batch.set("mage.broadcast", versions.to_string());
        }
    }

    pub fn transform_finished(&mut self, id: BatchId, error: Option<&str>) {
        if let Some(mut span) = self
            .batches
            .get_mut(&id)
            .and_then(|traced| traced.transform.take())
            && let Some(message) = error
        {
            // Taken either way: a span without an error ends as it is dropped.
            span.fail(message);
        }
    }

    pub fn write_started(&mut self, id: BatchId, sink: SinkId) {
        let after = self.sinks.get(sink.0 as usize).cloned().flatten();
        let tracer = self.tracer;
        if let Some(traced) = self.batches.get_mut(&id) {
            let mut span = Span::start_in(tracer, "write", Kind::Internal, traced.batch.context());
            span.set("mage.sink", u32::from(sink.0));
            span.set("mage.sink.after", after.unwrap_or_else(|| "source".into()));
            traced.writes.insert(sink, span);
        }
    }

    pub fn write_finished(&mut self, id: BatchId, sink: SinkId, error: Option<&str>) {
        if let Some(mut span) = self
            .batches
            .get_mut(&id)
            .and_then(|traced| traced.writes.remove(&sink))
            && let Some(message) = error
        {
            // Taken either way: a span without an error ends as it is dropped.
            span.fail(message);
        }
    }

    /// The batches through the committed position are done.
    pub fn committed(&mut self, commit: &Commit) {
        if self.batches.is_empty() {
            return;
        }
        self.batches.retain(|id, traced| {
            !(id.partition == commit.partition
                && id.epoch == commit.epoch
                && traced.last <= commit.position)
        });
    }

    pub fn cancelled(&mut self, ids: &[BatchId]) {
        for id in ids {
            if let Some(mut traced) = self.batches.remove(id) {
                traced.batch.set("mage.cancelled", true);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.batches.len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mage_stream_core::{AssignmentEpoch, PartitionId};
    use serde_json::Value;

    use super::*;

    fn meta(seq: u64) -> BatchMeta {
        BatchMeta {
            id: BatchId {
                partition: PartitionId(0),
                epoch: AssignmentEpoch(1),
                seq,
            },
            first: Position::Offset(seq as i64 * 10),
            last: Position::Offset(seq as i64 * 10 + 9),
            records: 10,
            bytes: 100,
        }
    }

    #[test]
    fn without_a_tracer_nothing_is_kept() {
        let mut traces = BatchTraces::new("orders", vec![None]);
        traces.read(&meta(0));
        assert_eq!(traces.len(), 0);
    }

    #[test]
    fn a_batch_spans_its_transform_and_writes_until_its_commit() {
        let (url, received) = crate::traces::testing::collector();
        let tracer = crate::traces::testing::tracer(&url);
        let mut traces =
            BatchTraces::with_tracer(Some(tracer), "orders", vec![Some("clean".into())]);
        let (first, second) = (meta(0), meta(1));
        traces.read(&first);
        traces.transform_started(first.id);
        traces.transform_finished(first.id, Some("clean: ValueError"));
        traces.write_started(first.id, SinkId(0));
        traces.write_finished(first.id, SinkId(0), None);
        // A later batch of the partition stays open past this commit.
        traces.read(&second);
        traces.committed(&Commit {
            partition: PartitionId(0),
            epoch: AssignmentEpoch(1),
            position: Position::Offset(9),
        });
        assert_eq!(traces.len(), 1);
        tracer.flush(Duration::from_secs(5));

        let body = received.recv_timeout(Duration::from_secs(5)).unwrap();
        let spans: Vec<Value> = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap()
            .clone();
        let names: Vec<&str> = spans.iter().map(|s| s["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["transform", "write", "stream orders batch"]);
        let batch = &spans[2];
        assert_eq!(spans[0]["parentSpanId"], batch["spanId"]);
        assert_eq!(spans[1]["parentSpanId"], batch["spanId"]);
        assert_eq!(spans[0]["status"]["message"], "clean: ValueError");
        let records = batch["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["key"] == "messaging.batch.message_count")
            .unwrap();
        assert_eq!(records["value"], serde_json::json!({"intValue": "10"}));
    }
}
