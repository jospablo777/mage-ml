//! A stream whose blocks mix Python and Rust, as a [`TransformSession`]: each run of blocks
//! in one language is its own session (a Python worker, a Rust program), and each takes the
//! messages of the run's last block before it. Sinks get the messages of the block they
//! follow, whichever run it is in.
//!
//! Messages that fail on their own are found in the first run only: later runs take messages
//! that are not the batch's own, so their failure fails the batch.

use std::collections::{BTreeMap, BTreeSet};

use mage_stream_ports::{
    BoxFuture, RecordBatch, TransformError, TransformOutput, TransformSession, envelope_schema,
};

pub struct ChainedSession {
    /// Each run's session and the uuid of its last block.
    runs: Vec<(Box<dyn TransformSession>, String)>,
    /// The blocks whose messages the sinks write.
    wanted: BTreeSet<String>,
}

impl ChainedSession {
    pub fn new(
        runs: Vec<(Box<dyn TransformSession>, String)>,
        wanted: BTreeSet<String>,
    ) -> ChainedSession {
        ChainedSession { runs, wanted }
    }
}

impl TransformSession for ChainedSession {
    fn transform(
        &mut self,
        batch: RecordBatch,
    ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
        Box::pin(async move {
            let mut input = batch;
            let mut outputs = BTreeMap::new();
            let mut row_errors = Vec::new();
            let mut blocks = Vec::new();
            for (index, (session, last)) in self.runs.iter_mut().enumerate() {
                let rows_in = input.num_rows() as u64;
                let started = std::time::Instant::now();
                let output = session.transform(input).await?;
                if index == 0 {
                    row_errors = output.row_errors;
                }
                let next = output
                    .outputs
                    .get(last.as_str())
                    .cloned()
                    .unwrap_or_else(|| RecordBatch::new_empty(envelope_schema()));
                if output.blocks.is_empty() {
                    // A run that times no block on its own (Rust, R) counts as its last block.
                    blocks.push(mage_stream_ports::BlockStats {
                        block: last.clone(),
                        messages_in: rows_in,
                        messages_out: next.num_rows() as u64,
                        micros: started.elapsed().as_micros() as u64,
                    });
                } else {
                    blocks.extend(output.blocks);
                }
                for (block, messages) in output.outputs {
                    if self.wanted.contains(&block) {
                        outputs.insert(block, messages);
                    }
                }
                if next.num_rows() == 0 {
                    // Nothing goes on: the later blocks make nothing of this batch.
                    break;
                }
                input = next;
            }
            Ok(TransformOutput {
                outputs,
                row_errors,
                blocks,
            })
        })
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            for (session, _) in &mut self.runs {
                session.close().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mage_stream_ports::{Envelope, EnvelopeBuilder, RowError, read_envelope};

    /// Appends its name to each message's value and keeps the rest; `drop_all` keeps none.
    struct Tag {
        blocks: Vec<String>,
        drop_all: bool,
        reject_first: bool,
    }

    impl TransformSession for Tag {
        fn transform(
            &mut self,
            batch: RecordBatch,
        ) -> BoxFuture<'_, Result<TransformOutput, TransformError>> {
            Box::pin(async move {
                let mut builder = EnvelopeBuilder::new();
                if !self.drop_all {
                    for message in read_envelope(&batch).unwrap() {
                        let mut value = message.value.clone();
                        value.extend_from_slice(self.blocks.last().unwrap().as_bytes());
                        builder.push(&Envelope {
                            value,
                            ..Envelope::default()
                        });
                    }
                }
                let out = builder.finish().unwrap();
                Ok(TransformOutput {
                    outputs: self
                        .blocks
                        .iter()
                        .map(|b| (b.clone(), out.clone()))
                        .collect(),
                    row_errors: if self.reject_first {
                        vec![RowError {
                            row: 0,
                            error: "bad".into(),
                        }]
                    } else {
                        Vec::new()
                    },
                    blocks: Vec::new(),
                })
            })
        }

        fn close(&mut self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    fn batch(values: &[&str]) -> RecordBatch {
        let mut builder = EnvelopeBuilder::new();
        for value in values {
            builder.push(&Envelope {
                value: value.as_bytes().to_vec(),
                ..Envelope::default()
            });
        }
        builder.finish().unwrap()
    }

    fn values(batch: &RecordBatch) -> Vec<String> {
        read_envelope(batch)
            .unwrap()
            .into_iter()
            .map(|m| String::from_utf8(m.value).unwrap())
            .collect()
    }

    fn run(
        blocks: &[&str],
        drop_all: bool,
        reject_first: bool,
    ) -> (Box<dyn TransformSession>, String) {
        (
            Box::new(Tag {
                blocks: blocks.iter().map(|b| b.to_string()).collect(),
                drop_all,
                reject_first,
            }),
            blocks.last().unwrap().to_string(),
        )
    }

    #[tokio::test]
    async fn each_run_takes_the_messages_of_the_one_before_and_sinks_get_theirs() {
        let mut chain = ChainedSession::new(
            vec![run(&["parse"], false, true), run(&["score"], false, true)],
            ["parse".to_string(), "score".to_string()].into(),
        );

        let output = chain.transform(batch(&["a", "b"])).await.unwrap();

        assert_eq!(values(&output.outputs["parse"]), ["aparse", "bparse"]);
        assert_eq!(
            values(&output.outputs["score"]),
            ["aparsescore", "bparsescore"]
        );
        // Only the first run's messages are the batch's own rows.
        assert_eq!(output.row_errors.len(), 1);
    }

    #[tokio::test]
    async fn a_run_that_keeps_nothing_ends_the_batch_there() {
        let mut chain = ChainedSession::new(
            vec![run(&["filter"], true, false), run(&["score"], false, false)],
            ["score".to_string()].into(),
        );

        let output = chain.transform(batch(&["a"])).await.unwrap();

        assert!(output.outputs.is_empty());
    }
}
