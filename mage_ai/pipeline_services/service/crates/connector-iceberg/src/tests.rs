use std::collections::BTreeMap;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use futures::TryStreamExt;
use mage_stream_core::{AssignmentEpoch, BatchId, PartitionId, Position};
use mage_stream_ports::{
    Commit, Envelope, EnvelopeBuilder, PositionNames, StartPositions, Transaction,
};

use super::*;
use crate::transaction::IcebergTransaction;

/// Names partition n `t/n`, as the Kafka source names topic t's partitions.
#[derive(Default)]
struct Names {
    committed: Mutex<Vec<(String, i64)>>,
}

impl PositionNames for Names {
    fn named(&self, commits: &[Commit]) -> Result<Vec<(String, i64)>, ConnectorError> {
        Ok(commits
            .iter()
            .map(|c| match c.position {
                Position::Offset(last) => (format!("t/{}", c.partition.0), last + 1),
                _ => unreachable!(),
            })
            .collect())
    }

    fn committed(&self, named: &[(String, i64)]) {
        self.committed.lock().unwrap().extend_from_slice(named);
    }
}

fn config(warehouse: &tempfile::TempDir, table: &str) -> Value {
    serde_json::json!({
        "catalog": {"type": "memory", "warehouse": format!("file://{}", warehouse.path().display())},
        "table": table,
        "create_table": true,
        "partition_by": ["region"],
    })
}

fn batch(partition: u32, first: i64, messages: &[&str]) -> (RecordBatch, BatchMeta) {
    let mut builder = EnvelopeBuilder::new();
    for m in messages {
        builder.push(&Envelope {
            value: m.as_bytes().to_vec(),
            ..Default::default()
        });
    }
    let meta = BatchMeta {
        id: BatchId {
            partition: PartitionId(partition),
            epoch: AssignmentEpoch(1),
            seq: first as u64,
        },
        first: Position::Offset(first),
        last: Position::Offset(first + messages.len() as i64 - 1),
        records: messages.len() as u32,
        bytes: 0,
    };
    (builder.finish().unwrap(), meta)
}

/// The table's rows as (id, region, the rest as JSON), by id.
async fn rows(config: &Value) -> BTreeMap<i64, (String, String)> {
    let settings = Settings::new(config).unwrap();
    let catalog = settings.catalog().await.unwrap();
    let table = catalog.load_table(&settings.ident).await.unwrap();
    let batches: Vec<arrow_array::RecordBatch> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = BTreeMap::new();
    for b in batches {
        let ids = b.column_by_name("id").unwrap().as_primitive::<Int64Type>();
        let regions = b.column_by_name("region").unwrap().as_string::<i32>();
        let tags = b
            .column_by_name("tags")
            .map(|c| c.as_string::<i32>().clone());
        for i in 0..b.num_rows() {
            let tag = tags
                .as_ref()
                .and_then(|t| (!t.is_null(i)).then(|| t.value(i).to_string()))
                .unwrap_or_default();
            rows.insert(ids.value(i), (regions.value(i).to_string(), tag));
        }
    }
    rows
}

async fn properties(config: &Value) -> HashMap<String, String> {
    let settings = Settings::new(config).unwrap();
    let catalog = settings.catalog().await.unwrap();
    let table = catalog.load_table(&settings.ident).await.unwrap();
    table.metadata().properties().clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_queued_together_are_confirmed_by_the_commit_that_holds_them() {
    let warehouse = tempfile::tempdir().unwrap();
    let config = config(&warehouse, "lake.events");
    let sink = IcebergSink::new(&config, "orders.lake").unwrap();
    let (b1, m1) = batch(
        0,
        0,
        &[
            r#"{"id": 1, "region": "eu", "tags": ["a", "b"]}"#,
            r#"{"id": 2, "region": "us"}"#,
        ],
    );
    let (b2, m2) = batch(0, 2, &[r#"{"id": 3, "region": "eu", "unknown": 1}"#]);
    let (b3, m3) = batch(1, 0, &[]);
    let c1 = sink.queue(&b1, &m1).await.unwrap();
    let c2 = sink.queue(&b2, &m2).await.unwrap();
    let c3 = sink.queue(&b3, &m3).await.unwrap();
    c1.await.unwrap();
    c2.await.unwrap();
    c3.await.unwrap();
    sink.flush().await;

    let got = rows(&config).await;
    assert_eq!(
        got,
        BTreeMap::from([
            (1, ("eu".to_string(), r#"["a","b"]"#.to_string())),
            (2, ("us".to_string(), String::new())),
            (3, ("eu".to_string(), String::new())),
        ])
    );
    // Partitioned by region, each commit names its batches.
    let settings = Settings::new(&config).unwrap();
    let catalog = settings.catalog().await.unwrap();
    let table = catalog.load_table(&settings.ident).await.unwrap();
    assert!(!table.metadata().default_partition_spec().is_unpartitioned());
    let mut snapshots: Vec<_> = table.metadata().snapshots().collect();
    snapshots.sort_by_key(|s| s.sequence_number());
    let named: Vec<String> = snapshots
        .iter()
        .filter_map(|s| {
            s.summary()
                .additional_properties
                .get(SUMMARY_BATCHES)
                .cloned()
        })
        .collect();
    assert_eq!(named.join(","), "p0-0-1,p0-2-2", "{named:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_message_that_does_not_fit_and_a_missing_table_stop_the_stream() {
    let warehouse = tempfile::tempdir().unwrap();
    let config = config(&warehouse, "lake.typed");
    let sink = IcebergSink::new(&config, "s").unwrap();
    let (b, m) = batch(0, 0, &[r#"{"id": 1, "region": "eu"}"#]);
    sink.write(&b, &m).await.unwrap();
    let (b, m) = batch(0, 1, &[r#"{"id": "one", "region": "eu"}"#]);
    let error = sink.write(&b, &m).await.err().unwrap();
    assert!(
        !error.retryable && error.message.contains("does not fit"),
        "{error:?}"
    );

    let mut missing = config.clone();
    missing["table"] = "lake.missing".into();
    missing["create_table"] = false.into();
    let sink = IcebergSink::new(&missing, "s").unwrap();
    let error = sink.write(&b, &m).await.err().unwrap();
    assert!(
        !error.retryable && error.message.contains("does not exist"),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn exactly_once_keeps_positions_with_the_rows_and_fences_a_lost_partition() {
    let warehouse = tempfile::tempdir().unwrap();
    let config = config(&warehouse, "lake.exact");
    let names = Arc::new(Names::default());
    let first = IcebergTransaction::new(&config, "orders").unwrap();
    first.name_positions_with(names.clone());
    // No table yet: nothing kept.
    assert_eq!(first.claim(&["t/0".to_string()]).unwrap(), vec![None]);
    let sink = first.sink();
    let (b, m) = batch(
        0,
        0,
        &[
            r#"{"id": 1, "region": "eu"}"#,
            r#"{"id": 2, "region": "eu"}"#,
        ],
    );
    sink.write(&b, &m).await.unwrap();
    let commit = |last| {
        vec![Commit {
            partition: PartitionId(0),
            epoch: AssignmentEpoch(1),
            position: Position::Offset(last),
        }]
    };
    first.commit(&commit(1)).await.unwrap();
    assert_eq!(rows(&config).await.len(), 2);
    assert_eq!(
        properties(&config)
            .await
            .get("mageml.stream.orders.position.t/0")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(
        names.committed.lock().unwrap().clone(),
        vec![("t/0".to_string(), 2)]
    );

    // Written but aborted: not in the table.
    let (b, m) = batch(0, 2, &[r#"{"id": 3, "region": "us"}"#]);
    sink.write(&b, &m).await.unwrap();
    first.abort().await;
    first.commit(&commit(1)).await.unwrap();
    assert_eq!(rows(&config).await.len(), 2);

    // A restart starts where the table says; the instance before it is fenced.
    let second = IcebergTransaction::new(&config, "orders").unwrap();
    second.name_positions_with(Arc::new(Names::default()));
    assert_eq!(second.claim(&["t/0".to_string()]).unwrap(), vec![Some(2)]);
    sink.write(&b, &m).await.unwrap();
    let error = first.commit(&commit(2)).await.err().unwrap();
    assert!(
        error.message.contains("now read by another instance"),
        "{error:?}"
    );
    assert_eq!(rows(&config).await.len(), 2);

    let sink = second.sink();
    sink.write(&b, &m).await.unwrap();
    second.commit(&commit(2)).await.unwrap();
    assert_eq!(
        rows(&config).await.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        properties(&config)
            .await
            .get("mageml.stream.orders.position.t/0")
            .map(String::as_str),
        Some("3")
    );
}
