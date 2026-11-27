//! Exactly-once from Kafka into PostgreSQL (`EXACTLY_ONCE_POSTGRES.md`): the stream's
//! PostgreSQL sinks write into one open transaction, and a commit writes the source's next
//! positions into `mageml_stream_positions` in that same transaction, so the rows and the
//! positions are kept or lost together.
//!
//! The source starts each partition it is assigned at the position kept there
//! ([`StartPositions`]). Claiming a partition raises its epoch, and a commit updates a
//! position only at the epoch its instance claimed: an instance that lost a partition to
//! another (a rebalance it did not see in time) has its whole commit rolled back.
//!
//! A transaction begins with the first write or commit after the last commit, so an idle
//! stream holds no transaction open. A failed write or commit leaves nothing visible: the
//! stream stops, and a restart reads from the kept positions.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use mage_stream_ports::{
    BoxFuture, Commit, ConnectorError, PositionNames, StartPositions, Transaction,
};
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::{PgSink, Settings, classify, connect, quote};

/// The table of kept positions, in the schema of the stream's first sink.
pub const POSITIONS_TABLE: &str = "mageml_stream_positions";

struct Open {
    client: Client,
    /// A transaction is open on the connection.
    begun: bool,
}

/// One stream's open PostgreSQL transaction ([`PgTransaction::new`]).
pub struct PgTransaction {
    settings: Settings,
    stream: String,
    names: OnceLock<Arc<dyn PositionNames>>,
    data: Mutex<Option<Open>>,
    /// The epoch this instance claimed each partition at, by name.
    claims: std::sync::Mutex<HashMap<String, i64>>,
}

impl PgTransaction {
    /// The transaction of stream `stream` (stable across restarts: the pipeline's uuid) on
    /// the database of `config`, a PostgreSQL sink's settings. Its positions table goes in
    /// that sink's schema.
    pub fn new(config: &Value, stream: &str) -> Result<Arc<PgTransaction>, ConnectorError> {
        let settings = Settings::new(config)?;
        crate::tls_for(&settings)?;
        Ok(Arc::new(PgTransaction {
            settings,
            stream: stream.to_string(),
            names: OnceLock::new(),
            data: Mutex::new(None),
            claims: std::sync::Mutex::new(HashMap::new()),
        }))
    }

    /// How the source names its commits; set once, after the source is made with this
    /// transaction as its start positions.
    pub fn name_positions_with(&self, names: Arc<dyn PositionNames>) {
        let _ = self.names.set(names);
    }

    /// Whether `config` (a sink's settings) writes to this transaction's database, as the
    /// same user: only those can write in it.
    pub fn same_database(&self, config: &Value) -> Result<bool, ConnectorError> {
        let other = Settings::new(config)?;
        let a = &self.settings;
        Ok((&a.host, a.port, &a.database, &a.user)
            == (&other.host, other.port, &other.database, &other.user))
    }

    /// A sink that writes to `config`'s table inside the transaction.
    pub fn sink(self: &Arc<Self>, config: &Value) -> Result<PgSink, ConnectorError> {
        let mut sink = PgSink::new(config)?;
        sink.shared = Some(self.clone());
        Ok(sink)
    }

    fn table(&self) -> String {
        format!(
            "{}.{}",
            quote(&self.settings.schema),
            quote(POSITIONS_TABLE)
        )
    }

    /// The open transaction's connection, beginning one when none is open.
    async fn begun<'a>(
        &self,
        guard: &'a mut tokio::sync::MutexGuard<'_, Option<Open>>,
    ) -> Result<&'a Client, ConnectorError> {
        if guard.as_ref().is_none_or(|o| o.client.is_closed()) {
            // A closed connection lost what it had written; the commit that follows the
            // writes fails on it and the stream starts again from the kept positions.
            if guard.as_ref().is_some_and(|o| o.begun) {
                return Err(ConnectorError::fatal(
                    "PostgreSQL: the connection of the open transaction closed, and with it \
                     the rows written since the last commit",
                ));
            }
            **guard = Some(Open {
                client: connect(&self.settings).await?,
                begun: false,
            });
        }
        let open = guard.as_mut().expect("connected");
        if !open.begun {
            open.client
                .batch_execute("BEGIN")
                .await
                .map_err(|e| classify("starting the stream's transaction", e))?;
            open.begun = true;
        }
        Ok(&open.client)
    }

    /// Writes a sink's rows in the open transaction.
    pub(crate) async fn write(
        &self,
        sink: &PgSink,
        values: &[Vec<u8>],
    ) -> Result<(), ConnectorError> {
        let mut guard = self.data.lock().await;
        let client = self.begun(&mut guard).await?;
        sink.write_rows(client, values).await
    }

    async fn rollback(&self, guard: &mut tokio::sync::MutexGuard<'_, Option<Open>>) {
        if let Some(open) = guard.as_mut()
            && open.begun
        {
            open.begun = false;
            if open.client.batch_execute("ROLLBACK").await.is_err() {
                **guard = None;
            }
        }
    }

    /// Claims the partitions for this instance: each one's epoch goes up by one, fencing
    /// an earlier reader, and its kept position comes back.
    async fn claim_now(&self, partitions: &[String]) -> Result<Vec<Option<i64>>, ConnectorError> {
        let client = connect(&self.settings).await?;
        let table = self.table();
        // Two instances starting at once would both create the table, and PostgreSQL fails
        // one of two concurrent CREATE ... IF NOT EXISTS: the lock takes them in turn.
        client
            .batch_execute(&format!(
                "BEGIN; SELECT pg_advisory_xact_lock(hashtext('mageml_stream_positions')); \
                 CREATE SCHEMA IF NOT EXISTS {schema}; CREATE TABLE IF NOT EXISTS {table} (\
                 stream TEXT NOT NULL, source_partition TEXT NOT NULL, next_offset BIGINT, \
                 epoch BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
                 PRIMARY KEY (stream, source_partition)); COMMIT",
                schema = quote(&self.settings.schema),
            ))
            .await
            .map_err(|e| classify("creating the kept positions table", e))?;
        let statement = client
            .prepare(&format!(
                "INSERT INTO {table} AS p (stream, source_partition, epoch) VALUES ($1, $2, 1) \
                 ON CONFLICT (stream, source_partition) DO UPDATE SET epoch = p.epoch + 1, \
                 updated_at = now() RETURNING next_offset, epoch"
            ))
            .await
            .map_err(|e| classify("claiming the stream's partitions", e))?;
        client
            .batch_execute("BEGIN")
            .await
            .map_err(|e| classify("claiming the stream's partitions", e))?;
        let mut found = Vec::with_capacity(partitions.len());
        let mut epochs = Vec::with_capacity(partitions.len());
        for name in partitions {
            let row = client
                .query_one(&statement, &[&self.stream, name])
                .await
                .map_err(|e| classify("claiming the stream's partitions", e))?;
            found.push(row.get::<_, Option<i64>>(0));
            epochs.push((name.clone(), row.get::<_, i64>(1)));
        }
        client
            .batch_execute("COMMIT")
            .await
            .map_err(|e| classify("claiming the stream's partitions", e))?;
        let mut claims = self.claims.lock().unwrap_or_else(|p| p.into_inner());
        claims.extend(epochs);
        Ok(found)
    }
}

impl StartPositions for PgTransaction {
    /// Runs on the source's poll thread, or where the source is made: on a thread of its
    /// own with a runtime of its own, as either may be inside the stream's runtime.
    fn claim(&self, partitions: &[String]) -> Result<Vec<Option<i64>>, ConnectorError> {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| ConnectorError::fatal(format!("PostgreSQL: {e}")))?
                        .block_on(self.claim_now(partitions))
                })
                .join()
                .unwrap_or_else(|_| {
                    Err(ConnectorError::fatal(
                        "PostgreSQL: claiming the stream's partitions panicked",
                    ))
                })
        })
    }
}

impl Transaction for PgTransaction {
    fn commit<'a>(&'a self, commits: &'a [Commit]) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let names = self.names.get().ok_or_else(|| {
                ConnectorError::fatal("PostgreSQL: the stream's source names no positions")
            })?;
            let named = names.named(commits)?;
            let claims: HashMap<String, i64> = self
                .claims
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let table = self.table();
            let mut guard = self.data.lock().await;
            let outcome = async {
                let client = self.begun(&mut guard).await?;
                for (name, next) in &named {
                    let epoch = claims.get(name).ok_or_else(|| {
                        ConnectorError::fatal(format!(
                            "PostgreSQL: partition {name} was never claimed by this instance"
                        ))
                    })?;
                    let updated = client
                        .execute(
                            &format!(
                                "UPDATE {table} SET next_offset = $1, updated_at = now() \
                                 WHERE stream = $2 AND source_partition = $3 AND epoch = $4"
                            ),
                            &[next, &self.stream, name, epoch],
                        )
                        .await
                        .map_err(|e| classify("keeping the stream's positions", e))?;
                    if updated != 1 {
                        return Err(ConnectorError::fatal(format!(
                            "PostgreSQL: partition {name} is now read by another instance of \
                             the stream, so this instance's rows since its last commit were \
                             rolled back"
                        )));
                    }
                }
                client
                    .batch_execute("COMMIT")
                    .await
                    .map_err(|e| classify("committing the stream's transaction", e))
            }
            .await;
            match outcome {
                Ok(()) => {
                    if let Some(open) = guard.as_mut() {
                        open.begun = false;
                    }
                    names.committed(&named);
                    Ok(())
                }
                Err(error) => {
                    self.rollback(&mut guard).await;
                    Err(error)
                }
            }
        })
    }

    fn abort(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut guard = self.data.lock().await;
            self.rollback(&mut guard).await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mage_stream_ports::{
        AssignmentEpoch, BatchMeta, Envelope, EnvelopeBuilder, PartitionId, Position, Sink,
    };

    /// Names partition n `t/n`, as the Kafka source names topic t's partitions.
    #[derive(Default)]
    struct Names {
        committed: std::sync::Mutex<Vec<(String, i64)>>,
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

    fn settings(table: &str) -> Option<Value> {
        let url = std::env::var("MAGE_TEST_POSTGRES_URL").ok()?;
        let (rest, _, _) = mage_pg::split_tls(&url);
        let config: tokio_postgres::Config = rest.parse().unwrap();
        let tokio_postgres::config::Host::Tcp(host) = &config.get_hosts()[0] else {
            return None;
        };
        Some(serde_json::json!({
            "host": host,
            "port": config.get_ports().first().copied().unwrap_or(5432),
            "database": config.get_dbname().unwrap_or("postgres"),
            "username": config.get_user().unwrap_or("postgres"),
            "password": config.get_password().map(|p| String::from_utf8_lossy(p).to_string()),
            "schema": "public",
            "table": table,
            "sslmode": "disable",
        }))
    }

    fn batch(ids: &[i64]) -> (mage_stream_ports::RecordBatch, BatchMeta) {
        let mut builder = EnvelopeBuilder::new();
        for id in ids {
            builder.push(&Envelope {
                value: format!("{{\"id\": {id}}}").into_bytes(),
                ..Default::default()
            });
        }
        let meta = BatchMeta {
            id: mage_stream_core::BatchId {
                partition: PartitionId(0),
                epoch: AssignmentEpoch(1),
                seq: 0,
            },
            first: Position::Offset(0),
            last: Position::Offset(0),
            records: ids.len() as u32,
            bytes: 0,
        };
        (builder.finish().unwrap(), meta)
    }

    fn commit(last: i64) -> Vec<Commit> {
        vec![Commit {
            partition: PartitionId(0),
            epoch: AssignmentEpoch(1),
            position: Position::Offset(last),
        }]
    }

    async fn rows(config: &Value, table: &str) -> i64 {
        let client = connect(&Settings::new(config).unwrap()).await.unwrap();
        let exists: Option<String> = client
            .query_one("SELECT to_regclass($1)::text", &[&table])
            .await
            .unwrap()
            .get(0);
        if exists.is_none() {
            return 0;
        }
        client
            .query_one(&format!("SELECT count(*) FROM {}", quote(table)), &[])
            .await
            .unwrap()
            .get(0)
    }

    fn stream(name: &str) -> String {
        format!("{name}_{}_{}", std::process::id(), rand_suffix())
    }

    fn rand_suffix() -> u32 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rows_and_positions_are_kept_or_lost_together() {
        let table = stream("eo_rows");
        let Some(config) = settings(&table) else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        let transaction = PgTransaction::new(&config, &table).unwrap();
        let names = Arc::new(Names::default());
        transaction.name_positions_with(names.clone());
        // A new stream keeps no position: the source starts as it would without.
        assert_eq!(transaction.claim(&["t/0".into()]).unwrap(), vec![None]);
        let sink = transaction.sink(&config).unwrap();

        let (first, meta) = batch(&[1, 2]);
        sink.write(&first, &meta).await.unwrap();
        transaction.abort().await;
        assert_eq!(
            rows(&config, &table).await,
            0,
            "an aborted batch leaves nothing"
        );

        let (second, meta) = batch(&[3, 4, 5]);
        sink.write(&second, &meta).await.unwrap();
        assert_eq!(
            rows(&config, &table).await,
            0,
            "uncommitted rows are not visible"
        );
        let (third, meta) = batch(&[6]);
        sink.write(&third, &meta).await.unwrap();
        transaction.commit(&commit(9)).await.unwrap();
        assert_eq!(rows(&config, &table).await, 4);
        assert_eq!(
            *names.committed.lock().unwrap(),
            vec![("t/0".to_string(), 10)]
        );
        // Committed: no transaction stays open while the stream is idle.
        assert!(!transaction.data.lock().await.as_ref().unwrap().begun);

        // A restart starts where the commit left off.
        let restarted = PgTransaction::new(&config, &table).unwrap();
        assert_eq!(restarted.claim(&["t/0".into()]).unwrap(), vec![Some(10)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_instance_that_lost_its_partition_commits_nothing() {
        let table = stream("eo_fence");
        let Some(config) = settings(&table) else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        let old = PgTransaction::new(&config, &table).unwrap();
        old.name_positions_with(Arc::new(Names::default()));
        old.claim(&["t/0".into()]).unwrap();
        let (rows_a, meta) = batch(&[1, 2, 3]);
        old.sink(&config)
            .unwrap()
            .write(&rows_a, &meta)
            .await
            .unwrap();

        // Another instance takes the partition while the first one's rows are uncommitted.
        let new = PgTransaction::new(&config, &table).unwrap();
        new.name_positions_with(Arc::new(Names::default()));
        assert_eq!(new.claim(&["t/0".into()]).unwrap(), vec![None]);

        let error = old.commit(&commit(2)).await.unwrap_err();
        assert!(!error.retryable);
        assert!(
            error.message.contains("is now read by another instance"),
            "{error:?}"
        );
        assert_eq!(rows(&config, &table).await, 0);

        let (rows_b, meta) = batch(&[1, 2, 3]);
        new.sink(&config)
            .unwrap()
            .write(&rows_b, &meta)
            .await
            .unwrap();
        new.commit(&commit(2)).await.unwrap();
        assert_eq!(rows(&config, &table).await, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn instances_starting_at_once_create_the_positions_table_once() {
        let schema = stream("eo_schema");
        let Some(mut config) = settings("rows") else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        config["schema"] = Value::String(schema.clone());
        let claims: Vec<_> = (0..6)
            .map(|i| {
                let config = config.clone();
                std::thread::spawn(move || {
                    let transaction = PgTransaction::new(&config, "s").unwrap();
                    transaction.claim(&[format!("t/{i}")])
                })
            })
            .collect();
        for claim in claims {
            assert_eq!(claim.join().unwrap().unwrap(), vec![None]);
        }
        let client = connect(&Settings::new(&config).unwrap()).await.unwrap();
        client
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", quote(&schema)))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upserts_stage_each_batch_in_one_transaction() {
        let table = stream("eo_upsert");
        let Some(mut config) = settings(&table) else {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
            return;
        };
        config["unique_constraints"] = serde_json::json!(["id"]);
        let transaction = PgTransaction::new(&config, &table).unwrap();
        transaction.name_positions_with(Arc::new(Names::default()));
        transaction.claim(&["t/0".into()]).unwrap();
        let sink = transaction.sink(&config).unwrap();
        // Two batches, each through the staging table, in one transaction.
        for ids in [&[1, 2][..], &[2, 3][..]] {
            let (rows_, meta) = batch(ids);
            sink.write(&rows_, &meta).await.unwrap();
        }
        transaction.commit(&commit(1)).await.unwrap();
        assert_eq!(rows(&config, &table).await, 3);
    }
}
