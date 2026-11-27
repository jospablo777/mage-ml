//! Exactly-once from Kafka into an Iceberg table (ADR-0042): the stream's batches become
//! data files as they are written, and a commit appends them in one snapshot that also sets
//! the source's next positions as table properties, so the rows and the positions are kept
//! or lost together. The source starts each partition it is assigned at the position kept
//! there ([`StartPositions`]).
//!
//! Each partition has its own properties, `mageml.stream.<stream>.position.<partition>` and
//! `.claim.<partition>`, so instances that read different partitions never overwrite each
//! other's when iceberg re-applies a commit over a newer version of the table. Claiming a
//! partition writes a token of this instance; a commit first reloads the table and refuses
//! when a partition it commits was claimed by another. iceberg re-applies a commit that
//! raced another one, so an instance that lost its partitions and commits in the moment
//! between that check and its commit, while another claims them, adds that commit's rows
//! twice (ADR-0042).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use iceberg::spec::DataFile;
use iceberg::table::Table;
use mage_stream_ports::{
    BatchMeta, BoxFuture, Commit, ConnectorError, PositionNames, RecordBatch, Sink,
    SinkCapabilities, StartPositions, Transaction,
};
use serde_json::Value;
use tokio::sync::{Mutex, OnceCell};

use crate::table::{self, Settings};
use crate::{SUMMARY_BATCHES, SUMMARY_STREAM, Shared, batch_name, values};

fn position_key(stream: &str, partition: &str) -> String {
    format!("mageml.stream.{stream}.position.{partition}")
}

fn claim_key(stream: &str, partition: &str) -> String {
    format!("mageml.stream.{stream}.claim.{partition}")
}

/// One stream's open Iceberg transaction ([`IcebergTransaction::new`]).
pub struct IcebergTransaction {
    settings: Settings,
    stream: String,
    /// This instance, in its claims.
    instance: String,
    names: OnceLock<Arc<dyn PositionNames>>,
    shared: OnceCell<Arc<Shared>>,
    /// Data files written since the last commit, with their batches' names.
    pending: Mutex<Vec<(Vec<DataFile>, String)>>,
    /// This instance's claim token of each partition it reads.
    claims: std::sync::Mutex<HashMap<String, String>>,
}

impl IcebergTransaction {
    /// The transaction of stream `stream` (stable across restarts: the pipeline's uuid) into
    /// the table of `config`, an Iceberg sink's settings.
    pub fn new(config: &Value, stream: &str) -> Result<Arc<IcebergTransaction>, ConnectorError> {
        Ok(Arc::new(IcebergTransaction {
            settings: Settings::new(config)?,
            stream: stream.to_string(),
            instance: uuid::Uuid::new_v4().simple().to_string(),
            names: OnceLock::new(),
            shared: OnceCell::new(),
            pending: Mutex::new(Vec::new()),
            claims: std::sync::Mutex::new(HashMap::new()),
        }))
    }

    /// How the source names its commits; set once, after the source is made with this
    /// transaction as its start positions.
    pub fn name_positions_with(&self, names: Arc<dyn PositionNames>) {
        let _ = self.names.set(names);
    }

    /// The sink that writes into this transaction.
    pub fn sink(self: &Arc<Self>) -> IcebergTransactionSink {
        IcebergTransactionSink {
            transaction: self.clone(),
        }
    }

    pub fn label(&self) -> String {
        self.settings.label()
    }

    async fn shared(&self, first: &[Vec<u8>]) -> Result<&Arc<Shared>, ConnectorError> {
        self.shared
            .get_or_try_init(|| async { Ok(Arc::new(Shared::open(&self.settings, first).await?)) })
            .await
    }

    /// The table as the catalog has it now, or None when it was not made yet.
    async fn latest(
        &self,
        catalog: &dyn iceberg::Catalog,
    ) -> Result<Option<Table>, ConnectorError> {
        match catalog.load_table(&self.settings.ident).await {
            Ok(table) => Ok(Some(table)),
            Err(e) if table::missing(&e) && self.settings.create => Ok(None),
            Err(e) => Err(table::classify("loading the table", e)),
        }
    }

    async fn claim_now(&self, partitions: &[String]) -> Result<Vec<Option<i64>>, ConnectorError> {
        let catalog = self.settings.catalog().await?;
        let Some(table) = self.latest(catalog.as_ref()).await? else {
            // The first commit makes the table and writes the claims.
            let mut claims = self.claims.lock().expect("claims");
            for name in partitions {
                claims.insert(name.clone(), self.token());
            }
            return Ok(vec![None; partitions.len()]);
        };
        let tokens: HashMap<String, String> = partitions
            .iter()
            .map(|p| (p.clone(), self.token()))
            .collect();
        let properties = tokens
            .iter()
            .map(|(p, token)| (claim_key(&self.stream, p), token.clone()))
            .collect();
        let table = table::append(
            catalog.as_ref(),
            &table,
            Vec::new(),
            HashMap::new(),
            properties,
        )
        .await?;
        let kept = table.metadata().properties();
        for (name, token) in &tokens {
            if kept.get(&claim_key(&self.stream, name)) != Some(token) {
                return Err(ConnectorError::fatal(format!(
                    "Iceberg: partition {name} was claimed by another instance of the stream \
                     at the same time"
                )));
            }
        }
        self.claims.lock().expect("claims").extend(tokens);
        Ok(partitions
            .iter()
            .map(|p| {
                kept.get(&position_key(&self.stream, p))
                    .and_then(|v| v.parse().ok())
            })
            .collect())
    }

    fn token(&self) -> String {
        format!(
            "{}-{}",
            self.instance,
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        )
    }
}

impl StartPositions for IcebergTransaction {
    /// Runs on the source's poll thread, or where the source is made: on a thread of its
    /// own with a runtime (and catalog client) of its own, as either may be inside the
    /// stream's runtime.
    fn claim(&self, partitions: &[String]) -> Result<Vec<Option<i64>>, ConnectorError> {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| ConnectorError::fatal(format!("Iceberg: {e}")))?
                        .block_on(self.claim_now(partitions))
                })
                .join()
                .unwrap_or_else(|_| {
                    Err(ConnectorError::fatal(
                        "Iceberg: claiming the stream's partitions panicked",
                    ))
                })
        })
    }
}

impl Transaction for IcebergTransaction {
    fn commit<'a>(&'a self, commits: &'a [Commit]) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let names = self.names.get().ok_or_else(|| {
                ConnectorError::fatal("Iceberg: the stream's source names no positions")
            })?;
            let named = names.named(commits)?;
            let written = std::mem::take(&mut *self.pending.lock().await);
            let Some(shared) = self.shared.get() else {
                // Nothing written yet, so no table to keep positions in: the messages up
                // to here made no rows, and reading them again makes none either.
                names.committed(&named);
                return Ok(());
            };
            let catalog = shared.catalog.clone();
            let table = catalog
                .load_table(&self.settings.ident)
                .await
                .map_err(|e| table::classify("loading the table", e))?;
            let kept = table.metadata().properties();
            let claims = self.claims.lock().expect("claims").clone();
            let mut properties = HashMap::new();
            for (name, next) in &named {
                let token = claims.get(name).ok_or_else(|| {
                    ConnectorError::fatal(format!(
                        "Iceberg: partition {name} was never claimed by this instance"
                    ))
                })?;
                let key = claim_key(&self.stream, name);
                match kept.get(&key) {
                    Some(other) if other != token => {
                        return Err(ConnectorError::fatal(format!(
                            "Iceberg: partition {name} is now read by another instance of the \
                             stream, so this instance's rows since its last commit were not \
                             committed"
                        )));
                    }
                    Some(_) => {}
                    None => {
                        properties.insert(key, token.clone());
                    }
                }
                properties.insert(position_key(&self.stream, name), next.to_string());
            }
            let mut files = Vec::new();
            let mut batches = Vec::new();
            for (f, name) in written {
                files.extend(f);
                batches.push(name);
            }
            let summary = HashMap::from([
                (SUMMARY_STREAM.to_string(), self.stream.clone()),
                (SUMMARY_BATCHES.to_string(), batches.join(",")),
            ]);
            let table = table::append(catalog.as_ref(), &table, files, summary, properties).await?;
            shared.advance(table)?;
            names.committed(&named);
            Ok(())
        })
    }

    fn abort(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            // Files written for the aborted commit are never part of the table.
            self.pending.lock().await.clear();
        })
    }
}

/// A sink that writes into its stream's [`IcebergTransaction`].
pub struct IcebergTransactionSink {
    transaction: Arc<IcebergTransaction>,
}

impl Sink for IcebergTransactionSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let values = values(batch)?;
            if values.is_empty() {
                return Ok(());
            }
            let shared = self.transaction.shared(&values).await?;
            let name = batch_name(meta);
            let files = shared.write(&values, &name).await?;
            self.transaction.pending.lock().await.push((files, name));
            Ok(())
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: false,
            transactional: true,
        }
    }
}
