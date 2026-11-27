//! Apache Iceberg sink for streams (ADR-0042), on iceberg-rust over a REST catalog (Polaris,
//! Lakekeeper, Nessie, Unity, AWS Glue and S3 Tables endpoints, Cloudflare R2).
//!
//! Each batch's messages (JSON objects) are decoded into the table's columns and written as
//! Parquet data files, one per partition the batch has rows in. A committer appends the files
//! of every batch ready since its last commit in one snapshot, and a batch is confirmed when
//! the commit with its files lands, so the source's positions never pass rows the table does
//! not have. A failed commit fails its batches, which are written again; files of a failed
//! commit are never part of the table.
//!
//! Under load a commit holds several batches; an idle stream commits each batch alone. The
//! stream's batch limits set the size of commits and files: for a table, batches of tens of
//! thousands of messages or tens of seconds keep snapshots and files few.
//!
//! Exactly-once from Kafka: [`transaction::IcebergTransaction`].
//!
//! Settings: `catalog` (`type: rest` and the catalog's properties: `uri`, `warehouse`,
//! `token` or `credential`, `oauth2-server-uri`, `scope`, `header.*`), `table`
//! (`namespace.table`), and to create a missing table from the first messages
//! `create_table: true`, `columns` (types that override the inferred ones) and
//! `partition_by` (`[day(ts), bucket(16, id)]`).

pub mod storage;
mod table;
pub mod transaction;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use iceberg::Catalog;
use iceberg::spec::DataFile;
use mage_stream_core::Position;
use mage_stream_ports::{
    BatchMeta, BoxFuture, Confirmation, ConnectorError, RecordBatch, Sink, SinkCapabilities,
    read_envelope,
};
use serde_json::Value;
use tokio::sync::{OnceCell, mpsc, oneshot};

use table::{Settings, Version};

/// Snapshot summary keys: the stream that committed it and the batches in it.
pub const SUMMARY_STREAM: &str = "mageml.stream";
pub const SUMMARY_BATCHES: &str = "mageml.batches";

fn position_text(position: &Position) -> String {
    match position {
        Position::Offset(o) => o.to_string(),
        Position::Lsn(l) => format!("{l:016X}"),
        Position::Tag(t) => t.to_string(),
        Position::Opaque(b) => b.iter().map(|x| format!("{x:02x}")).collect(),
    }
}

/// A batch's name in data file names and snapshot summaries.
fn batch_name(meta: &BatchMeta) -> String {
    format!(
        "p{}-{}-{}",
        meta.id.partition.0,
        position_text(&meta.first),
        position_text(&meta.last)
    )
}

/// The batch's messages, the JSON objects to write.
fn values(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, ConnectorError> {
    Ok(read_envelope(batch)
        .map_err(|e| ConnectorError::fatal(e.to_string()))?
        .into_iter()
        .map(|m| m.value)
        .collect())
}

/// The table at its latest version, shared by the writes and the commits.
pub(crate) struct Shared {
    catalog: Arc<dyn Catalog>,
    version: Mutex<Arc<Version>>,
}

impl Shared {
    async fn open(settings: &Settings, first: &[Vec<u8>]) -> Result<Shared, ConnectorError> {
        let catalog = settings.catalog().await?;
        let table = table::open(catalog.as_ref(), settings, first).await?;
        Ok(Shared {
            catalog,
            version: Mutex::new(Arc::new(Version::new(table)?)),
        })
    }

    fn current(&self) -> Arc<Version> {
        self.version.lock().expect("version").clone()
    }

    /// The table after a commit; its schema is read again only when it changed.
    fn advance(&self, table: iceberg::table::Table) -> Result<(), ConnectorError> {
        let mut version = self.version.lock().expect("version");
        let same_schema = version.table.metadata().current_schema_id()
            == table.metadata().current_schema_id()
            && version.table.metadata().default_partition_spec_id()
                == table.metadata().default_partition_spec_id();
        *version = if same_schema {
            Arc::new(version.with_table(table))
        } else {
            Arc::new(Version::new(table)?)
        };
        Ok(())
    }

    /// The batch's rows as data files of the table.
    async fn write(&self, values: &[Vec<u8>], name: &str) -> Result<Vec<DataFile>, ConnectorError> {
        let version = self.current();
        let batch = version.decode(values)?;
        version.write(batch, name).await
    }
}

enum Job {
    Files {
        files: Vec<DataFile>,
        name: String,
        reply: oneshot::Sender<Result<(), ConnectorError>>,
    },
    Flush(oneshot::Sender<()>),
}

struct Running {
    shared: Arc<Shared>,
    jobs: mpsc::UnboundedSender<Job>,
}

pub struct IcebergSink {
    settings: Settings,
    scope: String,
    running: OnceCell<Running>,
}

impl IcebergSink {
    /// `scope`: the stream (and sink) the commits name in their summaries.
    pub fn new(config: &Value, scope: &str) -> Result<IcebergSink, ConnectorError> {
        Ok(IcebergSink {
            settings: Settings::new(config)?,
            scope: scope.to_string(),
            running: OnceCell::new(),
        })
    }

    pub fn label(&self) -> String {
        self.settings.label()
    }

    /// Opens the table (making it from `first` when allowed) and starts the committer.
    async fn running(&self, first: &[Vec<u8>]) -> Result<&Running, ConnectorError> {
        self.running
            .get_or_try_init(|| async {
                let shared = Arc::new(Shared::open(&self.settings, first).await?);
                let (jobs, rx) = mpsc::unbounded_channel();
                tokio::spawn(commit_loop(shared.clone(), self.scope.clone(), rx));
                Ok(Running { shared, jobs })
            })
            .await
    }
}

/// Commits the files of every batch waiting, in one snapshot, and answers each batch.
async fn commit_loop(shared: Arc<Shared>, scope: String, mut jobs: mpsc::UnboundedReceiver<Job>) {
    while let Some(first) = jobs.recv().await {
        let mut waiting = vec![first];
        while let Ok(job) = jobs.try_recv() {
            waiting.push(job);
        }
        let mut files = Vec::new();
        let mut names = Vec::new();
        let mut replies = Vec::new();
        let mut flushes = Vec::new();
        for job in waiting {
            match job {
                Job::Files {
                    files: f,
                    name,
                    reply,
                } => {
                    files.extend(f);
                    names.push(name);
                    replies.push(reply);
                }
                Job::Flush(reply) => flushes.push(reply),
            }
        }
        if !replies.is_empty() {
            let outcome = commit(&shared, &scope, files, &names).await;
            for reply in replies {
                let _ = reply.send(outcome.clone());
            }
        }
        for reply in flushes {
            let _ = reply.send(());
        }
    }
}

async fn commit(
    shared: &Shared,
    scope: &str,
    files: Vec<DataFile>,
    names: &[String],
) -> Result<(), ConnectorError> {
    if files.is_empty() {
        return Ok(());
    }
    let summary = HashMap::from([
        (SUMMARY_STREAM.to_string(), scope.to_string()),
        (SUMMARY_BATCHES.to_string(), names.join(",")),
    ]);
    let table = shared.current().table.clone();
    match table::append(
        shared.catalog.as_ref(),
        &table,
        files,
        summary,
        HashMap::new(),
    )
    .await
    {
        Ok(table) => shared.advance(table),
        Err(error) => {
            // The batches are written again with the table as it is now (another writer
            // may have changed its schema or partitions).
            if let Ok(latest) = shared.catalog.load_table(table.identifier()).await {
                shared.advance(latest)?;
            }
            Err(error)
        }
    }
}

impl Sink for IcebergSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move { self.queue(batch, meta).await?.await })
    }

    /// Writes the batch's data files now; the confirmation is the commit that holds them.
    fn queue<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<Confirmation, ConnectorError>> {
        Box::pin(async move {
            let values = values(batch)?;
            if values.is_empty() {
                return Ok(Box::pin(async { Ok(()) }) as Confirmation);
            }
            let running = self.running(&values).await?;
            let name = batch_name(meta);
            let files = running.shared.write(&values, &name).await?;
            let (reply, answer) = oneshot::channel();
            running
                .jobs
                .send(Job::Files { files, name, reply })
                .map_err(|_| ConnectorError::fatal("Iceberg: the committer stopped"))?;
            Ok(Box::pin(async move {
                answer.await.unwrap_or_else(|_| {
                    Err(ConnectorError::retryable("Iceberg: the committer stopped"))
                })
            }) as Confirmation)
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Some(running) = self.running.get() {
                let (reply, done) = oneshot::channel();
                if running.jobs.send(Job::Flush(reply)).is_ok() {
                    let _ = done.await;
                }
            }
        })
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::default()
    }
}

/// Checks the catalog and the table: `mage-service check-connections`.
pub async fn check(config: &Value) -> Result<String, ConnectorError> {
    let settings = Settings::new(config)?;
    let catalog = settings.catalog().await?;
    match catalog.load_table(&settings.ident).await {
        Ok(table) => Ok(format!(
            "table {} with {} columns",
            settings.ident,
            table.metadata().current_schema().as_struct().fields().len()
        )),
        Err(e) if table::missing(&e) && settings.create => Ok(format!(
            "catalog reached; table {} will be created from the first messages",
            settings.ident
        )),
        Err(e) => Err(table::classify("loading the table", e)),
    }
}

#[cfg(test)]
mod tests;
