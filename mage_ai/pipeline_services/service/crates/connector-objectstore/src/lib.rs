//! Object storage sink for streams: each batch becomes one object, Parquet or JSON lines, on
//! Amazon S3 and S3-compatible stores (MinIO, IBM COS with HMAC keys, Cloudflare R2), Google
//! Cloud Storage, Azure Blob Storage, or local disk.
//!
//! A batch is confirmed once its object is written, so nothing is acknowledged that is not
//! stored. Object names hold the stream, partition and position range,
//! `<prefix>/<stream>-p<partition>-<first>-<last>.<ext>`: a batch written again after a
//! restart replaces its object when it covers the same range. File size follows the stream's
//! batch limits (the export maps `buffer_size_mb` and `buffer_timeout_seconds` onto them).
//!
//! Parquet keeps values exact: the schema is inferred from each batch's JSON objects, and
//! top-level integers past 64 bits become `Decimal128(38, 0)`, or text past 38 digits. A
//! nested integer past 64 bits stops the write rather than losing digits; JSON lines keep
//! every value byte for byte.
//!
//! Settings: `url` (`s3://bucket/prefix`, `gs://bucket/prefix`, `az://container/prefix`,
//! `file:///path`), `format` (`parquet` or `jsonl`), `date_partition_format` (a folder per
//! date, such as `%Y/%m/%d`), and for S3-compatible stores `endpoint_url`, `region`.
//! Credentials come from the environment, as each cloud's tools read them.

pub mod blocking;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch as ArrowBatch;
use arrow_schema::{DataType, Field, Schema};
use mage_stream_core::Position;
use mage_stream_ports::{
    BatchMeta, BoxFuture, ConnectorError, RecordBatch, Sink, SinkCapabilities, read_envelope,
};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme, PutPayload};
use serde_json::Value;
use serde_json::value::RawValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Parquet,
    JsonLines,
}

impl Format {
    fn extension(self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::JsonLines => "jsonl",
        }
    }
}

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        _ => None,
    }
}

pub struct ObjectSink {
    store: Arc<dyn ObjectStore>,
    base: Path,
    format: Format,
    stream: String,
    date_format: Option<String>,
    label: String,
}

impl ObjectSink {
    /// `stream`: the stream's name, part of each object's name.
    pub fn new(config: &Value, stream: &str) -> Result<ObjectSink, ConnectorError> {
        let location = text(config, "url").ok_or_else(|| {
            ConnectorError::fatal(
                "Object storage settings need url: s3://bucket/prefix, gs://bucket/prefix, \
                 az://container/prefix or file:///path.",
            )
        })?;
        let format = match text(config, "format").as_deref().unwrap_or("parquet") {
            "parquet" => Format::Parquet,
            "jsonl" | "json" | "ndjson" => Format::JsonLines,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "Object storage format {other:?}: use parquet or jsonl."
                )));
            }
        };
        let url = url::Url::parse(&location)
            .map_err(|e| ConnectorError::fatal(format!("Object storage url {location}: {e}")))?;
        let (store, base) = open(&url, config)?;
        Ok(ObjectSink {
            store,
            base,
            format,
            stream: sanitize(stream),
            date_format: text(config, "date_partition_format"),
            label: location,
        })
    }

    fn object_path(&self, meta: &BatchMeta, now_seconds: u64) -> Path {
        let mut parts: Vec<String> = Vec::new();
        if let Some(format) = &self.date_format {
            parts.extend(
                format_date(format, now_seconds)
                    .split('/')
                    .filter(|p| !p.is_empty())
                    .map(sanitize),
            );
        }
        parts.push(format!(
            "{}-p{}-{}-{}.{}",
            self.stream,
            meta.id.partition.0,
            position_text(&meta.first),
            position_text(&meta.last),
            self.format.extension()
        ));
        let mut path = self.base.clone();
        for part in parts {
            path = path.clone().join(part);
        }
        path
    }
}

pub(crate) fn open(
    url: &url::Url,
    config: &Value,
) -> Result<(Arc<dyn ObjectStore>, Path), ConnectorError> {
    let (scheme, path) = ObjectStoreScheme::parse(url)
        .map_err(|e| ConnectorError::fatal(format!("Object storage url {url}: {e}")))?;
    let fail = |e: object_store::Error| ConnectorError::fatal(format!("Object storage {url}: {e}"));
    let store: Arc<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::AmazonS3 => {
            let mut builder = object_store::aws::AmazonS3Builder::from_env().with_url(url.as_str());
            if let Some(endpoint) = text(config, "endpoint_url") {
                builder = builder
                    .with_allow_http(endpoint.starts_with("http://"))
                    .with_endpoint(endpoint);
            }
            if let Some(region) = text(config, "region") {
                builder = builder.with_region(region);
            }
            Arc::new(builder.build().map_err(fail)?)
        }
        ObjectStoreScheme::GoogleCloudStorage => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_url(url.as_str())
                .build()
                .map_err(fail)?,
        ),
        ObjectStoreScheme::MicrosoftAzure => Arc::new(
            object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_url(url.as_str())
                .build()
                .map_err(fail)?,
        ),
        ObjectStoreScheme::Local => {
            std::fs::create_dir_all(url.path()).map_err(|e| {
                ConnectorError::fatal(format!("Object storage folder {}: {e}", url.path()))
            })?;
            let store =
                object_store::local::LocalFileSystem::new_with_prefix(url.path()).map_err(fail)?;
            return Ok((Arc::new(store), Path::default()));
        }
        other => {
            return Err(ConnectorError::fatal(format!(
                "Object storage url {url}: {other:?} is not supported; use s3://, gs://, az:// \
                 or file://"
            )));
        }
    };
    Ok((store, path))
}

/// Characters object names keep everywhere; others become `_`.
fn sanitize(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '=') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn position_text(position: &Position) -> String {
    match position {
        Position::Offset(o) => o.to_string(),
        Position::Lsn(l) => format!("{l:016X}"),
        Position::Tag(t) => t.to_string(),
        Position::Opaque(b) => b.iter().map(|x| format!("{x:02x}")).collect(),
    }
}

/// `%Y`, `%m`, `%d`, `%H` and `%M` of a UTC time; other text is kept.
pub fn format_date(format: &str, seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let secs = seconds % 86_400;
    // Civil date from days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format
        .replace("%Y", &format!("{year:04}"))
        .replace("%m", &format!("{month:02}"))
        .replace("%d", &format!("{day:02}"))
        .replace("%H", &format!("{:02}", secs / 3600))
        .replace("%M", &format!("{:02}", (secs % 3600) / 60))
}

/// A JSON lines body: each value as it is, one per line.
fn json_lines(values: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::with_capacity(values.iter().map(|v| v.len() + 1).sum());
    for value in values {
        body.extend_from_slice(value);
        body.push(b'\n');
    }
    body
}

/// A Parquet file of JSON objects, with integers past 64 bits kept exact.
pub fn parquet(values: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    if values.is_empty() {
        return Err("no rows".into());
    }
    // Top-level fields with integers past 64 bits, and the most digits each has.
    let mut wide: BTreeMap<String, usize> = BTreeMap::new();
    let mut parsed = Vec::with_capacity(values.len());
    for (row, value) in values.iter().enumerate() {
        let text = std::str::from_utf8(value).map_err(|_| format!("row {row} is not UTF-8"))?;
        let fields: BTreeMap<String, Box<RawValue>> = serde_json::from_str(text).map_err(|_| {
            format!("row {row} is not a JSON object; Parquet takes objects, jsonl takes any value")
        })?;
        for (name, raw) in &fields {
            let raw = raw.get();
            let oversized = mage_service_core::json::oversized_integers(raw);
            if oversized.is_empty() {
                continue;
            }
            if raw.starts_with('{') || raw.starts_with('[') {
                return Err(format!(
                    "row {row}: {name} holds an integer past 64 bits inside it ({}); Parquet \
                     would lose its digits, so write jsonl",
                    oversized[0]
                ));
            }
            let digits = raw.trim_start_matches('-').len();
            let entry = wide.entry(name.clone()).or_default();
            *entry = (*entry).max(digits);
        }
        parsed.push(serde_json::from_str::<Value>(text).map_err(|e| e.to_string())?);
    }
    let inferred = arrow_json::reader::infer_json_schema_from_iterator(parsed.iter().map(Ok))
        .map_err(|e| format!("the rows have no common schema: {e}"))?;
    let fields: Vec<Field> = inferred
        .fields()
        .iter()
        .map(|field| match wide.get(field.name()) {
            Some(digits) if *digits <= 38 => {
                Field::new(field.name(), DataType::Decimal128(38, 0), true)
            }
            Some(_) => Field::new(field.name(), DataType::Utf8, true),
            None => field.as_ref().clone(),
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
        .with_coerce_primitive(true)
        .with_batch_size(values.len())
        .build_decoder()
        .map_err(|e| e.to_string())?;
    let body = json_lines(values);
    decoder.decode(&body).map_err(|e| e.to_string())?;
    let batch: ArrowBatch = decoder
        .flush()
        .map_err(|e| e.to_string())?
        .ok_or("no rows decoded")?;
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();
    let mut out = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut out, schema, Some(properties))
        .map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(out)
}

impl Sink for ObjectSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let rows = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
            if rows.is_empty() {
                return Ok(());
            }
            let values: Vec<Vec<u8>> = rows.into_iter().map(|r| r.value).collect();
            let body = match self.format {
                Format::JsonLines => json_lines(&values),
                Format::Parquet => {
                    let values = values.clone();
                    tokio::task::spawn_blocking(move || parquet(&values))
                        .await
                        .map_err(|e| ConnectorError::fatal(e.to_string()))?
                        .map_err(|e| ConnectorError::fatal(format!("Object storage: {e}")))?
                }
            };
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let path = self.object_path(meta, now);
            self.store
                .put(&path, PutPayload::from(body))
                .await
                .map_err(|e| {
                    let message = format!("Object storage {} writing {path}: {e}", self.label);
                    match e {
                        object_store::Error::PermissionDenied { .. }
                        | object_store::Error::Unauthenticated { .. }
                        | object_store::Error::NotFound { .. } => ConnectorError::fatal(message),
                        _ => ConnectorError::retryable(message),
                    }
                })?;
            Ok(())
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: true,
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use mage_stream_core::{AssignmentEpoch, BatchId, PartitionId};
    use mage_stream_ports::{Envelope, EnvelopeBuilder};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;

    fn batch(values: &[&str]) -> (RecordBatch, BatchMeta) {
        let mut builder = EnvelopeBuilder::new();
        for v in values {
            builder.push(&Envelope {
                value: v.as_bytes().to_vec(),
                ..Envelope::default()
            });
        }
        let meta = BatchMeta {
            id: BatchId {
                partition: PartitionId(2),
                epoch: AssignmentEpoch(1),
                seq: 0,
            },
            first: Position::Offset(100),
            last: Position::Offset(199),
            records: values.len() as u32,
            bytes: 0,
        };
        (builder.finish().unwrap(), meta)
    }

    fn read(bytes: Vec<u8>) -> ArrowBatch {
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
    }

    #[test]
    fn parquet_keeps_integers_past_64_bits_and_unicode() {
        let file = parquet(&[
            r#"{"id": 1, "big": 123456789012345678901234567890, "city": "Zürich 🚀", "x": 0.1}"#
                .as_bytes()
                .to_vec(),
            br#"{"id": 2, "big": -9223372036854775809, "city": null, "x": 2}"#.to_vec(),
        ])
        .unwrap();
        let batch = read(file);
        let big = batch
            .column_by_name("big")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(big.value(0), 123_456_789_012_345_678_901_234_567_890_i128);
        assert_eq!(big.value(1), -9_223_372_036_854_775_809_i128);
        let city = batch
            .column_by_name("city")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(city.value(0), "Zürich 🚀");
        assert!(city.is_null(1));
    }

    #[test]
    fn parquet_refuses_what_it_would_change() {
        let nested = parquet(&[br#"{"a": {"b": 123456789012345678901234567890}}"#.to_vec()]);
        assert!(nested.unwrap_err().contains("write jsonl"));
        let scalar = parquet(&[br#""text""#.to_vec()]);
        assert!(scalar.unwrap_err().contains("not a JSON object"));
        let huge = parquet(&[format!(r#"{{"n": {}}}"#, "9".repeat(45)).into_bytes()]).unwrap();
        let column = read(huge).column_by_name("n").unwrap().data_type().clone();
        assert_eq!(column, DataType::Utf8);
    }

    #[test]
    fn dates_format_in_utc() {
        assert_eq!(format_date("%Y/%m/%d/%H", 0), "1970/01/01/00");
        assert_eq!(format_date("dt=%Y-%m-%d", 1_791_894_645), "dt=2026-10-13");
    }

    #[tokio::test]
    async fn batches_become_objects_named_by_their_range() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().join("lake").display());
        for (format, ext) in [("parquet", "parquet"), ("jsonl", "jsonl")] {
            let sink = ObjectSink::new(
                &json!({"url": url, "format": format, "date_partition_format": "dt=%Y"}),
                "orders stream",
            )
            .unwrap();
            let (records, meta) = batch(&[r#"{"id": 1}"#, r#"{"id": 2, "x": 2.5e10}"#]);
            sink.write(&records, &meta).await.unwrap();
            let year = format_date(
                "%Y",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            let written = dir
                .path()
                .join("lake")
                .join(format!("dt={year}"))
                .join(format!("orders_stream-p2-100-199.{ext}"));
            assert!(written.is_file(), "{written:?}");
            if ext == "jsonl" {
                assert_eq!(
                    std::fs::read_to_string(&written).unwrap(),
                    "{\"id\": 1}\n{\"id\": 2, \"x\": 2.5e10}\n"
                );
            }
        }
    }

    #[test]
    fn settings_are_checked() {
        assert!(ObjectSink::new(&json!({}), "s").is_err());
        assert!(ObjectSink::new(&json!({"url": "ftp://x/y"}), "s").is_err());
        assert!(ObjectSink::new(&json!({"url": "file:///tmp/x", "format": "csv"}), "s").is_err());
    }
}
