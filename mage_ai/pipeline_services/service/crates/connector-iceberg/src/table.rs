//! The table a stream writes: its catalog, its schema as Arrow, messages decoded into it, and
//! data files written for it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataFile, DataFileFormat, NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use mage_stream_ports::ConnectorError;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::Value;

use crate::storage::ObjectStorageFactory;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CatalogKind {
    Rest,
    /// In this process only: for tests and trying a stream out.
    Memory,
}

/// A sink's settings (`config` of the manifest's connector).
#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub kind: CatalogKind,
    /// The catalog's properties as iceberg names them (`uri`, `warehouse`, `token`,
    /// `credential`, `oauth2-server-uri`, `scope`, `header.*`, ...).
    pub props: HashMap<String, String>,
    pub ident: TableIdent,
    pub create: bool,
    /// Column types that override the inferred ones when the table is created.
    pub columns: BTreeMap<String, String>,
    pub partition_by: Vec<String>,
}

fn fatal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::fatal(message)
}

/// An iceberg error as the stream treats it: the catalog or the storage unreachable is
/// retried; anything else stops the stream.
pub(crate) fn classify(doing: &str, error: iceberg::Error) -> ConnectorError {
    let message = format!("Iceberg: {doing}: {error}");
    match error.kind() {
        ErrorKind::DataInvalid
        | ErrorKind::FeatureUnsupported
        | ErrorKind::NamespaceNotFound
        | ErrorKind::TableNotFound
        | ErrorKind::PreconditionFailed => fatal(message),
        _ => ConnectorError::retryable(message),
    }
}

impl Settings {
    pub fn new(config: &Value) -> Result<Settings, ConnectorError> {
        let catalog = config
            .get("catalog")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                fatal(
                    "Iceberg settings need catalog: its type (rest) and properties, such as uri \
                     and warehouse.",
                )
            })?;
        let mut props = HashMap::new();
        for (key, value) in catalog {
            let text = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => continue,
                _ => {
                    return Err(fatal(format!(
                        "Iceberg catalog property {key} is not text."
                    )));
                }
            };
            props.insert(key.clone(), text);
        }
        let kind = match props.remove("type").as_deref().unwrap_or("rest") {
            "rest" => CatalogKind::Rest,
            "memory" => CatalogKind::Memory,
            other => {
                return Err(fatal(format!(
                    "Iceberg catalog type {other}: MageML's services use REST catalogs (rest)."
                )));
            }
        };
        if kind == CatalogKind::Rest && !props.contains_key("uri") {
            return Err(fatal(
                "Iceberg settings need catalog.uri, the REST catalog's URL.",
            ));
        }
        let table = config
            .get("table")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| fatal("Iceberg settings need table, such as analytics.events."))?;
        let parts: Vec<&str> = table.split('.').collect();
        if parts.len() < 2 || parts.iter().any(|p| p.is_empty()) {
            return Err(fatal(format!(
                "Iceberg table {table}: name it with its namespace, such as analytics.events."
            )));
        }
        let ident = TableIdent::from_strs(&parts)
            .map_err(|e| fatal(format!("Iceberg table {table}: {e}")))?;
        let columns = match config.get("columns") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(Value::Object(columns)) => columns
                .iter()
                .map(|(name, t)| match t.as_str() {
                    Some(t) => Ok((name.clone(), t.trim().to_string())),
                    None => Err(fatal(format!("Iceberg column {name}: its type is text."))),
                })
                .collect::<Result<_, _>>()?,
            Some(_) => return Err(fatal("Iceberg columns: a map of column to type.")),
        };
        let partition_by = match config.get("partition_by") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::String(one)) => vec![one.clone()],
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| fatal("Iceberg partition_by: a list of text."))
                })
                .collect::<Result<_, _>>()?,
            Some(_) => return Err(fatal("Iceberg partition_by: a list, such as [day(ts)].")),
        };
        let partition_by = rejoined(partition_by);
        for field in &partition_by {
            partition_field(field)?;
        }
        for (name, t) in &columns {
            primitive(name, t)?;
        }
        Ok(Settings {
            kind,
            props,
            ident,
            create: config
                .get("create_table")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            columns,
            partition_by,
        })
    }

    pub fn label(&self) -> String {
        let at = match self.kind {
            CatalogKind::Rest => self.props.get("uri").cloned().unwrap_or_default(),
            CatalogKind::Memory => "memory".to_string(),
        };
        format!("{} at {at}", self.ident)
    }

    /// The catalog, made in the runtime that calls this (its connections live there).
    pub async fn catalog(&self) -> Result<Arc<dyn Catalog>, ConnectorError> {
        match self.kind {
            CatalogKind::Rest => {
                let catalog = RestCatalogBuilder::default()
                    .with_storage_factory(Arc::new(ObjectStorageFactory))
                    .load("mageml", self.props.clone())
                    .await
                    .map_err(|e| classify("connecting to the catalog", e))?;
                Ok(Arc::new(catalog))
            }
            CatalogKind::Memory => {
                static CATALOGS: OnceLock<Mutex<HashMap<String, Arc<dyn Catalog>>>> =
                    OnceLock::new();
                let warehouse = self
                    .props
                    .get("warehouse")
                    .cloned()
                    .ok_or_else(|| fatal("An Iceberg memory catalog needs warehouse."))?;
                let existing = CATALOGS
                    .get_or_init(Default::default)
                    .lock()
                    .expect("catalogs")
                    .get(&warehouse)
                    .cloned();
                if let Some(catalog) = existing {
                    return Ok(catalog);
                }
                // Catalogs run their work on the runtime they were made in; a claim makes
                // one in a runtime of its own that ends with it.
                static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
                let runtime = RUNTIME.get_or_init(|| {
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .expect("a runtime for memory catalogs")
                });
                let catalog: Arc<dyn Catalog> = Arc::new(
                    MemoryCatalogBuilder::default()
                        .with_storage_factory(Arc::new(ObjectStorageFactory))
                        .with_runtime(iceberg::Runtime::new(runtime))
                        .load(
                            "mageml",
                            HashMap::from([(
                                MEMORY_CATALOG_WAREHOUSE.to_string(),
                                warehouse.clone(),
                            )]),
                        )
                        .await
                        .map_err(|e| classify("making the memory catalog", e))?,
                );
                Ok(CATALOGS
                    .get_or_init(Default::default)
                    .lock()
                    .expect("catalogs")
                    .entry(warehouse)
                    .or_insert(catalog)
                    .clone())
            }
        }
    }
}

/// A YAML flow list (`[bucket(16, id)]`) splits a call at its comma: the parts joined again.
fn rejoined(items: Vec<String>) -> Vec<String> {
    let mut joined: Vec<String> = Vec::new();
    for item in items {
        match joined.last_mut() {
            Some(open) if open.contains('(') && !open.contains(')') => {
                open.push_str(", ");
                open.push_str(item.trim());
            }
            _ => joined.push(item.trim().to_string()),
        }
    }
    joined
}

/// `day(ts)`, `bucket(16, id)`, `truncate(4, name)` or a column: (transform, column).
fn partition_field(text: &str) -> Result<(Transform, String), ConnectorError> {
    let text = text.trim();
    let bad = || {
        fatal(format!(
            "Iceberg partition_by {text:?}: a column, or year, month, day or hour of a time \
             column (day(ts)), bucket(16, id), or truncate(4, name)."
        ))
    };
    let Some(open) = text.find('(') else {
        return if text.is_empty() {
            Err(bad())
        } else {
            Ok((Transform::Identity, text.to_string()))
        };
    };
    let name = text[..open].trim();
    let inner = text[open + 1..].strip_suffix(')').ok_or_else(bad)?;
    let args: Vec<&str> = inner.split(',').map(str::trim).collect();
    match (name, args.as_slice()) {
        ("year" | "month" | "day" | "hour" | "identity", [column]) if !column.is_empty() => {
            Ok((name.parse().map_err(|_| bad())?, column.to_string()))
        }
        ("bucket", [n, column]) => Ok((
            Transform::Bucket(n.parse().map_err(|_| bad())?),
            column.to_string(),
        )),
        ("truncate", [n, column]) => Ok((
            Transform::Truncate(n.parse().map_err(|_| bad())?),
            column.to_string(),
        )),
        _ => Err(bad()),
    }
}

fn primitive(column: &str, text: &str) -> Result<PrimitiveType, ConnectorError> {
    let alias = match text {
        "bigint" | "integer" => "long",
        "text" | "varchar" => "string",
        "bool" => "boolean",
        other => other,
    };
    serde_json::from_value(Value::String(alias.to_string())).map_err(|_| {
        fatal(format!(
            "Iceberg column {column}: {text} is not a type; use long, int, double, float, \
             string, boolean, date, timestamp, timestamptz, or decimal(10, 2)."
        ))
    })
}

/// A table at one version, with what writing to it needs.
pub(crate) struct Version {
    pub table: Table,
    pub arrow: SchemaRef,
    splitter: Option<Arc<RecordBatchPartitionSplitter>>,
    /// Text columns: a list or an object in one is written as its JSON text.
    text_columns: Vec<String>,
}

impl Version {
    pub fn new(table: Table) -> Result<Version, ConnectorError> {
        let schema = table.metadata().current_schema().clone();
        let arrow = Arc::new(
            schema_to_arrow_schema(&schema)
                .map_err(|e| classify("reading the table's schema", e))?,
        );
        let spec = table.metadata().default_partition_spec().clone();
        let splitter = if spec.is_unpartitioned() {
            None
        } else {
            Some(Arc::new(
                RecordBatchPartitionSplitter::try_new_with_computed_values(schema, spec)
                    .map_err(|e| classify("reading the table's partitions", e))?,
            ))
        };
        // Every column must decode from JSON; a type that cannot stops here, not at a batch.
        arrow_json::ReaderBuilder::new(arrow.clone())
            .build_decoder()
            .map_err(|e| {
                fatal(format!(
                    "Iceberg: {} has a column MageML cannot write from JSON: {e}",
                    table.identifier()
                ))
            })?;
        let text_columns = arrow
            .fields()
            .iter()
            .filter(|f| matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
            .map(|f| f.name().clone())
            .collect();
        Ok(Version {
            table,
            arrow,
            splitter,
            text_columns,
        })
    }

    /// The same columns at a newer version of the table.
    pub fn with_table(&self, table: Table) -> Version {
        Version {
            table,
            arrow: self.arrow.clone(),
            splitter: self.splitter.clone(),
            text_columns: self.text_columns.clone(),
        }
    }

    /// The messages (JSON objects) as one batch of the table's columns: fields the table
    /// does not have are left out, columns a message does not have are null.
    pub fn decode(&self, values: &[Vec<u8>]) -> Result<RecordBatch, ConnectorError> {
        let mut decoder = arrow_json::ReaderBuilder::new(self.arrow.clone())
            .with_batch_size(values.len().max(1))
            .with_coerce_primitive(true)
            .build_decoder()
            .map_err(|e| fatal(format!("Iceberg: {e}")))?;
        let mut text = Vec::with_capacity(values.iter().map(|v| v.len() + 1).sum());
        for value in values {
            match self.nested_as_text(value)? {
                Some(rewritten) => text.extend_from_slice(&rewritten),
                None => text.extend_from_slice(value),
            }
            text.push(b'\n');
        }
        let mut rest = &text[..];
        while !rest.is_empty() {
            let read = decoder
                .decode(rest)
                .map_err(|e| fatal(format!("Iceberg: a message does not fit the table: {e}")))?;
            if read == 0 {
                break;
            }
            rest = &rest[read..];
        }
        decoder
            .flush()
            .map_err(|e| fatal(format!("Iceberg: a message does not fit the table: {e}")))?
            .ok_or_else(|| fatal("Iceberg: no rows decoded"))
    }

    /// The message with the lists and objects of its text columns as their JSON text; None
    /// when it has none (most messages: only those with a nested value are parsed).
    fn nested_as_text(&self, value: &[u8]) -> Result<Option<Vec<u8>>, ConnectorError> {
        let nested = value.iter().filter(|b| matches!(b, b'{' | b'[')).count() > 1;
        if !nested || self.text_columns.is_empty() {
            return Ok(None);
        }
        let mut object: serde_json::Map<String, Value> = serde_json::from_slice(value)
            .map_err(|_| fatal("Iceberg: a message is not a JSON object; tables take objects."))?;
        let mut changed = false;
        for name in &self.text_columns {
            if let Some(v) = object.get_mut(name)
                && (v.is_array() || v.is_object())
            {
                *v = Value::String(v.to_string());
                changed = true;
            }
        }
        if !changed {
            return Ok(None);
        }
        serde_json::to_vec(&object)
            .map(Some)
            .map_err(|e| fatal(format!("Iceberg: {e}")))
    }

    /// The batch as Parquet data files of the table, one per partition it has rows in;
    /// `name` goes in each file's name.
    pub async fn write(
        &self,
        batch: RecordBatch,
        name: &str,
    ) -> Result<Vec<DataFile>, ConnectorError> {
        let metadata = self.table.metadata();
        let location = DefaultLocationGenerator::new(metadata)
            .map_err(|e| classify("placing data files", e))?;
        let names = DefaultFileNameGenerator::new(
            "mageml".to_string(),
            Some(format!(
                "{name}-{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            )),
            DataFileFormat::Parquet,
        );
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let parquet = ParquetWriterBuilder::new(properties, metadata.current_schema().clone());
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            self.table.file_io().clone(),
            location,
            names,
        );
        let builder = DataFileWriterBuilder::new(rolling);
        let writing = async {
            match &self.splitter {
                None => {
                    let mut writer = builder.build(None).await?;
                    writer.write(batch).await?;
                    writer.close().await
                }
                Some(splitter) => {
                    let mut writer = FanoutWriter::new(builder);
                    for (key, part) in splitter.split(&batch)? {
                        writer.write(key, part).await?;
                    }
                    writer.close().await
                }
            }
        };
        writing.await.map_err(|e| classify("writing data files", e))
    }
}

/// The table or its namespace does not exist.
pub(crate) fn missing(error: &iceberg::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
    )
}

/// A JSON object's fields in the order they are written.
struct Fields(Vec<(String, Value)>);

impl<'de> serde::Deserialize<'de> for Fields {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Fields, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Fields;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Fields, A::Error> {
                let mut fields = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    fields.push(entry);
                }
                Ok(Fields(fields))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

/// The table, created from the first messages when `create_table` is on and it does not exist.
pub(crate) async fn open(
    catalog: &dyn Catalog,
    settings: &Settings,
    first: &[Vec<u8>],
) -> Result<Table, ConnectorError> {
    match catalog.load_table(&settings.ident).await {
        Ok(table) => return Ok(table),
        Err(e) if missing(&e) && settings.create => {}
        Err(e) if missing(&e) => {
            return Err(fatal(format!(
                "Iceberg table {} does not exist; create it, or set create_table: true to \
                 create it from the first messages.",
                settings.ident
            )));
        }
        Err(e) => return Err(classify("loading the table", e)),
    }
    let schema = inferred_schema(settings, first)?;
    let mut spec = PartitionSpec::builder(schema.clone());
    for field in &settings.partition_by {
        let (transform, column) = partition_field(field)?;
        if schema.field_by_name(&column).is_none() {
            return Err(fatal(format!(
                "Iceberg partition_by {field}: the first messages have no {column}; give its \
                 type under columns."
            )));
        }
        let target = match transform {
            Transform::Identity => column.clone(),
            Transform::Bucket(_) => format!("{column}_bucket"),
            Transform::Truncate(_) => format!("{column}_trunc"),
            other => format!("{column}_{other}"),
        };
        spec = spec
            .add_partition_field(&column, target, transform)
            .map_err(|e| fatal(format!("Iceberg partition_by {field}: {e}")))?;
    }
    let spec = spec
        .build()
        .map_err(|e| fatal(format!("Iceberg partition_by: {e}")))?;
    let namespace = settings.ident.namespace().clone();
    if !catalog
        .namespace_exists(&namespace)
        .await
        .map_err(|e| classify("reading the namespace", e))?
    {
        match catalog.create_namespace(&namespace, HashMap::new()).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::NamespaceAlreadyExists => {}
            Err(e) => return Err(classify("creating the namespace", e)),
        }
    }
    let creation = TableCreation::builder()
        .name(settings.ident.name().to_string())
        .schema(schema)
        .partition_spec(spec)
        .build();
    match catalog.create_table(&namespace, creation).await {
        Ok(table) => Ok(table),
        // Another instance made it first.
        Err(e) if e.kind() == ErrorKind::TableAlreadyExists => catalog
            .load_table(&settings.ident)
            .await
            .map_err(|e| classify("loading the table", e)),
        Err(e) => Err(classify("creating the table", e)),
    }
}

/// A schema from the messages: integers as long, other numbers as double, text, booleans;
/// lists and objects as their JSON text; `columns` overrides any of them.
fn inferred_schema(settings: &Settings, first: &[Vec<u8>]) -> Result<Schema, ConnectorError> {
    let mut seen: Vec<(String, PrimitiveType)> = Vec::new();
    for value in first {
        let Fields(object) = serde_json::from_slice(value)
            .map_err(|_| fatal("Iceberg: a message is not a JSON object; tables take objects."))?;
        for (name, v) in object {
            let t = match v {
                Value::Bool(_) => PrimitiveType::Boolean,
                Value::Number(n) if n.is_i64() || n.is_u64() => PrimitiveType::Long,
                Value::Number(_) => PrimitiveType::Double,
                Value::Null => continue,
                _ => PrimitiveType::String,
            };
            match seen.iter_mut().find(|(n, _)| *n == name) {
                None => seen.push((name, t)),
                Some((_, known)) if *known == t => {}
                Some((_, known))
                    if matches!((&*known, &t), (PrimitiveType::Long, PrimitiveType::Double)) =>
                {
                    *known = PrimitiveType::Double
                }
                Some((_, known))
                    if matches!((&*known, &t), (PrimitiveType::Double, PrimitiveType::Long)) => {}
                Some((_, known)) => *known = PrimitiveType::String,
            }
        }
    }
    for name in settings.columns.keys() {
        if !seen.iter().any(|(n, _)| n == name) {
            seen.push((name.clone(), PrimitiveType::String));
        }
    }
    if seen.is_empty() {
        return Err(fatal(
            "Iceberg: the first messages have no fields to make the table's columns of.",
        ));
    }
    let fields = seen
        .into_iter()
        .enumerate()
        .map(|(i, (name, t))| {
            let t = match settings.columns.get(&name) {
                Some(declared) => primitive(&name, declared)?,
                None => t,
            };
            Ok(NestedField::optional(i as i32 + 1, name, Type::Primitive(t)).into())
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| fatal(format!("Iceberg: the table's schema: {e}")))
}

/// Appends data files in one commit, with `summary` in the snapshot's summary and
/// `properties` set on the table in the same commit; returns the table after.
pub(crate) async fn append(
    catalog: &dyn Catalog,
    table: &Table,
    files: Vec<DataFile>,
    summary: HashMap<String, String>,
    properties: HashMap<String, String>,
) -> Result<Table, ConnectorError> {
    let mut tx = Transaction::new(table);
    if !files.is_empty() {
        tx = tx
            .fast_append()
            .add_data_files(files)
            .set_snapshot_properties(summary)
            .apply(tx)
            .map_err(|e| classify("preparing the commit", e))?;
    }
    if !properties.is_empty() {
        let mut update = tx.update_table_properties();
        for (key, value) in properties {
            update = update.set(key, value);
        }
        tx = update
            .apply(tx)
            .map_err(|e| classify("preparing the commit", e))?;
    }
    tx.commit(catalog)
        .await
        .map_err(|e| classify("committing", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_fields_read_as_transforms_of_columns() {
        assert_eq!(
            partition_field("region").unwrap(),
            (Transform::Identity, "region".into())
        );
        assert_eq!(
            partition_field("day(ts)").unwrap(),
            (Transform::Day, "ts".into())
        );
        assert_eq!(
            partition_field("bucket(16, id)").unwrap(),
            (Transform::Bucket(16), "id".into())
        );
        assert_eq!(
            partition_field("truncate(4,name)").unwrap(),
            (Transform::Truncate(4), "name".into())
        );
        for bad in ["", "day()", "bucket(id)", "week(ts)", "day(ts"] {
            assert!(partition_field(bad).is_err(), "{bad}");
        }
        // As YAML reads [day(ts), bucket(16, id)].
        assert_eq!(
            rejoined(
                ["day(ts)", "bucket(16", " id)", "region"]
                    .map(String::from)
                    .to_vec()
            ),
            ["day(ts)", "bucket(16, id)", "region"]
        );
    }

    #[test]
    fn settings_need_a_catalog_and_a_table_with_its_namespace() {
        let error = |config: Value| Settings::new(&config).err().unwrap().message;
        assert!(error(serde_json::json!({"table": "a.b"})).contains("need catalog"));
        assert!(
            error(serde_json::json!({"catalog": {"type": "rest"}, "table": "a.b"}))
                .contains("catalog.uri")
        );
        assert!(
            error(serde_json::json!({"catalog": {"uri": "http://c"}, "table": "events"}))
                .contains("with its namespace")
        );
        assert!(
            error(serde_json::json!({"catalog": {"type": "hive"}, "table": "a.b"}))
                .contains("REST catalogs")
        );
        assert!(
            error(serde_json::json!({
                "catalog": {"uri": "http://c"}, "table": "a.b", "columns": {"ts": "datetime"}
            }))
            .contains("not a type")
        );
        let settings = Settings::new(&serde_json::json!({
            "catalog": {"uri": "http://c", "warehouse": "lake", "token": "t"},
            "table": "lake.analytics.events",
            "create_table": true,
            "columns": {"ts": "timestamptz", "n": "bigint"},
            "partition_by": ["day(ts)"],
        }))
        .unwrap();
        assert_eq!(settings.kind, CatalogKind::Rest);
        assert_eq!(settings.props.get("token").map(String::as_str), Some("t"));
        assert_eq!(settings.ident.to_string(), "lake.analytics.events");
        assert!(settings.create);
    }

    #[test]
    fn the_schema_is_inferred_and_declared_columns_win() {
        let settings = Settings::new(&serde_json::json!({
            "catalog": {"uri": "http://c"}, "table": "a.b",
            "columns": {"ts": "timestamptz", "missing": "date"},
        }))
        .unwrap();
        let first = [
            br#"{"id": 1, "amount": 2, "ok": true, "ts": "2026-11-27T10:00:00Z", "tags": [1]}"#
                .to_vec(),
            br#"{"id": 2, "amount": 2.5, "name": "x", "mixed": 1}"#.to_vec(),
            br#"{"id": 3, "mixed": "y", "name": null}"#.to_vec(),
        ];
        let schema = inferred_schema(&settings, &first).unwrap();
        let types: Vec<(String, String)> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| (f.name.clone(), f.field_type.to_string()))
            .collect();
        assert_eq!(
            types,
            [
                ("id", "long"),
                ("amount", "double"),
                ("ok", "boolean"),
                ("ts", "timestamptz"),
                ("tags", "string"),
                ("name", "string"),
                ("mixed", "string"),
                ("missing", "date"),
            ]
            .map(|(a, b)| (a.to_string(), b.to_string()))
        );
    }
}
