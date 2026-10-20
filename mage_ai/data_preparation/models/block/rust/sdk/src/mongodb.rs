//! MongoDB for Rust blocks (feature `mongodb`), with the rules of MageML's Python client
//! (mage_ai/io/mongodb.py and mongodb_types.py).
//!
//! ```ignore
//! use mage::mongodb::{doc, MongoDb, WritePolicy};
//! use mage::prelude::*;
//!
//! fn load_data() -> Result<DataFrame> {
//!     MongoDb::connect()?.load("orders", doc! { "status": "paid" })
//! }
//!
//! fn export_data(scores: DataFrame) -> Result<()> {
//!     MongoDb::connect()?.write(&scores, "scores", WritePolicy::Replace)?;
//!     Ok(())
//! }
//! ```
//!
//! Reading: one column per field, a field missing from a document is null. Each column
//! takes the type of its values: integers stay integers with nulls, Decimal128 is an
//! exact decimal, dates are UTC datetimes, ObjectId and UUID are text, embedded documents
//! are structs and arrays lists. Integers and floats together are floats. A field whose
//! values have different types is JSON text.
//!
//! Writing: integers that fit 32 bits are Int32 and others Int64, as pymongo stores them;
//! decimals are Decimal128, dates midnight UTC, datetimes UTC (BSON keeps milliseconds),
//! durations seconds. `WritePolicy::Replace` writes a staging collection with the target's
//! indexes and renames it over the target: readers see the old documents or the new ones,
//! and a failed write leaves the target as it was.
use std::collections::HashMap;
use std::sync::Arc;

use mongodb::bson::spec::BinarySubtype;
pub use mongodb::bson::{self, Bson, Document, doc};
use mongodb::sync::{Client, Database};
use serde_json::Value as Json;

use crate::prelude::*;

/// Documents written per request, as in the Python client.
pub const WRITE_BATCH_SIZE: usize = 10_000;

/// What `write` does with the documents already in the collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    /// Add the documents.
    Append,
    /// Replace the collection's documents with these, keeping its indexes.
    Replace,
}

/// With unique key fields: what a document whose key is stored already does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnConflict {
    /// Replace the stored document.
    Update,
    /// Keep the stored document.
    Ignore,
}

pub struct MongoDb {
    client: Client,
    database: String,
    collection: Option<String>,
}

/// A BSON value as it is read.
#[derive(Debug, Clone, PartialEq)]
enum Val {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bin(Vec<u8>),
    /// Milliseconds since the epoch, UTC.
    Date(i64),
    /// Unscaled value and scale.
    Dec(i128, usize),
    Doc(Vec<(String, Val)>),
    List(Vec<Val>),
}

/// A column's type, from all its values.
#[derive(Debug, Clone, PartialEq)]
enum Shape {
    Null,
    Bool,
    Int,
    Float,
    Str,
    Bin,
    Date,
    Dec(usize),
    Struct(Vec<(String, Shape)>),
    List(Box<Shape>),
    Json,
}

fn decimal_parts(text: &str) -> Option<(i128, usize)> {
    let text = text.trim();
    let (mantissa, exponent) = match text.split_once(['E', 'e']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (text, 0),
    };
    let negative = mantissa.starts_with('-');
    let digits = mantissa.trim_start_matches(['-', '+']);
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let mut scale = fraction.len() as i32 - exponent;
    let mut joined = format!("{whole}{fraction}");
    while scale < 0 {
        joined.push('0');
        scale += 1;
    }
    let value: i128 = joined.parse().ok()?;
    Some((if negative { -value } else { value }, scale as usize))
}

fn val(value: &Bson) -> Val {
    match value {
        Bson::Null | Bson::Undefined => Val::Null,
        Bson::Boolean(b) => Val::Bool(*b),
        Bson::Int32(i) => Val::Int(*i as i64),
        Bson::Int64(i) => Val::Int(*i),
        Bson::Double(f) => Val::Float(*f),
        Bson::String(s) => Val::Str(s.clone()),
        Bson::ObjectId(id) => Val::Str(id.to_hex()),
        Bson::DateTime(at) => Val::Date(at.timestamp_millis()),
        Bson::Decimal128(d) => match decimal_parts(&d.to_string()) {
            Some((value, scale)) => Val::Dec(value, scale),
            // NaN and infinities have no decimal value.
            None => Val::Str(d.to_string()),
        },
        Bson::Binary(binary) => match binary.subtype {
            BinarySubtype::Uuid | BinarySubtype::UuidOld if binary.bytes.len() == 16 => {
                let b = &binary.bytes;
                Val::Str(format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    b[0],
                    b[1],
                    b[2],
                    b[3],
                    b[4],
                    b[5],
                    b[6],
                    b[7],
                    b[8],
                    b[9],
                    b[10],
                    b[11],
                    b[12],
                    b[13],
                    b[14],
                    b[15]
                ))
            }
            _ => Val::Bin(binary.bytes.clone()),
        },
        Bson::Document(document) => {
            Val::Doc(document.iter().map(|(k, v)| (k.clone(), val(v))).collect())
        }
        Bson::Array(items) => Val::List(items.iter().map(val).collect()),
        other => Val::Str(other.to_string()),
    }
}

fn shape_of(value: &Val) -> Shape {
    match value {
        Val::Null => Shape::Null,
        Val::Bool(_) => Shape::Bool,
        Val::Int(_) => Shape::Int,
        Val::Float(_) => Shape::Float,
        Val::Str(_) => Shape::Str,
        Val::Bin(_) => Shape::Bin,
        Val::Date(_) => Shape::Date,
        Val::Dec(_, scale) => Shape::Dec(*scale),
        Val::Doc(fields) => Shape::Struct(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), shape_of(v)))
                .collect(),
        ),
        Val::List(items) => Shape::List(Box::new(
            items.iter().map(shape_of).fold(Shape::Null, unify),
        )),
    }
}

fn unify(a: Shape, b: Shape) -> Shape {
    match (a, b) {
        (Shape::Null, other) | (other, Shape::Null) => other,
        (Shape::Json, _) | (_, Shape::Json) => Shape::Json,
        (Shape::Int, Shape::Float) | (Shape::Float, Shape::Int) => Shape::Float,
        (Shape::Dec(a), Shape::Dec(b)) => Shape::Dec(a.max(b)),
        (Shape::List(a), Shape::List(b)) => Shape::List(Box::new(unify(*a, *b))),
        (Shape::Struct(mut a), Shape::Struct(b)) => {
            for (name, shape) in b {
                match a.iter_mut().find(|(n, _)| *n == name) {
                    Some((_, existing)) => {
                        *existing = unify(std::mem::replace(existing, Shape::Null), shape)
                    }
                    None => a.push((name, shape)),
                }
            }
            if a.iter().any(|(_, s)| *s == Shape::Json) {
                Shape::Json
            } else {
                Shape::Struct(a)
            }
        }
        (a, b) if a == b => a,
        _ => Shape::Json,
    }
}

fn utc() -> Arc<TimeZone> {
    Arc::new(TimeZone::UTC)
}

fn dtype(shape: &Shape) -> DataType {
    match shape {
        Shape::Null => DataType::Null,
        Shape::Bool => DataType::Boolean,
        Shape::Int => DataType::Int64,
        Shape::Float => DataType::Float64,
        Shape::Str | Shape::Json => DataType::String,
        Shape::Bin => DataType::Binary,
        Shape::Date => DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC)),
        Shape::Dec(scale) => DataType::Decimal(38, *scale),
        Shape::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|(name, shape)| Field::new(name.into(), dtype(shape)))
                .collect(),
        ),
        Shape::List(inner) => DataType::List(Box::new(dtype(inner))),
    }
}

fn json(value: &Val) -> Json {
    match value {
        Val::Null => Json::Null,
        Val::Bool(b) => Json::Bool(*b),
        Val::Int(i) => Json::from(*i),
        Val::Float(f) => serde_json::Number::from_f64(*f).map_or(Json::Null, Json::Number),
        Val::Str(s) => Json::String(s.clone()),
        Val::Bin(b) => Json::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        Val::Date(ms) => Json::String(
            chrono::DateTime::from_timestamp_millis(*ms)
                .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
                .unwrap_or_default(),
        ),
        Val::Dec(value, scale) => Json::String(decimal_text(*value, *scale)),
        Val::Doc(fields) => {
            Json::Object(fields.iter().map(|(k, v)| (k.clone(), json(v))).collect())
        }
        Val::List(items) => Json::Array(items.iter().map(json).collect()),
    }
}

fn decimal_text(value: i128, scale: usize) -> String {
    let digits = value.unsigned_abs().to_string();
    let sign = if value < 0 { "-" } else { "" };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    format!("{sign}{whole}.{fraction}")
}

fn any_value(value: &Val, shape: &Shape) -> Result<AnyValue<'static>> {
    Ok(match (value, shape) {
        (Val::Null, _) => AnyValue::Null,
        (_, Shape::Json) => AnyValue::StringOwned(json(value).to_string().into()),
        (Val::Bool(b), _) => AnyValue::Boolean(*b),
        (Val::Int(i), Shape::Float) => AnyValue::Float64(*i as f64),
        (Val::Int(i), _) => AnyValue::Int64(*i),
        (Val::Float(f), _) => AnyValue::Float64(*f),
        (Val::Str(s), _) => AnyValue::StringOwned(s.as_str().into()),
        (Val::Bin(b), _) => AnyValue::BinaryOwned(b.clone()),
        (Val::Date(ms), _) => AnyValue::DatetimeOwned(*ms, TimeUnit::Milliseconds, Some(utc())),
        (Val::Dec(v, scale), Shape::Dec(column_scale)) => {
            let factor = 10i128
                .checked_pow((*column_scale - scale) as u32)
                .ok_or_else(|| anyhow!("decimal out of range"))?;
            let scaled = v
                .checked_mul(factor)
                .ok_or_else(|| anyhow!("decimal out of range"))?;
            AnyValue::Decimal(scaled, 38, *column_scale)
        }
        (Val::Doc(fields), Shape::Struct(shapes)) => {
            let values = shapes
                .iter()
                .map(|(name, shape)| {
                    let field = fields.iter().find(|(n, _)| n == name).map(|(_, v)| v);
                    any_value(field.unwrap_or(&Val::Null), shape)
                })
                .collect::<Result<Vec<_>>>()?;
            let names = shapes
                .iter()
                .map(|(name, shape)| Field::new(name.into(), dtype(shape)))
                .collect();
            AnyValue::StructOwned(Box::new((values, names)))
        }
        (Val::List(items), Shape::List(inner)) => {
            let values = items
                .iter()
                .map(|item| any_value(item, inner))
                .collect::<Result<Vec<_>>>()?;
            AnyValue::List(Series::from_any_values_and_dtype(
                PlSmallStr::EMPTY,
                &values,
                &dtype(inner),
                true,
            )?)
        }
        (value, shape) => bail!("{value:?} does not fit {shape:?}"),
    })
}

/// A frame of documents: one column per field, in the order fields first appear.
fn frame_of(documents: &[Document]) -> Result<DataFrame> {
    let mut names: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for document in documents {
        for key in document.keys() {
            if seen.insert(key.clone()) {
                names.push(key.clone());
            }
        }
    }
    let columns = names
        .iter()
        .map(|name| {
            let values: Vec<Val> = documents
                .iter()
                .map(|d| d.get(name).map(val).unwrap_or(Val::Null))
                .collect();
            let shape = values.iter().map(shape_of).fold(Shape::Null, unify);
            let any: Vec<AnyValue> = values
                .iter()
                .map(|v| any_value(v, &shape))
                .collect::<Result<_>>()
                .with_context(|| format!("Field {name}"))?;
            let series =
                Series::from_any_values_and_dtype(name.as_str().into(), &any, &dtype(&shape), true)
                    .with_context(|| format!("Field {name}"))?;
            Ok(series.into_column())
        })
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Ok(DataFrame::empty());
    }
    Ok(DataFrame::new_infer_height(columns)?)
}

fn integer(value: i64) -> Bson {
    // pymongo stores an int that fits 32 bits as Int32.
    match i32::try_from(value) {
        Ok(small) => Bson::Int32(small),
        Err(_) => Bson::Int64(value),
    }
}

/// A value of a frame as BSON, as the Python client's to_bson converts it.
fn to_bson(value: AnyValue, column: &str) -> Result<Bson> {
    Ok(match value {
        AnyValue::Null => Bson::Null,
        AnyValue::Boolean(b) => Bson::Boolean(b),
        AnyValue::Int8(v) => integer(v as i64),
        AnyValue::Int16(v) => integer(v as i64),
        AnyValue::Int32(v) => integer(v as i64),
        AnyValue::Int64(v) => integer(v),
        AnyValue::UInt8(v) => integer(v as i64),
        AnyValue::UInt16(v) => integer(v as i64),
        AnyValue::UInt32(v) => integer(v as i64),
        AnyValue::UInt64(v) => integer(i64::try_from(v).map_err(|_| {
            anyhow!("Column {column}: {v} is past the largest BSON integer (2^63 - 1)")
        })?),
        AnyValue::Float32(v) => Bson::Double(v as f64),
        AnyValue::Float64(v) => Bson::Double(v),
        AnyValue::String(s) => Bson::String(s.to_string()),
        AnyValue::StringOwned(s) => Bson::String(s.to_string()),
        AnyValue::Binary(b) => Bson::Binary(bson::Binary {
            subtype: BinarySubtype::Generic,
            bytes: b.to_vec(),
        }),
        AnyValue::BinaryOwned(b) => Bson::Binary(bson::Binary {
            subtype: BinarySubtype::Generic,
            bytes: b,
        }),
        AnyValue::Date(days) => {
            Bson::DateTime(bson::DateTime::from_millis(days as i64 * 86_400_000))
        }
        AnyValue::Datetime(v, unit, _) | AnyValue::DatetimeOwned(v, unit, _) => {
            let ms = match unit {
                TimeUnit::Milliseconds => v,
                TimeUnit::Microseconds => v.div_euclid(1_000),
                TimeUnit::Nanoseconds => v.div_euclid(1_000_000),
            };
            Bson::DateTime(bson::DateTime::from_millis(ms))
        }
        AnyValue::Duration(v, unit) => {
            let per_second = match unit {
                TimeUnit::Milliseconds => 1e3,
                TimeUnit::Microseconds => 1e6,
                TimeUnit::Nanoseconds => 1e9,
            };
            Bson::Double(v as f64 / per_second)
        }
        AnyValue::Time(ns) => {
            let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                (ns / 1_000_000_000) as u32,
                (ns % 1_000_000_000) as u32,
            )
            .ok_or_else(|| anyhow!("Column {column}: invalid time"))?;
            Bson::String(time.format("%H:%M:%S%.f").to_string())
        }
        AnyValue::Decimal(v, _, scale) => Bson::Decimal128(
            decimal_text(v, scale)
                .parse()
                .map_err(|e| anyhow!("Column {column}: {e}"))?,
        ),
        AnyValue::List(series) => Bson::Array(
            series
                .iter()
                .map(|v| to_bson(v, column))
                .collect::<Result<_>>()?,
        ),
        value @ (AnyValue::Struct(..) | AnyValue::StructOwned(_)) => {
            let mut document = Document::new();
            for (field, v) in struct_fields(&value).iter().zip(value._iter_struct_av()) {
                document.insert(field.name().to_string(), to_bson(v, column)?);
            }
            Bson::Document(document)
        }
        AnyValue::Categorical(..)
        | AnyValue::CategoricalOwned(..)
        | AnyValue::Enum(..)
        | AnyValue::EnumOwned(..) => Bson::String(value.str_value().to_string()),
        other => bail!(
            "Column {column}: {} cannot be written to MongoDB",
            other.dtype()
        ),
    })
}

fn struct_fields(value: &AnyValue) -> Vec<Field> {
    match value {
        AnyValue::Struct(_, _, fields) => fields.to_vec(),
        AnyValue::StructOwned(payload) => payload.1.clone(),
        _ => Vec::new(),
    }
}

/// The rows of a frame as documents.
fn documents_of(frame: &DataFrame) -> Result<Vec<Document>> {
    let mut documents = vec![Document::new(); frame.height()];
    for column in frame.columns() {
        let series = column.as_materialized_series();
        let name = series.name().to_string();
        for (document, value) in documents.iter_mut().zip(series.iter()) {
            document.insert(name.clone(), to_bson(value, &name)?);
        }
    }
    Ok(documents)
}

fn percent_encoded(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl MongoDb {
    /// Connects with the block's io_config profile.
    pub fn connect() -> Result<Self> {
        Self::connect_settings(&crate::profile::settings()?)
    }

    /// Connects with MONGODB_CONNECTION_STRING, or MONGODB_HOST, MONGODB_PORT, MONGODB_USER
    /// and MONGODB_PASSWORD; MONGODB_DATABASE names the database and MONGODB_COLLECTION the
    /// default collection, as in MageML's Python client.
    pub fn connect_settings(settings: &HashMap<String, String>) -> Result<Self> {
        let get = |key: &str| {
            settings
                .get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let uri = match get("MONGODB_CONNECTION_STRING") {
            Some(uri) => uri,
            None => {
                let host = get("MONGODB_HOST").unwrap_or_else(|| "localhost".into());
                let port = get("MONGODB_PORT").unwrap_or_else(|| "27017".into());
                // The user and password are percent-encoded: a password with '@', ':' or
                // '/' would end the credentials early.
                let credentials = match (get("MONGODB_USER"), get("MONGODB_PASSWORD")) {
                    (Some(user), Some(password)) => {
                        format!("{}:{}@", percent_encoded(&user), percent_encoded(&password))
                    }
                    (Some(user), None) => format!("{}@", percent_encoded(&user)),
                    _ => String::new(),
                };
                format!("mongodb://{credentials}{host}:{port}/")
            }
        };
        let database = get("MONGODB_DATABASE")
            .ok_or_else(|| anyhow!("The MongoDB profile has no MONGODB_DATABASE"))?;
        let client = Client::with_uri_str(&uri).context("Could not connect to MongoDB")?;
        Ok(Self {
            client,
            database,
            collection: get("MONGODB_COLLECTION"),
        })
    }

    /// The underlying database handle.
    pub fn database(&self) -> Database {
        self.client.database(&self.database)
    }

    fn collection_name<'a>(&'a self, collection: &'a str) -> Result<&'a str> {
        if !collection.is_empty() {
            return Ok(collection);
        }
        self.collection
            .as_deref()
            .ok_or_else(|| anyhow!("Name the collection, or set MONGODB_COLLECTION in the profile"))
    }

    /// The documents of a collection that match a filter (`doc! {}` for all); an empty
    /// collection name uses the profile's MONGODB_COLLECTION.
    pub fn load(&self, collection: &str, filter: Document) -> Result<DataFrame> {
        self.load_fields(collection, filter, None)
    }

    /// As `load`, returning only the fields of a projection.
    pub fn load_fields(
        &self,
        collection: &str,
        filter: Document,
        projection: Option<Document>,
    ) -> Result<DataFrame> {
        let name = self.collection_name(collection)?;
        let target = self.database().collection::<Document>(name);
        let mut find = target.find(filter);
        if let Some(projection) = projection {
            find = find.projection(projection);
        }
        let documents: Vec<Document> = find
            .run()
            .with_context(|| format!("MongoDB could not read {name}"))?
            .collect::<std::result::Result<_, _>>()
            .with_context(|| format!("MongoDB could not read {name}"))?;
        frame_of(&documents)
    }

    /// Writes the frame's rows as documents and returns how many were written.
    pub fn write(&self, frame: &DataFrame, collection: &str, policy: WritePolicy) -> Result<u64> {
        self.write_documents(
            documents_of(frame)?,
            collection,
            policy,
            &[],
            OnConflict::Update,
        )
    }

    /// As `write`, with key fields that identify a document: a document whose key is
    /// stored already replaces it (`OnConflict::Update`) or is skipped
    /// (`OnConflict::Ignore`). Rerunning the write then stores each document once.
    pub fn write_unique(
        &self,
        frame: &DataFrame,
        collection: &str,
        policy: WritePolicy,
        keys: &[&str],
        on_conflict: OnConflict,
    ) -> Result<u64> {
        ensure!(
            !keys.is_empty(),
            "write_unique needs at least one key field"
        );
        let documents = documents_of(frame)?;
        let missing: Vec<usize> = documents
            .iter()
            .enumerate()
            .filter(|(_, d)| {
                keys.iter()
                    .any(|k| matches!(d.get(*k), None | Some(Bson::Null)))
            })
            .map(|(i, _)| i)
            .take(5)
            .collect();
        ensure!(
            missing.is_empty(),
            "Rows {missing:?} have no value for a key field {keys:?}; they would all match one document"
        );
        self.write_documents(documents, collection, policy, keys, on_conflict)
    }

    fn write_documents(
        &self,
        documents: Vec<Document>,
        collection: &str,
        policy: WritePolicy,
        keys: &[&str],
        on_conflict: OnConflict,
    ) -> Result<u64> {
        let name = self.collection_name(collection)?.to_string();
        let database = self.database();
        let count = documents.len() as u64;
        if policy == WritePolicy::Append {
            if !documents.is_empty() {
                self.insert(&database, &name, documents, keys, on_conflict)?;
            }
            return Ok(count);
        }
        let staging = format!(
            "{name}__mage_staging_{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let result = (|| -> Result<()> {
            database.create_collection(&staging).run()?;
            let existing = database.list_collection_names().run()?;
            if existing.iter().any(|c| c == &name) {
                let indexes: Vec<_> = database
                    .collection::<Document>(&name)
                    .list_indexes()
                    .run()?
                    .collect::<std::result::Result<_, _>>()?;
                let indexes: Vec<_> = indexes
                    .into_iter()
                    .filter(|index| {
                        index.options.as_ref().and_then(|o| o.name.as_deref()) != Some("_id_")
                    })
                    .collect();
                if !indexes.is_empty() {
                    database
                        .collection::<Document>(&staging)
                        .create_indexes(indexes)
                        .run()?;
                }
            }
            if !documents.is_empty() {
                self.insert(&database, &staging, documents, keys, on_conflict)?;
            }
            self.client
                .database("admin")
                .run_command(doc! {
                    "renameCollection": format!("{}.{staging}", self.database),
                    "to": format!("{}.{name}", self.database),
                    "dropTarget": true,
                })
                .run()?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = database.collection::<Document>(&staging).drop().run();
            return Err(error.context(format!("MongoDB could not replace {name}")));
        }
        Ok(count)
    }

    fn insert(
        &self,
        database: &Database,
        name: &str,
        documents: Vec<Document>,
        keys: &[&str],
        on_conflict: OnConflict,
    ) -> Result<()> {
        let target = database.collection::<Document>(name);
        for batch in documents.chunks(WRITE_BATCH_SIZE) {
            if keys.is_empty() {
                target
                    .insert_many(batch)
                    .run()
                    .with_context(|| format!("MongoDB could not insert into {name}"))?;
                continue;
            }
            // One update command per batch, in order: within a write, the last document
            // with a key wins.
            let updates: Vec<Bson> = batch
                .iter()
                .map(|document| {
                    let mut filter = Document::new();
                    for key in keys {
                        filter.insert(*key, document.get(*key).cloned().unwrap_or(Bson::Null));
                    }
                    let update = match on_conflict {
                        OnConflict::Update => Bson::Document(document.clone()),
                        OnConflict::Ignore => {
                            Bson::Document(doc! { "$setOnInsert": document.clone() })
                        }
                    };
                    Bson::Document(doc! { "q": filter, "u": update, "upsert": true })
                })
                .collect();
            let reply = database
                .run_command(doc! { "update": name, "updates": updates, "ordered": true })
                .run()
                .with_context(|| format!("MongoDB could not write {name}"))?;
            if let Ok(errors) = reply.get_array("writeErrors") {
                bail!("MongoDB could not write {name}: {errors:?}");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_parse_exactly() {
        assert_eq!(decimal_parts("1.250"), Some((1250, 3)));
        assert_eq!(decimal_parts("-0.5"), Some((-5, 1)));
        assert_eq!(decimal_parts("12E+2"), Some((1200, 0)));
        assert_eq!(decimal_parts("1.5E-3"), Some((15, 4)));
        assert_eq!(decimal_text(-5, 1), "-0.5");
        assert_eq!(decimal_text(1250, 3), "1.250");
        assert_eq!(decimal_text(7, 0), "7");
    }

    #[test]
    fn columns_take_the_type_of_their_values() -> Result<()> {
        let documents = vec![
            doc! { "id": 1, "n": 1, "d": "1.5".parse::<bson::Decimal128>().unwrap(), "tags": ["a"],
            "address": { "city": "Paris" }, "mixed": 1 },
            doc! { "id": 2, "n": 2.5, "d": "2.25".parse::<bson::Decimal128>().unwrap(),
            "address": { "zip": 75001 }, "mixed": "x" },
        ];
        let frame = frame_of(&documents)?;
        assert_eq!(frame.column("id")?.dtype(), &DataType::Int64);
        assert_eq!(frame.column("n")?.dtype(), &DataType::Float64);
        assert_eq!(frame.column("d")?.dtype(), &DataType::Decimal(38, 2));
        assert_eq!(frame.column("tags")?.null_count(), 1);
        assert_eq!(frame.column("mixed")?.dtype(), &DataType::String);
        assert_eq!(frame.column("mixed")?.str()?.get(1), Some("\"x\""));
        let DataType::Struct(fields) = frame.column("address")?.dtype() else {
            panic!("address is not a struct");
        };
        assert_eq!(fields.len(), 2);
        Ok(())
    }

    #[test]
    fn frames_become_documents_as_pymongo_stores_them() -> Result<()> {
        let frame = df!("small" => [1i64], "big" => [5_000_000_000i64], "f" => [0.5f64])?;
        let documents = documents_of(&frame)?;
        assert_eq!(documents[0].get("small"), Some(&Bson::Int32(1)));
        assert_eq!(documents[0].get("big"), Some(&Bson::Int64(5_000_000_000)));
        let too_big = df!("u" => [u64::MAX])?;
        assert!(
            documents_of(&too_big)
                .unwrap_err()
                .to_string()
                .contains("largest BSON integer")
        );
        assert_eq!(percent_encoded("p@ss:w/rd"), "p%40ss%3Aw%2Frd");
        Ok(())
    }

    /// A client on a database of its own in the integration tests' MongoDB
    /// (MAGE_TEST_MONGODB_URL); skipped without it.
    fn live(name: &str) -> Option<MongoDb> {
        let url = std::env::var("MAGE_TEST_MONGODB_URL").ok()?;
        let settings: HashMap<String, String> = [
            ("MONGODB_CONNECTION_STRING".to_string(), url),
            (
                "MONGODB_DATABASE".to_string(),
                format!("rust_{name}_{}", std::process::id()),
            ),
        ]
        .into_iter()
        .collect();
        let client = MongoDb::connect_settings(&settings).unwrap();
        client.database().drop().run().unwrap();
        Some(client)
    }

    #[test]
    fn live_every_type_reads_exactly() -> Result<()> {
        let Some(client) = live("types") else {
            return Ok(());
        };
        let target = client.database().collection::<Document>("things");
        let id = bson::oid::ObjectId::new();
        target
            .insert_many([
                doc! {
                    "_id": id, "n": 1i32, "big": 9_007_199_254_740_993i64, "f": 0.5,
                    "d": "12345678901234567890.123456789012".parse::<bson::Decimal128>().unwrap(),
                    "at": bson::DateTime::from_millis(1_704_164_645_123), "flag": true,
                    "u": bson::Binary::from_uuid(bson::Uuid::parse_str("00000000-0000-0000-0000-00000000002a").unwrap()),
                    "nested": { "a": 1, "b": [1, 2] }, "tags": ["x", "y"], "mixed": 1,
                },
                doc! { "_id": bson::oid::ObjectId::new(), "n": Bson::Null, "mixed": { "k": "v" } },
            ])
            .run()?;
        let frame = client.load("things", doc! {})?;
        assert_eq!(frame.height(), 2);
        assert_eq!(
            frame.column("_id")?.str()?.get(0),
            Some(id.to_hex().as_str())
        );
        assert_eq!(frame.column("n")?.dtype(), &DataType::Int64);
        assert_eq!(
            frame.column("big")?.i64()?.get(0),
            Some(9_007_199_254_740_993)
        );
        assert_eq!(frame.column("d")?.dtype(), &DataType::Decimal(38, 12));
        assert_eq!(
            frame.column("d")?.get(0)?.to_string(),
            "12345678901234567890.123456789012"
        );
        assert_eq!(
            frame.column("at")?.dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC))
        );
        assert_eq!(
            frame.column("u")?.str()?.get(0),
            Some("00000000-0000-0000-0000-00000000002a")
        );
        assert!(matches!(
            frame.column("nested")?.dtype(),
            DataType::Struct(_)
        ));
        assert_eq!(
            frame.column("tags")?.dtype(),
            &DataType::List(Box::new(DataType::String))
        );
        assert_eq!(frame.column("mixed")?.str()?.get(1), Some(r#"{"k":"v"}"#));
        let only = client.load_fields(
            "things",
            doc! { "flag": true },
            Some(doc! { "n": 1, "_id": 0 }),
        )?;
        assert_eq!(only.get_column_names(), vec!["n"]);
        assert_eq!(only.height(), 1);
        client.database().drop().run()?;
        Ok(())
    }

    #[test]
    fn live_frames_round_trip_replace_and_upsert() -> Result<()> {
        let Some(client) = live("writes") else {
            return Ok(());
        };
        let frame = df!(
            "id" => [1i64, 2, 3],
            "name" => [Some("a"), None, Some("é")],
            "amount" => [Some(1.5f64), Some(2.25), None],
        )?
        .lazy()
        .with_columns([
            col("amount").cast(DataType::Decimal(10, 2)).alias("exact"),
            lit(1_704_164_645_123i64)
                .cast(DataType::Datetime(
                    TimeUnit::Milliseconds,
                    Some(TimeZone::UTC),
                ))
                .alias("at"),
        ])
        .collect()?;
        let target = client.database().collection::<Document>("scores");
        target
            .create_index(
                mongodb::IndexModel::builder()
                    .keys(doc! { "id": 1 })
                    .options(
                        mongodb::options::IndexOptions::builder()
                            .name("by_id".to_string())
                            .build(),
                    )
                    .build(),
            )
            .run()?;
        assert_eq!(client.write(&frame, "scores", WritePolicy::Replace)?, 3);
        let back = client.load_fields("scores", doc! {}, Some(doc! { "_id": 0 }))?;
        assert_eq!(back.column("id")?.i64()?.get(2), Some(3));
        assert_eq!(back.column("exact")?.get(0)?.to_string(), "1.50");
        assert_eq!(back.column("at")?.get(0)?, frame.column("at")?.get(0)?);
        // The index survives the replace.
        let names = target.list_index_names().run()?;
        assert!(names.contains(&"by_id".to_string()), "{names:?}");

        client.write(&frame, "scores", WritePolicy::Append)?;
        assert_eq!(target.count_documents(doc! {}).run()?, 6);
        client.write(&frame.head(Some(1)), "scores", WritePolicy::Replace)?;
        assert_eq!(target.count_documents(doc! {}).run()?, 1);

        // Upserts by key: a rerun stores each document once.
        let changed = frame
            .clone()
            .lazy()
            .with_column(lit("new").alias("name"))
            .collect()?;
        client.write_unique(
            &changed,
            "scores",
            WritePolicy::Append,
            &["id"],
            OnConflict::Update,
        )?;
        client.write_unique(
            &changed,
            "scores",
            WritePolicy::Append,
            &["id"],
            OnConflict::Update,
        )?;
        assert_eq!(target.count_documents(doc! {}).run()?, 3);
        assert_eq!(target.count_documents(doc! { "name": "new" }).run()?, 3);
        client.write_unique(
            &frame,
            "scores",
            WritePolicy::Append,
            &["id"],
            OnConflict::Ignore,
        )?;
        assert_eq!(target.count_documents(doc! { "name": "new" }).run()?, 3);
        client.database().drop().run()?;
        Ok(())
    }
}
