//! The models a pipeline service embeds, and the audit records of their scores, from Rust
//! blocks. Behind the `models` feature, which a block that names `mage::models` turns on.
//!
//! ```ignore
//! use mage::prelude::*;
//!
//! fn transform(customers: DataFrame, context: BlockContext) -> Result<DataFrame> {
//!     let model = mage::onnx::model(mage::models::dir("churn")?.join("artifacts/model.onnx"))?;
//!     let features = ["tenure", "monthly_spend", "tickets"];
//!     let scores = model.predict(&customers, &features)?;
//!     let score = scores.get_columns()[0].as_materialized_series();
//!     mage::models::audit(&context, "churn", &customers, "customer_id", &features, score)?;
//!     Ok(customers.hstack(scores.get_columns())?)
//! }
//! ```
//!
//! `audit` writes what the Python `predict` writes for a model with `audit` in its service
//! definition (mage_ai/pipeline_services/models.py): one record per input, before the block
//! returns, in `<audit dir>/<model>/<day>-<process>.jsonl`, each record holding the SHA-256
//! of the one before it in its file. The JSON is written as Python writes it, so a column
//! hashed in a Rust block and in a Python block gives the same hash.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result, anyhow, ensure};
use hmac::{Hmac, KeyInit, Mac};
use polars::prelude::*;
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

use crate::context::BlockContext;

const MODELS_DIR_ENV: &str = "MAGE_SERVICE_MODELS_DIR";
const AUDIT_DIR_ENV: &str = "MAGE_SERVICE_AUDIT_DIR";
const AUDIT_KEY_ENV: &str = "MAGE_SERVICE_AUDIT_KEY";
const METADATA_FILE: &str = "mage-model.json";
const AUDIT_FILE: &str = "audit.json";

/// Per file of this process: the hash of its last record.
static CHAIN: Mutex<BTreeMap<PathBuf, Option<String>>> = Mutex::new(BTreeMap::new());

/// Where the service keeps its models and audit records, and what it is: the engine sets
/// these variables for the blocks of a service.
struct Service {
    models_dir: Option<PathBuf>,
    audit_dir: Option<PathBuf>,
    name: Option<String>,
    source_sha256: Option<String>,
    audit_key: Option<String>,
}

impl Service {
    fn from_env() -> Service {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Service {
            models_dir: var(MODELS_DIR_ENV).map(PathBuf::from),
            audit_dir: var(AUDIT_DIR_ENV).map(PathBuf::from),
            name: var("MAGE_SERVICE_NAME"),
            source_sha256: var("MAGE_SERVICE_SOURCE_SHA256"),
            audit_key: var(AUDIT_KEY_ENV),
        }
    }
}

/// The directory of a model the service embeds; its files are under `artifacts/`.
pub fn dir(name: &str) -> Result<PathBuf> {
    model_dir(&Service::from_env(), name)
}

fn model_dir(service: &Service, name: &str) -> Result<PathBuf> {
    let root = service.models_dir.as_ref().ok_or_else(|| {
        anyhow!(
            "Model {name}: models are embedded in pipeline services ({MODELS_DIR_ENV} is not set)"
        )
    })?;
    let path = root.join(name);
    ensure!(
        path.is_dir(),
        "Model {name} is not embedded in this service ({} does not exist)",
        path.display()
    );
    Ok(path)
}

/// The model's version, digest, flavors and signature, as recorded at export.
pub fn metadata(name: &str) -> Result<Value> {
    model_metadata(&Service::from_env(), name)
}

fn model_metadata(service: &Service, name: &str) -> Result<Value> {
    let path = model_dir(service, name)?.join(METADATA_FILE);
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("reading {}", path.display()))
}

/// The model's audit settings; None when the service does not audit it.
fn settings(service: &Service, name: &str) -> Result<Option<Value>> {
    let Ok(dir) = model_dir(service, name) else {
        return Ok(None);
    };
    let path = dir.join(AUDIT_FILE);
    if !path.is_file() {
        return Ok(None);
    }
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Some(serde_json::from_str(&text)?))
}

/// Writes the audit records of a model's scores: one per row of `data`, keyed by its `key`
/// column, with the `features` the model read and the row's `output`. Nothing when the
/// service does not audit the model. A record that cannot be written is an error, so the
/// block fails rather than return an unrecorded decision.
pub fn audit(
    context: &BlockContext,
    name: &str,
    data: &DataFrame,
    key: &str,
    features: &[&str],
    output: &Series,
) -> Result<()> {
    write_records(
        &Service::from_env(),
        context,
        name,
        data,
        key,
        features,
        output,
    )
}

fn write_records(
    service: &Service,
    context: &BlockContext,
    name: &str,
    data: &DataFrame,
    key: &str,
    features: &[&str],
    output: &Series,
) -> Result<()> {
    let Some(settings) = settings(service, name)? else {
        return Ok(());
    };
    let folder = service
        .audit_dir
        .clone()
        .ok_or_else(|| anyhow!("Model {name} is audited, but {AUDIT_DIR_ENV} is not set"))?
        .join(name);
    let secret = service.audit_key.as_deref();
    let rows = data.height();
    ensure!(
        output.len() == rows,
        "Model {name}: the output has {} rows for {rows} inputs",
        output.len()
    );
    let keys = data
        .column(key)
        .map_err(|_| anyhow!("Model {name}: the data has no key column {key}"))?
        .as_materialized_series()
        .clone();
    let columns: Vec<Series> = features
        .iter()
        .map(|f| {
            data.column(f)
                .map(|c| c.as_materialized_series().clone())
                .map_err(|_| anyhow!("Model {name}: the data has no feature column {f}"))
        })
        .collect::<Result<_>>()?;

    let stored = settings["features"]
        .as_str()
        .unwrap_or("values")
        .to_string();
    let listed = |field: &str| -> Vec<String> {
        settings[field]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    let (masked, hashed) = (listed("mask"), listed("hash"));
    let model = model_metadata(service, name).unwrap_or(Value::Null);
    let now = chrono::Utc::now();
    let mut common = Map::new();
    common.insert("model".into(), name.into());
    common.insert("version".into(), model["version"].clone());
    common.insert("model_sha256".into(), model["sha256"].clone());
    for (field, value) in [
        ("service", &service.name),
        ("service_sha256", &service.source_sha256),
    ] {
        common.insert(
            field.into(),
            value.clone().map(Value::from).unwrap_or(Value::Null),
        );
    }
    // The decision context: present fields only, as the Python worker sets them.
    let run_id = context.variables.raw().get("pipeline_run_id").cloned();
    for (field, value) in [
        ("pipeline", context.pipeline_uuid.clone().map(Value::from)),
        ("block", Some(Value::from(context.block_uuid.clone()))),
        (
            "block_sha256",
            context.block_sha256.clone().map(Value::from),
        ),
        ("run_id", run_id.filter(|v| !v.is_null())),
    ] {
        if let Some(value) = value {
            common.insert(field.into(), value);
        }
    }

    std::fs::create_dir_all(&folder).with_context(|| format!("creating {}", folder.display()))?;
    let path = folder.join(format!(
        "{}-{}.jsonl",
        now.format("%Y-%m-%d"),
        std::process::id()
    ));
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, false);
    let mut chain = CHAIN.lock().unwrap_or_else(|p| p.into_inner());
    let mut previous = match chain.get(&path) {
        Some(previous) => previous.clone(),
        None => last_hash(&path)?,
    };
    let mut text = String::new();
    for row in 0..rows {
        let mut record = common.clone();
        record.insert("at".into(), at.clone().into());
        record.insert("key".into(), plain(keys.get(row)?));
        record.insert("output".into(), plain(output.get(row)?));
        let mut values = Map::new();
        for (feature, column) in features.iter().zip(&columns) {
            values.insert(feature.to_string(), plain(column.get(row)?));
        }
        match stored.as_str() {
            "hash" => {
                record.insert(
                    "features_sha256".into(),
                    hash_value(&Value::Object(values), secret).into(),
                );
            }
            "values" => {
                let shown = values
                    .into_iter()
                    .map(|(k, v)| {
                        let v = if masked.contains(&k) {
                            Value::from("***")
                        } else if hashed.contains(&k) {
                            Value::from(hash_value(&v, secret))
                        } else {
                            v
                        };
                        (k, v)
                    })
                    .collect();
                record.insert("features".into(), Value::Object(shown));
            }
            _ => {}
        }
        record.insert(
            "previous".into(),
            previous.clone().map(Value::from).unwrap_or(Value::Null),
        );
        let line = python_json(&Value::Object(record));
        previous = Some(sha256_hex(line.as_bytes()));
        text.push_str(&line);
        text.push('\n');
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("writing {}", path.display()))?;
    file.write_all(text.as_bytes())
        .with_context(|| format!("writing {}", path.display()))?;
    chain.insert(path, previous);
    Ok(())
}

/// The hash of the last line of an existing file, where this process continues its chain.
fn last_hash(path: &std::path::Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(text
            .trim_end_matches('\n')
            .rsplit('\n')
            .next()
            .filter(|l| !l.is_empty())
            .map(|l| sha256_hex(l.as_bytes()))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(anyhow!("reading {}: {error}", path.display())),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A hashed column's value: SHA-256 of its JSON, or HMAC-SHA-256 with
/// MAGE_SERVICE_AUDIT_KEY, as Python's models._hash_value.
fn hash_value(value: &Value, secret: Option<&str>) -> String {
    let text = python_json(value);
    match secret {
        Some(secret) => {
            let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                .expect("HMAC takes keys of any length");
            mac.update(text.as_bytes());
            hex(&mac.finalize().into_bytes())
        }
        None => sha256_hex(text.as_bytes()),
    }
}

/// A value as Python's models._plain gives it: floats rounded to 9 decimals.
fn plain(value: AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => b.into(),
        AnyValue::String(s) => s.into(),
        AnyValue::StringOwned(s) => s.to_string().into(),
        AnyValue::Int8(v) => v.into(),
        AnyValue::Int16(v) => v.into(),
        AnyValue::Int32(v) => v.into(),
        AnyValue::Int64(v) => v.into(),
        AnyValue::UInt8(v) => v.into(),
        AnyValue::UInt16(v) => v.into(),
        AnyValue::UInt32(v) => v.into(),
        AnyValue::UInt64(v) => v.into(),
        AnyValue::Float32(v) => float(f64::from(v)),
        AnyValue::Float64(v) => float(v),
        other => other.to_string().trim_matches('"').to_string().into(),
    }
}

fn float(value: f64) -> Value {
    if !value.is_finite() {
        // Python writes NaN and Infinity; JSON numbers cannot hold them.
        return Value::String(python_float(value));
    }
    // round(value, 9): the decimal rounding of the exact binary value.
    let rounded: f64 = format!("{value:.9}").parse().unwrap_or(value);
    Number::from_f64(rounded).map_or(Value::Null, Value::Number)
}

/// Python's repr of a float: the shortest text that reads back as it, with an exponent
/// below 1e-4 and from 1e16, written e+16 and e-05.
fn python_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    let text = format!("{value:?}");
    match text.split_once('e') {
        Some((mantissa, exponent)) => {
            let (sign, digits) = match exponent.strip_prefix('-') {
                Some(digits) => ('-', digits),
                None => ('+', exponent),
            };
            format!("{mantissa}e{sign}{digits:0>2}")
        }
        None => text,
    }
}

/// JSON as Python's json.dumps(sort_keys=True) writes it: `, ` and `: ` separators,
/// non-ASCII as \u escapes.
fn python_json(value: &Value) -> String {
    let mut out = String::new();
    write_python(value, &mut out);
    out
}

fn write_python(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => match n.as_f64() {
            Some(f) if n.is_f64() => out.push_str(&python_float(f)),
            _ => out.push_str(&n.to_string()),
        },
        Value::String(s) => write_python_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_python(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_python_string(key, out);
                out.push_str(": ");
                write_python(&map[key], out);
            }
            out.push('}');
        }
    }
}

fn write_python_string(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Vars;
    use serde_json::json;

    #[test]
    fn json_is_written_as_python_writes_it() {
        // Each expected text is Python's json.dumps(value, sort_keys=True).
        assert_eq!(
            python_json(&json!({"b": 1, "a": [1.5, null, true], "é": "x\"y\n"})),
            r#"{"a": [1.5, null, true], "b": 1, "\u00e9": "x\"y\n"}"#
        );
        assert_eq!(python_json(&json!("😀 \u{7f}")), r#""\ud83d\ude00 \u007f""#);
        for (value, python) in [
            (1.0, "1.0"),
            (0.1, "0.1"),
            (1e16, "1e+16"),
            (1.5e-5, "1.5e-05"),
            (0.0001, "0.0001"),
            (123456789012345680.0, "1.2345678901234568e+17"),
            (-2.5e-7, "-2.5e-07"),
        ] {
            assert_eq!(python_float(value), python, "{value}");
        }
    }

    #[test]
    fn values_are_hashed_as_python_hashes_them() {
        // hashlib.sha256(json.dumps('ana@corp.org', sort_keys=True).encode()).hexdigest()
        assert_eq!(
            hash_value(&json!("ana@corp.org"), None),
            sha256_hex(b"\"ana@corp.org\"")
        );
        assert_eq!(
            hash_value(&json!({"y": 2, "x": 1.5}), None),
            sha256_hex(b"{\"x\": 1.5, \"y\": 2}")
        );
        // hmac.new(b'k', json.dumps('a').encode(), hashlib.sha256).hexdigest()
        assert_eq!(
            hash_value(&json!("a"), Some("k")),
            "7d13e4106960ecfd6ffdd105228f98a3c5e464ea51a4fd736783b3edb5227161"
        );
    }

    #[test]
    fn floats_are_rounded_to_nine_decimals() {
        assert_eq!(float(0.1234567894), json!(0.123456789));
        assert_eq!(float(2.0000000005), json!(2.000000001));
        assert_eq!(float(f64::NAN), json!("NaN"));
    }

    #[test]
    fn records_are_written_masked_hashed_and_chained() {
        let root = tempfile::tempdir().unwrap();
        let models = root.path().join("models");
        let audit_dir = root.path().join("audit");
        std::fs::create_dir_all(models.join("churn")).unwrap();
        std::fs::write(
            models.join("churn/mage-model.json"),
            r#"{"version": "3", "sha256": "abc"}"#,
        )
        .unwrap();
        let service = Service {
            models_dir: Some(models.clone()),
            audit_dir: Some(audit_dir.clone()),
            name: Some("scores".into()),
            source_sha256: None,
            audit_key: None,
        };
        let context = BlockContext {
            block_uuid: "score".into(),
            block_type: "transformer".into(),
            pipeline_uuid: Some("scoring".into()),
            execution_partition: None,
            variables: Vars(
                json!({"pipeline_run_id": "run-7"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
            scratch_dir: root.path().into(),
            block_sha256: Some("def".into()),
        };
        let data = df!(
            "id" => [1i64, 2],
            "email" => ["ana@corp.org", "bo@corp.org"],
            "ssn" => ["111", "222"],
            "x" => [0.5f64, 1.25],
        )
        .unwrap();
        let output = Series::new("score".into(), [0.9f64, 0.1]);
        let features = ["email", "ssn", "x"];

        // Not audited: nothing is written.
        write_records(&service, &context, "churn", &data, "id", &features, &output).unwrap();
        assert!(!audit_dir.exists());

        std::fs::write(
            models.join("churn/audit.json"),
            r#"{"features": "values", "mask": ["ssn"], "hash": ["email"], "reasons": 0}"#,
        )
        .unwrap();
        write_records(&service, &context, "churn", &data, "id", &features, &output).unwrap();
        let (one, one_output) = (data.head(Some(1)), output.head(Some(1)));
        write_records(
            &service,
            &context,
            "churn",
            &one,
            "id",
            &features,
            &one_output,
        )
        .unwrap();

        let files: Vec<PathBuf> = std::fs::read_dir(audit_dir.join("churn"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1, "one file per process and day");
        let text = std::fs::read_to_string(&files[0]).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["key"], 1);
        assert_eq!(first["output"], 0.9);
        assert_eq!(first["features"]["ssn"], "***");
        assert_eq!(
            first["features"]["email"],
            hash_value(&json!("ana@corp.org"), None)
        );
        assert_eq!(first["features"]["x"], 0.5);
        assert_eq!(
            (&first["model"], &first["version"], &first["model_sha256"]),
            (&json!("churn"), &json!("3"), &json!("abc"))
        );
        assert_eq!(
            (&first["service"], &first["pipeline"], &first["block"]),
            (&json!("scores"), &json!("scoring"), &json!("score"))
        );
        assert_eq!(
            (&first["block_sha256"], &first["run_id"]),
            (&json!("def"), &json!("run-7"))
        );
        assert_eq!(first["previous"], Value::Null);
        // Each record holds the hash of the line before it, across calls.
        for pair in lines.windows(2) {
            let next: Value = serde_json::from_str(pair[1]).unwrap();
            assert_eq!(next["previous"], sha256_hex(pair[0].as_bytes()));
        }

        // A mismatched output is an error, not a partial record.
        let error = write_records(
            &service,
            &context,
            "churn",
            &data,
            "id",
            &features,
            &one_output,
        );
        assert!(
            error
                .unwrap_err()
                .to_string()
                .contains("1 rows for 2 inputs")
        );
    }
}
