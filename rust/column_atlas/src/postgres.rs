//! PostgreSQL for Python blocks through binary COPY: the Rust blocks' client
//! (`mage::postgres` in the block SDK), with frames passed to and from Python as Arrow IPC.
//!
//! On 1 million rows of seven columns, a load took 0.5 seconds against 1.6 to 2.0 through
//! psycopg2, and an export 0.6 to 0.9 against 7 through COPY text rendered in Python.
//! Settings are an io_config profile as JSON (`POSTGRES_HOST`, `POSTGRES_SSLMODE`, ...).

use std::collections::HashMap;
use std::io::Cursor;

use mage::postgres::{Postgres, WritePolicy};
use polars::prelude::*;

use crate::error::AtlasError;

fn settings(json: &str) -> Result<HashMap<String, String>, AtlasError> {
    let value: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| AtlasError::Invalid(format!("the PostgreSQL settings are not JSON: {e}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| AtlasError::Invalid("the PostgreSQL settings are not an object".into()))?;
    Ok(object
        .iter()
        .filter_map(|(key, value)| match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(text) => Some((key.clone(), text.clone())),
            other => Some((key.clone(), other.to_string())),
        })
        .collect())
}

fn engine(error: impl std::fmt::Display) -> AtlasError {
    AtlasError::Engine(error.to_string())
}

fn policy(name: &str) -> Result<WritePolicy, AtlasError> {
    match name {
        "replace" => Ok(WritePolicy::Replace),
        "append" => Ok(WritePolicy::Append),
        "fail" => Ok(WritePolicy::Fail),
        other => Err(AtlasError::Invalid(format!(
            "unknown write policy {other:?}: replace, append or fail"
        ))),
    }
}

/// The result of a query as an Arrow IPC stream.
pub fn query(settings_json: &str, sql: &str) -> Result<Vec<u8>, AtlasError> {
    let mut client = Postgres::connect_settings(&settings(settings_json)?).map_err(engine)?;
    let mut frame = client.query(sql).map_err(engine)?;
    let mut buffer = Vec::new();
    IpcStreamWriter::new(&mut buffer)
        .finish(&mut frame)
        .map_err(engine)?;
    Ok(buffer)
}

/// Writes an Arrow IPC stream (or file) into a table; returns the rows written.
pub fn write(
    settings_json: &str,
    ipc: &[u8],
    table: &str,
    policy_name: &str,
) -> Result<u64, AtlasError> {
    let policy = policy(policy_name)?;
    let frame = read_ipc(ipc)?;
    let mut client = Postgres::connect_settings(&settings(settings_json)?).map_err(engine)?;
    client.write(&frame, table, policy).map_err(engine)
}

/// For a frame as an Arrow IPC stream: the column types binary COPY writes, and the COPY
/// data, which MageML's Python client sends in its own transaction.
pub fn copy_binary(ipc: &[u8]) -> Result<(Vec<String>, Vec<u8>), AtlasError> {
    let frame = read_ipc(ipc)?;
    mage::postgres::binary_copy(&frame).map_err(engine)
}

fn read_ipc(ipc: &[u8]) -> Result<DataFrame, AtlasError> {
    // The file format starts with ARROW1; pyarrow and Polars write either.
    if ipc.starts_with(b"ARROW1") {
        IpcReader::new(Cursor::new(ipc)).finish().map_err(engine)
    } else {
        IpcStreamReader::new(Cursor::new(ipc))
            .finish()
            .map_err(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_take_strings_numbers_and_skip_nulls() {
        let parsed = settings(
            r#"{"POSTGRES_HOST": "db", "POSTGRES_PORT": 5432, "POSTGRES_PASSWORD": null}"#,
        )
        .unwrap();
        assert_eq!(parsed.get("POSTGRES_HOST").map(String::as_str), Some("db"));
        assert_eq!(
            parsed.get("POSTGRES_PORT").map(String::as_str),
            Some("5432")
        );
        assert!(!parsed.contains_key("POSTGRES_PASSWORD"));
        assert!(settings("[]").is_err());
        assert!(settings("not json").is_err());
    }

    #[test]
    fn policies_are_named_as_in_mage() {
        assert_eq!(policy("replace").unwrap(), WritePolicy::Replace);
        assert_eq!(policy("append").unwrap(), WritePolicy::Append);
        assert_eq!(policy("fail").unwrap(), WritePolicy::Fail);
        assert!(policy("merge").is_err());
    }

    #[test]
    fn frames_come_back_from_both_ipc_formats() {
        let mut frame = df!("a" => [1i64, 2, 3], "b" => ["x", "y", "z"]).unwrap();
        let mut stream = Vec::new();
        IpcStreamWriter::new(&mut stream)
            .finish(&mut frame)
            .unwrap();
        let mut file = Vec::new();
        IpcWriter::new(&mut file).finish(&mut frame).unwrap();
        assert_eq!(read_ipc(&stream).unwrap(), frame);
        assert_eq!(read_ipc(&file).unwrap(), frame);
    }
}
