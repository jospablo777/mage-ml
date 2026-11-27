//! Two outputs of a block compared (change previews, ADR-0044): the baseline's and a
//! candidate's. Schema changes first; then, by key, the rows added, removed, changed and
//! unchanged, how many rows changed in each column, and sample keys of each; without a key,
//! the rows only in one of the two, counted as multisets.
//!
//! Values compare by type: floats within a relative tolerance (`|a - b| <= tol * max(|a|,
//! |b|)`, nulls equal to nulls), everything else exactly. A column whose type changed is
//! reported and left out of the value comparison.

use polars::prelude::*;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::engine::Table;
use crate::error::AtlasError;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffRequest {
    /// The key columns; empty compares rows as multisets.
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default = "default_tolerance")]
    pub tolerance: f64,
    /// Sample keys kept for each kind of difference.
    #[serde(default = "default_samples")]
    pub samples: usize,
}

fn default_tolerance() -> f64 {
    1e-9
}

fn default_samples() -> usize {
    20
}

const BASE: &str = "__mage_base";

fn invalid(message: impl Into<String>) -> AtlasError {
    AtlasError::invalid(message)
}

fn polars(error: PolarsError) -> AtlasError {
    AtlasError::from(error)
}

fn count(frame: LazyFrame) -> Result<u64, AtlasError> {
    let counted = frame.select([len().alias("n")]).collect().map_err(polars)?;
    Ok(counted
        .column("n")
        .map_err(polars)?
        .u32()
        .map_err(polars)?
        .get(0)
        .unwrap_or(0) as u64)
}

/// The rows of `frame`, its columns as JSON values by name.
fn rows_json(frame: DataFrame) -> Vec<Value> {
    let names: Vec<String> = frame
        .get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect();
    (0..frame.height())
        .map(|row| {
            let mut object = serde_json::Map::new();
            for (name, column) in names.iter().zip(frame.columns()) {
                let value = column.get(row).map(|v| any_json(&v)).unwrap_or(Value::Null);
                object.insert(name.clone(), value);
            }
            Value::Object(object)
        })
        .collect()
}

fn any_json(value: &AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => json!(b),
        AnyValue::String(s) => json!(s),
        AnyValue::StringOwned(s) => json!(s.as_str()),
        AnyValue::Int8(v) => json!(v),
        AnyValue::Int16(v) => json!(v),
        AnyValue::Int32(v) => json!(v),
        AnyValue::Int64(v) => json!(v),
        AnyValue::UInt8(v) => json!(v),
        AnyValue::UInt16(v) => json!(v),
        AnyValue::UInt32(v) => json!(v),
        AnyValue::UInt64(v) => json!(v),
        AnyValue::Float32(v) => json!(v),
        AnyValue::Float64(v) => json!(v),
        other => json!(other.to_string()),
    }
}

/// Whether a column's value differs between the candidate (`name`) and the baseline
/// (`name` + BASE) in a joined frame.
fn differs(name: &str, dtype: &DataType, tolerance: f64) -> Expr {
    let candidate = col(name);
    let baseline = col(format!("{name}{BASE}"));
    let one_null = candidate.clone().is_null().xor(baseline.clone().is_null());
    if dtype.is_float() {
        let a = candidate.clone().cast(DataType::Float64);
        let b = baseline.clone().cast(DataType::Float64);
        let both = candidate.is_not_null().and(baseline.is_not_null());
        let scale = a.clone().abs().max_horizontal_with(b.clone().abs());
        let far = (a.clone() - b.clone()).abs().gt(scale * lit(tolerance));
        // NaN equals NaN; a NaN against a number differs.
        let nan = a.clone().is_nan().xor(b.clone().is_nan());
        one_null.or(both.and(far.fill_null(lit(false)).or(nan)))
    } else {
        one_null.or(candidate.neq(baseline).fill_null(lit(false)))
    }
}

/// `max(a, b)` of two expressions, row by row.
trait MaxWith {
    fn max_horizontal_with(self, other: Expr) -> Expr;
}

impl MaxWith for Expr {
    fn max_horizontal_with(self, other: Expr) -> Expr {
        when(self.clone().gt_eq(other.clone()))
            .then(self)
            .otherwise(other)
    }
}

pub fn diff(baseline: &str, candidate: &str, request: &str) -> Result<Value, AtlasError> {
    let request: DiffRequest = serde_json::from_str(request)
        .map_err(|error| invalid(format!("Invalid comparison request: {error}")))?;
    if !request.tolerance.is_finite() || request.tolerance < 0.0 {
        return Err(invalid("The tolerance is a number, 0 or more"));
    }
    let base = Table::open(baseline, 0)?;
    let cand = Table::open(candidate, 0)?;
    let base_schema = base.scan()?.collect_schema().map_err(polars)?;
    let cand_schema = cand.scan()?.collect_schema().map_err(polars)?;

    let mut added_columns = Vec::new();
    let mut removed_columns = Vec::new();
    let mut retyped = Vec::new();
    let mut common: Vec<(String, DataType)> = Vec::new();
    for (name, dtype) in cand_schema.iter() {
        match base_schema.get(name) {
            None => added_columns.push(json!({"name": name.as_str(), "type": dtype.to_string()})),
            Some(old) if old != dtype => retyped.push(json!({
                "name": name.as_str(), "from": old.to_string(), "to": dtype.to_string(),
            })),
            Some(_) => common.push((name.to_string(), dtype.clone())),
        }
    }
    for (name, dtype) in base_schema.iter() {
        if !cand_schema.contains(name) {
            removed_columns.push(json!({"name": name.as_str(), "type": dtype.to_string()}));
        }
    }
    let schema = json!({
        "added": added_columns, "removed": removed_columns, "changed": retyped,
    });
    let rows = json!({"baseline": base.row_count(), "candidate": cand.row_count()});

    let common_names: Vec<&str> = common.iter().map(|(n, _)| n.as_str()).collect();
    for key in &request.keys {
        if !common_names.contains(&key.as_str()) {
            return Err(invalid(format!(
                "The key column {key} is not in both outputs with one type"
            )));
        }
    }
    if request.keys.is_empty() {
        return multiset(&base, &cand, &common_names, schema, rows);
    }
    let keys: Vec<Expr> = request.keys.iter().map(col).collect();
    let duplicates = |table: &Table| -> Result<u64, AtlasError> {
        count(
            table
                .scan()?
                .group_by(keys.clone())
                .agg([len().alias("n")])
                .filter(col("n").gt(lit(1))),
        )
    };
    let duplicate_keys = json!({"baseline": duplicates(&base)?, "candidate": duplicates(&cand)?});
    if duplicate_keys["baseline"] != 0 || duplicate_keys["candidate"] != 0 {
        return Ok(json!({
            "schema": schema, "rows": rows, "keys": request.keys,
            "duplicate_keys": duplicate_keys, "complete": false,
        }));
    }
    let keys_only = |frame: LazyFrame| frame.select(keys.clone());
    let anti = |left: LazyFrame, right: LazyFrame| {
        left.join(
            right,
            keys.clone(),
            keys.clone(),
            JoinArgs::new(JoinType::Anti),
        )
    };
    let added = anti(cand.scan()?, keys_only(base.scan()?));
    let removed = anti(base.scan()?, keys_only(cand.scan()?));
    let added_count = count(added.clone())?;
    let removed_count = count(removed.clone())?;
    let sample = |frame: LazyFrame| -> Result<Vec<Value>, AtlasError> {
        Ok(rows_json(
            keys_only(frame)
                .sort(request.keys.clone(), Default::default())
                .limit(request.samples as IdxSize)
                .collect()
                .map_err(polars)?,
        ))
    };
    let compared: Vec<&(String, DataType)> = common
        .iter()
        .filter(|(n, _)| !request.keys.contains(n))
        .collect();
    let base_side = base.scan()?.select(
        keys.iter()
            .cloned()
            .chain(
                compared
                    .iter()
                    .map(|(n, _)| col(n.as_str()).alias(format!("{n}{BASE}"))),
            )
            .collect::<Vec<_>>(),
    );
    let cand_side = cand.scan()?.select(
        keys.iter()
            .cloned()
            .chain(compared.iter().map(|(n, _)| col(n.as_str())))
            .collect::<Vec<_>>(),
    );
    let matched = cand_side.join(
        base_side,
        keys.clone(),
        keys.clone(),
        JoinArgs::new(JoinType::Inner),
    );
    let flags: Vec<Expr> = compared
        .iter()
        .map(|(n, t)| differs(n, t, request.tolerance).alias(format!("{n}__changed")))
        .collect();
    let any_changed = compared
        .iter()
        .map(|(n, t)| differs(n, t, request.tolerance))
        .reduce(|a, b| a.or(b))
        .unwrap_or(lit(false));
    let flagged = matched
        .with_columns(flags)
        .with_column(any_changed.alias("__changed"));
    let mut totals: Vec<Expr> = vec![
        len().alias("__matched"),
        col("__changed")
            .cast(DataType::UInt64)
            .sum()
            .alias("__changed_rows"),
    ];
    totals.extend(compared.iter().map(|(n, _)| {
        col(format!("{n}__changed"))
            .cast(DataType::UInt64)
            .sum()
            .alias(n.as_str())
    }));
    let summed = flagged.clone().select(totals).collect().map_err(polars)?;
    let total = |name: &str| -> u64 {
        summed
            .column(name)
            .ok()
            .and_then(|c| c.get(0).ok())
            .and_then(|v| v.extract::<u64>())
            .unwrap_or(0)
    };
    let matched_rows = total("__matched");
    let changed_rows = total("__changed_rows");
    let mut columns = serde_json::Map::new();
    for (name, _) in &compared {
        let changed = total(name);
        if changed > 0 {
            columns.insert(name.clone(), json!(changed));
        }
    }
    // Sample changed rows: their keys and the columns that changed.
    let changed_sample = flagged
        .filter(col("__changed"))
        .sort(request.keys.clone(), Default::default())
        .limit(request.samples as IdxSize)
        .collect()
        .map_err(polars)?;
    let mut changed_samples = Vec::new();
    for row in 0..changed_sample.height() {
        let mut key = serde_json::Map::new();
        for name in &request.keys {
            let value = changed_sample
                .column(name)
                .map_err(polars)?
                .get(row)
                .map(|v| any_json(&v))
                .unwrap_or(Value::Null);
            key.insert(name.clone(), value);
        }
        let mut changed = Vec::new();
        for (name, _) in &compared {
            let flag = changed_sample
                .column(&format!("{name}__changed"))
                .map_err(polars)?
                .get(row)
                .ok()
                .and_then(|v| v.extract::<u8>())
                .unwrap_or(0);
            if flag == 1 {
                changed.push(name.clone());
            }
        }
        changed_samples.push(json!({"key": key, "columns": changed}));
    }
    Ok(json!({
        "schema": schema,
        "rows": rows,
        "keys": request.keys,
        "duplicate_keys": duplicate_keys,
        "complete": true,
        "added": added_count,
        "removed": removed_count,
        "changed": changed_rows,
        "unchanged": matched_rows - changed_rows,
        "columns": columns,
        "samples": {
            "added": sample(added)?,
            "removed": sample(removed)?,
            "changed": changed_samples,
        },
    }))
}

/// Without a key: each distinct row of the common columns counted in both, and the rows
/// one has more of than the other.
fn multiset(
    base: &Table,
    cand: &Table,
    common: &[&str],
    schema: Value,
    rows: Value,
) -> Result<Value, AtlasError> {
    if common.is_empty() {
        return Ok(json!({"schema": schema, "rows": rows, "keys": [], "complete": false}));
    }
    let columns: Vec<Expr> = common.iter().map(|n| col(*n)).collect();
    let counted = |table: &Table, name: &str| -> Result<LazyFrame, AtlasError> {
        Ok(table
            .scan()?
            .group_by(columns.clone())
            .agg([len().cast(DataType::Int64).alias(name)]))
    };
    let joined = counted(cand, "__c")?.join(
        counted(base, "__b")?,
        columns.clone(),
        columns.clone(),
        JoinArgs::new(JoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns),
    );
    let totals = joined
        .select([
            (col("__c").fill_null(lit(0i64)) - col("__b").fill_null(lit(0i64)))
                .clip_min(lit(0i64))
                .sum()
                .alias("only_candidate"),
            (col("__b").fill_null(lit(0i64)) - col("__c").fill_null(lit(0i64)))
                .clip_min(lit(0i64))
                .sum()
                .alias("only_baseline"),
        ])
        .collect()
        .map_err(polars)?;
    let total = |name: &str| -> u64 {
        totals
            .column(name)
            .ok()
            .and_then(|c| c.get(0).ok())
            .and_then(|v| v.extract::<i64>())
            .unwrap_or(0) as u64
    };
    Ok(json!({
        "schema": schema,
        "rows": rows,
        "keys": [],
        "complete": true,
        "added": total("only_candidate"),
        "removed": total("only_baseline"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &std::path::Path, name: &str, mut frame: DataFrame) -> String {
        let path = dir.join(name);
        let file = std::fs::File::create(&path).unwrap();
        ParquetWriter::new(file).finish(&mut frame).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn rows_are_added_removed_and_changed_by_key_with_float_tolerance() {
        let dir = tempfile::tempdir().unwrap();
        let base = write(
            dir.path(),
            "b.parquet",
            df!(
                "id" => [1i64, 2, 3, 4],
                "name" => ["a", "b", "c", "d"],
                "price" => [1.0f64, 2.0, 3.0, 4.0],
                "gone" => [true, true, true, true],
            )
            .unwrap(),
        );
        let cand = write(
            dir.path(),
            "c.parquet",
            df!(
                "id" => [2i64, 3, 4, 5],
                "name" => ["b", "C", "d", "e"],
                "price" => [2.0 + 1e-12, 3.0, 4.5, 5.0],
                "new" => [1i32, 1, 1, 1],
            )
            .unwrap(),
        );

        let result = diff(&base, &cand, r#"{"keys": ["id"]}"#).unwrap();

        assert_eq!(result["rows"], json!({"baseline": 4, "candidate": 4}));
        assert_eq!(
            result["schema"]["added"],
            json!([{"name": "new", "type": "i32"}])
        );
        assert_eq!(
            result["schema"]["removed"],
            json!([{"name": "gone", "type": "bool"}])
        );
        assert_eq!(
            (
                &result["added"],
                &result["removed"],
                &result["changed"],
                &result["unchanged"]
            ),
            (&json!(1), &json!(1), &json!(2), &json!(1))
        );
        assert_eq!(result["columns"], json!({"name": 1, "price": 1}));
        assert_eq!(result["samples"]["added"], json!([{"id": 5}]));
        assert_eq!(result["samples"]["removed"], json!([{"id": 1}]));
        assert_eq!(
            result["samples"]["changed"],
            json!([
                {"key": {"id": 3}, "columns": ["name"]},
                {"key": {"id": 4}, "columns": ["price"]},
            ])
        );
    }

    #[test]
    fn duplicate_keys_and_retyped_columns_are_reported_and_multisets_count_rows() {
        let dir = tempfile::tempdir().unwrap();
        let base = write(
            dir.path(),
            "b.parquet",
            df!(
                "id" => [1i64, 1, 2], "v" => ["x", "x", "y"], "n" => [1i64, 2, 3],
            )
            .unwrap(),
        );
        let cand = write(
            dir.path(),
            "c.parquet",
            df!(
                "id" => [1i64, 2, 3], "v" => ["x", "y", "z"], "n" => [1.0f64, 2.0, 3.0],
            )
            .unwrap(),
        );

        let keyed = diff(&base, &cand, r#"{"keys": ["id"]}"#).unwrap();
        assert_eq!(
            keyed["duplicate_keys"],
            json!({"baseline": 1, "candidate": 0})
        );
        assert_eq!(keyed["complete"], json!(false));
        assert_eq!(
            keyed["schema"]["changed"],
            json!([{"name": "n", "from": "i64", "to": "f64"}])
        );

        // id and v as multisets: (1, x) twice before, once now; (3, z) is new.
        let rows = diff(&base, &cand, "{}").unwrap();
        assert_eq!((&rows["added"], &rows["removed"]), (&json!(1), &json!(1)));

        let error = diff(&base, &cand, r#"{"keys": ["n"]}"#)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("not in both outputs with one type"),
            "{error}"
        );
    }
}
