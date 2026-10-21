//! Column profiles for monitoring: for each column of a Parquet file, its missing values
//! and the shape of its values, read over the whole file. Numeric columns get bin counts
//! over edges (the edges given, the baseline's, or quantiles of the column); text and
//! boolean columns get their most frequent values; temporal columns their range.
//!
//! Bins are left-closed: [e0, e1), [e1, e2), ... with an underflow bin below e0 and an
//! overflow bin from the last edge up, so counts has one more entry than edges. NaN is
//! missing and in no bin.

use std::collections::HashMap;

use polars::prelude::*;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::{Error as AtlasError, guarded};
use crate::kind::Kind;
use crate::table::Table;

pub const MAX_BINS: usize = 100;
pub const MAX_TOP: usize = 200;
const VALUE_CHARS: usize = 120;

fn default_bins() -> usize {
    20
}

fn default_top() -> usize {
    20
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileRequest {
    /// The columns to profile; every column when absent.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default = "default_bins")]
    pub bins: usize,
    #[serde(default = "default_top")]
    pub top: usize,
    /// Bin edges for numeric columns, such as a baseline's.
    #[serde(default)]
    pub edges: HashMap<String, Vec<f64>>,
}

fn number(column: &str, dtype: &DataType) -> Expr {
    if matches!(dtype, DataType::Decimal(_, _)) || dtype.is_integer() {
        col(column).cast(DataType::Float64)
    } else {
        col(column)
    }
}

fn missing(column: &str, dtype: &DataType) -> Expr {
    if dtype.is_float() {
        col(column)
            .is_null()
            .or(col(column).is_nan().fill_null(false))
    } else {
        col(column).is_null()
    }
}

fn finite(column: &str, dtype: &DataType) -> Expr {
    let present = col(column).is_not_null();
    if dtype.is_float() {
        present.and(col(column).is_nan().not())
    } else {
        present
    }
}

fn value_u64(frame: &DataFrame, name: &str) -> Result<u64, AtlasError> {
    let value = frame.column(name)?.get(0)?;
    Ok(value.extract::<u64>().unwrap_or(0))
}

fn value_f64(frame: &DataFrame, name: &str) -> Option<f64> {
    frame
        .column(name)
        .ok()?
        .get(0)
        .ok()?
        .extract::<f64>()
        .filter(|v| v.is_finite())
}

fn clean_edges(mut edges: Vec<f64>) -> Vec<f64> {
    edges.retain(|e| e.is_finite());
    edges.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    edges.dedup();
    edges
}

fn shorten(text: &str) -> String {
    if text.chars().count() <= VALUE_CHARS {
        return text.to_string();
    }
    let mut short: String = text.chars().take(VALUE_CHARS).collect();
    short.push('…');
    short
}

pub fn profile(path: &str, request: &str) -> Result<Value, AtlasError> {
    let request: ProfileRequest = serde_json::from_str(request)
        .map_err(|error| AtlasError::invalid(format!("Invalid profile request: {error}")))?;
    if request.bins < 1 || request.bins > MAX_BINS {
        return Err(AtlasError::invalid(format!("bins must be 1 to {MAX_BINS}")));
    }
    if request.top > MAX_TOP {
        return Err(AtlasError::invalid(format!(
            "top must be at most {MAX_TOP}"
        )));
    }
    guarded(path, || profile_unguarded(path, &request))
}

fn profile_unguarded(path: &str, request: &ProfileRequest) -> Result<Value, AtlasError> {
    let table = Table::open(path, 0)?;
    let columns: Vec<(String, DataType, Kind)> = table
        .columns()
        .iter()
        .filter(|c| {
            request
                .columns
                .as_ref()
                .map(|wanted| wanted.contains(&c.name))
                .unwrap_or(true)
        })
        .map(|c| (c.name.clone(), c.dtype.clone(), Kind::of(&c.dtype)))
        .collect();
    if let Some(wanted) = &request.columns {
        let known: Vec<&String> = columns.iter().map(|(name, _, _)| name).collect();
        let unknown: Vec<&String> = wanted.iter().filter(|w| !known.contains(w)).collect();
        if !unknown.is_empty() {
            return Err(AtlasError::invalid(format!(
                "The output has no column {}",
                unknown
                    .iter()
                    .map(|c| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }

    // Rows, missing values, and the quantiles of numeric columns that need edges.
    let mut first = vec![len().cast(DataType::UInt64).alias("__rows")];
    let quantiles: Vec<f64> = (1..request.bins)
        .map(|k| k as f64 / request.bins as f64)
        .collect();
    for (index, (name, dtype, kind)) in columns.iter().enumerate() {
        first.push(
            missing(name, dtype)
                .sum()
                .cast(DataType::UInt64)
                .alias(format!("__missing_{index}")),
        );
        if *kind == Kind::Numeric && !request.edges.contains_key(name) {
            let values = number(name, dtype).filter(finite(name, dtype));
            first.push(values.clone().min().alias(format!("__min_{index}")));
            for (q, quantile) in quantiles.iter().enumerate() {
                first.push(
                    values
                        .clone()
                        .quantile(lit(*quantile), QuantileMethod::Linear)
                        .alias(format!("__q_{index}_{q}")),
                );
            }
        }
        if *kind == Kind::Temporal {
            first.push(
                col(name)
                    .min()
                    .cast(DataType::String)
                    .alias(format!("__tmin_{index}")),
            );
            first.push(
                col(name)
                    .max()
                    .cast(DataType::String)
                    .alias(format!("__tmax_{index}")),
            );
        }
    }
    let summary = table.collect(table.scan()?.select(first))?;
    let rows = value_u64(&summary, "__rows")?;

    // The edges of each numeric column, then all their bin counts in one pass.
    let mut edges_of: HashMap<usize, Vec<f64>> = HashMap::new();
    let mut counting = vec![];
    for (index, (name, dtype, kind)) in columns.iter().enumerate() {
        if *kind != Kind::Numeric {
            continue;
        }
        let edges = match request.edges.get(name) {
            Some(given) => clean_edges(given.clone()),
            None => {
                let mut found: Vec<f64> = (0..quantiles.len())
                    .filter_map(|q| value_f64(&summary, &format!("__q_{index}_{q}")))
                    .collect();
                if let Some(min) = value_f64(&summary, &format!("__min_{index}")) {
                    found.insert(0, min);
                }
                clean_edges(found)
            }
        };
        let value = number(name, dtype);
        let present = finite(name, dtype);
        if let Some(first_edge) = edges.first() {
            counting.push(
                present
                    .clone()
                    .and(value.clone().lt(lit(*first_edge)))
                    .fill_null(false)
                    .sum()
                    .cast(DataType::UInt64)
                    .alias(format!("__bin_{index}_0")),
            );
            for (bin, pair) in edges.windows(2).enumerate() {
                counting.push(
                    present
                        .clone()
                        .and(value.clone().gt_eq(lit(pair[0])))
                        .and(value.clone().lt(lit(pair[1])))
                        .fill_null(false)
                        .sum()
                        .cast(DataType::UInt64)
                        .alias(format!("__bin_{index}_{}", bin + 1)),
                );
            }
            counting.push(
                present
                    .clone()
                    .and(value.clone().gt_eq(lit(*edges.last().unwrap())))
                    .fill_null(false)
                    .sum()
                    .cast(DataType::UInt64)
                    .alias(format!("__bin_{index}_{}", edges.len())),
            );
        }
        edges_of.insert(index, edges);
    }
    let counts = if counting.is_empty() {
        DataFrame::empty()
    } else {
        table.collect(table.scan()?.select(counting))?
    };

    let mut profiled = Vec::new();
    for (index, (name, dtype, kind)) in columns.iter().enumerate() {
        let missing_count = value_u64(&summary, &format!("__missing_{index}"))?;
        let mut entry = json!({
            "name": name,
            "dtype": dtype.to_string(),
            "kind": kind.as_str(),
            "rows": rows,
            "missing": missing_count,
        });
        match kind {
            Kind::Numeric => {
                let edges = edges_of.remove(&index).unwrap_or_default();
                let bins: Vec<u64> = if edges.is_empty() {
                    vec![]
                } else {
                    (0..=edges.len())
                        .map(|bin| value_u64(&counts, &format!("__bin_{index}_{bin}")))
                        .collect::<Result<_, _>>()?
                };
                entry["edges"] = json!(edges);
                entry["counts"] = json!(bins);
            }
            Kind::String | Kind::Boolean => {
                let present = finite(name, dtype);
                let frequent = table.collect(
                    table
                        .scan()?
                        .filter(present)
                        .group_by([col(name).cast(DataType::String).alias("__value")])
                        .agg([len().cast(DataType::UInt64).alias("__count")])
                        .sort_by_exprs(
                            [col("__count"), col("__value")],
                            SortMultipleOptions::default()
                                .with_order_descending_multi([true, false]),
                        )
                        .limit(request.top as IdxSize),
                )?;
                let values = frequent.column("__value")?.str()?.clone();
                let tallies = frequent.column("__count")?.u64()?.clone();
                let mut top = Vec::new();
                let mut covered = 0u64;
                for (value, count) in values.iter().zip(tallies.iter()) {
                    let count = count.unwrap_or(0);
                    covered += count;
                    top.push(json!([shorten(value.unwrap_or("")), count]));
                }
                entry["top"] = json!(top);
                entry["other"] = json!(rows.saturating_sub(missing_count).saturating_sub(covered));
            }
            Kind::Temporal => {
                let text = |key: String| -> Value {
                    summary
                        .column(&key)
                        .ok()
                        .and_then(|c| c.get(0).ok())
                        .and_then(|v| v.get_str().map(str::to_string))
                        .map(Value::String)
                        .unwrap_or(Value::Null)
                };
                entry["min"] = text(format!("__tmin_{index}"));
                entry["max"] = text(format!("__tmax_{index}"));
            }
            _ => {}
        }
        profiled.push(entry);
    }
    Ok(json!({ "rows": rows, "columns": profiled }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(frame: &mut DataFrame) -> tempfile::NamedTempFile {
        let file = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();
        ParquetWriter::new(std::fs::File::create(file.path()).unwrap())
            .finish(frame)
            .unwrap();
        file
    }

    #[test]
    fn numeric_text_and_temporal_columns_are_profiled() {
        let mut frame = df!(
            "x" => [Some(1.0f64), Some(2.0), Some(3.0), Some(4.0), None, Some(f64::NAN)],
            "n" => [1i64, 1, 2, 2, 3, 10],
            "label" => [Some("a"), Some("a"), Some("b"), None, Some("a"), Some("c")],
            "day" => ["2024-01-01", "2024-01-03", "2024-01-02", "2024-01-05", "2024-01-04", "2024-01-06"],
        )
        .unwrap();
        frame = frame
            .lazy()
            .with_column(col("day").cast(DataType::Date))
            .collect()
            .unwrap();
        let file = write(&mut frame);
        let path = file.path().to_str().unwrap();

        let request = json!({"bins": 4, "top": 2, "edges": {"n": [0, 2, 5]}});
        let result = profile(path, &request.to_string()).unwrap();
        let columns = result["columns"].as_array().unwrap();
        assert_eq!(result["rows"], 6);

        let x = &columns[0];
        assert_eq!(x["missing"], 2);
        let counts: u64 = x["counts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_u64().unwrap())
            .sum();
        assert_eq!(counts, 4, "{x}");
        assert_eq!(x["counts"][0], 0, "nothing below the minimum: {x}");

        let n = &columns[1];
        assert_eq!(n["edges"], json!([0.0, 2.0, 5.0]));
        // below 0, [0, 2), [2, 5), 5 and up
        assert_eq!(n["counts"], json!([0, 2, 3, 1]));

        let label = &columns[2];
        assert_eq!(label["missing"], 1);
        assert_eq!(label["top"], json!([["a", 3], ["b", 1]]));
        assert_eq!(label["other"], 1);

        let day = &columns[3];
        assert_eq!(
            (day["min"].as_str(), day["max"].as_str()),
            (Some("2024-01-01"), Some("2024-01-06"))
        );
    }

    #[test]
    fn unknown_columns_and_bad_requests_are_explained() {
        let mut frame = df!("x" => [1i64]).unwrap();
        let file = write(&mut frame);
        let path = file.path().to_str().unwrap();
        let error = profile(path, &json!({"columns": ["y"]}).to_string()).unwrap_err();
        assert!(error.message().contains("no column y"), "{error}");
        let error = profile(path, &json!({"bins": 0}).to_string()).unwrap_err();
        assert!(error.message().contains("bins"), "{error}");
        let error = profile(path, &json!({"bnis": 3}).to_string()).unwrap_err();
        assert!(error.message().contains("unknown field"), "{error}");
    }
}
