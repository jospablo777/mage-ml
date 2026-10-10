//! Column summaries over a view: one aggregation scan per column, then a histogram or a
//! value count. Every number is exact except distinct counts of numeric and temporal
//! columns, which are HyperLogLog estimates and say so.

use polars::prelude::*;
use serde_json::{json, Value};

use crate::engine::Table;
use crate::error::AtlasError;
use crate::format;
use crate::schema::{ColumnInfo, Kind};

const TOP_VALUES: usize = 10;

fn value(frame: &DataFrame, name: &str) -> Result<AnyValue<'static>, AtlasError> {
    Ok(frame.column(name)?.get(0)?.into_static())
}

fn int(frame: &DataFrame, name: &str) -> Result<u64, AtlasError> {
    Ok(value(frame, name)?.extract::<u64>().unwrap_or(0))
}

fn number(frame: &DataFrame, name: &str) -> Result<Option<f64>, AtlasError> {
    Ok(value(frame, name)?
        .extract::<f64>()
        .filter(|value| value.is_finite()))
}

fn text(frame: &DataFrame, name: &str) -> Result<Option<String>, AtlasError> {
    Ok(format::cell(&value(frame, name)?))
}

fn finite(number: Option<f64>) -> Value {
    match number {
        Some(value) if value.is_finite() => json!(value),
        _ => Value::Null,
    }
}

pub fn column(
    table: &Table,
    filtered: LazyFrame,
    info: &ColumnInfo,
    bins: usize,
) -> Result<Value, AtlasError> {
    let mut summary = match info.kind {
        Kind::Numeric => numeric(table, filtered, info, bins)?,
        Kind::String => categorical(table, filtered, info)?,
        Kind::Boolean => boolean(table, filtered, info)?,
        Kind::Temporal => temporal(table, filtered, info, bins)?,
        Kind::Nested | Kind::Other => basic(table, filtered, info)?,
    };
    summary["column_id"] = json!(info.id);
    summary["kind"] = json!(info.kind.as_str());
    Ok(summary)
}

fn basic(table: &Table, filtered: LazyFrame, info: &ColumnInfo) -> Result<Value, AtlasError> {
    let name = info.name.as_str();
    let result = table.collect(filtered.select([
        len().alias("count"),
        col(name).null_count().alias("missing"),
    ]))?;
    Ok(json!({
        "count": int(&result, "count")?,
        "missing": int(&result, "missing")?,
        "distinct": Value::Null,
        "distinct_exact": false,
        "metrics": {},
        "histogram": [],
        "top_values": [],
    }))
}

fn is_float(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Float32 | DataType::Float64)
}

fn numeric(
    table: &Table,
    filtered: LazyFrame,
    info: &ColumnInfo,
    bins: usize,
) -> Result<Value, AtlasError> {
    let name = info.name.as_str();
    let float = is_float(&info.dtype);
    // Finite values only: NaN and infinity are counted on their own, as missing values are.
    let finite_values = if float {
        col(name).filter(col(name).is_finite())
    } else {
        col(name)
    };
    let as_float = finite_values.clone().cast(DataType::Float64);
    let mut aggregations = vec![
        len().alias("count"),
        col(name).null_count().alias("missing"),
        finite_values.clone().count().alias("finite"),
        finite_values.clone().min().alias("min"),
        finite_values.clone().max().alias("max"),
        as_float.clone().mean().alias("mean"),
        as_float.clone().median().alias("median"),
        as_float.clone().std(1).alias("std"),
        as_float
            .clone()
            .quantile(lit(0.25), QuantileMethod::Linear)
            .alias("q25"),
        as_float
            .clone()
            .quantile(lit(0.75), QuantileMethod::Linear)
            .alias("q75"),
        finite_values.clone().eq(lit(0)).sum().alias("zeros"),
        finite_values.clone().lt(lit(0)).sum().alias("negatives"),
        col(name).approx_n_unique().alias("distinct"),
    ];
    if float {
        aggregations.push(col(name).is_nan().sum().alias("nan"));
        aggregations.push(col(name).is_infinite().sum().alias("infinite"));
    }
    let result = table.collect(filtered.clone().select(aggregations))?;
    let finite_count = int(&result, "finite")?;
    let minimum = number_of(&result, "min")?;
    let maximum = number_of(&result, "max")?;

    let histogram = match (minimum, maximum) {
        (Some(low), Some(high)) if finite_count > 0 => histogram(
            table,
            filtered,
            name,
            float,
            low,
            high,
            bins,
            info.dtype.is_integer(),
        )?,
        _ => Vec::new(),
    };
    Ok(json!({
        "count": int(&result, "count")?,
        "missing": int(&result, "missing")?,
        "distinct": int(&result, "distinct")?,
        "distinct_exact": false,
        "metrics": {
            "min": text(&result, "min")?,
            "max": text(&result, "max")?,
            "mean": finite(number(&result, "mean")?),
            "median": finite(number(&result, "median")?),
            "std": finite(number(&result, "std")?),
            "q25": finite(number(&result, "q25")?),
            "q75": finite(number(&result, "q75")?),
            "zeros": int(&result, "zeros")?,
            "negatives": int(&result, "negatives")?,
            "finite": finite_count,
            "nan": if float { json!(int(&result, "nan")?) } else { json!(0) },
            "infinite": if float { json!(int(&result, "infinite")?) } else { json!(0) },
        },
        "histogram": histogram,
        "top_values": [],
    }))
}

/// The minimum or maximum as a float for histogram edges; decimals and wide integers are
/// read through their text.
fn number_of(frame: &DataFrame, name: &str) -> Result<Option<f64>, AtlasError> {
    let value = value(frame, name)?;
    if let Some(number) = value.extract::<f64>() {
        return Ok(Some(number).filter(|number| number.is_finite()));
    }
    Ok(format::cell(&value).and_then(|text| text.parse::<f64>().ok()))
}

#[allow(clippy::too_many_arguments)]
fn histogram(
    table: &Table,
    filtered: LazyFrame,
    name: &str,
    float: bool,
    low: f64,
    high: f64,
    bins: usize,
    integer: bool,
) -> Result<Vec<Value>, AtlasError> {
    let values = col(name).cast(DataType::Float64);
    let mut frame = filtered.filter(col(name).is_not_null());
    if float {
        frame = frame.filter(col(name).is_finite());
    }
    if high <= low {
        let count = table.collect(frame.select([len().alias("n")]))?;
        return Ok(vec![
            json!({ "start": low, "end": high, "count": int(&count, "n")? }),
        ]);
    }
    // Integers spanning fewer values than bins get one bin per value, so no bin covers a
    // fraction of an integer.
    let (bins, width) = if integer && (high - low + 1.0) <= bins as f64 {
        let count = (high - low + 1.0) as usize;
        (count, 1.0)
    } else {
        (bins, (high - low) / bins as f64)
    };
    let last = bins as i64 - 1;
    let bucket = ((values - lit(low)) / lit(width))
        .floor()
        .cast(DataType::Int64);
    let bucket = when(bucket.clone().lt(lit(0i64)))
        .then(lit(0i64))
        .when(bucket.clone().gt(lit(last)))
        .then(lit(last))
        .otherwise(bucket);
    let grouped = table.collect(
        frame
            .select([bucket.alias("bucket")])
            .group_by([col("bucket")])
            .agg([len().alias("n")]),
    )?;
    let mut counts = vec![0u64; bins];
    let buckets = grouped.column("bucket")?.as_materialized_series().clone();
    let totals = grouped.column("n")?.as_materialized_series().clone();
    for index in 0..grouped.height() {
        let bucket = buckets.get(index)?.extract::<i64>();
        let total = totals.get(index)?.extract::<u64>().unwrap_or(0);
        if let Some(bucket) = bucket.filter(|bucket| *bucket >= 0 && (*bucket as usize) < bins) {
            counts[bucket as usize] += total;
        }
    }
    Ok(counts
        .iter()
        .enumerate()
        .map(|(index, count)| {
            let start = low + width * index as f64;
            let end = if integer && width == 1.0 {
                start
            } else if index + 1 == bins {
                high
            } else {
                low + width * (index + 1) as f64
            };
            json!({ "start": start, "end": end, "count": count })
        })
        .collect())
}

fn categorical(table: &Table, filtered: LazyFrame, info: &ColumnInfo) -> Result<Value, AtlasError> {
    let name = info.name.as_str();
    let values = match info.dtype {
        DataType::String => col(name),
        _ => col(name).cast(DataType::String),
    };
    let lengths = values.clone().str().len_chars();
    let result = table.collect(filtered.clone().select([
        len().alias("count"),
        col(name).null_count().alias("missing"),
        values.clone().eq(lit("")).sum().alias("empty"),
        lengths.clone().min().alias("min_length"),
        lengths.clone().max().alias("max_length"),
        lengths.mean().alias("mean_length"),
    ]))?;
    let groups = filtered
        .filter(col(name).is_not_null())
        .group_by([values.alias("value")])
        .agg([len().alias("n")]);
    let distinct = table.collect(groups.clone().select([len().alias("distinct")]))?;
    let top = table.collect(
        groups
            .sort_by_exprs(
                [col("n"), col("value")],
                SortMultipleOptions::default().with_order_descending_multi([true, false]),
            )
            .limit(TOP_VALUES as IdxSize),
    )?;
    let count = int(&result, "count")?;
    let missing = int(&result, "missing")?;
    let (top_values, shown) = top_values(&top)?;
    Ok(json!({
        "count": count,
        "missing": missing,
        "distinct": int(&distinct, "distinct")?,
        "distinct_exact": true,
        "metrics": {
            "empty": int(&result, "empty")?,
            "min_length": int(&result, "min_length")?,
            "max_length": int(&result, "max_length")?,
            "mean_length": finite(number(&result, "mean_length")?),
        },
        "histogram": [],
        "top_values": top_values,
        "other_count": count.saturating_sub(missing).saturating_sub(shown),
    }))
}

fn top_values(frame: &DataFrame) -> Result<(Vec<Value>, u64), AtlasError> {
    let values = frame.column("value")?.as_materialized_series().clone();
    let counts = frame.column("n")?.as_materialized_series().clone();
    let mut entries = Vec::with_capacity(frame.height());
    let mut shown = 0u64;
    for index in 0..frame.height() {
        let count = counts.get(index)?.extract::<u64>().unwrap_or(0);
        shown += count;
        entries.push(json!({ "value": format::cell(&values.get(index)?), "count": count }));
    }
    Ok((entries, shown))
}

fn boolean(table: &Table, filtered: LazyFrame, info: &ColumnInfo) -> Result<Value, AtlasError> {
    let name = info.name.as_str();
    let result = table.collect(filtered.select([
        len().alias("count"),
        col(name).null_count().alias("missing"),
        col(name).sum().alias("true"),
    ]))?;
    let count = int(&result, "count")?;
    let missing = int(&result, "missing")?;
    let trues = int(&result, "true")?;
    let falses = count.saturating_sub(missing).saturating_sub(trues);
    let mut top = vec![
        json!({ "value": "true", "count": trues }),
        json!({ "value": "false", "count": falses }),
    ];
    top.sort_by(|a, b| b["count"].as_u64().cmp(&a["count"].as_u64()));
    Ok(json!({
        "count": count,
        "missing": missing,
        "distinct": (trues > 0) as u64 + (falses > 0) as u64,
        "distinct_exact": true,
        "metrics": { "true": trues, "false": falses },
        "histogram": [],
        "top_values": top,
        "other_count": 0,
    }))
}

fn temporal(
    table: &Table,
    filtered: LazyFrame,
    info: &ColumnInfo,
    bins: usize,
) -> Result<Value, AtlasError> {
    let name = info.name.as_str();
    let physical = col(name).to_physical().cast(DataType::Int64);
    let result = table.collect(filtered.clone().select([
        len().alias("count"),
        col(name).null_count().alias("missing"),
        col(name).min().alias("min"),
        col(name).max().alias("max"),
        physical.clone().min().alias("low"),
        physical.clone().max().alias("high"),
        col(name).approx_n_unique().alias("distinct"),
    ]))?;
    let low = value(&result, "low")?.extract::<i64>();
    let high = value(&result, "high")?.extract::<i64>();
    let histogram = match (low, high) {
        (Some(low), Some(high)) => temporal_histogram(table, filtered, info, low, high, bins)?,
        _ => Vec::new(),
    };
    Ok(json!({
        "count": int(&result, "count")?,
        "missing": int(&result, "missing")?,
        "distinct": int(&result, "distinct")?,
        "distinct_exact": false,
        "metrics": { "min": text(&result, "min")?, "max": text(&result, "max")? },
        "histogram": histogram,
        "top_values": [],
    }))
}

/// A histogram over the physical values (days, or time units since the epoch); each bin
/// carries its edges as text in the column's type.
fn temporal_histogram(
    table: &Table,
    filtered: LazyFrame,
    info: &ColumnInfo,
    low: i64,
    high: i64,
    bins: usize,
) -> Result<Vec<Value>, AtlasError> {
    let name = info.name.as_str();
    let span = (high - low) as f64;
    let width = if span > 0.0 { span / bins as f64 } else { 1.0 };
    let bins = if span > 0.0 { bins } else { 1 };
    let last = bins as i64 - 1;
    let physical = col(name).to_physical().cast(DataType::Float64);
    let bucket = ((physical - lit(low as f64)) / lit(width))
        .floor()
        .cast(DataType::Int64);
    let bucket = when(bucket.clone().gt(lit(last)))
        .then(lit(last))
        .otherwise(bucket);
    let grouped = table.collect(
        filtered
            .filter(col(name).is_not_null())
            .select([bucket.alias("bucket")])
            .group_by([col("bucket")])
            .agg([len().alias("n")]),
    )?;
    let mut counts = vec![0u64; bins];
    let buckets = grouped.column("bucket")?.as_materialized_series().clone();
    let totals = grouped.column("n")?.as_materialized_series().clone();
    for index in 0..grouped.height() {
        if let Some(bucket) = buckets
            .get(index)?
            .extract::<i64>()
            .filter(|b| *b >= 0 && (*b as usize) < bins)
        {
            counts[bucket as usize] += totals.get(index)?.extract::<u64>().unwrap_or(0);
        }
    }
    let edges: Vec<i64> = (0..=bins)
        .map(|index| {
            if index == bins {
                high
            } else {
                low + (width * index as f64) as i64
            }
        })
        .collect();
    let labels = edge_labels(&edges, &info.dtype)?;
    Ok(counts
        .iter()
        .enumerate()
        .map(|(index, count)| {
            json!({
                "start": edges[index] as f64,
                "end": edges[index + 1] as f64,
                "count": count,
                "start_label": labels[index],
                "end_label": labels[index + 1],
            })
        })
        .collect())
}

fn edge_labels(edges: &[i64], dtype: &DataType) -> Result<Vec<Option<String>>, AtlasError> {
    let physical = Series::new("edges".into(), edges.to_vec());
    let typed = match dtype {
        DataType::Date => physical.cast(&DataType::Int32)?.cast(dtype)?,
        DataType::Time | DataType::Datetime(_, _) | DataType::Duration(_) => {
            physical.cast(dtype)?
        }
        _ => physical,
    };
    (0..typed.len())
        .map(|index| Ok(format::cell(&typed.get(index)?)))
        .collect()
}
