//! The view protocol: typed filters and sort keys over column ids. Requests carry no
//! expressions, so every query the engine runs is built from validated parts.

use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::error::AtlasError;
use crate::schema::{ColumnInfo, Kind};

pub const MAX_FILTERS: usize = 16;
pub const MAX_SORT_KEYS: usize = 8;
const MAX_VALUE_CHARS: usize = 1_000;

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ViewSpec {
    #[serde(default)]
    pub filters: Vec<Filter>,
    #[serde(default)]
    pub sort: Vec<SortKey>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub column_id: usize,
    pub op: FilterOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub high: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    IsNull,
    IsNotNull,
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Between,
    Contains,
    NotContains,
    StartsWith,
    EndsWith,
    IsEmpty,
    IsNotEmpty,
    IsTrue,
    IsFalse,
    IsNan,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SortKey {
    pub column_id: usize,
    pub descending: bool,
}

impl ViewSpec {
    pub fn parse(text: &str) -> Result<Self, AtlasError> {
        let view: ViewSpec = serde_json::from_str(text)
            .map_err(|error| AtlasError::invalid(format!("Invalid view: {error}")))?;
        if view.filters.len() > MAX_FILTERS {
            return Err(AtlasError::invalid(format!(
                "A view takes at most {MAX_FILTERS} filters"
            )));
        }
        if view.sort.len() > MAX_SORT_KEYS {
            return Err(AtlasError::invalid(format!(
                "A view takes at most {MAX_SORT_KEYS} sort keys"
            )));
        }
        Ok(view)
    }

    pub fn is_identity(&self) -> bool {
        self.filters.is_empty() && self.sort.is_empty()
    }

    /// The same view written the same way, for cache keys.
    pub fn canonical(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn has_filters(&self) -> bool {
        !self.filters.is_empty()
    }
}

fn column(columns: &[ColumnInfo], id: usize) -> Result<&ColumnInfo, AtlasError> {
    columns
        .get(id)
        .ok_or_else(|| AtlasError::invalid(format!("Unknown column {id}")))
}

fn required<'a>(value: &'a Option<String>, label: &str) -> Result<&'a str, AtlasError> {
    let text = value
        .as_deref()
        .ok_or_else(|| AtlasError::invalid(format!("The filter needs a {label}")))?;
    if text.chars().count() > MAX_VALUE_CHARS {
        return Err(AtlasError::invalid(format!(
            "Filter values take at most {MAX_VALUE_CHARS} characters"
        )));
    }
    Ok(text)
}

/// The column as an expression to compare with: categorical columns compare by their
/// text, decimals by their exact value.
fn comparable(info: &ColumnInfo) -> Expr {
    match info.dtype {
        DataType::Categorical(_, _) | DataType::Enum(_, _) => {
            col(info.name.as_str()).cast(DataType::String)
        }
        _ => col(info.name.as_str()),
    }
}

/// A literal of the column's type from the text the browser sends; integers stay exact.
fn literal(info: &ColumnInfo, text: &str) -> Result<Expr, AtlasError> {
    let value = text.trim();
    let dtype = &info.dtype;
    match dtype {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => value
            .parse::<i64>()
            .map(|parsed| lit(parsed).cast(dtype.clone()))
            .map_err(|_| AtlasError::invalid(format!("{value} is not an integer"))),
        DataType::Int128 => value
            .parse::<i128>()
            .map(|parsed| lit(Scalar::new(DataType::Int128, AnyValue::Int128(parsed))))
            .map_err(|_| AtlasError::invalid(format!("{value} is not an integer"))),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => value
            .parse::<u64>()
            .map(|parsed| lit(parsed).cast(dtype.clone()))
            .map_err(|_| AtlasError::invalid(format!("{value} is not an unsigned integer"))),
        DataType::Float32 | DataType::Float64 => {
            let parsed = value
                .parse::<f64>()
                .map_err(|_| AtlasError::invalid(format!("{value} is not a number")))?;
            if !parsed.is_finite() {
                return Err(AtlasError::invalid(
                    "Filters compare with finite numbers; use is NaN for NaN",
                ));
            }
            Ok(lit(parsed).cast(dtype.clone()))
        }
        DataType::Decimal(_, _) => {
            value
                .parse::<f64>()
                .map_err(|_| AtlasError::invalid(format!("{value} is not a number")))?;
            // Strict cast from text keeps every digit of the value.
            Ok(lit(value.to_string()).strict_cast(dtype.clone()))
        }
        DataType::Boolean => match value.to_ascii_lowercase().as_str() {
            "true" => Ok(lit(true)),
            "false" => Ok(lit(false)),
            _ => Err(AtlasError::invalid("Boolean filters take true or false")),
        },
        DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) => {
            Ok(lit(text.to_string()))
        }
        DataType::Date => Ok(lit(value.to_string()).strict_cast(DataType::Date)),
        DataType::Datetime(unit, zone) => {
            // A value without an offset is a wall time in the column's zone, as shown in
            // the grid; one with an offset is an instant.
            let naive = lit(value.to_string()).str().to_datetime(
                Some(*unit),
                None,
                StrptimeOptions {
                    format: None,
                    strict: true,
                    exact: true,
                    cache: false,
                },
                lit("raise"),
            );
            let expression = match zone {
                Some(zone) => naive.dt().replace_time_zone(
                    Some(zone.clone()),
                    lit("raise"),
                    NonExistent::Raise,
                ),
                None => naive,
            };
            Ok(expression)
        }
        DataType::Time => Ok(lit(value.to_string()).strict_cast(DataType::Time)),
        _ => Err(AtlasError::invalid(format!(
            "Columns of type {dtype} take null filters only"
        ))),
    }
}

/// Polars orders NaN above every number, so it would pass greater than; it fails every
/// ordering comparison, as in IEEE 754 and pandas.
fn ordering(info: &ColumnInfo, comparison: Expr) -> Expr {
    if matches!(info.dtype, DataType::Float32 | DataType::Float64) {
        comparison.and(col(info.name.as_str()).is_nan().not())
    } else {
        comparison
    }
}

fn filter_expr(filter: &Filter, info: &ColumnInfo) -> Result<Expr, AtlasError> {
    let name = info.name.as_str();
    let target = comparable(info);
    let value = || literal(info, required(&filter.value, "value")?);
    let text_only = |op: &str| -> Result<(), AtlasError> {
        if info.kind == Kind::String {
            Ok(())
        } else {
            Err(AtlasError::invalid(format!("{op} applies to text columns")))
        }
    };
    let ordered = |op: &str| -> Result<(), AtlasError> {
        match info.kind {
            Kind::Numeric | Kind::Temporal | Kind::String => Ok(()),
            _ => Err(AtlasError::invalid(format!(
                "{op} does not apply to {} columns",
                info.kind.as_str()
            ))),
        }
    };
    Ok(match filter.op {
        FilterOp::IsNull => col(name).is_null(),
        FilterOp::IsNotNull => col(name).is_not_null(),
        FilterOp::Eq => {
            ordered_or_equal(info)?;
            target.eq(value()?)
        }
        FilterOp::Ne => {
            ordered_or_equal(info)?;
            // Not equal keeps missing values out, as a comparison with null is null.
            target.neq(value()?)
        }
        FilterOp::Lt => {
            ordered("less than")?;
            ordering(info, target.lt(value()?))
        }
        FilterOp::Lte => {
            ordered("at most")?;
            ordering(info, target.lt_eq(value()?))
        }
        FilterOp::Gt => {
            ordered("greater than")?;
            ordering(info, target.gt(value()?))
        }
        FilterOp::Gte => {
            ordered("at least")?;
            ordering(info, target.gt_eq(value()?))
        }
        FilterOp::Between => {
            ordered("between")?;
            let low = value()?;
            let high = literal(info, required(&filter.high, "upper value")?)?;
            ordering(info, target.clone().gt_eq(low).and(target.lt_eq(high)))
        }
        FilterOp::Contains | FilterOp::NotContains | FilterOp::StartsWith | FilterOp::EndsWith => {
            text_only("Text matching")?;
            let needle = lit(required(&filter.value, "value")?.to_string());
            match filter.op {
                FilterOp::Contains => target.str().contains_literal(needle),
                FilterOp::NotContains => target.str().contains_literal(needle).not(),
                FilterOp::StartsWith => target.str().starts_with(needle),
                _ => target.str().ends_with(needle),
            }
        }
        FilterOp::IsEmpty => {
            text_only("is empty")?;
            target.eq(lit(""))
        }
        FilterOp::IsNotEmpty => {
            text_only("is not empty")?;
            target.neq(lit(""))
        }
        FilterOp::IsTrue | FilterOp::IsFalse => {
            if info.kind != Kind::Boolean {
                return Err(AtlasError::invalid(
                    "is true and is false apply to boolean columns",
                ));
            }
            if filter.op == FilterOp::IsTrue {
                col(name).eq(lit(true))
            } else {
                col(name).eq(lit(false))
            }
        }
        FilterOp::IsNan => {
            if !matches!(info.dtype, DataType::Float32 | DataType::Float64) {
                return Err(AtlasError::invalid(
                    "is NaN applies to floating point columns",
                ));
            }
            col(name).is_nan()
        }
    })
}

fn ordered_or_equal(info: &ColumnInfo) -> Result<(), AtlasError> {
    match info.kind {
        Kind::Numeric | Kind::Temporal | Kind::String | Kind::Boolean => Ok(()),
        _ => Err(AtlasError::invalid(format!(
            "Equality does not apply to {} columns",
            info.kind.as_str()
        ))),
    }
}

/// The view's filters applied to a frame.
pub fn apply_filters(
    mut frame: LazyFrame,
    columns: &[ColumnInfo],
    view: &ViewSpec,
) -> Result<LazyFrame, AtlasError> {
    for filter in &view.filters {
        let info = column(columns, filter.column_id)?;
        frame = frame.filter(filter_expr(filter, info)?);
    }
    Ok(frame)
}

/// The view's sort, stable, with missing values last. Categorical columns sort by text.
pub fn apply_sort(
    frame: LazyFrame,
    columns: &[ColumnInfo],
    view: &ViewSpec,
) -> Result<LazyFrame, AtlasError> {
    if view.sort.is_empty() {
        return Ok(frame);
    }
    let mut keys = Vec::with_capacity(view.sort.len());
    let mut descending = Vec::with_capacity(view.sort.len());
    for key in &view.sort {
        let info = column(columns, key.column_id)?;
        if matches!(info.kind, Kind::Other) && !matches!(info.dtype, DataType::Binary) {
            return Err(AtlasError::invalid(format!(
                "Columns of type {} cannot be sorted",
                info.dtype
            )));
        }
        keys.push(comparable(info));
        descending.push(key.descending);
    }
    Ok(frame.sort_by_exprs(
        keys,
        SortMultipleOptions::default()
            .with_order_descending_multi(descending)
            .with_nulls_last(true)
            .with_maintain_order(true),
    ))
}

/// Checks a view against the columns without running it.
pub fn validate(columns: &[ColumnInfo], view: &ViewSpec) -> Result<(), AtlasError> {
    for filter in &view.filters {
        let _ = filter_expr(filter, column(columns, filter.column_id)?)?;
    }
    for key in &view.sort {
        column(columns, key.column_id)?;
    }
    Ok(())
}
