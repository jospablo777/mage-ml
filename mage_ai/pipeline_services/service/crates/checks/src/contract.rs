//! Data contracts: checks a Parquet or Arrow IPC file against the column rules of a contract and
//! reports each rule the data breaks, with how many rows break it, the positions of the
//! first rows and a few of the values. Every check reads the whole file; nothing samples.
//!
//! The rules of each column are compiled into boolean Polars expressions that are true for
//! a violating row. One query counts them all; the rules with violations then read their
//! example rows. A contract also infers from a file: its columns, types and nullability.
//!
//! MageML runs check block outputs with it through ColumnAtlas (column_atlas_native), and
//! exported services in the engine (feature `checks` of mage-service): one checker.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::error::{Error, guarded};
use crate::table::Table;

/// The error type the checks return; the name ColumnAtlas used.
type AtlasError = Error;

pub const MAX_EXAMPLES: usize = 100;
const ROW: &str = "__contract_row";
const VALUE_CHARS: usize = 80;
const TYPES: &[&str] = &[
    "any", "integer", "float", "number", "decimal", "string", "boolean", "date", "datetime",
    "time", "duration", "list", "struct",
];

fn yes() -> bool {
    true
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtraColumns {
    #[default]
    Allow,
    Forbid,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub columns: Vec<ColumnRule>,
    /// Columns whose values together identify a row; no two rows may share them.
    #[serde(default)]
    pub unique: Vec<String>,
    #[serde(default)]
    pub extra_columns: ExtraColumns,
    #[serde(default)]
    pub min_rows: Option<u64>,
    #[serde(default)]
    pub max_rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnRule {
    pub name: String,
    #[serde(default, rename = "type")]
    pub dtype: Option<String>,
    #[serde(default = "yes")]
    pub required: bool,
    #[serde(default = "yes")]
    pub nullable: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub min: Option<Value>,
    #[serde(default)]
    pub max: Option<Value>,
    #[serde(default)]
    pub values: Option<Vec<Value>>,
    #[serde(default)]
    pub min_length: Option<u32>,
    #[serde(default)]
    pub max_length: Option<u32>,
    /// A regular expression each value must match in full.
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Violation {
    pub column: Option<String>,
    pub rule: &'static str,
    /// Rows that break the rule; 0 for rules about the schema.
    pub rows: u64,
    pub message: String,
    /// Positions (from 0) of the first violating rows.
    pub examples: Vec<u64>,
    /// A few distinct violating values, as text.
    pub values: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub contract: String,
    pub version: Option<String>,
    pub rows: u64,
    pub passed: bool,
    pub violations: Vec<Violation>,
}

impl Contract {
    pub fn parse(text: &str) -> Result<Self, AtlasError> {
        let contract: Contract = serde_json::from_str(text)
            .map_err(|error| AtlasError::invalid(format!("Invalid contract: {error}")))?;
        contract.check()?;
        Ok(contract)
    }

    fn check(&self) -> Result<(), AtlasError> {
        let mut seen = std::collections::HashSet::new();
        for rule in &self.columns {
            if !seen.insert(rule.name.as_str()) {
                return Err(AtlasError::invalid(format!(
                    "Column {} is listed twice",
                    rule.name
                )));
            }
            if let Some(dtype) = &rule.dtype
                && !TYPES.contains(&dtype.as_str())
            {
                return Err(AtlasError::invalid(format!(
                    "Column {} has the unknown type {dtype}; the types are {}",
                    rule.name,
                    TYPES.join(", ")
                )));
            }
            for bound in [&rule.min, &rule.max].into_iter().flatten() {
                if !(bound.is_number() || bound.is_string()) {
                    return Err(AtlasError::invalid(format!(
                        "The min and max of column {} must be numbers, or dates as text",
                        rule.name
                    )));
                }
            }
            if let (Some(Value::Number(min)), Some(Value::Number(max))) = (&rule.min, &rule.max)
                && min.as_f64() > max.as_f64()
            {
                return Err(AtlasError::invalid(format!(
                    "The min of column {} is greater than its max",
                    rule.name
                )));
            }
            if let (Some(min), Some(max)) = (rule.min_length, rule.max_length)
                && min > max
            {
                return Err(AtlasError::invalid(format!(
                    "The min_length of column {} is greater than its max_length",
                    rule.name
                )));
            }
            if let Some(values) = &rule.values {
                let kinds: std::collections::HashSet<_> = values.iter().map(value_kind).collect();
                if values.is_empty() || kinds.len() > 1 || kinds.contains("other") {
                    return Err(AtlasError::invalid(format!(
                        "The values of column {} must be a list of strings, numbers or booleans \
                         of one kind",
                        rule.name
                    )));
                }
            }
        }
        if let (Some(min), Some(max)) = (self.min_rows, self.max_rows)
            && min > max
        {
            return Err(AtlasError::invalid("min_rows is greater than max_rows"));
        }
        Ok(())
    }
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "float",
        _ => "other",
    }
}

fn type_matches(expected: &str, dtype: &DataType) -> bool {
    match expected {
        "integer" => dtype.is_integer(),
        "float" => dtype.is_float(),
        "number" => dtype.is_primitive_numeric() || matches!(dtype, DataType::Decimal(_, _)),
        "decimal" => matches!(dtype, DataType::Decimal(_, _)),
        "string" => matches!(
            dtype,
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _)
        ),
        "boolean" => matches!(dtype, DataType::Boolean),
        "date" => matches!(dtype, DataType::Date),
        "datetime" => matches!(dtype, DataType::Datetime(_, _)),
        "time" => matches!(dtype, DataType::Time),
        "duration" => matches!(dtype, DataType::Duration(_)),
        "list" => matches!(dtype, DataType::List(_) | DataType::Array(_, _)),
        "struct" => matches!(dtype, DataType::Struct(_)),
        _ => true,
    }
}

/// The contract type of a Polars type, for inferred contracts.
fn type_of(dtype: &DataType) -> &'static str {
    TYPES
        .iter()
        .skip(1)
        .filter(|name| !matches!(**name, "number"))
        .find(|name| type_matches(name, dtype))
        .copied()
        .unwrap_or("any")
}

fn is_text(dtype: &DataType) -> bool {
    type_matches("string", dtype)
}

fn bound(value: &Value, dtype: &DataType, column: &str) -> Result<Expr, AtlasError> {
    let temporal = dtype.is_temporal();
    match value {
        Value::Number(number) if !temporal && dtype.is_primitive_numeric() => {
            Ok(match (number.as_i64(), number.as_f64()) {
                (Some(integer), _) => lit(integer),
                (None, Some(float)) => lit(float),
                _ => {
                    return Err(AtlasError::invalid(format!(
                        "The bound of {column} is too large"
                    )));
                }
            })
        }
        Value::Number(number) if matches!(dtype, DataType::Decimal(_, _)) => {
            Ok(lit(number.as_f64().unwrap_or(f64::NAN)).cast(DataType::Float64))
        }
        Value::String(text) if temporal => Ok(lit(text.clone()).cast(dtype.clone())),
        _ => Err(AtlasError::invalid(format!(
            "Column {column} has a min or max, which needs a numeric column (or a date column \
             with dates as text); it is {dtype}"
        ))),
    }
}

fn column_value(dtype: &DataType, column: &str) -> Expr {
    if matches!(dtype, DataType::Decimal(_, _)) {
        col(column).cast(DataType::Float64)
    } else {
        col(column)
    }
}

fn allowed_values(values: &[Value], dtype: &DataType, column: &str) -> Result<Expr, AtlasError> {
    let name = PlSmallStr::from("allowed");
    let (series, cast) = match value_kind(&values[0]) {
        "string" => {
            let items: Vec<&str> = values.iter().filter_map(Value::as_str).collect();
            (Series::new(name, items), DataType::String)
        }
        "boolean" => {
            let items: Vec<bool> = values.iter().filter_map(Value::as_bool).collect();
            (Series::new(name, items), DataType::Boolean)
        }
        "integer" if dtype.is_integer() => {
            let items: Vec<i64> = values.iter().filter_map(Value::as_i64).collect();
            (Series::new(name, items), DataType::Int64)
        }
        _ => {
            let items: Vec<f64> = values.iter().filter_map(Value::as_f64).collect();
            (Series::new(name, items), DataType::Float64)
        }
    };
    let compatible = match cast {
        DataType::String => is_text(dtype),
        DataType::Boolean => matches!(dtype, DataType::Boolean),
        _ => dtype.is_primitive_numeric() || matches!(dtype, DataType::Decimal(_, _)),
    };
    if !compatible {
        return Err(AtlasError::invalid(format!(
            "The values of column {column} do not match its type {dtype}"
        )));
    }
    Ok(col(column)
        .cast(cast)
        .is_in(lit(series).implode(false), false)
        .not())
}

struct Check {
    column: Option<String>,
    rule: &'static str,
    message: String,
    violates: Expr,
    /// The expression whose distinct values show in the report, if any.
    shown: Option<Expr>,
}

fn present(column: &str, violates: Expr) -> Expr {
    col(column).is_not_null().and(violates).fill_null(false)
}

fn column_checks(
    rule: &ColumnRule,
    dtype: &DataType,
    checks: &mut Vec<Check>,
    violations: &mut Vec<Violation>,
) -> Result<(), AtlasError> {
    let name = rule.name.as_str();
    let schema_violation = |rule_name: &'static str, message: String| Violation {
        column: Some(name.to_string()),
        rule: rule_name,
        rows: 0,
        message,
        examples: vec![],
        values: vec![],
    };
    if let Some(expected) = &rule.dtype
        && !type_matches(expected, dtype)
    {
        violations.push(schema_violation(
            "type",
            format!("{name} is {dtype}, not {expected}"),
        ));
        // Its value rules assume the declared type.
        return Ok(());
    }
    let shown = || Some(col(name).cast(DataType::String));
    if !rule.nullable {
        let missing = if dtype.is_float() {
            col(name).is_null().or(col(name).is_nan().fill_null(false))
        } else {
            col(name).is_null()
        };
        checks.push(Check {
            column: Some(name.to_string()),
            rule: "nullable",
            message: format!("{name} has missing values"),
            violates: missing,
            shown: None,
        });
    }
    for (rule_name, value, comparison) in [("min", &rule.min, true), ("max", &rule.max, false)] {
        let Some(value) = value else { continue };
        let limit = bound(value, dtype, name)?;
        let current = column_value(dtype, name);
        let mut violates = if comparison {
            current.lt(limit)
        } else {
            current.gt(limit)
        };
        if dtype.is_float() {
            // NaN is missing, which `nullable` checks; Polars orders it above every number.
            violates = violates.and(col(name).is_nan().not());
        }
        let text = match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        checks.push(Check {
            column: Some(name.to_string()),
            rule: rule_name,
            message: format!(
                "{name} has values {} {text}",
                if comparison {
                    "below the min"
                } else {
                    "above the max"
                }
            ),
            violates: present(name, violates),
            shown: shown(),
        });
    }
    if let Some(values) = &rule.values {
        checks.push(Check {
            column: Some(name.to_string()),
            rule: "values",
            message: format!("{name} has values outside its allowed values"),
            violates: present(name, allowed_values(values, dtype, name)?),
            shown: shown(),
        });
    }
    let lengths = rule.min_length.is_some() || rule.max_length.is_some();
    if (lengths || rule.pattern.is_some()) && !is_text(dtype) {
        return Err(AtlasError::invalid(format!(
            "Column {name} has a length or pattern rule, which needs a string column; it is \
             {dtype}"
        )));
    }
    let text = || col(name).cast(DataType::String);
    if let Some(min) = rule.min_length {
        checks.push(Check {
            column: Some(name.to_string()),
            rule: "min_length",
            message: format!("{name} has values shorter than {min} characters"),
            violates: present(name, text().str().len_chars().lt(lit(min))),
            shown: shown(),
        });
    }
    if let Some(max) = rule.max_length {
        checks.push(Check {
            column: Some(name.to_string()),
            rule: "max_length",
            message: format!("{name} has values longer than {max} characters"),
            violates: present(name, text().str().len_chars().gt(lit(max))),
            shown: shown(),
        });
    }
    if let Some(pattern) = &rule.pattern {
        checks.push(Check {
            column: Some(name.to_string()),
            rule: "pattern",
            message: format!("{name} has values that do not match {pattern}"),
            violates: present(
                name,
                text()
                    .str()
                    .contains(lit(format!("^(?:{pattern})$")), true)
                    .not(),
            ),
            shown: shown(),
        });
    }
    if rule.unique {
        checks.push(unique_check(&[name.to_string()]));
    }
    Ok(())
}

fn unique_check(columns: &[String]) -> Check {
    let (key, shown) = if columns.len() == 1 {
        let column = col(columns[0].as_str());
        (column.clone(), column.cast(DataType::String))
    } else {
        let fields: Vec<Expr> = columns.iter().map(|c| col(c.as_str())).collect();
        let parts: Vec<Expr> = columns
            .iter()
            .map(|c| {
                concat_str(
                    [
                        lit(format!("{c}=")),
                        col(c.as_str())
                            .cast(DataType::String)
                            .fill_null(lit("null")),
                    ],
                    "",
                    false,
                )
            })
            .collect();
        (as_struct(fields), concat_str(parts, ", ", false))
    };
    Check {
        column: Some(columns.join(", ")),
        rule: "unique",
        message: format!(
            "{} {} repeated",
            columns.join(", "),
            if columns.len() == 1 {
                "has values"
            } else {
                "have combinations"
            }
        ),
        violates: key.is_duplicated(),
        shown: Some(shown),
    }
}

fn first_value(frame: &DataFrame, name: &str) -> Result<u64, AtlasError> {
    frame
        .column(name)?
        .get(0)?
        .extract::<u64>()
        .ok_or_else(|| AtlasError::engine("A count is not a number"))
}

fn shorten(text: &str) -> String {
    if text.chars().count() <= VALUE_CHARS {
        return text.to_string();
    }
    let mut short: String = text.chars().take(VALUE_CHARS).collect();
    short.push('…');
    short
}

/// Checks the file against the contract; `examples` caps the rows and values listed for
/// each violation (the counts are always exact).
pub fn validate(path: &str, contract: &str, examples: usize) -> Result<Report, AtlasError> {
    let contract = Contract::parse(contract)?;
    guarded(path, || {
        validate_unguarded(path, &contract, examples.min(MAX_EXAMPLES))
    })
}

fn validate_unguarded(
    path: &str,
    contract: &Contract,
    examples: usize,
) -> Result<Report, AtlasError> {
    let table = Table::open(path, 0)?;
    let dtypes: std::collections::HashMap<&str, &DataType> = table
        .columns()
        .iter()
        .map(|column| (column.name.as_str(), &column.dtype))
        .collect();
    let mut violations = Vec::new();
    let mut checks = Vec::new();
    for rule in &contract.columns {
        match dtypes.get(rule.name.as_str()) {
            Some(dtype) => column_checks(rule, dtype, &mut checks, &mut violations)?,
            None if rule.required => violations.push(Violation {
                column: Some(rule.name.clone()),
                rule: "required",
                rows: 0,
                message: format!("{} is missing", rule.name),
                examples: vec![],
                values: vec![],
            }),
            None => {}
        }
    }
    if contract.extra_columns == ExtraColumns::Forbid {
        let declared: std::collections::HashSet<&str> = contract
            .columns
            .iter()
            .map(|rule| rule.name.as_str())
            .collect();
        for column in table.columns() {
            if !declared.contains(column.name.as_str()) {
                violations.push(Violation {
                    column: Some(column.name.clone()),
                    rule: "extra_columns",
                    rows: 0,
                    message: format!("{} is not in the contract", column.name),
                    examples: vec![],
                    values: vec![],
                });
            }
        }
    }
    if !contract.unique.is_empty() {
        let missing: Vec<&String> = contract
            .unique
            .iter()
            .filter(|column| !dtypes.contains_key(column.as_str()))
            .collect();
        if missing.is_empty() {
            checks.push(unique_check(&contract.unique));
        } else {
            violations.push(Violation {
                column: Some(contract.unique.join(", ")),
                rule: "unique",
                rows: 0,
                message: format!(
                    "The unique columns {} are missing",
                    missing
                        .iter()
                        .map(|c| c.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                examples: vec![],
                values: vec![],
            });
        }
    }

    // One pass counts the rows and every rule's violations.
    let mut counts = vec![len().cast(DataType::UInt64).alias("__rows")];
    for (index, check) in checks.iter().enumerate() {
        counts.push(
            check
                .violates
                .clone()
                .sum()
                .cast(DataType::UInt64)
                .alias(format!("__check_{index}")),
        );
    }
    let counted = table.collect(table.scan()?.select(counts))?;
    let rows = first_value(&counted, "__rows")?;
    for bound in [
        ("min_rows", contract.min_rows),
        ("max_rows", contract.max_rows),
    ] {
        let (rule, Some(limit)) = bound else { continue };
        let broken = if rule == "min_rows" {
            rows < limit
        } else {
            rows > limit
        };
        if broken {
            violations.push(Violation {
                column: None,
                rule,
                rows: 0,
                message: format!(
                    "The output has {rows} rows; the contract needs {} {limit}",
                    if rule == "min_rows" {
                        "at least"
                    } else {
                        "at most"
                    }
                ),
                examples: vec![],
                values: vec![],
            });
        }
    }
    for (index, check) in checks.into_iter().enumerate() {
        let count = first_value(&counted, &format!("__check_{index}"))?;
        if count == 0 {
            continue;
        }
        let mut found = Vec::new();
        let mut values = Vec::new();
        if examples > 0 {
            let positions = table.collect(
                table
                    .scan()?
                    .with_row_index(ROW, None)
                    .filter(check.violates.clone())
                    .select([col(ROW).cast(DataType::UInt64)])
                    .limit(examples as IdxSize),
            )?;
            found = positions.column(ROW)?.u64()?.into_no_null_iter().collect();
            if let Some(shown) = check.shown.clone() {
                let distinct = table.collect(
                    table
                        .scan()?
                        .filter(check.violates.clone())
                        .select([shown.alias("__value")])
                        .unique_stable(None, UniqueKeepStrategy::First)
                        .limit(examples.min(10) as IdxSize),
                )?;
                values = distinct
                    .column("__value")?
                    .str()?
                    .iter()
                    .map(|value| shorten(value.unwrap_or("null")))
                    .collect();
            }
        }
        violations.push(Violation {
            column: check.column,
            rule: check.rule,
            rows: count,
            message: format!("{} ({count} of {rows} rows)", check.message),
            examples: found,
            values,
        });
    }
    Ok(Report {
        contract: contract.name.clone(),
        version: contract.version.clone(),
        rows,
        passed: violations.is_empty(),
        violations,
    })
}

/// A contract draft for the file: each column with its type and whether it has missing
/// values. Ranges, allowed values and keys are the author's to add.
pub fn infer(path: &str, name: &str) -> Result<Value, AtlasError> {
    guarded(path, || {
        let table = Table::open(path, 0)?;
        let nulls: Vec<Expr> = table
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let name = column.name.as_str();
                let missing = if column.dtype.is_float() {
                    col(name).is_null().or(col(name).is_nan().fill_null(false))
                } else {
                    col(name).is_null()
                };
                missing.any(true).alias(format!("__nulls_{index}"))
            })
            .collect();
        let counted = if nulls.is_empty() {
            DataFrame::empty()
        } else {
            table.collect(table.scan()?.select(nulls))?
        };
        let mut columns = Vec::new();
        for (index, column) in table.columns().iter().enumerate() {
            let has_nulls = counted
                .column(&format!("__nulls_{index}"))
                .ok()
                .and_then(|values| values.get(0).ok())
                .map(|value| matches!(value, AnyValue::Boolean(true)))
                .unwrap_or(true);
            columns.push(json!({
                "name": column.name,
                "type": type_of(&column.dtype),
                "nullable": has_nulls,
            }));
        }
        Ok(json!({
            "name": name,
            "version": "1.0.0",
            "columns": columns,
        }))
    })
}

/// One difference between two versions of a contract. Breaking changes weaken what readers
/// of the data can rely on; the others keep or tighten it.
#[derive(Debug, Serialize, PartialEq)]
pub struct Change {
    pub breaking: bool,
    pub column: Option<String>,
    pub message: String,
}

/// How a new version of a contract relates to the old one, and whether its version number
/// says so: a breaking change needs a new major version, any change a new version.
#[derive(Debug, Serialize)]
pub struct Compatibility {
    pub contract: String,
    pub old_version: Option<String>,
    pub new_version: Option<String>,
    pub changes: Vec<Change>,
    /// Why the version number does not fit the changes; None when it does.
    pub problem: Option<String>,
}

fn major(version: &Option<String>) -> Option<u64> {
    version.as_deref()?.split('.').next()?.trim().parse().ok()
}

fn ordered(old: &Value, new: &Value) -> Option<std::cmp::Ordering> {
    match (old, new) {
        (Value::Number(a), Value::Number(b)) => a.as_f64()?.partial_cmp(&b.as_f64()?),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn shown(value: &Option<Value>) -> String {
    match value {
        None => "none".into(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

/// The differences between two versions of a contract, each JSON as MageML loads it, and
/// whether the new version number fits them.
pub fn compatibility(old: &str, new: &str) -> Result<Compatibility, AtlasError> {
    let old = Contract::parse(old)?;
    let new = Contract::parse(new)?;
    let mut changes = Vec::new();
    let mut change = |breaking: bool, column: Option<&str>, message: String| {
        changes.push(Change {
            breaking,
            column: column.map(str::to_string),
            message,
        });
    };
    let columns: std::collections::HashMap<&str, &ColumnRule> =
        new.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    for before in &old.columns {
        let name = before.name.as_str();
        let Some(after) = columns.get(name) else {
            change(
                before.required,
                Some(name),
                format!(
                    "column {name} was removed{}",
                    if before.required {
                        ""
                    } else {
                        "; it was optional"
                    }
                ),
            );
            continue;
        };
        if before.dtype != after.dtype {
            change(
                true,
                Some(name),
                format!(
                    "the type of {name} changed from {} to {}",
                    before.dtype.as_deref().unwrap_or("any"),
                    after.dtype.as_deref().unwrap_or("any")
                ),
            );
        }
        if before.required != after.required {
            change(
                before.required,
                Some(name),
                format!(
                    "{name} is {} required",
                    if after.required { "now" } else { "no longer" }
                ),
            );
        }
        if before.nullable != after.nullable {
            change(
                after.nullable,
                Some(name),
                format!(
                    "{name} {} missing values",
                    if after.nullable {
                        "now allows"
                    } else {
                        "no longer allows"
                    }
                ),
            );
        }
        if before.unique != after.unique {
            change(
                before.unique,
                Some(name),
                format!(
                    "{name} values are {} unique",
                    if after.unique { "now" } else { "no longer" }
                ),
            );
        }
        for (rule, lower) in [("min", true), ("max", false)] {
            let (was, is) = if lower {
                (&before.min, &after.min)
            } else {
                (&before.max, &after.max)
            };
            if was == is {
                continue;
            }
            // Wider is breaking: a lower min, a higher max, or the bound gone.
            let breaking = match (was, is) {
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(a), Some(b)) => match ordered(a, b) {
                    Some(order) => {
                        if lower {
                            order == std::cmp::Ordering::Greater
                        } else {
                            order == std::cmp::Ordering::Less
                        }
                    }
                    None => true,
                },
                (None, None) => false,
            };
            change(
                breaking,
                Some(name),
                format!(
                    "the {rule} of {name} changed from {} to {}",
                    shown(was),
                    shown(is)
                ),
            );
        }
        if before.values != after.values {
            let breaking = match (&before.values, &after.values) {
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(was), Some(is)) => is.iter().any(|v| !was.contains(v)),
                (None, None) => false,
            };
            let message = match (&before.values, &after.values) {
                (Some(was), Some(is)) => {
                    let added: Vec<String> = is
                        .iter()
                        .filter(|v| !was.contains(v))
                        .map(|v| shown(&Some(v.clone())))
                        .collect();
                    if added.is_empty() {
                        format!("{name} allows fewer values")
                    } else {
                        format!("{name} allows new values: {}", added.join(", "))
                    }
                }
                (Some(_), None) => format!("{name} allows any value"),
                _ => format!("{name} allows only listed values"),
            };
            change(breaking, Some(name), message);
        }
        for (rule, was, is, lower) in [
            ("min_length", before.min_length, after.min_length, true),
            ("max_length", before.max_length, after.max_length, false),
        ] {
            if was == is {
                continue;
            }
            let breaking = match (was, is) {
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(a), Some(b)) => {
                    if lower {
                        b < a
                    } else {
                        b > a
                    }
                }
                (None, None) => false,
            };
            change(
                breaking,
                Some(name),
                format!(
                    "the {rule} of {name} changed from {} to {}",
                    was.map_or("none".into(), |v| v.to_string()),
                    is.map_or("none".into(), |v| v.to_string())
                ),
            );
        }
        if before.pattern != after.pattern {
            change(
                before.pattern.is_some(),
                Some(name),
                format!(
                    "the pattern of {name} changed from {} to {}",
                    before.pattern.as_deref().unwrap_or("none"),
                    after.pattern.as_deref().unwrap_or("none")
                ),
            );
        }
    }
    let previous: std::collections::HashSet<&str> =
        old.columns.iter().map(|c| c.name.as_str()).collect();
    for added in new
        .columns
        .iter()
        .filter(|c| !previous.contains(c.name.as_str()))
    {
        change(
            false,
            Some(&added.name),
            format!(
                "column {} was added{}",
                added.name,
                if added.required { "" } else { ", optional" }
            ),
        );
    }
    if old.unique != new.unique {
        // A key that is a superset of the old one guarantees less.
        let breaking = !old.unique.is_empty()
            && (new.unique.is_empty() || !new.unique.iter().all(|c| old.unique.contains(c)));
        change(
            breaking,
            None,
            format!(
                "the unique key changed from [{}] to [{}]",
                old.unique.join(", "),
                new.unique.join(", ")
            ),
        );
    }
    if old.extra_columns != new.extra_columns {
        change(
            new.extra_columns == ExtraColumns::Allow,
            None,
            format!(
                "extra columns are {}",
                if new.extra_columns == ExtraColumns::Allow {
                    "now allowed"
                } else {
                    "no longer allowed"
                }
            ),
        );
    }
    for (rule, was, is, lower) in [
        ("min_rows", old.min_rows, new.min_rows, true),
        ("max_rows", old.max_rows, new.max_rows, false),
    ] {
        if was == is {
            continue;
        }
        let breaking = match (was, is) {
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(a), Some(b)) => {
                if lower {
                    b < a
                } else {
                    b > a
                }
            }
            (None, None) => false,
        };
        change(
            breaking,
            None,
            format!(
                "{rule} changed from {} to {}",
                was.map_or("none".into(), |v| v.to_string()),
                is.map_or("none".into(), |v| v.to_string())
            ),
        );
    }

    let breaking = changes.iter().any(|c| c.breaking);
    let problem = if changes.is_empty() {
        None
    } else if old.version == new.version {
        Some(format!(
            "the contract changed but its version is still {}; {}",
            new.version.as_deref().unwrap_or("unset"),
            if breaking {
                "a breaking change needs a new major version"
            } else {
                "give it a new version"
            }
        ))
    } else if breaking && major(&old.version) == major(&new.version) {
        Some(format!(
            "a breaking change needs a new major version; {} to {} keeps major {}",
            old.version.as_deref().unwrap_or("unset"),
            new.version.as_deref().unwrap_or("unset"),
            major(&new.version).map_or("unset".into(), |m| m.to_string())
        ))
    } else {
        None
    };
    Ok(Compatibility {
        contract: new.name,
        old_version: old.version,
        new_version: new.version,
        changes,
        problem,
    })
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

    fn scores() -> tempfile::NamedTempFile {
        let mut frame = df!(
            "customer_id" => [1i64, 2, 3, 3, 5],
            "score" => [Some(0.5f64), Some(1.7), None, Some(0.2), Some(f64::NAN)],
            "segment" => ["a", "b", "z", "a", "b"],
            "email" => ["x@y.io", "bad", "q@r.io", "s@t.io", "u@v.io"],
            "day" => ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        )
        .unwrap();
        frame = frame
            .lazy()
            .with_column(col("day").cast(DataType::Date))
            .collect()
            .unwrap();
        write(&mut frame)
    }

    fn rule<'a>(report: &'a Report, column: &str, rule: &str) -> Option<&'a Violation> {
        report
            .violations
            .iter()
            .find(|v| v.column.as_deref() == Some(column) && v.rule == rule)
    }

    #[test]
    fn every_broken_rule_is_counted_with_its_rows_and_values() {
        let file = scores();
        let contract = json!({
            "name": "customer_scores",
            "version": "1.0.0",
            "columns": [
                {"name": "customer_id", "type": "integer", "nullable": false, "unique": true},
                {"name": "score", "type": "float", "nullable": false, "min": 0, "max": 1},
                {"name": "segment", "type": "string", "values": ["a", "b"]},
                {"name": "email", "type": "string", "pattern": "[^@]+@[^@]+", "max_length": 5},
                {"name": "day", "type": "date", "min": "2024-01-02"},
                {"name": "region", "type": "string"},
            ],
        });
        let report = validate(file.path().to_str().unwrap(), &contract.to_string(), 5).unwrap();
        assert!(!report.passed);
        assert_eq!(report.rows, 5);

        let duplicate = rule(&report, "customer_id", "unique").unwrap();
        assert_eq!(
            (duplicate.rows, duplicate.examples.clone()),
            (2, vec![2, 3])
        );
        assert_eq!(duplicate.values, vec!["3"]);
        // NaN counts as missing, as pandas writes a missing float.
        let missing = rule(&report, "score", "nullable").unwrap();
        assert_eq!((missing.rows, missing.examples.clone()), (2, vec![2, 4]));
        let above = rule(&report, "score", "max").unwrap();
        assert_eq!(
            (above.rows, above.values.clone()),
            (1, vec!["1.7".to_string()])
        );
        assert!(rule(&report, "score", "min").is_none());
        let outside = rule(&report, "segment", "values").unwrap();
        assert_eq!((outside.rows, outside.examples.clone()), (1, vec![2]));
        assert_eq!(rule(&report, "email", "pattern").unwrap().examples, vec![1]);
        assert_eq!(rule(&report, "email", "max_length").unwrap().rows, 4);
        assert_eq!(rule(&report, "day", "min").unwrap().examples, vec![0]);
        assert_eq!(rule(&report, "region", "required").unwrap().rows, 0);
    }

    #[test]
    fn an_arrow_ipc_file_is_checked_as_a_parquet_one() {
        // Exported services write block outputs as Arrow IPC.
        let mut frame = df!(
            "customer_id" => [1i64, 2, 2],
            "score" => [Some(0.5f64), None, Some(1.5)],
        )
        .unwrap();
        let file = tempfile::Builder::new()
            .suffix(".arrow")
            .tempfile()
            .unwrap();
        IpcWriter::new(std::fs::File::create(file.path()).unwrap())
            .finish(&mut frame)
            .unwrap();
        let contract = json!({
            "name": "c",
            "columns": [
                {"name": "customer_id", "type": "integer", "unique": true},
                {"name": "score", "type": "float", "nullable": false, "max": 1},
            ],
        });
        let report = validate(file.path().to_str().unwrap(), &contract.to_string(), 5).unwrap();
        assert!(!report.passed);
        assert_eq!(report.rows, 3);
        assert_eq!(rule(&report, "customer_id", "unique").unwrap().rows, 2);
        assert_eq!(rule(&report, "score", "nullable").unwrap().rows, 1);
        assert_eq!(rule(&report, "score", "max").unwrap().rows, 1);
    }

    #[test]
    fn a_matching_output_passes_and_schema_rules_need_no_scan_of_values() {
        let file = scores();
        let contract = json!({
            "name": "c",
            "columns": [
                {"name": "customer_id", "type": "number"},
                {"name": "segment", "type": "integer", "values": [1, 2]},
                {"name": "maybe", "required": false},
            ],
            "extra_columns": "forbid",
            "min_rows": 6,
        });
        let report = validate(file.path().to_str().unwrap(), &contract.to_string(), 5).unwrap();
        let rules: Vec<(&str, &str)> = report
            .violations
            .iter()
            .map(|v| (v.column.as_deref().unwrap_or(""), v.rule))
            .collect();
        assert_eq!(
            rules,
            vec![
                ("segment", "type"),
                ("score", "extra_columns"),
                ("email", "extra_columns"),
                ("day", "extra_columns"),
                ("", "min_rows"),
            ]
        );

        let passing = json!({
            "name": "c",
            "columns": [{"name": "segment", "values": ["a", "b", "z"], "min_length": 1}],
            "unique": ["customer_id", "segment"],
        });
        let report = validate(file.path().to_str().unwrap(), &passing.to_string(), 5).unwrap();
        assert!(report.passed, "{:?}", report.violations);
    }

    #[test]
    fn composite_keys_show_their_columns() {
        let mut frame = df!("a" => [1i64, 1, 2], "b" => ["x", "x", "y"]).unwrap();
        let file = write(&mut frame);
        let contract = json!({"name": "c", "unique": ["a", "b"]});
        let report = validate(file.path().to_str().unwrap(), &contract.to_string(), 5).unwrap();
        let duplicate = rule(&report, "a, b", "unique").unwrap();
        assert_eq!(duplicate.rows, 2);
        assert_eq!(duplicate.values, vec!["a=1, b=x"]);
    }

    #[test]
    fn invalid_contracts_are_explained() {
        for (contract, expected) in [
            (
                json!({"name": "c", "columns": [{"name": "a", "nulable": false}]}),
                "unknown field",
            ),
            (
                json!({"name": "c", "columns": [{"name": "a", "type": "int"}]}),
                "unknown type int",
            ),
            (
                json!({"name": "c", "columns": [{"name": "a", "min": 2, "max": 1}]}),
                "greater",
            ),
            (
                json!({"name": "c", "columns": [{"name": "a"}, {"name": "a"}]}),
                "twice",
            ),
            (
                json!({"name": "c", "columns": [{"name": "a", "values": [1, "x"]}]}),
                "one kind",
            ),
        ] {
            let error = Contract::parse(&contract.to_string()).unwrap_err();
            assert!(error.message().contains(expected), "{error}");
        }
        let file = scores();
        let contract = json!({"name": "c", "columns": [{"name": "segment", "min": 1}]});
        let error = validate(file.path().to_str().unwrap(), &contract.to_string(), 5).unwrap_err();
        assert!(
            error.message().contains("needs a numeric column"),
            "{error}"
        );
    }

    #[test]
    fn a_draft_lists_columns_types_and_missing_values() {
        let file = scores();
        let draft = infer(file.path().to_str().unwrap(), "scores").unwrap();
        let columns = draft["columns"].as_array().unwrap();
        assert_eq!(
            columns[0],
            json!({"name": "customer_id", "type": "integer", "nullable": false})
        );
        assert_eq!(
            columns[1],
            json!({"name": "score", "type": "float", "nullable": true})
        );
        assert_eq!(columns[4]["type"], "date");
    }

    #[test]
    fn breaking_changes_need_a_new_major_version() {
        let old = json!({"name": "scores", "version": "1.2.0", "columns": [
            {"name": "id", "type": "integer", "nullable": false, "unique": true},
            {"name": "score", "type": "float", "min": 0, "max": 1},
            {"name": "segment", "type": "string", "values": ["new", "active"]},
        ], "unique": ["id"]});
        let tightened = json!({"name": "scores", "version": "1.3.0", "columns": [
            {"name": "id", "type": "integer", "nullable": false, "unique": true},
            {"name": "score", "type": "float", "min": 0, "max": 0.9},
            {"name": "segment", "type": "string", "values": ["new"]},
            {"name": "region", "type": "string", "required": false},
        ], "unique": ["id"]});
        let result = compatibility(&old.to_string(), &tightened.to_string()).unwrap();
        assert!(
            result.changes.iter().all(|c| !c.breaking),
            "{:?}",
            result.changes
        );
        assert_eq!(result.problem, None);

        let widened = json!({"name": "scores", "version": "1.3.0", "columns": [
            {"name": "id", "type": "integer", "unique": true},
            {"name": "score", "type": "float", "min": -1, "max": 1},
            {"name": "segment", "type": "string", "values": ["new", "active", "churned"]},
        ]});
        let result = compatibility(&old.to_string(), &widened.to_string()).unwrap();
        let breaking: Vec<&str> = result
            .changes
            .iter()
            .filter(|c| c.breaking)
            .map(|c| c.message.as_str())
            .collect();
        assert!(
            breaking.contains(&"id now allows missing values"),
            "{breaking:?}"
        );
        assert!(
            breaking.contains(&"the min of score changed from 0 to -1"),
            "{breaking:?}"
        );
        assert!(
            breaking.contains(&"segment allows new values: churned"),
            "{breaking:?}"
        );
        assert!(
            breaking.contains(&"the unique key changed from [id] to []"),
            "{breaking:?}"
        );
        assert!(
            result
                .problem
                .unwrap()
                .contains("needs a new major version")
        );

        let bumped = json!({"name": "scores", "version": "2.0.0", "columns": [
            {"name": "id", "type": "string"},
        ]});
        let result = compatibility(&old.to_string(), &bumped.to_string()).unwrap();
        assert!(
            result
                .changes
                .iter()
                .any(|c| c.message == "column score was removed")
        );
        assert!(
            result
                .changes
                .iter()
                .any(|c| c.message == "the type of id changed from integer to string")
        );
        assert_eq!(result.problem, None);

        let same_version = json!({"name": "scores", "version": "1.2.0", "columns": [
            {"name": "id", "type": "integer", "nullable": false, "unique": true},
            {"name": "score", "type": "float", "min": 0, "max": 1},
            {"name": "segment", "type": "string", "values": ["new"]},
        ], "unique": ["id"]});
        let result = compatibility(&old.to_string(), &same_version.to_string()).unwrap();
        assert!(
            result
                .problem
                .unwrap()
                .contains("its version is still 1.2.0")
        );
        let unchanged = compatibility(&old.to_string(), &old.to_string()).unwrap();
        assert!(unchanged.changes.is_empty() && unchanged.problem.is_none());
    }
}
