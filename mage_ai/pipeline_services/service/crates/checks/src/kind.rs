//! The kind of a column, from its Polars type: what a profile reads from it.
use polars::prelude::*;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Numeric,
    String,
    Boolean,
    Temporal,
    Nested,
    Other,
}

impl Kind {
    pub fn of(dtype: &DataType) -> Kind {
        match dtype {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Int128
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::UInt128
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal(_, _) => Kind::Numeric,
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) => Kind::String,
            DataType::Boolean => Kind::Boolean,
            DataType::Date | DataType::Datetime(_, _) | DataType::Time | DataType::Duration(_) => {
                Kind::Temporal
            }
            DataType::List(_) | DataType::Array(_, _) | DataType::Struct(_) => Kind::Nested,
            _ => Kind::Other,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Numeric => "numeric",
            Kind::String => "string",
            Kind::Boolean => "boolean",
            Kind::Temporal => "temporal",
            Kind::Nested => "nested",
            Kind::Other => "other",
        }
    }
}
