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

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub id: usize,
    pub name: String,
    pub dtype: DataType,
    pub kind: Kind,
    /// A WKB geometry column of a GeoParquet file, with its geometry type when the file
    /// names one (`Point`), shown as WKT.
    pub geometry: Option<Option<String>>,
}

#[derive(Debug, Serialize)]
pub struct ColumnJson<'a> {
    pub id: usize,
    pub name: &'a str,
    pub dtype: String,
    pub kind: Kind,
    /// Integer and decimal columns hold exact values; the browser keeps them as text.
    pub exact_integer: bool,
}

impl ColumnInfo {
    pub fn to_json(&self) -> ColumnJson<'_> {
        ColumnJson {
            id: self.id,
            name: &self.name,
            dtype: match &self.geometry {
                Some(Some(kind)) => format!("Geometry({kind})"),
                Some(None) => "Geometry".to_string(),
                None => display_dtype(&self.dtype),
            },
            kind: self.kind,
            exact_integer: self.geometry.is_none() && self.dtype.is_integer(),
        }
    }
}

/// Short type names for headers: Datetime(us, UTC) instead of datetime[μs, UTC].
pub fn display_dtype(dtype: &DataType) -> String {
    match dtype {
        DataType::Datetime(unit, zone) => match zone {
            Some(zone) => format!("Datetime({unit}, {zone})"),
            None => format!("Datetime({unit})"),
        },
        DataType::Duration(unit) => format!("Duration({unit})"),
        DataType::Decimal(precision, scale) => format!("Decimal({precision}, {scale})"),
        DataType::Categorical(_, _) => "Categorical".to_string(),
        DataType::Enum(_, _) => "Enum".to_string(),
        DataType::List(inner) => format!("List({})", display_dtype(inner)),
        DataType::Array(inner, width) => format!("Array({}, {width})", display_dtype(inner)),
        DataType::Struct(fields) => format!("Struct({})", fields.len()),
        other => format!("{other:?}"),
    }
}

/// The columns of a file. `geometry` maps the names of its WKB geometry columns to
/// their geometry type, from the file's GeoParquet metadata.
pub fn columns_of(
    schema: &Schema,
    geometry: &std::collections::HashMap<String, Option<String>>,
) -> Vec<ColumnInfo> {
    schema
        .iter()
        .enumerate()
        .map(|(id, (name, dtype))| {
            let geometry = match dtype {
                DataType::Binary | DataType::BinaryOffset => geometry.get(name.as_str()).cloned(),
                _ => None,
            };
            ColumnInfo {
                id,
                name: name.to_string(),
                dtype: dtype.clone(),
                kind: Kind::of(dtype),
                geometry,
            }
        })
        .collect()
}

/// The WKB geometry columns that GeoParquet metadata (the `geo` key) lists, with the
/// geometry type when there is exactly one.
pub fn geometry_columns(geo: &str) -> std::collections::HashMap<String, Option<String>> {
    let mut found = std::collections::HashMap::new();
    let Ok(value) = serde_json::from_str::<serde_json::Value>(geo) else {
        return found;
    };
    let Some(columns) = value.get("columns").and_then(|columns| columns.as_object()) else {
        return found;
    };
    for (name, column) in columns {
        let encoding = column
            .get("encoding")
            .and_then(|encoding| encoding.as_str());
        if !encoding.is_some_and(|encoding| encoding.eq_ignore_ascii_case("wkb")) {
            continue;
        }
        let types: Vec<&str> = column
            .get("geometry_types")
            .and_then(|types| types.as_array())
            .map(|types| types.iter().filter_map(|kind| kind.as_str()).collect())
            .unwrap_or_default();
        let kind = match types.as_slice() {
            [single] => Some(single.to_string()),
            _ => None,
        };
        found.insert(name.clone(), kind);
    }
    found
}
