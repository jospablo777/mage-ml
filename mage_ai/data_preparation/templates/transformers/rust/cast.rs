use mage::prelude::*;

/// Casts columns to the types named in the pipeline variable `types`, a map such as
/// {"amount": "float64", "count": "int64", "day": "date", "label": "string"}. A value that
/// does not convert fails the block and names the column, instead of becoming null.
fn transform(rows: LazyFrame, vars: Vars) -> Result<LazyFrame> {
    let types: std::collections::BTreeMap<String, String> = vars.get_or("types", Default::default())?;
    let mut casts = Vec::new();
    for (name, kind) in &types {
        let dtype = match kind.as_str() {
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "float32" => DataType::Float32,
            "float64" => DataType::Float64,
            "bool" | "boolean" => DataType::Boolean,
            "string" | "text" => DataType::String,
            "date" => DataType::Date,
            "datetime" => DataType::Datetime(TimeUnit::Microseconds, None),
            other => bail!("Column {name}: {other} is not a type this block casts to"),
        };
        casts.push(col(name.as_str()).strict_cast(dtype));
    }
    Ok(rows.with_columns(casts))
}
