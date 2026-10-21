use mage::prelude::*;

/// Turns columns into rows (unpivot): the pipeline variable `id_columns` (default
/// ["id"]) names the columns that stay, and every other column becomes a pair per row:
/// its name in `column_name` and its value in `column_value` (the variables
/// `variable_name` and `value_name` rename them). Values of different types become text.
fn transform(rows: LazyFrame, vars: Vars) -> Result<LazyFrame> {
    let id_columns: Vec<String> = vars.get_or("id_columns", vec!["id".to_string()])?;
    let variable_name: String = vars.get_or("variable_name", "column_name".to_string())?;
    let value_name: String = vars.get_or("value_name", "column_value".to_string())?;
    Ok(rows.unpivot(UnpivotArgsDSL {
        // None: every column that is not an id column.
        on: None,
        index: by_name(id_columns.iter().map(String::as_str), true, false),
        variable_name: Some(variable_name.into()),
        value_name: Some(value_name.into()),
    }))
}
