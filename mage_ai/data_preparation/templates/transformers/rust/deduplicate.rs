use mage::prelude::*;

/// Keeps the latest row of each key: the pipeline variable `key` names the key column
/// (default id) and `order_by` the column whose largest value is the latest (default
/// updated_at). Rows whose key is missing are dropped.
fn transform(rows: LazyFrame, vars: Vars) -> Result<LazyFrame> {
    let key: String = vars.get_or("key", "id".to_string())?;
    let order_by: String = vars.get_or("order_by", "updated_at".to_string())?;
    Ok(rows
        .filter(col(&key).is_not_null())
        .sort(
            [order_by.as_str()],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true),
        )
        .unique_stable(Some(cols([key.as_str()])), UniqueKeepStrategy::First))
}

fn test_ids_are_unique(output: &DataFrame) -> Result<()> {
    // With another key column, check that one instead.
    if let Ok(column) = output.column("id") {
        ensure!(column.n_unique()? == output.height(), "Column id has repeated values");
    }
    Ok(())
}
