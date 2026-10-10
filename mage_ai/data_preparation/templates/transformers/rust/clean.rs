use mage::prelude::*;

/// Removes duplicate rows and rows without an id, trims text and fills missing numbers.
/// Change the column names to your data.
fn transform(data: LazyFrame) -> Result<LazyFrame> {
    Ok(data
        .unique_stable(None, UniqueKeepStrategy::First)
        .filter(col("id").is_not_null())
        .with_columns([
            col("name").str().strip_chars(lit(NULL)),
            col("value").fill_null(lit(0.0)),
        ]))
}
