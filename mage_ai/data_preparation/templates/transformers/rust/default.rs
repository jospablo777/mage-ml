use mage::prelude::*;

/// Takes the upstream block's output as a LazyFrame and returns the transformed table.
/// Polars optimizes the whole query and streams it to the output.
fn transform(data: LazyFrame) -> Result<LazyFrame> {
    Ok(data
        .filter(col("value").is_not_null())
        .with_columns([(col("value") * lit(2.0)).alias("value_doubled")]))
}

fn test_output(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "The block returned no rows");
    Ok(())
}
