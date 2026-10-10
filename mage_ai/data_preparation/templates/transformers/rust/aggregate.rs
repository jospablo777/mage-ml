use mage::prelude::*;

/// Totals per group. Change the grouping column and the aggregations to your data.
fn transform(data: LazyFrame) -> Result<LazyFrame> {
    Ok(data
        .group_by([col("category")])
        .agg([
            len().alias("rows"),
            col("amount").sum().alias("total"),
            col("amount").mean().alias("mean"),
            col("amount").max().alias("max"),
        ])
        .sort(["total"], SortMultipleOptions::default().with_order_descending(true)))
}

fn test_one_row_per_group(output: &DataFrame) -> Result<()> {
    ensure!(
        output.column("category")?.n_unique()? == output.height(),
        "A group appears twice"
    );
    Ok(())
}
