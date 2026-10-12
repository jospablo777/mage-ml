use mage::prelude::*;

/// Where the Diamond's branches meet: each store's revenue (Polars) and margin (R),
/// joined in Rust, with the margin rate, best margin first.
fn transform(revenue: LazyFrame, margin: LazyFrame) -> Result<LazyFrame> {
    Ok(revenue
        .join(
            margin,
            [col("store_id")],
            [col("store_id")],
            JoinArgs::new(JoinType::Inner),
        )
        .with_column((col("margin") / col("revenue")).alias("margin_rate"))
        .sort(
            ["margin"],
            SortMultipleOptions::default().with_order_descending(true),
        ))
}

fn test_every_store_has_a_rate(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "No stores");
    ensure!(
        output.column("margin_rate")?.null_count() == 0,
        "A store has no margin rate"
    );
    Ok(())
}
