use mage::prelude::*;

/// Revenue and purchases per device over the last 15 minutes.
fn transform(purchases: LazyFrame) -> Result<LazyFrame> {
    Ok(purchases
        .group_by([col("snapshot_at"), col("device")])
        .agg([
            col("amount").sum().alias("revenue"),
            col("amount").count().alias("purchases"),
        ]))
}
