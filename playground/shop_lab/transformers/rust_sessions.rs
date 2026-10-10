use mage::prelude::*;

/// One row per web session: its events, distinct pages, time on page, deepest scroll and
/// whether it ended in a purchase. Polars runs the plan on every core.
fn transform(events: LazyFrame) -> Result<LazyFrame> {
    Ok(events
        .group_by([col("session_id")])
        .agg([
            col("customer_id").first().alias("customer_id"),
            col("device").first().alias("device"),
            len().cast(DataType::Int64).alias("events"),
            col("page").n_unique().cast(DataType::Int64).alias("pages"),
            col("duration_ms").cast(DataType::Int64).sum().alias("duration_ms"),
            col("scroll_depth").cast(DataType::Float64).max().alias("max_scroll_depth"),
            col("occurred_at").min().alias("started_at"),
            col("event_type")
                .eq(lit("purchase"))
                .cast(DataType::Int64)
                .sum()
                .gt(lit(0))
                .alias("purchased"),
        ])
        .sort(["started_at"], SortMultipleOptions::default()))
}

fn test_one_row_per_session(output: &DataFrame) -> Result<()> {
    ensure!(
        output.column("session_id")?.n_unique()? == output.height(),
        "A session appears twice"
    );
    Ok(())
}
