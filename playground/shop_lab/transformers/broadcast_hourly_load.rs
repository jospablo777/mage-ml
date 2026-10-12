use mage::prelude::*;

/// Events per hour of the day, in Rust: when the site is busiest.
fn transform(events: LazyFrame) -> Result<LazyFrame> {
    Ok(events
        .with_column(col("occurred_at").dt().hour().cast(DataType::Int32).alias("hour"))
        .group_by([col("hour")])
        .agg([len().cast(DataType::Int64).alias("events")])
        .sort(["hour"], SortMultipleOptions::default()))
}

fn test_hours_are_hours(output: &DataFrame) -> Result<()> {
    let hours = output.column("hour")?.i32()?;
    ensure!(
        hours.min().unwrap_or(0) >= 0 && hours.max().unwrap_or(0) <= 23,
        "An hour outside 0..23"
    );
    Ok(())
}
