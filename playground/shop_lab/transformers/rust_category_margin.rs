use mage::prelude::*;

/// Revenue, cost and margin per day and category, without cancelled orders. Gross
/// revenue and cost stay exact decimals; the discount is a percentage, so net revenue
/// is a float.
fn transform(lines: LazyFrame) -> Result<LazyFrame> {
    let quantity = col("quantity").cast(DataType::Decimal(38, 0));
    let gross = (col("unit_price") * quantity.clone()).alias("gross");
    let cost = (col("cost") * quantity).alias("line_cost");
    let net = (col("gross").cast(DataType::Float64)
        * (lit(1.0) - col("discount_pct").cast(DataType::Float64) / lit(100.0)))
    .alias("net");
    Ok(lines
        .filter(col("status").neq(lit("cancelled")))
        .with_columns([col("ordered_at").dt().date().alias("day"), gross, cost])
        .with_column(net)
        .group_by([col("day"), col("category")])
        .agg([
            col("order_id").n_unique().cast(DataType::Int64).alias("orders"),
            col("quantity").cast(DataType::Int64).sum().alias("units"),
            col("gross").sum().alias("gross_revenue"),
            col("net").sum().alias("net_revenue"),
            col("line_cost").sum().alias("cost"),
        ])
        .with_column(
            ((col("net_revenue") - col("cost").cast(DataType::Float64)) / col("net_revenue"))
                .alias("margin"),
        )
        .sort(["day", "category"], SortMultipleOptions::default()))
}

fn test_margins(output: &DataFrame) -> Result<()> {
    let margin = output.column("margin")?.f64()?;
    let (low, high) = (margin.min().unwrap_or(0.0), margin.max().unwrap_or(0.0));
    ensure!(
        low > -1.0 && high < 1.0,
        "Margins run from {low} to {high}, outside -100%..100%"
    );
    Ok(())
}
