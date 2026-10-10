use mage::prelude::*;

/// Recency, frequency and monetary value per customer, from delivered orders, with a
/// tier from the three.
fn transform(lines: LazyFrame) -> Result<LazyFrame> {
    let quantity = col("quantity").cast(DataType::Decimal(38, 0));
    let customers = lines
        .filter(col("status").eq(lit("delivered")))
        .with_column((col("unit_price") * quantity).alias("gross"))
        .group_by([col("customer_id")])
        .agg([
            col("order_id").n_unique().cast(DataType::Int64).alias("orders"),
            col("gross").sum().alias("spent"),
            col("ordered_at").max().alias("last_order_at"),
        ]);
    let recency = (col("last_order_at").max() - col("last_order_at"))
        .dt()
        .total_days(false)
        .alias("recency_days");
    Ok(customers
        .with_column(recency)
        .with_column(
            when(
                col("orders")
                    .gt_eq(lit(8))
                    .and(col("recency_days").lt_eq(lit(60))),
            )
            .then(lit("champion"))
            .when(col("orders").gt_eq(lit(4)))
            .then(lit("loyal"))
            .when(col("recency_days").gt(lit(365)))
            .then(lit("lapsed"))
            .otherwise(lit("occasional"))
            .alias("tier"),
        )
        .sort(["customer_id"], SortMultipleOptions::default()))
}

fn test_one_row_per_customer(output: &DataFrame) -> Result<()> {
    ensure!(
        output.column("customer_id")?.n_unique()? == output.height(),
        "A customer appears twice"
    );
    Ok(())
}
