use mage::prelude::*;

/// Where the Merge's roots meet: each customer with their orders and web activity,
/// joined in Rust. Customers without orders or sessions get zeros.
fn transform(
    customers: LazyFrame,
    orders: LazyFrame,
    web: LazyFrame,
) -> Result<LazyFrame> {
    let left = JoinArgs::new(JoinType::Left);
    Ok(customers
        .join(orders, [col("customer_id")], [col("customer_id")], left.clone())
        .join(web, [col("customer_id")], [col("customer_id")], left)
        .with_columns([
            col("orders").fill_null(lit(0i64)),
            col("sessions").fill_null(lit(0i64)),
            col("events").fill_null(lit(0i64)),
        ])
        .with_column(
            (col("orders").cast(DataType::Float64) * lit(2.0)
                + col("sessions").cast(DataType::Float64))
            .alias("engagement"),
        )
        .sort(
            ["engagement"],
            SortMultipleOptions::default().with_order_descending(true),
        ))
}

fn test_one_row_per_customer(output: &DataFrame) -> Result<()> {
    let ids = output.column("customer_id")?;
    ensure!(
        ids.n_unique()? == output.height(),
        "A customer appears more than once"
    );
    Ok(())
}
