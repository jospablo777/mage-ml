use mage::prelude::*;

/// Joins the outputs of two upstream blocks, in the order of the upstream blocks.
fn transform(orders: LazyFrame, customers: LazyFrame) -> Result<LazyFrame> {
    Ok(orders.join(
        customers,
        [col("customer_id")],
        [col("customer_id")],
        JoinArgs::new(JoinType::Left),
    ))
}
