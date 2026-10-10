use mage::postgres::Postgres;
use mage::prelude::*;

/// Every order line with its order and product, read from PostgreSQL with binary COPY.
/// numeric(10, 2) prices and costs arrive exact, as Decimal(38, 2).
fn load_data() -> Result<DataFrame> {
    Postgres::connect()?.query(
        "SELECT o.order_id, o.customer_id, o.ordered_at, o.status, o.channel,
                o.discount_pct, p.category, i.quantity, i.unit_price, p.cost
         FROM shop.order_items i
         JOIN shop.orders o USING (order_id)
         JOIN shop.products p USING (product_id)",
    )
}

fn test_prices_are_exact(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "No order lines");
    ensure!(
        output.column("unit_price")?.dtype() == &DataType::Decimal(38, 2),
        "unit_price is {}, not Decimal(38, 2)",
        output.column("unit_price")?.dtype()
    );
    Ok(())
}
