use mage::postgres::Postgres;
use mage::prelude::*;

/// Orders per customer and the last order, in Rust with binary COPY: a root of a Merge.
fn load_data() -> Result<DataFrame> {
    Postgres::connect()?.query(
        "SELECT customer_id, count(*)::bigint AS orders, max(ordered_at) AS last_order
         FROM shop.orders
         WHERE status <> 'cancelled'
         GROUP BY customer_id",
    )
}
