#* @data_loader
load_orders <- function(...) {
  # One row per order that was not cancelled, with the customer's sign-up date.
  read_sql(
    "SELECT o.order_id, o.customer_id, o.ordered_at, o.channel,
            c.signed_up_at, c.segment
     FROM shop.orders o
     JOIN shop.customers c USING (customer_id)
     WHERE o.status <> 'cancelled'",
    database = "postgres",
    profile = "default"
  )
}

#* @test
orders_were_read <- function(output) {
  stopifnot("No orders were read" = nrow(output) > 0)
}
