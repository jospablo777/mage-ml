use mage::postgres::Postgres;
use mage::prelude::*;

/// The purchases of the last 15 minutes, read from PostgreSQL with binary COPY, with the
/// minute of this snapshot.
fn load_data() -> Result<DataFrame> {
    Postgres::connect()?.query(
        "SELECT date_trunc('minute', now()) AS snapshot_at, device, amount
         FROM analytics.live_events
         WHERE event_type = 'purchase' AND occurred_at >= now() - interval '15 minutes'",
    )
}
