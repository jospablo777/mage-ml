use mage::postgres::{Postgres, WritePolicy};
use mage::prelude::*;

/// Appends this minute's revenue per device to analytics.live_device_revenue.
fn export_data(rows: DataFrame) -> Result<()> {
    let mut postgres = Postgres::connect()?;
    postgres.execute("CREATE SCHEMA IF NOT EXISTS analytics")?;
    let written = postgres.write(&rows, "analytics.live_device_revenue", WritePolicy::Append)?;
    println!("Appended {written} rows to analytics.live_device_revenue");
    Ok(())
}
