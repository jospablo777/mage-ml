use mage::postgres::{Postgres, WritePolicy};
use mage::prelude::*;

/// Replaces analytics.rust_category_margin with binary COPY, in one transaction.
fn export_data(rows: DataFrame) -> Result<()> {
    let mut postgres = Postgres::connect()?;
    postgres.execute("CREATE SCHEMA IF NOT EXISTS analytics")?;
    let written = postgres.write(&rows, "analytics.rust_category_margin", WritePolicy::Replace)?;
    println!("Wrote {written} rows to analytics.rust_category_margin");
    Ok(())
}
