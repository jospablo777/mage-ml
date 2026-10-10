use mage::postgres::{Postgres, WritePolicy};
use mage::prelude::*;

// Needs mage-block = { path = ".mage/sdk", features = ["postgres"] } under
// [workspace.dependencies] in the project's rust/Cargo.toml; projects created with this
// version of Mage have it. The connection comes from the block's io_config profile
// (data_provider_profile in its settings, default otherwise).
/// Writes the upstream table to PostgreSQL with binary COPY, in one transaction. The table
/// comes from the pipeline variable `table` (schema.table, or the profile's schema);
/// Replace drops and creates it, Append inserts by column name, Fail stops if it exists.
fn export_data(rows: DataFrame, vars: Vars) -> Result<()> {
    let table: String = vars.get_or("table", "mage_output".to_string())?;
    let written = Postgres::connect()?.write(&rows, &table, WritePolicy::Replace)?;
    println!("Wrote {written} rows to {table}");
    Ok(())
}
