use mage::duckdb::{DuckDb, WritePolicy};
use mage::prelude::*;

// DuckDB is built into the block's program: the first build compiles it, which takes a
// few minutes. The database comes from the block's io_config profile (data_provider_profile
// in its settings, default otherwise): DUCKDB_DATABASE and DUCKDB_SCHEMA.
/// Writes the rows to the DuckDB table named by the pipeline variable `table`. The
/// variable `write_policy` is replace (the default: readers see the old rows or the new
/// ones), append or fail.
fn export_data(rows: DataFrame, vars: Vars) -> Result<()> {
    let table: String = vars.get_or("table", "mage_output".to_string())?;
    let policy = match vars.get_or("write_policy", "replace".to_string())?.as_str() {
        "append" => WritePolicy::Append,
        "fail" => WritePolicy::Fail,
        _ => WritePolicy::Replace,
    };
    let written = DuckDb::connect()?.write(&rows, &table, policy)?;
    println!("Wrote {written} rows to {table}.");
    Ok(())
}
