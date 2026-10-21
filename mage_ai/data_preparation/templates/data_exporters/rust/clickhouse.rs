use mage::clickhouse::{ClickHouse, WritePolicy};
use mage::prelude::*;

// The connection comes from the block's io_config profile (data_provider_profile in its
// settings, default otherwise): CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE,
// CLICKHOUSE_USERNAME and CLICKHOUSE_PASSWORD.
/// Inserts the rows into the ClickHouse table named by the pipeline variable `table`, as
/// one Arrow insert. The variable `write_policy` is replace (the default), append or fail.
fn export_data(rows: DataFrame, vars: Vars) -> Result<()> {
    let table: String = vars.get_or("table", "mage_output".to_string())?;
    let policy = match vars.get_or("write_policy", "replace".to_string())?.as_str() {
        "append" => WritePolicy::Append,
        "fail" => WritePolicy::Fail,
        _ => WritePolicy::Replace,
    };
    let written = ClickHouse::connect()?.write(&rows, &table, policy)?;
    println!("Wrote {written} rows to {table}.");
    Ok(())
}
