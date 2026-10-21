use mage::mysql::{MySql, WritePolicy};
use mage::prelude::*;

// The connection comes from the block's io_config profile (data_provider_profile in its
// settings, default otherwise): MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER,
// MYSQL_PASSWORD and MYSQL_SSL_MODE.
/// Writes the rows to the MySQL table named by the pipeline variable `table`, in batches
/// of prepared INSERTs in one transaction. The variable `write_policy` is replace (the
/// default), append or fail.
fn export_data(rows: DataFrame, vars: Vars) -> Result<()> {
    let table: String = vars.get_or("table", "mage_output".to_string())?;
    let policy = match vars.get_or("write_policy", "replace".to_string())?.as_str() {
        "append" => WritePolicy::Append,
        "fail" => WritePolicy::Fail,
        _ => WritePolicy::Replace,
    };
    let written = MySql::connect()?.write(&rows, &table, policy)?;
    println!("Wrote {written} rows to {table}.");
    Ok(())
}
