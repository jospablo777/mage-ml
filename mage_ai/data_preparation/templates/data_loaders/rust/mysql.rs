use mage::mysql::MySql;
use mage::prelude::*;

// The connection comes from the block's io_config profile (data_provider_profile in its
// settings, default otherwise): MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER,
// MYSQL_PASSWORD and MYSQL_SSL_MODE, as for MageML's Python client.
/// Reads a query's rows from MySQL or MariaDB into typed columns: integers keep their width,
/// DECIMAL stays exact, TIMESTAMP is UTC. The query comes from the pipeline variable `query`.
fn load_data(vars: Vars) -> Result<DataFrame> {
    let query: String = vars.get_or("query", "SELECT 1 AS id".to_string())?;
    MySql::connect()?.query(&query)
}

fn test_has_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "The query returned no rows");
    Ok(())
}
