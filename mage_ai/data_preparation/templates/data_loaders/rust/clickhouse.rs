use mage::clickhouse::ClickHouse;
use mage::prelude::*;

// The connection comes from the block's io_config profile (data_provider_profile in its
// settings, default otherwise): CLICKHOUSE_HOST, CLICKHOUSE_PORT (the HTTP port, 8123),
// CLICKHOUSE_DATABASE, CLICKHOUSE_USERNAME and CLICKHOUSE_PASSWORD.
/// Reads a query's rows from ClickHouse as an Arrow stream, column by column. The query
/// comes from the pipeline variable `query`.
fn load_data(vars: Vars) -> Result<DataFrame> {
    let query: String = vars.get_or("query", "SELECT 1 AS id".to_string())?;
    ClickHouse::connect()?.query(&query)
}

fn test_has_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "The query returned no rows");
    Ok(())
}
