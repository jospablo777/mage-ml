use mage::postgres::Postgres;
use mage::prelude::*;

// Needs mage-block = { path = ".mage/sdk", features = ["postgres"] } under
// [workspace.dependencies] in the project's rust/Cargo.toml; projects created with this
// version of Mage have it. The connection comes from the block's io_config profile
// (data_provider_profile in its settings, default otherwise).
/// Reads a query's rows from PostgreSQL with binary COPY. The query comes from the
/// pipeline variable `query`.
fn load_data(vars: Vars) -> Result<DataFrame> {
    let query: String = vars.get_or("query", "SELECT 1 AS id".to_string())?;
    Postgres::connect()?.query(&query)
}

fn test_has_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "The query returned no rows");
    Ok(())
}
