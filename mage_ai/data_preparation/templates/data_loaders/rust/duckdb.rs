use mage::duckdb::DuckDb;
use mage::prelude::*;

// DuckDB is built into the block's program: the first build compiles it, which takes a
// few minutes. The database comes from the block's io_config profile (data_provider_profile
// in its settings, default otherwise): DUCKDB_DATABASE, a file relative to the project,
// :memory: or md:name for MotherDuck, and DUCKDB_SCHEMA for unqualified tables.
/// Reads a query's rows from DuckDB, with their types. The query comes from the pipeline
/// variable `query`; DuckDB reads files too, as in SELECT * FROM 'data/*.parquet'.
fn load_data(vars: Vars) -> Result<DataFrame> {
    let query: String = vars.get_or("query", "SELECT 1 AS id".to_string())?;
    DuckDb::connect()?.load(&query)
}

fn test_has_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "The query returned no rows");
    Ok(())
}
