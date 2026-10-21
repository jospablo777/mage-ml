use mage::mongodb::{MongoDb, doc};
use mage::prelude::*;

// The connection comes from the block's io_config profile (data_provider_profile in its
// settings, default otherwise): MONGODB_CONNECTION_STRING, or MONGODB_HOST, MONGODB_PORT,
// MONGODB_USER and MONGODB_PASSWORD, with MONGODB_DATABASE.
/// Reads the documents of the collection named by the pipeline variable `collection`, one
/// column per field: a missing field is null, embedded documents are structs. Change the
/// filter to read some of them.
fn load_data(vars: Vars) -> Result<DataFrame> {
    let collection: String = vars.get("collection")?;
    MongoDb::connect()?.load(&collection, doc! {})
}

fn test_has_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "The collection has no documents");
    Ok(())
}
