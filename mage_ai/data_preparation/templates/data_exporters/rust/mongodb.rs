use mage::mongodb::{MongoDb, WritePolicy};
use mage::prelude::*;

// The connection comes from the block's io_config profile (data_provider_profile in its
// settings, default otherwise): MONGODB_CONNECTION_STRING, or MONGODB_HOST, MONGODB_PORT,
// MONGODB_USER and MONGODB_PASSWORD, with MONGODB_DATABASE.
/// Writes the rows as documents to the collection named by the pipeline variable
/// `collection`. The variable `write_policy` is replace (the default: a staging collection
/// renamed over the target, so readers see the old documents or the new ones) or append.
fn export_data(rows: DataFrame, vars: Vars) -> Result<()> {
    let collection: String = vars.get("collection")?;
    let policy = match vars.get_or("write_policy", "replace".to_string())?.as_str() {
        "append" => WritePolicy::Append,
        _ => WritePolicy::Replace,
    };
    let written = MongoDb::connect()?.write(&rows, &collection, policy)?;
    println!("Wrote {written} documents to {collection}.");
    Ok(())
}
