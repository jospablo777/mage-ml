use mage::prelude::*;
use mage::storage::Storage;

// Credentials come from the block's io_config profile (data_provider_profile in its
// settings, default otherwise) with the keys MageML's Python clients use: AWS_* for S3 and
// S3-compatible stores (AWS_ENDPOINT for MinIO or IBM Cloud Object Storage),
// GOOGLE_SERVICE_ACC_KEY_FILEPATH for Google Cloud Storage, AZURE_* for Azure; otherwise
// from the environment and the provider's own files.
/// Reads Parquet or CSV files lazily from S3, Google Cloud Storage, Azure or a local path.
/// The pipeline variable `path` is a URL (s3://bucket/events/*.parquet, gs://, az://) or a
/// path, globs allowed. A filter or column choice after this block reaches the files: a
/// Parquet scan downloads only the row groups and columns it needs.
fn load_data(vars: Vars) -> Result<LazyFrame> {
    let path: String = vars.get("path")?;
    let storage = Storage::connect()?;
    if path.ends_with(".csv") {
        storage.scan_csv(&path)
    } else {
        storage.scan_parquet(&path)
    }
}

fn test_has_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "The files have no rows");
    Ok(())
}
