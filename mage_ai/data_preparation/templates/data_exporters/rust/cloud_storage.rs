use mage::prelude::*;
use mage::storage::Storage;

// Credentials come from the block's io_config profile (data_provider_profile in its
// settings, default otherwise) with the keys MageML's Python clients use: AWS_* for S3 and
// S3-compatible stores (AWS_ENDPOINT for MinIO or IBM Cloud Object Storage),
// GOOGLE_SERVICE_ACC_KEY_FILEPATH for Google Cloud Storage, AZURE_* for Azure; otherwise
// from the environment and the provider's own files.
/// Writes the rows as a Parquet file (zstd) or, for a path that ends in .csv, a CSV file
/// to S3, Google Cloud Storage, Azure or a local path: the pipeline variable `output_path`,
/// such as s3://bucket/scores/latest.parquet.
fn export_data(rows: DataFrame, vars: Vars) -> Result<()> {
    let path: String = vars.get("output_path")?;
    let storage = Storage::connect()?;
    if path.ends_with(".csv") {
        storage.write_csv(&rows, &path)
    } else {
        storage.write_parquet(&rows, &path)
    }
}
