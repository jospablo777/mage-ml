//! Files in object storage for Rust blocks (feature `storage`): Amazon S3 and S3-compatible
//! stores (MinIO, IBM Cloud Object Storage, Cloudflare R2), Google Cloud Storage and Azure
//! Blob Storage, read and written by Polars.
//!
//! Reads are lazy scans: a filter or a column selection on the result reaches the file, so
//! a Parquet scan downloads only the row groups and columns it needs. Paths are URLs
//! (`s3://bucket/key`, `gs://`, `gcs://`, `az://`, `abfss://`) or local paths, and may hold
//! globs (`s3://bucket/events/*.parquet`).
//!
//! Credentials come from the block's io_config profile, with the keys MageML's Python
//! clients use, and otherwise from the environment and the provider's own files
//! (`~/.aws/credentials`, instance roles, workload identity):
//!
//! - S3: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION and
//!   AWS_ENDPOINT (for S3-compatible stores; `http://` endpoints are allowed).
//! - Google Cloud Storage: GOOGLE_SERVICE_ACC_KEY_FILEPATH, or GOOGLE_SERVICE_ACC_KEY
//!   (the key itself).
//! - Azure: AZURE_STORAGE_ACCOUNT_NAME, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and
//!   AZURE_TENANT_ID.
//!
//! ```ignore
//! use mage::prelude::*;
//! use mage::storage::Storage;
//!
//! fn load_data(vars: Vars) -> Result<LazyFrame> {
//!     let day: String = vars.get("day")?;
//!     Ok(Storage::connect()?
//!         .scan_parquet("s3://events/*.parquet")?
//!         .filter(col("day").eq(lit(day))))
//! }
//!
//! fn export_data(scores: DataFrame) -> Result<()> {
//!     Storage::connect()?.write_parquet(&scores, "s3://scores/latest.parquet")
//! }
//! ```
use std::collections::HashMap;
use std::sync::Arc;

use polars::io::cloud::{AmazonS3ConfigKey, AzureConfigKey, CloudOptions, GoogleConfigKey};

use crate::prelude::*;

/// object_store's key for plain HTTP endpoints, which Polars does not re-export.
fn allow_http() -> AmazonS3ConfigKey {
    "aws_allow_http"
        .parse()
        .expect("object_store knows aws_allow_http")
}

/// A client for the object stores of one io_config profile.
#[derive(Clone, Debug, Default)]
pub struct Storage {
    settings: HashMap<String, String>,
}

/// Which provider a path is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    S3,
    Gcs,
    Azure,
    Local,
}

fn provider(path: &str) -> Result<Provider> {
    let Some((scheme, _)) = path.split_once("://") else {
        return Ok(Provider::Local);
    };
    Ok(match scheme.to_ascii_lowercase().as_str() {
        "s3" | "s3a" => Provider::S3,
        "gs" | "gcs" => Provider::Gcs,
        "az" | "azure" | "abfs" | "abfss" | "adl" => Provider::Azure,
        "file" => Provider::Local,
        other => {
            bail!("{other}:// paths are not supported; use s3://, gs://, az:// or a local path")
        }
    })
}

impl Storage {
    /// The client of the block's io_config profile; without one, the environment's
    /// credentials and the provider's own files, as for a local path.
    pub fn connect() -> Result<Self> {
        Ok(Self::with_settings(crate::profile::settings_or_empty()?))
    }

    /// A client with these settings (the io_config keys above); empty settings use the
    /// environment's credentials.
    pub fn with_settings(settings: HashMap<String, String>) -> Self {
        Self { settings }
    }

    fn setting(&self, key: &str) -> Option<&str> {
        self.settings
            .get(key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    }

    /// The Polars cloud options for a path; None for local paths.
    pub fn cloud_options(&self, path: &str) -> Result<Option<CloudOptions>> {
        Ok(match provider(path)? {
            Provider::Local => None,
            Provider::S3 => Some(CloudOptions::default().with_aws(self.s3_config())),
            Provider::Gcs => Some(CloudOptions::default().with_gcp(self.gcs_config())),
            Provider::Azure => Some(CloudOptions::default().with_azure(self.azure_config())),
        })
    }

    fn s3_config(&self) -> Vec<(AmazonS3ConfigKey, String)> {
        let mut config = Vec::new();
        for (key, name) in [
            (AmazonS3ConfigKey::AccessKeyId, "AWS_ACCESS_KEY_ID"),
            (AmazonS3ConfigKey::SecretAccessKey, "AWS_SECRET_ACCESS_KEY"),
            (AmazonS3ConfigKey::Token, "AWS_SESSION_TOKEN"),
            (AmazonS3ConfigKey::Region, "AWS_REGION"),
        ] {
            if let Some(value) = self.setting(name) {
                config.push((key, value.to_string()));
            }
        }
        if let Some(endpoint) = self.setting("AWS_ENDPOINT") {
            if endpoint.starts_with("http://") {
                config.push((allow_http(), "true".to_string()));
            }
            config.push((AmazonS3ConfigKey::Endpoint, endpoint.to_string()));
        }
        config
    }

    fn gcs_config(&self) -> Vec<(GoogleConfigKey, String)> {
        let mut config = Vec::new();
        if let Some(path) = self.setting("GOOGLE_SERVICE_ACC_KEY_FILEPATH") {
            config.push((GoogleConfigKey::ServiceAccount, path.to_string()));
        } else if let Some(key) = self.setting("GOOGLE_SERVICE_ACC_KEY") {
            config.push((GoogleConfigKey::ServiceAccountKey, key.to_string()));
        }
        config
    }

    fn azure_config(&self) -> Vec<(AzureConfigKey, String)> {
        let mut config = Vec::new();
        for (key, name) in [
            (AzureConfigKey::AccountName, "AZURE_STORAGE_ACCOUNT_NAME"),
            (AzureConfigKey::ClientId, "AZURE_CLIENT_ID"),
            (AzureConfigKey::ClientSecret, "AZURE_CLIENT_SECRET"),
            (AzureConfigKey::AuthorityId, "AZURE_TENANT_ID"),
        ] {
            if let Some(value) = self.setting(name) {
                config.push((key, value.to_string()));
            }
        }
        config
    }

    /// A lazy scan of the Parquet files at a path or glob.
    pub fn scan_parquet(&self, path: &str) -> Result<LazyFrame> {
        let args = ScanArgsParquet {
            cloud_options: self.cloud_options(path)?,
            ..Default::default()
        };
        LazyFrame::scan_parquet(PlRefPath::new(path), args)
            .with_context(|| format!("Could not read {path}"))
    }

    /// The Parquet files at a path or glob, read whole.
    pub fn read_parquet(&self, path: &str) -> Result<DataFrame> {
        self.scan_parquet(path)?
            .collect()
            .with_context(|| format!("Could not read {path}"))
    }

    /// A lazy scan of the CSV files at a path or glob, with a header row.
    pub fn scan_csv(&self, path: &str) -> Result<LazyFrame> {
        LazyCsvReader::new(PlRefPath::new(path))
            .with_has_header(true)
            .with_cloud_options(self.cloud_options(path)?)
            .finish()
            .with_context(|| format!("Could not read {path}"))
    }

    /// The CSV files at a path or glob, read whole.
    pub fn read_csv(&self, path: &str) -> Result<DataFrame> {
        self.scan_csv(path)?
            .collect()
            .with_context(|| format!("Could not read {path}"))
    }

    /// Writes a frame to one Parquet file, replacing it. The file appears whole or not at
    /// all: object stores publish an upload when it completes.
    pub fn write_parquet(&self, frame: &DataFrame, path: &str) -> Result<()> {
        self.sink(
            frame.clone().lazy(),
            path,
            FileWriteFormat::Parquet(Arc::new(ParquetWriteOptions::default())),
        )
    }

    /// Writes a lazy frame to one Parquet file without collecting it first: Polars streams
    /// the rows to the file.
    pub fn sink_parquet(&self, frame: LazyFrame, path: &str) -> Result<()> {
        self.sink(
            frame,
            path,
            FileWriteFormat::Parquet(Arc::new(ParquetWriteOptions::default())),
        )
    }

    /// Writes a frame to one CSV file with a header row, replacing it.
    pub fn write_csv(&self, frame: &DataFrame, path: &str) -> Result<()> {
        self.sink(
            frame.clone().lazy(),
            path,
            FileWriteFormat::Csv(CsvWriterOptions::default()),
        )
    }

    fn sink(&self, frame: LazyFrame, path: &str, format: FileWriteFormat) -> Result<()> {
        let local = provider(path)? == Provider::Local;
        let args = UnifiedSinkArgs {
            // Local folders are created as object stores create key prefixes.
            mkdir: local,
            cloud_options: self.cloud_options(path)?.map(Arc::new),
            ..Default::default()
        };
        frame
            .sink(
                SinkDestination::File {
                    target: SinkTarget::Path(PlRefPath::new(path)),
                },
                format,
                args,
            )
            .and_then(|plan| plan.collect())
            .with_context(|| format!("Could not write {path}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(pairs: &[(&str, &str)]) -> Storage {
        Storage::with_settings(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn paths_name_their_provider() {
        assert_eq!(provider("s3://b/k.parquet").unwrap(), Provider::S3);
        assert_eq!(provider("S3A://b/k").unwrap(), Provider::S3);
        assert_eq!(provider("gs://b/k").unwrap(), Provider::Gcs);
        assert_eq!(
            provider("abfss://c@a.dfs.core.windows.net/k").unwrap(),
            Provider::Azure
        );
        assert_eq!(provider("/tmp/x.parquet").unwrap(), Provider::Local);
        assert_eq!(provider("file:///tmp/x").unwrap(), Provider::Local);
        let error = provider("ftp://host/x").unwrap_err().to_string();
        assert!(error.contains("ftp:// paths are not supported"), "{error}");
    }

    #[test]
    fn s3_settings_map_to_object_store_keys() {
        let config = storage(&[
            ("AWS_ACCESS_KEY_ID", "id"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
            ("AWS_SESSION_TOKEN", ""),
            ("AWS_ENDPOINT", "http://127.0.0.1:9000"),
        ])
        .s3_config();
        let keys: HashMap<_, _> = config.into_iter().collect();
        assert_eq!(keys[&AmazonS3ConfigKey::AccessKeyId], "id");
        assert_eq!(keys[&AmazonS3ConfigKey::SecretAccessKey], "secret");
        assert_eq!(keys[&AmazonS3ConfigKey::Endpoint], "http://127.0.0.1:9000");
        // An empty value is no value: the environment's applies.
        assert!(!keys.contains_key(&AmazonS3ConfigKey::Token));
        assert_eq!(keys[&allow_http()], "true");
        let https = storage(&[("AWS_ENDPOINT", "https://s3.example.com")]).s3_config();
        assert_eq!(https.len(), 1);
    }

    #[test]
    fn a_google_key_file_comes_before_the_key() {
        let config = storage(&[
            ("GOOGLE_SERVICE_ACC_KEY_FILEPATH", "/keys/sa.json"),
            ("GOOGLE_SERVICE_ACC_KEY", "{}"),
        ])
        .gcs_config();
        assert_eq!(
            config,
            vec![(GoogleConfigKey::ServiceAccount, "/keys/sa.json".to_string())]
        );
        let key = storage(&[("GOOGLE_SERVICE_ACC_KEY", "{\"type\":\"x\"}")]).gcs_config();
        assert_eq!(key[0].0, GoogleConfigKey::ServiceAccountKey);
    }

    #[test]
    fn local_files_round_trip_and_scans_select_columns() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let frame = df!(
            "id" => [1i64, 2, 3],
            "name" => ["a", "b", "c"],
        )?;
        let client = Storage::default();
        let parquet = directory.path().join("nested/rows.parquet");
        let parquet = parquet.to_str().unwrap();
        client.write_parquet(&frame, parquet)?;
        assert!(client.read_parquet(parquet)?.equals(&frame));
        let picked = client
            .scan_parquet(parquet)?
            .filter(col("id").gt(lit(1)))
            .select([col("name")])
            .collect()?;
        assert_eq!(picked, df!("name" => ["b", "c"])?);

        let csv = directory.path().join("rows.csv");
        let csv = csv.to_str().unwrap();
        client.write_csv(&frame, csv)?;
        assert!(client.read_csv(csv)?.equals(&frame));

        client.sink_parquet(frame.clone().lazy().filter(col("id").eq(lit(2))), parquet)?;
        assert_eq!(client.read_parquet(parquet)?.height(), 1);
        Ok(())
    }
}
