//! Iceberg's file IO on the engine's object_store (ADR-0042): S3 and compatible stores, Google
//! Cloud Storage, Azure, and local disk, with the credentials a REST catalog vends for a
//! table (`s3.access-key-id`, `gcs.oauth2.token`, `adls.sas-token`, ...) or, without them,
//! those of the environment as each cloud's tools read them.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObjectStorageFactory;

#[typetag::serde]
impl StorageFactory for ObjectStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(ObjectStorage {
            props: config.props().clone(),
            stores: Default::default(),
        }))
    }
}

/// Files at full URLs (`s3://bucket/key`), a store per bucket.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ObjectStorage {
    props: HashMap<String, String>,
    #[serde(skip)]
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

fn io_error(what: &str, location: &str, error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, format!("{what} {location}: {error}"))
}

impl ObjectStorage {
    fn prop(&self, keys: &[&str]) -> Option<&str> {
        keys.iter()
            .find_map(|k| self.props.get(*k))
            .map(|v| v.as_str())
            .filter(|v| !v.is_empty())
    }

    /// The store of `location`'s bucket and the object's path in it.
    fn locate(&self, location: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
        let url = url::Url::parse(location)
            .map_err(|e| io_error("Iceberg file location", location, e))?;
        let path = Path::from_url_path(url.path())
            .map_err(|e| io_error("Iceberg file location", location, e))?;
        let root = match url.scheme() {
            "file" => "file://".to_string(),
            scheme => format!("{scheme}://{}", url.host_str().unwrap_or_default()),
        };
        let mut stores = self.stores.lock().expect("stores");
        if let Some(store) = stores.get(&root) {
            return Ok((store.clone(), path));
        }
        let store = self
            .open(&url)
            .map_err(|e| io_error("Iceberg storage", &root, e))?;
        stores.insert(root, store.clone());
        Ok((store, path))
    }

    fn open(&self, url: &url::Url) -> std::result::Result<Arc<dyn ObjectStore>, String> {
        let bucket = url.host_str().unwrap_or_default();
        let store: Arc<dyn ObjectStore> = match url.scheme() {
            "s3" | "s3a" | "s3n" => {
                let mut builder =
                    object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
                if let Some(v) = self.prop(&["s3.access-key-id"]) {
                    builder = builder.with_access_key_id(v);
                }
                if let Some(v) = self.prop(&["s3.secret-access-key"]) {
                    builder = builder.with_secret_access_key(v);
                }
                if let Some(v) = self.prop(&["s3.session-token"]) {
                    builder = builder.with_token(v);
                }
                if let Some(v) = self.prop(&["s3.region", "client.region"]) {
                    builder = builder.with_region(v);
                }
                if let Some(v) = self.prop(&["s3.endpoint"]) {
                    builder = builder
                        .with_allow_http(v.starts_with("http://"))
                        .with_endpoint(v);
                }
                if let Some(v) = self.prop(&["s3.path-style-access"]) {
                    builder = builder.with_virtual_hosted_style_request(v != "true");
                }
                Arc::new(builder.build().map_err(|e| e.to_string())?)
            }
            "gs" | "gcs" => {
                let mut builder = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket);
                if let Some(token) = self.prop(&["gcs.oauth2.token"]) {
                    builder = builder.with_credentials(Arc::new(
                        object_store::StaticCredentialProvider::new(
                            object_store::gcp::GcpCredential {
                                bearer: token.to_string(),
                            },
                        ),
                    ));
                }
                if let Some(json) = self.prop(&["gcs.credentials-json"]) {
                    builder = builder.with_service_account_key(json);
                }
                Arc::new(builder.build().map_err(|e| e.to_string())?)
            }
            "abfs" | "abfss" | "wasb" | "wasbs" | "az" | "azure" => {
                let mut builder =
                    object_store::azure::MicrosoftAzureBuilder::from_env().with_url(url.as_str());
                if let Some(v) = self.prop(&["adls.account-name"]) {
                    builder = builder.with_account(v);
                }
                if let Some(v) = self.prop(&["adls.account-key"]) {
                    builder = builder.with_access_key(v);
                }
                if let Some(v) = self.prop(&["adls.sas-token"]) {
                    builder = builder.with_config(object_store::azure::AzureConfigKey::SasKey, v);
                }
                Arc::new(builder.build().map_err(|e| e.to_string())?)
            }
            "file" => Arc::new(object_store::local::LocalFileSystem::new()),
            other => return Err(format!("{other}:// is not a storage MageML writes to")),
        };
        Ok(store)
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for ObjectStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (store, key) = self.locate(path)?;
        match store.head(&key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(io_error("Checking", path, e)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (store, key) = self.locate(path)?;
        let meta = store
            .head(&key)
            .await
            .map_err(|e| io_error("Reading", path, e))?;
        Ok(FileMetadata { size: meta.size })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (store, key) = self.locate(path)?;
        let got = store
            .get(&key)
            .await
            .map_err(|e| io_error("Reading", path, e))?;
        got.bytes().await.map_err(|e| io_error("Reading", path, e))
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (store, key) = self.locate(path)?;
        Ok(Box::new(ObjectRead {
            store,
            key,
            location: path.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (store, key) = self.locate(path)?;
        store
            .put(&key, PutPayload::from(bs))
            .await
            .map_err(|e| io_error("Writing", path, e))?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (store, key) = self.locate(path)?;
        let upload = store
            .put_multipart(&key)
            .await
            .map_err(|e| io_error("Writing", path, e))?;
        Ok(Box::new(ObjectWrite {
            upload: Some(WriteMultipart::new(upload)),
            written: 0,
            location: path.to_string(),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (store, key) = self.locate(path)?;
        match store.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(io_error("Deleting", path, e)),
        }
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (store, key) = self.locate(path)?;
        let mut listed = store.list(Some(&key));
        while let Some(meta) = listed.next().await {
            let meta = meta.map_err(|e| io_error("Listing", path, e))?;
            match store.delete(&meta.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(io_error("Deleting", path, e)),
            }
        }
        Ok(())
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.share()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.share()), path.to_string()))
    }
}

impl ObjectStorage {
    /// The same storage, with the stores opened so far.
    fn share(&self) -> ObjectStorage {
        ObjectStorage {
            props: self.props.clone(),
            stores: Mutex::new(self.stores.lock().expect("stores").clone()),
        }
    }
}

struct ObjectRead {
    store: Arc<dyn ObjectStore>,
    key: Path,
    location: String,
}

#[async_trait]
impl FileRead for ObjectRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store
            .get_range(&self.key, range)
            .await
            .map_err(|e| io_error("Reading", &self.location, e))
    }
}

struct ObjectWrite {
    upload: Option<WriteMultipart>,
    written: u64,
    location: String,
}

#[async_trait]
impl FileWrite for ObjectWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| io_error("Writing", &self.location, "the file is closed"))?;
        upload
            .wait_for_capacity(8)
            .await
            .map_err(|e| io_error("Writing", &self.location, e))?;
        self.written += bs.len() as u64;
        upload.put(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        let upload = self
            .upload
            .take()
            .ok_or_else(|| io_error("Writing", &self.location, "the file is closed"))?;
        upload
            .finish()
            .await
            .map_err(|e| io_error("Writing", &self.location, e))?;
        Ok(FileMetadata { size: self.written })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn files_are_written_read_and_deleted_on_local_disk() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ObjectStorageFactory.build(&StorageConfig::new()).unwrap();
        let path = format!("file://{}/a/b.bin", dir.path().display());
        assert!(!storage.exists(&path).await.unwrap());

        let mut writer = storage.writer(&path).await.unwrap();
        writer.write(Bytes::from_static(b"hello ")).await.unwrap();
        writer.write(Bytes::from_static(b"iceberg")).await.unwrap();
        assert_eq!(writer.close().await.unwrap().size, 13);

        assert!(storage.exists(&path).await.unwrap());
        assert_eq!(storage.metadata(&path).await.unwrap().size, 13);
        assert_eq!(&storage.read(&path).await.unwrap()[..], b"hello iceberg");
        let reader = storage.reader(&path).await.unwrap();
        assert_eq!(&reader.read(6..13).await.unwrap()[..], b"iceberg");

        storage
            .delete_prefix(&format!("file://{}/a", dir.path().display()))
            .await
            .unwrap();
        assert!(!storage.exists(&path).await.unwrap());
    }

    #[test]
    fn a_scheme_it_does_not_write_to_is_refused() {
        let storage = ObjectStorage::default();
        let error = storage.locate("ftp://host/x").err().unwrap().to_string();
        assert!(error.contains("ftp:// is not a storage"), "{error}");
    }
}
