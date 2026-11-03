//! S3 adapter (Garage in-cluster, any S3-compatible store works).

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload,
    aws::{AmazonS3, AmazonS3Builder},
    path::Path,
};

use super::{StorageBackend, StorageError};

#[derive(Debug, Clone)]
pub struct S3Config {
    /// e.g. `http://localhost:3900` for a local Garage.
    pub endpoint: String,
    /// Garage defaults to `garage`.
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

pub(crate) struct S3Storage {
    inner: AmazonS3,
}

impl S3Storage {
    pub(crate) fn new(cfg: &S3Config) -> Result<Self, StorageError> {
        let inner = AmazonS3Builder::new()
            .with_endpoint(&cfg.endpoint)
            .with_region(&cfg.region)
            .with_bucket_name(&cfg.bucket)
            .with_access_key_id(&cfg.access_key)
            .with_secret_access_key(&cfg.secret_key)
            .with_allow_http(cfg.endpoint.starts_with("http://"))
            // Garage uses path-style addressing.
            .with_virtual_hosted_style_request(false)
            .build()
            .map_err(map_err)?;
        Ok(Self { inner })
    }
}

fn map_err(e: object_store::Error) -> StorageError {
    match e {
        object_store::Error::NotFound { path, .. } => StorageError::NotFound(path),
        e => StorageError::Backend(e.to_string()),
    }
}

#[async_trait]
impl StorageBackend for S3Storage {
    async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        self.inner
            .put(&Path::from(key), PutPayload::from(data))
            .await
            .map(|_| ())
            .map_err(map_err)
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        let res = self.inner.get(&Path::from(key)).await.map_err(map_err)?;
        res.bytes().await.map_err(map_err)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        // object_store lists by path segment, so list the parent directory
        // and filter by the full string prefix.
        let dir = prefix.rsplit_once('/').map(|(d, _)| Path::from(d));
        let mut keys: Vec<String> = self
            .inner
            .list(dir.as_ref())
            .map_ok(|meta| meta.location.to_string())
            .try_filter(|k| futures::future::ready(k.starts_with(prefix)))
            .try_collect()
            .await
            .map_err(map_err)?;
        keys.sort();
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        match self.inner.delete(&Path::from(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_err(e)),
        }
    }
}
