//! Raw storage backends (adapters) behind [`ObjectStorage`](super::ObjectStorage).
//!
//! The `StorageBackend` trait works on plain string keys and bytes and is
//! visible only inside `storage`, so other crates can reach a backend only
//! through the typed `ObjectStorage` methods. The adapter types are public
//! so they can be configured (`S3Config`) or inspected in tests
//! (`MemoryStorage`).

use async_trait::async_trait;
use bytes::Bytes;

use super::StorageError;

#[cfg(any(test, feature = "mock"))]
pub mod memory;
#[cfg(feature = "s3")]
pub mod s3;

#[async_trait]
pub(super) trait StorageBackend: Send + Sync {
    async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError>;
    async fn get(&self, key: &str) -> Result<Bytes, StorageError>;
    /// Keys starting with `prefix`, sorted.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
    /// Deleting a missing key is not an error.
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
}
