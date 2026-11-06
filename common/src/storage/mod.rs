//! Object storage port (Garage / any S3 in production).
//!
//! Storage is typed: [`ObjectStorage<O>`] is a handle to one bucket holding
//! objects of kind `O` (a marker type such as [`objects::artifact::Artifact`]). The operations are implemented per kind in
//! `impl ObjectStorage<Kind>` blocks (see [`objects`]), each taking that
//! kind's key and contents types and validating / encoding them, so only
//! well-formed keys and valid contents can be uploaded. The raw string/bytes
//! backend is crate-private.

use std::{marker::PhantomData, sync::Arc};

use backend::StorageBackend;
#[cfg(any(test, feature = "mock"))]
use backend::memory;
#[cfg(feature = "s3")]
use backend::s3;

pub mod backend;
pub mod objects;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),
    /// Contents failed validation or (de)serialization.
    #[error("invalid object: {0}")]
    Invalid(String),
    #[error("storage backend error: {0}")]
    Backend(String),
}

/// Typed handle to one bucket holding objects of kind `O`. Cheap to clone.
///
/// Only the constructors are generic; `put` / `get` / `list` / `delete` exist
/// only for kinds that implement them (`impl ObjectStorage<Artifact>`, …).
pub struct ObjectStorage<O> {
    backend: Arc<dyn StorageBackend>,
    _kind: PhantomData<fn() -> O>,
}

impl<O> Clone for ObjectStorage<O> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            _kind: PhantomData,
        }
    }
}

impl<O> ObjectStorage<O> {
    /// S3 / Garage bucket.
    #[cfg(feature = "s3")]
    pub fn s3(cfg: &s3::S3Config) -> Result<Self, StorageError> {
        Ok(Self {
            backend: Arc::new(s3::S3Storage::new(cfg)?),
            _kind: PhantomData,
        })
    }

    /// In-memory mock bucket; keep `store` to inspect what was written.
    #[cfg(any(test, feature = "mock"))]
    pub fn memory(store: &memory::MemoryStorage) -> Self {
        Self {
            backend: Arc::new(store.clone()),
            _kind: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use uuid::Uuid;

    use super::{
        backend::memory::MemoryStorage,
        objects::artifact::{Artifact, ArtifactKey},
        *,
    };

    /// Smallest valid component header (magic + component version/layer).
    const COMPONENT: &[u8] = b"\0asm\x0d\0\x01\0";

    #[tokio::test]
    async fn rejects_invalid_contents_without_writing() {
        let raw = MemoryStorage::new();
        let artifacts = ObjectStorage::<Artifact>::memory(&raw);
        let key = ArtifactKey {
            app_id: Uuid::new_v4(),
            version_id: Uuid::new_v4(),
        };

        let not_wasm = Bytes::from_static(b"not wasm");
        assert!(matches!(
            artifacts.put(&key, &not_wasm).await,
            Err(StorageError::Invalid(_))
        ));
        assert!(raw.keys().is_empty());
    }

    #[tokio::test]
    async fn typed_put_get_list_delete() {
        let raw = MemoryStorage::new();
        let artifacts = ObjectStorage::<Artifact>::memory(&raw);
        let app_id = Uuid::new_v4();
        let key = ArtifactKey {
            app_id,
            version_id: Uuid::new_v4(),
        };
        let component = Bytes::from_static(COMPONENT);

        artifacts.put(&key, &component).await.unwrap();
        raw.insert(&format!("apps/{app_id}/junk.txt"), "x");
        assert_eq!(artifacts.list_app_versions(app_id).await.unwrap(), vec![key]);
        assert_eq!(artifacts.get(&key).await.unwrap(), component);
        artifacts.delete(&key).await.unwrap();
        assert!(matches!(
            artifacts.get(&key).await,
            Err(StorageError::NotFound(_))
        ));
    }
}
