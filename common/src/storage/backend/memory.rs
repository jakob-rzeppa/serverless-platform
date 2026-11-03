//! In-memory object storage mock for tests and local development.
//!
//! Pass it to [`ObjectStorage::memory`](crate::storage::ObjectStorage::memory) and keep
//! the `MemoryStorage` to inspect raw objects or plant corrupt ones.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;

use super::{StorageBackend, StorageError};

/// Cloneable; clones share the same objects.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    objects: Arc<Mutex<BTreeMap<String, Bytes>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// All keys, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.objects.lock().unwrap().keys().cloned().collect()
    }

    /// Raw insert, bypassing validation (e.g. to plant a corrupt object).
    pub fn insert(&self, key: &str, data: impl Into<Bytes>) {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), data.into());
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        self.insert(key, data);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(key.to_string()))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let objects = self.objects.lock().unwrap();
        Ok(objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn crud() {
        let s = MemoryStorage::new();
        s.put("a/1", Bytes::from_static(b"x")).await.unwrap();
        s.put("a/2", Bytes::from_static(b"y")).await.unwrap();
        s.put("b/1", Bytes::from_static(b"z")).await.unwrap();
        assert_eq!(s.get("a/1").await.unwrap(), "x");
        assert_eq!(s.list("a/").await.unwrap(), vec!["a/1", "a/2"]);
        s.delete("a/1").await.unwrap();
        s.delete("missing").await.unwrap();
        assert!(matches!(s.get("a/1").await, Err(StorageError::NotFound(_))));
    }
}
