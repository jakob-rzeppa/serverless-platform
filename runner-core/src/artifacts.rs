//! Artifact fetching: object store → sha256 check → disk cache → compiled
//! `Component`. Artifacts are immutable, so caching by sha256 is safe.
//!
//! Every call compiles the bytes; callers that run a component repeatedly
//! should keep the returned `Component` (or an `InstancePre`) themselves.

use std::path::PathBuf;

use common::storage::{
    ObjectStorage, StorageError,
    objects::artifact::{Artifact, ArtifactKey},
};
use sha2::{Digest, Sha256};
use wasmtime::{Engine, component::Component};

use crate::Runtime;

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    /// The expected hash is not 64 hex characters.
    #[error("invalid sha256 {0:?}")]
    InvalidHash(String),
    /// No object exists under the artifact key.
    #[error("artifact {0} not found")]
    NotFound(String),
    /// The stored bytes do not match the expected hash.
    #[error("sha256 mismatch for {key}: expected {expected}, got {actual}")]
    HashMismatch {
        key: String,
        expected: String,
        actual: String,
    },
    /// Any other object storage failure.
    #[error("artifact storage: {0}")]
    Storage(StorageError),
    /// Reading or writing the disk cache failed.
    #[error("artifact cache io: {0}")]
    Io(#[from] std::io::Error),
    /// The bytes are not a valid wasm component.
    #[error("invalid component: {0}")]
    Compile(String),
}

/// Loads artifacts from object storage through a sha256-keyed disk cache in
/// `dir` and compiles them with the runtime's engine.
pub struct ArtifactCache {
    storage: ObjectStorage<Artifact>,
    dir: PathBuf,
    engine: Engine,
}

impl ArtifactCache {
    /// `dir` is created on first write if it does not exist.
    pub fn new(storage: ObjectStorage<Artifact>, dir: PathBuf, runtime: &Runtime) -> Self {
        Self {
            storage,
            dir,
            engine: runtime.engine().clone(),
        }
    }

    /// Fetch the artifact at `key` (from disk if cached, else from storage),
    /// verify it against `sha256` (hex, any case) and compile it. Compilation
    /// runs on a blocking thread.
    pub async fn component(
        &self,
        key: &ArtifactKey,
        sha256: &str,
    ) -> Result<Component, ArtifactError> {
        let sha256 = sha256.to_ascii_lowercase();
        // The hash becomes a file name, so it must be exactly 64 hex chars.
        if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ArtifactError::InvalidHash(sha256));
        }
        let bytes = self.fetch(key, &sha256).await?;
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || Component::new(&engine, &bytes))
            .await
            .expect("compile task panicked")
            .map_err(|e| ArtifactError::Compile(format!("{e:#}")))
    }

    async fn fetch(&self, key: &ArtifactKey, sha256: &str) -> Result<Vec<u8>, ArtifactError> {
        let path = self.dir.join(format!("{sha256}.wasm"));
        if let Ok(bytes) = tokio::fs::read(&path).await {
            if hex_sha256(&bytes) == sha256 {
                return Ok(bytes);
            }
            tracing::warn!(path = %path.display(), "corrupt cache entry, refetching");
        }

        let bytes = self.storage.get(key).await.map_err(|e| match e {
            StorageError::NotFound(k) => ArtifactError::NotFound(k),
            // Not a wasm component: report it like a compile failure.
            StorageError::Invalid(e) => ArtifactError::Compile(e),
            e => ArtifactError::Storage(e),
        })?;
        let actual = hex_sha256(&bytes);
        if actual != sha256 {
            return Err(ArtifactError::HashMismatch {
                key: key.path(),
                expected: sha256.into(),
                actual,
            });
        }

        // Write atomically so a crash never leaves a partial file under the final name.
        tokio::fs::create_dir_all(&self.dir).await?;
        let tmp = self
            .dir
            .join(format!("{sha256}.{}.tmp", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp, &bytes).await?;
        tokio::fs::rename(&tmp, &path).await?;
        Ok(bytes.to_vec())
    }
}

/// Lowercase hex sha256 of `data`.
pub fn hex_sha256(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}
