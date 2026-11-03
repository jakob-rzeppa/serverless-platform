//! WASM component artifacts, in the artifacts bucket.

use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::storage::{
    ObjectStorage, StorageError,
    objects::{invalid, segments},
};

/// Object kind: a WASM component binary, in the artifacts bucket.
pub struct Artifact;

/// `apps/<app_id>/<version_id>/component.wasm`.
///
/// (De)serializes as its path string, so a malformed key in a Kafka message
/// fails decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArtifactKey {
    pub app_id: Uuid,
    pub version_id: Uuid,
}

impl ArtifactKey {
    pub fn path(&self) -> String {
        format!("apps/{}/{}/component.wasm", self.app_id, self.version_id)
    }

    /// Inverse of [`path`](Self::path); `None` for foreign or malformed paths.
    pub fn parse(path: &str) -> Option<Self> {
        match segments(path, "apps/", "/component.wasm")?[..] {
            [app, version] => Some(Self {
                app_id: app.parse().ok()?,
                version_id: version.parse().ok()?,
            }),
            _ => None,
        }
    }
}

impl Serialize for ArtifactKey {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.path())
    }
}

impl<'de> Deserialize<'de> for ArtifactKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let path = String::deserialize(d)?;
        Self::parse(&path)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid artifact key {path:?}")))
    }
}

/// `\0asm` magic followed by the component-model version/layer (0x0d, layer 1).
/// Core modules (version 1) are rejected.
const COMPONENT_HEADER: [u8; 8] = [0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];

fn check_component(data: &[u8]) -> Result<(), StorageError> {
    if data.starts_with(&COMPONENT_HEADER) {
        Ok(())
    } else if data.starts_with(&COMPONENT_HEADER[..4]) {
        Err(invalid(
            "not a wasm component (core module or unsupported version)",
        ))
    } else {
        Err(invalid("not a wasm binary"))
    }
}

impl ObjectStorage<Artifact> {
    /// Upload a component; rejected unless it is a wasm component binary.
    pub async fn put(&self, key: &ArtifactKey, component: &Bytes) -> Result<(), StorageError> {
        check_component(component)?;
        self.backend.put(&key.path(), component.clone()).await
    }

    pub async fn get(&self, key: &ArtifactKey) -> Result<Bytes, StorageError> {
        let data = self.backend.get(&key.path()).await?;
        check_component(&data)?;
        Ok(data)
    }

    /// All stored versions of `app_id`, sorted by key.
    pub async fn list_app_versions(&self, app_id: Uuid) -> Result<Vec<ArtifactKey>, StorageError> {
        let paths = self.backend.list(&format!("apps/{app_id}/")).await?;
        Ok(paths.iter().filter_map(|p| ArtifactKey::parse(p)).collect())
    }

    pub async fn delete(&self, key: &ArtifactKey) -> Result<(), StorageError> {
        self.backend.delete(&key.path()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_key_round_trip_and_serde() {
        let key = ArtifactKey {
            app_id: Uuid::new_v4(),
            version_id: Uuid::new_v4(),
        };
        assert_eq!(ArtifactKey::parse(&key.path()), Some(key));
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(serde_json::from_str::<ArtifactKey>(&json).unwrap(), key);
        assert!(serde_json::from_str::<ArtifactKey>(r#""apps/../../etc/passwd""#).is_err());
        assert_eq!(ArtifactKey::parse("apps/x/y/component.wasm"), None);
    }

    #[test]
    fn artifact_validation() {
        let mut component = COMPONENT_HEADER.to_vec();
        component.extend_from_slice(b"rest");
        assert!(check_component(&component).is_ok());
        let core_module = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        assert!(matches!(
            check_component(&core_module),
            Err(StorageError::Invalid(_))
        ));
        assert!(matches!(
            check_component(b"not wasm"),
            Err(StorageError::Invalid(_))
        ));
    }
}
