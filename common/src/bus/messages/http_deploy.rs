//! `http.deploy`: desired-state change for an HTTP app.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::HTTP_DEPLOY;
use crate::{
    bus::{BusError, Delivery, Publisher, Subscriber},
    storage::objects::artifact::ArtifactKey,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HttpDeploy {
    Deploy {
        app_id: Uuid,
        version_id: Uuid,
        subdomain: String,
        artifact_key: ArtifactKey,
        sha256: String,
    },
    Remove {
        app_id: Uuid,
    },
}

impl HttpDeploy {
    pub fn app_id(&self) -> Uuid {
        match self {
            HttpDeploy::Deploy { app_id, .. } | HttpDeploy::Remove { app_id } => *app_id,
        }
    }
}

impl Publisher<HttpDeploy> {
    pub async fn publish(&self, deploy: &HttpDeploy) -> Result<(), BusError> {
        self.send_json(HTTP_DEPLOY, &deploy.app_id().to_string(), deploy)
            .await
    }
}

impl Subscriber<HttpDeploy> {
    /// Join `group_id` and subscribe to `http.deploy`.
    #[cfg(feature = "kafka")]
    pub fn kafka(brokers: &str, group_id: &str) -> Result<Self, BusError> {
        Self::kafka_topic(brokers, group_id, HTTP_DEPLOY)
    }

    #[cfg(all(feature = "kafka", any(test, feature = "mock")))]
    pub fn memory(bus: &crate::bus::backend::memory::MemoryBus) -> Self {
        Self::memory_topic(bus, HTTP_DEPLOY)
    }

    pub async fn recv(&self) -> Result<Delivery<HttpDeploy>, BusError> {
        self.recv_json().await
    }
}
