//! `http.deploy`: desired-state change for an HTTP app.
//!
//! Every runner-http replica serves every app, so each replica consumes the
//! whole topic (its own consumer group, from the earliest offset) to rebuild
//! the current state. The topic should be log-compacted by `app_id`.

use std::collections::BTreeMap;

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
    /// Serve `artifact_key` at `<subdomain>.<apps domain>`, replacing any
    /// earlier version of the app (and its old subdomain).
    Deploy {
        app_id: Uuid,
        version_id: Uuid,
        /// One DNS label (`[a-z0-9-]`, at most 63 characters).
        subdomain: String,
        artifact_key: ArtifactKey,
        /// Lowercase hex sha256 of the component; verified by the runner.
        sha256: String,
        /// Plain env vars for every request's instance.
        #[serde(default)]
        env: BTreeMap<String, String>,
    },
    /// Stop serving the app.
    Remove { app_id: Uuid },
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
