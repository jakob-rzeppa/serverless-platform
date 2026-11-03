//! `cli.run`: execute one app version once.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::CLI_RUN;
use crate::{
    bus::{BusError, Delivery, Publisher, Subscriber},
    storage::objects::artifact::ArtifactKey,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CliRun {
    pub run_id: Uuid,
    pub app_id: Uuid,
    pub version_id: Uuid,
    /// Key in the artifacts bucket.
    pub artifact_key: ArtifactKey,
    /// Lowercase hex sha256 of the component; verified by the runner.
    pub sha256: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// Plain env vars for the guest. Secrets are not sent in clear here;
    /// they will be passed as references and resolved by the runner later.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Overrides the runner's default timeout.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

impl Publisher<CliRun> {
    pub async fn publish(&self, run: &CliRun) -> Result<(), BusError> {
        self.send_json(CLI_RUN, &run.app_id.to_string(), run).await
    }
}

impl Subscriber<CliRun> {
    /// Join `group_id` and subscribe to `cli.run`.
    #[cfg(feature = "kafka")]
    pub fn kafka(brokers: &str, group_id: &str) -> Result<Self, BusError> {
        Self::kafka_topic(brokers, group_id, CLI_RUN)
    }

    #[cfg(all(feature = "kafka", any(test, feature = "mock")))]
    pub fn memory(bus: &crate::bus::backend::memory::MemoryBus) -> Self {
        Self::memory_topic(bus, CLI_RUN)
    }

    pub async fn recv(&self) -> Result<Delivery<CliRun>, BusError> {
        self.recv_json().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let run: CliRun = serde_json::from_str(&format!(
            r#"{{"run_id":"{0}","app_id":"{0}","version_id":"{0}","artifact_key":"apps/{0}/{0}/component.wasm","sha256":"s"}}"#,
            Uuid::nil()
        ))
        .unwrap();
        assert!(run.args.is_empty() && run.env.is_empty() && run.timeout_secs.is_none());
    }

    #[test]
    fn rejects_malformed_artifact_key() {
        let json = format!(
            r#"{{"run_id":"{0}","app_id":"{0}","version_id":"{0}","artifact_key":"../x","sha256":"s"}}"#,
            Uuid::nil()
        );
        assert!(serde_json::from_str::<CliRun>(&json).is_err());
    }
}
