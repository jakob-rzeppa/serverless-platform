//! `run.events`: status of a run, published by runners.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{RUN_EVENTS, cli_run::CliRun};
use crate::bus::{BusError, Delivery, Publisher, Subscriber};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEvent {
    pub run_id: Uuid,
    pub app_id: Uuid,
    pub at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: RunEventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEventKind {
    Started,
    /// The guest ran to completion (any exit code).
    Finished {
        exit_code: i32,
    },
    /// The run could not complete: bad artifact, trap, timeout, …
    Failed {
        error: String,
    },
}

impl RunEvent {
    pub fn new(run: &CliRun, kind: RunEventKind) -> Self {
        Self {
            run_id: run.run_id,
            app_id: run.app_id,
            at: Utc::now(),
            kind,
        }
    }
}

impl Publisher<RunEvent> {
    pub async fn publish(&self, event: &RunEvent) -> Result<(), BusError> {
        self.send_json(RUN_EVENTS, &event.app_id.to_string(), event)
            .await
    }
}

impl Subscriber<RunEvent> {
    /// Join `group_id` and subscribe to `run.events`.
    #[cfg(feature = "kafka")]
    pub fn kafka(brokers: &str, group_id: &str) -> Result<Self, BusError> {
        Self::kafka_topic(brokers, group_id, RUN_EVENTS)
    }

    #[cfg(all(feature = "kafka", any(test, feature = "mock")))]
    pub fn memory(bus: &crate::bus::backend::memory::MemoryBus) -> Self {
        Self::memory_topic(bus, RUN_EVENTS)
    }

    pub async fn recv(&self) -> Result<Delivery<RunEvent>, BusError> {
        self.recv_json().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_shape() {
        let ev = RunEvent {
            run_id: Uuid::nil(),
            app_id: Uuid::nil(),
            at: DateTime::UNIX_EPOCH,
            kind: RunEventKind::Finished { exit_code: 3 },
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "finished");
        assert_eq!(json["exit_code"], 3);
        assert_eq!(serde_json::from_value::<RunEvent>(json).unwrap(), ev);
    }
}
