//! Raw bus backends (adapters) behind [`Publisher`](super::Publisher) and
//! [`Subscriber`](super::Subscriber).
//!
//! The `ProducerBackend` / `ConsumerBackend` traits work on plain topics and
//! byte payloads and are visible only inside `bus`. The adapter types are
//! public so they can be constructed (`KafkaProducer`) or inspected in tests
//! (`MemoryBus`).

use async_trait::async_trait;

use super::BusError;

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(all(feature = "kafka", any(test, feature = "mock")))]
pub mod memory;

/// A received message as the backend sees it. `partition`/`offset` identify
/// it for [`ConsumerBackend::commit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RawMessage {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) offset: i64,
    pub(super) key: Option<Vec<u8>>,
    pub(super) payload: Vec<u8>,
}

#[async_trait]
pub(super) trait ProducerBackend: Send + Sync {
    async fn send(&self, topic: &str, key: &str, payload: &[u8]) -> Result<(), BusError>;
}

#[async_trait]
pub(super) trait ConsumerBackend: Send + Sync {
    async fn recv(&self) -> Result<RawMessage, BusError>;
    async fn commit(&self, message: &RawMessage) -> Result<(), BusError>;
}
