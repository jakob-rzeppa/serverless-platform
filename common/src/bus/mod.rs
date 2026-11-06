//! Message bus port (Kafka in production).
//!
//! The bus is typed: [`Publisher<M>`] and [`Subscriber<M>`] are handles for
//! one message kind `M` (e.g. [`messages::http_deploy::HttpDeploy`]). Publishing,
//! receiving and the subscriber constructors are implemented per kind in
//! `impl Publisher<Kind>` / `impl Subscriber<Kind>` blocks (see [`messages`]),
//! which fix the topic, partition key and encoding. The raw producer /
//! consumer backend is visible only inside `bus`, so other crates cannot send
//! arbitrary payloads to arbitrary topics.

use std::{marker::PhantomData, sync::Arc};

use backend::{ConsumerBackend, ProducerBackend, RawMessage};
use serde::{Serialize, de::DeserializeOwned};

pub mod backend;
pub mod messages;

#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("bus backend error: {0}")]
    Backend(String),
    #[error("invalid message payload: {0}")]
    Decode(#[from] serde_json::Error),
}

/// Typed handle for publishing messages of kind `M`. Cheap to clone; all
/// publishers built from one `KafkaProducer` share its connection.
pub struct Publisher<M> {
    backend: Arc<dyn ProducerBackend>,
    _kind: PhantomData<fn(M)>,
}

impl<M> Clone for Publisher<M> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            _kind: PhantomData,
        }
    }
}

impl<M> Publisher<M> {
    #[cfg(feature = "kafka")]
    pub fn kafka(producer: &backend::kafka::KafkaProducer) -> Self {
        Self {
            backend: Arc::new(producer.clone()),
            _kind: PhantomData,
        }
    }

    #[cfg(all(feature = "kafka", any(test, feature = "mock")))]
    pub fn memory(bus: &backend::memory::MemoryBus) -> Self {
        Self {
            backend: Arc::new(bus.producer()),
            _kind: PhantomData,
        }
    }

    /// JSON-encode and send; used by the per-kind `publish` methods.
    async fn send_json(&self, topic: &str, key: &str, message: &M) -> Result<(), BusError>
    where
        M: Serialize,
    {
        let payload = serde_json::to_vec(message)?;
        self.backend.send(topic, key, &payload).await
    }
}

/// Typed handle for consuming messages of kind `M` as a member of a consumer
/// group. Offsets are committed explicitly, only after a message has been
/// fully processed (at-least-once).
///
/// Constructors are per kind (`Subscriber::<HttpDeploy>::kafka(..)`), because
/// they subscribe to that kind's topic.
pub struct Subscriber<M> {
    backend: Arc<dyn ConsumerBackend>,
    _kind: PhantomData<fn() -> M>,
}

impl<M> Subscriber<M> {
    #[cfg(feature = "kafka")]
    fn kafka_topic(brokers: &str, group_id: &str, topic: &str) -> Result<Self, BusError> {
        Ok(Self {
            backend: Arc::new(backend::kafka::KafkaConsumer::new(
                brokers, group_id, topic,
            )?),
            _kind: PhantomData,
        })
    }

    #[cfg(all(feature = "kafka", any(test, feature = "mock")))]
    fn memory_topic(bus: &backend::memory::MemoryBus, topic: &str) -> Self {
        Self {
            backend: Arc::new(bus.consumer(topic)),
            _kind: PhantomData,
        }
    }

    /// Receive the next message and JSON-decode it; used by the per-kind
    /// `recv` methods. Decode failures are returned inside the [`Delivery`],
    /// so the caller can still commit (skip) a poison message.
    async fn recv_json(&self) -> Result<Delivery<M>, BusError>
    where
        M: DeserializeOwned,
    {
        let raw = self.backend.recv().await?;
        let message = serde_json::from_slice(&raw.payload).map_err(BusError::from);
        Ok(Delivery { message, raw })
    }

    /// Mark `delivery` (and everything before it on its partition) as processed.
    pub async fn commit(&self, delivery: &Delivery<M>) -> Result<(), BusError> {
        self.backend.commit(&delivery.raw).await
    }
}

/// A received message of kind `M`, plus what is needed to commit it.
pub struct Delivery<M> {
    /// The decoded message, or why it could not be decoded (poison message).
    pub message: Result<M, BusError>,
    raw: RawMessage,
}

impl<M> Delivery<M> {
    pub fn topic(&self) -> &str {
        &self.raw.topic
    }
    pub fn partition(&self) -> i32 {
        self.raw.partition
    }
    pub fn offset(&self) -> i64 {
        self.raw.offset
    }
}
