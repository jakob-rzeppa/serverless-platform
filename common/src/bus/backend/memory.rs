//! In-memory bus mock for tests and local development.
//!
//! Build publishers / subscribers from it with `Publisher::memory(&bus)` and
//! `Subscriber::<Kind>::memory(&bus)`. All subscribers of one [`MemoryBus`]
//! behave like members of a single consumer group: each message is delivered
//! to exactly one of them. Everything ever published stays inspectable via
//! [`MemoryBus::published`].

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Notify;

use super::{BusError, ConsumerBackend, ProducerBackend, RawMessage};

#[derive(Default)]
struct Topic {
    messages: Vec<RawMessage>,
    next_read: usize,
    committed: Option<i64>,
}

#[derive(Clone, Default)]
pub struct MemoryBus {
    topics: Arc<Mutex<HashMap<String, Topic>>>,
    notify: Arc<Notify>,
}

impl MemoryBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub(in crate::bus) fn producer(&self) -> MemoryProducer {
        MemoryProducer { bus: self.clone() }
    }

    pub(in crate::bus) fn consumer(&self, topic: &str) -> MemoryConsumer {
        MemoryConsumer {
            bus: self.clone(),
            topic: topic.to_string(),
        }
    }

    /// Append raw bytes to `topic` (e.g. a poison message); returns the offset.
    pub fn push_raw(&self, topic: &str, key: &str, payload: &[u8]) -> i64 {
        let mut topics = self.topics.lock().unwrap();
        let t = topics.entry(topic.to_string()).or_default();
        let offset = t.messages.len() as i64;
        t.messages.push(RawMessage {
            topic: topic.to_string(),
            partition: 0,
            offset,
            key: Some(key.as_bytes().to_vec()),
            payload: payload.to_vec(),
        });
        drop(topics);
        self.notify.notify_waiters();
        offset
    }

    /// Append a JSON-encoded message to `topic`; returns the offset.
    pub fn push<T: Serialize>(&self, topic: &str, message: &T) -> i64 {
        let payload = serde_json::to_vec(message).expect("serializable message");
        self.push_raw(topic, "", &payload)
    }

    /// Every message ever published to `topic`, decoded as `T`.
    pub fn published<T: DeserializeOwned>(&self, topic: &str) -> Vec<T> {
        let topics = self.topics.lock().unwrap();
        topics
            .get(topic)
            .map(|t| {
                t.messages
                    .iter()
                    .map(|m| serde_json::from_slice(&m.payload).expect("valid message"))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Highest committed offset on `topic`, if any.
    pub fn committed(&self, topic: &str) -> Option<i64> {
        self.topics
            .lock()
            .unwrap()
            .get(topic)
            .and_then(|t| t.committed)
    }

    /// Number of messages on `topic` not yet handed to a subscriber.
    pub fn pending(&self, topic: &str) -> usize {
        self.topics
            .lock()
            .unwrap()
            .get(topic)
            .map_or(0, |t| t.messages.len() - t.next_read)
    }

    fn try_take(&self, topic: &str) -> Option<RawMessage> {
        let mut all = self.topics.lock().unwrap();
        let t = all.get_mut(topic)?;
        let m = t.messages.get(t.next_read)?.clone();
        t.next_read += 1;
        Some(m)
    }
}

pub(in crate::bus) struct MemoryProducer {
    bus: MemoryBus,
}

#[async_trait]
impl ProducerBackend for MemoryProducer {
    async fn send(&self, topic: &str, key: &str, payload: &[u8]) -> Result<(), BusError> {
        self.bus.push_raw(topic, key, payload);
        Ok(())
    }
}

pub(in crate::bus) struct MemoryConsumer {
    bus: MemoryBus,
    topic: String,
}

#[async_trait]
impl ConsumerBackend for MemoryConsumer {
    async fn recv(&self) -> Result<RawMessage, BusError> {
        loop {
            // Register for wake-ups before checking, so a push between the
            // check and the await is not missed.
            let notified = self.bus.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(m) = self.bus.try_take(&self.topic) {
                return Ok(m);
            }
            notified.await;
        }
    }

    async fn commit(&self, message: &RawMessage) -> Result<(), BusError> {
        let mut topics = self.bus.topics.lock().unwrap();
        let t = topics.entry(message.topic.clone()).or_default();
        t.committed = Some(
            t.committed
                .map_or(message.offset, |c| c.max(message.offset)),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use uuid::Uuid;

    use super::*;
    use crate::bus::{
        Publisher, Subscriber,
        messages::{HTTP_DEPLOY, http_deploy::HttpDeploy},
    };

    #[tokio::test]
    async fn publish_recv_commit() {
        let bus = MemoryBus::new();
        let subscriber = Subscriber::<HttpDeploy>::memory(&bus);
        let msg = HttpDeploy::Remove {
            app_id: Uuid::new_v4(),
        };

        // recv waits until something is published
        let recv = tokio::spawn(async move {
            let d = subscriber.recv().await.unwrap();
            subscriber.commit(&d).await.unwrap();
            d.message.unwrap()
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        Publisher::<HttpDeploy>::memory(&bus)
            .publish(&msg)
            .await
            .unwrap();

        assert_eq!(recv.await.unwrap(), msg);
        assert_eq!(bus.committed(HTTP_DEPLOY), Some(0));
        assert_eq!(bus.published::<HttpDeploy>(HTTP_DEPLOY), vec![msg]);
        assert_eq!(bus.pending(HTTP_DEPLOY), 0);
        assert_eq!(bus.committed("other.topic"), None);
    }

    #[tokio::test]
    async fn each_message_delivered_once_across_subscribers() {
        let bus = MemoryBus::new();
        let a = Subscriber::<HttpDeploy>::memory(&bus);
        let b = Subscriber::<HttpDeploy>::memory(&bus);
        bus.push_raw(HTTP_DEPLOY, "k", b"1");
        bus.push_raw(HTTP_DEPLOY, "k", b"2");
        let (m1, m2) = (a.recv().await.unwrap(), b.recv().await.unwrap());
        assert_eq!((m1.offset(), m2.offset()), (0, 1));
        // payloads are not valid HttpDeploy JSON: delivered as decode errors
        assert!(m1.message.is_err() && m2.message.is_err());
    }
}
