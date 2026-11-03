//! Kafka adapter (rdkafka / librdkafka).

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::{
    ClientConfig, Message as _, Offset, TopicPartitionList,
    consumer::{CommitMode, Consumer as _, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
};

use super::{BusError, ConsumerBackend, ProducerBackend, RawMessage};

fn backend(e: impl std::fmt::Display) -> BusError {
    BusError::Backend(e.to_string())
}

/// One Kafka producer connection. Create it once per process and build all
/// [`Publisher`](crate::bus::Publisher)s from it; they share the connection.
#[derive(Clone)]
pub struct KafkaProducer {
    inner: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Result<Self, BusError> {
        let inner = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .map_err(backend)?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl ProducerBackend for KafkaProducer {
    async fn send(&self, topic: &str, key: &str, payload: &[u8]) -> Result<(), BusError> {
        let record = FutureRecord::to(topic).key(key).payload(payload);
        self.inner
            .send(record, Duration::from_secs(30))
            .await
            .map(|_| ())
            .map_err(|(e, _)| backend(e))
    }
}

/// A group member subscribed to one topic, with auto-commit disabled.
pub(in crate::bus) struct KafkaConsumer {
    inner: StreamConsumer,
}

impl KafkaConsumer {
    pub(in crate::bus) fn new(
        brokers: &str,
        group_id: &str,
        topic: &str,
    ) -> Result<Self, BusError> {
        let inner: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(backend)?;
        inner.subscribe(&[topic]).map_err(backend)?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl ConsumerBackend for KafkaConsumer {
    async fn recv(&self) -> Result<RawMessage, BusError> {
        let m = self.inner.recv().await.map_err(backend)?;
        Ok(RawMessage {
            topic: m.topic().to_string(),
            partition: m.partition(),
            offset: m.offset(),
            key: m.key().map(<[u8]>::to_vec),
            payload: m.payload().unwrap_or_default().to_vec(),
        })
    }

    async fn commit(&self, message: &RawMessage) -> Result<(), BusError> {
        let mut tpl = TopicPartitionList::new();
        // Kafka commits the offset of the *next* message to read.
        tpl.add_partition_offset(
            &message.topic,
            message.partition,
            Offset::Offset(message.offset + 1),
        )
        .map_err(backend)?;
        self.inner.commit(&tpl, CommitMode::Async).map_err(backend)
    }
}
