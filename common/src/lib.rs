//! Shared building blocks for the platform services.
//!
//! Infrastructure is accessed through typed handles over crate-private raw
//! backends; services get a handle with a concrete adapter injected at startup:
//!
//! | Handle | Real adapter (feature) | Mock adapter (feature `mock`) |
//! |--------|------------------------|-------------------------------|
//! | [`bus::Publisher<M>`] / [`bus::Subscriber<M>`] | `Publisher::kafka(&KafkaProducer)`, `Subscriber::<M>::kafka(..)` (`kafka`) | `::memory(&MemoryBus)` |
//! | [`storage::ObjectStorage<O>`] | `ObjectStorage::s3(&S3Config)` (`s3`) | `ObjectStorage::memory(&MemoryStorage)` |
//!
//! Both follow the same pattern: one submodule per kind (`bus::messages::*`,
//! `storage::objects::*`) defines the kind's types and its operations in
//! `impl Publisher<Kind>` / `impl Subscriber<Kind>` / `impl ObjectStorage<Kind>`
//! blocks, which fix the topic / key layout and validate and (de)serialize
//! the bytes. Adapters live in `bus::backend` / `storage::backend`.
//!
//! Mocks are only compiled with the `mock` feature (or in this crate's own
//! tests), and only alongside the real adapter they stand in for: `MemoryBus`
//! needs `kafka`, `MemoryStorage` needs `s3`. Enable `mock` from
//! `[dev-dependencies]` so release builds never ship it.

pub mod bus;
pub mod env;
pub mod storage;
