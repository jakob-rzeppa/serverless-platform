//! `runner-core`: the Wasmtime host pieces used by `runner-http`.
//!
//! - [`Runtime`]: engine + epoch ticker; makes per-instance stores with a
//!   memory cap and deadline. [`run_guest`] awaits a guest and reports
//!   timeouts as [`GuestError::Timeout`].
//! - [`ArtifactCache`]: object store → sha256 check → disk cache → compiled
//!   `Component`.
//!
//! Anything tied to an export (`wasi:http/incoming-handler`) or to a Kafka
//! topic stays in the runner crate.

mod artifacts;
mod runtime;

pub use artifacts::{ArtifactCache, ArtifactError, hex_sha256};
pub use runtime::{GuestError, HasHostState, HostState, Runtime, run_guest};
