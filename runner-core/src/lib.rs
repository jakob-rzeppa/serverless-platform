//! `runner-core`: the Wasmtime host pieces shared by `runner-cli` and
//! `runner-http`.
//!
//! - [`Runtime`]: engine + epoch ticker; makes per-instance stores with a
//!   memory cap and deadline. [`run_guest`] awaits a guest and reports
//!   timeouts as [`GuestError::Timeout`].
//! - [`ArtifactCache`]: object store → sha256 check → disk cache → compiled
//!   `Component`.
//! - [`CapturePipe`]: capped stdout/stderr capture, drained as log lines.
//!
//! Anything tied to one export (`wasi:cli/run`, `wasi:http/incoming-handler`),
//! to a Kafka topic or to a log key layout stays in the runner crates.

mod artifacts;
mod capture;
mod runtime;

pub use artifacts::{ArtifactCache, ArtifactError, hex_sha256};
pub use capture::CapturePipe;
pub use runtime::{GuestError, HasHostState, HostState, Runtime, run_guest};
