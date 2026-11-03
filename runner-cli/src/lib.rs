//! `runner-cli`: consumes `cli.run`, executes the WASM component's
//! `wasi:cli/run` export in a fresh sandbox, ships logs to the object store
//! and reports status on `run.events`.
//!
//! Events, per valid `cli.run` message, all sharing its `run_id`/`app_id`:
//! `started`, then `finished { exit_code }` or `failed { error }`. Logs are
//! uploaded before the final event, and the offset is committed after it.
//! Undecodable messages are committed without an event. See README → Events.
//!
//! Wasmtime host pieces shared with runner-http live in `runner_core`.
//!
//! The binary (`main.rs`) wires real adapters (Kafka, S3); tests wire the
//! mocks from `common` (feature `mock`).

pub mod config;
pub mod logs;
pub mod runner;
pub mod sandbox;

pub use config::{Config, Limits};
pub use runner::{Deps, Runner};
