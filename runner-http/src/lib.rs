//! `runner-http`: serves HTTP apps (`wasi:http/incoming-handler`
//! components) by `Host` header.
//!
//! - [`deploy`]: consumes `http.deploy` and keeps the routing table
//!   ([`apps`]) in sync: fetch + verify + compile the component, then swap in
//!   a pre-instantiated `ProxyPre`. Every replica serves every app, so each
//!   replica replays the whole topic on start.
//! - [`server`]: `<subdomain>.<APPS_DOMAIN>` → app → fresh instance per
//!   request, with memory cap, timeout and no outgoing network.
//! - [`logs`]: per-request stdout/stderr, tagged with the request id,
//!   batched per app and uploaded as `http/<app>/<date>/<hour>/…` chunks.
//!
//! runner-http publishes no Kafka messages. Wasmtime host pieces shared with
//! runner-cli live in `runner_core`. The binary (`main.rs`) wires real
//! adapters (Kafka, S3); tests wire the mocks from `common` (feature `mock`).

pub mod apps;
pub mod config;
pub mod deploy;
pub mod guest;
pub mod logs;
pub mod runner;
pub mod server;

pub use config::{Config, Limits};
pub use runner::{Deps, Runner, Settings};
pub use server::REQUEST_ID;
