//! Kafka message kinds (JSON encoded), one submodule per kind, each with its
//! `impl Publisher<Kind>` / `impl Subscriber<Kind>` operations.
//!
//! Messages carry references only (ids, object keys, hashes), never the WASM
//! itself. Delivery is at-least-once, so consumers must tolerate duplicates
//! (deduplicate on `run_id`). Every kind is partitioned by `app_id`, so
//! ordering per app is preserved.

pub mod cli_run;
pub mod http_deploy;
pub mod run_event;

pub const CLI_RUN: &str = "cli.run";
pub const HTTP_DEPLOY: &str = "http.deploy";
pub const RUN_EVENTS: &str = "run.events";
