//! Kafka message kinds (JSON encoded), one submodule per kind, each with its
//! `impl Publisher<Kind>` / `impl Subscriber<Kind>` operations.
//!
//! Messages carry references only (ids, object keys, hashes), never the WASM
//! itself. Delivery is at-least-once, so consumers must tolerate duplicates.
//! Every kind is partitioned by `app_id`, so ordering per app is preserved.

pub mod http_deploy;

pub const HTTP_DEPLOY: &str = "http.deploy";
