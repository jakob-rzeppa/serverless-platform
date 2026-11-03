//! Configuration from environment variables (see `common::env` for `.env` loading).

use std::{path::PathBuf, time::Duration};

use common::{
    bus::messages::cli_run::CliRun,
    env::{EnvError, parse_or, required, s3_bucket_config},
    storage::backend::s3::S3Config,
};

/// Per-run sandbox and logging limits.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Used when the message does not set `timeout_secs`.
    pub default_timeout: Duration,
    /// Upper bound for a message-supplied `timeout_secs`.
    pub max_timeout: Duration,
    /// Linear memory cap per run.
    pub memory_bytes: usize,
    /// Captured bytes per stream (stdout / stderr); excess is dropped.
    pub log_max_bytes: usize,
    /// How often log chunks are uploaded while a run is in progress.
    pub log_flush_interval: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(300),
            max_timeout: Duration::from_secs(3600),
            memory_bytes: 256 << 20,
            log_max_bytes: 10 << 20,
            log_flush_interval: Duration::from_secs(5),
        }
    }
}

impl Limits {
    /// The run's `timeout_secs`, or the default, capped at `max_timeout`.
    pub fn timeout_for(&self, run: &CliRun) -> Duration {
        run.timeout_secs
            .map(Duration::from_secs)
            .unwrap_or(self.default_timeout)
            .min(self.max_timeout)
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    // shared (project-root .env)
    pub kafka_brokers: String,
    pub artifacts: S3Config,
    pub logs: S3Config,
    // runner-cli specific (runner-cli/.env)
    pub kafka_group_id: String,
    pub cache_dir: PathBuf,
    pub limits: Limits,
}

impl Config {
    pub fn from_env() -> Result<Self, EnvError> {
        let d = Limits::default();
        Ok(Self {
            kafka_brokers: required("KAFKA_BROKERS")?,
            // Separate client + optionally separate credentials per bucket;
            // see common::env::s3_bucket_config.
            artifacts: s3_bucket_config("ARTIFACT")?,
            logs: s3_bucket_config("LOG")?,
            kafka_group_id: parse_or("KAFKA_GROUP_ID", "runner-cli".to_string())?,
            cache_dir: parse_or("CACHE_DIR", PathBuf::from("/var/cache/runner-cli"))?,
            limits: Limits {
                default_timeout: secs("RUN_TIMEOUT_SECS", d.default_timeout)?,
                max_timeout: secs("RUN_MAX_TIMEOUT_SECS", d.max_timeout)?,
                memory_bytes: parse_or("MEMORY_LIMIT_MB", d.memory_bytes >> 20)? << 20,
                log_max_bytes: parse_or("LOG_MAX_BYTES", d.log_max_bytes)?,
                log_flush_interval: secs("LOG_FLUSH_SECS", d.log_flush_interval)?,
            },
        })
    }
}

fn secs(name: &str, default: Duration) -> Result<Duration, EnvError> {
    parse_or(name, default.as_secs()).map(Duration::from_secs)
}
