//! Configuration from environment variables (see `common::env` for `.env` loading).

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use common::{
    env::{EnvError, parse_or, required, s3_bucket_config},
    storage::backend::s3::S3Config,
};

/// Per-request sandbox limits.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Wall-clock limit for one request, including streaming the body.
    pub request_timeout: Duration,
    /// Linear memory cap per request instance.
    pub memory_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            memory_bytes: 128 << 20,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    // shared (project-root .env)
    pub kafka_brokers: String,
    pub artifacts: S3Config,
    // runner-http specific (runner-http/.env)
    pub listen_addr: SocketAddr,
    /// Apps are served at `<subdomain>.<apps_domain>`.
    pub apps_domain: String,
    /// Name of this replica (the pod name in Kubernetes).
    pub pod_name: String,
    pub cache_dir: PathBuf,
    pub limits: Limits,
}

impl Config {
    pub fn from_env() -> Result<Self, EnvError> {
        let d = Limits::default();
        let pod_name = std::env::var("POD_NAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "local".to_string());
        Ok(Self {
            kafka_brokers: required("KAFKA_BROKERS")?,
            artifacts: s3_bucket_config("ARTIFACT")?,
            listen_addr: parse_or("LISTEN_ADDR", SocketAddr::from(([0, 0, 0, 0], 8080)))?,
            apps_domain: required("APPS_DOMAIN")?
                .trim_matches('.')
                .to_ascii_lowercase(),
            pod_name,
            cache_dir: parse_or("CACHE_DIR", PathBuf::from("/var/cache/runner-http"))?,
            limits: Limits {
                request_timeout: secs("REQUEST_TIMEOUT_SECS", d.request_timeout)?,
                memory_bytes: parse_or("MEMORY_LIMIT_MB", d.memory_bytes >> 20)? << 20,
            },
        })
    }
}

fn secs(name: &str, default: Duration) -> Result<Duration, EnvError> {
    parse_or(name, default.as_secs()).map(Duration::from_secs)
}
