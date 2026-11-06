//! `.env` loading for local development.
//!
//! Precedence, highest first:
//!
//! 1. variables already set in the process environment
//! 2. `<crate>/.env.local`
//! 3. `<crate>/.env`
//! 4. `<project-root>/.env.local`
//! 5. `<project-root>/.env`
//!
//! The project root holds variables shared by all services (Kafka, S3, …);
//! the crate directory holds service-specific ones. `.env.local` files are
//! gitignored and meant for personal overrides. `dotenvy` never overwrites a
//! variable that is already set, so the files are loaded highest-first.
//!
//! Missing files are skipped, so in containers only real env vars are used.

use std::path::{Path, PathBuf};

/// Load the `.env` files for the crate at `crate_dir` (pass
/// `env!("CARGO_MANIFEST_DIR")`). Returns the files that were loaded.
pub fn load_dotenv(crate_dir: impl AsRef<Path>) -> Vec<PathBuf> {
    let crate_dir = crate_dir.as_ref();
    let root = crate_dir.parent().unwrap_or(crate_dir);
    let candidates = [
        crate_dir.join(".env.local"),
        crate_dir.join(".env"),
        root.join(".env.local"),
        root.join(".env"),
    ];
    candidates
        .into_iter()
        .filter(|p| dotenvy::from_path(p).is_ok())
        .collect()
}

/// Read a required env var.
pub fn required(name: &str) -> Result<String, EnvError> {
    std::env::var(name).map_err(|_| EnvError::Missing(name.to_string()))
}

/// Read an optional env var, parsing it, falling back to `default`.
pub fn parse_or<T: std::str::FromStr>(name: &str, default: T) -> Result<T, EnvError> {
    match std::env::var(name) {
        Ok(v) => v.parse().map_err(|_| EnvError::Invalid {
            name: name.to_string(),
            value: v,
        }),
        Err(_) => Ok(default),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EnvError {
    #[error("missing required env var {0}")]
    Missing(String),
    #[error("invalid value {value:?} for env var {name}")]
    Invalid { name: String, value: String },
    #[error("env var {set} is set but {missing} is not; set both or neither")]
    Incomplete { set: String, missing: String },
}

/// S3 settings for one bucket, read from the environment.
///
/// For `bucket = "ARTIFACT"` this reads:
///
/// - `ARTIFACT_BUCKET` (required)
/// - `ARTIFACT_S3_ACCESS_KEY` + `ARTIFACT_S3_SECRET_KEY` (optional, both or
///   neither), falling back to the shared `S3_ACCESS_KEY` + `S3_SECRET_KEY`
/// - `S3_ENDPOINT` (required) and `S3_REGION` (default `garage`), shared
///
/// Each bucket gets its own client anyway (the bucket is part of the client
/// config), so per-bucket credentials cost nothing and let production scope
/// keys tightly, e.g. read-only artifacts for runners.
/// Local dev just sets the shared pair.
#[cfg(feature = "s3")]
pub fn s3_bucket_config(bucket: &str) -> Result<crate::storage::backend::s3::S3Config, EnvError> {
    s3_bucket_config_from(bucket, |name| std::env::var(name).ok())
}

#[cfg(feature = "s3")]
fn s3_bucket_config_from(
    bucket: &str,
    var: impl Fn(&str) -> Option<String>,
) -> Result<crate::storage::backend::s3::S3Config, EnvError> {
    let get = |name: &str| var(name).ok_or_else(|| EnvError::Missing(name.to_string()));
    let (key_var, secret_var) = (
        format!("{bucket}_S3_ACCESS_KEY"),
        format!("{bucket}_S3_SECRET_KEY"),
    );
    let (access_key, secret_key) = match (var(&key_var), var(&secret_var)) {
        (Some(k), Some(s)) => (k, s),
        (None, None) => (get("S3_ACCESS_KEY")?, get("S3_SECRET_KEY")?),
        (Some(_), None) => {
            return Err(EnvError::Incomplete {
                set: key_var,
                missing: secret_var,
            });
        }
        (None, Some(_)) => {
            return Err(EnvError::Incomplete {
                set: secret_var,
                missing: key_var,
            });
        }
    };
    Ok(crate::storage::backend::s3::S3Config {
        endpoint: get("S3_ENDPOINT")?,
        region: var("S3_REGION").unwrap_or_else(|| "garage".to_string()),
        bucket: get(&format!("{bucket}_BUCKET"))?,
        access_key,
        secret_key,
    })
}

#[cfg(all(test, feature = "s3"))]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    const SHARED: [(&str, &str); 4] = [
        ("S3_ENDPOINT", "http://localhost:3900"),
        ("S3_ACCESS_KEY", "shared-key"),
        ("S3_SECRET_KEY", "shared-secret"),
        ("ARTIFACT_BUCKET", "apps"),
    ];

    #[test]
    fn falls_back_to_shared_credentials() {
        let cfg = s3_bucket_config_from("ARTIFACT", lookup(&SHARED)).unwrap();
        assert_eq!(
            (cfg.bucket.as_str(), cfg.region.as_str()),
            ("apps", "garage")
        );
        assert_eq!(
            (cfg.access_key.as_str(), cfg.secret_key.as_str()),
            ("shared-key", "shared-secret")
        );
    }

    #[test]
    fn per_bucket_credentials_override() {
        let mut vars = SHARED.to_vec();
        vars.extend([
            ("ARTIFACT_S3_ACCESS_KEY", "artifact-key"),
            ("ARTIFACT_S3_SECRET_KEY", "artifact-secret"),
        ]);
        let cfg = s3_bucket_config_from("ARTIFACT", lookup(&vars)).unwrap();
        assert_eq!(
            (cfg.access_key.as_str(), cfg.secret_key.as_str()),
            ("artifact-key", "artifact-secret")
        );
    }

    #[test]
    fn half_set_pair_is_an_error() {
        let mut vars = SHARED.to_vec();
        vars.push(("ARTIFACT_S3_ACCESS_KEY", "artifact-key"));
        assert!(matches!(
            s3_bucket_config_from("ARTIFACT", lookup(&vars)),
            Err(EnvError::Incomplete { .. })
        ));
    }

    #[test]
    fn per_bucket_pair_works_without_shared_pair() {
        let vars = [
            ("S3_ENDPOINT", "http://x"),
            ("ARTIFACT_BUCKET", "apps"),
            ("ARTIFACT_S3_ACCESS_KEY", "k"),
            ("ARTIFACT_S3_SECRET_KEY", "s"),
        ];
        assert!(s3_bucket_config_from("ARTIFACT", lookup(&vars)).is_ok());
    }
}
