//! The object kinds stored by the platform, their key layout (see
//! docs/design.md → Object Storage), and the typed [`ObjectStorage`]
//! operations for each kind, one submodule per kind.
//!
//! [`ObjectStorage`]: super::ObjectStorage

use super::StorageError;

pub mod artifact;

fn invalid(e: impl std::fmt::Display) -> StorageError {
    StorageError::Invalid(e.to_string())
}

/// Split `path` into the segments between `prefix` and `suffix` on `/`.
fn segments<'a>(path: &'a str, prefix: &str, suffix: &str) -> Option<Vec<&'a str>> {
    let inner = path.strip_prefix(prefix)?.strip_suffix(suffix)?;
    Some(inner.split('/').collect())
}
