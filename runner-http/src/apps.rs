//! The routing table: which app (and version) is served at which subdomain.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use uuid::Uuid;
use wasmtime_wasi_http::p2::bindings::ProxyPre;

use crate::guest::GuestState;

/// One deployed app version, ready to serve.
pub struct App {
    pub app_id: Uuid,
    pub version_id: Uuid,
    pub subdomain: String,
    pub env: Vec<(String, String)>,
    pub pre: ProxyPre<GuestState>,
}

/// Subdomain → app. Requests clone the `Arc<App>` they resolve, so swapping
/// in a new version never interrupts requests still running on the old one.
#[derive(Default)]
pub struct AppTable {
    inner: RwLock<Tables>,
}

#[derive(Default)]
struct Tables {
    by_subdomain: HashMap<String, Arc<App>>,
    subdomain_of: HashMap<Uuid, String>,
}

impl AppTable {
    pub fn get(&self, subdomain: &str) -> Option<Arc<App>> {
        self.inner
            .read()
            .unwrap()
            .by_subdomain
            .get(subdomain)
            .cloned()
    }

    /// Serve `app` at its subdomain, replacing the app's previous version and
    /// releasing its previous subdomain. If another app held the subdomain,
    /// it is no longer served (the latest deploy wins).
    pub fn insert(&self, app: App) {
        let mut t = self.inner.write().unwrap();
        let app = Arc::new(app);
        if let Some(old) = t.subdomain_of.insert(app.app_id, app.subdomain.clone())
            && old != app.subdomain
        {
            t.by_subdomain.remove(&old);
        }
        if let Some(prev) = t.by_subdomain.insert(app.subdomain.clone(), app.clone())
            && prev.app_id != app.app_id
        {
            tracing::warn!(subdomain = %app.subdomain, previous_app = %prev.app_id, app_id = %app.app_id, "subdomain taken over by another app");
            t.subdomain_of.remove(&prev.app_id);
        }
    }

    /// Stop serving `app_id`. Returns whether it was being served.
    pub fn remove(&self, app_id: Uuid) -> bool {
        let mut t = self.inner.write().unwrap();
        match t.subdomain_of.remove(&app_id) {
            Some(subdomain) => {
                t.by_subdomain.remove(&subdomain);
                true
            }
            None => false,
        }
    }
}

/// Whether `s` is a valid subdomain: one lowercase DNS label.
pub fn valid_subdomain(s: &str) -> bool {
    (1..=63).contains(&s.len())
        && !s.starts_with('-')
        && !s.ends_with('-')
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subdomains() {
        for ok in ["a", "hello", "my-app-2", &"x".repeat(63)] {
            assert!(valid_subdomain(ok), "{ok}");
        }
        for bad in ["", "-a", "a-", "A", "a.b", "a_b", &"x".repeat(64)] {
            assert!(!valid_subdomain(bad), "{bad}");
        }
    }
}
