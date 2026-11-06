//! End-to-end tests: the runner serves a real TCP listener and consumes
//! `http.deploy` from the in-memory Kafka (`MemoryBus`), with artifacts in
//! the in-memory object store (`MemoryStorage`) from `common` (feature
//! `mock`, enabled in [dev-dependencies]).
//!
//! Each test acts like the control plane plus a client: it uploads the
//! component through the typed artifact store, publishes `http.deploy`
//! messages, sends HTTP requests by `Host` and checks responses and committed
//! offsets.

use std::{
    net::SocketAddr,
    path::PathBuf,
    process::Command,
    sync::{
        OnceLock,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use common::{
    bus::{
        Publisher, Subscriber,
        backend::memory::MemoryBus,
        messages::{HTTP_DEPLOY, http_deploy::HttpDeploy},
    },
    storage::{
        ObjectStorage,
        backend::memory::MemoryStorage,
        objects::artifact::{Artifact, ArtifactKey},
    },
};
use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use runner_core::hex_sha256;
use runner_http::{Deps, Limits, REQUEST_ID, Runner, Settings};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use uuid::Uuid;

const DOMAIN: &str = "apps.test";
const WAIT: Duration = Duration::from_secs(60);

/// Build `test-guest` for wasm32-wasip2 once per test binary.
fn guest() -> &'static Bytes {
    static WASM: OnceLock<Bytes> = OnceLock::new();
    WASM.get_or_init(|| {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-guest");
        let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(["build", "--release", "--target", "wasm32-wasip2"])
            .arg("--manifest-path")
            .arg(dir.join("Cargo.toml"))
            .status()
            .expect("run cargo");
        assert!(status.success(), "building runner-http/test-guest failed");
        std::fs::read(dir.join("target/wasm32-wasip2/release/test-guest.wasm"))
            .unwrap()
            .into()
    })
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: String,
}

impl Reply {
    fn request_id(&self) -> &str {
        self.headers[REQUEST_ID].to_str().unwrap()
    }
}

/// A runner on a background task plus the control plane's and a client's
/// view of it.
struct Harness {
    bus: MemoryBus,
    /// Producer for `http.deploy`, as the control plane uses it.
    deploys: Publisher<HttpDeploy>,
    /// Messages put on `http.deploy` so far.
    sent: AtomicI64,
    artifacts: ObjectStorage<Artifact>,
    /// Raw artifacts bucket, to plant objects that fail validation.
    raw_artifacts: MemoryStorage,
    addr: SocketAddr,
    _cache: tempfile::TempDir,
    shutdown: Option<oneshot::Sender<()>>,
    runner: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Harness {
    async fn start() -> Self {
        Self::with_limits(Limits::default()).await
    }

    async fn with_limits(limits: Limits) -> Self {
        let bus = MemoryBus::new();
        let raw_artifacts = MemoryStorage::new();
        let cache = tempfile::tempdir().unwrap();
        let runner = Runner::new(
            Deps {
                deploys: Subscriber::<HttpDeploy>::memory(&bus),
                artifacts: ObjectStorage::memory(&raw_artifacts),
                cache_dir: cache.path().to_path_buf(),
            },
            Settings {
                apps_domain: DOMAIN.into(),
                limits,
            },
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, stop) = oneshot::channel::<()>();
        let runner = tokio::spawn(runner.run(listener, async {
            let _ = stop.await;
        }));
        Self {
            deploys: Publisher::memory(&bus),
            sent: AtomicI64::new(0),
            artifacts: ObjectStorage::memory(&raw_artifacts),
            bus,
            raw_artifacts,
            addr,
            _cache: cache,
            shutdown: Some(shutdown),
            runner: Some(runner),
        }
    }

    /// Upload the test guest as a new version of `app_id` and return a
    /// matching deploy message.
    async fn version(&self, app_id: Uuid, subdomain: &str, env: &[(&str, &str)]) -> HttpDeploy {
        let artifact_key = ArtifactKey {
            app_id,
            version_id: Uuid::new_v4(),
        };
        self.artifacts.put(&artifact_key, guest()).await.unwrap();
        HttpDeploy::Deploy {
            app_id,
            version_id: artifact_key.version_id,
            subdomain: subdomain.into(),
            artifact_key,
            sha256: hex_sha256(guest()),
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    /// Deploy a new app at `subdomain`; returns its id once it is served.
    async fn deploy(&self, subdomain: &str, env: &[(&str, &str)]) -> Uuid {
        let app_id = Uuid::new_v4();
        let msg = self.version(app_id, subdomain, env).await;
        self.apply(&msg).await;
        app_id
    }

    /// Publish `msg` and wait until the runner has processed (committed) it.
    async fn apply(&self, msg: &HttpDeploy) {
        self.deploys.publish(msg).await.unwrap();
        self.wait_processed().await;
    }

    async fn wait_processed(&self) {
        let last = self.sent.fetch_add(1, Ordering::SeqCst);
        tokio::time::timeout(WAIT, async {
            while self.bus.committed(HTTP_DEPLOY) < Some(last) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("deploy message not processed");
    }

    async fn request(&self, method: Method, host: &str, path: &str, body: &str) -> Reply {
        send(self.addr, method, host, path, body).await
    }

    async fn get(&self, host: &str, path: &str) -> Reply {
        self.request(Method::GET, host, path, "").await
    }

    /// Request shutdown (like SIGTERM) and wait for the runner to return.
    async fn stop(&mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        tokio::time::timeout(WAIT, self.runner.take().unwrap())
            .await
            .expect("runner did not shut down")
            .unwrap()
            .unwrap();
    }
}

/// One request on a fresh connection.
async fn send(addr: SocketAddr, method: Method, host: &str, path: &str, body: &str) -> Reply {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(conn);
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", host)
        .header(REQUEST_ID, "client-supplied")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let resp = tokio::time::timeout(WAIT, sender.send_request(req))
        .await
        .expect("request timed out")
        .unwrap();
    let (parts, body) = resp.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body: String::from_utf8(body.to_vec()).unwrap(),
    }
}

fn host(subdomain: &str) -> String {
    format!("{subdomain}.{DOMAIN}")
}

// --- routing -----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn serves_deployed_app_by_host() {
    let h = Harness::start().await;
    h.deploy("hello", &[("GREETING", "hi")]).await;

    let r = h.get(&host("hello"), "/env/GREETING").await;
    assert_eq!((r.status, r.body.as_str()), (StatusCode::OK, "hi"));
    let r = h
        .request(Method::POST, &host("hello"), "/echo", "payload")
        .await;
    assert_eq!(r.body, "POST payload");
    // Case and port do not matter.
    let r = h.get("HELLO.Apps.Test:8080", "/env/GREETING").await;
    assert_eq!(r.body, "hi");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_hosts_get_404_and_healthz_answers_outside_the_apps_domain() {
    let h = Harness::start().await;
    h.deploy("hello", &[]).await;

    for host in [
        "missing.apps.test",
        "a.hello.apps.test",
        "hello.apps.test.evil",
        "evilapps.test",
        "apps.test",
        "127.0.0.1",
    ] {
        let r = h.get(host, "/env/X").await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{host}");
        assert!(!r.request_id().is_empty());
    }
    assert_eq!(
        h.get("10.0.0.7:8080", "/healthz").await.status,
        StatusCode::OK
    );
    // Inside the apps domain, /healthz belongs to the app.
    assert_eq!(
        h.get(&host("hello"), "/healthz").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        h.get(&host("missing"), "/healthz").await.status,
        StatusCode::NOT_FOUND
    );
}

// --- deploys -------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn redeploy_switches_version_and_subdomain() {
    let h = Harness::start().await;
    let app = Uuid::new_v4();
    h.apply(&h.version(app, "v", &[("VERSION", "1")]).await)
        .await;
    assert_eq!(h.get(&host("v"), "/env/VERSION").await.body, "1");

    h.apply(&h.version(app, "v", &[("VERSION", "2")]).await)
        .await;
    assert_eq!(h.get(&host("v"), "/env/VERSION").await.body, "2");

    h.apply(&h.version(app, "renamed", &[("VERSION", "3")]).await)
        .await;
    assert_eq!(h.get(&host("renamed"), "/env/VERSION").await.body, "3");
    assert_eq!(
        h.get(&host("v"), "/env/VERSION").await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_stops_serving() {
    let h = Harness::start().await;
    let app = h.deploy("gone", &[]).await;
    assert_eq!(h.get(&host("gone"), "/env/X").await.status, StatusCode::OK);

    h.apply(&HttpDeploy::Remove { app_id: app }).await;
    assert_eq!(
        h.get(&host("gone"), "/env/X").await.status,
        StatusCode::NOT_FOUND
    );
    // Removing an unknown app is a no-op.
    h.apply(&HttpDeploy::Remove {
        app_id: Uuid::new_v4(),
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn latest_deploy_takes_over_a_subdomain() {
    let h = Harness::start().await;
    let first = h.deploy("shared", &[("WHO", "first")]).await;
    h.deploy("shared", &[("WHO", "second")]).await;
    assert_eq!(h.get(&host("shared"), "/env/WHO").await.body, "second");

    // The first app lost its subdomain: removing it must not affect the second.
    h.apply(&HttpDeploy::Remove { app_id: first }).await;
    assert_eq!(h.get(&host("shared"), "/env/WHO").await.body, "second");
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_deploy_keeps_the_previous_version() {
    let h = Harness::start().await;
    let app = Uuid::new_v4();
    h.apply(&h.version(app, "stable", &[("VERSION", "1")]).await)
        .await;

    // Hash mismatch.
    let mut bad = h.version(app, "stable", &[("VERSION", "2")]).await;
    if let HttpDeploy::Deploy { sha256, .. } = &mut bad {
        *sha256 = "0".repeat(64);
    }
    h.apply(&bad).await;

    // Not a wasm component.
    let mut not_wasm = h.version(app, "stable", &[("VERSION", "3")]).await;
    if let HttpDeploy::Deploy {
        artifact_key,
        sha256,
        ..
    } = &mut not_wasm
    {
        h.raw_artifacts
            .insert(&artifact_key.path(), &b"not wasm"[..]);
        *sha256 = hex_sha256(b"not wasm");
    }
    h.apply(&not_wasm).await;

    // Invalid subdomain.
    h.apply(&h.version(app, "Bad.Name", &[("VERSION", "4")]).await)
        .await;

    assert_eq!(h.get(&host("stable"), "/env/VERSION").await.body, "1");
}

#[tokio::test(flavor = "multi_thread")]
async fn poison_deploy_message_is_committed_and_skipped() {
    let h = Harness::start().await;
    h.bus.push_raw(HTTP_DEPLOY, "k", b"{not json");
    h.wait_processed().await;
    h.deploy("after", &[]).await;
    assert_eq!(h.get(&host("after"), "/env/X").await.status, StatusCode::OK);
}

// --- requests ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn request_id_replaces_the_client_header() {
    let h = Harness::start().await;
    h.deploy("ids", &[]).await;

    let r = h.get(&host("ids"), "/header/x-request-id").await;
    assert_ne!(r.body, "client-supplied");
    assert_eq!(r.body, r.request_id());
    let other = h.get(&host("ids"), "/header/x-request-id").await;
    assert_ne!(other.request_id(), r.request_id());
}

#[tokio::test(flavor = "multi_thread")]
async fn guest_status_codes_are_passed_through() {
    let h = Harness::start().await;
    h.deploy("status", &[]).await;
    assert_eq!(
        h.get(&host("status"), "/status/418").await.status.as_u16(),
        418
    );
    assert_eq!(
        h.get(&host("status"), "/nope").await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_requests_get_fresh_instances() {
    let h = Harness::start().await;
    h.deploy("busy", &[]).await;
    // Every request sleeps 300 ms; run sequentially, 10 would take 3 s.
    let started = std::time::Instant::now();
    let tasks: Vec<_> = (0..10)
        .map(|_| {
            let addr = h.addr;
            tokio::spawn(
                async move { send(addr, Method::GET, &host("busy"), "/sleep/300", "").await },
            )
        })
        .collect();
    for task in tasks {
        let r = task.await.unwrap();
        assert_eq!((r.status, r.body.as_str()), (StatusCode::OK, "slept"));
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn outgoing_requests_are_denied() {
    let h = Harness::start().await;
    h.deploy("net", &[]).await;
    // The guest's `Client::send` fails (the guest answers 502 with the
    // error). The host hook never opens a connection; wstd may surface the
    // denial as an error on the dropped request body rather than as
    // `HTTP-request-denied`, so only the failure itself is asserted.
    let r = h.get(&host("net"), "/fetch").await;
    assert_eq!(r.status, StatusCode::BAD_GATEWAY, "{}", r.body);
}

// --- guest failures ------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn panic_and_handler_error_are_500() {
    let h = Harness::start().await;
    h.deploy("fail", &[]).await;

    let panicked = h.get(&host("fail"), "/panic").await;
    assert_eq!(panicked.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(panicked.body, "internal error\n"); // no guest details leak
    let errored = h.get(&host("fail"), "/error").await;
    assert_eq!(errored.status, StatusCode::INTERNAL_SERVER_ERROR);
    // A failed request does not affect the next one.
    assert_eq!(h.get(&host("fail"), "/env/X").await.status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn cpu_bound_request_times_out_with_504() {
    let h = Harness::with_limits(Limits {
        request_timeout: Duration::from_secs(1),
        ..Limits::default()
    })
    .await;
    h.deploy("slow", &[]).await;
    let r = h.get(&host("slow"), "/spin").await;
    assert_eq!(r.status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(h.get(&host("slow"), "/env/X").await.status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_limit_fails_the_request() {
    let h = Harness::with_limits(Limits {
        memory_bytes: 16 << 20,
        ..Limits::default()
    })
    .await;
    h.deploy("mem", &[]).await;
    assert_eq!(h.get(&host("mem"), "/alloc/1").await.status, StatusCode::OK);
    assert_eq!(
        h.get(&host("mem"), "/alloc/64").await.status,
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

// --- shutdown --------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_finishes_in_flight_requests() {
    let mut h = Harness::start().await;
    h.deploy("drain", &[]).await;

    let stream = tokio::net::TcpStream::connect(h.addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(conn);
    let req = Request::get("/sleep/500")
        .header("host", host("drain"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let in_flight = tokio::spawn(sender.send_request(req));
    tokio::time::sleep(Duration::from_millis(100)).await;

    h.stop().await;
    let resp = in_flight.await.unwrap().unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, "slept");
    // No longer accepting connections.
    assert!(tokio::net::TcpStream::connect(h.addr).await.is_err());
}
