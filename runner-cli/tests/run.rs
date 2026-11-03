//! End-to-end tests: the runner runs as a background task against the
//! in-memory Kafka (`MemoryBus`) and object store (`MemoryStorage`) from
//! `common` (feature `mock`, enabled in [dev-dependencies]).
//!
//! Each test acts like the control plane: it uploads the component through
//! the typed artifact store, publishes `cli.run` messages and asserts on what
//! arrives on `run.events`, what lands in the logs bucket and which offsets
//! get committed.

use std::{path::PathBuf, process::Command, sync::OnceLock, time::Duration};

use bytes::Bytes;
use common::{
    bus::{
        Publisher, Subscriber,
        backend::memory::MemoryBus,
        messages::{
            CLI_RUN,
            cli_run::CliRun,
            run_event::{RunEvent, RunEventKind},
        },
    },
    logs::{LogLine, Stream},
    storage::{
        ObjectStorage,
        backend::memory::MemoryStorage,
        objects::{
            artifact::{Artifact, ArtifactKey},
            logs::{CliLogChunk, CliLogPrefix},
        },
    },
};
use runner_cli::{Deps, Limits, Runner};
use runner_core::hex_sha256;
use tokio::{sync::oneshot, task::JoinHandle};
use uuid::Uuid;

/// How long to wait for any single event before failing the test.
const EVENT_TIMEOUT: Duration = Duration::from_secs(60);

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
        assert!(status.success(), "building runner-cli/test-guest failed");
        std::fs::read(dir.join("target/wasm32-wasip2/release/test-guest.wasm"))
            .unwrap()
            .into()
    })
}

/// A runner on a background task plus the control plane's view of the bus
/// and buckets.
struct Harness {
    bus: MemoryBus,
    /// Producer for `cli.run`, as the control plane uses it.
    runs: Publisher<CliRun>,
    /// Consumer for `run.events`, as the control plane uses it.
    events: Subscriber<RunEvent>,
    artifacts: ObjectStorage<Artifact>,
    /// Raw artifacts bucket, to plant objects that fail validation.
    raw_artifacts: MemoryStorage,
    logs: ObjectStorage<CliLogChunk>,
    cache: tempfile::TempDir,
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
        let logs = ObjectStorage::memory(&MemoryStorage::new());
        let cache = tempfile::tempdir().unwrap();
        let runner = Runner::new(
            Deps {
                runs: Subscriber::<CliRun>::memory(&bus),
                events: Publisher::memory(&bus),
                artifacts: ObjectStorage::memory(&raw_artifacts),
                logs: logs.clone(),
                cache_dir: cache.path().to_path_buf(),
            },
            Limits {
                log_flush_interval: Duration::from_millis(50),
                ..limits
            },
        )
        .unwrap();
        let (shutdown, stop) = oneshot::channel::<()>();
        let runner = tokio::spawn(async move {
            runner
                .run(async {
                    let _ = stop.await;
                })
                .await
        });
        Self {
            runs: Publisher::memory(&bus),
            events: Subscriber::<RunEvent>::memory(&bus),
            artifacts: ObjectStorage::memory(&raw_artifacts),
            bus,
            raw_artifacts,
            logs,
            cache,
            shutdown: Some(shutdown),
            runner: Some(runner),
        }
    }

    /// Upload the test guest as a new app version and return a matching
    /// `cli.run` message running `args`.
    async fn deploy(&self, args: &[&str]) -> CliRun {
        let (app_id, version_id) = (Uuid::new_v4(), Uuid::new_v4());
        let key = ArtifactKey { app_id, version_id };
        self.artifacts.put(&key, guest()).await.unwrap();
        CliRun {
            run_id: Uuid::new_v4(),
            app_id,
            version_id,
            artifact_key: key,
            sha256: hex_sha256(guest()),
            args: args.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
            timeout_secs: None,
        }
    }

    async fn submit(&self, run: &CliRun) {
        self.runs.publish(run).await.unwrap();
    }

    /// Next event on `run.events` (committed, like the control plane would).
    async fn next_event(&self) -> RunEvent {
        let delivery = tokio::time::timeout(EVENT_TIMEOUT, self.events.recv())
            .await
            .expect("timed out waiting for run.events")
            .unwrap();
        self.events.commit(&delivery).await.unwrap();
        delivery.message.unwrap()
    }

    /// Expect `started` then a final event for `run`; return the final kind.
    async fn outcome(&self, run: &CliRun) -> RunEventKind {
        let started = self.next_event().await;
        assert_eq!(
            (started.run_id, started.app_id, &started.kind),
            (run.run_id, run.app_id, &RunEventKind::Started)
        );
        let done = self.next_event().await;
        assert_eq!((done.run_id, done.app_id), (run.run_id, run.app_id));
        assert!(started.at <= done.at);
        done.kind
    }

    /// Publish `run` and wait for its final event.
    async fn execute(&self, run: &CliRun) -> RunEventKind {
        self.submit(run).await;
        self.outcome(run).await
    }

    /// Every log line of `run`, in upload order.
    async fn log_lines(&self, run: &CliRun) -> Vec<LogLine> {
        let prefix = CliLogPrefix {
            app_id: run.app_id,
            run_id: run.run_id,
        };
        let mut lines = Vec::new();
        for key in self.logs.list_log_chunks(&prefix).await.unwrap() {
            lines.extend(self.logs.get(&key).await.unwrap());
        }
        lines
    }

    /// `(stream, line)` pairs of `run`'s logs.
    async fn logs(&self, run: &CliRun) -> Vec<(Stream, String)> {
        self.log_lines(run)
            .await
            .into_iter()
            .map(|l| (l.stream, l.line))
            .collect()
    }

    /// Request shutdown (like SIGTERM) and wait for the runner to return.
    async fn stop(&mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        tokio::time::timeout(EVENT_TIMEOUT, self.runner.take().unwrap())
            .await
            .expect("runner did not shut down")
            .unwrap()
            .unwrap();
    }
}

fn finished(exit_code: i32) -> RunEventKind {
    RunEventKind::Finished { exit_code }
}

fn failed_with(kind: &RunEventKind, needle: &str) -> bool {
    matches!(kind, RunEventKind::Failed { error } if error.contains(needle))
}

fn stdout(line: &str) -> (Stream, String) {
    (Stream::Stdout, line.to_string())
}

// --- successful runs --------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn run_finishes_and_ships_stdout() {
    let mut h = Harness::start().await;
    let run = h.deploy(&["echo", "Hello", "World"]).await;

    assert_eq!(h.execute(&run).await, finished(0));
    // Logs are complete as soon as the final event is out.
    assert_eq!(h.logs(&run).await, [stdout("Hello"), stdout("World")]);
    let lines = h.log_lines(&run).await;
    assert!(lines.iter().all(|l| l.request_id.is_none()));

    h.stop().await;
    assert_eq!(h.bus.committed(CLI_RUN), Some(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn env_and_stderr_reach_the_guest_and_the_logs() {
    let h = Harness::start().await;
    let mut run = h.deploy(&["env", "GREETING"]).await;
    run.env.insert("GREETING".into(), "hi from env".into());
    assert_eq!(h.execute(&run).await, finished(0));
    assert_eq!(h.logs(&run).await, [stdout("hi from env")]);

    let run = h.deploy(&["stderr", "oops"]).await;
    assert_eq!(h.execute(&run).await, finished(0));
    assert_eq!(h.logs(&run).await, [(Stream::Stderr, "oops".into())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn trailing_partial_line_is_shipped() {
    let h = Harness::start().await;
    let run = h.deploy(&["partial", "no newline"]).await;
    assert_eq!(h.execute(&run).await, finished(0));
    assert_eq!(h.logs(&run).await, [stdout("no newline")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn nonzero_exit_is_finished_not_failed() {
    let h = Harness::start().await;
    // wasip2 guests can only report success or failure: wasi-libc maps any
    // non-zero `exit(n)` to `wasi:cli/exit.exit(err)`, which arrives as 1.
    let run = h.deploy(&["exit", "3"]).await;
    assert_eq!(h.execute(&run).await, finished(1));
    assert!(h.logs(&run).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_are_processed_in_order_with_fresh_instances() {
    let mut h = Harness::start().await;
    let a = h.deploy(&["echo", "A"]).await;
    let mut b = a.clone();
    b.run_id = Uuid::new_v4();
    b.args = vec!["echo".into(), "B".into()];

    // Both queued before the runner gets to them: events must not interleave.
    h.submit(&a).await;
    h.submit(&b).await;
    assert_eq!(h.outcome(&a).await, finished(0));
    assert_eq!(h.outcome(&b).await, finished(0));
    assert_eq!(h.logs(&a).await, [stdout("A")]);
    assert_eq!(h.logs(&b).await, [stdout("B")]);

    h.stop().await;
    assert_eq!(h.bus.committed(CLI_RUN), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn logs_are_shipped_in_chunks_while_running() {
    let h = Harness::start().await; // flushes every 50 ms
    let run = h.deploy(&["tick", "5", "100"]).await;
    assert_eq!(h.execute(&run).await, finished(0));

    let prefix = CliLogPrefix {
        app_id: run.app_id,
        run_id: run.run_id,
    };
    let chunks = h.logs.list_log_chunks(&prefix).await.unwrap();
    assert!(chunks.len() >= 2, "expected several chunks, got {chunks:?}");
    let expected: Vec<_> = (0..5).map(|i| stdout(&format!("tick {i}"))).collect();
    assert_eq!(h.logs(&run).await, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn output_beyond_the_cap_is_dropped_with_a_marker() {
    let h = Harness::with_limits(Limits {
        log_max_bytes: 90,
        ..Limits::default()
    })
    .await;
    let run = h.deploy(&["spam", "9000"]).await;
    assert_eq!(h.execute(&run).await, finished(0));

    let logs = h.logs(&run).await;
    let (marker, output) = logs.split_last().unwrap();
    assert_eq!(output, vec![stdout("xxxxxxxx"); 10]);
    assert_eq!(
        marker,
        &(
            Stream::System,
            "[stdout truncated: output limit reached]".into()
        )
    );
}

// --- artifact cache ----------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn artifact_is_served_from_cache_once_fetched() {
    let h = Harness::start().await;
    let run = h.deploy(&["echo", "first"]).await;
    assert_eq!(h.execute(&run).await, finished(0));
    assert!(h.cache.path().join(format!("{}.wasm", run.sha256)).exists());

    // Gone from the bucket: the second run must come from the disk cache.
    h.artifacts.delete(&run.artifact_key).await.unwrap();
    let mut again = run.clone();
    again.run_id = Uuid::new_v4();
    again.args = vec!["echo".into(), "second".into()];
    assert_eq!(h.execute(&again).await, finished(0));
    assert_eq!(h.logs(&again).await, [stdout("second")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_cache_entry_is_refetched() {
    let h = Harness::start().await;
    let run = h.deploy(&["echo", "ok"]).await;
    std::fs::write(h.cache.path().join(format!("{}.wasm", run.sha256)), b"junk").unwrap();

    assert_eq!(h.execute(&run).await, finished(0));
    let cached = std::fs::read(h.cache.path().join(format!("{}.wasm", run.sha256))).unwrap();
    assert_eq!(hex_sha256(&cached), run.sha256);
}

// --- failed runs: artifact problems ------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn artifact_errors_fail_the_run() {
    let mut h = Harness::start().await;

    let mut mismatch = h.deploy(&[]).await;
    mismatch.sha256 = "0".repeat(64);

    let mut bad_hash = h.deploy(&[]).await;
    bad_hash.sha256 = "../../etc/passwd".into();

    let mut missing = h.deploy(&[]).await;
    missing.artifact_key.version_id = Uuid::new_v4();

    let mut not_wasm = h.deploy(&[]).await;
    h.raw_artifacts
        .insert(&not_wasm.artifact_key.path(), &b"not wasm"[..]);
    not_wasm.sha256 = hex_sha256(b"not wasm");

    for (run, error) in [
        (&mismatch, "sha256 mismatch"),
        (&bad_hash, "invalid sha256"),
        (&missing, "not found"),
        (&not_wasm, "invalid component"),
    ] {
        let kind = h.execute(run).await;
        assert!(
            failed_with(&kind, error),
            "expected {error:?}, got {kind:?}"
        );
        assert!(h.logs(run).await.is_empty());
    }

    // Failed runs are still committed: retrying would fail the same way.
    h.stop().await;
    assert_eq!(h.bus.committed(CLI_RUN), Some(3));
}

// --- failed runs: guest problems -----------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn panic_fails_the_run_and_keeps_its_stderr() {
    let h = Harness::start().await;
    let run = h.deploy(&["panic", "boom"]).await;

    let kind = h.execute(&run).await;
    assert!(failed_with(&kind, "unreachable"), "got {kind:?}");
    let logs = h.logs(&run).await;
    assert!(
        logs.iter()
            .any(|(s, l)| *s == Stream::Stderr && l.contains("boom")),
        "panic message missing from {logs:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cpu_bound_guest_times_out() {
    let h = Harness::start().await;
    let mut run = h.deploy(&["spin"]).await;
    run.timeout_secs = Some(1);
    assert_eq!(
        h.execute(&run).await,
        RunEventKind::Failed {
            error: "timed out after 1s".into()
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn guest_blocked_in_host_call_times_out() {
    let h = Harness::start().await;
    // Sleeping is a host call, so no epoch check runs; the backstop
    // (timeout + 5 s) has to stop it.
    let mut run = h.deploy(&["sleep", "600000"]).await;
    run.timeout_secs = Some(1);
    assert_eq!(
        h.execute(&run).await,
        RunEventKind::Failed {
            error: "timed out after 1s".into()
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn message_timeout_is_capped_by_max_timeout() {
    let h = Harness::with_limits(Limits {
        max_timeout: Duration::from_secs(1),
        ..Limits::default()
    })
    .await;
    let mut run = h.deploy(&["spin"]).await;
    run.timeout_secs = Some(3600);
    assert_eq!(
        h.execute(&run).await,
        RunEventKind::Failed {
            error: "timed out after 1s".into()
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_limit_fails_the_run() {
    let h = Harness::with_limits(Limits {
        memory_bytes: 16 << 20,
        ..Limits::default()
    })
    .await;

    let small = h.deploy(&["alloc", "1"]).await;
    assert_eq!(h.execute(&small).await, finished(0));

    let big = h.deploy(&["alloc", "64"]).await;
    let kind = h.execute(&big).await;
    assert!(matches!(kind, RunEventKind::Failed { .. }), "got {kind:?}");
}

// --- bus semantics ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn poison_message_is_committed_and_skipped() {
    let mut h = Harness::start().await;
    h.bus.push_raw(CLI_RUN, "k", b"{not json");
    let run = h.deploy(&["echo", "after"]).await;

    // No event for the poison message: the first one belongs to `run`.
    assert_eq!(h.execute(&run).await, finished(0));
    h.stop().await;
    assert_eq!(h.bus.committed(CLI_RUN), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_finishes_the_run_in_progress() {
    let mut h = Harness::start().await;
    let run = h.deploy(&["tick", "3", "200"]).await;
    h.submit(&run).await;
    assert_eq!(h.next_event().await.kind, RunEventKind::Started);

    h.stop().await;

    let done = h.next_event().await;
    assert_eq!((done.run_id, done.kind), (run.run_id, finished(0)));
    assert_eq!(h.logs(&run).await.len(), 3);
    assert_eq!(h.bus.committed(CLI_RUN), Some(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_while_idle_returns_without_committing() {
    let mut h = Harness::start().await;
    h.stop().await;
    assert_eq!(h.bus.committed(CLI_RUN), None);
}
