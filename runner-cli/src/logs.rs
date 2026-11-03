//! Uploads a run's captured output as numbered log chunks:
//! `cli/<app_id>/<run_id>/<seq>.log.gz`. Encoding is owned by
//! `common::storage::objects::logs::CliLogChunk`.

use std::time::Duration;

use chrono::Utc;
use common::{
    logs::Stream,
    storage::{
        ObjectStorage,
        objects::logs::{CliLogChunk, CliLogKey},
    },
};
use tokio::{sync::oneshot, task::JoinHandle, time::Instant};
use uuid::Uuid;

use runner_core::CapturePipe;

/// Captures one run's stdout/stderr and uploads it every `every` on a
/// background task. Flushing runs on its own task because a CPU-bound guest
/// occupies the current task's thread until it yields or hits the deadline.
pub struct LogShipper {
    stdout: CapturePipe,
    stderr: CapturePipe,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl LogShipper {
    /// Start capturing (at most `cap` bytes per stream) and uploading.
    /// Must be called inside a Tokio runtime.
    pub fn start(
        storage: ObjectStorage<CliLogChunk>,
        app_id: Uuid,
        run_id: Uuid,
        cap: usize,
        every: Duration,
    ) -> Self {
        let stdout = CapturePipe::new(cap);
        let stderr = CapturePipe::new(cap);
        let mut chunks = Chunks {
            storage,
            app_id,
            run_id,
            stdout: stdout.clone(),
            stderr: stderr.clone(),
            seq: 0,
        };
        let (stop, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(Instant::now() + every, every);
            loop {
                tokio::select! {
                    _ = interval.tick() => chunks.flush(false).await,
                    _ = &mut stop_rx => return chunks.flush(true).await,
                }
            }
        });
        Self {
            stdout,
            stderr,
            stop,
            task,
        }
    }

    /// Pipe to hand to the guest as stdout.
    pub fn stdout(&self) -> CapturePipe {
        self.stdout.clone()
    }

    /// Pipe to hand to the guest as stderr.
    pub fn stderr(&self) -> CapturePipe {
        self.stderr.clone()
    }

    /// Stop periodic uploads and upload everything that is left, including
    /// partial lines and a truncation marker. All logs are uploaded once
    /// this returns.
    pub async fn finish(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

/// Upload state, owned by the shipper's background task.
struct Chunks {
    storage: ObjectStorage<CliLogChunk>,
    app_id: Uuid,
    run_id: Uuid,
    stdout: CapturePipe,
    stderr: CapturePipe,
    seq: u32,
}

impl Chunks {
    /// Upload complete lines captured since the last flush (with `last`, all
    /// remaining output). Upload errors are logged, not returned: losing
    /// logs must not fail the run.
    async fn flush(&mut self, last: bool) {
        let ts = Utc::now();
        let mut lines = self.stdout.drain_log_lines(Stream::Stdout, last, ts, None);
        lines.extend(self.stderr.drain_log_lines(Stream::Stderr, last, ts, None));
        if lines.is_empty() {
            return;
        }

        let key = CliLogKey {
            app_id: self.app_id,
            run_id: self.run_id,
            seq: self.seq,
        };
        match self.storage.put(&key, &lines).await {
            Ok(()) => self.seq += 1,
            Err(error) => {
                tracing::warn!(key = %key.path(), %error, dropped_lines = lines.len(), "log chunk upload failed")
            }
        }
    }
}
