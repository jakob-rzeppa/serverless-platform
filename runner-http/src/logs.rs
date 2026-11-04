//! Per-app log batching and upload:
//! `http/<app_id>/<yyyy-mm-dd>/<hh>/<writer>-<seq>.log.gz`. Encoding is owned
//! by `common::storage::objects::logs::HttpLogChunk`.
//!
//! Each request's output is captured separately and handed to [`LogBook`]
//! once the request is done, tagged with its `request_id`. The log book
//! buffers lines per app and uploads them every flush interval, or earlier
//! once an app's buffer reaches the flush size. Each app may keep a limited
//! number of bytes per UTC hour; beyond that, lines are dropped and one
//! marker line is written for the rest of the hour.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
    time::Duration,
};

use chrono::{DateTime, Utc};
use common::{
    logs::{LogLine, Stream},
    storage::{
        ObjectStorage,
        objects::logs::{HttpLogChunk, HttpLogKey, HttpLogPrefix},
    },
};
use tokio::{sync::Notify, time::Instant};
use uuid::Uuid;

const HOURLY_LIMIT_MARKER: &str = "[log limit for this hour reached: further output dropped]";

pub struct LogBook {
    storage: ObjectStorage<HttpLogChunk>,
    writer: String,
    max_bytes_per_hour: usize,
    flush_bytes: usize,
    apps: Mutex<HashMap<Uuid, AppLog>>,
    /// Signalled when an app's buffer reached `flush_bytes`.
    full: Notify,
}

#[derive(Default)]
struct AppLog {
    pending: Vec<LogLine>,
    pending_bytes: usize,
    /// Hour the byte budget applies to, bytes used in it, and whether the
    /// limit marker was written.
    budget_hour: Option<DateTime<Utc>>,
    budget_used: usize,
    budget_marked: bool,
    /// Next chunk number of this app (per writer).
    seq: u32,
}

impl LogBook {
    /// `writer` must be unique per process; see
    /// [`HttpLogKey`].
    pub fn new(
        storage: ObjectStorage<HttpLogChunk>,
        writer: String,
        max_bytes_per_hour: usize,
        flush_bytes: usize,
    ) -> Self {
        Self {
            storage,
            writer,
            max_bytes_per_hour,
            flush_bytes,
            apps: Mutex::default(),
            full: Notify::new(),
        }
    }

    /// Buffer `lines` of `app_id` for upload, applying the hourly budget.
    /// Platform lines (`Stream::System`) do not count against the budget.
    pub fn record(&self, app_id: Uuid, lines: Vec<LogLine>) {
        if lines.is_empty() {
            return;
        }
        let mut apps = self.apps.lock().unwrap();
        let log = apps.entry(app_id).or_default();
        for line in lines {
            let hour = HttpLogPrefix::new(app_id, line.ts).hour;
            if log.budget_hour != Some(hour) {
                log.budget_hour = Some(hour);
                log.budget_used = 0;
                log.budget_marked = false;
            }
            if line.stream != Stream::System {
                if log.budget_used + line.line.len() > self.max_bytes_per_hour {
                    if !log.budget_marked {
                        log.budget_marked = true;
                        log.push(LogLine {
                            ts: line.ts,
                            stream: Stream::System,
                            line: HOURLY_LIMIT_MARKER.into(),
                            request_id: None,
                        });
                    }
                    continue;
                }
                log.budget_used += line.line.len();
            }
            log.push(line);
        }
        if log.pending_bytes >= self.flush_bytes {
            self.full.notify_one();
        }
    }

    /// Upload everything buffered, one chunk per app and hour. Upload errors
    /// are logged, not returned: losing logs must not affect serving.
    pub async fn flush(&self) {
        let chunks: Vec<(HttpLogKey, Vec<LogLine>)> = {
            let mut apps = self.apps.lock().unwrap();
            let mut chunks = Vec::new();
            for (&app_id, log) in apps.iter_mut() {
                let mut by_hour: BTreeMap<DateTime<Utc>, Vec<LogLine>> = BTreeMap::new();
                for line in log.pending.drain(..) {
                    let hour = HttpLogPrefix::new(app_id, line.ts).hour;
                    by_hour.entry(hour).or_default().push(line);
                }
                log.pending_bytes = 0;
                for (hour, lines) in by_hour {
                    let key = HttpLogKey {
                        prefix: HttpLogPrefix { app_id, hour },
                        writer: self.writer.clone(),
                        seq: log.seq,
                    };
                    log.seq += 1;
                    chunks.push((key, lines));
                }
            }
            chunks
        };
        for (key, lines) in chunks {
            if let Err(error) = self.storage.put(&key, &lines).await {
                tracing::warn!(key = %key.path(), %error, dropped_lines = lines.len(), "log chunk upload failed");
            }
        }
    }

    /// Flush every `every`, or earlier when an app's buffer is full, until
    /// `stop` resolves. Does not flush on stop; call [`flush`](Self::flush)
    /// once the last request has finished.
    pub async fn run(&self, every: Duration, stop: impl Future<Output = ()>) {
        tokio::pin!(stop);
        let mut interval = tokio::time::interval_at(Instant::now() + every, every);
        loop {
            tokio::select! {
                _ = &mut stop => return,
                _ = interval.tick() => {}
                _ = self.full.notified() => {}
            }
            self.flush().await;
        }
    }
}

impl AppLog {
    fn push(&mut self, line: LogLine) {
        self.pending_bytes += line.line.len();
        self.pending.push(line);
    }
}

#[cfg(test)]
mod tests {
    use common::storage::backend::memory::MemoryStorage;

    use super::*;

    fn line(ts: &str, stream: Stream, text: &str) -> LogLine {
        LogLine {
            ts: ts.parse().unwrap(),
            stream,
            line: text.into(),
            request_id: Some("r".into()),
        }
    }

    #[tokio::test]
    async fn hourly_budget_and_chunk_per_hour() {
        let raw = MemoryStorage::new();
        let storage = ObjectStorage::<HttpLogChunk>::memory(&raw);
        let book = LogBook::new(storage.clone(), "pod".into(), 10, usize::MAX);
        let app = Uuid::new_v4();

        book.record(
            app,
            vec![
                line("2026-11-04T09:59:00Z", Stream::Stdout, "123456"),
                line("2026-11-04T09:59:01Z", Stream::Stderr, "7890"),
                line("2026-11-04T09:59:02Z", Stream::Stdout, "dropped"),
                line("2026-11-04T09:59:03Z", Stream::Stdout, "also dropped"),
                line("2026-11-04T09:59:04Z", Stream::System, "platform line"),
                // New hour: fresh budget.
                line("2026-11-04T10:00:00Z", Stream::Stdout, "next hour"),
            ],
        );
        book.flush().await;

        let nine = HttpLogPrefix::new(app, "2026-11-04T09:30:00Z".parse().unwrap());
        let ten = HttpLogPrefix::new(app, "2026-11-04T10:30:00Z".parse().unwrap());
        let keys = storage.list_log_chunks(&nine).await.unwrap();
        assert_eq!(keys.len(), 1);
        let text: Vec<_> = storage
            .get(&keys[0])
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.line)
            .collect();
        assert_eq!(
            text,
            ["123456", "7890", HOURLY_LIMIT_MARKER, "platform line"]
        );

        let keys = storage.list_log_chunks(&ten).await.unwrap();
        assert_eq!((keys.len(), keys[0].seq), (1, 1));
        assert_eq!(storage.get(&keys[0]).await.unwrap()[0].line, "next hour");

        // Nothing buffered: no new objects.
        book.flush().await;
        assert_eq!(raw.keys().len(), 2);
    }
}
