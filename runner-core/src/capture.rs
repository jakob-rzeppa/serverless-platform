//! Captured guest stdout/stderr.
//!
//! Unlike wasmtime's `MemoryOutputPipe` (which traps the guest once full),
//! this silently drops output beyond the cap and records that it truncated,
//! so a noisy guest keeps running but cannot exhaust memory or the store.
//! The runner's log shipper drains the buffer periodically.

use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use common::logs::{LogLine, Stream};
use tokio::io::AsyncWrite;
use wasmtime_wasi::{
    cli::{IsTerminal, StdoutStream},
    p2::{OutputStream, Pollable, StreamResult},
};

/// Capped in-memory sink for guest stdout or stderr. Clones share the same
/// buffer, so the runner keeps one clone to drain while the guest writes to
/// another.
#[derive(Clone)]
pub struct CapturePipe {
    state: Arc<Mutex<State>>,
}

struct State {
    buf: BytesMut,
    /// Bytes accepted over the whole run (drained or not).
    total: usize,
    cap: usize,
    truncated: bool,
}

impl CapturePipe {
    /// Pipe that accepts at most `cap` bytes over its lifetime; draining
    /// does not free up room.
    pub fn new(cap: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                buf: BytesMut::new(),
                total: 0,
                cap,
                truncated: false,
            })),
        }
    }

    fn append(&self, data: &[u8]) {
        let mut s = self.state.lock().unwrap();
        let n = data.len().min(s.cap - s.total);
        s.buf.extend_from_slice(&data[..n]);
        s.total += n;
        if n < data.len() {
            s.truncated = true;
        }
    }

    /// Drain complete lines (everything up to and including the last `\n`).
    fn take_lines(&self) -> Bytes {
        let mut s = self.state.lock().unwrap();
        match s.buf.iter().rposition(|&b| b == b'\n') {
            Some(i) => s.buf.split_to(i + 1).freeze(),
            None => Bytes::new(),
        }
    }

    /// Drain everything, including a trailing partial line.
    fn take_all(&self) -> Bytes {
        self.state.lock().unwrap().buf.split().freeze()
    }

    /// Whether any output was dropped because the cap was reached.
    fn truncated(&self) -> bool {
        self.state.lock().unwrap().truncated
    }

    /// Drain captured output as log lines tagged with `stream`. Without
    /// `last`, only complete lines are drained. With `last`, a trailing
    /// partial line is drained too, followed by a truncation marker if output
    /// was dropped.
    pub fn drain_log_lines(
        &self,
        stream: Stream,
        last: bool,
        ts: DateTime<Utc>,
        request_id: Option<&str>,
    ) -> Vec<LogLine> {
        let data = if last {
            self.take_all()
        } else {
            self.take_lines()
        };
        let line = |stream, line: String| LogLine {
            ts,
            stream,
            line,
            request_id: request_id.map(str::to_string),
        };
        let mut lines: Vec<_> = String::from_utf8_lossy(&data)
            .lines()
            .map(|l| line(stream, l.to_string()))
            .collect();
        if last && self.truncated() {
            lines.push(line(
                Stream::System,
                format!("[{stream:?} truncated: output limit reached]").to_lowercase(),
            ));
        }
        lines
    }
}

impl IsTerminal for CapturePipe {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for CapturePipe {
    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(self.clone())
    }
    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for CapturePipe {
    async fn ready(&mut self) {}
}

impl OutputStream for CapturePipe {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        self.append(&bytes);
        Ok(())
    }
    fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }
    fn check_write(&mut self) -> StreamResult<usize> {
        // Always writable; excess is dropped in `append`.
        Ok(64 * 1024)
    }
}

impl AsyncWrite for CapturePipe {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.append(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_and_drains_lines() {
        let p = CapturePipe::new(10);
        p.append(b"ab\ncd");
        assert_eq!(p.take_lines(), "ab\n");
        assert_eq!(p.take_lines(), "");
        p.append(b"efghijkl"); // only 5 bytes of room left
        assert!(p.truncated());
        assert_eq!(p.take_all(), "cdefghi");
    }

    #[test]
    fn drains_log_lines_with_truncation_marker() {
        let p = CapturePipe::new(8);
        p.append(b"one\ntwo\nthree"); // "three" exceeds the cap
        let ts = Utc::now();

        let lines = p.drain_log_lines(Stream::Stderr, false, ts, Some("req-1"));
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1].line, "two");
        assert_eq!(lines[1].stream, Stream::Stderr);
        assert_eq!(lines[1].request_id.as_deref(), Some("req-1"));

        let lines = p.drain_log_lines(Stream::Stderr, true, ts, None);
        let text: Vec<_> = lines.iter().map(|l| (l.stream, l.line.as_str())).collect();
        assert_eq!(
            text,
            [(Stream::System, "[stderr truncated: output limit reached]")]
        );
    }
}
