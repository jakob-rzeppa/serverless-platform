//! Log chunks in the logs bucket: per HTTP app and hour ([`HttpLogChunk`]),
//! stored as gzip-compressed newline-delimited JSON [`LogLine`]s.

use std::io::{BufRead, BufReader, Write};

use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use uuid::Uuid;

use crate::{
    logs::LogLine,
    storage::{
        ObjectStorage, StorageError,
        objects::{invalid, segments},
    },
};

fn encode_log_chunk(lines: &[LogLine]) -> Result<Bytes, StorageError> {
    if lines.is_empty() {
        return Err(invalid("empty log chunk"));
    }
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    for line in lines {
        serde_json::to_writer(&mut enc, line).map_err(invalid)?;
        enc.write_all(b"\n").map_err(invalid)?;
    }
    Ok(enc.finish().map_err(invalid)?.into())
}

fn decode_log_chunk(data: &[u8]) -> Result<Vec<LogLine>, StorageError> {
    BufReader::new(GzDecoder::new(data))
        .lines()
        .filter(|l| !matches!(l, Ok(s) if s.is_empty()))
        .map(|l| serde_json::from_str(&l.map_err(invalid)?).map_err(invalid))
        .collect()
}

/// Object kind: a chunk of an HTTP app's log lines, in the logs bucket,
/// stored as gzip-compressed newline-delimited JSON.
pub struct HttpLogChunk;

/// All HTTP log chunks of one app for one hour: `http/<app_id>/<yyyy-mm-dd>/<hh>/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HttpLogPrefix {
    pub app_id: Uuid,
    /// Start of the hour (UTC). [`HttpLogPrefix::new`] truncates for you.
    pub hour: DateTime<Utc>,
}

impl HttpLogPrefix {
    /// Prefix of the hour containing `at`.
    pub fn new(app_id: Uuid, at: DateTime<Utc>) -> Self {
        let hour = at
            .date_naive()
            .and_hms_opt(at.hour(), 0, 0)
            .expect("valid hour")
            .and_utc();
        Self { app_id, hour }
    }

    pub fn path(&self) -> String {
        format!("http/{}/{}/", self.app_id, self.hour.format("%Y-%m-%d/%H"))
    }
}

/// `http/<app_id>/<yyyy-mm-dd>/<hh>/<writer>-<seq>.log.gz`.
///
/// `writer` identifies the runner process that wrote the chunk (pod name
/// plus a per-start id), so concurrent replicas never overwrite each other.
/// It may only contain ASCII letters, digits, `.`, `_` and `-`. `seq` is
/// zero-padded so one writer's keys sort in upload order.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HttpLogKey {
    pub prefix: HttpLogPrefix,
    pub writer: String,
    pub seq: u32,
}

impl HttpLogKey {
    pub fn path(&self) -> String {
        format!(
            "{}{}-{:06}.log.gz",
            self.prefix.path(),
            self.writer,
            self.seq
        )
    }

    /// Inverse of [`path`](Self::path); `None` for foreign or malformed paths.
    pub fn parse(path: &str) -> Option<Self> {
        let [app, date, hh, file] = segments(path, "http/", ".log.gz")?[..] else {
            return None;
        };
        let (writer, seq) = file.rsplit_once('-')?;
        if !valid_writer(writer) || seq.is_empty() || !seq.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let hour: u32 = hh.parse().ok().filter(|_| hh.len() == 2)?;
        let hour = NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .ok()?
            .and_hms_opt(hour, 0, 0)?
            .and_utc();
        Some(Self {
            prefix: HttpLogPrefix {
                app_id: app.parse().ok()?,
                hour,
            },
            writer: writer.to_string(),
            seq: seq.parse().ok()?,
        })
    }
}

/// Whether `writer` is usable as the writer segment of an [`HttpLogKey`].
pub fn valid_writer(writer: &str) -> bool {
    !writer.is_empty()
        && writer
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

impl ObjectStorage<HttpLogChunk> {
    /// Upload a chunk of lines; an empty chunk or an invalid writer is
    /// rejected.
    pub async fn put(&self, key: &HttpLogKey, lines: &[LogLine]) -> Result<(), StorageError> {
        if !valid_writer(&key.writer) {
            return Err(invalid(format!("invalid log writer {:?}", key.writer)));
        }
        let data = encode_log_chunk(lines)?;
        self.backend.put(&key.path(), data).await
    }

    pub async fn get(&self, key: &HttpLogKey) -> Result<Vec<LogLine>, StorageError> {
        decode_log_chunk(&self.backend.get(&key.path()).await?)
    }

    /// Chunk keys of one app and hour, sorted by writer, then upload order.
    /// Foreign objects are skipped.
    pub async fn list_log_chunks(
        &self,
        prefix: &HttpLogPrefix,
    ) -> Result<Vec<HttpLogKey>, StorageError> {
        let paths = self.backend.list(&prefix.path()).await?;
        Ok(paths.iter().filter_map(|p| HttpLogKey::parse(p)).collect())
    }

    pub async fn delete(&self, key: &HttpLogKey) -> Result<(), StorageError> {
        self.backend.delete(&key.path()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs::Stream;

    #[test]
    fn log_chunk_round_trip() {
        let lines = vec![
            LogLine {
                ts: Utc::now(),
                stream: Stream::Stdout,
                line: "hi".into(),
                request_id: None,
            },
            LogLine {
                ts: Utc::now(),
                stream: Stream::Stderr,
                line: "{\"x\":1}".into(),
                request_id: Some("r1".into()),
            },
        ];
        let data = encode_log_chunk(&lines).unwrap();
        assert_eq!(decode_log_chunk(&data).unwrap(), lines);
        assert!(matches!(
            encode_log_chunk(&[]),
            Err(StorageError::Invalid(_))
        ));
        assert!(matches!(
            decode_log_chunk(b"garbage"),
            Err(StorageError::Invalid(_))
        ));
    }

    #[test]
    fn http_log_key_round_trip() {
        let at = "2026-11-04T09:41:07.5Z".parse::<DateTime<Utc>>().unwrap();
        let prefix = HttpLogPrefix::new(Uuid::new_v4(), at);
        assert_eq!(
            prefix.hour,
            "2026-11-04T09:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        let key = HttpLogKey {
            prefix,
            writer: "runner-http-7d9f.ab12cd34".into(),
            seq: 7,
        };
        assert_eq!(
            key.path(),
            format!(
                "http/{}/2026-11-04/09/runner-http-7d9f.ab12cd34-000007.log.gz",
                prefix.app_id
            )
        );
        assert!(key.path().starts_with(&prefix.path()));
        assert_eq!(HttpLogKey::parse(&key.path()), Some(key.clone()));
        for bad in [
            key.path().replace("000007", "x"),
            key.path().replace("/09/", "/9/"),
            key.path().replace("/09/", "/24/"),
            key.path().replace("runner-http-7d9f.ab12cd34-", ""),
        ] {
            assert_eq!(HttpLogKey::parse(&bad), None, "{bad}");
        }
    }

    #[tokio::test]
    async fn http_log_put_rejects_invalid_writer() {
        let raw = crate::storage::backend::memory::MemoryStorage::new();
        let logs = ObjectStorage::<HttpLogChunk>::memory(&raw);
        let line = LogLine {
            ts: Utc::now(),
            stream: Stream::Stdout,
            line: "hi".into(),
            request_id: Some("r".into()),
        };
        let mut key = HttpLogKey {
            prefix: HttpLogPrefix::new(Uuid::new_v4(), line.ts),
            writer: "../x".into(),
            seq: 0,
        };
        assert!(matches!(
            logs.put(&key, std::slice::from_ref(&line)).await,
            Err(StorageError::Invalid(_))
        ));
        key.writer = "pod-1".into();
        logs.put(&key, std::slice::from_ref(&line)).await.unwrap();
        assert_eq!(
            logs.list_log_chunks(&key.prefix).await.unwrap(),
            [key.clone()]
        );
        assert_eq!(logs.get(&key).await.unwrap(), [line]);
    }
}
