//! CLI run log chunks, in the logs bucket.

use std::io::{BufRead, BufReader, Write};

use bytes::Bytes;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use uuid::Uuid;

use crate::{
    logs::LogLine,
    storage::{
        ObjectStorage, StorageError,
        objects::{invalid, segments},
    },
};

/// Object kind: a chunk of CLI run log lines, in the logs bucket, stored as
/// gzip-compressed newline-delimited JSON.
pub struct CliLogChunk;

/// All log chunks of one CLI run: `cli/<app_id>/<run_id>/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CliLogPrefix {
    pub app_id: Uuid,
    pub run_id: Uuid,
}

impl CliLogPrefix {
    pub fn path(&self) -> String {
        format!("cli/{}/{}/", self.app_id, self.run_id)
    }
}

/// `cli/<app_id>/<run_id>/<seq>.log.gz`; `seq` is zero-padded so keys sort
/// in upload order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CliLogKey {
    pub app_id: Uuid,
    pub run_id: Uuid,
    pub seq: u32,
}

impl CliLogKey {
    pub fn path(&self) -> String {
        format!("cli/{}/{}/{:06}.log.gz", self.app_id, self.run_id, self.seq)
    }

    /// Inverse of [`path`](Self::path); `None` for foreign or malformed paths.
    pub fn parse(path: &str) -> Option<Self> {
        match segments(path, "cli/", ".log.gz")?[..] {
            [app, run, seq] if !seq.is_empty() && seq.bytes().all(|b| b.is_ascii_digit()) => {
                Some(Self {
                    app_id: app.parse().ok()?,
                    run_id: run.parse().ok()?,
                    seq: seq.parse().ok()?,
                })
            }
            _ => None,
        }
    }
}

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

impl ObjectStorage<CliLogChunk> {
    /// Upload a chunk of lines; an empty chunk is rejected.
    pub async fn put(&self, key: &CliLogKey, lines: &[LogLine]) -> Result<(), StorageError> {
        let data = encode_log_chunk(lines)?;
        self.backend.put(&key.path(), data).await
    }

    pub async fn get(&self, key: &CliLogKey) -> Result<Vec<LogLine>, StorageError> {
        decode_log_chunk(&self.backend.get(&key.path()).await?)
    }

    /// Chunk keys of one run in upload order. Foreign objects are skipped.
    pub async fn list_log_chunks(
        &self,
        prefix: &CliLogPrefix,
    ) -> Result<Vec<CliLogKey>, StorageError> {
        let paths = self.backend.list(&prefix.path()).await?;
        Ok(paths.iter().filter_map(|p| CliLogKey::parse(p)).collect())
    }

    pub async fn delete(&self, key: &CliLogKey) -> Result<(), StorageError> {
        self.backend.delete(&key.path()).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::logs::Stream;

    #[test]
    fn log_key_round_trip() {
        let key = CliLogKey {
            app_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            seq: 42,
        };
        assert!(key.path().ends_with("/000042.log.gz"));
        assert_eq!(CliLogKey::parse(&key.path()), Some(key));
        let prefix = CliLogPrefix {
            app_id: key.app_id,
            run_id: key.run_id,
        };
        assert!(key.path().starts_with(&prefix.path()));
        assert_eq!(CliLogKey::parse(&key.path().replace("000042", "x")), None);
    }

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
}
