//! Log line types. Chunks of lines are stored as
//! [`storage::objects::logs::CliLogChunk`](crate::storage::objects::logs::CliLogChunk),
//! which owns the byte encoding.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stream {
    Stdout,
    Stderr,
    /// Lines written by the platform itself (e.g. truncation markers).
    System,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogLine {
    pub ts: DateTime<Utc>,
    pub stream: Stream,
    pub line: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}
