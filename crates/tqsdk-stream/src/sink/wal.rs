use std::collections::BTreeSet;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tqsdk_core::{CommitResult, CommitScope};

use crate::{Result, StreamFacadeError};

/// Fsync policy for a JSONL stream sink WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSinkWalFsyncPolicy {
    Never,
    EveryRecord,
}

/// Stable record kind written by the JSONL sink WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamSinkWalRecordKind {
    Received,
    AttemptFailed,
    Delivered,
    Lagged,
    FlushSucceeded,
    FlushFailed,
}

/// JSONL sink WAL record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSinkWalRecord {
    pub sink: String,
    pub kind: StreamSinkWalRecordKind,
    pub revision: Option<u64>,
    pub attempt: u32,
    pub scope: Option<String>,
    pub domains: Vec<String>,
    pub paths: Vec<String>,
    pub error: Option<String>,
}

/// Local JSONL WAL compaction policy for managed stream sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSinkWalCompaction {
    retain_revisions_from: Option<u64>,
    retain_non_revision_records: bool,
}

/// Report returned after compacting a JSONL stream sink WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSinkWalCompactionReport {
    original_records: u64,
    retained_records: u64,
    dropped_records: u64,
}

/// Scanner for a local JSONL stream sink WAL.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamSinkWalRecovery;

/// Typed recovery report derived from a local JSONL stream sink WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSinkWalRecoveryReport {
    total_records: u64,
    delivered_revisions: Vec<u64>,
    pending_revisions: Vec<u64>,
    failed_revisions: Vec<u64>,
    lagged_records: u64,
    flush_failed_records: u64,
}

impl Default for StreamSinkWalCompaction {
    fn default() -> Self {
        Self {
            retain_revisions_from: None,
            retain_non_revision_records: true,
        }
    }
}

impl StreamSinkWalCompaction {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn retain_revisions_from(mut self, revision: u64) -> Self {
        self.retain_revisions_from = Some(revision);
        self
    }

    #[must_use]
    pub fn retain_non_revision_records(mut self, retain: bool) -> Self {
        self.retain_non_revision_records = retain;
        self
    }

    pub fn compact_jsonl(self, path: impl AsRef<Path>) -> Result<StreamSinkWalCompactionReport> {
        compact_jsonl_wal(path.as_ref(), self)
    }
}

impl StreamSinkWalCompactionReport {
    #[must_use]
    pub fn original_records(&self) -> u64 {
        self.original_records
    }

    #[must_use]
    pub fn retained_records(&self) -> u64 {
        self.retained_records
    }

    #[must_use]
    pub fn dropped_records(&self) -> u64 {
        self.dropped_records
    }
}

impl StreamSinkWalRecovery {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn scan_jsonl(self, path: impl AsRef<Path>) -> Result<StreamSinkWalRecoveryReport> {
        scan_jsonl_wal(path.as_ref())
    }
}

impl StreamSinkWalRecoveryReport {
    #[must_use]
    pub fn total_records(&self) -> u64 {
        self.total_records
    }

    #[must_use]
    pub fn delivered_revisions(&self) -> &[u64] {
        &self.delivered_revisions
    }

    #[must_use]
    pub fn pending_revisions(&self) -> &[u64] {
        &self.pending_revisions
    }

    #[must_use]
    pub fn failed_revisions(&self) -> &[u64] {
        &self.failed_revisions
    }

    #[must_use]
    pub fn last_delivered_revision(&self) -> Option<u64> {
        self.delivered_revisions.last().copied()
    }

    #[must_use]
    pub fn lagged_records(&self) -> u64 {
        self.lagged_records
    }

    #[must_use]
    pub fn flush_failed_records(&self) -> u64 {
        self.flush_failed_records
    }

    #[must_use]
    pub fn has_incomplete_deliveries(&self) -> bool {
        !self.pending_revisions.is_empty()
    }
}

fn compact_jsonl_wal(
    path: &Path,
    policy: StreamSinkWalCompaction,
) -> Result<StreamSinkWalCompactionReport> {
    let input = std::fs::File::open(path).map_err(|error| StreamFacadeError::Io {
        operation: "open stream sink jsonl wal for compaction",
        message: error.to_string(),
    })?;
    let temp_path = compaction_temp_path(path);
    let mut output = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|error| StreamFacadeError::Io {
                operation: "create compacted stream sink jsonl wal",
                message: error.to_string(),
            })?,
    );

    let mut original_records = 0;
    let mut retained_records = 0;
    for line in std::io::BufReader::new(input).lines() {
        let line = line.map_err(|error| StreamFacadeError::Io {
            operation: "read stream sink jsonl wal for compaction",
            message: error.to_string(),
        })?;
        if line.trim().is_empty() {
            continue;
        }

        let record: StreamSinkWalRecord =
            serde_json::from_str(&line).map_err(|error| StreamFacadeError::Io {
                operation: "parse stream sink jsonl wal record for compaction",
                message: error.to_string(),
            })?;
        original_records += 1;
        if policy.retains(&record) {
            serde_json::to_writer(&mut output, &record).map_err(|error| StreamFacadeError::Io {
                operation: "serialize compacted stream sink jsonl wal record",
                message: error.to_string(),
            })?;
            output
                .write_all(b"\n")
                .map_err(|error| StreamFacadeError::Io {
                    operation: "write compacted stream sink jsonl wal record",
                    message: error.to_string(),
                })?;
            retained_records += 1;
        }
    }
    output.flush().map_err(|error| StreamFacadeError::Io {
        operation: "flush compacted stream sink jsonl wal",
        message: error.to_string(),
    })?;
    drop(output);

    std::fs::rename(&temp_path, path).map_err(|error| StreamFacadeError::Io {
        operation: "replace stream sink jsonl wal after compaction",
        message: error.to_string(),
    })?;

    Ok(StreamSinkWalCompactionReport {
        original_records,
        retained_records,
        dropped_records: original_records - retained_records,
    })
}

fn scan_jsonl_wal(path: &Path) -> Result<StreamSinkWalRecoveryReport> {
    let input = std::fs::File::open(path).map_err(|error| StreamFacadeError::Io {
        operation: "open stream sink jsonl wal for recovery scan",
        message: error.to_string(),
    })?;
    let mut total_records = 0;
    let mut started_revisions = BTreeSet::new();
    let mut delivered_revisions = BTreeSet::new();
    let mut failed_revisions = BTreeSet::new();
    let mut lagged_records = 0;
    let mut flush_failed_records = 0;

    for line in std::io::BufReader::new(input).lines() {
        let line = line.map_err(|error| StreamFacadeError::Io {
            operation: "read stream sink jsonl wal for recovery scan",
            message: error.to_string(),
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let record: StreamSinkWalRecord =
            serde_json::from_str(&line).map_err(|error| StreamFacadeError::Io {
                operation: "parse stream sink jsonl wal record for recovery scan",
                message: error.to_string(),
            })?;
        total_records += 1;
        match record.kind {
            StreamSinkWalRecordKind::Received => {
                if let Some(revision) = record.revision {
                    started_revisions.insert(revision);
                }
            }
            StreamSinkWalRecordKind::AttemptFailed => {
                if let Some(revision) = record.revision {
                    started_revisions.insert(revision);
                    failed_revisions.insert(revision);
                }
            }
            StreamSinkWalRecordKind::Delivered => {
                if let Some(revision) = record.revision {
                    delivered_revisions.insert(revision);
                }
            }
            StreamSinkWalRecordKind::Lagged => {
                lagged_records += 1;
            }
            StreamSinkWalRecordKind::FlushSucceeded => {}
            StreamSinkWalRecordKind::FlushFailed => {
                flush_failed_records += 1;
            }
        }
    }

    let pending_revisions = started_revisions
        .difference(&delivered_revisions)
        .copied()
        .collect();

    Ok(StreamSinkWalRecoveryReport {
        total_records,
        delivered_revisions: delivered_revisions.into_iter().collect(),
        pending_revisions,
        failed_revisions: failed_revisions.into_iter().collect(),
        lagged_records,
        flush_failed_records,
    })
}

impl StreamSinkWalCompaction {
    fn retains(&self, record: &StreamSinkWalRecord) -> bool {
        match record.revision {
            Some(revision) => self
                .retain_revisions_from
                .is_none_or(|minimum| revision >= minimum),
            None => self.retain_non_revision_records,
        }
    }
}

fn compaction_temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("stream-sink-wal.jsonl");
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.with_file_name(format!(
        ".{file_name}.compact-{}-{unique}",
        std::process::id()
    ))
}

impl StreamSinkWalRecord {
    pub(super) fn from_commit(
        name: &str,
        kind: StreamSinkWalRecordKind,
        commit: &CommitResult,
        attempt: u32,
        error: Option<String>,
    ) -> Self {
        Self {
            sink: name.to_string(),
            kind,
            revision: Some(commit.revision.get()),
            attempt,
            scope: Some(commit_scope(commit.scope).to_string()),
            domains: commit
                .domains
                .iter()
                .map(|domain| domain.as_str().to_string())
                .collect(),
            paths: commit
                .changes
                .path_hits
                .iter()
                .map(|path| path.segments().join("/"))
                .collect(),
            error,
        }
    }

    pub(super) fn lagged(name: &str, skipped: u64) -> Self {
        Self {
            sink: name.to_string(),
            kind: StreamSinkWalRecordKind::Lagged,
            revision: None,
            attempt: 0,
            scope: None,
            domains: Vec::new(),
            paths: Vec::new(),
            error: Some(format!(
                "stream sink lagged and skipped {skipped} commit(s)"
            )),
        }
    }

    pub(super) fn flush(name: &str, kind: StreamSinkWalRecordKind, error: Option<String>) -> Self {
        Self {
            sink: name.to_string(),
            kind,
            revision: None,
            attempt: 0,
            scope: None,
            domains: Vec::new(),
            paths: Vec::new(),
            error,
        }
    }
}

fn commit_scope(scope: CommitScope) -> &'static str {
    match scope {
        CommitScope::InitialReady => "initial_ready",
        CommitScope::RealtimeUpdate => "realtime_update",
        CommitScope::ResyncRecovery => "resync_recovery",
        CommitScope::ReplayStep => "replay_step",
        CommitScope::QueryRefresh => "query_refresh",
        CommitScope::SessionTransition => "session_transition",
    }
}
