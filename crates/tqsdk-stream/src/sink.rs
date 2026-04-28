#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::BTreeSet;
use std::future::Future;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{CommitStream, Result, StreamFacadeError};

pub type StreamSinkFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

/// User-provided commit sink run by `tqsdk-stream`.
pub trait CommitSink: Send + 'static {
    fn handle_commit(&mut self, commit: tqsdk_core::CommitResult) -> StreamSinkFuture;

    fn flush(&mut self) -> StreamSinkFuture {
        Box::pin(async { Ok(()) })
    }
}

/// Options for a managed stream sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSinkOptions {
    retry_policy: StreamSinkRetryPolicy,
    wal_path: Option<PathBuf>,
    wal_fsync_policy: StreamSinkWalFsyncPolicy,
}

/// Retry policy applied inside a managed stream sink task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSinkRetryPolicy {
    max_attempts: u32,
    retry_delay: Duration,
}

/// Fsync policy for a JSONL stream sink WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSinkWalFsyncPolicy {
    Never,
    EveryRecord,
}

/// Runtime status for a managed stream sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSinkStatus {
    Running,
    Stopping,
    Stopped,
    Failed,
}

/// Point-in-time stats for a managed stream sink.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamSinkStats {
    processed_commits: u64,
    lagged_commits: u64,
    errors: u64,
    retry_attempts: u64,
    wal_records: u64,
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

/// Handle returned after spawning a managed commit sink.
pub struct StreamSinkHandle {
    name: String,
    shared: Arc<Mutex<StreamSinkState>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<StreamSinkShutdownReport>,
}

/// Report returned when a managed commit sink has shut down.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSinkShutdownReport {
    name: String,
    status: StreamSinkStatus,
    stats: StreamSinkStats,
    last_error: Option<StreamFacadeError>,
    flushed: bool,
}

#[derive(Debug, Clone)]
struct StreamSinkState {
    status: StreamSinkStatus,
    stats: StreamSinkStats,
    last_error: Option<StreamFacadeError>,
}

impl<F> CommitSink for F
where
    F: FnMut(tqsdk_core::CommitResult) -> StreamSinkFuture + Send + 'static,
{
    fn handle_commit(&mut self, commit: tqsdk_core::CommitResult) -> StreamSinkFuture {
        self(commit)
    }
}

impl Default for StreamSinkOptions {
    fn default() -> Self {
        Self {
            retry_policy: StreamSinkRetryPolicy::none(),
            wal_path: None,
            wal_fsync_policy: StreamSinkWalFsyncPolicy::Never,
        }
    }
}

impl StreamSinkOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn retry_policy(mut self, retry_policy: StreamSinkRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    #[must_use]
    pub fn jsonl_wal(mut self, path: impl Into<PathBuf>) -> Self {
        self.wal_path = Some(path.into());
        self
    }

    #[must_use]
    pub fn wal_fsync_policy(mut self, policy: StreamSinkWalFsyncPolicy) -> Self {
        self.wal_fsync_policy = policy;
        self
    }

    #[must_use]
    pub fn fsync_policy(&self) -> StreamSinkWalFsyncPolicy {
        self.wal_fsync_policy
    }
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

impl StreamSinkRetryPolicy {
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            retry_delay: Duration::ZERO,
        }
    }

    pub fn limited(max_attempts: u32) -> Result<Self> {
        if max_attempts == 0 {
            return Err(StreamFacadeError::InvalidState(
                "stream sink retry max attempts must be greater than zero",
            ));
        }
        Ok(Self {
            max_attempts,
            retry_delay: Duration::ZERO,
        })
    }

    #[must_use]
    pub fn fixed_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }

    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    #[must_use]
    pub fn retry_delay(&self) -> Duration {
        self.retry_delay
    }
}

impl StreamSinkHandle {
    pub(crate) fn spawn<S>(
        name: String,
        commits: CommitStream,
        sink: S,
        options: StreamSinkOptions,
    ) -> Result<Self>
    where
        S: CommitSink,
    {
        let wal = StreamSinkWalWriter::open(options.wal_path.as_deref(), options.wal_fsync_policy)?;
        let shared = Arc::new(Mutex::new(StreamSinkState {
            status: StreamSinkStatus::Running,
            stats: StreamSinkStats::default(),
            last_error: None,
        }));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(run_sink(
            name.clone(),
            commits,
            sink,
            shutdown_rx,
            Arc::clone(&shared),
            options.retry_policy,
            wal,
        ));

        Ok(Self {
            name,
            shared,
            shutdown_tx: Some(shutdown_tx),
            task,
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn status(&self) -> StreamSinkStatus {
        self.shared
            .lock()
            .expect("stream sink state mutex poisoned")
            .status
    }

    #[must_use]
    pub fn stats(&self) -> StreamSinkStats {
        self.shared
            .lock()
            .expect("stream sink state mutex poisoned")
            .stats
    }

    #[must_use]
    pub fn last_error(&self) -> Option<StreamFacadeError> {
        self.shared
            .lock()
            .expect("stream sink state mutex poisoned")
            .last_error
            .clone()
    }

    pub async fn shutdown(mut self) -> Result<StreamSinkShutdownReport> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.task
            .await
            .map_err(|_| StreamFacadeError::InvalidState("managed stream sink task failed to join"))
    }
}

impl StreamSinkStats {
    #[must_use]
    pub fn processed_commits(&self) -> u64 {
        self.processed_commits
    }

    #[must_use]
    pub fn lagged_commits(&self) -> u64 {
        self.lagged_commits
    }

    #[must_use]
    pub fn errors(&self) -> u64 {
        self.errors
    }

    #[must_use]
    pub fn retry_attempts(&self) -> u64 {
        self.retry_attempts
    }

    #[must_use]
    pub fn wal_records(&self) -> u64 {
        self.wal_records
    }
}

impl StreamSinkShutdownReport {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn status(&self) -> StreamSinkStatus {
        self.status
    }

    #[must_use]
    pub fn stats(&self) -> StreamSinkStats {
        self.stats
    }

    #[must_use]
    pub fn last_error(&self) -> Option<&StreamFacadeError> {
        self.last_error.as_ref()
    }

    #[must_use]
    pub fn flushed(&self) -> bool {
        self.flushed
    }
}

async fn run_sink<S>(
    name: String,
    mut commits: CommitStream,
    mut sink: S,
    mut shutdown_rx: oneshot::Receiver<()>,
    shared: Arc<Mutex<StreamSinkState>>,
    retry_policy: StreamSinkRetryPolicy,
    mut wal: Option<StreamSinkWalWriter>,
) -> StreamSinkShutdownReport
where
    S: CommitSink,
{
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                set_status(&shared, StreamSinkStatus::Stopping);
                let flushed = flush_sink(&name, &shared, &mut wal, &mut sink).await;
                if current_status(&shared) != StreamSinkStatus::Failed {
                    set_status(&shared, StreamSinkStatus::Stopped);
                }
                return report(name, shared, flushed);
            }
            update = commits.next() => {
                match update {
                    Some(Ok(commit)) => {
                        if let Err(error) = deliver_commit(
                            &name,
                            &shared,
                            &mut wal,
                            &mut sink,
                            commit,
                            retry_policy,
                        ).await {
                            record_error(&shared, error);
                            set_status(&shared, StreamSinkStatus::Failed);
                            return report(name, shared, false);
                        }
                    }
                    Some(Err(StreamFacadeError::Lagged { skipped })) => {
                        add_lagged(&shared, skipped);
                        let record = StreamSinkWalRecord::lagged(&name, skipped);
                        if let Err(error) = write_wal_record(&shared, &mut wal, &record) {
                            record_error(&shared, error);
                            set_status(&shared, StreamSinkStatus::Failed);
                            return report(name, shared, false);
                        }
                    }
                    Some(Err(StreamFacadeError::Closed)) | None => {
                        set_status(&shared, StreamSinkStatus::Stopping);
                        let flushed = flush_sink(&name, &shared, &mut wal, &mut sink).await;
                        if current_status(&shared) != StreamSinkStatus::Failed {
                            set_status(&shared, StreamSinkStatus::Stopped);
                        }
                        return report(name, shared, flushed);
                    }
                    Some(Err(error)) => {
                        record_error(&shared, error);
                        set_status(&shared, StreamSinkStatus::Failed);
                        return report(name, shared, false);
                    }
                }
            }
        }
    }
}

async fn deliver_commit<S>(
    name: &str,
    shared: &Arc<Mutex<StreamSinkState>>,
    wal: &mut Option<StreamSinkWalWriter>,
    sink: &mut S,
    commit: tqsdk_core::CommitResult,
    retry_policy: StreamSinkRetryPolicy,
) -> Result<()>
where
    S: CommitSink,
{
    let mut attempt = 1;
    write_wal_record(
        shared,
        wal,
        &StreamSinkWalRecord::from_commit(
            name,
            StreamSinkWalRecordKind::Received,
            &commit,
            attempt,
            None,
        ),
    )?;

    loop {
        match sink.handle_commit(commit.clone()).await {
            Ok(()) => {
                clear_error(shared);
                write_wal_record(
                    shared,
                    wal,
                    &StreamSinkWalRecord::from_commit(
                        name,
                        StreamSinkWalRecordKind::Delivered,
                        &commit,
                        attempt,
                        None,
                    ),
                )?;
                increment_processed(shared);
                return Ok(());
            }
            Err(error) => {
                let failed = StreamSinkWalRecord::from_commit(
                    name,
                    StreamSinkWalRecordKind::AttemptFailed,
                    &commit,
                    attempt,
                    Some(error.to_string()),
                );
                write_wal_record(shared, wal, &failed)?;

                if attempt >= retry_policy.max_attempts {
                    return Err(error);
                }

                record_error(shared, error);
                increment_retry_attempts(shared);
                if !retry_policy.retry_delay.is_zero() {
                    tokio::time::sleep(retry_policy.retry_delay).await;
                }
                attempt += 1;
            }
        }
    }
}

async fn flush_sink<S>(
    name: &str,
    shared: &Arc<Mutex<StreamSinkState>>,
    wal: &mut Option<StreamSinkWalWriter>,
    sink: &mut S,
) -> bool
where
    S: CommitSink,
{
    match sink.flush().await {
        Ok(()) => {
            let record =
                StreamSinkWalRecord::flush(name, StreamSinkWalRecordKind::FlushSucceeded, None);
            match write_wal_record(shared, wal, &record) {
                Ok(()) => true,
                Err(error) => {
                    record_error(shared, error);
                    set_status(shared, StreamSinkStatus::Failed);
                    false
                }
            }
        }
        Err(error) => {
            let record = StreamSinkWalRecord::flush(
                name,
                StreamSinkWalRecordKind::FlushFailed,
                Some(error.to_string()),
            );
            let _ = write_wal_record(shared, wal, &record);
            record_error(shared, error);
            set_status(shared, StreamSinkStatus::Failed);
            false
        }
    }
}

fn current_status(shared: &Arc<Mutex<StreamSinkState>>) -> StreamSinkStatus {
    shared
        .lock()
        .expect("stream sink state mutex poisoned")
        .status
}

fn set_status(shared: &Arc<Mutex<StreamSinkState>>, status: StreamSinkStatus) {
    shared
        .lock()
        .expect("stream sink state mutex poisoned")
        .status = status;
}

fn increment_processed(shared: &Arc<Mutex<StreamSinkState>>) {
    let mut state = shared.lock().expect("stream sink state mutex poisoned");
    state.stats.processed_commits += 1;
}

fn add_lagged(shared: &Arc<Mutex<StreamSinkState>>, skipped: u64) {
    let mut state = shared.lock().expect("stream sink state mutex poisoned");
    state.stats.lagged_commits += skipped;
}

fn increment_retry_attempts(shared: &Arc<Mutex<StreamSinkState>>) {
    let mut state = shared.lock().expect("stream sink state mutex poisoned");
    state.stats.retry_attempts += 1;
}

fn increment_wal_records(shared: &Arc<Mutex<StreamSinkState>>) {
    let mut state = shared.lock().expect("stream sink state mutex poisoned");
    state.stats.wal_records += 1;
}

fn record_error(shared: &Arc<Mutex<StreamSinkState>>, error: StreamFacadeError) {
    let mut state = shared.lock().expect("stream sink state mutex poisoned");
    state.stats.errors += 1;
    state.last_error = Some(error);
}

fn clear_error(shared: &Arc<Mutex<StreamSinkState>>) {
    shared
        .lock()
        .expect("stream sink state mutex poisoned")
        .last_error = None;
}

fn report(
    name: String,
    shared: Arc<Mutex<StreamSinkState>>,
    flushed: bool,
) -> StreamSinkShutdownReport {
    let state = shared
        .lock()
        .expect("stream sink state mutex poisoned")
        .clone();
    StreamSinkShutdownReport {
        name,
        status: state.status,
        stats: state.stats,
        last_error: state.last_error,
        flushed,
    }
}

fn write_wal_record(
    shared: &Arc<Mutex<StreamSinkState>>,
    wal: &mut Option<StreamSinkWalWriter>,
    record: &StreamSinkWalRecord,
) -> Result<()> {
    if let Some(wal) = wal {
        wal.write(record)?;
        increment_wal_records(shared);
    }
    Ok(())
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

struct StreamSinkWalWriter {
    writer: std::io::BufWriter<std::fs::File>,
    fsync_policy: StreamSinkWalFsyncPolicy,
}

impl StreamSinkWalWriter {
    fn open(path: Option<&Path>, fsync_policy: StreamSinkWalFsyncPolicy) -> Result<Option<Self>> {
        let Some(path) = path else {
            return Ok(None);
        };
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| StreamFacadeError::Io {
                operation: "open stream sink jsonl wal",
                message: error.to_string(),
            })?;
        Ok(Some(Self {
            writer: std::io::BufWriter::new(file),
            fsync_policy,
        }))
    }

    fn write(&mut self, record: &StreamSinkWalRecord) -> Result<()> {
        serde_json::to_writer(&mut self.writer, record).map_err(|error| StreamFacadeError::Io {
            operation: "serialize stream sink jsonl wal record",
            message: error.to_string(),
        })?;
        self.writer
            .write_all(b"\n")
            .and_then(|()| self.writer.flush())
            .map_err(|error| StreamFacadeError::Io {
                operation: "write stream sink jsonl wal record",
                message: error.to_string(),
            })?;
        if self.fsync_policy == StreamSinkWalFsyncPolicy::EveryRecord {
            self.writer
                .get_ref()
                .sync_data()
                .map_err(|error| StreamFacadeError::Io {
                    operation: "fsync stream sink jsonl wal record",
                    message: error.to_string(),
                })?;
        }
        Ok(())
    }
}

impl StreamSinkWalRecord {
    fn from_commit(
        name: &str,
        kind: StreamSinkWalRecordKind,
        commit: &tqsdk_core::CommitResult,
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

    fn lagged(name: &str, skipped: u64) -> Self {
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

    fn flush(name: &str, kind: StreamSinkWalRecordKind, error: Option<String>) -> Self {
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

fn commit_scope(scope: tqsdk_core::CommitScope) -> &'static str {
    match scope {
        tqsdk_core::CommitScope::InitialReady => "initial_ready",
        tqsdk_core::CommitScope::RealtimeUpdate => "realtime_update",
        tqsdk_core::CommitScope::ResyncRecovery => "resync_recovery",
        tqsdk_core::CommitScope::ReplayStep => "replay_step",
        tqsdk_core::CommitScope::QueryRefresh => "query_refresh",
        tqsdk_core::CommitScope::SessionTransition => "session_transition",
    }
}
