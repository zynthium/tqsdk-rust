use std::sync::{Arc, Mutex};

use crate::StreamFacadeError;

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
    journal_records: u64,
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

#[derive(Debug, Clone)]
pub(super) struct SharedStreamSinkState {
    inner: Arc<Mutex<StreamSinkState>>,
}

impl SharedStreamSinkState {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StreamSinkState {
                status: StreamSinkStatus::Running,
                stats: StreamSinkStats::default(),
                last_error: None,
            })),
        }
    }

    fn with<R>(&self, f: impl FnOnce(&StreamSinkState) -> R) -> R {
        let state = self.inner.lock().expect("stream sink state mutex poisoned");
        f(&state)
    }

    fn with_mut<R>(&self, f: impl FnOnce(&mut StreamSinkState) -> R) -> R {
        let mut state = self.inner.lock().expect("stream sink state mutex poisoned");
        f(&mut state)
    }

    fn snapshot(&self) -> StreamSinkState {
        self.with(Clone::clone)
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

    #[must_use]
    pub fn journal_records(&self) -> u64 {
        self.journal_records
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

pub(super) fn current_status(shared: &SharedStreamSinkState) -> StreamSinkStatus {
    shared.with(|state| state.status)
}

pub(super) fn stats(shared: &SharedStreamSinkState) -> StreamSinkStats {
    shared.with(|state| state.stats)
}

pub(super) fn last_error(shared: &SharedStreamSinkState) -> Option<StreamFacadeError> {
    shared.with(|state| state.last_error.clone())
}

pub(super) fn set_status(shared: &SharedStreamSinkState, status: StreamSinkStatus) {
    shared.with_mut(|state| state.status = status);
}

pub(super) fn increment_processed(shared: &SharedStreamSinkState) {
    shared.with_mut(|state| state.stats.processed_commits += 1);
}

pub(super) fn add_lagged(shared: &SharedStreamSinkState, skipped: u64) {
    shared.with_mut(|state| state.stats.lagged_commits += skipped);
}

pub(super) fn increment_retry_attempts(shared: &SharedStreamSinkState) {
    shared.with_mut(|state| state.stats.retry_attempts += 1);
}

pub(super) fn increment_wal_records(shared: &SharedStreamSinkState) {
    shared.with_mut(|state| state.stats.wal_records += 1);
}

pub(super) fn increment_journal_records(shared: &SharedStreamSinkState) {
    shared.with_mut(|state| state.stats.journal_records += 1);
}

pub(super) fn record_error(shared: &SharedStreamSinkState, error: StreamFacadeError) {
    shared.with_mut(|state| {
        state.stats.errors += 1;
        state.last_error = Some(error);
    });
}

pub(super) fn clear_error(shared: &SharedStreamSinkState) {
    shared.with_mut(|state| state.last_error = None);
}

pub(super) fn report(
    name: String,
    shared: SharedStreamSinkState,
    flushed: bool,
) -> StreamSinkShutdownReport {
    let state = shared.snapshot();
    StreamSinkShutdownReport {
        name,
        status: state.status,
        stats: state.stats,
        last_error: state.last_error,
        flushed,
    }
}
