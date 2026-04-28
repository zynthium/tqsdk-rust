#![cfg_attr(not(test), forbid(unsafe_code))]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
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

impl StreamSinkHandle {
    pub(crate) fn spawn<S>(name: String, commits: CommitStream, sink: S) -> Self
    where
        S: CommitSink,
    {
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
        ));

        Self {
            name,
            shared,
            shutdown_tx: Some(shutdown_tx),
            task,
        }
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
) -> StreamSinkShutdownReport
where
    S: CommitSink,
{
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                set_status(&shared, StreamSinkStatus::Stopping);
                let flushed = flush_sink(&shared, &mut sink).await;
                if current_status(&shared) != StreamSinkStatus::Failed {
                    set_status(&shared, StreamSinkStatus::Stopped);
                }
                return report(name, shared, flushed);
            }
            update = commits.next() => {
                match update {
                    Some(Ok(commit)) => {
                        if let Err(error) = sink.handle_commit(commit).await {
                            record_error(&shared, error);
                            set_status(&shared, StreamSinkStatus::Failed);
                            return report(name, shared, false);
                        }
                        increment_processed(&shared);
                    }
                    Some(Err(StreamFacadeError::Lagged { skipped })) => {
                        add_lagged(&shared, skipped);
                    }
                    Some(Err(StreamFacadeError::Closed)) | None => {
                        set_status(&shared, StreamSinkStatus::Stopping);
                        let flushed = flush_sink(&shared, &mut sink).await;
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

async fn flush_sink<S>(shared: &Arc<Mutex<StreamSinkState>>, sink: &mut S) -> bool
where
    S: CommitSink,
{
    match sink.flush().await {
        Ok(()) => true,
        Err(error) => {
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

fn record_error(shared: &Arc<Mutex<StreamSinkState>>, error: StreamFacadeError) {
    let mut state = shared.lock().expect("stream sink state mutex poisoned");
    state.stats.errors += 1;
    state.last_error = Some(error);
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
