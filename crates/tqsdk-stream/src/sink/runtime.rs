use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::oneshot;
use tqsdk_core::{CommitResult, SharedCommitResult};

use crate::{CommitStream, Result, StreamFacadeError};

use super::options::{StreamSinkOptions, StreamSinkRetryPolicy};
use super::state::{
    SharedStreamSinkState, StreamSinkShutdownReport, StreamSinkStats, StreamSinkStatus, add_lagged,
    clear_error, current_status, increment_journal_records, increment_processed,
    increment_retry_attempts, increment_wal_records, last_error, record_error, report, set_status,
    stats,
};
use super::wal::{StreamSinkWalRecord, StreamSinkWalRecordKind};
use super::writer::{StreamCommitJournalWriter, StreamSinkWalWriter};

pub type StreamSinkFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

/// User-provided commit sink run by `tqsdk-stream`.
pub trait CommitSink: Send + 'static {
    fn handle_commit(&mut self, commit: SharedCommitResult) -> StreamSinkFuture;

    fn flush(&mut self) -> StreamSinkFuture {
        Box::pin(async { Ok(()) })
    }
}

/// Handle returned after spawning a managed commit sink.
pub struct StreamSinkHandle {
    name: String,
    shared: SharedStreamSinkState,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<StreamSinkShutdownReport>,
}

struct StreamSinkRuntime<S> {
    name: String,
    commits: CommitStream,
    sink: S,
    shutdown_rx: oneshot::Receiver<()>,
    shared: SharedStreamSinkState,
    retry_policy: StreamSinkRetryPolicy,
    wal: Option<StreamSinkWalWriter>,
    journal: Option<StreamCommitJournalWriter>,
}

impl<F> CommitSink for F
where
    F: FnMut(SharedCommitResult) -> StreamSinkFuture + Send + 'static,
{
    fn handle_commit(&mut self, commit: SharedCommitResult) -> StreamSinkFuture {
        self(commit)
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
        let wal = StreamSinkWalWriter::open(options.wal_path(), options.fsync_policy())?;
        let journal =
            StreamCommitJournalWriter::open(options.commit_journal_path(), options.fsync_policy())?;
        let shared = SharedStreamSinkState::new();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(run_sink(StreamSinkRuntime {
            name: name.clone(),
            commits,
            sink,
            shutdown_rx,
            shared: shared.clone(),
            retry_policy: options.retry_policy_config(),
            wal,
            journal,
        }));

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
        current_status(&self.shared)
    }

    #[must_use]
    pub fn stats(&self) -> StreamSinkStats {
        stats(&self.shared)
    }

    #[must_use]
    pub fn last_error(&self) -> Option<StreamFacadeError> {
        last_error(&self.shared)
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

async fn run_sink<S>(runtime: StreamSinkRuntime<S>) -> StreamSinkShutdownReport
where
    S: CommitSink,
{
    let StreamSinkRuntime {
        name,
        mut commits,
        mut sink,
        mut shutdown_rx,
        shared,
        retry_policy,
        mut wal,
        mut journal,
    } = runtime;

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
                            &mut journal,
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
    shared: &SharedStreamSinkState,
    wal: &mut Option<StreamSinkWalWriter>,
    journal: &mut Option<StreamCommitJournalWriter>,
    sink: &mut S,
    commit: SharedCommitResult,
    retry_policy: StreamSinkRetryPolicy,
) -> Result<()>
where
    S: CommitSink,
{
    let mut attempt = 1;
    write_commit_journal_record(shared, journal, &commit)?;
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
        match sink.handle_commit(Arc::clone(&commit)).await {
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

                if attempt >= retry_policy.max_attempts() {
                    return Err(error);
                }

                record_error(shared, error);
                increment_retry_attempts(shared);
                if !retry_policy.retry_delay().is_zero() {
                    tokio::time::sleep(retry_policy.retry_delay()).await;
                }
                attempt += 1;
            }
        }
    }
}

async fn flush_sink<S>(
    name: &str,
    shared: &SharedStreamSinkState,
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

fn write_wal_record(
    shared: &SharedStreamSinkState,
    wal: &mut Option<StreamSinkWalWriter>,
    record: &StreamSinkWalRecord,
) -> Result<()> {
    if let Some(wal) = wal {
        wal.write(record)?;
        increment_wal_records(shared);
    }
    Ok(())
}

fn write_commit_journal_record(
    shared: &SharedStreamSinkState,
    journal: &mut Option<StreamCommitJournalWriter>,
    commit: &CommitResult,
) -> Result<()> {
    if let Some(journal) = journal {
        journal.write(commit)?;
        increment_journal_records(shared);
    }
    Ok(())
}
