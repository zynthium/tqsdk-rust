#![cfg_attr(not(test), forbid(unsafe_code))]

mod journal;
mod options;
mod runtime;
mod state;
mod wal;
mod writer;

pub use journal::{
    StreamCommitJournal, StreamCommitJournalDomain, StreamCommitJournalRecord,
    StreamCommitJournalReplayReport, StreamCommitJournalScope,
};
pub use options::{StreamSinkOptions, StreamSinkProfile, StreamSinkRetryPolicy};
pub use runtime::{CommitSink, StreamSinkFuture, StreamSinkHandle};
pub use state::{StreamSinkShutdownReport, StreamSinkStats, StreamSinkStatus};
pub use wal::{
    StreamSinkWalCompaction, StreamSinkWalCompactionReport, StreamSinkWalFsyncPolicy,
    StreamSinkWalRecord, StreamSinkWalRecordKind, StreamSinkWalRecovery,
    StreamSinkWalRecoveryReport,
};
