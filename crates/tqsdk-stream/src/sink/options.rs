use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::{Result, StreamFacadeError};

use super::wal::StreamSinkWalFsyncPolicy;

/// Options for a managed stream sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSinkOptions {
    retry_policy: StreamSinkRetryPolicy,
    wal_path: Option<PathBuf>,
    wal_fsync_policy: StreamSinkWalFsyncPolicy,
    commit_journal_path: Option<PathBuf>,
}

/// Reusable configuration profile for common managed sink deployments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSinkProfile {
    options: StreamSinkOptions,
}

/// Retry policy applied inside a managed stream sink task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSinkRetryPolicy {
    max_attempts: u32,
    retry_delay: Duration,
}

impl Default for StreamSinkOptions {
    fn default() -> Self {
        Self {
            retry_policy: StreamSinkRetryPolicy::none(),
            wal_path: None,
            wal_fsync_policy: StreamSinkWalFsyncPolicy::Never,
            commit_journal_path: None,
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
    pub fn retry_policy_config(&self) -> StreamSinkRetryPolicy {
        self.retry_policy
    }

    #[must_use]
    pub fn jsonl_wal(mut self, path: impl Into<PathBuf>) -> Self {
        self.wal_path = Some(path.into());
        self
    }

    #[must_use]
    pub fn wal_path(&self) -> Option<&Path> {
        self.wal_path.as_deref()
    }

    #[must_use]
    pub fn jsonl_commit_journal(mut self, path: impl Into<PathBuf>) -> Self {
        self.commit_journal_path = Some(path.into());
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

    #[must_use]
    pub fn commit_journal_path(&self) -> Option<&Path> {
        self.commit_journal_path.as_deref()
    }
}

impl StreamSinkProfile {
    #[must_use]
    pub fn memory() -> Self {
        Self {
            options: StreamSinkOptions::new(),
        }
    }

    #[must_use]
    pub fn reliable_jsonl(wal_path: impl Into<PathBuf>, journal_path: impl Into<PathBuf>) -> Self {
        Self {
            options: StreamSinkOptions::new()
                .jsonl_wal(wal_path)
                .jsonl_commit_journal(journal_path),
        }
    }

    #[must_use]
    pub fn retry_policy(mut self, retry_policy: StreamSinkRetryPolicy) -> Self {
        self.options = self.options.retry_policy(retry_policy);
        self
    }

    #[must_use]
    pub fn fsync_policy(mut self, policy: StreamSinkWalFsyncPolicy) -> Self {
        self.options = self.options.wal_fsync_policy(policy);
        self
    }

    #[must_use]
    pub fn into_options(self) -> StreamSinkOptions {
        self.options
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
