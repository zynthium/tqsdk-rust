#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::{Result, WaitFacadeError};

pub(crate) struct WaitDriver {
    pub(crate) session: tqsdk_session::SessionClient,
    pub(crate) reader: tqsdk_core::RuntimeReader,
    pub(crate) cursor: tqsdk_core::UpdateCursor,
    pub(crate) deferred_commits: VecDeque<tqsdk_core::SharedCommitResult>,
    pub(crate) last_commit: Option<tqsdk_core::SharedCommitResult>,
    pub(crate) waiting: AtomicBool,
    pub(crate) next_order_seq: AtomicU64,
    pub(crate) serial_charts: HashSet<String>,
}

impl WaitDriver {
    pub(crate) fn begin_wait(&self) -> Result<WaitGuard<'_>> {
        WaitGuard::new(&self.waiting)
    }
}

pub(crate) struct WaitGuard<'a> {
    waiting: &'a AtomicBool,
}

impl<'a> WaitGuard<'a> {
    pub(crate) fn new(waiting: &'a AtomicBool) -> Result<Self> {
        if waiting.swap(true, Ordering::AcqRel) {
            return Err(WaitFacadeError::ConcurrentWaitUpdate);
        }

        Ok(Self { waiting })
    }
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.waiting.store(false, Ordering::Release);
    }
}

impl std::fmt::Debug for WaitGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitGuard").finish_non_exhaustive()
    }
}

impl PartialEq for WaitGuard<'_> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.waiting, other.waiting)
    }
}

impl Eq for WaitGuard<'_> {}
