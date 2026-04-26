#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::error::{Result, WaitFacadeError};
use tqsdk_core::internal::SessionRuntime;

pub(crate) struct WaitDriver {
    pub(crate) session: tqsdk_session::SessionClient,
    pub(crate) reader: tqsdk_core::RuntimeReader,
    pub(crate) cursor: tqsdk_core::UpdateCursor,
    pub(crate) runtime: SessionRuntime,
    pub(crate) deferred_commits: VecDeque<tqsdk_core::CommitResult>,
    pub(crate) last_commit: Option<tqsdk_core::CommitResult>,
    pub(crate) waiting: AtomicBool,
    pub(crate) next_order_seq: AtomicU64,
}

impl WaitDriver {
    pub(crate) fn begin_wait(&self) -> Result<WaitGuard<'_>> {
        WaitGuard::new(&self.waiting)
    }
}

#[doc(hidden)]
pub struct WaitGuard<'a> {
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
