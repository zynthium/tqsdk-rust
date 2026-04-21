#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64};

use tqsdk_session::SessionClient;

use crate::driver::{WaitDriver, WaitGuard};

pub struct TqApi {
    pub(crate) driver: WaitDriver,
}

impl TqApi {
    pub fn new(session: SessionClient) -> Self {
        let handle = session.handle().clone();
        Self::new_for_test(handle, session)
    }

    #[doc(hidden)]
    pub fn new_for_test(handle: tqsdk_core::RuntimeHandle, session: SessionClient) -> Self {
        let reader = handle.reader();
        let cursor = reader.cursor();
        let runtime = session.runtime_clone();

        Self {
            driver: WaitDriver {
                session,
                reader,
                cursor,
                runtime,
                deferred_commits: VecDeque::new(),
                last_commit: None,
                waiting: AtomicBool::new(false),
                next_order_seq: AtomicU64::new(1),
            },
        }
    }

    pub async fn wait_update(
        &mut self,
        _deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<bool> {
        let _guard = WaitGuard::new(&self.driver.waiting)?;

        if let Some(commit) = self.driver.deferred_commits.pop_front() {
            self.driver.last_commit = Some(commit);
            return Ok(true);
        }

        Ok(false)
    }

    pub fn last_commit(&self) -> Option<&tqsdk_core::CommitResult> {
        self.driver.last_commit.as_ref()
    }

    #[doc(hidden)]
    pub fn begin_wait_for_test(&self) -> crate::error::Result<WaitGuard<'_>> {
        self.driver.begin_wait()
    }

    #[doc(hidden)]
    pub fn handle_for_test(&self) -> tqsdk_core::RuntimeHandle {
        self.driver.runtime.handle()
    }

    #[doc(hidden)]
    pub fn push_deferred_commit_for_test(&mut self, commit: tqsdk_core::CommitResult) {
        self.driver.deferred_commits.push_back(commit);
    }
}
