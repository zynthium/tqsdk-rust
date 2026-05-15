#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::TqApi;
use crate::TqBacktest;
use crate::driver::WaitGuard;

/// Explicit wait-facade fixture driver for deterministic tests.
///
/// This type exposes wait-loop characterization hooks without placing hidden
/// `_for_test` methods on the production [`TqApi`] facade.
pub struct WaitTestDriver;

impl WaitTestDriver {
    /// Hold the single-owner wait guard to characterize concurrent wait calls.
    pub fn begin_wait(api: &TqApi) -> crate::error::Result<WaitTestGuard<'_>> {
        Ok(WaitTestGuard {
            guard: api.begin_fixture_wait()?,
        })
    }

    /// Queue a commit so the next `wait_update()` observes it before polling IO.
    pub fn push_deferred_commit(
        api: &mut TqApi,
        commit: impl Into<tqsdk_core::SharedCommitResult>,
    ) {
        api.push_fixture_deferred_commit(commit.into());
    }

    pub fn from_session_with_backtest(
        session: tqsdk_session::SessionClient,
        backtest: TqBacktest,
    ) -> TqApi {
        TqApi::new_with_backtest(session, Some(backtest))
    }
}

/// Opaque guard returned by [`WaitTestDriver::begin_wait`].
pub struct WaitTestGuard<'a> {
    guard: WaitGuard<'a>,
}

impl std::fmt::Debug for WaitTestGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitTestGuard").finish_non_exhaustive()
    }
}

impl PartialEq for WaitTestGuard<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.guard == other.guard
    }
}

impl Eq for WaitTestGuard<'_> {}
