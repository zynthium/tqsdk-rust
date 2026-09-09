use std::{fmt, marker::PhantomData};

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    Result,
    ids::Revision,
    state::{
        CommitResult, MarketStateReadGuard, MarketTradeStateReadGuard, SharedCommitResult,
        StateReadTelemetry, StateReadView, StateSnapshot, TradeStateReadGuard, UpdateCursor,
    },
};

use super::{CommitLogLagged, CommitLogTelemetry, SharedState};

/// Revision-bound read guard over a materialized runtime state snapshot.
pub struct SnapshotReadGuard<'a> {
    snapshot: StateSnapshot,
    _marker: PhantomData<&'a ()>,
}

impl SnapshotReadGuard<'_> {
    /// Returns a borrowed view over the materialized snapshot.
    pub fn view(&self) -> StateReadView<'_> {
        self.snapshot.read()
    }

    /// Returns the snapshot revision visible through this guard.
    pub fn revision(&self) -> Revision {
        self.view().revision()
    }

    /// Looks up a value at the provided path.
    pub fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.view().get(path)
    }

    /// Looks up a value using a borrowed path slice.
    pub fn get_path(&self, path: &[&str]) -> Option<&Value> {
        self.view().get_path(path)
    }

    /// Decodes a value at the provided path.
    pub fn decode<T, I, S>(&self, path: I) -> Result<Option<T>>
    where
        T: DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.view().decode(path)
    }

    /// Decodes a value using a borrowed path slice without per-segment
    /// allocations on the success path.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.view().decode_path(path)
    }
}

/// Revision-consistent read of a just-consumed commit.
pub struct CommitReadGuard<'a> {
    commit: SharedCommitResult,
    snapshot: StateSnapshot,
    _marker: PhantomData<&'a ()>,
}

impl CommitReadGuard<'_> {
    /// Returns metadata for the commit this guard is pinned to.
    pub fn commit(&self) -> &CommitResult {
        &self.commit
    }

    /// Returns a borrowed view over the state revision paired with this commit.
    pub fn view(&self) -> StateReadView<'_> {
        self.snapshot.read()
    }

    /// Returns the commit revision represented by this guard.
    pub fn revision(&self) -> Revision {
        self.commit.revision
    }

    /// Looks up a value at the provided path.
    pub fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.view().get(path)
    }

    /// Looks up a value using a borrowed path slice.
    pub fn get_path(&self, path: &[&str]) -> Option<&Value> {
        self.view().get_path(path)
    }

    /// Decodes a value at the provided path.
    pub fn decode<T, I, S>(&self, path: I) -> Result<Option<T>>
    where
        T: DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.view().decode(path)
    }

    /// Decodes a value using a borrowed path slice without per-segment
    /// allocations on the success path.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.view().decode_path(path)
    }
}

impl fmt::Debug for CommitReadGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitReadGuard")
            .field("commit", &self.commit)
            .field("revision", &self.snapshot.revision())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorLagged {
    expected_revision: Revision,
    oldest_available_revision: Revision,
    current_revision: Revision,
}

impl CursorLagged {
    /// Returns the next revision the caller attempted to consume.
    pub fn expected_revision(self) -> Revision {
        self.expected_revision
    }

    /// Returns the oldest revision still retained in the shared commit log.
    pub fn oldest_available_revision(self) -> Revision {
        self.oldest_available_revision
    }

    /// Returns the current head revision visible in the shared state.
    pub fn current_revision(self) -> Revision {
        self.current_revision
    }
}

impl From<CommitLogLagged> for CursorLagged {
    fn from(lagged: CommitLogLagged) -> Self {
        Self {
            expected_revision: lagged.expected_revision(),
            oldest_available_revision: lagged.oldest_available_revision(),
            current_revision: lagged.current_revision(),
        }
    }
}

/// Canonical read-side surface for state reads and cursor-driven commit
/// consumption.
#[derive(Clone)]
pub struct RuntimeReader {
    pub(crate) state: SharedState,
    pub(crate) commit_log: super::CommitLog,
}

impl RuntimeReader {
    /// Returns the current head revision in the shared commit log.
    pub fn head_revision(&self) -> Option<Revision> {
        self.commit_log.head_revision()
    }

    /// Returns a reference to the commit notification primitive.
    ///
    /// Use `notified().notified().await` to wait for the next commit.
    pub fn notified(&self) -> &tokio::sync::Notify {
        self.commit_log.notified()
    }

    /// Creates a cursor positioned after the current head revision.
    pub fn cursor(&self) -> UpdateCursor {
        let next_revision = Revision::new(
            self.commit_log
                .head_revision()
                .map_or(1, |revision| revision.get() + 1),
        );
        self.commit_log.new_cursor(next_revision)
    }

    /// Acquires a revision-bound snapshot read guard.
    ///
    /// This briefly acquires every live state partition before returning a
    /// detached immutable snapshot. Do not call it while holding a live
    /// partition guard from this reader: a waiting writer can otherwise make
    /// the nested read block indefinitely.
    pub fn read(&self) -> SnapshotReadGuard<'_> {
        SnapshotReadGuard {
            snapshot: self.state.snapshot(),
            _marker: PhantomData,
        }
    }

    /// Borrows only market state partitions needed by typed market readers.
    ///
    /// Do not acquire another live partition guard while this guard is held.
    /// Use [`Self::read_market_trade_state`] when market and trade state must be
    /// read together.
    pub fn read_market_state(&self) -> MarketStateReadGuard<'_> {
        self.state.read_market_state()
    }

    /// Borrows only the trade state partition needed by typed trade readers.
    ///
    /// Do not acquire another live partition guard while this guard is held.
    /// Use [`Self::read_market_trade_state`] when market and trade state must be
    /// read together.
    pub fn read_trade_state(&self) -> TradeStateReadGuard<'_> {
        self.state.read_trade_state()
    }

    /// Borrows market and trade partitions under one revision-bound guard.
    ///
    /// Do not call this while holding any live partition guard from this
    /// reader. Use this method as the one combined acquisition instead of
    /// nesting market, trade, or full-state reads.
    pub fn read_market_trade_state(&self) -> MarketTradeStateReadGuard<'_> {
        self.state.read_market_trade_state()
    }

    /// Returns cumulative COW snapshot and live-partition lock telemetry.
    ///
    /// Counters are recorded after read locks release, so collecting them does
    /// not extend a runtime-state lock critical section.
    #[must_use]
    pub fn state_read_telemetry(&self) -> StateReadTelemetry {
        self.state.read_telemetry()
    }

    /// Returns the next retained commit for the provided cursor, if available.
    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<SharedCommitResult> {
        self.commit_log.next(cursor)
    }

    /// Returns the next commit, or explicitly reports that retention trimmed it.
    ///
    /// On lag, `cursor` remains unchanged. Call
    /// [`Self::resync_cursor_to_head`] only when losing the intervening commits is
    /// an accepted recovery policy.
    pub fn next_checked(
        &self,
        cursor: &mut UpdateCursor,
    ) -> std::result::Result<Option<SharedCommitResult>, CursorLagged> {
        self.commit_log.next_checked(cursor).map_err(Into::into)
    }

    /// Moves a lagged cursor after the current commit-log head.
    ///
    /// This is explicit lossy recovery. Consumers requiring every revision must
    /// instead rebuild from a durable checkpoint or fail their work.
    pub fn resync_cursor_to_head(&self, cursor: &mut UpdateCursor) -> Option<Revision> {
        self.commit_log.resync_cursor_to_head(cursor)
    }

    /// Returns bounded-log and cursor-lag telemetry.
    #[must_use]
    pub fn commit_log_telemetry(&self) -> CommitLogTelemetry {
        self.commit_log.telemetry()
    }

    /// Returns a guard pairing the next commit with the matching state
    /// revision, or reports cursor lag when the caller fell behind retention.
    pub fn next_view(
        &self,
        cursor: &mut UpdateCursor,
    ) -> std::result::Result<Option<CommitReadGuard<'_>>, CursorLagged> {
        let expected_revision = cursor.next_revision();
        let Some((commit, snapshot)) = self
            .commit_log
            .view_at(expected_revision)
            .map_err(CursorLagged::from)?
        else {
            return Ok(None);
        };
        cursor.set_next_revision(Revision::new(commit.revision.get() + 1));

        Ok(Some(CommitReadGuard {
            commit,
            snapshot,
            _marker: PhantomData,
        }))
    }
}
