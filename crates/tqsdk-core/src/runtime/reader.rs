use std::{fmt, marker::PhantomData};

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    Result,
    ids::Revision,
    state::{
        CommitResult, MarketStateReadGuard, StateReadView, StateSnapshot, TradeStateReadGuard,
        UpdateCursor,
    },
};

use super::SharedState;

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
    commit: CommitResult,
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
    pub fn read(&self) -> SnapshotReadGuard<'_> {
        SnapshotReadGuard {
            snapshot: self.state.snapshot(),
            _marker: PhantomData,
        }
    }

    /// Borrows only market state partitions needed by typed market readers.
    pub fn read_market_state(&self) -> MarketStateReadGuard<'_> {
        self.state.read_market_state()
    }

    /// Borrows only the trade state partition needed by typed trade readers.
    pub fn read_trade_state(&self) -> TradeStateReadGuard<'_> {
        self.state.read_trade_state()
    }

    /// Returns the next retained commit for the provided cursor, if available.
    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<CommitResult> {
        self.commit_log.next(cursor)
    }

    /// Returns a guard pairing the next commit with the matching state
    /// revision, or reports cursor lag when the caller fell behind retention.
    pub fn next_view(
        &self,
        cursor: &mut UpdateCursor,
    ) -> std::result::Result<Option<CommitReadGuard<'_>>, CursorLagged> {
        let snapshot = self.state.snapshot();
        let current_revision = snapshot.revision();
        let expected_revision = cursor.next_revision();

        if current_revision.get() < expected_revision.get() {
            return Ok(None);
        }

        if current_revision != expected_revision {
            let oldest_available_revision = self
                .commit_log
                .oldest_revision()
                .unwrap_or(expected_revision);
            return Err(CursorLagged {
                expected_revision,
                oldest_available_revision,
                current_revision,
            });
        }

        let Some(commit) = self.commit_log.commit_at(expected_revision) else {
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
