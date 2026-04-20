use std::fmt;
use std::sync::RwLockReadGuard;

use serde_json::Value;

use crate::{
    ids::Revision,
    state::{CommitResult, StateReadView, StateStore, UpdateCursor},
};

use super::SharedState;

/// Locked, revision-bound read guard over the runtime state tree.
pub struct SnapshotReadGuard<'a> {
    guard: RwLockReadGuard<'a, StateStore>,
}

impl SnapshotReadGuard<'_> {
    pub fn view(&self) -> StateReadView<'_> {
        self.guard.read()
    }

    pub fn revision(&self) -> Revision {
        self.view().revision()
    }

    pub fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.view().get(path)
    }
}

/// Zero-copy, revision-consistent read of a just-consumed commit.
pub struct CommitReadGuard<'a> {
    commit: CommitResult,
    guard: RwLockReadGuard<'a, StateStore>,
}

impl CommitReadGuard<'_> {
    pub fn commit(&self) -> &CommitResult {
        &self.commit
    }

    pub fn view(&self) -> StateReadView<'_> {
        self.guard.read()
    }

    pub fn revision(&self) -> Revision {
        self.commit.revision
    }

    pub fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.view().get(path)
    }
}

impl fmt::Debug for CommitReadGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitReadGuard")
            .field("commit", &self.commit)
            .field("revision", &self.guard.revision())
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
    pub fn expected_revision(self) -> Revision {
        self.expected_revision
    }

    pub fn oldest_available_revision(self) -> Revision {
        self.oldest_available_revision
    }

    pub fn current_revision(self) -> Revision {
        self.current_revision
    }
}

/// Canonical read-side surface for zero-copy state reads and cursor-driven
/// commit consumption.
#[derive(Clone)]
pub struct RuntimeReader {
    pub(crate) state: SharedState,
    pub(crate) commit_log: super::CommitLog,
}

impl RuntimeReader {
    pub fn head_revision(&self) -> Option<Revision> {
        self.commit_log.head_revision()
    }

    pub fn cursor(&self) -> UpdateCursor {
        let next_revision = Revision::new(
            self.commit_log
                .head_revision()
                .map_or(1, |revision| revision.get() + 1),
        );
        self.commit_log.new_cursor(next_revision)
    }

    pub fn read(&self) -> SnapshotReadGuard<'_> {
        SnapshotReadGuard {
            guard: self.state.read().expect("runtime state rwlock poisoned"),
        }
    }

    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<CommitResult> {
        self.commit_log.next(cursor)
    }

    pub fn next_view(
        &self,
        cursor: &mut UpdateCursor,
    ) -> std::result::Result<Option<CommitReadGuard<'_>>, CursorLagged> {
        let guard = self.state.read().expect("runtime state rwlock poisoned");
        let current_revision = guard.revision();
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

        Ok(Some(CommitReadGuard { commit, guard }))
    }
}
