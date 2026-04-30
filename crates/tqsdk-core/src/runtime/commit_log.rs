use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, RwLock, Weak},
};

use tokio::sync::Notify;

use crate::{
    ids::{CursorId, Revision},
    state::{CursorTracker, SharedCommitResult, UpdateCursor},
};

use super::recover_poisoned_lock;

const DEFAULT_MAX_ENTRIES: usize = 8_192;

/// Underlying append-only commit buffer.
///
/// Prefer consuming commits through `RuntimeReader::next` unless raw access to
/// the shared log primitive is specifically required.
#[derive(Debug, Clone)]
pub struct CommitLog {
    inner: Arc<RwLock<CommitLogInner>>,
    notified: Arc<Notify>,
}

impl CommitLog {
    pub fn new() -> Self {
        Self::with_retention(DEFAULT_MAX_ENTRIES)
    }

    pub fn with_retention(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CommitLogInner::new(max_entries))),
            notified: Arc::new(Notify::new()),
        }
    }

    pub fn head_revision(&self) -> Option<Revision> {
        recover_poisoned_lock(self.inner.read()).head
    }

    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<SharedCommitResult> {
        let state = recover_poisoned_lock(self.inner.read());
        let commit = state.commit_at(cursor.next_revision())?.clone();
        drop(state);

        cursor.set_next_revision(Revision::new(commit.revision.get() + 1));
        Some(commit)
    }

    pub(crate) fn new_cursor(&self, next_revision: Revision) -> UpdateCursor {
        let mut state = recover_poisoned_lock(self.inner.write());
        let cursor_id = CursorId::new(state.next_cursor_id);
        state.next_cursor_id += 1;
        state.cursor_positions.insert(cursor_id, next_revision);
        drop(state);

        UpdateCursor::with_tracker(
            cursor_id,
            next_revision,
            Arc::new(CommitLogCursorTracker {
                inner: Arc::downgrade(&self.inner),
                cursor_id,
            }),
        )
    }

    pub(crate) fn commit_at(&self, revision: Revision) -> Option<SharedCommitResult> {
        recover_poisoned_lock(self.inner.read())
            .commit_at(revision)
            .cloned()
    }

    pub fn notified(&self) -> &Notify {
        self.notified.as_ref()
    }

    pub(crate) fn oldest_revision(&self) -> Option<Revision> {
        recover_poisoned_lock(self.inner.read()).oldest_revision()
    }

    pub(crate) fn publish(&self, commit: SharedCommitResult) {
        let mut state = recover_poisoned_lock(self.inner.write());
        state.head = Some(commit.revision);
        if state.entries.is_empty() {
            state.first_retained_revision = Some(commit.revision);
        }
        state.entries.push_back(commit);
        state.trim();
        drop(state);
        self.notified.notify_waiters();
    }
}

impl Default for CommitLog {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct CommitLogInner {
    next_cursor_id: u64,
    head: Option<Revision>,
    first_retained_revision: Option<Revision>,
    entries: VecDeque<SharedCommitResult>,
    cursor_positions: BTreeMap<CursorId, Revision>,
    max_entries: usize,
}

impl CommitLogInner {
    fn new(max_entries: usize) -> Self {
        Self {
            next_cursor_id: 1,
            head: None,
            first_retained_revision: None,
            entries: VecDeque::new(),
            cursor_positions: BTreeMap::new(),
            max_entries: max_entries.max(1),
        }
    }

    fn oldest_revision(&self) -> Option<Revision> {
        self.first_retained_revision
    }

    fn commit_at(&self, revision: Revision) -> Option<&SharedCommitResult> {
        let first = self.first_retained_revision?;
        if revision.get() < first.get() {
            return None;
        }
        let index = (revision.get() - first.get()) as usize;
        self.entries
            .get(index)
            .filter(|commit| commit.revision == revision)
    }

    fn trim(&mut self) {
        while self.entries.len() > self.max_entries {
            let Some(first) = self.first_retained_revision else {
                break;
            };
            let protected_revision = self
                .cursor_positions
                .values()
                .map(|revision| revision.get())
                .min()
                .unwrap_or_else(|| self.head.map_or(first.get(), |revision| revision.get() + 1));
            if first.get() >= protected_revision {
                break;
            }

            self.entries.pop_front();
            self.first_retained_revision = if self.entries.is_empty() {
                None
            } else {
                Some(Revision::new(first.get() + 1))
            };
        }
    }
}

struct CommitLogCursorTracker {
    inner: Weak<RwLock<CommitLogInner>>,
    cursor_id: CursorId,
}

impl CursorTracker for CommitLogCursorTracker {
    fn update(&self, next_revision: Revision) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = recover_poisoned_lock(inner.write());
        if let Some(cursor_revision) = state.cursor_positions.get_mut(&self.cursor_id)
            && next_revision.get() > cursor_revision.get()
        {
            *cursor_revision = next_revision;
            state.trim();
        }
    }
}

impl Drop for CommitLogCursorTracker {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = recover_poisoned_lock(inner.write());
        state.cursor_positions.remove(&self.cursor_id);
        state.trim();
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;

    use super::CommitLog;

    #[test]
    fn commit_log_recovers_from_poisoned_rwlock() {
        let log = CommitLog::new();
        let inner = Arc::clone(&log.inner);

        let panic = catch_unwind(AssertUnwindSafe(move || {
            let _guard = inner.write().unwrap();
            panic!("poison commit log rwlock");
        }));
        assert!(panic.is_err());

        assert_eq!(log.head_revision(), None);
    }
}
