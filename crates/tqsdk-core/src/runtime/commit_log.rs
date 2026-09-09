use std::{
    collections::{BTreeMap, VecDeque},
    fmt::{self, Write},
    mem::size_of,
    sync::{Arc, RwLock, Weak},
    time::SystemTime,
};

use tokio::sync::Notify;

use crate::{
    ids::{CursorId, Revision},
    state::{CursorTracker, SharedCommitResult, StateSnapshot, UpdateCursor},
};

use super::recover_poisoned_lock;

const DEFAULT_MAX_ENTRIES: usize = 8_192;
const DEFAULT_MAX_RETAINED_BYTES: usize = 32 * 1024 * 1024;

/// Hard resource limits for the in-memory commit log.
///
/// Both limits are enforced. A cursor that falls behind a trimmed revision must
/// explicitly resynchronize or fail through the checked reader API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitLogRetention {
    max_entries: usize,
    max_retained_bytes: usize,
}

impl CommitLogRetention {
    #[must_use]
    pub fn new(max_entries: usize, max_retained_bytes: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            max_retained_bytes: max_retained_bytes.max(1),
        }
    }

    #[must_use]
    pub fn max_entries(self) -> usize {
        self.max_entries
    }

    #[must_use]
    pub fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes
    }
}

impl Default for CommitLogRetention {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES, DEFAULT_MAX_RETAINED_BYTES)
    }
}

/// Lag information returned by [`CommitLog::next_checked`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitLogLagged {
    expected_revision: Revision,
    oldest_available_revision: Revision,
    current_revision: Revision,
}

impl CommitLogLagged {
    #[must_use]
    pub fn expected_revision(self) -> Revision {
        self.expected_revision
    }

    /// Returns the oldest retained revision, or the next head revision when no
    /// commit payload is retained and resynchronization must skip to head.
    #[must_use]
    pub fn oldest_available_revision(self) -> Revision {
        self.oldest_available_revision
    }

    #[must_use]
    pub fn current_revision(self) -> Revision {
        self.current_revision
    }
}

/// Per-cursor lag and activity measurements for the in-memory log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLogCursorTelemetry {
    pub cursor_id: CursorId,
    pub next_revision: Revision,
    pub lag_revisions: u64,
    /// Estimated bytes still retained at or after `next_revision`.
    pub lag_bytes: usize,
    pub last_advance_at: SystemTime,
}

/// Snapshot of log retention and cursor pressure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitLogTelemetry {
    pub retention: CommitLogRetention,
    pub retained_entries: usize,
    pub retained_bytes: usize,
    pub cursors: Vec<CommitLogCursorTelemetry>,
}

/// Underlying append-only commit buffer.
///
/// Prefer consuming commits through `RuntimeReader::next_checked` unless a raw
/// shared log primitive is specifically required.
#[derive(Debug, Clone)]
pub struct CommitLog {
    inner: Arc<RwLock<CommitLogInner>>,
    notified: Arc<Notify>,
}

impl CommitLog {
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(CommitLogRetention::default())
    }

    /// Creates a hard-bounded log using the default byte budget.
    #[must_use]
    pub fn with_retention(max_entries: usize) -> Self {
        Self::with_limits(CommitLogRetention::new(
            max_entries,
            DEFAULT_MAX_RETAINED_BYTES,
        ))
    }

    /// Creates a hard-bounded log with entry and byte limits.
    #[must_use]
    pub fn with_retention_limits(max_entries: usize, max_retained_bytes: usize) -> Self {
        Self::with_limits(CommitLogRetention::new(max_entries, max_retained_bytes))
    }

    #[must_use]
    pub fn with_limits(retention: CommitLogRetention) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CommitLogInner::new(retention))),
            notified: Arc::new(Notify::new()),
        }
    }

    #[must_use]
    pub fn retention(&self) -> CommitLogRetention {
        recover_poisoned_lock(self.inner.read()).retention
    }

    #[must_use]
    pub fn telemetry(&self) -> CommitLogTelemetry {
        recover_poisoned_lock(self.inner.read()).telemetry()
    }

    #[must_use]
    pub fn head_revision(&self) -> Option<Revision> {
        recover_poisoned_lock(self.inner.read()).head
    }

    /// Advances a cursor only when its expected revision is retained.
    ///
    /// A lagged cursor stays unchanged so the caller can observe the failure
    /// and select a recovery policy, such as [`Self::resync_cursor_to_head`].
    pub fn next_checked(
        &self,
        cursor: &mut UpdateCursor,
    ) -> Result<Option<SharedCommitResult>, CommitLogLagged> {
        let state = recover_poisoned_lock(self.inner.read());
        let expected_revision = cursor.next_revision();
        let commit = state.next(expected_revision)?.cloned();
        drop(state);

        if let Some(commit) = &commit {
            cursor.set_next_revision(Revision::new(commit.revision.get() + 1));
        }
        Ok(commit)
    }

    /// Compatibility read surface.
    ///
    /// If retention has trimmed the requested revision, this method performs a
    /// resync-to-head and returns `None`; use [`Self::next_checked`] when lost
    /// commits must be surfaced to the caller.
    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<SharedCommitResult> {
        match self.next_checked(cursor) {
            Ok(commit) => commit,
            Err(_) => {
                self.resync_cursor_to_head(cursor);
                None
            }
        }
    }

    /// Skips all current commits and positions `cursor` for the next head
    /// revision. Returns the new cursor position when the log has a head.
    pub fn resync_cursor_to_head(&self, cursor: &mut UpdateCursor) -> Option<Revision> {
        let next_revision = recover_poisoned_lock(self.inner.read())
            .head
            .map(|revision| Revision::new(revision.get() + 1))?;
        cursor.set_next_revision(next_revision);
        Some(next_revision)
    }

    pub(crate) fn new_cursor(&self, next_revision: Revision) -> UpdateCursor {
        let mut state = recover_poisoned_lock(self.inner.write());
        let cursor_id = CursorId::new(state.next_cursor_id);
        state.next_cursor_id += 1;
        state.cursor_positions.insert(
            cursor_id,
            CursorState {
                next_revision,
                last_advance_at: SystemTime::now(),
            },
        );
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

    pub(crate) fn view_at(
        &self,
        revision: Revision,
    ) -> Result<Option<(SharedCommitResult, StateSnapshot)>, CommitLogLagged> {
        recover_poisoned_lock(self.inner.read())
            .view_at(revision)
            .map(|view| view.map(|(commit, snapshot)| (commit.clone(), snapshot.clone())))
    }

    pub(crate) fn notified(&self) -> &Notify {
        self.notified.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn push(&self, commit: SharedCommitResult) {
        self.push_with_snapshot(commit, None);
    }

    fn push_with_snapshot(&self, commit: SharedCommitResult, snapshot: Option<StateSnapshot>) {
        let mut state = recover_poisoned_lock(self.inner.write());
        state.push(commit, snapshot);
        drop(state);
        self.notified.notify_waiters();
    }

    pub(crate) fn publish(&self, commit: SharedCommitResult, snapshot: StateSnapshot) {
        self.push_with_snapshot(commit, Some(snapshot));
    }
}

impl Default for CommitLog {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct RetainedCommit {
    commit: SharedCommitResult,
    snapshot: Option<StateSnapshot>,
    payload_bytes: usize,
    root_keys: Vec<usize>,
}

#[derive(Debug, Clone, Copy)]
struct RetainedSnapshotRoot {
    references: usize,
    bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct CursorState {
    next_revision: Revision,
    last_advance_at: SystemTime,
}

#[derive(Debug)]
struct CommitLogInner {
    next_cursor_id: u64,
    head: Option<Revision>,
    first_retained_revision: Option<Revision>,
    entries: VecDeque<RetainedCommit>,
    retained_roots: BTreeMap<usize, RetainedSnapshotRoot>,
    cursor_positions: BTreeMap<CursorId, CursorState>,
    retention: CommitLogRetention,
    retained_bytes: usize,
}

impl CommitLogInner {
    fn new(retention: CommitLogRetention) -> Self {
        Self {
            next_cursor_id: 1,
            head: None,
            first_retained_revision: None,
            entries: VecDeque::new(),
            retained_roots: BTreeMap::new(),
            cursor_positions: BTreeMap::new(),
            retention,
            retained_bytes: 0,
        }
    }

    fn commit_at(&self, revision: Revision) -> Option<&SharedCommitResult> {
        self.entry_at(revision).map(|entry| &entry.commit)
    }

    fn entry_at(&self, revision: Revision) -> Option<&RetainedCommit> {
        let first = self.first_retained_revision?;
        if revision.get() < first.get() {
            return None;
        }
        let index = (revision.get() - first.get()) as usize;
        self.entries
            .get(index)
            .filter(|entry| entry.commit.revision == revision)
    }

    fn view_at(
        &self,
        revision: Revision,
    ) -> Result<Option<(&SharedCommitResult, &StateSnapshot)>, CommitLogLagged> {
        if let Some(entry) = self.entry_at(revision) {
            return Ok(entry
                .snapshot
                .as_ref()
                .map(|snapshot| (&entry.commit, snapshot)));
        }

        let Some(head) = self.head else {
            return Ok(None);
        };
        if revision.get() > head.get() {
            return Ok(None);
        }
        Err(CommitLogLagged {
            expected_revision: revision,
            oldest_available_revision: self
                .first_retained_revision
                .unwrap_or_else(|| Revision::new(head.get() + 1)),
            current_revision: head,
        })
    }

    fn next(&self, revision: Revision) -> Result<Option<&SharedCommitResult>, CommitLogLagged> {
        if let Some(commit) = self.commit_at(revision) {
            return Ok(Some(commit));
        }
        let Some(head) = self.head else {
            return Ok(None);
        };
        if revision.get() > head.get() {
            return Ok(None);
        }
        Err(CommitLogLagged {
            expected_revision: revision,
            oldest_available_revision: self
                .first_retained_revision
                .unwrap_or_else(|| Revision::new(head.get() + 1)),
            current_revision: head,
        })
    }

    fn push(&mut self, commit: SharedCommitResult, snapshot: Option<StateSnapshot>) {
        self.head = Some(commit.revision);
        let payload_bytes = estimate_commit_bytes(&commit);
        let root_keys = snapshot
            .as_ref()
            .map_or_else(Vec::new, StateSnapshot::retained_root_keys);
        let new_root_bytes = root_keys
            .iter()
            .filter(|key| !self.retained_roots.contains_key(key))
            .map(|key| {
                (
                    *key,
                    snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.retained_root_bytes_for(*key))
                        .expect("snapshot root key must resolve to accounted bytes"),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if self.entries.is_empty() {
            self.first_retained_revision = Some(commit.revision);
        }
        self.entries.push_back(RetainedCommit {
            commit,
            snapshot,
            payload_bytes,
            root_keys: root_keys.clone(),
        });
        self.retained_bytes = self.retained_bytes.saturating_add(payload_bytes);
        for key in root_keys {
            if let Some(root) = self.retained_roots.get_mut(&key) {
                root.references = root.references.saturating_add(1);
            } else {
                let bytes = new_root_bytes
                    .get(&key)
                    .copied()
                    .expect("new snapshot root must have accounted bytes");
                self.retained_roots.insert(
                    key,
                    RetainedSnapshotRoot {
                        references: 1,
                        bytes,
                    },
                );
                self.retained_bytes = self.retained_bytes.saturating_add(bytes);
            }
        }
        self.trim();
    }

    fn trim(&mut self) {
        while self.entries.len() > self.retention.max_entries
            || self.retained_bytes > self.retention.max_retained_bytes
        {
            let Some(entry) = self.entries.pop_front() else {
                break;
            };
            self.release_entry(entry);
        }
        self.first_retained_revision = self.entries.front().map(|entry| entry.commit.revision);
    }

    fn release_entry(&mut self, entry: RetainedCommit) {
        self.retained_bytes = self.retained_bytes.saturating_sub(entry.payload_bytes);
        for key in entry.root_keys {
            let Some(root) = self.retained_roots.get_mut(&key) else {
                continue;
            };
            root.references = root.references.saturating_sub(1);
            if root.references == 0 {
                let bytes = root.bytes;
                self.retained_roots.remove(&key);
                self.retained_bytes = self.retained_bytes.saturating_sub(bytes);
            }
        }
    }

    fn entry_accounted_bytes(&self, entry: &RetainedCommit) -> usize {
        entry
            .root_keys
            .iter()
            .fold(entry.payload_bytes, |total, key| {
                total.saturating_add(self.retained_roots.get(key).map_or(0, |root| root.bytes))
            })
    }

    fn telemetry(&self) -> CommitLogTelemetry {
        let head = self.head;
        let cursors = self
            .cursor_positions
            .iter()
            .map(|(&cursor_id, state)| CommitLogCursorTelemetry {
                cursor_id,
                next_revision: state.next_revision,
                lag_revisions: head.map_or(0, |head| {
                    head.get()
                        .saturating_sub(state.next_revision.get())
                        .saturating_add(1)
                }),
                lag_bytes: self
                    .entries
                    .iter()
                    .filter(|entry| entry.commit.revision.get() >= state.next_revision.get())
                    .fold(0_usize, |total, entry| {
                        total.saturating_add(self.entry_accounted_bytes(entry))
                    }),
                last_advance_at: state.last_advance_at,
            })
            .collect();
        CommitLogTelemetry {
            retention: self.retention,
            retained_entries: self.entries.len(),
            retained_bytes: self.retained_bytes,
            cursors,
        }
    }
}

struct DebugByteCounter(usize);

impl Write for DebugByteCounter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0 = self.0.saturating_add(value.len());
        Ok(())
    }
}

fn estimate_commit_bytes(commit: &SharedCommitResult) -> usize {
    // `CommitResult` is intentionally shared rather than serialized for every
    // consumer. Counting its debug representation avoids a second retained
    // allocation while accounting for dynamic strings in the change set. The
    // multiplier leaves room for container and allocator overhead.
    let mut counter = DebugByteCounter(0);
    let _ = write!(&mut counter, "{:?}", **commit);
    size_of::<RetainedCommit>()
        .saturating_add(counter.0.saturating_mul(2))
        .saturating_add(256)
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
        if let Some(cursor) = state.cursor_positions.get_mut(&self.cursor_id)
            && next_revision.get() >= cursor.next_revision.get()
        {
            cursor.next_revision = next_revision;
            cursor.last_advance_at = SystemTime::now();
        }
    }
}

impl Drop for CommitLogCursorTracker {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        recover_poisoned_lock(inner.write())
            .cursor_positions
            .remove(&self.cursor_id);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
    };

    use crate::{
        CommitScope, ProtocolDomain,
        state::{ChangeSet, CommitResult},
    };

    use super::{CommitLog, Revision};

    fn commit(revision: u64) -> Arc<CommitResult> {
        Arc::new(CommitResult::new(
            Revision::new(revision),
            vec![ProtocolDomain::Market],
            ChangeSet::default(),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        ))
    }

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

    #[test]
    fn hard_entry_bound_trims_a_live_slow_cursor_and_reports_lag() {
        let log = CommitLog::with_retention_limits(2, usize::MAX);
        let mut cursor = log.new_cursor(Revision::new(1));
        log.push(commit(1));
        log.push(commit(2));
        log.push(commit(3));

        let telemetry = log.telemetry();
        assert_eq!(telemetry.retained_entries, 2);
        assert_eq!(telemetry.cursors.len(), 1);
        assert_eq!(telemetry.cursors[0].lag_revisions, 3);
        let lagged = log
            .next_checked(&mut cursor)
            .expect_err("trimmed cursor must receive an explicit lag signal");
        assert_eq!(lagged.expected_revision(), Revision::new(1));
        assert_eq!(lagged.oldest_available_revision(), Revision::new(2));
        assert_eq!(lagged.current_revision(), Revision::new(3));

        assert_eq!(
            log.resync_cursor_to_head(&mut cursor),
            Some(Revision::new(4))
        );
        assert_eq!(cursor.next_revision(), Revision::new(4));
    }

    #[test]
    fn hard_byte_bound_never_grows_with_a_live_slow_cursor() {
        let log = CommitLog::with_retention_limits(128, 2_048);
        let _cursor = log.new_cursor(Revision::new(1));
        for revision in 1..=64 {
            log.push(commit(revision));
            let telemetry = log.telemetry();
            assert!(telemetry.retained_bytes <= telemetry.retention.max_retained_bytes());
        }
        assert!(log.telemetry().retained_entries < 64);
    }
}
