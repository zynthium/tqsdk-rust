use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

use crate::{ids::Revision, state::{CommitResult, UpdateCursor}};

use super::RuntimeCore;

pub struct SnapshotReadGuard<'a> {
    guard: MutexGuard<'a, RuntimeCore>,
}

impl SnapshotReadGuard<'_> {
    pub fn revision(&self) -> Revision {
        self.guard.commit_engine.snapshot().revision()
    }

    pub fn get<I, S>(&self, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.guard.commit_engine.snapshot().get(path)
    }
}

#[derive(Clone)]
pub struct RuntimeReader {
    pub(crate) inner: Arc<Mutex<RuntimeCore>>,
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
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        inner.next_cursor(next_revision)
    }

    pub fn read(&self) -> SnapshotReadGuard<'_> {
        SnapshotReadGuard {
            guard: self.inner.lock().expect("runtime mutex poisoned"),
        }
    }

    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<CommitResult> {
        self.commit_log.next(cursor)
    }
}
