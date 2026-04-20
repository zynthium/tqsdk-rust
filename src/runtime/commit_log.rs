use std::sync::{Arc, Mutex};

use crate::{ids::Revision, state::{CommitResult, UpdateCursor}};

#[derive(Debug, Clone, Default)]
pub struct CommitLog {
    inner: Arc<Mutex<CommitLogInner>>,
}

impl CommitLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn head_revision(&self) -> Option<Revision> {
        self.inner.lock().expect("commit log mutex poisoned").head
    }

    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<CommitResult> {
        let state = self.inner.lock().expect("commit log mutex poisoned");
        let commit = state
            .entries
            .iter()
            .find(|commit| commit.revision == cursor.next_revision())?
            .clone();
        drop(state);

        cursor.set_next_revision(Revision::new(commit.revision.get() + 1));
        Some(commit)
    }

    pub(crate) fn publish(&self, commit: CommitResult) {
        let mut state = self.inner.lock().expect("commit log mutex poisoned");
        state.head = Some(commit.revision);
        state.entries.push(commit);
    }
}

#[derive(Debug, Default)]
struct CommitLogInner {
    head: Option<Revision>,
    entries: Vec<CommitResult>,
}
