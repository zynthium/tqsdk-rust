#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::Arc;

use tqsdk_core::{CommitResult, Revision, RuntimeReader};

use crate::change::{ChangeTrackedRef, matches_any, matches_fields};

#[derive(Clone)]
pub(crate) struct WaitReadHandle {
    reader: RuntimeReader,
}

impl WaitReadHandle {
    pub(crate) fn new(reader: RuntimeReader) -> Self {
        Self { reader }
    }

    pub(crate) fn reader(&self) -> &RuntimeReader {
        &self.reader
    }
}

#[derive(Debug, Clone)]
pub struct WaitStep {
    commit: Arc<CommitResult>,
    current_dt: Option<i64>,
}

impl WaitStep {
    pub(crate) fn new(commit: Arc<CommitResult>, current_dt: Option<i64>) -> Self {
        Self { commit, current_dt }
    }

    #[must_use]
    pub fn revision(&self) -> Revision {
        self.commit.revision
    }

    #[must_use]
    pub fn current_dt(&self) -> Option<i64> {
        self.current_dt
    }

    #[must_use]
    pub fn is_changing(&self, target: &impl ChangeTrackedRef) -> bool {
        matches_any(&self.commit.changes, target)
    }

    #[must_use]
    pub fn is_changing_fields(&self, target: &impl ChangeTrackedRef, fields: &[&str]) -> bool {
        matches_fields(&self.commit.changes, target, fields)
    }
}
