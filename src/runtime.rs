use std::future::Future;

use crate::{
    commands::RuntimeCommand,
    error::Result,
    ids::{CommandId, CursorId, Revision},
    state::{StateSnapshot, UpdateCursor},
};

pub trait Runtime {
    fn submit(&self, cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send;
    fn latest_snapshot(&self) -> StateSnapshot;
    fn cursor(&self) -> UpdateCursor;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitLog {
    head: Option<Revision>,
}

impl CommitLog {
    pub fn new() -> Self {
        Self { head: None }
    }

    pub fn head_revision(&self) -> Option<Revision> {
        self.head
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeHandle;

impl RuntimeHandle {
    pub fn new() -> Self {
        Self
    }
}

impl Runtime for RuntimeHandle {
    fn submit(&self, _cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send {
        async { Ok(CommandId::new(1)) }
    }

    fn latest_snapshot(&self) -> StateSnapshot {
        StateSnapshot::new(Revision::new(0))
    }

    fn cursor(&self) -> UpdateCursor {
        UpdateCursor::new(CursorId::new(1), Revision::new(1))
    }
}
