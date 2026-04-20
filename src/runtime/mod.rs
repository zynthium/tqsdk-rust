use std::collections::VecDeque;

use crate::{adapter::AdapterRegistry, ids::{CursorId, Revision}, state::UpdateCursor};

mod command_ledger;
mod commit_engine;
mod commit_log;
mod handle;
mod reader;

pub(crate) use command_ledger::CommandLedger;
pub(crate) use commit_engine::CommitEngine;
pub use commit_log::CommitLog;
pub use handle::{OutboundEnvelope, Runtime, RuntimeHandle};
pub use reader::{RuntimeReader, SnapshotReadGuard};

pub(crate) struct RuntimeCore {
    next_cursor_id: u64,
    commit_engine: CommitEngine,
    adapters: AdapterRegistry,
    outbound: VecDeque<OutboundEnvelope>,
    command_ledger: CommandLedger,
}

impl RuntimeCore {
    pub(crate) fn new(adapters: AdapterRegistry) -> Self {
        Self {
            next_cursor_id: 1,
            commit_engine: CommitEngine::new(),
            adapters,
            outbound: VecDeque::new(),
            command_ledger: CommandLedger::new(),
        }
    }

    pub(crate) fn next_cursor(&mut self, next_revision: Revision) -> UpdateCursor {
        let cursor_id = CursorId::new(self.next_cursor_id);
        self.next_cursor_id += 1;
        UpdateCursor::new(cursor_id, next_revision)
    }
}
