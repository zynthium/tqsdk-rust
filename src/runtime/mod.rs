use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use crate::{adapter::AdapterRegistry, state::StateStore};

mod command_ledger;
mod commit_engine;
mod commit_log;
mod handle;
mod reader;

pub(crate) use command_ledger::CommandLedger;
pub use commit_log::CommitLog;
pub use handle::{OutboundEnvelope, Runtime, RuntimeHandle};
pub use reader::{CommitReadGuard, CursorLagged, RuntimeReader, SnapshotReadGuard};

pub(crate) struct RuntimeCore {
    adapters: AdapterRegistry,
    outbound: VecDeque<OutboundEnvelope>,
    command_ledger: CommandLedger,
}

pub(crate) type SharedState = Arc<RwLock<StateStore>>;

impl RuntimeCore {
    pub(crate) fn new(adapters: AdapterRegistry) -> Self {
        Self {
            adapters,
            outbound: VecDeque::new(),
            command_ledger: CommandLedger::new(),
        }
    }
}
