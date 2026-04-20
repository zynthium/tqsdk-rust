use std::{
    collections::VecDeque,
    sync::{Arc, LockResult, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
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
    pub(crate) fn new(adapters: AdapterRegistry, max_retained_terminal_commands: usize) -> Self {
        Self {
            adapters,
            outbound: VecDeque::new(),
            command_ledger: CommandLedger::with_retention(max_retained_terminal_commands),
        }
    }
}

fn recover_poisoned_lock<G>(result: LockResult<G>) -> G {
    // Stability-first substrate: once a lock is poisoned, keep the contract
    // usable and let higher layers observe state explicitly instead of
    // permanently panicking every caller.
    match result {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    recover_poisoned_lock(mutex.lock())
}

pub(crate) fn rwlock_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    recover_poisoned_lock(lock.read())
}

pub(crate) fn rwlock_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    recover_poisoned_lock(lock.write())
}
