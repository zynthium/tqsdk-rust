use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
};

use crate::{
    adapter::AdapterRegistry,
    commands::{OutboundRequest, RuntimeCommand},
    events::RuntimeInput,
    error::Result,
    ids::{CommandId, CursorId, Revision},
    state::{ChangeSet, CommitResult, CommitScope, StateSnapshot, UpdateCursor},
};

pub trait Runtime {
    fn submit(&self, cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send;
    fn latest_snapshot(&self) -> StateSnapshot;
    fn cursor(&self) -> UpdateCursor;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundEnvelope {
    pub command_id: CommandId,
    pub request: OutboundRequest,
}

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

struct RuntimeCore {
    next_command_id: u64,
    next_cursor_id: u64,
    snapshot: StateSnapshot,
    adapters: AdapterRegistry,
    outbound: VecDeque<OutboundEnvelope>,
}

#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<Mutex<RuntimeCore>>,
    commit_log: CommitLog,
}

impl RuntimeHandle {
    pub fn new() -> Self {
        Self::with_adapters(AdapterRegistry::new())
    }

    pub fn with_adapters(adapters: AdapterRegistry) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RuntimeCore {
                next_command_id: 1,
                next_cursor_id: 1,
                snapshot: StateSnapshot::new(Revision::new(0)),
                adapters,
                outbound: VecDeque::new(),
            })),
            commit_log: CommitLog::new(),
        }
    }

    pub fn commit_log(&self) -> CommitLog {
        self.commit_log.clone()
    }

    pub fn drain_outbound(&self) -> Vec<OutboundEnvelope> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        inner.outbound.drain(..).collect()
    }

    pub fn cursor_from(&self, next_revision: Revision) -> UpdateCursor {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let cursor_id = CursorId::new(inner.next_cursor_id);
        inner.next_cursor_id += 1;
        UpdateCursor::new(cursor_id, next_revision)
    }

    pub fn ingest(
        &self,
        input: RuntimeInput,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let mutations = inner.adapters.decode_input(&input)?;
        if mutations.is_empty() {
            return Ok(None);
        }

        let next_revision = Revision::new(inner.snapshot.revision().get() + 1);
        let changes = ChangeSet::from_mutations(&mutations);
        inner.snapshot.apply(next_revision, &mutations);
        let commit = CommitResult::new(next_revision, changes, caused_by, scope);
        drop(inner);

        self.commit_log.publish(commit.clone());
        Ok(Some(commit))
    }
}

impl Runtime for RuntimeHandle {
    fn submit(&self, cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send {
        let this = self.clone();
        async move {
            let mut inner = this.inner.lock().expect("runtime mutex poisoned");
            let command_id = CommandId::new(inner.next_command_id);
            inner.next_command_id += 1;

            for request in inner.adapters.encode_command(&cmd)? {
                inner.outbound.push_back(OutboundEnvelope { command_id, request });
            }

            Ok(command_id)
        }
    }

    fn latest_snapshot(&self) -> StateSnapshot {
        self.inner
            .lock()
            .expect("runtime mutex poisoned")
            .snapshot
            .clone()
    }

    fn cursor(&self) -> UpdateCursor {
        let next_revision = Revision::new(self.commit_log.head_revision().map_or(1, |revision| revision.get() + 1));
        self.cursor_from(next_revision)
    }
}
