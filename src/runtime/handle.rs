use std::{
    future::Future,
    sync::{Arc, Mutex, RwLock},
};

use serde_json::{Value, json};

use crate::{
    adapter::AdapterRegistry,
    commands::{CommandStatus, OutboundDispatch, OutboundRequest, RuntimeCommand},
    error::{ContractError, Result},
    events::{FieldMutation, MutationSource, NormalizedMutation, RuntimeInput},
    ids::{CommandId, Revision},
    state::{CommitResult, CommitScope, ObjectKey, StatePath, StateSnapshot, UpdateCursor},
    transport::{BootstrapResult, SessionPhase},
};

use super::{
    CommitLog, RuntimeCore, RuntimeReader, SharedState,
    command_ledger::command_detail_fields_from_command,
    commit_engine::{
        CommitEngine, session_lifecycle_mutation, session_snapshot_mutations, sort_field_mutations,
    },
};

/// Low-level runtime contract.
///
/// `reader()` is the canonical read-side entry point. `latest_snapshot()` and
/// `cursor()` remain available as compatibility helpers for detached snapshots
/// and legacy consumers.
pub trait Runtime {
    fn submit(&self, cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send;
    fn reader(&self) -> RuntimeReader;
    fn latest_snapshot(&self) -> StateSnapshot;
    fn cursor(&self) -> UpdateCursor;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundEnvelope {
    pub command_id: CommandId,
    pub request: OutboundRequest,
}

/// Mutable runtime owner for command submission, input ingestion, and commit publication.
#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<Mutex<RuntimeCore>>,
    state: SharedState,
    commit_log: CommitLog,
}

impl RuntimeHandle {
    pub fn new() -> Self {
        Self::with_adapters(AdapterRegistry::new())
    }

    pub fn with_adapters(adapters: AdapterRegistry) -> Self {
        Self::with_adapters_and_commit_log_retention(adapters, 8_192)
    }

    pub fn with_adapters_and_commit_log_retention(
        adapters: AdapterRegistry,
        max_commit_log_entries: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RuntimeCore::new(adapters))),
            state: Arc::new(RwLock::new(StateSnapshot::new(Revision::new(0)))),
            commit_log: CommitLog::with_retention(max_commit_log_entries),
        }
    }

    pub fn commit_log(&self) -> CommitLog {
        self.commit_log.clone()
    }

    pub fn reader(&self) -> RuntimeReader {
        RuntimeReader {
            state: Arc::clone(&self.state),
            commit_log: self.commit_log.clone(),
        }
    }

    pub fn drain_outbound(&self) -> Vec<OutboundEnvelope> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        inner.outbound.drain(..).collect()
    }

    pub fn drain_dispatches(&self) -> Result<Vec<OutboundDispatch>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let envelopes = inner.outbound.drain(..).collect::<Vec<_>>();
        envelopes
            .into_iter()
            .map(|envelope| {
                let domain = inner
                    .command_ledger
                    .domain(envelope.command_id)
                    .ok_or_else(|| {
                        ContractError::validation(format!(
                            "unknown command id for outbound dispatch: {}",
                            envelope.command_id.get()
                        ))
                    })?;
                Ok(OutboundDispatch {
                    command_id: envelope.command_id,
                    domain,
                    request: envelope.request,
                })
            })
            .collect()
    }

    pub fn cursor_from(&self, next_revision: Revision) -> UpdateCursor {
        self.commit_log.new_cursor(next_revision)
    }

    pub fn ingest(
        &self,
        input: RuntimeInput,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let mutations = inner.adapters.decode_input(&input)?;
        self.apply_and_publish_locked(&mut inner, mutations, caused_by, scope)
    }

    pub fn ingest_batch(
        &self,
        inputs: Vec<RuntimeInput>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let mut mutations = Vec::new();
        for input in &inputs {
            mutations.extend(inner.adapters.decode_input(input)?);
        }
        self.apply_and_publish_locked(&mut inner, mutations, caused_by, scope)
    }

    pub fn record_command_status(
        &self,
        command_id: CommandId,
        status: CommandStatus,
        detail: Option<Value>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let Some(domain) = inner.command_ledger.domain(command_id) else {
            return Err(ContractError::validation(format!(
                "unknown command id for command status update: {}",
                command_id.get()
            )));
        };
        let detail = inner.command_ledger.merged_detail(command_id, detail);

        let command_segment = command_id.get().to_string();
        let mut fields = vec![
            FieldMutation {
                field: "domain".to_string(),
                value: json!(domain.as_str()),
            },
            FieldMutation {
                field: "status".to_string(),
                value: json!(status.as_str()),
            },
            FieldMutation {
                field: "detail".to_string(),
                value: detail,
            },
        ];
        fields.sort_by(|left, right| left.field.cmp(&right.field));

        self.apply_and_publish_locked(
            &mut inner,
            vec![NormalizedMutation {
                path: StatePath::new(vec![
                    "runtime".to_string(),
                    "commands".to_string(),
                    command_segment,
                ]),
                object: Some(ObjectKey::Command { command_id }),
                fields,
                source: MutationSource::SessionControl,
            }],
            vec![command_id],
            scope,
        )
    }

    pub fn record_session_phase(
        &self,
        phase: SessionPhase,
        detail: Option<Value>,
        caused_by: Vec<CommandId>,
    ) -> Result<Option<CommitResult>> {
        self.record_mutations(
            vec![session_lifecycle_mutation(phase, detail)],
            caused_by,
            CommitScope::SessionTransition,
        )
    }

    pub fn record_session_bootstrap(
        &self,
        result: &BootstrapResult,
        caused_by: Vec<CommandId>,
    ) -> Result<Option<CommitResult>> {
        self.record_mutations(
            session_snapshot_mutations(result),
            caused_by,
            CommitScope::InitialReady,
        )
    }

    pub fn record_session_resync(
        &self,
        result: &BootstrapResult,
        caused_by: Vec<CommandId>,
    ) -> Result<Option<CommitResult>> {
        self.record_mutations(
            session_snapshot_mutations(result),
            caused_by,
            CommitScope::ResyncRecovery,
        )
    }

    pub fn record_session_reconnect(
        &self,
        attempt: u32,
        scheduled_backoff_ms: u64,
        max_attempts: Option<u32>,
        exhausted: bool,
        detail: Option<Value>,
        caused_by: Vec<CommandId>,
    ) -> Result<Option<CommitResult>> {
        let mut fields = vec![
            FieldMutation {
                field: "attempt".to_string(),
                value: json!(attempt),
            },
            FieldMutation {
                field: "scheduled_backoff_ms".to_string(),
                value: json!(scheduled_backoff_ms),
            },
            FieldMutation {
                field: "max_attempts".to_string(),
                value: max_attempts.map_or(Value::Null, |value| json!(value)),
            },
            FieldMutation {
                field: "exhausted".to_string(),
                value: json!(exhausted),
            },
            FieldMutation {
                field: "detail".to_string(),
                value: detail.unwrap_or(Value::Null),
            },
        ];
        sort_field_mutations(&mut fields);

        self.record_mutations(
            vec![NormalizedMutation {
                path: StatePath::new(["system", "session", "reconnect"]),
                object: Some(ObjectKey::SessionReconnect),
                fields,
                source: MutationSource::SessionControl,
            }],
            caused_by,
            CommitScope::SessionTransition,
        )
    }

    fn record_mutations(
        &self,
        mutations: Vec<NormalizedMutation>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        self.apply_and_publish_locked(&mut inner, mutations, caused_by, scope)
    }

    fn apply_and_publish_locked(
        &self,
        _inner: &mut RuntimeCore,
        mutations: Vec<NormalizedMutation>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let mut snapshot = self.state.write().expect("runtime state rwlock poisoned");
        let commit = CommitEngine::apply(&mut snapshot, mutations, caused_by, scope);
        if let Some(commit_ref) = commit.as_ref() {
            self.commit_log.publish(commit_ref.clone());
        }
        Ok(commit)
    }
}

impl Default for RuntimeHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime for RuntimeHandle {
    fn submit(&self, cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send {
        let this = self.clone();
        async move {
            let mut inner = this.inner.lock().expect("runtime mutex poisoned");
            let detail_seed = command_detail_fields_from_command(&cmd);
            let outbound = inner.adapters.encode_command(&cmd)?;
            let command_id = inner.command_ledger.allocate(cmd.domain(), detail_seed);

            for request in outbound {
                inner.outbound.push_back(OutboundEnvelope {
                    command_id,
                    request,
                });
            }

            Ok(command_id)
        }
    }

    fn reader(&self) -> RuntimeReader {
        self.reader()
    }

    fn latest_snapshot(&self) -> StateSnapshot {
        self.state
            .read()
            .expect("runtime state rwlock poisoned")
            .clone()
    }

    fn cursor(&self) -> UpdateCursor {
        let next_revision = Revision::new(
            self.commit_log
                .head_revision()
                .map_or(1, |revision| revision.get() + 1),
        );
        self.cursor_from(next_revision)
    }
}
