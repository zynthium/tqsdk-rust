use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
};

use crate::{
    adapter::AdapterRegistry,
    commands::{CommandStatus, OutboundRequest, RuntimeCommand},
    events::{FieldMutation, MutationSource, NormalizedMutation, RuntimeInput},
    error::{ContractError, Result},
    ids::{CommandId, CursorId, ProtocolDomain, Revision},
    state::{ChangeSet, CommitResult, CommitScope, ObjectKey, StatePath, StateSnapshot, UpdateCursor},
    transport::{BootstrapResult, SessionPhase, SessionRoute, SessionRouteEndpoint, SessionTarget},
};
use serde_json::{Value, json};

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
    command_domains: BTreeMap<CommandId, ProtocolDomain>,
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
                command_domains: BTreeMap::new(),
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
        let commit = self.build_commit(&mut inner, mutations, caused_by, scope);
        drop(inner);

        if let Some(commit) = commit.clone() {
            self.commit_log.publish(commit);
        }
        Ok(commit)
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

        let commit = self.build_commit(&mut inner, mutations, caused_by, scope);
        drop(inner);

        if let Some(commit) = commit.clone() {
            self.commit_log.publish(commit);
        }
        Ok(commit)
    }

    pub fn record_command_status(
        &self,
        command_id: CommandId,
        status: CommandStatus,
        detail: Option<Value>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let Some(domain) = inner.command_domains.get(&command_id).copied() else {
            return Err(ContractError::validation(format!(
                "unknown command id for command status update: {}",
                command_id.get()
            )));
        };

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
                value: detail.unwrap_or(Value::Null),
            },
        ];
        fields.sort_by(|left, right| left.field.cmp(&right.field));

        let commit = self.build_commit(
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
        );
        drop(inner);

        if let Some(commit) = commit.clone() {
            self.commit_log.publish(commit);
        }
        Ok(commit)
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

    fn record_mutations(
        &self,
        mutations: Vec<NormalizedMutation>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let commit = self.build_commit(&mut inner, mutations, caused_by, scope);
        drop(inner);

        if let Some(commit) = commit.clone() {
            self.commit_log.publish(commit);
        }
        Ok(commit)
    }

    fn build_commit(
        &self,
        inner: &mut RuntimeCore,
        mutations: Vec<NormalizedMutation>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Option<CommitResult> {
        if mutations.is_empty() {
            return None;
        }

        let next_revision = Revision::new(inner.snapshot.revision().get() + 1);
        let applied = inner.snapshot.apply(next_revision, &mutations);
        if applied.is_empty() {
            return None;
        }

        let changes = ChangeSet::from_mutations(&applied);
        Some(CommitResult::new(next_revision, changes, caused_by, scope))
    }
}

fn session_snapshot_mutations(result: &BootstrapResult) -> Vec<NormalizedMutation> {
    vec![
        session_auth_mutation(result),
        session_lifecycle_mutation(result.phase, None),
        session_topology_mutation(result),
    ]
}

fn session_auth_mutation(result: &BootstrapResult) -> NormalizedMutation {
    let mut fields = vec![
        FieldMutation {
            field: "access_token_present".to_string(),
            value: json!(!result.auth.access_token().is_empty()),
        },
        FieldMutation {
            field: "auth_id".to_string(),
            value: result
                .auth
                .auth_id()
                .map(|auth_id| json!(auth_id.as_str()))
                .unwrap_or(Value::Null),
        },
        FieldMutation {
            field: "features".to_string(),
            value: json!(result.auth.features()),
        },
    ];
    sort_field_mutations(&mut fields);

    NormalizedMutation {
        path: StatePath::new(["system", "auth", "context"]),
        object: Some(ObjectKey::SessionAuth),
        fields,
        source: MutationSource::SessionControl,
    }
}

fn session_lifecycle_mutation(phase: SessionPhase, detail: Option<Value>) -> NormalizedMutation {
    let mut fields = vec![
        FieldMutation {
            field: "detail".to_string(),
            value: detail.unwrap_or(Value::Null),
        },
        FieldMutation {
            field: "phase".to_string(),
            value: json!(phase.as_str()),
        },
    ];
    sort_field_mutations(&mut fields);

    NormalizedMutation {
        path: StatePath::new(["system", "session", "lifecycle"]),
        object: Some(ObjectKey::SessionLifecycle),
        fields,
        source: MutationSource::SessionControl,
    }
}

fn session_topology_mutation(result: &BootstrapResult) -> NormalizedMutation {
    let mut fields = vec![
        FieldMutation {
            field: "enabled_domains".to_string(),
            value: json!(
                result
                    .enabled_domains
                    .iter()
                    .copied()
                    .map(ProtocolDomain::as_str)
                    .collect::<Vec<_>>()
            ),
        },
        FieldMutation {
            field: "routes".to_string(),
            value: Value::Array(result.topology.routes.iter().map(normalize_session_route).collect()),
        },
    ];
    sort_field_mutations(&mut fields);

    NormalizedMutation {
        path: StatePath::new(["system", "session", "topology"]),
        object: Some(ObjectKey::SessionTopology),
        fields,
        source: MutationSource::SessionControl,
    }
}

fn normalize_session_route(route: &SessionRoute) -> Value {
    json!({
        "label": route.label,
        "target": normalize_session_target(&route.target),
        "domains": route.domains.iter().copied().map(ProtocolDomain::as_str).collect::<Vec<_>>(),
        "endpoint": normalize_session_endpoint(&route.endpoint),
    })
}

fn normalize_session_target(target: &SessionTarget) -> Value {
    match target {
        SessionTarget::Shared => json!({ "kind": "shared" }),
        SessionTarget::Account(account_id) => json!({
            "kind": "account",
            "account_id": account_id.as_str(),
        }),
        SessionTarget::Replay(session_id) => json!({
            "kind": "replay",
            "session_id": session_id.as_str(),
        }),
    }
}

fn normalize_session_endpoint(endpoint: &SessionRouteEndpoint) -> Value {
    match endpoint {
        SessionRouteEndpoint::WebSocket { url, .. } => json!({
            "kind": "websocket",
            "url": url,
        }),
        SessionRouteEndpoint::Http { url } => json!({
            "kind": "http",
            "url": url,
        }),
        SessionRouteEndpoint::Replay { label } => json!({
            "kind": "replay",
            "label": label,
        }),
        SessionRouteEndpoint::Internal { label } => json!({
            "kind": "internal",
            "label": label,
        }),
    }
}

fn sort_field_mutations(fields: &mut [FieldMutation]) {
    fields.sort_by(|left, right| left.field.cmp(&right.field));
}

impl Runtime for RuntimeHandle {
    fn submit(&self, cmd: RuntimeCommand) -> impl Future<Output = Result<CommandId>> + Send {
        let this = self.clone();
        async move {
            let mut inner = this.inner.lock().expect("runtime mutex poisoned");
            let outbound = inner.adapters.encode_command(&cmd)?;
            let command_id = CommandId::new(inner.next_command_id);
            inner.next_command_id += 1;
            inner.command_domains.insert(command_id, cmd.domain());

            for request in outbound {
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
