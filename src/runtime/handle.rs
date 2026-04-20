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
    ids::{CommandId, ProtocolDomain, Revision},
    state::{CommitResult, CommitScope, ObjectKey, StatePath, StateSnapshot, UpdateCursor},
    transport::{BootstrapResult, SessionPhase},
};

use super::{
    CommitLog, RuntimeCore, RuntimeReader, SharedState,
    command_ledger::{command_detail_fields_from_command, merged_detail_from_seed},
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
        Self::with_adapters_and_retention_limits(
            adapters,
            8_192,
            super::CommandLedger::DEFAULT_MAX_RETAINED_TERMINAL_COMMANDS,
        )
    }

    pub fn with_adapters_and_commit_log_retention(
        adapters: AdapterRegistry,
        max_commit_log_entries: usize,
    ) -> Self {
        Self::with_adapters_and_retention_limits(
            adapters,
            max_commit_log_entries,
            super::CommandLedger::DEFAULT_MAX_RETAINED_TERMINAL_COMMANDS,
        )
    }

    pub fn with_adapters_and_retention_limits(
        adapters: AdapterRegistry,
        max_commit_log_entries: usize,
        max_retained_terminal_commands: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RuntimeCore::new(
                adapters,
                max_retained_terminal_commands,
            ))),
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
        let domain_from_ledger = inner.command_ledger.domain(command_id);
        let detail_seed_from_ledger = inner.command_ledger.detail_seed(command_id);

        let (domain, seed_from_snapshot) = if let Some(domain) = domain_from_ledger {
            (Some(domain), None)
        } else if inner.command_ledger.is_evicted_terminal(command_id) {
            return Ok(None);
        } else {
            let snapshot_guard = self.state.read().expect("runtime state rwlock poisoned");
            let snapshot = snapshot_guard.read();
            let domain = command_domain_from_snapshot(snapshot, command_id);
            let seed = command_detail_seed_from_snapshot(snapshot, command_id);
            drop(snapshot_guard);
            (domain, seed)
        };

        let Some(domain) = domain else {
            return Err(ContractError::validation(format!(
                "unknown command id for command status update: {}",
                command_id.get()
            )));
        };
        let detail = merged_detail_from_seed(
            detail_seed_from_ledger.or(seed_from_snapshot.as_ref()),
            detail,
        );

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

        let evicted_terminal_command_id = status
            .is_terminal()
            .then(|| inner.command_ledger.pending_terminal_eviction(command_id))
            .flatten();

        let mut mutations = vec![NormalizedMutation {
            path: StatePath::new(vec![
                "runtime".to_string(),
                "commands".to_string(),
                command_segment,
            ]),
            object: Some(ObjectKey::Command { command_id }),
            fields,
            source: MutationSource::SessionControl,
        }];
        if let Some(evicted_command_id) = evicted_terminal_command_id {
            mutations.push(command_cleanup_mutation(evicted_command_id));
        }

        let commit = self.apply_and_publish_locked(
            &mut inner,
            mutations,
            vec![command_id],
            scope,
        )?;

        if status.is_terminal() && commit.is_some() {
            inner
                .command_ledger
                .commit_terminal(command_id, evicted_terminal_command_id);
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

fn command_domain_from_snapshot(
    snapshot: crate::state::StateReadView<'_>,
    command_id: CommandId,
) -> Option<ProtocolDomain> {
    let command_segment = command_id.get().to_string();
    let domain = snapshot
        .get(["runtime", "commands", command_segment.as_str(), "domain"])?
        .as_str()?;
    match domain {
        "system" => Some(ProtocolDomain::System),
        "market" => Some(ProtocolDomain::Market),
        "trade" => Some(ProtocolDomain::Trade),
        "replay" => Some(ProtocolDomain::Replay),
        "query" => Some(ProtocolDomain::Query),
        "schema" => Some(ProtocolDomain::Schema),
        _ => None,
    }
}

fn command_detail_seed_from_snapshot(
    snapshot: crate::state::StateReadView<'_>,
    command_id: CommandId,
) -> Option<serde_json::Map<String, Value>> {
    let command_segment = command_id.get().to_string();
    snapshot
        .get(["runtime", "commands", command_segment.as_str(), "detail"])
        .and_then(Value::as_object)
        .cloned()
}

fn command_cleanup_mutation(command_id: CommandId) -> NormalizedMutation {
    NormalizedMutation {
        path: StatePath::new(vec![
            "runtime".to_string(),
            "commands".to_string(),
            command_id.get().to_string(),
        ]),
        object: Some(ObjectKey::Command { command_id }),
        fields: vec![
            FieldMutation {
                field: "detail".to_string(),
                value: Value::Null,
            },
            FieldMutation {
                field: "domain".to_string(),
                value: Value::Null,
            },
            FieldMutation {
                field: "status".to_string(),
                value: Value::Null,
            },
        ],
        source: MutationSource::SessionControl,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    };

    use serde_json::json;

    use crate::{
        adapter::AdapterRegistry,
        commands::{
            CommandStatus, RuntimeCommand, TradeCommand, TradeDirection, TradeInsertOrderCommand,
            TradeOffset, TradePriceType, TradeTimeCondition, TradeVolumeCondition,
        },
        ids::{AccountId, CommandId, OrderId, ProtocolDomain, Symbol},
        state::CommitScope,
    };

    use super::{Runtime, RuntimeHandle};

    #[test]
    fn released_terminal_command_statuses_drop_ledger_metadata_but_remain_idempotent() {
        let handle = runtime_with_default_adapters();

        let command_id = block_on(handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new("order-1"),
                symbol: Symbol::new("SHFE.au2602"),
                direction: TradeDirection::Buy,
                offset: Some(TradeOffset::Open),
                volume: 2,
                price_type: TradePriceType::Limit,
                limit_price: Some(json!(618.5)),
                time_condition: TradeTimeCondition::Gfd,
                volume_condition: TradeVolumeCondition::Any,
            },
        ))))
        .unwrap();

        assert_eq!(
            handle
                .inner
                .lock()
                .expect("runtime mutex poisoned")
                .command_ledger
                .domain(command_id),
            Some(ProtocolDomain::Trade)
        );

        handle
            .record_command_status(
                command_id,
                CommandStatus::Rejected,
                Some(json!({"reason": "insufficient_margin"})),
                CommitScope::RealtimeUpdate,
            )
            .unwrap()
            .expect("first terminal status should publish a commit");

        assert_eq!(
            handle
                .inner
                .lock()
                .expect("runtime mutex poisoned")
                .command_ledger
                .domain(command_id),
            None
        );

        let repeated = handle
            .record_command_status(
                command_id,
                CommandStatus::Rejected,
                Some(json!({"reason": "insufficient_margin"})),
                CommitScope::RealtimeUpdate,
            )
            .unwrap();
        assert_eq!(repeated, None);
    }

    #[test]
    fn terminal_command_state_retention_prunes_old_entries_but_keeps_idempotence() {
        let handle = runtime_with_terminal_command_retention(1);

        let first_command_id = submit_rejected_trade_command(&handle, "order-1");
        let second_command_id = submit_rejected_trade_command(&handle, "order-2");

        let first_command_segment = first_command_id.get().to_string();
        let second_command_segment = second_command_id.get().to_string();

        assert_eq!(
            handle
                .latest_snapshot()
                .get(["runtime", "commands", first_command_segment.as_str()]),
            None
        );
        assert_eq!(
            handle.latest_snapshot().get([
                "runtime",
                "commands",
                first_command_segment.as_str(),
                "status"
            ]),
            None
        );
        assert_eq!(
            handle.latest_snapshot().get([
                "runtime",
                "commands",
                second_command_segment.as_str(),
                "status"
            ]),
            Some(&json!("rejected"))
        );

        let repeated = handle
            .record_command_status(
                first_command_id,
                CommandStatus::Rejected,
                Some(json!({"reason": "insufficient_margin"})),
                CommitScope::RealtimeUpdate,
            )
            .unwrap();
        assert_eq!(repeated, None);
    }

    fn runtime_with_default_adapters() -> RuntimeHandle {
        let mut registry = AdapterRegistry::new();
        registry.register_default_adapters();
        RuntimeHandle::with_adapters(registry)
    }

    fn runtime_with_terminal_command_retention(
        max_retained_terminal_commands: usize,
    ) -> RuntimeHandle {
        let mut registry = AdapterRegistry::new();
        registry.register_default_adapters();
        RuntimeHandle::with_adapters_and_retention_limits(
            registry,
            8_192,
            max_retained_terminal_commands,
        )
    }

    fn submit_rejected_trade_command(handle: &RuntimeHandle, order_id: &str) -> CommandId {
        let command_id = block_on(handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new(order_id),
                symbol: Symbol::new("SHFE.au2602"),
                direction: TradeDirection::Buy,
                offset: Some(TradeOffset::Open),
                volume: 2,
                price_type: TradePriceType::Limit,
                limit_price: Some(json!(618.5)),
                time_condition: TradeTimeCondition::Gfd,
                volume_condition: TradeVolumeCondition::Any,
            },
        ))))
        .unwrap();

        handle
            .record_command_status(
                command_id,
                CommandStatus::Rejected,
                Some(json!({"reason": "insufficient_margin"})),
                CommitScope::RealtimeUpdate,
            )
            .unwrap()
            .expect("terminal status should publish a commit");

        command_id
    }

    fn block_on<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let mut future = Pin::from(Box::new(future));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn noop_waker() -> Waker {
        unsafe { Waker::from_raw(noop_raw_waker()) }
    }

    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
    }

    unsafe fn noop_clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }

    unsafe fn noop(_: *const ()) {}

    static NOOP_WAKER_VTABLE: RawWakerVTable =
        RawWakerVTable::new(noop_clone, noop, noop, noop);
}
