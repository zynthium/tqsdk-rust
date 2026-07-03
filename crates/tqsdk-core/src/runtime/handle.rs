use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use serde_json::{Map, Value, json};

use crate::{
    adapter::AdapterRegistry,
    commands::{CommandStatus, OutboundDispatch, OutboundRequest, RuntimeCommand},
    error::{ContractError, Result},
    events::{FieldMutation, MutationSource, NormalizedMutation, RuntimeInput},
    ids::{AccountId, CommandId, OrderId, ProtocolDomain, Revision, Symbol},
    order_lifecycle::OrderLifecycle,
    state::{
        CommitScope, ObjectKey, SharedCommitResult, StatePartitionReadGuard, StatePath,
        StateSnapshot, StateStore, UpdateCursor,
    },
    transport::{BootstrapResult, SessionPhase},
};

use super::{
    CommitLog, RuntimeCore, RuntimeReader, SharedState,
    command_ledger::{command_detail_fields_from_command, merged_detail_from_seed},
    commit_engine::{
        CommitEngine, session_lifecycle_mutation, session_snapshot_mutations, sort_field_mutations,
    },
    mutex_lock,
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
pub(crate) struct OutboundEnvelope {
    pub(crate) command_id: CommandId,
    pub(crate) request: OutboundRequest,
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
            state: Arc::new(StateStore::new(Revision::new(0))),
            commit_log: CommitLog::with_retention(max_commit_log_entries),
        }
    }

    pub fn commit_log(&self) -> CommitLog {
        self.commit_log.clone()
    }

    pub(crate) fn recovery_commands(&self) -> Vec<RuntimeCommand> {
        let inner = mutex_lock(&self.inner);
        inner.adapters.recovery_commands()
    }

    pub fn reader(&self) -> RuntimeReader {
        RuntimeReader {
            state: Arc::clone(&self.state),
            commit_log: self.commit_log.clone(),
        }
    }

    pub fn drain_dispatches(&self) -> Result<Vec<OutboundDispatch>> {
        let mut inner = mutex_lock(&self.inner);
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
                let account_id = inner
                    .command_ledger
                    .detail_seed(envelope.command_id)
                    .and_then(dispatch_account_id_from_seed);
                Ok(OutboundDispatch {
                    command_id: envelope.command_id,
                    domain,
                    account_id,
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
    ) -> Result<Option<SharedCommitResult>> {
        let mut inner = mutex_lock(&self.inner);
        let mutations = inner.adapters.decode_input(&input)?;
        self.apply_and_publish_locked(
            &mut inner,
            mutations,
            input_domains(&input),
            caused_by,
            scope,
        )
    }

    pub fn ingest_batch(
        &self,
        inputs: Vec<RuntimeInput>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<SharedCommitResult>> {
        let mut inner = mutex_lock(&self.inner);
        let mut mutations = Vec::new();
        for input in &inputs {
            mutations.extend(inner.adapters.decode_input(input)?);
        }
        self.apply_and_publish_locked(
            &mut inner,
            mutations,
            batch_input_domains(&inputs),
            caused_by,
            scope,
        )
    }

    #[doc(hidden)]
    pub fn ingest_market_quote_fields<I>(
        &self,
        quotes: I,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<SharedCommitResult>>
    where
        I: IntoIterator<Item = (Symbol, Vec<FieldMutation>)>,
    {
        let mutations = quotes
            .into_iter()
            .map(|(symbol, mut fields)| {
                sort_field_mutations(&mut fields);
                NormalizedMutation {
                    path: StatePath::new(["quotes".to_string(), symbol.as_str().to_string()]),
                    object: Some(ObjectKey::Quote { symbol }),
                    fields,
                    source: MutationSource::MarketDiff,
                }
            })
            .collect::<Vec<_>>();
        if mutations.is_empty() {
            return Ok(None);
        }
        self.record_mutations(mutations, vec![ProtocolDomain::Market], caused_by, scope)
    }

    pub fn record_command_status(
        &self,
        command_id: CommandId,
        status: CommandStatus,
        detail: Option<Value>,
        scope: CommitScope,
    ) -> Result<Option<SharedCommitResult>> {
        let mut inner = mutex_lock(&self.inner);
        let domain_from_ledger = inner.command_ledger.domain(command_id);
        let detail_seed_from_ledger = inner.command_ledger.detail_seed(command_id);
        let status_from_ledger = inner.command_ledger.status(command_id);

        let (domain, seed_from_snapshot, status_from_snapshot) =
            if let Some(domain) = domain_from_ledger {
                (Some(domain), None, None)
            } else if inner.command_ledger.is_evicted_terminal(command_id) {
                return Ok(None);
            } else {
                let runtime = self.state.read_runtime_state();
                let domain = command_domain_from_runtime(&runtime, command_id);
                let seed = command_detail_seed_from_runtime(&runtime, command_id);
                let current_status = command_status_from_runtime(&runtime, command_id);
                (domain, seed, current_status)
            };

        let Some(domain) = domain else {
            return Err(ContractError::validation(format!(
                "unknown command id for command status update: {}",
                command_id.get()
            )));
        };
        let current_status = status_from_ledger.or(status_from_snapshot);
        validate_command_status_transition(command_id, current_status, status)?;
        if current_status == Some(status) && status.is_terminal() {
            return Ok(None);
        }

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
            vec![domain],
            vec![command_id],
            scope,
        )?;

        if status.is_terminal() && commit.is_some() {
            inner
                .command_ledger
                .commit_terminal(command_id, evicted_terminal_command_id);
        } else if commit.is_some() {
            inner.command_ledger.update_status(command_id, status);
        }

        Ok(commit)
    }

    pub fn record_session_phase(
        &self,
        phase: SessionPhase,
        detail: Option<Value>,
        caused_by: Vec<CommandId>,
    ) -> Result<Option<SharedCommitResult>> {
        self.record_mutations(
            vec![session_lifecycle_mutation(phase, detail)],
            vec![ProtocolDomain::System],
            caused_by,
            CommitScope::SessionTransition,
        )
    }

    pub fn record_session_bootstrap(
        &self,
        result: &BootstrapResult,
        caused_by: Vec<CommandId>,
    ) -> Result<Option<SharedCommitResult>> {
        self.record_mutations(
            session_snapshot_mutations(result),
            vec![ProtocolDomain::System],
            caused_by,
            CommitScope::InitialReady,
        )
    }

    pub fn record_session_resync(
        &self,
        result: &BootstrapResult,
        caused_by: Vec<CommandId>,
    ) -> Result<Option<SharedCommitResult>> {
        self.record_mutations(
            session_snapshot_mutations(result),
            vec![ProtocolDomain::System],
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
    ) -> Result<Option<SharedCommitResult>> {
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
            vec![ProtocolDomain::System],
            caused_by,
            CommitScope::SessionTransition,
        )
    }

    fn record_mutations(
        &self,
        mutations: Vec<NormalizedMutation>,
        domains: Vec<ProtocolDomain>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<SharedCommitResult>> {
        let mut inner = mutex_lock(&self.inner);
        self.apply_and_publish_locked(&mut inner, mutations, domains, caused_by, scope)
    }

    fn apply_and_publish_locked(
        &self,
        _inner: &mut RuntimeCore,
        mutations: Vec<NormalizedMutation>,
        domains: Vec<ProtocolDomain>,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<SharedCommitResult>> {
        let mutations = if domains_are_pure_market(&domains) {
            mutations
        } else {
            normalize_order_lifecycle_mutations(&self.state, mutations)?
        };
        validate_mutation_domains(&mutations)?;
        let commit = CommitEngine::apply(
            &self.state,
            mutations,
            domains,
            caused_by,
            scope,
            |commit| self.commit_log.publish(commit),
        );
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
            let mut inner = mutex_lock(&this.inner);
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
        self.state.snapshot()
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

fn input_domains(input: &RuntimeInput) -> Vec<ProtocolDomain> {
    match input {
        RuntimeInput::Io(event) => event.domains.clone(),
        RuntimeInput::Replay(_) => vec![ProtocolDomain::Replay],
        RuntimeInput::Auth(_) | RuntimeInput::Timer(_) | RuntimeInput::Internal(_) => {
            vec![ProtocolDomain::System]
        }
    }
}

fn batch_input_domains(inputs: &[RuntimeInput]) -> Vec<ProtocolDomain> {
    let mut domains = Vec::new();

    for input in inputs {
        for domain in input_domains(input) {
            if !domains.contains(&domain) {
                domains.push(domain);
            }
        }
    }

    domains
}

fn domains_are_pure_market(domains: &[ProtocolDomain]) -> bool {
    domains.len() == 1 && domains[0] == ProtocolDomain::Market
}

fn command_domain_from_runtime(
    runtime: &StatePartitionReadGuard<'_>,
    command_id: CommandId,
) -> Option<ProtocolDomain> {
    let command_segment = command_id.get().to_string();
    let domain = runtime
        .get_path(&["commands", command_segment.as_str(), "domain"])?
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

fn command_detail_seed_from_runtime(
    runtime: &StatePartitionReadGuard<'_>,
    command_id: CommandId,
) -> Option<serde_json::Map<String, Value>> {
    let command_segment = command_id.get().to_string();
    runtime
        .get_path(&["commands", command_segment.as_str(), "detail"])
        .and_then(Value::as_object)
        .cloned()
}

fn command_status_from_runtime(
    runtime: &StatePartitionReadGuard<'_>,
    command_id: CommandId,
) -> Option<CommandStatus> {
    let command_segment = command_id.get().to_string();
    runtime
        .get_path(&["commands", command_segment.as_str(), "status"])
        .and_then(Value::as_str)
        .and_then(|status| status.parse().ok())
}

fn validate_command_status_transition(
    command_id: CommandId,
    current: Option<CommandStatus>,
    next: CommandStatus,
) -> Result<()> {
    let Some(current) = current else {
        return Err(ContractError::validation(format!(
            "unknown command status for command status update: {}",
            command_id.get()
        )));
    };

    let valid = match current {
        CommandStatus::Queued => matches!(
            next,
            CommandStatus::Sent
                | CommandStatus::Rejected
                | CommandStatus::Failed
                | CommandStatus::Cancelled
        ),
        CommandStatus::Sent => matches!(
            next,
            CommandStatus::Acked
                | CommandStatus::PartiallyApplied
                | CommandStatus::Completed
                | CommandStatus::Rejected
                | CommandStatus::Failed
                | CommandStatus::Cancelled
        ),
        CommandStatus::Acked => matches!(
            next,
            CommandStatus::PartiallyApplied
                | CommandStatus::Completed
                | CommandStatus::Rejected
                | CommandStatus::Failed
                | CommandStatus::Cancelled
        ),
        CommandStatus::PartiallyApplied => matches!(
            next,
            CommandStatus::Completed
                | CommandStatus::Rejected
                | CommandStatus::Failed
                | CommandStatus::Cancelled
        ),
        CommandStatus::Completed
        | CommandStatus::Rejected
        | CommandStatus::Failed
        | CommandStatus::Cancelled => current == next,
    };

    if valid {
        Ok(())
    } else {
        Err(ContractError::validation(format!(
            "invalid command status transition for command {}: {} -> {}",
            command_id.get(),
            current.as_str(),
            next.as_str()
        )))
    }
}

fn normalize_order_lifecycle_mutations(
    state: &StateStore,
    mutations: Vec<NormalizedMutation>,
) -> Result<Vec<NormalizedMutation>> {
    if !mutations.iter().any(is_trade_order_mutation) {
        return Ok(mutations);
    }

    let trade_guard = state.read_trade_state();
    let mut normalized = Vec::with_capacity(mutations.len());

    for mut mutation in mutations {
        let Some((account_id, order_id)) = trade_order_key(&mutation) else {
            normalized.push(mutation);
            continue;
        };

        let partition_segments = mutation
            .path
            .segments()
            .iter()
            .skip(1)
            .map(String::as_str)
            .collect::<Vec<_>>();
        let current_order = trade_guard.get_path(&partition_segments);
        let current_lifecycle = current_order.and_then(OrderLifecycle::infer_from_order_value);
        let mut next_order = current_order
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new()));
        apply_order_fields_to_value(&mut next_order, &mutation.fields);

        let next_lifecycle = infer_order_lifecycle_after_mutation(&next_order);
        if let Some(next_lifecycle) = next_lifecycle {
            if let Some(current_lifecycle) = current_lifecycle
                && !current_lifecycle.can_transition_to(next_lifecycle)
            {
                return Err(ContractError::validation(format!(
                    "invalid order lifecycle transition for order {}/{}: {} -> {}",
                    account_id.as_str(),
                    order_id.as_str(),
                    current_lifecycle.as_str(),
                    next_lifecycle.as_str()
                )));
            }

            upsert_order_lifecycle_field(&mut mutation.fields, next_lifecycle);
        }

        normalized.push(mutation);
    }

    Ok(normalized)
}

fn is_trade_order_mutation(mutation: &NormalizedMutation) -> bool {
    trade_order_key(mutation).is_some()
}

fn trade_order_key(mutation: &NormalizedMutation) -> Option<(&AccountId, &OrderId)> {
    if mutation.source != MutationSource::TradeReply {
        return None;
    }

    match mutation.object.as_ref()? {
        ObjectKey::Order {
            account_id,
            order_id,
        } => Some((account_id, order_id)),
        _ => None,
    }
}

fn apply_order_fields_to_value(order: &mut Value, fields: &[FieldMutation]) {
    if !order.is_object() {
        *order = Value::Object(Map::new());
    }

    let Some(map) = order.as_object_mut() else {
        return;
    };

    for field in fields {
        if field.value.is_null() {
            map.remove(&field.field);
        } else {
            map.insert(field.field.clone(), field.value.clone());
        }
    }
}

fn infer_order_lifecycle_after_mutation(order: &Value) -> Option<OrderLifecycle> {
    OrderLifecycle::infer_from_order_value_ignoring_lifecycle(order)
}

fn upsert_order_lifecycle_field(fields: &mut Vec<FieldMutation>, lifecycle: OrderLifecycle) {
    let value = json!(lifecycle.as_str());
    if let Some(field) = fields.iter_mut().find(|field| field.field == "lifecycle") {
        field.value = value;
    } else {
        fields.push(FieldMutation {
            field: "lifecycle".to_string(),
            value,
        });
    }
    sort_field_mutations(fields);
}

fn validate_mutation_domains(mutations: &[NormalizedMutation]) -> Result<()> {
    for mutation in mutations {
        let root = mutation
            .path
            .segments()
            .first()
            .map(String::as_str)
            .ok_or_else(|| {
                ContractError::validation(format!(
                    "{} mutation cannot write an empty state path",
                    mutation_source_label(mutation.source)
                ))
            })?;
        if !mutation_source_allows_root(mutation.source, root) {
            return Err(ContractError::validation(format!(
                "{} mutation cannot write state root `{root}`",
                mutation_source_label(mutation.source)
            )));
        }
    }
    Ok(())
}

fn mutation_source_allows_root(source: MutationSource, root: &str) -> bool {
    match source {
        MutationSource::MarketDiff => {
            matches!(
                root,
                "ins_list"
                    | "mdhis_more_data"
                    | "symbols"
                    | "quotes"
                    | "trading_status"
                    | "charts"
                    | "klines"
                    | "ticks"
            )
        }
        MutationSource::TradeReply => root == "trade",
        MutationSource::QueryResult => root == "query",
        MutationSource::SchemaBootstrap => root == "schema",
        MutationSource::ReplayStep => root == "replay",
        MutationSource::SessionControl => matches!(root, "system" | "runtime"),
    }
}

fn mutation_source_label(source: MutationSource) -> &'static str {
    match source {
        MutationSource::MarketDiff => "market",
        MutationSource::TradeReply => "trade",
        MutationSource::QueryResult => "query",
        MutationSource::SchemaBootstrap => "schema",
        MutationSource::ReplayStep => "replay",
        MutationSource::SessionControl => "session control",
    }
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

fn dispatch_account_id_from_seed(seed: &serde_json::Map<String, Value>) -> Option<AccountId> {
    seed.get("account_id")
        .and_then(Value::as_str)
        .map(AccountId::new)
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    };

    use serde_json::json;

    use crate::{
        adapter::AdapterRegistry,
        commands::{
            CommandStatus, RuntimeCommand, SystemCommand, TradeCommand, TradeDirection,
            TradeInsertOrderCommand, TradeOffset, TradePriceType, TradeTimeCondition,
            TradeVolumeCondition,
        },
        events::{
            FieldMutation, InputPayload, IoEvent, MutationSource, NormalizedMutation, RuntimeInput,
        },
        ids::{AccountId, CommandId, OrderId, ProtocolDomain, Revision, Symbol},
        state::{CommitScope, StatePath},
    };

    use super::{Runtime, RuntimeHandle};

    #[test]
    fn market_diff_allows_symbols_root_from_market_sessions() {
        let market_symbols = NormalizedMutation {
            path: StatePath::new(["symbols", "SHFE.au2602"]),
            object: None,
            fields: vec![FieldMutation {
                field: "instrument_name".to_string(),
                value: json!("gold 2602"),
            }],
            source: MutationSource::MarketDiff,
        };

        assert!(super::validate_mutation_domains(&[market_symbols]).is_ok());

        let trade_symbols = NormalizedMutation {
            path: StatePath::new(["symbols", "SHFE.au2602"]),
            object: None,
            fields: vec![FieldMutation {
                field: "instrument_name".to_string(),
                value: json!("gold 2602"),
            }],
            source: MutationSource::TradeReply,
        };

        let err = super::validate_mutation_domains(&[trade_symbols]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "validation error: trade mutation cannot write state root `symbols`"
        );
    }

    #[test]
    fn released_terminal_command_statuses_drop_ledger_metadata_but_remain_idempotent() {
        let handle = runtime_with_default_adapters();

        let command_id = block_on(
            handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
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
            ))),
        )
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
    fn runtime_handle_recovers_from_poisoned_inner_mutex() {
        let handle = runtime_with_default_adapters();
        let poisoned = handle.clone();

        let panic = catch_unwind(AssertUnwindSafe(move || {
            let _guard = poisoned.inner.lock().unwrap();
            panic!("poison runtime mutex");
        }));
        assert!(panic.is_err());

        let command_id =
            block_on(handle.submit(RuntimeCommand::System(SystemCommand::RefreshAuth))).unwrap();
        assert_eq!(command_id.get(), 1);
    }

    #[test]
    fn runtime_handle_recovers_from_poisoned_state_lock() {
        let handle = runtime_with_default_adapters();
        let poisoned = handle.clone();

        let panic = catch_unwind(AssertUnwindSafe(move || {
            poisoned.state.poison_partition_for_test("runtime");
        }));
        assert!(panic.is_err());

        assert_eq!(handle.latest_snapshot().revision(), Revision::new(0));
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

    #[test]
    fn typed_market_quote_ingest_matches_json_quote_fast_path() {
        let json_handle = runtime_with_default_adapters();
        let typed_handle = runtime_with_default_adapters();

        let json_commit = json_handle
            .ingest(
                RuntimeInput::Io(IoEvent {
                    route: "backtest".to_string(),
                    domains: vec![ProtocolDomain::Market],
                    payload: InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "quotes": {
                                "SHFE.rb2601": {
                                    "datetime": "1781182800000000000",
                                    "last_price": 3012.5,
                                    "volume": 12
                                }
                            }
                        }]
                    })),
                }),
                vec![],
                CommitScope::ReplayStep,
            )
            .unwrap()
            .expect("json quote ingest should publish a commit");

        let typed_commit = typed_handle
            .ingest_market_quote_fields(
                [(
                    Symbol::new("SHFE.rb2601"),
                    vec![
                        FieldMutation {
                            field: "volume".to_string(),
                            value: json!(12),
                        },
                        FieldMutation {
                            field: "datetime".to_string(),
                            value: json!("1781182800000000000"),
                        },
                        FieldMutation {
                            field: "last_price".to_string(),
                            value: json!(3012.5),
                        },
                    ],
                )],
                vec![],
                CommitScope::ReplayStep,
            )
            .unwrap()
            .expect("typed quote ingest should publish a commit");

        assert_eq!(typed_commit.domains, json_commit.domains);
        assert_eq!(typed_commit.scope, json_commit.scope);
        assert_eq!(typed_commit.changes, json_commit.changes);
        assert_eq!(
            typed_handle
                .latest_snapshot()
                .get(["quotes", "SHFE.rb2601", "last_price"]),
            json_handle
                .latest_snapshot()
                .get(["quotes", "SHFE.rb2601", "last_price"])
        );
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
        let command_id = block_on(
            handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
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
            ))),
        )
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
        // SAFETY: the static null-data waker owns no resources and is only
        // used to poll test futures that are expected to complete synchronously.
        unsafe { Waker::from_raw(noop_raw_waker()) }
    }

    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
    }

    unsafe fn noop_clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }

    unsafe fn noop(_: *const ()) {}

    static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
}
