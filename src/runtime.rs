use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
};

use crate::{
    adapter::AdapterRegistry,
    commands::{
        CommandStatus, MarketCommand, OutboundDispatch, OutboundRequest, QueryCommand,
        ReplayCommand, RuntimeCommand, SchemaCommand, SystemCommand, TradeCommand,
    },
    error::{ContractError, Result},
    events::{FieldMutation, MutationSource, NormalizedMutation, RuntimeInput},
    ids::{CommandId, CursorId, ProtocolDomain, Revision},
    state::{
        ChangeSet, CommitResult, CommitScope, ObjectKey, StatePath, StateSnapshot, UpdateCursor,
    },
    transport::{BootstrapResult, SessionPhase, SessionRoute, SessionRouteEndpoint, SessionTarget},
};
use serde_json::{Map, Value, json};

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
    command_detail_seeds: BTreeMap<CommandId, Map<String, Value>>,
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
                command_detail_seeds: BTreeMap::new(),
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

    pub fn drain_dispatches(&self) -> Result<Vec<OutboundDispatch>> {
        let mut inner = self.inner.lock().expect("runtime mutex poisoned");
        let envelopes = inner.outbound.drain(..).collect::<Vec<_>>();
        envelopes
            .into_iter()
            .map(|envelope| {
                let domain = inner
                    .command_domains
                    .get(&envelope.command_id)
                    .copied()
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
        let detail = merge_command_detail(inner.command_detail_seeds.get(&command_id), detail);

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

impl Default for RuntimeHandle {
    fn default() -> Self {
        Self::new()
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
            value: Value::Array(
                result
                    .topology
                    .routes
                    .iter()
                    .map(normalize_session_route)
                    .collect(),
            ),
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
            let detail_seed = command_detail_fields_from_command(&cmd);
            let outbound = inner.adapters.encode_command(&cmd)?;
            let command_id = CommandId::new(inner.next_command_id);
            inner.next_command_id += 1;
            inner.command_domains.insert(command_id, cmd.domain());
            if !detail_seed.is_empty() {
                inner.command_detail_seeds.insert(command_id, detail_seed);
            }

            for request in outbound {
                inner.outbound.push_back(OutboundEnvelope {
                    command_id,
                    request,
                });
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
        let next_revision = Revision::new(
            self.commit_log
                .head_revision()
                .map_or(1, |revision| revision.get() + 1),
        );
        self.cursor_from(next_revision)
    }
}

fn merge_command_detail(seed: Option<&Map<String, Value>>, detail: Option<Value>) -> Value {
    let mut merged = seed.cloned().unwrap_or_default();

    match detail {
        Some(Value::Object(fields)) => {
            merged.extend(fields);
            Value::Object(merged)
        }
        Some(value) if merged.is_empty() => value,
        Some(value) => {
            merged.insert("value".to_string(), value);
            Value::Object(merged)
        }
        None if merged.is_empty() => Value::Null,
        None => Value::Object(merged),
    }
}

fn command_detail_fields_from_command(cmd: &RuntimeCommand) -> Map<String, Value> {
    let mut detail = Map::new();

    match cmd {
        RuntimeCommand::System(SystemCommand::Shutdown) => {
            detail.insert("label".to_string(), json!("shutdown-runtime"));
        }
        RuntimeCommand::System(SystemCommand::RefreshAuth) => {
            detail.insert("label".to_string(), json!("refresh-auth"));
        }
        RuntimeCommand::Market(MarketCommand::SubscribeQuotes { symbols }) => {
            detail.insert("aid".to_string(), json!("subscribe_quote"));
            detail.insert("operation".to_string(), json!("subscribe"));
            detail.insert("symbols".to_string(), symbols_json(symbols));
        }
        RuntimeCommand::Market(MarketCommand::UnsubscribeQuotes { symbols }) => {
            detail.insert("aid".to_string(), json!("subscribe_quote"));
            detail.insert("operation".to_string(), json!("unsubscribe"));
            detail.insert("symbols".to_string(), symbols_json(symbols));
        }
        RuntimeCommand::Market(MarketCommand::SetChart(chart)) => {
            detail.insert("aid".to_string(), json!("set_chart"));
            detail.insert("chart_id".to_string(), json!(chart.chart_id));
            detail.insert("symbols".to_string(), symbols_json(&chart.symbols));
            detail.insert("duration_ns".to_string(), json!(chart.duration_ns));
            detail.insert("view_width".to_string(), json!(chart.view_width));
            if let Some(left_kline_id) = chart.left_kline_id {
                detail.insert("left_kline_id".to_string(), json!(left_kline_id));
            }
            if let Some(focus_datetime_ns) = chart.focus_datetime_ns {
                detail.insert("focus_datetime_ns".to_string(), json!(focus_datetime_ns));
            }
            if let Some(focus_position) = chart.focus_position {
                detail.insert("focus_position".to_string(), json!(focus_position));
            }
        }
        RuntimeCommand::Market(MarketCommand::CancelChart { chart_id }) => {
            detail.insert("aid".to_string(), json!("set_chart"));
            detail.insert("operation".to_string(), json!("cancel"));
            detail.insert("chart_id".to_string(), json!(chart_id));
        }
        RuntimeCommand::Market(MarketCommand::SubscribeTradingStatus { symbols }) => {
            detail.insert("aid".to_string(), json!("subscribe_trading_status"));
            detail.insert("operation".to_string(), json!("subscribe"));
            detail.insert("symbols".to_string(), symbols_json(symbols));
        }
        RuntimeCommand::Market(MarketCommand::UnsubscribeTradingStatus { symbols }) => {
            detail.insert("aid".to_string(), json!("subscribe_trading_status"));
            detail.insert("operation".to_string(), json!("unsubscribe"));
            detail.insert("symbols".to_string(), symbols_json(symbols));
        }
        RuntimeCommand::Trade(TradeCommand::Login(login)) => {
            detail.insert("aid".to_string(), json!("req_login"));
            detail.insert("account_id".to_string(), json!(login.account_id.as_str()));
            detail.insert("broker_id".to_string(), json!(login.broker_id));
            detail.insert(
                "account_type".to_string(),
                json!(login.account_type.as_str()),
            );
        }
        RuntimeCommand::Trade(TradeCommand::ConfirmSettlement { account_id }) => {
            detail.insert("aid".to_string(), json!("confirm_settlement"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::QueryAccountInfo { account_id }) => {
            detail.insert("aid".to_string(), json!("qry_account_info"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::QueryAccountRegister { account_id }) => {
            detail.insert("aid".to_string(), json!("qry_account_register"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::QuerySettlementInfo {
            account_id,
            trading_day,
        }) => {
            detail.insert("aid".to_string(), json!("qry_settlement_info"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
            detail.insert("trading_day".to_string(), json!(trading_day.to_string()));
        }
        RuntimeCommand::Trade(TradeCommand::PreInsertOrder(order)) => {
            detail.insert("aid".to_string(), json!("pre_insert_order"));
            detail.insert("account_id".to_string(), json!(order.account_id.as_str()));
            detail.insert("order_id".to_string(), json!(order.order_id.as_str()));
            detail.insert("symbol".to_string(), json!(order.symbol.as_str()));
            detail.insert("hedge_flag".to_string(), json!(order.hedge_flag));
            detail.insert(
                "contingent_condition".to_string(),
                json!(order.contingent_condition),
            );
        }
        RuntimeCommand::Trade(TradeCommand::InsertOrder(order)) => {
            detail.insert("aid".to_string(), json!("insert_order"));
            detail.insert("account_id".to_string(), json!(order.account_id.as_str()));
            detail.insert("order_id".to_string(), json!(order.order_id.as_str()));
            detail.insert("symbol".to_string(), json!(order.symbol.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::CancelOrder {
            account_id,
            order_id,
        }) => {
            detail.insert("aid".to_string(), json!("cancel_order"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
            detail.insert("order_id".to_string(), json!(order_id.as_str()));
        }
        RuntimeCommand::Trade(TradeCommand::Transfer {
            account_id,
            bank_id,
            future_account,
            currency,
            amount,
            ..
        }) => {
            detail.insert("aid".to_string(), json!("req_transfer"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
            detail.insert("bank_id".to_string(), json!(bank_id));
            detail.insert("future_account".to_string(), json!(future_account));
            detail.insert("currency".to_string(), json!(currency));
            detail.insert("amount".to_string(), amount.clone());
        }
        RuntimeCommand::Trade(TradeCommand::SetRiskManagementRule { account_id, rule }) => {
            detail.insert("aid".to_string(), json!("set_risk_management_rule"));
            detail.insert("account_id".to_string(), json!(account_id.as_str()));
            if let Some(exchange_id) = rule.get("exchange_id").and_then(Value::as_str) {
                detail.insert("exchange_id".to_string(), json!(exchange_id));
            }
        }
        RuntimeCommand::Replay(ReplayCommand::Step) => {
            detail.insert("action".to_string(), json!("step"));
        }
        RuntimeCommand::Replay(ReplayCommand::Reset) => {
            detail.insert("action".to_string(), json!("reset"));
        }
        RuntimeCommand::Query(QueryCommand::Fetch { query_id, .. }) => {
            detail.insert("aid".to_string(), json!("ins_query"));
            detail.insert("query_id".to_string(), json!(query_id.as_str()));
        }
        RuntimeCommand::Schema(SchemaCommand::Refresh { schema_id, path }) => {
            detail.insert("schema_id".to_string(), json!(schema_id.as_str()));
            detail.insert("path".to_string(), json!(path));
        }
    }

    detail
}

fn symbols_json(symbols: &[crate::ids::Symbol]) -> Value {
    json!(
        symbols
            .iter()
            .map(|symbol| symbol.as_str())
            .collect::<Vec<_>>()
    )
}
