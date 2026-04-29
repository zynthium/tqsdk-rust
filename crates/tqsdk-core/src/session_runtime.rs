use std::{future::Future, pin::Pin};

use serde_json::{Map, Value, json};

use crate::{
    Result,
    adapter::AdapterRegistry,
    auth::DynAuthProvider,
    commands::{CommandStatus, OutboundDispatch, OutboundFrame},
    events::{InternalEvent, RuntimeInput, TimerEvent},
    ids::CommandId,
    runtime::RuntimeHandle,
    state::{CommitResult, CommitScope, StatePath},
    transport::{
        BootstrapResult, ConnectedTopology, DispatchReceipt, SessionBootstrap, SessionConfig,
        SessionPhase, SessionRouteConnector, SessionTopologyResolver,
    },
};

mod command_status;
mod reconnect;

pub struct SessionRun {
    pub bootstrap: BootstrapResult,
    pub connected: ConnectedTopology,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoutePumpOutcome {
    pub commits: Vec<CommitResult>,
    pub reconnect_required: bool,
    pub reconnect_reason: Option<&'static str>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionStepOutcome {
    pub dispatches: Vec<DispatchReceipt>,
    pub commits: Vec<CommitResult>,
    pub recovered: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingRouteStepOutcome {
    pub requests: Vec<OutboundDispatch>,
    pub commits: Vec<CommitResult>,
}

pub trait RouteRequestExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        route: &'a crate::transport::SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RuntimeInput>>> + Send + 'a>>;
}

/// Borrowed dependency bundle required to drive reconnect and timer flows.
#[derive(Clone, Copy)]
pub struct SessionRuntimeDeps<'a> {
    pub auth: &'a dyn DynAuthProvider,
    pub resolver: &'a dyn SessionTopologyResolver,
    pub connector: &'a dyn SessionRouteConnector,
    pub config: &'a SessionConfig,
    pub adapters: &'a AdapterRegistry,
}

impl<'a> SessionRuntimeDeps<'a> {
    /// Creates a dependency bundle for session orchestration helpers.
    pub fn new(
        auth: &'a dyn DynAuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        connector: &'a dyn SessionRouteConnector,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> Self {
        Self {
            auth,
            resolver,
            connector,
            config,
            adapters,
        }
    }
}

/// Low-level session orchestrator for auth, topology establishment, dispatch,
/// reconnect, and runtime ingestion.
///
/// This type intentionally stays below any end-user facade. It coordinates the
/// substrate pieces that talk to TQ services and publishes all resulting state
/// through the shared runtime handle.
#[derive(Clone)]
pub struct SessionRuntime {
    handle: RuntimeHandle,
    bootstrap: SessionBootstrap,
}

impl SessionRuntime {
    /// Creates a session runtime bound to the provided shared runtime handle.
    pub fn new(handle: RuntimeHandle, bootstrap: SessionBootstrap) -> Self {
        Self { handle, bootstrap }
    }

    /// Returns a clone of the shared runtime handle backing this session.
    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }

    /// Authenticates, resolves topology, connects routes, and records the
    /// initial session bootstrap commits.
    pub async fn establish(
        &self,
        auth: &dyn DynAuthProvider,
        resolver: &dyn SessionTopologyResolver,
        connector: &dyn SessionRouteConnector,
        config: &SessionConfig,
        adapters: &AdapterRegistry,
    ) -> Result<SessionRun> {
        self.handle
            .record_session_phase(SessionPhase::Authenticating, None, vec![])?;

        let bootstrap = match self
            .bootstrap
            .establish_with_resolver(auth, resolver, config, adapters)
            .await
        {
            Ok(bootstrap) => bootstrap,
            Err(err) => {
                self.record_session_failure("session-establish-error", "bootstrap", &err, vec![])?;
                return Err(err);
            }
        };
        let connected = match self
            .connect_if_needed(&bootstrap, connector, Some(SessionPhase::Connecting))
            .await
        {
            Ok(connected) => connected,
            Err(err) => {
                self.record_session_failure("session-establish-error", "connect", &err, vec![])?;
                return Err(err);
            }
        };

        self.handle
            .record_session_phase(SessionPhase::Bootstrapping, None, vec![])?;
        self.handle.record_session_bootstrap(&bootstrap, vec![])?;

        Ok(SessionRun {
            bootstrap,
            connected,
        })
    }

    pub async fn recover(
        &self,
        auth: &dyn DynAuthProvider,
        resolver: &dyn SessionTopologyResolver,
        connector: &dyn SessionRouteConnector,
        config: &SessionConfig,
        adapters: &AdapterRegistry,
    ) -> Result<SessionRun> {
        let recovery = match self
            .recover_internal(
                SessionRuntimeDeps::new(auth, resolver, connector, config, adapters),
                true,
            )
            .await
        {
            Ok(recovery) => recovery,
            Err(err) => {
                self.record_session_failure("session-recovery-error", "recover", &err, vec![])?;
                return Err(err);
            }
        };
        Ok(recovery.run)
    }

    pub async fn flush_outbound(&self, run: &mut SessionRun) -> Result<Vec<DispatchReceipt>> {
        let dispatches = self.handle.drain_dispatches()?;
        let mut receipts = Vec::with_capacity(dispatches.len());
        let mut sent_command_ids = Vec::new();
        for dispatch in dispatches {
            let receipt = match run.connected.dispatch_ref(&dispatch).await {
                Ok(receipt) => receipt,
                Err(err) => {
                    let route_label = run
                        .connected
                        .route_label_for_dispatch(&dispatch)
                        .map(str::to_string);
                    let mut detail = Map::new();
                    detail.insert("message".to_string(), json!(err.to_string()));
                    self.handle.record_command_status(
                        dispatch.command_id,
                        CommandStatus::Failed,
                        self.command_detail(
                            dispatch.command_id,
                            route_label.as_deref(),
                            Some(&dispatch),
                            detail,
                        ),
                        CommitScope::RealtimeUpdate,
                    )?;
                    return Err(err);
                }
            };
            if !sent_command_ids.contains(&receipt.command_id) {
                self.handle.record_command_status(
                    receipt.command_id,
                    CommandStatus::Sent,
                    self.command_detail(
                        receipt.command_id,
                        Some(receipt.route_label.as_str()),
                        Some(&dispatch),
                        Map::new(),
                    ),
                    CommitScope::RealtimeUpdate,
                )?;
                sent_command_ids.push(receipt.command_id);
            }
            receipts.push(receipt);
        }
        Ok(receipts)
    }

    pub async fn recv_route_and_ingest(
        &self,
        run: &mut SessionRun,
        route_label: &str,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let Some(input) = run.connected.recv_route_input(route_label).await? else {
            return Ok(None);
        };
        let commit = self.handle.ingest(input, caused_by.clone(), scope)?;
        if let Some(commit_result) = commit.as_ref() {
            self.record_transport_commit_statuses(route_label, commit_result, &caused_by, scope)?;
        }
        Ok(commit)
    }

    pub async fn pump_route_once(
        &self,
        run: &mut SessionRun,
        route_label: &str,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<RoutePumpOutcome> {
        if !run.connected.has_route(route_label) {
            return Err(crate::ContractError::validation(format!(
                "unknown connected route for route pump: {route_label}"
            )));
        }

        match run.connected.recv_route_input(route_label).await {
            Ok(Some(RuntimeInput::Internal(event))) if event.label == "transport-close" => {
                self.handle_disconnect(route_label, event, caused_by)
            }
            Ok(Some(RuntimeInput::Internal(event)))
                if matches!(event.label, "transport-pong" | "transport-ping") =>
            {
                self.handle_transport_signal(event, caused_by)
            }
            Ok(Some(input)) => {
                let mut outcome = RoutePumpOutcome::default();
                if let Some(commit) = self.handle.ingest(input, caused_by.clone(), scope)? {
                    self.record_transport_commit_statuses(route_label, &commit, &caused_by, scope)?;
                    outcome.commits.push(commit);
                }
                Ok(outcome)
            }
            Ok(None) => Ok(RoutePumpOutcome::default()),
            Err(err) => self.handle_transport_error(route_label, err, caused_by),
        }
    }

    pub async fn drive_route_once(
        &self,
        run: &mut SessionRun,
        route_label: &str,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
        deps: SessionRuntimeDeps<'_>,
    ) -> Result<SessionStepOutcome> {
        let mut outcome = SessionStepOutcome {
            dispatches: self.flush_outbound(run).await?,
            commits: Vec::new(),
            recovered: false,
        };

        let route_outcome = self
            .pump_route_once(run, route_label, caused_by.clone(), scope)
            .await?;
        outcome.commits.extend(route_outcome.commits);

        if route_outcome.reconnect_required {
            let recovery = self
                .recover_with_policy(
                    route_label,
                    route_outcome.reconnect_reason.unwrap_or("transport-close"),
                    caused_by,
                    deps,
                )
                .await?;
            outcome.recovered = true;
            outcome.commits.extend(recovery.commits);
            *run = recovery.run;
            return Ok(outcome);
        }

        if let Some(commit) = self.ingest_queued_inputs(run, caused_by, scope)? {
            outcome.commits.push(commit);
        }

        Ok(outcome)
    }

    pub async fn drive_timer_once(
        &self,
        run: &mut SessionRun,
        timer: TimerEvent,
        caused_by: Vec<CommandId>,
        deps: SessionRuntimeDeps<'_>,
    ) -> Result<SessionStepOutcome> {
        let mut outcome = SessionStepOutcome::default();

        match timer.label {
            "heartbeat-due" | "heartbeat-timeout" => {
                let route_label = timer_route_label(&timer)?;
                if !run.connected.has_route(route_label) {
                    return Err(crate::ContractError::validation(format!(
                        "unknown connected route for timer event: {route_label}"
                    )));
                }
            }
            _ => {}
        }

        if let Some(commit) = self.handle.ingest(
            RuntimeInput::Timer(timer.clone()),
            caused_by.clone(),
            CommitScope::SessionTransition,
        )? {
            outcome.commits.push(commit);
        }

        match timer.label {
            "heartbeat-due" => {
                let route_label = timer_route_label(&timer)?;
                run.connected
                    .send_route_frame(route_label, OutboundFrame::Ping)
                    .await?;
                if let Some(commit) = self.handle.ingest(
                    RuntimeInput::Internal(InternalEvent {
                        label: "transport-ping",
                        payload: Some(json!({
                            "route": route_label,
                            "reason": "heartbeat-due",
                        })),
                    }),
                    caused_by,
                    CommitScope::SessionTransition,
                )? {
                    outcome.commits.push(commit);
                }
            }
            "heartbeat-timeout" => {
                let route_label = timer_route_label(&timer)?;
                if let Some(commit) = self.handle.record_session_phase(
                    SessionPhase::Reconnecting,
                    Some(json!({
                        "route": route_label,
                        "reason": "heartbeat-timeout",
                    })),
                    caused_by.clone(),
                )? {
                    outcome.commits.push(commit);
                }

                let recovery = self
                    .recover_with_policy(route_label, "heartbeat-timeout", caused_by, deps)
                    .await?;
                outcome.recovered = true;
                outcome.commits.extend(recovery.commits);
                *run = recovery.run;
            }
            _ => {}
        }

        Ok(outcome)
    }

    pub fn ingest_queued_inputs(
        &self,
        run: &mut SessionRun,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<Option<CommitResult>> {
        let inputs = run.connected.drain_queued_inputs();
        if inputs.is_empty() {
            return Ok(None);
        }
        self.handle.ingest_batch(inputs, caused_by, scope)
    }

    pub async fn drive_pending_route_once(
        &self,
        run: &mut SessionRun,
        route_label: &str,
        executor: &dyn RouteRequestExecutor,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> Result<PendingRouteStepOutcome> {
        let (route, requests) = run.connected.take_route_requests(route_label)?;
        if requests.is_empty() {
            return Ok(PendingRouteStepOutcome::default());
        }
        let request_command_ids = request_command_ids(&requests);

        let mut outcome = PendingRouteStepOutcome {
            requests: requests.clone(),
            commits: Vec::new(),
        };
        let inputs = match executor.execute(&route, requests).await {
            Ok(inputs) => inputs,
            Err(err) => {
                self.record_command_failure(&request_command_ids, route.label.as_str(), &err)?;
                return Err(err);
            }
        };
        let inputs = self.annotate_pending_route_inputs(&outcome.requests, inputs)?;
        match self.handle.ingest_batch(inputs, caused_by.clone(), scope) {
            Ok(Some(commit)) => {
                outcome.commits.push(commit);
                self.record_command_statuses(
                    &request_command_ids,
                    CommandStatus::Completed,
                    Some(json!({ "route": route.label })),
                    scope,
                )?;
            }
            Ok(None) => {}
            Err(err) => {
                self.record_command_failure(&request_command_ids, route.label.as_str(), &err)?;
                return Err(err);
            }
        }

        Ok(outcome)
    }

    fn annotate_pending_route_inputs(
        &self,
        requests: &[OutboundDispatch],
        inputs: Vec<RuntimeInput>,
    ) -> Result<Vec<RuntimeInput>> {
        let reader = self.handle.reader();
        let snapshot = reader.read();
        let snapshot = snapshot.view();
        let schema_ids = requests
            .iter()
            .map(|dispatch| {
                command_detail_map_from_snapshot(snapshot, dispatch.command_id).and_then(|detail| {
                    detail
                        .get("schema_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
            })
            .collect::<Vec<_>>();

        let schema_request_count = schema_ids.iter().flatten().count();
        if schema_request_count == 0 {
            return Ok(inputs);
        }

        if schema_request_count == 1 {
            let Some(schema_id) = schema_ids.iter().flatten().next().map(String::as_str) else {
                return Err(crate::ContractError::validation(
                    "schema response annotation invariant violated",
                ));
            };
            return Ok(inputs
                .into_iter()
                .map(|input| annotate_schema_input(input, Some(schema_id)))
                .collect());
        }

        if inputs.len() == requests.len() {
            return Ok(inputs
                .into_iter()
                .zip(schema_ids)
                .map(|(input, schema_id)| annotate_schema_input(input, schema_id.as_deref()))
                .collect());
        }

        if inputs.iter().all(schema_input_is_annotated_or_non_schema) {
            return Ok(inputs);
        }

        Err(crate::ContractError::validation(
            "ambiguous schema response mapping: multiple schema requests returned an unexpected number of inputs",
        ))
    }

    async fn connect_if_needed(
        &self,
        bootstrap: &BootstrapResult,
        connector: &dyn SessionRouteConnector,
        phase: Option<SessionPhase>,
    ) -> Result<ConnectedTopology> {
        if bootstrap.topology.routes.is_empty() {
            return Ok(ConnectedTopology::default());
        }

        if let Some(phase) = phase {
            self.handle.record_session_phase(
                phase,
                Some(json!({ "route_count": bootstrap.topology.routes.len() })),
                vec![],
            )?;
        }

        self.bootstrap
            .connect_topology(&bootstrap.topology, connector)
            .await
    }

    fn handle_disconnect(
        &self,
        route_label: &str,
        event: InternalEvent,
        caused_by: Vec<CommandId>,
    ) -> Result<RoutePumpOutcome> {
        let mut outcome = RoutePumpOutcome {
            commits: Vec::new(),
            reconnect_required: true,
            reconnect_reason: Some("transport-close"),
        };

        if let Some(commit) = self.handle.ingest(
            RuntimeInput::Internal(event),
            caused_by.clone(),
            CommitScope::SessionTransition,
        )? {
            outcome.commits.push(commit);
        }

        if let Some(commit) = self.handle.record_session_phase(
            SessionPhase::Reconnecting,
            Some(json!({
                "route": route_label,
                "reason": "transport-close",
            })),
            caused_by,
        )? {
            outcome.commits.push(commit);
        }

        Ok(outcome)
    }

    fn handle_transport_signal(
        &self,
        event: InternalEvent,
        caused_by: Vec<CommandId>,
    ) -> Result<RoutePumpOutcome> {
        let mut outcome = RoutePumpOutcome::default();
        if let Some(commit) = self.handle.ingest(
            RuntimeInput::Internal(event),
            caused_by,
            CommitScope::SessionTransition,
        )? {
            outcome.commits.push(commit);
        }
        Ok(outcome)
    }

    fn handle_transport_error(
        &self,
        route_label: &str,
        err: crate::ContractError,
        caused_by: Vec<CommandId>,
    ) -> Result<RoutePumpOutcome> {
        let mut outcome = RoutePumpOutcome {
            commits: Vec::new(),
            reconnect_required: true,
            reconnect_reason: Some("transport-error"),
        };

        if let Some(commit) = self.handle.ingest(
            RuntimeInput::Internal(InternalEvent {
                label: "transport-error",
                payload: Some(json!({
                    "route": route_label,
                    "message": err.to_string(),
                })),
            }),
            caused_by.clone(),
            CommitScope::SessionTransition,
        )? {
            outcome.commits.push(commit);
        }

        if let Some(commit) = self.handle.record_session_phase(
            SessionPhase::Reconnecting,
            Some(json!({
                "route": route_label,
                "reason": "transport-error",
            })),
            caused_by,
        )? {
            outcome.commits.push(commit);
        }

        Ok(outcome)
    }
}

fn timer_route_label(timer: &TimerEvent) -> Result<&str> {
    timer
        .payload
        .as_ref()
        .and_then(|payload| payload.get("route"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            crate::ContractError::validation(format!(
                "timer event '{}' requires payload.route string",
                timer.label
            ))
        })
}

impl SessionRuntime {
    fn record_transport_commit_statuses(
        &self,
        route_label: &str,
        commit: &CommitResult,
        command_ids: &[CommandId],
        scope: CommitScope,
    ) -> Result<()> {
        for &command_id in command_ids {
            let current = self.command_status(command_id);
            if is_terminal_command_status(current.as_deref()) {
                continue;
            }

            if let Some((status, detail)) =
                self.derive_transport_command_status(route_label, commit, command_id)
            {
                self.handle
                    .record_command_status(command_id, status, detail, scope)?;
                continue;
            }

            if matches!(current.as_deref(), Some("acked")) {
                continue;
            }

            self.handle.record_command_status(
                command_id,
                CommandStatus::PartiallyApplied,
                self.command_detail(command_id, Some(route_label), None, Map::new()),
                scope,
            )?;
        }

        Ok(())
    }

    fn record_command_statuses(
        &self,
        command_ids: &[CommandId],
        status: CommandStatus,
        detail: Option<Value>,
        scope: CommitScope,
    ) -> Result<()> {
        for &command_id in command_ids {
            self.handle
                .record_command_status(command_id, status, detail.clone(), scope)?;
        }
        Ok(())
    }

    fn record_command_failure(
        &self,
        command_ids: &[CommandId],
        route_label: &str,
        err: &crate::ContractError,
    ) -> Result<()> {
        for &command_id in command_ids {
            let mut detail = Map::new();
            detail.insert("message".to_string(), json!(err.to_string()));
            self.handle.record_command_status(
                command_id,
                CommandStatus::Failed,
                self.command_detail(command_id, Some(route_label), None, detail),
                CommitScope::RealtimeUpdate,
            )?;
        }

        Ok(())
    }

    fn record_session_failure(
        &self,
        label: &'static str,
        stage: &'static str,
        err: &crate::ContractError,
        caused_by: Vec<CommandId>,
    ) -> Result<()> {
        let _ = self.handle.ingest(
            RuntimeInput::Internal(InternalEvent {
                label,
                payload: Some(json!({
                    "stage": stage,
                    "message": err.to_string(),
                })),
            }),
            caused_by.clone(),
            CommitScope::SessionTransition,
        )?;

        let _ = self.handle.record_session_phase(
            SessionPhase::Closed,
            Some(json!({
                "reason": label,
                "stage": stage,
                "message": err.to_string(),
            })),
            caused_by,
        )?;

        Ok(())
    }

    fn derive_transport_command_status(
        &self,
        route_label: &str,
        commit: &CommitResult,
        command_id: CommandId,
    ) -> Option<(CommandStatus, Option<Value>)> {
        let reader = self.handle.reader();
        let snapshot = reader.read();
        let snapshot = snapshot.view();
        let detail = command_detail_map_from_snapshot(snapshot, command_id)?;
        let aid = detail.get("aid").and_then(Value::as_str)?;

        match aid {
            "insert_order" | "cancel_order" => self.derive_trade_order_command_status(
                snapshot,
                route_label,
                commit,
                command_id,
                &detail,
            ),
            "req_login" => self.derive_trade_login_command_status(
                snapshot,
                route_label,
                commit,
                command_id,
                &detail,
            ),
            "pre_insert_order" => self.derive_trade_pre_insert_order_command_status(
                snapshot,
                route_label,
                commit,
                command_id,
                &detail,
            ),
            "qry_account_info" => self.derive_trade_account_info_command_status(
                snapshot,
                route_label,
                commit,
                command_id,
                &detail,
            ),
            "set_risk_management_rule" => self.derive_trade_risk_management_rule_command_status(
                snapshot,
                route_label,
                commit,
                command_id,
                &detail,
            ),
            "qry_settlement_info" => self.derive_trade_settlement_query_command_status(
                snapshot,
                route_label,
                commit,
                command_id,
                &detail,
            ),
            "ins_query" => {
                self.derive_query_command_status(snapshot, route_label, commit, command_id, &detail)
            }
            _ => None,
        }
    }

    fn derive_query_command_status(
        &self,
        snapshot: crate::state::StateReadView<'_>,
        route_label: &str,
        commit: &CommitResult,
        command_id: CommandId,
        detail: &Map<String, Value>,
    ) -> Option<(CommandStatus, Option<Value>)> {
        command_status::query_completed_status(snapshot, route_label, commit, command_id, detail)
    }

    fn derive_trade_login_command_status(
        &self,
        snapshot: crate::state::StateReadView<'_>,
        route_label: &str,
        commit: &CommitResult,
        command_id: CommandId,
        detail: &Map<String, Value>,
    ) -> Option<(CommandStatus, Option<Value>)> {
        command_status::trade_login_completed_status(
            snapshot,
            route_label,
            commit,
            command_id,
            detail,
        )
    }

    fn derive_trade_account_info_command_status(
        &self,
        snapshot: crate::state::StateReadView<'_>,
        route_label: &str,
        commit: &CommitResult,
        command_id: CommandId,
        detail: &Map<String, Value>,
    ) -> Option<(CommandStatus, Option<Value>)> {
        let account_id = detail.get("account_id").and_then(Value::as_str)?;
        let mut extra_detail = Map::new();
        extra_detail.insert("currency".to_string(), json!("CNY"));
        command_status::path_completed_status(
            snapshot,
            route_label,
            commit,
            command_id,
            detail,
            ["trade", account_id, "accounts", "CNY"],
            extra_detail,
        )
    }

    fn derive_trade_pre_insert_order_command_status(
        &self,
        snapshot: crate::state::StateReadView<'_>,
        route_label: &str,
        commit: &CommitResult,
        command_id: CommandId,
        detail: &Map<String, Value>,
    ) -> Option<(CommandStatus, Option<Value>)> {
        command_status::pre_insert_order_completed_status(
            snapshot,
            route_label,
            commit,
            command_id,
            detail,
        )
    }

    fn derive_trade_risk_management_rule_command_status(
        &self,
        snapshot: crate::state::StateReadView<'_>,
        route_label: &str,
        commit: &CommitResult,
        command_id: CommandId,
        detail: &Map<String, Value>,
    ) -> Option<(CommandStatus, Option<Value>)> {
        let account_id = detail.get("account_id").and_then(Value::as_str)?;
        let exchange_id = detail.get("exchange_id").and_then(Value::as_str)?;
        let mut extra_detail = Map::new();
        extra_detail.insert("exchange_id".to_string(), json!(exchange_id));
        command_status::path_completed_status(
            snapshot,
            route_label,
            commit,
            command_id,
            detail,
            ["trade", account_id, "risk_management_rule", exchange_id],
            extra_detail,
        )
    }

    fn derive_trade_settlement_query_command_status(
        &self,
        snapshot: crate::state::StateReadView<'_>,
        route_label: &str,
        commit: &CommitResult,
        command_id: CommandId,
        detail: &Map<String, Value>,
    ) -> Option<(CommandStatus, Option<Value>)> {
        let account_id = detail.get("account_id").and_then(Value::as_str)?;
        let trading_day = detail.get("trading_day").and_then(Value::as_str)?;
        let mut extra_detail = Map::new();
        extra_detail.insert("trading_day".to_string(), json!(trading_day));
        command_status::path_completed_status(
            snapshot,
            route_label,
            commit,
            command_id,
            detail,
            ["trade", account_id, "his_settlements", trading_day],
            extra_detail,
        )
    }

    fn derive_trade_order_command_status(
        &self,
        snapshot: crate::state::StateReadView<'_>,
        route_label: &str,
        commit: &CommitResult,
        command_id: CommandId,
        detail: &Map<String, Value>,
    ) -> Option<(CommandStatus, Option<Value>)> {
        command_status::trade_order_status(snapshot, route_label, commit, command_id, detail)
    }

    fn command_status(&self, command_id: CommandId) -> Option<String> {
        let reader = self.handle.reader();
        let snapshot = reader.read();
        let snapshot = snapshot.view();
        let command_segment = command_id.get().to_string();
        snapshot
            .get(["runtime", "commands", command_segment.as_str(), "status"])
            .and_then(Value::as_str)
            .map(str::to_string)
    }

    fn command_detail(
        &self,
        command_id: CommandId,
        route_label: Option<&str>,
        dispatch: Option<&OutboundDispatch>,
        extra: Map<String, Value>,
    ) -> Option<Value> {
        let reader = self.handle.reader();
        let snapshot = reader.read();
        let snapshot = snapshot.view();
        let mut detail = command_detail_map_from_snapshot(snapshot, command_id).unwrap_or_default();

        if let Some(route_label) = route_label {
            detail.insert("route".to_string(), json!(route_label));
        }

        if let Some(dispatch) = dispatch {
            for (key, value) in command_detail_fields_from_dispatch(dispatch) {
                detail.entry(key).or_insert(value);
            }
        }

        detail.extend(extra);

        if detail.is_empty() {
            None
        } else {
            Some(Value::Object(detail))
        }
    }
}

fn request_command_ids(requests: &[OutboundDispatch]) -> Vec<CommandId> {
    let mut command_ids = Vec::with_capacity(requests.len());
    for request in requests {
        if !command_ids.contains(&request.command_id) {
            command_ids.push(request.command_id);
        }
    }
    command_ids
}

fn command_detail_map_from_snapshot(
    snapshot: crate::state::StateReadView<'_>,
    command_id: CommandId,
) -> Option<Map<String, Value>> {
    let command_segment = command_id.get().to_string();
    snapshot
        .get(["runtime", "commands", command_segment.as_str(), "detail"])
        .and_then(Value::as_object)
        .cloned()
}

fn command_detail_from_seed(
    mut seed: Map<String, Value>,
    _command_id: CommandId,
    route_label: Option<&str>,
    dispatch: Option<&OutboundDispatch>,
    extra: Map<String, Value>,
) -> Option<Value> {
    if let Some(route_label) = route_label {
        seed.insert("route".to_string(), json!(route_label));
    }

    if let Some(dispatch) = dispatch {
        for (key, value) in command_detail_fields_from_dispatch(dispatch) {
            seed.entry(key).or_insert(value);
        }
    }

    seed.extend(extra);

    if seed.is_empty() {
        None
    } else {
        Some(Value::Object(seed))
    }
}

fn command_detail_fields_from_dispatch(dispatch: &OutboundDispatch) -> Map<String, Value> {
    let mut detail = Map::new();

    match &dispatch.request {
        crate::commands::OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            if let Ok(Value::Object(request)) = serde_json::from_str::<Value>(text) {
                if let Some(aid) = request.get("aid").and_then(Value::as_str) {
                    detail.insert("aid".to_string(), json!(aid));
                }

                if dispatch.domain == crate::ids::ProtocolDomain::Trade {
                    if let Some(account_id) = request
                        .get("user_id")
                        .or_else(|| request.get("user_name"))
                        .and_then(Value::as_str)
                    {
                        detail.insert("account_id".to_string(), json!(account_id));
                    }
                    if let Some(order_id) = request.get("order_id").and_then(Value::as_str) {
                        detail.insert("order_id".to_string(), json!(order_id));
                    }
                    if let Some(trading_day) = request.get("trading_day").and_then(Value::as_str) {
                        detail.insert("trading_day".to_string(), json!(trading_day));
                    }
                    if let Some(exchange_id) = request.get("exchange_id").and_then(Value::as_str) {
                        detail.insert("exchange_id".to_string(), json!(exchange_id));
                    }
                }
            }
        }
        crate::commands::OutboundRequest::Transport(OutboundFrame::Binary(_)) => {
            detail.insert("frame".to_string(), json!("binary"));
        }
        crate::commands::OutboundRequest::Transport(OutboundFrame::Ping) => {
            detail.insert("frame".to_string(), json!("ping"));
        }
        crate::commands::OutboundRequest::Transport(OutboundFrame::Close) => {
            detail.insert("frame".to_string(), json!("close"));
        }
        crate::commands::OutboundRequest::Http(request) => {
            detail.insert("method".to_string(), json!(request.method.as_str()));
            if let Some(path) = &request.path {
                detail.insert("path".to_string(), json!(path));
            }
            if let Some(Value::Object(body)) = &request.body {
                if let Some(aid) = body.get("aid").and_then(Value::as_str) {
                    detail.insert("aid".to_string(), json!(aid));
                }
                if let Some(query_id) = body.get("query_id").and_then(Value::as_str) {
                    detail.insert("query_id".to_string(), json!(query_id));
                }
            }
        }
        crate::commands::OutboundRequest::Query(request) => {
            detail.insert("aid".to_string(), json!("ins_query"));
            detail.insert("query_id".to_string(), json!(request.query_id.as_str()));
        }
        crate::commands::OutboundRequest::Replay(request) => {
            detail.insert("action".to_string(), json!(request.action));
        }
        crate::commands::OutboundRequest::Internal(request) => {
            detail.insert("label".to_string(), json!(request.label));
        }
    }

    detail
}

fn is_terminal_command_status(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("completed" | "rejected" | "failed" | "cancelled")
    )
}

fn annotate_schema_input(input: RuntimeInput, schema_id: Option<&str>) -> RuntimeInput {
    let Some(schema_id) = schema_id else {
        return input;
    };

    match input {
        RuntimeInput::Io(mut event) if event.domains.contains(&crate::ProtocolDomain::Schema) => {
            if let crate::InputPayload::Json(payload) = event.payload {
                event.payload = crate::InputPayload::Json(wrap_schema_payload(payload, schema_id));
            }
            RuntimeInput::Io(event)
        }
        other => other,
    }
}

fn schema_input_is_annotated_or_non_schema(input: &RuntimeInput) -> bool {
    match input {
        RuntimeInput::Io(event) if event.domains.contains(&crate::ProtocolDomain::Schema) => {
            matches!(
                &event.payload,
                crate::InputPayload::Json(payload)
                    if payload.get("schema_id").and_then(Value::as_str).is_some()
            )
        }
        _ => true,
    }
}

fn wrap_schema_payload(payload: Value, schema_id: &str) -> Value {
    if payload.get("schema_id").and_then(Value::as_str).is_some() {
        return payload;
    }

    json!({
        "schema_id": schema_id,
        "data": payload,
    })
}

fn commit_touches_path<I, S>(commit: &CommitResult, path: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    commit.changes.path_hits.contains(&StatePath::new(path))
}

fn commit_touches_path_prefix<I, S>(commit: &CommitResult, path: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let prefix = path.into_iter().map(Into::into).collect::<Vec<_>>();
    commit.changes.path_hits.iter().any(|hit| {
        let segments = hit.segments();
        segments.len() >= prefix.len()
            && segments
                .iter()
                .zip(prefix.iter())
                .all(|(left, right)| left == right)
    })
}
