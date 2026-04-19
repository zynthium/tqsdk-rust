use serde_json::{Value, json};

use crate::{
    Result,
    adapter::AdapterRegistry,
    auth::{AuthProvider, ContractFuture},
    commands::{CommandStatus, OutboundDispatch, OutboundFrame},
    events::{InternalEvent, RuntimeInput, TimerEvent},
    ids::CommandId,
    runtime::RuntimeHandle,
    state::{CommitResult, CommitScope},
    transport::{
        BootstrapResult, ConnectedTopology, DispatchReceipt, SessionBootstrap, SessionConfig,
        SessionPhase, SessionRouteConnector, SessionTopologyResolver,
    },
};

pub struct SessionRun {
    pub bootstrap: BootstrapResult,
    pub connected: ConnectedTopology,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoutePumpOutcome {
    pub commits: Vec<CommitResult>,
    pub reconnect_required: bool,
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

struct RecoveryOutcome {
    run: SessionRun,
    commits: Vec<CommitResult>,
}

pub trait RouteRequestExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        route: &'a crate::transport::SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> ContractFuture<'a, Vec<RuntimeInput>>;
}

#[derive(Clone)]
pub struct SessionRuntime {
    handle: RuntimeHandle,
    bootstrap: SessionBootstrap,
}

impl SessionRuntime {
    pub fn new(handle: RuntimeHandle, bootstrap: SessionBootstrap) -> Self {
        Self { handle, bootstrap }
    }

    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }

    pub fn establish<'a>(
        &'a self,
        auth: &'a dyn AuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        connector: &'a dyn SessionRouteConnector,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> ContractFuture<'a, SessionRun> {
        Box::pin(async move {
            self.handle
                .record_session_phase(SessionPhase::Authenticating, None, vec![])?;

            let bootstrap = self
                .bootstrap
                .establish_with_resolver(auth, resolver, config, adapters)
                .await?;
            let connected = self
                .connect_if_needed(&bootstrap, connector, Some(SessionPhase::Connecting))
                .await?;

            self.handle
                .record_session_phase(SessionPhase::Bootstrapping, None, vec![])?;
            self.handle.record_session_bootstrap(&bootstrap, vec![])?;

            Ok(SessionRun {
                bootstrap,
                connected,
            })
        })
    }

    pub fn recover<'a>(
        &'a self,
        auth: &'a dyn AuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        connector: &'a dyn SessionRouteConnector,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> ContractFuture<'a, SessionRun> {
        Box::pin(async move {
            let recovery = self
                .recover_internal(auth, resolver, connector, config, adapters, true)
                .await?;
            Ok(recovery.run)
        })
    }

    pub fn flush_outbound<'a>(
        &'a self,
        run: &'a mut SessionRun,
    ) -> ContractFuture<'a, Vec<DispatchReceipt>> {
        Box::pin(async move {
            let dispatches = self.handle.drain_dispatches()?;
            let mut receipts = Vec::with_capacity(dispatches.len());
            for dispatch in dispatches {
                let receipt = run.connected.dispatch(dispatch).await?;
                let route_label = receipt.route_label.clone();
                let domain = receipt.domain;
                self.handle.record_command_status(
                    receipt.command_id,
                    CommandStatus::Sent,
                    Some(json!({
                        "route": route_label,
                        "domain": domain.as_str(),
                    })),
                    CommitScope::RealtimeUpdate,
                )?;
                receipts.push(receipt);
            }
            Ok(receipts)
        })
    }

    pub fn recv_route_and_ingest<'a>(
        &'a self,
        run: &'a mut SessionRun,
        route_label: &'a str,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> ContractFuture<'a, Option<CommitResult>> {
        Box::pin(async move {
            let Some(input) = run.connected.recv_route_input(route_label).await? else {
                return Ok(None);
            };
            self.handle.ingest(input, caused_by, scope)
        })
    }

    pub fn pump_route_once<'a>(
        &'a self,
        run: &'a mut SessionRun,
        route_label: &'a str,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> ContractFuture<'a, RoutePumpOutcome> {
        Box::pin(async move {
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
                    if let Some(commit) = self.handle.ingest(input, caused_by, scope)? {
                        outcome.commits.push(commit);
                    }
                    Ok(outcome)
                }
                Ok(None) => Ok(RoutePumpOutcome::default()),
                Err(err) => self.handle_transport_error(route_label, err, caused_by),
            }
        })
    }

    pub fn drive_route_once<'a>(
        &'a self,
        run: &'a mut SessionRun,
        route_label: &'a str,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
        auth: &'a dyn AuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        connector: &'a dyn SessionRouteConnector,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> ContractFuture<'a, SessionStepOutcome> {
        Box::pin(async move {
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
                    .recover_internal(auth, resolver, connector, config, adapters, false)
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
        })
    }

    pub fn drive_timer_once<'a>(
        &'a self,
        run: &'a mut SessionRun,
        timer: TimerEvent,
        caused_by: Vec<CommandId>,
        auth: &'a dyn AuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        connector: &'a dyn SessionRouteConnector,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> ContractFuture<'a, SessionStepOutcome> {
        Box::pin(async move {
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
                        caused_by,
                    )? {
                        outcome.commits.push(commit);
                    }

                    let recovery = self
                        .recover_internal(auth, resolver, connector, config, adapters, false)
                        .await?;
                    outcome.recovered = true;
                    outcome.commits.extend(recovery.commits);
                    *run = recovery.run;
                }
                _ => {}
            }

            Ok(outcome)
        })
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

    pub fn drive_pending_route_once<'a>(
        &'a self,
        run: &'a mut SessionRun,
        route_label: &'a str,
        executor: &'a dyn RouteRequestExecutor,
        caused_by: Vec<CommandId>,
        scope: CommitScope,
    ) -> ContractFuture<'a, PendingRouteStepOutcome> {
        Box::pin(async move {
            let (route, requests) = run.connected.take_route_requests(route_label)?;
            if requests.is_empty() {
                return Ok(PendingRouteStepOutcome::default());
            }

            let mut outcome = PendingRouteStepOutcome {
                requests: requests.clone(),
                commits: Vec::new(),
            };
            let inputs = match executor.execute(&route, requests).await {
                Ok(inputs) => inputs,
                Err(err) => {
                    self.record_command_failure(&caused_by, route.label.as_str(), &err)?;
                    return Err(err);
                }
            };
            match self.handle.ingest_batch(inputs, caused_by.clone(), scope) {
                Ok(Some(commit)) => outcome.commits.push(commit),
                Ok(None) => {}
                Err(err) => {
                    self.record_command_failure(&caused_by, route.label.as_str(), &err)?;
                    return Err(err);
                }
            }

            Ok(outcome)
        })
    }

    fn connect_if_needed<'a>(
        &'a self,
        bootstrap: &'a BootstrapResult,
        connector: &'a dyn SessionRouteConnector,
        phase: Option<SessionPhase>,
    ) -> ContractFuture<'a, ConnectedTopology> {
        Box::pin(async move {
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
        })
    }

    fn recover_internal<'a>(
        &'a self,
        auth: &'a dyn AuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        connector: &'a dyn SessionRouteConnector,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
        record_reconnecting: bool,
    ) -> ContractFuture<'a, RecoveryOutcome> {
        Box::pin(async move {
            let mut commits = Vec::new();

            if record_reconnecting {
                if let Some(commit) =
                    self.handle
                        .record_session_phase(SessionPhase::Reconnecting, None, vec![])?
                {
                    commits.push(commit);
                }
            }

            let bootstrap = self
                .bootstrap
                .establish_with_resolver(auth, resolver, config, adapters)
                .await?;
            let connected = self.connect_if_needed(&bootstrap, connector, None).await?;

            if let Some(commit) =
                self.handle
                    .record_session_phase(SessionPhase::Resyncing, None, vec![])?
            {
                commits.push(commit);
            }
            if let Some(commit) = self.handle.record_session_resync(&bootstrap, vec![])? {
                commits.push(commit);
            }

            Ok(RecoveryOutcome {
                run: SessionRun {
                    bootstrap,
                    connected,
                },
                commits,
            })
        })
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
    fn record_command_failure(
        &self,
        command_ids: &[CommandId],
        route_label: &str,
        err: &crate::ContractError,
    ) -> Result<()> {
        for &command_id in command_ids {
            self.handle.record_command_status(
                command_id,
                CommandStatus::Failed,
                Some(json!({
                    "route": route_label,
                    "message": err.to_string(),
                })),
                CommitScope::RealtimeUpdate,
            )?;
        }
        Ok(())
    }
}
