use serde_json::{Value, json};

use crate::{
    Result,
    commands::OutboundFrame,
    events::{InternalEvent, RuntimeInput, TimerEvent},
    ids::CommandId,
    state::CommitScope,
    transport::SessionPhase,
};

use super::{RoutePumpOutcome, SessionRun, SessionRuntime, SessionRuntimeDeps, SessionStepOutcome};

impl SessionRuntime {
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
                let mut outcome = RoutePumpOutcome {
                    received_input: true,
                    ..RoutePumpOutcome::default()
                };
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

    pub async fn drive_timer_once(
        &self,
        run: &mut SessionRun,
        timer: TimerEvent,
        caused_by: Vec<CommandId>,
        deps: SessionRuntimeDeps<'_>,
    ) -> Result<SessionStepOutcome> {
        let mut outcome = SessionStepOutcome::default();

        let heartbeat_route = match timer.label {
            "heartbeat-due" | "heartbeat-timeout" => {
                let route_label = timer_route_label(&timer)?;
                if !run.connected.has_route(route_label) {
                    return Err(crate::ContractError::validation(format!(
                        "unknown connected route for timer event: {route_label}"
                    )));
                }
                Some(route_label.to_string())
            }
            _ => None,
        };

        if let Some(commit) = self.handle.ingest(
            RuntimeInput::Timer(timer.clone()),
            caused_by.clone(),
            CommitScope::SessionTransition,
        )? {
            outcome.commits.push(commit);
        }

        match timer.label {
            "heartbeat-due" => {
                let route_label = heartbeat_route
                    .as_deref()
                    .expect("heartbeat timer route was validated");
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
                let route_label = heartbeat_route
                    .as_deref()
                    .expect("heartbeat timer route was validated");
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
            received_input: false,
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
            received_input: false,
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
