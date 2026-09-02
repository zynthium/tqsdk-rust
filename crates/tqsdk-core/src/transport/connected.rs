use std::collections::VecDeque;

use crate::commands::{OutboundDispatch, OutboundFrame, OutboundRequest};
use crate::events::RuntimeInput;
use crate::ids::{CommandId, ProtocolDomain};
use crate::{ContractError, Result};

use super::frame::map_raw_frame_to_input;
use super::io::DynTransport;
use super::topology::{SessionRoute, SessionRouteEndpoint, SessionTarget};

pub struct ConnectedSessionRoute {
    pub route: SessionRoute,
    pub transport: Box<dyn DynTransport>,
    pub(super) pending_requests: VecDeque<OutboundDispatch>,
    pub(super) pending_inputs: VecDeque<RuntimeInput>,
}

impl ConnectedSessionRoute {
    pub fn drain_pending_requests(&mut self) -> Vec<OutboundDispatch> {
        self.pending_requests.drain(..).collect()
    }

    pub fn queue_input(&mut self, input: RuntimeInput) {
        self.pending_inputs.push_back(input);
    }

    pub fn drain_queued_inputs(&mut self) -> Vec<RuntimeInput> {
        self.pending_inputs.drain(..).collect()
    }

    pub async fn recv_input(&mut self) -> Result<Option<RuntimeInput>> {
        if let Some(input) = self.pending_inputs.pop_front() {
            return Ok(Some(input));
        }

        if !matches!(self.route.endpoint, SessionRouteEndpoint::WebSocket { .. }) {
            return Ok(None);
        }

        let frame = self.transport.recv_boxed().await?;
        map_raw_frame_to_input(&self.route, frame)
    }
}

#[derive(Default)]
pub struct ConnectedTopology {
    pub routes: Vec<ConnectedSessionRoute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchReceipt {
    pub command_id: CommandId,
    pub domain: ProtocolDomain,
    pub route_label: String,
}

impl ConnectedTopology {
    pub async fn close_all(&mut self) -> Result<()> {
        for route in &mut self.routes {
            route.transport.close_boxed().await?;
        }
        Ok(())
    }

    pub async fn dispatch(&mut self, dispatch: OutboundDispatch) -> Result<DispatchReceipt> {
        self.dispatch_ref(&dispatch).await
    }

    /// Dispatches a request without moving ownership out of the caller.
    ///
    /// This is the preferred hot-path surface when the caller still needs the
    /// dispatch metadata for command-status projection after the route send.
    pub async fn dispatch_ref(&mut self, dispatch: &OutboundDispatch) -> Result<DispatchReceipt> {
        let route = self
            .routes
            .iter_mut()
            .filter_map(|route| {
                route_dispatch_match_score(&route.route, dispatch).map(|score| (score, route))
            })
            .max_by_key(|(score, _route)| *score)
            .map(|(_score, route)| route)
            .ok_or_else(|| {
                ContractError::validation(format!(
                    "no connected route for {} {} request",
                    dispatch.domain.as_str(),
                    outbound_request_kind(&dispatch.request)
                ))
            })?;

        match &dispatch.request {
            OutboundRequest::Transport(frame) => {
                route.transport.send_boxed(frame.clone()).await?;
            }
            OutboundRequest::Query(query) => match &route.route.endpoint {
                SessionRouteEndpoint::WebSocket { .. } => {
                    route
                        .transport
                        .send_boxed(OutboundFrame::Text(query.body().to_string()))
                        .await?;
                }
                SessionRouteEndpoint::Http { .. } => {
                    route.pending_requests.push_back(dispatch.clone());
                }
                SessionRouteEndpoint::Replay { .. } | SessionRouteEndpoint::Internal { .. } => {
                    return Err(ContractError::validation(format!(
                        "query request cannot be dispatched to {:?}",
                        route.route.endpoint
                    )));
                }
            },
            OutboundRequest::Http(_)
            | OutboundRequest::Replay(_)
            | OutboundRequest::Internal(_) => {
                route.pending_requests.push_back(dispatch.clone());
            }
        }

        Ok(DispatchReceipt {
            command_id: dispatch.command_id,
            domain: dispatch.domain,
            route_label: route.route.label.clone(),
        })
    }

    pub fn route_mut(&mut self, label: &str) -> Option<&mut ConnectedSessionRoute> {
        self.routes
            .iter_mut()
            .find(|route| route.route.label == label)
    }

    pub fn route_label_for_dispatch(&self, dispatch: &OutboundDispatch) -> Option<&str> {
        self.routes
            .iter()
            .filter_map(|route| {
                route_dispatch_match_score(&route.route, dispatch).map(|score| (score, route))
            })
            .max_by_key(|(score, _route)| *score)
            .map(|(_score, route)| route.route.label.as_str())
    }

    pub fn has_route(&self, label: &str) -> bool {
        self.routes.iter().any(|route| route.route.label == label)
    }

    pub async fn recv_route_input(&mut self, label: &str) -> Result<Option<RuntimeInput>> {
        let Some(route) = self.route_mut(label) else {
            return Err(ContractError::validation(format!(
                "unknown connected route for input recv: {label}"
            )));
        };
        route.recv_input().await
    }

    pub async fn send_route_frame(&mut self, label: &str, frame: OutboundFrame) -> Result<()> {
        let Some(route) = self.route_mut(label) else {
            return Err(ContractError::validation(format!(
                "unknown connected route for frame send: {label}"
            )));
        };
        route.transport.send_boxed(frame).await
    }

    pub fn take_route_requests(
        &mut self,
        label: &str,
    ) -> Result<(SessionRoute, Vec<OutboundDispatch>)> {
        let Some(route) = self.route_mut(label) else {
            return Err(ContractError::validation(format!(
                "unknown connected route for pending request drain: {label}"
            )));
        };
        Ok((route.route.clone(), route.drain_pending_requests()))
    }

    pub fn drain_queued_inputs(&mut self) -> Vec<RuntimeInput> {
        let mut inputs = Vec::new();
        for route in &mut self.routes {
            inputs.extend(route.drain_queued_inputs());
        }
        inputs
    }
}

fn outbound_request_kind(request: &OutboundRequest) -> &'static str {
    match request {
        OutboundRequest::Transport(_) => "transport",
        OutboundRequest::Http(_) => "http",
        OutboundRequest::Query(_) => "query",
        OutboundRequest::Replay(_) => "replay",
        OutboundRequest::Internal(_) => "internal",
    }
}

fn route_dispatch_match_score(route: &SessionRoute, dispatch: &OutboundDispatch) -> Option<u8> {
    let request_matches_route = route.domains.contains(&dispatch.domain)
        && matches!(
            (&route.endpoint, &dispatch.request),
            (
                SessionRouteEndpoint::WebSocket { .. },
                OutboundRequest::Transport(_)
            ) | (
                SessionRouteEndpoint::WebSocket { .. },
                OutboundRequest::Query(_)
            ) | (SessionRouteEndpoint::Http { .. }, OutboundRequest::Http(_))
                | (SessionRouteEndpoint::Http { .. }, OutboundRequest::Query(_))
                | (
                    SessionRouteEndpoint::Replay { .. },
                    OutboundRequest::Replay(_)
                )
                | (
                    SessionRouteEndpoint::Internal { .. },
                    OutboundRequest::Internal(_)
                )
        );
    if !request_matches_route {
        return None;
    }

    match (&route.target, dispatch.account_id.as_ref()) {
        (SessionTarget::Account(route_account_id), Some(dispatch_account_id))
            if route_account_id == dispatch_account_id =>
        {
            Some(2)
        }
        (SessionTarget::Account(_), _) => None,
        (SessionTarget::Shared, _) | (SessionTarget::Replay(_), _) => Some(1),
    }
}
