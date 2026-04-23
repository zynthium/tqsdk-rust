use std::collections::VecDeque;
use std::time::Duration;

use futures::SinkExt;
use url::Url;
use yawc::frame::{Frame, OpCode};
use yawc::{HttpRequestBuilder, Options, TcpWebSocket, WebSocket};

use crate::adapter::AdapterRegistry;
use crate::auth::{AuthContext, AuthProvider, ContractFuture};
use crate::commands::{OutboundDispatch, OutboundFrame, OutboundRequest};
use crate::events::{InputPayload, InternalEvent, IoEvent, RuntimeInput};
use crate::ids::{AccountId, ProtocolDomain, ReplaySessionId};
use crate::{ContractError, Result};
use serde_json::{Value, json};

const DEFAULT_AUTH_URL: &str = "https://auth.shinnytech.com";

fn read_optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_env_or_default(name: &str, default: &str) -> String {
    read_optional_env(name).unwrap_or_else(|| default.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping,
    Pong,
    Close,
}

/// Handshake options applied when opening a websocket route.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebSocketConnectOptions {
    pub headers: Vec<(String, String)>,
}

impl WebSocketConnectOptions {
    /// Adds a header to the websocket handshake request.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// Minimal async transport abstraction used by connected session routes.
pub trait Transport: Send {
    fn connect(&mut self) -> ContractFuture<'_, ()>;
    fn recv(&mut self) -> ContractFuture<'_, RawFrame>;
    fn send(&mut self, frame: OutboundFrame) -> ContractFuture<'_, ()>;
    fn close(&mut self) -> ContractFuture<'_, ()>;
}

/// Thin websocket transport built on `yawc`.
///
/// The transport requires an ambient Tokio runtime and only covers raw frame
/// I/O. Route selection, reconnect policy, heartbeat semantics, and state
/// projection remain the responsibility of higher contract layers.
pub struct WebSocketTransport {
    url: String,
    connect_options: WebSocketConnectOptions,
    socket: Option<TcpWebSocket>,
}

impl WebSocketTransport {
    /// Creates a websocket transport for the provided route URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connect_options: WebSocketConnectOptions::default(),
            socket: None,
        }
    }

    /// Replaces the current websocket handshake options.
    pub fn with_connect_options(mut self, connect_options: WebSocketConnectOptions) -> Self {
        self.connect_options = connect_options;
        self
    }

    /// Adds a handshake header to the websocket request.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.connect_options = self.connect_options.with_header(name, value);
        self
    }

    fn decode_frame(frame: Frame) -> Result<RawFrame> {
        match frame.opcode() {
            OpCode::Text => {
                let text = String::from_utf8(frame.payload().to_vec()).map_err(|err| {
                    ContractError::validation(format!("invalid websocket text frame: {err}"))
                })?;
                Ok(RawFrame::Text(text))
            }
            OpCode::Binary => Ok(RawFrame::Binary(frame.payload().to_vec())),
            OpCode::Ping => Ok(RawFrame::Ping),
            OpCode::Pong => Ok(RawFrame::Pong),
            OpCode::Close => Ok(RawFrame::Close),
            other => Err(ContractError::validation(format!(
                "unsupported websocket message: {other:?}"
            ))),
        }
    }

    async fn connect_with_request(
        url: Url,
        request: HttpRequestBuilder,
    ) -> std::result::Result<TcpWebSocket, yawc::WebSocketError> {
        let options = Options::default()
            .client_no_context_takeover()
            .server_no_context_takeover();
        WebSocket::connect(url)
            .with_options(options)
            .with_request(request)
            .await
    }

    async fn connect_async(&mut self) -> Result<()> {
        require_tokio_runtime()?;
        let url = Url::parse(&self.url)
            .map_err(|err| ContractError::validation(format!("invalid websocket url: {err}")))?;
        let mut request = HttpRequestBuilder::new();
        for (name, value) in &self.connect_options.headers {
            request = request.header(name.as_str(), value.as_str());
        }

        let socket = Self::connect_with_request(url, request)
            .await
            .map_err(|err| ContractError::transport(format!("websocket connect failed: {err}")))?;
        self.socket = Some(socket);
        Ok(())
    }

    async fn recv_async(&mut self) -> Result<RawFrame> {
        require_tokio_runtime()?;
        let Self { socket, .. } = self;
        let socket = socket
            .as_mut()
            .ok_or_else(|| ContractError::validation("websocket transport is not connected"))?;
        let frame = socket
            .next_frame()
            .await
            .map_err(|err| ContractError::transport(format!("websocket recv failed: {err}")))?;
        Self::decode_frame(frame)
    }

    async fn send_async(&mut self, frame: OutboundFrame) -> Result<()> {
        require_tokio_runtime()?;
        let frame = match frame {
            OutboundFrame::Text(text) => Frame::text(text),
            OutboundFrame::Binary(bytes) => Frame::binary(bytes),
            OutboundFrame::Ping => Frame::ping(Vec::<u8>::new()),
            OutboundFrame::Close => return self.close_async().await,
        };

        let Self { socket, .. } = self;
        let socket = socket
            .as_mut()
            .ok_or_else(|| ContractError::validation("websocket transport is not connected"))?;
        socket
            .send(frame)
            .await
            .map_err(|err| ContractError::transport(format!("websocket send failed: {err}")))
    }

    async fn close_async(&mut self) -> Result<()> {
        require_tokio_runtime()?;
        let Some(mut socket) = self.socket.take() else {
            return Ok(());
        };
        socket
            .close()
            .await
            .map_err(|err| ContractError::transport(format!("websocket close failed: {err}")))?;

        Ok(())
    }
}

impl Transport for WebSocketTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async move { self.connect_async().await })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        Box::pin(async move { self.recv_async().await })
    }

    fn send(&mut self, frame: OutboundFrame) -> ContractFuture<'_, ()> {
        Box::pin(async move { self.send_async(frame).await })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async move { self.close_async().await })
    }
}

fn require_tokio_runtime() -> Result<()> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        ContractError::validation("websocket transport requires an active Tokio runtime")
    })?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointConfig {
    pub auth_url: Option<String>,
    pub market_url: Option<String>,
    pub trade_url: Option<String>,
    pub query_url: Option<String>,
    pub replay_url: Option<String>,
    pub schema_url: Option<String>,
}

impl EndpointConfig {
    pub fn new(auth_url: impl Into<String>) -> Self {
        Self {
            auth_url: Some(auth_url.into()),
            market_url: None,
            trade_url: None,
            query_url: None,
            replay_url: None,
            schema_url: None,
        }
    }

    pub fn from_env() -> Self {
        Self {
            auth_url: Some(read_env_or_default("TQ_AUTH_URL", DEFAULT_AUTH_URL)),
            market_url: read_optional_env("TQ_MD_URL"),
            trade_url: read_optional_env("TQ_TD_URL"),
            query_url: None,
            replay_url: None,
            schema_url: None,
        }
    }

    pub fn with_market_url(mut self, market_url: impl Into<String>) -> Self {
        self.market_url = Some(market_url.into());
        self
    }

    pub fn with_trade_url(mut self, trade_url: impl Into<String>) -> Self {
        self.trade_url = Some(trade_url.into());
        self
    }

    pub fn with_query_url(mut self, query_url: impl Into<String>) -> Self {
        self.query_url = Some(query_url.into());
        self
    }

    pub fn with_replay_url(mut self, replay_url: impl Into<String>) -> Self {
        self.replay_url = Some(replay_url.into());
        self
    }

    pub fn with_schema_url(mut self, schema_url: impl Into<String>) -> Self {
        self.schema_url = Some(schema_url.into());
        self
    }
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatPolicy {
    pub interval: Duration,
    pub timeout: Duration,
}

impl HeartbeatPolicy {
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self { interval, timeout }
    }
}

impl Default for HeartbeatPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_attempts: Option<u32>,
}

impl ReconnectPolicy {
    pub fn new(
        initial_backoff: Duration,
        max_backoff: Duration,
        max_attempts: Option<u32>,
    ) -> Self {
        Self {
            initial_backoff,
            max_backoff,
            max_attempts,
        }
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            max_attempts: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    pub endpoints: EndpointConfig,
    pub heartbeat: HeartbeatPolicy,
    pub reconnect: ReconnectPolicy,
    pub market_target: MarketSessionTarget,
    pub trade_targets: Vec<TradeSessionTarget>,
    pub enabled_domains: Vec<ProtocolDomain>,
}

impl SessionConfig {
    pub fn new(endpoints: EndpointConfig) -> Self {
        Self {
            endpoints,
            heartbeat: HeartbeatPolicy::default(),
            reconnect: ReconnectPolicy::default(),
            market_target: MarketSessionTarget::default(),
            trade_targets: Vec::new(),
            enabled_domains: Vec::new(),
        }
    }

    pub fn with_heartbeat(mut self, heartbeat: HeartbeatPolicy) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    pub fn with_reconnect(mut self, reconnect: ReconnectPolicy) -> Self {
        self.reconnect = reconnect;
        self
    }

    pub fn with_market_target(mut self, market_target: MarketSessionTarget) -> Self {
        self.market_target = market_target;
        self
    }

    pub fn add_trade_target(mut self, trade_target: TradeSessionTarget) -> Self {
        self.trade_targets.push(trade_target);
        self
    }

    pub fn enable_domain(mut self, domain: ProtocolDomain) -> Self {
        if !self.enabled_domains.contains(&domain) {
            self.enabled_domains.push(domain);
        }
        self
    }

    pub fn enabled_domains(&self) -> &[ProtocolDomain] {
        &self.enabled_domains
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketSessionTarget {
    pub stock: bool,
    pub backtest: bool,
}

impl MarketSessionTarget {
    pub fn new(stock: bool, backtest: bool) -> Self {
        Self { stock, backtest }
    }
}

impl Default for MarketSessionTarget {
    fn default() -> Self {
        Self {
            stock: true,
            backtest: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeSessionTarget {
    pub broker_id: String,
    pub account_id: AccountId,
    pub trade_url: Option<String>,
}

impl TradeSessionTarget {
    pub fn new(broker_id: impl Into<String>, account_id: AccountId) -> Self {
        Self {
            broker_id: broker_id.into(),
            account_id,
            trade_url: None,
        }
    }

    pub fn with_trade_url(mut self, trade_url: impl Into<String>) -> Self {
        self.trade_url = Some(trade_url.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTarget {
    Shared,
    Account(AccountId),
    Replay(ReplaySessionId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionRouteEndpoint {
    WebSocket {
        url: String,
        connect: WebSocketConnectOptions,
    },
    Http {
        url: String,
    },
    Replay {
        label: String,
    },
    Internal {
        label: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRoute {
    pub label: String,
    pub target: SessionTarget,
    pub domains: Vec<ProtocolDomain>,
    pub endpoint: SessionRouteEndpoint,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionTopology {
    pub routes: Vec<SessionRoute>,
}

impl SessionTopology {
    pub fn with_route(mut self, route: SessionRoute) -> Self {
        self.routes.push(route);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPhase {
    Idle,
    Authenticating,
    Connecting,
    Bootstrapping,
    Running,
    Reconnecting,
    Resyncing,
    Closed,
}

impl SessionPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Authenticating => "authenticating",
            Self::Connecting => "connecting",
            Self::Bootstrapping => "bootstrapping",
            Self::Running => "running",
            Self::Reconnecting => "reconnecting",
            Self::Resyncing => "resyncing",
            Self::Closed => "closed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapResult {
    pub phase: SessionPhase,
    pub auth: AuthContext,
    pub enabled_domains: Vec<ProtocolDomain>,
    pub topology: SessionTopology,
}

impl BootstrapResult {
    pub fn new(auth: AuthContext, enabled_domains: Vec<ProtocolDomain>) -> Self {
        Self {
            phase: SessionPhase::Running,
            auth,
            enabled_domains,
            topology: SessionTopology::default(),
        }
    }

    pub fn with_topology(mut self, topology: SessionTopology) -> Self {
        self.topology = topology;
        self
    }
}

pub trait SessionTopologyResolver: Send + Sync {
    fn resolve_topology<'a>(
        &'a self,
        auth: &'a AuthContext,
        config: &'a SessionConfig,
        enabled_domains: &'a [ProtocolDomain],
    ) -> ContractFuture<'a, SessionTopology>;
}

pub struct ConnectedSessionRoute {
    pub route: SessionRoute,
    pub transport: Box<dyn Transport>,
    pending_requests: VecDeque<OutboundDispatch>,
    pending_inputs: VecDeque<RuntimeInput>,
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

    pub fn recv_input<'a>(&'a mut self) -> ContractFuture<'a, Option<RuntimeInput>> {
        Box::pin(async move {
            if let Some(input) = self.pending_inputs.pop_front() {
                return Ok(Some(input));
            }

            if !matches!(self.route.endpoint, SessionRouteEndpoint::WebSocket { .. }) {
                return Ok(None);
            }

            let frame = self.transport.recv().await?;
            map_raw_frame_to_input(&self.route, frame)
        })
    }
}

#[derive(Default)]
pub struct ConnectedTopology {
    pub routes: Vec<ConnectedSessionRoute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchReceipt {
    pub command_id: crate::ids::CommandId,
    pub domain: ProtocolDomain,
    pub route_label: String,
}

impl ConnectedTopology {
    pub fn close_all<'a>(&'a mut self) -> ContractFuture<'a, ()> {
        Box::pin(async move {
            for route in &mut self.routes {
                route.transport.close().await?;
            }
            Ok(())
        })
    }

    pub fn dispatch<'a>(
        &'a mut self,
        dispatch: OutboundDispatch,
    ) -> ContractFuture<'a, DispatchReceipt> {
        Box::pin(async move { self.dispatch_ref(&dispatch).await })
    }

    /// Dispatches a request without moving ownership out of the caller.
    ///
    /// This is the preferred hot-path surface when the caller still needs the
    /// dispatch metadata for command-status projection after the route send.
    pub fn dispatch_ref<'a>(
        &'a mut self,
        dispatch: &'a OutboundDispatch,
    ) -> ContractFuture<'a, DispatchReceipt> {
        Box::pin(async move {
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
                        "no connected route for {} {:?} request",
                        dispatch.domain.as_str(),
                        dispatch.request
                    ))
                })?;

            match &dispatch.request {
                OutboundRequest::Transport(frame) => {
                    route.transport.send(frame.clone()).await?;
                }
                OutboundRequest::Query(query) => match &route.route.endpoint {
                    SessionRouteEndpoint::WebSocket { .. } => {
                        route
                            .transport
                            .send(OutboundFrame::Text(query.body().to_string()))
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

    pub fn recv_route_input<'a>(
        &'a mut self,
        label: &'a str,
    ) -> ContractFuture<'a, Option<RuntimeInput>> {
        Box::pin(async move {
            let Some(route) = self.route_mut(label) else {
                return Err(ContractError::validation(format!(
                    "unknown connected route for input recv: {label}"
                )));
            };
            route.recv_input().await
        })
    }

    pub fn send_route_frame<'a>(
        &'a mut self,
        label: &'a str,
        frame: OutboundFrame,
    ) -> ContractFuture<'a, ()> {
        Box::pin(async move {
            let Some(route) = self.route_mut(label) else {
                return Err(ContractError::validation(format!(
                    "unknown connected route for frame send: {label}"
                )));
            };
            route.transport.send(frame).await
        })
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

pub trait SessionRouteConnector: Send + Sync {
    fn connect_route<'a>(
        &'a self,
        route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PassiveRouteTransport {
    kind: &'static str,
}

impl PassiveRouteTransport {
    fn new(kind: &'static str) -> Self {
        Self { kind }
    }
}

impl Transport for PassiveRouteTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        let kind = self.kind;
        Box::pin(async move {
            Err(ContractError::validation(format!(
                "{kind} route transport does not support frame recv"
            )))
        })
    }

    fn send(&mut self, _frame: OutboundFrame) -> ContractFuture<'_, ()> {
        let kind = self.kind;
        Box::pin(async move {
            Err(ContractError::validation(format!(
                "{kind} route transport does not support frame send"
            )))
        })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebSocketRouteConnector;

impl SessionRouteConnector for WebSocketRouteConnector {
    fn connect_route<'a>(
        &'a self,
        route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>> {
        Box::pin(async move {
            match &route.endpoint {
                SessionRouteEndpoint::WebSocket { url, connect } => {
                    let mut transport =
                        WebSocketTransport::new(url.clone()).with_connect_options(connect.clone());
                    transport.connect().await?;
                    Ok(Box::new(transport) as Box<dyn Transport>)
                }
                other => Err(ContractError::validation(format!(
                    "unsupported route endpoint for websocket connector: {other:?}"
                ))),
            }
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefaultRouteConnector {
    websocket: WebSocketRouteConnector,
}

impl SessionRouteConnector for DefaultRouteConnector {
    fn connect_route<'a>(
        &'a self,
        route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>> {
        Box::pin(async move {
            match &route.endpoint {
                SessionRouteEndpoint::WebSocket { .. } => self.websocket.connect_route(route).await,
                SessionRouteEndpoint::Http { .. } => {
                    Ok(Box::new(PassiveRouteTransport::new("http")) as Box<dyn Transport>)
                }
                SessionRouteEndpoint::Replay { .. } => {
                    Ok(Box::new(PassiveRouteTransport::new("replay")) as Box<dyn Transport>)
                }
                SessionRouteEndpoint::Internal { .. } => {
                    Ok(Box::new(PassiveRouteTransport::new("internal")) as Box<dyn Transport>)
                }
            }
        })
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

fn map_raw_frame_to_input(route: &SessionRoute, frame: RawFrame) -> Result<Option<RuntimeInput>> {
    match frame {
        RawFrame::Text(text) => Ok(Some(RuntimeInput::Io(IoEvent {
            route: route.label.clone(),
            domains: route.domains.clone(),
            payload: parse_text_payload(text)?,
        }))),
        RawFrame::Binary(bytes) => Ok(Some(RuntimeInput::Io(IoEvent {
            route: route.label.clone(),
            domains: route.domains.clone(),
            payload: parse_binary_payload(bytes)?,
        }))),
        RawFrame::Ping => Ok(Some(RuntimeInput::Internal(InternalEvent {
            label: "transport-ping",
            payload: Some(json!({
                "route": route.label,
                "domains": route.domains.iter().copied().map(ProtocolDomain::as_str).collect::<Vec<_>>(),
            })),
        }))),
        RawFrame::Pong => Ok(Some(RuntimeInput::Internal(InternalEvent {
            label: "transport-pong",
            payload: Some(json!({
                "route": route.label,
                "domains": route.domains.iter().copied().map(ProtocolDomain::as_str).collect::<Vec<_>>(),
            })),
        }))),
        RawFrame::Close => Ok(Some(RuntimeInput::Internal(InternalEvent {
            label: "transport-close",
            payload: Some(json!({
                "route": route.label,
                "domains": route.domains.iter().copied().map(ProtocolDomain::as_str).collect::<Vec<_>>(),
            })),
        }))),
    }
}

fn parse_text_payload(text: String) -> Result<InputPayload> {
    match serde_json::from_str::<Value>(&text) {
        Ok(value) => Ok(InputPayload::Json(value)),
        Err(_) => Ok(InputPayload::Text(text)),
    }
}

fn parse_binary_payload(bytes: Vec<u8>) -> Result<InputPayload> {
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => Ok(InputPayload::Json(value)),
        Err(_) => Ok(InputPayload::Binary(bytes)),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionBootstrap;

impl SessionBootstrap {
    pub fn new() -> Self {
        Self
    }

    pub fn establish<'a>(
        &self,
        auth: &'a dyn AuthProvider,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> ContractFuture<'a, BootstrapResult> {
        Box::pin(async move {
            let auth = auth.authenticate().await?;
            let enabled_domains = if config.enabled_domains.is_empty() {
                adapters.domains().to_vec()
            } else {
                config.enabled_domains.clone()
            };

            Ok(BootstrapResult::new(auth, enabled_domains))
        })
    }

    pub fn establish_with_resolver<'a>(
        &self,
        auth: &'a dyn AuthProvider,
        resolver: &'a dyn SessionTopologyResolver,
        config: &'a SessionConfig,
        adapters: &'a AdapterRegistry,
    ) -> ContractFuture<'a, BootstrapResult> {
        Box::pin(async move {
            let auth = auth.authenticate().await?;
            let enabled_domains = if config.enabled_domains.is_empty() {
                adapters.domains().to_vec()
            } else {
                config.enabled_domains.clone()
            };
            let topology = resolver
                .resolve_topology(&auth, config, &enabled_domains)
                .await?;

            Ok(BootstrapResult::new(auth, enabled_domains).with_topology(topology))
        })
    }

    pub fn connect_topology<'a>(
        &self,
        topology: &'a SessionTopology,
        connector: &'a dyn SessionRouteConnector,
    ) -> ContractFuture<'a, ConnectedTopology> {
        Box::pin(async move {
            let mut connected = ConnectedTopology::default();

            for route in &topology.routes {
                match connector.connect_route(route).await {
                    Ok(transport) => connected.routes.push(ConnectedSessionRoute {
                        route: route.clone(),
                        transport,
                        pending_requests: VecDeque::new(),
                        pending_inputs: VecDeque::new(),
                    }),
                    Err(err) => {
                        let _ = connected.close_all().await;
                        return Err(err);
                    }
                }
            }

            Ok(connected)
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        InputPayload, IoEvent, ProtocolDomain, RawFrame, RuntimeInput, SessionRoute,
        SessionRouteEndpoint, SessionTarget, map_raw_frame_to_input, parse_binary_payload,
        parse_text_payload,
    };

    #[test]
    fn parse_text_payload_decodes_json_when_possible() {
        let payload = parse_text_payload(r#"{"aid":"rtn_data"}"#.to_string()).unwrap();
        assert_eq!(payload, InputPayload::Json(json!({ "aid": "rtn_data" })));
    }

    #[test]
    fn parse_binary_payload_decodes_json_when_possible() {
        let payload = parse_binary_payload(br#"{"aid":"rtn_data"}"#.to_vec()).unwrap();
        assert_eq!(payload, InputPayload::Json(json!({ "aid": "rtn_data" })));
    }

    #[test]
    fn parse_binary_payload_preserves_non_json_bytes() {
        let payload = parse_binary_payload(vec![0_u8, 1, 2, 3]).unwrap();
        assert_eq!(payload, InputPayload::Binary(vec![0_u8, 1, 2, 3]));
    }

    #[test]
    fn map_raw_binary_frame_to_json_io_when_payload_is_json() {
        let route = SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Market],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: "wss://market.example".to_string(),
                connect: super::WebSocketConnectOptions::default(),
            },
        };

        let input = map_raw_frame_to_input(
            &route,
            RawFrame::Binary(
                br#"{"aid":"rtn_data","data":[{"quotes":{"SHFE.au2602":{"last_price":618.5}}}]}"#
                    .to_vec(),
            ),
        )
        .unwrap();

        assert!(matches!(
            input,
            Some(RuntimeInput::Io(IoEvent {
                payload: InputPayload::Json(_),
                ..
            }))
        ));
    }
}
