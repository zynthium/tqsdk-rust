use std::collections::VecDeque;
use std::time::Duration;

use std::net::TcpStream;

use tungstenite::client::IntoClientRequest;
use tungstenite::http::{HeaderName, HeaderValue};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message, WebSocket};

use crate::adapter::AdapterRegistry;
use crate::auth::{AuthContext, AuthProvider, ContractFuture};
use crate::commands::{OutboundDispatch, OutboundFrame, OutboundRequest};
use crate::ids::{AccountId, ProtocolDomain, ReplaySessionId};
use crate::{ContractError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping,
    Pong,
    Close,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebSocketConnectOptions {
    pub headers: Vec<(String, String)>,
}

impl WebSocketConnectOptions {
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

pub trait Transport: Send {
    fn connect(&mut self) -> ContractFuture<'_, ()>;
    fn recv(&mut self) -> ContractFuture<'_, RawFrame>;
    fn send(&mut self, frame: OutboundFrame) -> ContractFuture<'_, ()>;
    fn close(&mut self) -> ContractFuture<'_, ()>;
}

#[derive(Debug)]
pub struct WebSocketTransport {
    url: String,
    connect_options: WebSocketConnectOptions,
    socket: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
}

impl WebSocketTransport {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            connect_options: WebSocketConnectOptions::default(),
            socket: None,
        }
    }

    pub fn with_connect_options(mut self, connect_options: WebSocketConnectOptions) -> Self {
        self.connect_options = connect_options;
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.connect_options = self.connect_options.with_header(name, value);
        self
    }

    fn socket_mut(&mut self) -> Result<&mut WebSocket<MaybeTlsStream<TcpStream>>> {
        self.socket
            .as_mut()
            .ok_or_else(|| ContractError::validation("websocket transport is not connected"))
    }

    fn connect_blocking(&mut self) -> Result<()> {
        let mut request = self
            .url
            .as_str()
            .into_client_request()
            .map_err(|err| ContractError::validation(format!("invalid websocket request: {err}")))?;

        for (name, value) in &self.connect_options.headers {
            let header_name = HeaderName::try_from(name.as_str())
                .map_err(|err| ContractError::validation(format!("invalid websocket header name: {err}")))?;
            let header_value = HeaderValue::from_str(value)
                .map_err(|err| ContractError::validation(format!("invalid websocket header value: {err}")))?;
            request.headers_mut().insert(header_name, header_value);
        }

        let (socket, _) = connect(request)
            .map_err(|err| ContractError::auth(format!("websocket connect failed: {err}")))?;
        self.socket = Some(socket);
        Ok(())
    }

    fn recv_blocking(&mut self) -> Result<RawFrame> {
        let message = self
            .socket_mut()?
            .read()
            .map_err(|err| ContractError::auth(format!("websocket recv failed: {err}")))?;

        match message {
            Message::Text(text) => Ok(RawFrame::Text(text.to_string())),
            Message::Binary(bytes) => Ok(RawFrame::Binary(bytes.to_vec())),
            Message::Ping(_) => Ok(RawFrame::Ping),
            Message::Pong(_) => Ok(RawFrame::Pong),
            Message::Close(_) => Ok(RawFrame::Close),
            other => Err(ContractError::validation(format!(
                "unsupported websocket message: {other:?}"
            ))),
        }
    }

    fn send_blocking(&mut self, frame: OutboundFrame) -> Result<()> {
        let message = match frame {
            OutboundFrame::Text(text) => Message::Text(text.into()),
            OutboundFrame::Binary(bytes) => Message::Binary(bytes.into()),
            OutboundFrame::Ping => Message::Ping(Vec::new().into()),
            OutboundFrame::Close => Message::Close(None),
        };

        self.socket_mut()?
            .send(message)
            .map_err(|err| ContractError::auth(format!("websocket send failed: {err}")))
    }

    fn close_blocking(&mut self) -> Result<()> {
        if let Some(mut socket) = self.socket.take() {
            socket
                .close(None)
                .map_err(|err| ContractError::auth(format!("websocket close failed: {err}")))?;
        }

        Ok(())
    }
}

impl Transport for WebSocketTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async move { self.connect_blocking() })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        Box::pin(async move { self.recv_blocking() })
    }

    fn send(&mut self, frame: OutboundFrame) -> ContractFuture<'_, ()> {
        Box::pin(async move { self.send_blocking(frame) })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async move { self.close_blocking() })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    pub fn new(initial_backoff: Duration, max_backoff: Duration, max_attempts: Option<u32>) -> Self {
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
            stock: false,
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
}

impl ConnectedSessionRoute {
    pub fn drain_pending_requests(&mut self) -> Vec<OutboundDispatch> {
        self.pending_requests.drain(..).collect()
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

    pub fn dispatch<'a>(&'a mut self, dispatch: OutboundDispatch) -> ContractFuture<'a, DispatchReceipt> {
        Box::pin(async move {
            let route = self
                .routes
                .iter_mut()
                .find(|route| route_accepts_dispatch(&route.route, &dispatch))
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
                OutboundRequest::Http(_) | OutboundRequest::Replay(_) | OutboundRequest::Internal(_) => {
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
}

pub trait SessionRouteConnector: Send + Sync {
    fn connect_route<'a>(&'a self, route: &'a SessionRoute) -> ContractFuture<'a, Box<dyn Transport>>;
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
    fn connect_route<'a>(&'a self, route: &'a SessionRoute) -> ContractFuture<'a, Box<dyn Transport>> {
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
    fn connect_route<'a>(&'a self, route: &'a SessionRoute) -> ContractFuture<'a, Box<dyn Transport>> {
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

fn route_accepts_dispatch(route: &SessionRoute, dispatch: &OutboundDispatch) -> bool {
    route.domains.contains(&dispatch.domain)
        && matches!(
            (&route.endpoint, &dispatch.request),
            (SessionRouteEndpoint::WebSocket { .. }, OutboundRequest::Transport(_))
                | (SessionRouteEndpoint::Http { .. }, OutboundRequest::Http(_))
                | (SessionRouteEndpoint::Replay { .. }, OutboundRequest::Replay(_))
                | (SessionRouteEndpoint::Internal { .. }, OutboundRequest::Internal(_))
        )
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
