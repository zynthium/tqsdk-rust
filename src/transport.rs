use std::time::Duration;

use std::net::TcpStream;

use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message, WebSocket};

use crate::adapter::AdapterRegistry;
use crate::auth::{AuthContext, AuthProvider, ContractFuture};
use crate::commands::OutboundFrame;
use crate::ids::ProtocolDomain;
use crate::{ContractError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping,
    Pong,
    Close,
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
    socket: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
}

impl WebSocketTransport {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            socket: None,
        }
    }

    fn socket_mut(&mut self) -> Result<&mut WebSocket<MaybeTlsStream<TcpStream>>> {
        self.socket
            .as_mut()
            .ok_or_else(|| ContractError::validation("websocket transport is not connected"))
    }

    fn connect_blocking(&mut self) -> Result<()> {
        let (socket, _) = connect(self.url.as_str())
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
    pub enabled_domains: Vec<ProtocolDomain>,
}

impl SessionConfig {
    pub fn new(endpoints: EndpointConfig) -> Self {
        Self {
            endpoints,
            heartbeat: HeartbeatPolicy::default(),
            reconnect: ReconnectPolicy::default(),
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
}

impl BootstrapResult {
    pub fn new(auth: AuthContext, enabled_domains: Vec<ProtocolDomain>) -> Self {
        Self {
            phase: SessionPhase::Running,
            auth,
            enabled_domains,
        }
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
}
