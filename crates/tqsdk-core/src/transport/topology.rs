use std::future::Future;
use std::pin::Pin;

use crate::Result;
use crate::auth::AuthContext;
use crate::ids::{AccountId, ProtocolDomain, ReplaySessionId};

use super::config::SessionConfig;
use super::websocket::WebSocketConnectOptions;

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
    ) -> Pin<Box<dyn Future<Output = Result<SessionTopology>> + Send + 'a>>;
}
