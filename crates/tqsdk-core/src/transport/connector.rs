use std::future::Future;
use std::pin::Pin;

use crate::commands::OutboundFrame;
use crate::{ContractError, Result};

use super::frame::RawFrame;
use super::topology::{SessionRoute, SessionRouteEndpoint};
use super::websocket::{DynTransport, Transport, WebSocketTransport};

#[doc(hidden)]
pub type DynRouteConnectFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn DynTransport>>> + Send + 'a>>;

pub trait SessionRouteConnector: Send + Sync {
    fn connect_route<'a>(&'a self, route: &'a SessionRoute) -> DynRouteConnectFuture<'a>;
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
    async fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    fn recv(&mut self) -> impl Future<Output = Result<RawFrame>> + Send + '_ {
        let kind = self.kind;
        async move {
            Err(ContractError::validation(format!(
                "{kind} route transport does not support frame recv"
            )))
        }
    }

    fn send(&mut self, _frame: OutboundFrame) -> impl Future<Output = Result<()>> + Send + '_ {
        let kind = self.kind;
        async move {
            Err(ContractError::validation(format!(
                "{kind} route transport does not support frame send"
            )))
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebSocketRouteConnector;

impl SessionRouteConnector for WebSocketRouteConnector {
    fn connect_route<'a>(&'a self, route: &'a SessionRoute) -> DynRouteConnectFuture<'a> {
        Box::pin(async move {
            match &route.endpoint {
                SessionRouteEndpoint::WebSocket { url, connect } => {
                    let mut transport =
                        WebSocketTransport::new(url.clone()).with_connect_options(connect.clone());
                    transport.connect().await?;
                    Ok(Box::new(transport) as Box<dyn DynTransport>)
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
    fn connect_route<'a>(&'a self, route: &'a SessionRoute) -> DynRouteConnectFuture<'a> {
        Box::pin(async move {
            match &route.endpoint {
                SessionRouteEndpoint::WebSocket { .. } => self.websocket.connect_route(route).await,
                SessionRouteEndpoint::Http { .. } => {
                    Ok(Box::new(PassiveRouteTransport::new("http")) as Box<dyn DynTransport>)
                }
                SessionRouteEndpoint::Replay { .. } => {
                    Ok(Box::new(PassiveRouteTransport::new("replay")) as Box<dyn DynTransport>)
                }
                SessionRouteEndpoint::Internal { .. } => {
                    Ok(Box::new(PassiveRouteTransport::new("internal")) as Box<dyn DynTransport>)
                }
            }
        })
    }
}
