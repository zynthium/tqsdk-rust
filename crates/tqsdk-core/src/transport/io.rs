use std::future::Future;
use std::pin::Pin;

use crate::Result;
use crate::commands::OutboundFrame;

use super::frame::RawFrame;

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
    fn connect(&mut self) -> impl Future<Output = Result<()>> + Send + '_;
    fn recv(&mut self) -> impl Future<Output = Result<RawFrame>> + Send + '_;
    fn send(&mut self, frame: OutboundFrame) -> impl Future<Output = Result<()>> + Send + '_;
    fn close(&mut self) -> impl Future<Output = Result<()>> + Send + '_;
}

#[doc(hidden)]
pub trait DynTransport: Send {
    fn connect_boxed(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn recv_boxed(&mut self) -> Pin<Box<dyn Future<Output = Result<RawFrame>> + Send + '_>>;
    fn send_boxed(
        &mut self,
        frame: OutboundFrame,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn close_boxed(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

impl<T> DynTransport for T
where
    T: Transport,
{
    fn connect_boxed(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(self.connect())
    }

    fn recv_boxed(&mut self) -> Pin<Box<dyn Future<Output = Result<RawFrame>> + Send + '_>> {
        Box::pin(self.recv())
    }

    fn send_boxed(
        &mut self,
        frame: OutboundFrame,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(self.send(frame))
    }

    fn close_boxed(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(self.close())
    }
}
