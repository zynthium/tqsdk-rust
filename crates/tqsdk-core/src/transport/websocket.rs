use std::future::Future;
use std::pin::Pin;

use futures::SinkExt;
use url::Url;
use yawc::frame::{Frame, OpCode};
use yawc::{HttpRequestBuilder, Options, TcpWebSocket, WebSocket};

use crate::commands::OutboundFrame;
use crate::{ContractError, Result};

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
    async fn connect(&mut self) -> Result<()> {
        self.connect_async().await
    }

    async fn recv(&mut self) -> Result<RawFrame> {
        self.recv_async().await
    }

    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        self.send_async(frame).await
    }

    async fn close(&mut self) -> Result<()> {
        self.close_async().await
    }
}

fn require_tokio_runtime() -> Result<()> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        ContractError::validation("websocket transport requires an active Tokio runtime")
    })?;
    Ok(())
}
