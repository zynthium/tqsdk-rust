use std::time::Duration;

use futures::SinkExt;
use url::Url;
use yawc::frame::{Frame, OpCode};
use yawc::{CompressionLevel, HttpRequestBuilder, Options, TcpWebSocket, WebSocket};

use crate::commands::OutboundFrame;
use crate::{ContractError, Result};

use super::frame::RawFrame;
use super::io::{Transport, WebSocketConnectOptions};

const WEBSOCKET_CONNECT_ATTEMPTS: usize = 3;
const WEBSOCKET_CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);
const WEBSOCKET_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Thin websocket transport built on `yawc`.
///
/// The transport requires an ambient Tokio runtime. Initial socket/TLS
/// establishment uses a small bounded retry budget so a transient blackholed
/// route does not fail the whole session bootstrap. Route selection,
/// established-session reconnect policy, heartbeat semantics, and state
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
        // Tianqin replies to a bare deflate offer with a quoted window-bits parameter
        // that yawc 0.3.3 does not parse, so keep server no-context negotiation.
        let options = Options::default()
            .with_compression_level(CompressionLevel::default())
            .server_no_context_takeover();
        WebSocket::connect(url)
            .with_options(options)
            .with_request(request)
            .await
    }

    fn connect_request(&self) -> HttpRequestBuilder {
        let mut request = HttpRequestBuilder::new();
        for (name, value) in &self.connect_options.headers {
            request = request.header(name.as_str(), value.as_str());
        }
        request
    }

    async fn connect_with_retry(
        &self,
        url: Url,
        attempts: usize,
        attempt_timeout: Duration,
        retry_delay: Duration,
    ) -> Result<TcpWebSocket> {
        let attempts = attempts.max(1);
        let endpoint = websocket_endpoint_label(&url);
        let mut last_error = String::new();
        for attempt in 1..=attempts {
            match tokio::time::timeout(
                attempt_timeout,
                Self::connect_with_request(url.clone(), self.connect_request()),
            )
            .await
            {
                Ok(Ok(socket)) => return Ok(socket),
                Ok(Err(error)) => last_error = error.to_string(),
                Err(_) => {
                    last_error = format!("attempt timed out after {attempt_timeout:?}");
                }
            }
            if attempt < attempts {
                tokio::time::sleep(retry_delay).await;
            }
        }

        Err(ContractError::transport(format!(
            "websocket connect failed: after {attempts} attempts to {endpoint}; last error: {last_error}"
        )))
    }

    async fn connect_async(&mut self) -> Result<()> {
        require_tokio_runtime()?;
        let url = Url::parse(&self.url)
            .map_err(|err| ContractError::validation(format!("invalid websocket url: {err}")))?;
        let socket = self
            .connect_with_retry(
                url,
                WEBSOCKET_CONNECT_ATTEMPTS,
                WEBSOCKET_CONNECT_ATTEMPT_TIMEOUT,
                WEBSOCKET_CONNECT_RETRY_DELAY,
            )
            .await?;
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

fn websocket_endpoint_label(url: &Url) -> String {
    let host = url.host_str().unwrap_or("<unknown>");
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    match url.port_or_known_default() {
        Some(port) => format!("{host}:{port}"),
        None => host,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::net::TcpListener;
    use url::Url;

    use super::{WebSocketTransport, websocket_endpoint_label};

    #[test]
    fn endpoint_label_omits_sensitive_url_components() {
        let url = Url::parse("wss://user:secret@example.com:8443/private?token=sensitive")
            .expect("valid websocket URL");

        assert_eq!(websocket_endpoint_label(&url), "example.com:8443");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connect_retry_bounds_blackholed_attempts() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind blackhole listener");
        let address = listener.local_addr().expect("blackhole listener address");
        let accepted = Arc::new(AtomicUsize::new(0));
        let server_accepted = Arc::clone(&accepted);
        let server = tokio::spawn(async move {
            let mut sockets = Vec::new();
            for _ in 0..3 {
                let (socket, _) = listener.accept().await.expect("accept retry attempt");
                server_accepted.fetch_add(1, Ordering::AcqRel);
                sockets.push(socket);
            }
            std::future::pending::<()>().await;
        });

        let transport = WebSocketTransport::new(format!("ws://{address}"));
        let result = transport
            .connect_with_retry(
                Url::parse(&format!("ws://{address}")).expect("valid websocket URL"),
                3,
                Duration::from_millis(25),
                Duration::from_millis(1),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("blackholed handshakes must exhaust the retry budget"),
            Err(error) => error,
        };

        assert_eq!(accepted.load(Ordering::Acquire), 3);
        assert!(
            error
                .to_string()
                .contains("websocket connect failed: after 3 attempts"),
            "{error}"
        );
        assert!(error.to_string().contains(&address.to_string()), "{error}");
        server.abort();
    }
}
