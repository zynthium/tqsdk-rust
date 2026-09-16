use std::io;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use futures::SinkExt;
use percent_encoding::percent_decode_str;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{
    self,
    pki_types::{ServerName, TrustAnchor},
};
use url::Url;
use yawc::frame::{Frame, OpCode};
use yawc::{CompressionLevel, HttpRequestBuilder, MaybeTlsStream, Options, WebSocket};

use crate::commands::OutboundFrame;
use crate::{ContractError, Result};

use super::frame::RawFrame;
use super::io::{Transport, WebSocketConnectOptions};

const WEBSOCKET_CONNECT_ATTEMPTS: usize = 3;
const WEBSOCKET_CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);
const WEBSOCKET_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(250);
const PROXY_RESPONSE_LIMIT: usize = 16 * 1024;

type ProxyWebSocket = WebSocket<MaybeTlsStream<Box<dyn AsyncStream>>>;

trait AsyncStream: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

#[derive(Debug)]
enum WebSocketConnectError {
    HttpStatus(u16),
    Transport(yawc::WebSocketError),
}

impl From<yawc::WebSocketError> for WebSocketConnectError {
    fn from(error: yawc::WebSocketError) -> Self {
        Self::Transport(error)
    }
}

impl From<io::Error> for WebSocketConnectError {
    fn from(error: io::Error) -> Self {
        Self::Transport(yawc::WebSocketError::IoError(error))
    }
}

/// Thin websocket transport built on `yawc`.
///
/// The transport requires an ambient Tokio runtime. Initial socket/TLS
/// establishment uses a small bounded retry budget so a transient blackholed
/// route does not fail the whole session bootstrap. Route selection,
/// established-session reconnect policy, heartbeat semantics, and state
/// projection remain the responsibility of higher contract layers.
pub struct WebSocketTransport {
    connect_attempts: Option<std::num::NonZeroUsize>,
    url: String,
    connect_options: WebSocketConnectOptions,
    socket: Option<ProxyWebSocket>,
}

impl WebSocketTransport {
    /// Overrides initial socket attempts without changing reconnect policy.
    pub fn with_connect_attempts(mut self, attempts: std::num::NonZeroUsize) -> Self {
        self.connect_attempts = Some(attempts);
        self
    }

    /// Creates a websocket transport for the provided route URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            connect_attempts: None,
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
    ) -> std::result::Result<ProxyWebSocket, WebSocketConnectError> {
        // Tianqin replies to a bare deflate offer with a quoted window-bits parameter
        // that yawc 0.3.3 does not parse, so keep server no-context negotiation.
        let options = Options::default()
            .with_compression_level(CompressionLevel::default())
            .server_no_context_takeover();
        let stream = connect_websocket_stream(&url).await?;
        WebSocket::handshake_with_request(url, stream, options, request)
            .await
            .map_err(Into::into)
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
    ) -> Result<ProxyWebSocket> {
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
                Ok(Err(WebSocketConnectError::HttpStatus(status)))
                | Ok(Err(WebSocketConnectError::Transport(
                    yawc::WebSocketError::InvalidStatusCode(status),
                ))) => {
                    return Err(ContractError::HttpStatus {
                        status,
                        retry_after_secs: None,
                    });
                }
                Ok(Err(WebSocketConnectError::Transport(error))) => last_error = error.to_string(),
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
                self.connect_attempts
                    .map_or(WEBSOCKET_CONNECT_ATTEMPTS, std::num::NonZeroUsize::get),
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

async fn connect_websocket_stream(
    url: &Url,
) -> std::result::Result<MaybeTlsStream<Box<dyn AsyncStream>>, WebSocketConnectError> {
    let host = url.host_str().ok_or_else(|| {
        WebSocketConnectError::Transport(yawc::WebSocketError::IoError(io::Error::new(
            io::ErrorKind::InvalidInput,
            "websocket URL has no host",
        )))
    })?;
    let port = url.port_or_known_default().ok_or_else(|| {
        WebSocketConnectError::Transport(yawc::WebSocketError::IoError(io::Error::new(
            io::ErrorKind::InvalidInput,
            "websocket URL has no port",
        )))
    })?;
    let authority = format_authority(host, port);
    let stream = if let Some(proxy) = proxy_url(url)? {
        connect_http_proxy(&proxy, &authority).await?
    } else {
        let stream = TcpStream::connect(authority).await?;
        stream.set_nodelay(true)?;
        Box::new(stream) as Box<dyn AsyncStream>
    };
    match url.scheme() {
        "ws" => Ok(MaybeTlsStream::Plain(stream)),
        "wss" => {
            let server_name = ServerName::try_from(host.to_owned()).map_err(|_| {
                WebSocketConnectError::Transport(yawc::WebSocketError::IoError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid websocket TLS hostname",
                )))
            })?;
            Ok(MaybeTlsStream::Tls(
                tls_connector().connect(server_name, stream).await?,
            ))
        }
        _ => Err(WebSocketConnectError::Transport(
            yawc::WebSocketError::InvalidHttpScheme,
        )),
    }
}

fn proxy_url(url: &Url) -> std::result::Result<Option<Url>, WebSocketConnectError> {
    if force_no_proxy() || no_proxy_matches(url) {
        return Ok(None);
    }
    let names = match url.scheme() {
        "ws" => ["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"],
        "wss" => ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"],
        _ => return Ok(None),
    };
    let Some(value) = names.into_iter().find_map(environment_value) else {
        return Ok(None);
    };
    let proxy = Url::parse(&value).map_err(|_| {
        WebSocketConnectError::Transport(yawc::WebSocketError::IoError(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid WebSocket proxy URL",
        )))
    })?;
    if proxy.scheme() != "http" || proxy.host_str().is_none() {
        return Err(WebSocketConnectError::Transport(
            yawc::WebSocketError::IoError(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WebSocket proxy must be an http://host URL",
            )),
        ));
    }
    Ok(Some(proxy))
}

fn force_no_proxy() -> bool {
    std::env::var_os("TQSDK_HTTP_NO_PROXY").as_deref() == Some(std::ffi::OsStr::new("1"))
}

fn environment_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn no_proxy_matches(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let value = std::env::var("NO_PROXY")
        .or_else(|_| std::env::var("no_proxy"))
        .ok();
    value.is_some_and(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .any(|entry| no_proxy_entry_matches(host, entry))
    })
}

fn no_proxy_entry_matches(host: &str, entry: &str) -> bool {
    if entry == "*" {
        return true;
    }
    if let Some((network, bits)) = entry.rsplit_once('/')
        && let Ok(bits) = bits.parse::<u8>()
    {
        return cidr_matches(host, network, bits);
    }
    if let Ok(address) = entry.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
        return host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|host| host == address);
    }
    let entry = if entry.starts_with('[') {
        entry.find(']').map_or(entry, |end| &entry[1..end])
    } else {
        entry
    };
    let entry = entry.strip_prefix('.').unwrap_or(entry);
    let entry = entry
        .rsplit_once(':')
        .filter(|(_, port)| port.parse::<u16>().is_ok())
        .map_or(entry, |(host, _)| host);
    host.eq_ignore_ascii_case(entry)
        || host
            .strip_suffix(entry)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn cidr_matches(host: &str, network: &str, bits: u8) -> bool {
    match (
        host.parse::<std::net::IpAddr>(),
        network.parse::<std::net::IpAddr>(),
    ) {
        (Ok(std::net::IpAddr::V4(host)), Ok(std::net::IpAddr::V4(network))) if bits <= 32 => {
            let mask = u32::MAX.checked_shl(32 - u32::from(bits)).unwrap_or(0);
            u32::from(host) & mask == u32::from(network) & mask
        }
        (Ok(std::net::IpAddr::V6(host)), Ok(std::net::IpAddr::V6(network))) if bits <= 128 => {
            let mask = u128::MAX.checked_shl(128 - u32::from(bits)).unwrap_or(0);
            u128::from(host) & mask == u128::from(network) & mask
        }
        _ => false,
    }
}

async fn connect_http_proxy(
    proxy: &Url,
    authority: &str,
) -> std::result::Result<Box<dyn AsyncStream>, WebSocketConnectError> {
    let host = proxy.host_str().expect("proxy host validated");
    let port = proxy
        .port_or_known_default()
        .expect("proxy URL has a default port");
    let mut stream = TcpStream::connect(format_authority(host, port)).await?;
    stream.set_nodelay(true)?;
    let mut request = format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: Keep-Alive\r\n"
    );
    if !proxy.username().is_empty() || proxy.password().is_some() {
        let username = percent_decode_str(proxy.username()).decode_utf8_lossy();
        let password = percent_decode_str(proxy.password().unwrap_or_default()).decode_utf8_lossy();
        let credentials = format!("{username}:{password}");
        let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
        request.push_str(&format!("Proxy-Authorization: Basic {encoded}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;
    let response = read_proxy_response(&mut stream).await?;
    let status = response
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok());
    if let Some(status) = status.filter(|status| *status != 200) {
        return Err(WebSocketConnectError::HttpStatus(status));
    }
    if status.is_none() {
        return Err(WebSocketConnectError::Transport(
            yawc::WebSocketError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid WebSocket proxy response",
            )),
        ));
    }
    Ok(Box::new(stream))
}

async fn read_proxy_response(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut response = Vec::with_capacity(512);
    while response.len() < PROXY_RESPONSE_LIMIT {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await?;
        response.push(byte[0]);
        if response.ends_with(b"\r\n\r\n") {
            return Ok(response);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "WebSocket proxy response headers exceed limit",
    ))
}

fn format_authority(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn tls_connector() -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    roots.add_parsable_certificates(native.certs);
    roots.extend(
        webpki_roots::TLS_SERVER_ROOTS
            .iter()
            .map(|anchor| TrustAnchor {
                subject: anchor.subject.clone(),
                subject_public_key_info: anchor.subject_public_key_info.clone(),
                name_constraints: anchor.name_constraints.clone(),
            }),
    );
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .expect("Rustls supported versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec!["http/1.1".into()];
    TlsConnector::from(Arc::new(config))
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
    #[tokio::test]
    async fn handshake_status_is_typed_and_never_retried_inside_transport() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for status in [401, 403, 429, 503] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let accepted = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&accepted);
            let server = tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    count.fetch_add(1, Ordering::SeqCst);
                    let mut buffer = [0; 4096];
                    let _ = socket.read(&mut buffer).await;
                    socket.write_all(format!("HTTP/1.1 {status} Refused\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                }
            });
            let transport = WebSocketTransport::new(format!("ws://{address}"));
            let error = transport
                .connect_with_retry(
                    Url::parse(&format!("ws://{address}")).unwrap(),
                    3,
                    Duration::from_secs(1),
                    Duration::from_millis(1),
                )
                .await
                .err()
                .expect("handshake must be rejected");
            assert!(
                matches!(error, crate::ContractError::HttpStatus { status: found, .. } if found == status)
            );
            assert_eq!(accepted.load(Ordering::SeqCst), 1);
            server.abort();
        }
    }

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::net::TcpListener;
    use url::Url;

    use super::{
        WebSocketConnectError, WebSocketTransport, connect_http_proxy, format_authority,
        no_proxy_entry_matches, websocket_endpoint_label,
    };

    #[test]
    fn endpoint_label_omits_sensitive_url_components() {
        let url = Url::parse("wss://user:secret@example.com:8443/private?token=sensitive")
            .expect("valid websocket URL");

        assert_eq!(websocket_endpoint_label(&url), "example.com:8443");
    }

    #[test]
    fn no_proxy_matches_exact_hosts_and_domain_suffixes() {
        assert!(no_proxy_entry_matches("localhost", "localhost"));
        assert!(no_proxy_entry_matches("api.example.com", ".example.com"));
        assert!(!no_proxy_entry_matches("notexample.com", "example.com"));
        assert!(no_proxy_entry_matches("example.com", "*"));
        assert!(no_proxy_entry_matches("127.0.0.1", "127.0.0.0/8"));
        assert!(no_proxy_entry_matches("::1", "::1"));
        assert!(no_proxy_entry_matches("2001:db8::1", "2001:db8::/32"));
    }

    #[tokio::test]
    async fn http_proxy_uses_connect_tunnel() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                stream.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        let proxy = Url::parse(&format!("http://{address}")).unwrap();
        let stream = connect_http_proxy(&proxy, &format_authority("example.invalid", 443))
            .await
            .unwrap();
        drop(stream);
        assert!(
            server
                .await
                .unwrap()
                .starts_with("CONNECT example.invalid:443 HTTP/1.1\r\n")
        );
    }

    #[tokio::test]
    async fn proxy_status_is_typed_without_a_transport_retry() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                stream.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                .await
                .unwrap();
        });
        let proxy = Url::parse(&format!("http://{address}")).unwrap();
        let error = match connect_http_proxy(&proxy, "example.invalid:443").await {
            Ok(_) => panic!("proxy status must fail"),
            Err(error) => error,
        };
        assert!(matches!(error, WebSocketConnectError::HttpStatus(407)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_userinfo_is_percent_decoded_before_basic_auth() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                stream.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        let proxy = Url::parse(&format!("http://user%40name:p%3Aword@{address}")).unwrap();
        let stream = connect_http_proxy(&proxy, "example.invalid:443")
            .await
            .unwrap();
        drop(stream);
        assert!(
            server
                .await
                .unwrap()
                .contains("Proxy-Authorization: Basic dXNlckBuYW1lOnA6d29yZA==\r\n")
        );
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
