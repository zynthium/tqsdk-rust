use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use tqsdk_runtime_contract::{AuthId, AuthProvider, PasswordCredentials, TqAuthProvider};

#[test]
fn tq_auth_provider_authenticates_against_token_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0_u8; 4096];
        let n = stream.read(&mut buf).unwrap();
        let request = String::from_utf8_lossy(&buf[..n]).to_string();

        assert!(
            request
                .starts_with("POST /auth/realms/shinnytech/protocol/openid-connect/token HTTP/1.1")
        );
        assert!(request.contains("grant_type=password"));
        assert!(request.contains("username=demo"));
        assert!(request.contains("password=secret"));
        assert!(request.contains("client_id=shinny_tq"));
        assert!(request.contains("client_secret=be30b9f4-6862-488a-99ad-21bde0400081"));

        let body = token_response_body();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    });

    let provider = TqAuthProvider::new(PasswordCredentials::new("demo", "secret"))
        .with_auth_url(format!("http://{addr}"));
    let context = block_on(provider.authenticate()).unwrap();

    assert_eq!(
        context.access_token(),
        "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyLTEiLCJncmFudHMiOnsiZmVhdHVyZXMiOlsiZnV0ciIsInNlYyJdfX0.sig"
    );
    assert_eq!(context.auth_id().map(AuthId::as_str), Some("user-1"));
    assert_eq!(context.features(), &["futr".to_string(), "sec".to_string()]);

    server.join().unwrap();
}

#[test]
fn tq_auth_provider_resolves_market_url_after_authenticate() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        respond_with_token_request(&listener);

        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        let normalized = request.to_ascii_lowercase();

        assert!(request.starts_with("GET /ns?stock=false&backtest=true HTTP/1.1"));
        assert!(
            normalized.contains(&format!(
                "authorization: bearer {}",
                test_access_token().to_ascii_lowercase()
            )),
            "{request}"
        );

        let body = r#"{"mdurl":"wss://md.example/live"}"#;
        write_http_ok(&mut stream, body);
    });

    let provider = TqAuthProvider::new(PasswordCredentials::new("demo", "secret"))
        .with_auth_url(format!("http://{addr}"))
        .with_name_service_url(format!("http://{addr}/ns"));
    let auth = block_on(provider.authenticate()).unwrap();
    let market_url = block_on(provider.fetch_market_url(&auth, false, true)).unwrap();

    assert_eq!(market_url, "wss://md.example/live");
    server.join().unwrap();
}

#[test]
fn tq_auth_provider_resolves_trade_broker_metadata_after_authenticate() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        respond_with_token_request(&listener);

        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        let normalized = request.to_ascii_lowercase();

        assert!(request.starts_with("GET /BROKER.json?account_id=022631&auth=demo HTTP/1.1"));
        assert!(
            normalized.contains(&format!(
                "authorization: bearer {}",
                test_access_token().to_ascii_lowercase()
            )),
            "{request}"
        );

        let body = r#"{"BROKER":{"category":["TQ","FUTURE"],"url":"wss://td.example/trade","broker_type":"FUTURE","smtype":"sm","smconfig":"cfg","condition_type":"ctp","condition_config":"fast"}}"#;
        write_http_ok(&mut stream, body);
    });

    let provider = TqAuthProvider::new(PasswordCredentials::new("demo", "secret"))
        .with_auth_url(format!("http://{addr}"))
        .with_broker_base_url(format!("http://{addr}"));
    let auth = block_on(provider.authenticate()).unwrap();
    let broker = block_on(provider.fetch_trade_broker(&auth, "BROKER", "022631")).unwrap();

    assert_eq!(broker.url, "wss://td.example/trade");
    assert_eq!(
        broker.category,
        vec!["TQ".to_string(), "FUTURE".to_string()]
    );
    assert_eq!(broker.broker_type.as_deref(), Some("FUTURE"));
    assert_eq!(broker.smtype.as_deref(), Some("sm"));
    assert_eq!(broker.condition_config.as_deref(), Some("fast"));
    server.join().unwrap();
}

fn respond_with_token_request(listener: &TcpListener) {
    let (mut stream, _) = listener.accept().unwrap();
    let request = read_request(&mut stream);

    assert!(
        request.starts_with("POST /auth/realms/shinnytech/protocol/openid-connect/token HTTP/1.1")
    );
    assert!(request.contains("grant_type=password"));
    assert!(request.contains("username=demo"));
    assert!(request.contains("password=secret"));
    assert!(request.contains("client_id=shinny_tq"));
    assert!(request.contains("client_secret=be30b9f4-6862-488a-99ad-21bde0400081"));

    write_http_ok(&mut stream, token_response_body());
}

fn token_response_body() -> &'static str {
    r#"{"access_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyLTEiLCJncmFudHMiOnsiZmVhdHVyZXMiOlsiZnV0ciIsInNlYyJdfX0.sig","refresh_token":"refresh-token"}"#
}

fn test_access_token() -> &'static str {
    "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyLTEiLCJncmFudHMiOnsiZmVhdHVyZXMiOlsiZnV0ciIsInNlYyJdfX0.sig"
}

fn read_request(stream: &mut std::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0, "unexpected EOF while reading HTTP request");
        buffer.extend_from_slice(&chunk[..read]);

        let Some(header_end) = find_header_end(&buffer) else {
            continue;
        };
        let content_length = content_length(&buffer[..header_end]);
        if buffer.len() >= header_end + 4 + content_length {
            return String::from_utf8_lossy(&buffer).to_string();
        }
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &[u8]) -> usize {
    let text = String::from_utf8_lossy(headers).to_ascii_lowercase();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("content-length:") {
            return value.trim().parse().unwrap_or(0);
        }
    }
    0
}

fn write_http_ok(stream: &mut std::net::TcpStream, body: &str) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .unwrap();
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let mut future = Pin::from(Box::new(future));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn noop_waker() -> Waker {
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

fn noop_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
}

unsafe fn noop_clone(_: *const ()) -> RawWaker {
    noop_raw_waker()
}

unsafe fn noop(_: *const ()) {}

static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
