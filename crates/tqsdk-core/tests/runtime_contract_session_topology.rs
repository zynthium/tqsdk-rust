use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use tqsdk_core::{
    AccountId, AdapterRegistry, AuthContext, AuthId, AuthProvider, BootstrapResult, ContractFuture,
    EndpointConfig, MarketSessionTarget, PasswordCredentials, ProtocolDomain, ReplaySessionId,
    SessionBootstrap, SessionConfig, SessionRoute, SessionRouteEndpoint, SessionTarget,
    SessionTopology, SessionTopologyResolver, TqAuthProvider, TradeSessionTarget,
    WebSocketConnectOptions,
};

struct TestAuthProvider;

impl AuthProvider for TestAuthProvider {
    fn authenticate(&self) -> ContractFuture<'_, AuthContext> {
        Box::pin(async {
            Ok(AuthContext::new("test-access-token")
                .with_auth_id(AuthId::new("auth-1"))
                .with_feature("futr"))
        })
    }
}

struct TestTopologyResolver;

impl SessionTopologyResolver for TestTopologyResolver {
    fn resolve_topology<'a>(
        &'a self,
        auth: &'a AuthContext,
        _config: &'a SessionConfig,
        enabled_domains: &'a [ProtocolDomain],
    ) -> ContractFuture<'a, SessionTopology> {
        let enabled_domains = enabled_domains.to_vec();
        let auth_id = auth.auth_id().map(|id| id.as_str().to_string());

        Box::pin(async move {
            assert_eq!(auth_id.as_deref(), Some("auth-1"));
            assert_eq!(
                enabled_domains,
                vec![
                    ProtocolDomain::Market,
                    ProtocolDomain::Trade,
                    ProtocolDomain::Schema
                ]
            );

            Ok(SessionTopology::default().with_route(SessionRoute {
                label: "market".to_string(),
                target: SessionTarget::Shared,
                domains: vec![ProtocolDomain::Market, ProtocolDomain::Schema],
                endpoint: SessionRouteEndpoint::WebSocket {
                    url: "wss://md.example/live".to_string(),
                    connect: WebSocketConnectOptions::default()
                        .with_header("Authorization", "Bearer test-access-token"),
                },
            }))
        })
    }
}

#[test]
fn session_bootstrap_establish_with_resolver_returns_topology() {
    let auth = TestAuthProvider;
    let resolver = TestTopologyResolver;
    let mut registry = AdapterRegistry::new();
    registry.register_domain(ProtocolDomain::Market);
    registry.register_domain(ProtocolDomain::Trade);
    registry.register_domain(ProtocolDomain::Schema);

    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .with_market_target(MarketSessionTarget::futures_live())
        .add_trade_target(TradeSessionTarget::new("9999", AccountId::new("simnow")))
        .enable_domain(ProtocolDomain::Market)
        .enable_domain(ProtocolDomain::Trade)
        .enable_domain(ProtocolDomain::Schema);

    let result: BootstrapResult = block_on(
        SessionBootstrap::new().establish_with_resolver(&auth, &resolver, &config, &registry),
    )
    .unwrap();

    assert_eq!(result.phase.as_str(), "running");
    assert_eq!(
        result.enabled_domains,
        vec![
            ProtocolDomain::Market,
            ProtocolDomain::Trade,
            ProtocolDomain::Schema
        ]
    );
    assert_eq!(result.topology.routes.len(), 1);
}

#[test]
fn tq_auth_provider_resolves_shared_market_and_account_trade_routes() {
    run_on_tokio(async {
        let md_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let md_addr = md_listener.local_addr().unwrap();
        let td_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let td_addr = td_listener.local_addr().unwrap();
        let ns_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let ns_addr = ns_listener.local_addr().unwrap();
        let broker_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let broker_addr = broker_listener.local_addr().unwrap();

        let ns_server = std::thread::spawn(move || {
            let (mut stream, _) = ns_listener.accept().unwrap();
            let request = read_request(&mut stream);
            let normalized = request.to_ascii_lowercase();

            assert!(request.starts_with("GET /ns?stock=false&backtest=true HTTP/1.1"));
            assert!(
                normalized.contains("authorization: bearer test-access-token"),
                "{request}"
            );
            write_http_ok(&mut stream, &format!(r#"{{"mdurl":"ws://{md_addr}/md"}}"#));
        });

        let broker_server = std::thread::spawn(move || {
            let (mut stream, _) = broker_listener.accept().unwrap();
            let request = read_request(&mut stream);
            let normalized = request.to_ascii_lowercase();

            assert!(request.starts_with("GET /9999.json?account_id=simnow&auth=demo HTTP/1.1"));
            assert!(
                normalized.contains("authorization: bearer test-access-token"),
                "{request}"
            );
            write_http_ok(
                &mut stream,
                &format!(
                    r#"{{"9999":{{"category":["TQ","FUTURE"],"url":"ws://{td_addr}/trade","broker_type":"FUTURE"}}}}"#
                ),
            );
        });

        let provider = TqAuthProvider::new(PasswordCredentials::new("demo", "secret"))
            .with_name_service_url(format!("http://{ns_addr}/ns"))
            .with_broker_base_url(format!("http://{broker_addr}"));
        let config = SessionConfig::new(
            EndpointConfig::new("https://auth.example")
                .with_query_url("https://query.example/graphql")
                .with_schema_url("https://schema.example"),
        )
        .with_market_target(MarketSessionTarget::futures_backtest())
        .add_trade_target(TradeSessionTarget::new("9999", AccountId::new("simnow")))
        .enable_domain(ProtocolDomain::System)
        .enable_domain(ProtocolDomain::Market)
        .enable_domain(ProtocolDomain::Query)
        .enable_domain(ProtocolDomain::Schema)
        .enable_domain(ProtocolDomain::Trade);
        let auth = AuthContext::new("test-access-token").with_auth_id(AuthId::new("auth-1"));

        let topology = provider
            .resolve_topology(
                &auth,
                &config,
                &[
                    ProtocolDomain::System,
                    ProtocolDomain::Market,
                    ProtocolDomain::Query,
                    ProtocolDomain::Schema,
                    ProtocolDomain::Trade,
                ],
            )
            .await
            .unwrap();

        assert_eq!(topology.routes.len(), 5);

        match &topology.routes[0].endpoint {
            SessionRouteEndpoint::WebSocket { url, connect } => {
                assert_eq!(url, &format!("ws://{md_addr}/md"));
                assert!(connect.headers.iter().any(|(name, value)| {
                    name == "Authorization" && value == "Bearer test-access-token"
                }));
                assert!(
                    connect
                        .headers
                        .iter()
                        .any(|(name, value)| { name == "Accept" && value == "application/json" })
                );
                assert!(connect.headers.iter().any(|(name, value)| {
                    name == "User-Agent" && value == "tqsdk-python 3.8.1"
                }));
            }
            other => panic!("expected market websocket route, got {other:?}"),
        }
        assert_eq!(topology.routes[0].label, "market");
        assert_eq!(topology.routes[0].target, SessionTarget::Shared);
        assert_eq!(topology.routes[0].domains, vec![ProtocolDomain::Market]);

        match &topology.routes[1].endpoint {
            SessionRouteEndpoint::WebSocket { url, connect } => {
                assert_eq!(url, &format!("ws://{td_addr}/trade"));
                assert!(connect.headers.iter().any(|(name, value)| {
                    name == "Authorization" && value == "Bearer test-access-token"
                }));
                assert!(
                    connect
                        .headers
                        .iter()
                        .any(|(name, value)| { name == "Accept" && value == "application/json" })
                );
                assert!(connect.headers.iter().any(|(name, value)| {
                    name == "User-Agent" && value == "tqsdk-python 3.8.1"
                }));
            }
            other => panic!("expected trade websocket route, got {other:?}"),
        }
        assert_eq!(topology.routes[1].label, "trade:simnow");
        assert_eq!(
            topology.routes[1].target,
            SessionTarget::Account(AccountId::new("simnow"))
        );
        assert_eq!(topology.routes[1].domains, vec![ProtocolDomain::Trade]);

        assert_eq!(topology.routes[2].label, "query");
        assert_eq!(topology.routes[2].target, SessionTarget::Shared);
        assert_eq!(topology.routes[2].domains, vec![ProtocolDomain::Query]);
        assert_eq!(
            topology.routes[2].endpoint,
            SessionRouteEndpoint::Http {
                url: "https://query.example/graphql".to_string(),
            }
        );

        assert_eq!(topology.routes[3].label, "schema");
        assert_eq!(topology.routes[3].target, SessionTarget::Shared);
        assert_eq!(topology.routes[3].domains, vec![ProtocolDomain::Schema]);
        assert_eq!(
            topology.routes[3].endpoint,
            SessionRouteEndpoint::Http {
                url: "https://schema.example".to_string(),
            }
        );

        assert_eq!(topology.routes[4].label, "system");
        assert_eq!(topology.routes[4].target, SessionTarget::Shared);
        assert_eq!(topology.routes[4].domains, vec![ProtocolDomain::System]);
        assert_eq!(
            topology.routes[4].endpoint,
            SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            }
        );

        ns_server.join().unwrap();
        broker_server.join().unwrap();
    });
}

#[test]
fn tq_auth_provider_resolves_query_to_websocket_without_explicit_query_url() {
    run_on_tokio(async {
        let md_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let md_addr = md_listener.local_addr().unwrap();
        let ns_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let ns_addr = ns_listener.local_addr().unwrap();

        let ns_server = std::thread::spawn(move || {
            let (mut stream, _) = ns_listener.accept().unwrap();
            let request = read_request(&mut stream);
            let normalized = request.to_ascii_lowercase();

            assert!(request.starts_with("GET /ns?stock=false&backtest=false HTTP/1.1"));
            assert!(
                normalized.contains("authorization: bearer test-access-token"),
                "{request}"
            );
            write_http_ok(&mut stream, &format!(r#"{{"mdurl":"ws://{md_addr}/md"}}"#));
        });

        let provider = TqAuthProvider::new(PasswordCredentials::new("demo", "secret"))
            .with_name_service_url(format!("http://{ns_addr}/ns"));
        let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
            .with_market_target(MarketSessionTarget::futures_live())
            .enable_domain(ProtocolDomain::Query);
        let auth = AuthContext::new("test-access-token").with_auth_id(AuthId::new("auth-1"));

        let topology = provider
            .resolve_topology(&auth, &config, &[ProtocolDomain::Query])
            .await
            .unwrap();

        assert_eq!(topology.routes.len(), 1);
        assert_eq!(topology.routes[0].label, "query");
        assert_eq!(topology.routes[0].target, SessionTarget::Shared);
        assert_eq!(topology.routes[0].domains, vec![ProtocolDomain::Query]);

        match &topology.routes[0].endpoint {
            SessionRouteEndpoint::WebSocket { url, connect } => {
                assert_eq!(url, &format!("ws://{md_addr}/md"));
                assert!(connect.headers.iter().any(|(name, value)| {
                    name == "Authorization" && value == "Bearer test-access-token"
                }));
                assert!(
                    connect
                        .headers
                        .iter()
                        .any(|(name, value)| { name == "Accept" && value == "application/json" })
                );
                assert!(connect.headers.iter().any(|(name, value)| {
                    name == "User-Agent" && value == "tqsdk-python 3.8.1"
                }));
            }
            other => panic!("expected query websocket route, got {other:?}"),
        }

        ns_server.join().unwrap();
    });
}

#[test]
fn tq_auth_provider_merges_query_into_market_websocket_without_explicit_query_url() {
    run_on_tokio(async {
        let md_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let md_addr = md_listener.local_addr().unwrap();
        let ns_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let ns_addr = ns_listener.local_addr().unwrap();

        let ns_server = std::thread::spawn(move || {
            let (mut stream, _) = ns_listener.accept().unwrap();
            let request = read_request(&mut stream);
            let normalized = request.to_ascii_lowercase();

            assert!(request.starts_with("GET /ns?stock=false&backtest=false HTTP/1.1"));
            assert!(
                normalized.contains("authorization: bearer test-access-token"),
                "{request}"
            );
            write_http_ok(&mut stream, &format!(r#"{{"mdurl":"ws://{md_addr}/md"}}"#));
        });

        let provider = TqAuthProvider::new(PasswordCredentials::new("demo", "secret"))
            .with_name_service_url(format!("http://{ns_addr}/ns"));
        let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
            .with_market_target(MarketSessionTarget::futures_live())
            .enable_domain(ProtocolDomain::Market)
            .enable_domain(ProtocolDomain::Query);
        let auth = AuthContext::new("test-access-token").with_auth_id(AuthId::new("auth-1"));

        let topology = provider
            .resolve_topology(
                &auth,
                &config,
                &[ProtocolDomain::Market, ProtocolDomain::Query],
            )
            .await
            .unwrap();

        assert_eq!(topology.routes.len(), 1);
        assert_eq!(topology.routes[0].label, "market");
        assert_eq!(topology.routes[0].target, SessionTarget::Shared);
        assert_eq!(
            topology.routes[0].domains,
            vec![ProtocolDomain::Market, ProtocolDomain::Query]
        );

        match &topology.routes[0].endpoint {
            SessionRouteEndpoint::WebSocket { url, connect } => {
                assert_eq!(url, &format!("ws://{md_addr}/md"));
                assert!(connect.headers.iter().any(|(name, value)| {
                    name == "Authorization" && value == "Bearer test-access-token"
                }));
                assert!(
                    connect
                        .headers
                        .iter()
                        .any(|(name, value)| { name == "Accept" && value == "application/json" })
                );
                assert!(connect.headers.iter().any(|(name, value)| {
                    name == "User-Agent" && value == "tqsdk-python 3.8.1"
                }));
            }
            other => panic!("expected merged market/query websocket route, got {other:?}"),
        }

        ns_server.join().unwrap();
    });
}

#[test]
fn tq_auth_provider_resolves_replay_and_system_routes_from_explicit_endpoints() {
    let provider = TqAuthProvider::new(PasswordCredentials::new("demo", "secret"));
    let config = SessionConfig::new(
        EndpointConfig::new("https://auth.example").with_replay_url("replay-driver"),
    )
    .enable_domain(ProtocolDomain::Replay)
    .enable_domain(ProtocolDomain::System);
    let auth = AuthContext::new("test-access-token").with_auth_id(AuthId::new("auth-1"));

    let topology = block_on(provider.resolve_topology(
        &auth,
        &config,
        &[ProtocolDomain::Replay, ProtocolDomain::System],
    ))
    .unwrap();

    assert_eq!(topology.routes.len(), 2);
    assert_eq!(topology.routes[0].label, "replay");
    assert_eq!(
        topology.routes[0].target,
        SessionTarget::Replay(ReplaySessionId::new("replay-driver"))
    );
    assert_eq!(topology.routes[0].domains, vec![ProtocolDomain::Replay]);
    assert_eq!(
        topology.routes[0].endpoint,
        SessionRouteEndpoint::Replay {
            label: "replay-driver".to_string(),
        }
    );

    assert_eq!(topology.routes[1].label, "system");
    assert_eq!(topology.routes[1].target, SessionTarget::Shared);
    assert_eq!(topology.routes[1].domains, vec![ProtocolDomain::System]);
    assert_eq!(
        topology.routes[1].endpoint,
        SessionRouteEndpoint::Internal {
            label: "system-driver".to_string(),
        }
    );
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

fn run_on_tokio<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
