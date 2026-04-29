#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

use tqsdk_core::internal::SessionBootstrap;
use tqsdk_core::{
    AdapterRegistry, AuthContext, AuthId, AuthProvider, BootstrapResult, EndpointConfig,
    HeartbeatPolicy, OutboundFrame, ProtocolDomain, RawFrame, ReconnectPolicy,
    Result as CoreResult, SessionConfig, SessionPhase, Transport,
};

struct TestAuthProvider;

impl AuthProvider for TestAuthProvider {
    fn authenticate(&self) -> impl Future<Output = CoreResult<AuthContext>> + Send + '_ {
        Box::pin(async {
            Ok(AuthContext::new("access-token")
                .with_auth_id(AuthId::new("auth-1"))
                .with_feature("trade"))
        })
    }
}

#[derive(Default)]
struct TestTransport {
    frames: Vec<RawFrame>,
}

impl Transport for TestTransport {
    fn connect(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }

    fn recv(&mut self) -> impl Future<Output = CoreResult<RawFrame>> + Send + '_ {
        let frame = self.frames.pop().unwrap_or(RawFrame::Pong);
        async move { Ok(frame) }
    }

    fn send(&mut self, frame: OutboundFrame) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        if matches!(frame, OutboundFrame::Ping) {
            self.frames.push(RawFrame::Pong);
        }
        async { Ok(()) }
    }

    fn close(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }
}

#[test]
fn auth_and_session_contracts_cover_login_bootstrap_shells() {
    let auth = TestAuthProvider;
    let mut transport = TestTransport::default();
    let mut registry = AdapterRegistry::new();
    registry.register_domain(ProtocolDomain::System);
    registry.register_domain(ProtocolDomain::Trade);

    let config = SessionConfig::new(
        EndpointConfig::new("https://auth.example")
            .with_market_url("wss://market.example")
            .with_trade_url("wss://trade.example"),
    )
    .with_heartbeat(HeartbeatPolicy::new(
        Duration::from_secs(5),
        Duration::from_secs(20),
    ))
    .with_reconnect(ReconnectPolicy::new(
        Duration::from_secs(1),
        Duration::from_secs(30),
        Some(8),
    ))
    .enable_domain(ProtocolDomain::System)
    .enable_domain(ProtocolDomain::Trade);

    let bootstrap = SessionBootstrap::new();
    let result: BootstrapResult = block_on(bootstrap.establish(&auth, &config, &registry)).unwrap();

    assert_eq!(result.phase, SessionPhase::Running);
    assert_eq!(result.auth.access_token(), "access-token");
    assert_eq!(result.auth.auth_id().map(AuthId::as_str), Some("auth-1"));
    assert_eq!(
        result.enabled_domains,
        vec![ProtocolDomain::System, ProtocolDomain::Trade]
    );
    assert_eq!(
        config.enabled_domains(),
        &[ProtocolDomain::System, ProtocolDomain::Trade]
    );
    assert_eq!(config.heartbeat.interval, Duration::from_secs(5));
    assert_eq!(config.reconnect.max_attempts, Some(8));
    assert_eq!(SessionPhase::Authenticating.as_str(), "authenticating");

    block_on(transport.connect()).unwrap();
    block_on(transport.send(OutboundFrame::Ping)).unwrap();
    assert!(matches!(
        block_on(transport.recv()).unwrap(),
        RawFrame::Pong
    ));
    block_on(transport.close()).unwrap();
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
    // SAFETY: the static null-data waker owns no resources and is only used to
    // poll test futures that are expected to complete synchronously.
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
