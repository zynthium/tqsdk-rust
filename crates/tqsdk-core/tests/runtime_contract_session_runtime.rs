#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_core::session_runtime::SessionRuntime;
use tqsdk_core::transport::{DynTransport, SessionBootstrap};
use tqsdk_core::{
    AdapterRegistry, AuthContext, AuthId, AuthProvider, CommitScope, ContractError, EndpointConfig,
    ProtocolDomain, RawFrame, Result as CoreResult, Runtime, RuntimeHandle, SessionConfig,
    SessionPhase, SessionRoute, SessionRouteConnector, SessionRouteEndpoint, SessionTarget,
    SessionTopology, SessionTopologyResolver, StatePath, Transport,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = CoreResult<T>> + Send + 'a>>;

struct TestAuthProvider;

impl AuthProvider for TestAuthProvider {
    fn authenticate(&self) -> impl Future<Output = CoreResult<AuthContext>> + Send + '_ {
        Box::pin(async {
            Ok(AuthContext::new("test-token")
                .with_auth_id(AuthId::new("auth-1"))
                .with_feature("trade"))
        })
    }
}

struct FailingAuthProvider;

impl AuthProvider for FailingAuthProvider {
    fn authenticate(&self) -> impl Future<Output = CoreResult<AuthContext>> + Send + '_ {
        Box::pin(async { Err(ContractError::auth("token expired")) })
    }
}

struct TestTopologyResolver;

impl SessionTopologyResolver for TestTopologyResolver {
    fn resolve_topology<'a>(
        &'a self,
        auth: &'a AuthContext,
        _config: &'a SessionConfig,
        enabled_domains: &'a [ProtocolDomain],
    ) -> BoxFuture<'a, SessionTopology> {
        Box::pin(async move {
            assert_eq!(auth.auth_id().map(AuthId::as_str), Some("auth-1"));
            assert_eq!(
                enabled_domains,
                &[ProtocolDomain::System, ProtocolDomain::Trade]
            );

            Ok(SessionTopology::default().with_route(SessionRoute {
                label: "trade:simnow".to_string(),
                target: SessionTarget::Shared,
                domains: vec![ProtocolDomain::System, ProtocolDomain::Trade],
                endpoint: SessionRouteEndpoint::Internal {
                    label: "trade-router".to_string(),
                },
            }))
        })
    }
}

#[derive(Default)]
struct TestTransport;

impl Transport for TestTransport {
    fn connect(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }

    fn recv(&mut self) -> impl Future<Output = CoreResult<RawFrame>> + Send + '_ {
        async { Ok(RawFrame::Pong) }
    }

    fn send(
        &mut self,
        _frame: tqsdk_core::OutboundFrame,
    ) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }

    fn close(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }
}

#[derive(Default)]
struct TestRouteConnector {
    connected_labels: Arc<Mutex<Vec<String>>>,
}

impl SessionRouteConnector for TestRouteConnector {
    fn connect_route<'a>(
        &'a self,
        route: &'a SessionRoute,
    ) -> BoxFuture<'a, Box<dyn DynTransport>> {
        let connected_labels = Arc::clone(&self.connected_labels);
        let label = route.label.clone();
        Box::pin(async move {
            connected_labels.lock().unwrap().push(label);
            Ok(Box::new(TestTransport) as Box<dyn DynTransport>)
        })
    }
}

struct FailingRouteConnector;

impl SessionRouteConnector for FailingRouteConnector {
    fn connect_route<'a>(
        &'a self,
        _route: &'a SessionRoute,
    ) -> BoxFuture<'a, Box<dyn DynTransport>> {
        Box::pin(async { Err(ContractError::auth("route connect refused")) })
    }
}

#[test]
fn session_runtime_orchestrator_methods_do_not_box_futures() {
    let source = include_str!("../src/session_runtime.rs");
    let orchestrator_impl = source
        .split("impl SessionRuntime {")
        .nth(1)
        .and_then(|tail| tail.split("fn timer_route_label").next())
        .expect("SessionRuntime orchestrator impl block should be present");

    assert!(
        !orchestrator_impl.contains("Box::pin"),
        "SessionRuntime orchestration methods should use native async futures"
    );
}

#[test]
fn session_runtime_establishes_topology_and_records_initial_ready_flow() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = TestRouteConnector::default();
    let config = session_config();

    let established = block_on(runtime.establish(
        &TestAuthProvider,
        &TestTopologyResolver,
        &connector,
        &config,
        &adapter_registry(),
    ))
    .unwrap();

    assert_eq!(established.bootstrap.phase, SessionPhase::Running);
    assert_eq!(established.connected.routes.len(), 1);
    assert_eq!(
        connector.connected_labels.lock().unwrap().as_slice(),
        &["trade:simnow".to_string()]
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("running"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "auth", "context", "auth_id"]),
        Some(&json!("auth-1"))
    );

    let mut cursor = handle.cursor_from(tqsdk_core::Revision::new(1));
    let first = log.next(&mut cursor).unwrap();
    let second = log.next(&mut cursor).unwrap();
    let third = log.next(&mut cursor).unwrap();
    let fourth = log.next(&mut cursor).unwrap();

    assert_eq!(first.scope, CommitScope::SessionTransition);
    assert_eq!(
        first.changes.path_hits,
        vec![StatePath::new(["system", "session", "lifecycle"])]
    );
    assert_eq!(second.scope, CommitScope::SessionTransition);
    assert_eq!(third.scope, CommitScope::SessionTransition);
    assert_eq!(fourth.scope, CommitScope::InitialReady);
    assert_eq!(log.next(&mut cursor), None);
}

#[test]
fn session_runtime_recovery_uses_resync_recovery_commit() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = TestRouteConnector::default();
    let config = session_config();

    let recovered = block_on(runtime.recover(
        &TestAuthProvider,
        &TestTopologyResolver,
        &connector,
        &config,
        &adapter_registry(),
    ))
    .unwrap();

    assert_eq!(recovered.bootstrap.phase, SessionPhase::Running);
    assert_eq!(recovered.connected.routes.len(), 1);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("running"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "topology", "routes"]),
        Some(&json!([{
            "label": "trade:simnow",
            "target": {"kind": "shared"},
            "domains": ["system", "trade"],
            "endpoint": {"kind": "internal", "label": "trade-router"},
        }]))
    );

    let mut cursor = handle.cursor_from(tqsdk_core::Revision::new(1));
    let first = log.next(&mut cursor).unwrap();
    let second = log.next(&mut cursor).unwrap();
    let third = log.next(&mut cursor).unwrap();

    assert_eq!(first.scope, CommitScope::SessionTransition);
    assert_eq!(second.scope, CommitScope::SessionTransition);
    assert_eq!(third.scope, CommitScope::ResyncRecovery);
    assert_eq!(log.next(&mut cursor), None);
}

#[test]
fn session_runtime_establish_commits_bootstrap_failures_under_system_state() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = TestRouteConnector::default();
    let config = session_config();

    let err = match block_on(runtime.establish(
        &FailingAuthProvider,
        &TestTopologyResolver,
        &connector,
        &config,
        &adapter_registry(),
    )) {
        Ok(_) => panic!("establish unexpectedly succeeded"),
        Err(err) => err,
    };

    assert_eq!(err.to_string(), "auth error: token expired");
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "session-establish-error", "stage"]),
        Some(&json!("bootstrap"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "session-establish-error", "message"]),
        Some(&json!("auth error: token expired"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("closed"))
    );
}

#[test]
fn session_runtime_establish_commits_connect_failures_under_system_state() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let config = session_config();

    let err = match block_on(runtime.establish(
        &TestAuthProvider,
        &TestTopologyResolver,
        &FailingRouteConnector,
        &config,
        &adapter_registry(),
    )) {
        Ok(_) => panic!("establish unexpectedly succeeded"),
        Err(err) => err,
    };

    assert_eq!(err.to_string(), "auth error: route connect refused");
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "session-establish-error", "stage"]),
        Some(&json!("connect"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "session-establish-error", "message"]),
        Some(&json!("auth error: route connect refused"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("closed"))
    );
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    RuntimeHandle::with_adapters(adapter_registry())
}

fn adapter_registry() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    registry
}

fn session_config() -> SessionConfig {
    SessionConfig::new(
        EndpointConfig::new("https://auth.example")
            .with_trade_url("wss://trade.example")
            .with_market_url("wss://market.example"),
    )
    .enable_domain(ProtocolDomain::System)
    .enable_domain(ProtocolDomain::Trade)
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
