use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, AuthContext, AuthProvider, CommitScope, ContractFuture, EndpointConfig,
    OutboundFrame, ProtocolDomain, RawFrame, ReconnectPolicy, Runtime, RuntimeHandle,
    SessionBootstrap, SessionConfig, SessionPhase, SessionRoute, SessionRouteConnector,
    SessionRouteEndpoint, SessionRuntime, SessionRuntimeDeps, SessionTarget, SessionTopology,
    SessionTopologyResolver, StatePath, TimerEvent, Transport,
};

#[derive(Clone)]
enum RecvBehavior {
    Frame(RawFrame),
}

#[derive(Clone)]
struct HeartbeatTransport {
    behavior: RecvBehavior,
    sent_frames: Arc<Mutex<Vec<OutboundFrame>>>,
}

impl Transport for HeartbeatTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        let behavior = self.behavior.clone();
        Box::pin(async move {
            match behavior {
                RecvBehavior::Frame(frame) => Ok(frame),
            }
        })
    }

    fn send(&mut self, frame: OutboundFrame) -> ContractFuture<'_, ()> {
        self.sent_frames.lock().unwrap().push(frame);
        Box::pin(async { Ok(()) })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct HeartbeatConnector {
    behaviors: Arc<Mutex<VecDeque<RecvBehavior>>>,
    sent_frames: Arc<Mutex<Vec<OutboundFrame>>>,
    connected_labels: Arc<Mutex<Vec<String>>>,
}

impl HeartbeatConnector {
    fn new(behaviors: Vec<RecvBehavior>) -> Self {
        Self {
            behaviors: Arc::new(Mutex::new(behaviors.into())),
            sent_frames: Arc::new(Mutex::new(Vec::new())),
            connected_labels: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn sent_frames(&self) -> Vec<OutboundFrame> {
        self.sent_frames.lock().unwrap().clone()
    }

    fn connected_labels(&self) -> Vec<String> {
        self.connected_labels.lock().unwrap().clone()
    }
}

impl SessionRouteConnector for HeartbeatConnector {
    fn connect_route<'a>(
        &'a self,
        route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>> {
        let behaviors = Arc::clone(&self.behaviors);
        let sent_frames = Arc::clone(&self.sent_frames);
        let connected_labels = Arc::clone(&self.connected_labels);
        let label = route.label.clone();
        Box::pin(async move {
            connected_labels.lock().unwrap().push(label);
            let behavior = behaviors
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(RecvBehavior::Frame(RawFrame::Pong));
            Ok(Box::new(HeartbeatTransport {
                behavior,
                sent_frames,
            }) as Box<dyn Transport>)
        })
    }
}

struct TestAuthProvider;

impl AuthProvider for TestAuthProvider {
    fn authenticate(&self) -> ContractFuture<'_, AuthContext> {
        Box::pin(async { Ok(AuthContext::new("test-token")) })
    }
}

struct MarketTopologyResolver;

impl SessionTopologyResolver for MarketTopologyResolver {
    fn resolve_topology<'a>(
        &'a self,
        _auth: &'a AuthContext,
        _config: &'a SessionConfig,
        enabled_domains: &'a [ProtocolDomain],
    ) -> ContractFuture<'a, SessionTopology> {
        Box::pin(async move {
            assert_eq!(enabled_domains, &[ProtocolDomain::Market]);
            Ok(market_topology())
        })
    }
}

#[test]
fn session_runtime_pump_route_once_commits_transport_pong_frames() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = HeartbeatConnector::new(vec![RecvBehavior::Frame(RawFrame::Pong)]);
    let adapters = adapter_registry();
    let config = session_config();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &MarketTopologyResolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let outcome =
        block_on(runtime.pump_route_once(&mut run, "market", vec![], CommitScope::RealtimeUpdate))
            .unwrap();

    assert!(!outcome.reconnect_required);
    assert_eq!(outcome.commits.len(), 1);
    assert_eq!(outcome.commits[0].scope, CommitScope::SessionTransition);
    assert_eq!(
        outcome.commits[0].changes.path_hits,
        vec![StatePath::new(["system", "internal", "transport-pong"])]
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "transport-pong", "route"]),
        Some(&json!("market"))
    );
}

#[test]
fn session_runtime_drive_timer_once_sends_ping_and_commits_heartbeat_due() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = HeartbeatConnector::new(vec![RecvBehavior::Frame(RawFrame::Pong)]);
    let adapters = adapter_registry();
    let config = session_config();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &MarketTopologyResolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let outcome = block_on(runtime.drive_timer_once(
        &mut run,
        TimerEvent {
            label: "heartbeat-due",
            payload: Some(json!({ "route": "market" })),
        },
        vec![],
        SessionRuntimeDeps::new(
            &TestAuthProvider,
            &MarketTopologyResolver,
            &connector,
            &config,
            &adapters,
        ),
    ))
    .unwrap();

    assert!(!outcome.recovered);
    assert!(outcome.dispatches.is_empty());
    assert_eq!(connector.sent_frames(), vec![OutboundFrame::Ping]);
    assert_eq!(outcome.commits.len(), 2);
    assert_eq!(
        outcome.commits[0].changes.path_hits,
        vec![StatePath::new(["system", "timers", "heartbeat-due"])]
    );
    assert_eq!(
        outcome.commits[1].changes.path_hits,
        vec![StatePath::new(["system", "internal", "transport-ping"])]
    );
}

#[test]
fn session_runtime_drive_timer_once_recovers_after_heartbeat_timeout() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = HeartbeatConnector::new(vec![
        RecvBehavior::Frame(RawFrame::Pong),
        RecvBehavior::Frame(RawFrame::Pong),
    ]);
    let adapters = adapter_registry();
    let config = session_config();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &MarketTopologyResolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let outcome = block_on(runtime.drive_timer_once(
        &mut run,
        TimerEvent {
            label: "heartbeat-timeout",
            payload: Some(json!({ "route": "market" })),
        },
        vec![],
        SessionRuntimeDeps::new(
            &TestAuthProvider,
            &MarketTopologyResolver,
            &connector,
            &config,
            &adapters,
        ),
    ))
    .unwrap();

    assert!(outcome.recovered);
    assert_eq!(outcome.commits.len(), 5);
    assert_eq!(
        outcome.commits[0].changes.path_hits,
        vec![StatePath::new(["system", "timers", "heartbeat-timeout"])]
    );
    assert_eq!(
        outcome.commits[2].changes.path_hits,
        vec![StatePath::new(["system", "session", "reconnect"])]
    );
    assert_eq!(outcome.commits[4].scope, CommitScope::ResyncRecovery);
    assert_eq!(
        connector.connected_labels(),
        vec!["market".to_string(), "market".to_string()]
    );
    assert_eq!(run.bootstrap.phase, SessionPhase::Running);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("running"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "attempt"]),
        Some(&json!(1))
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
        EndpointConfig::new("https://auth.example").with_market_url("wss://market.example"),
    )
    .with_reconnect(ReconnectPolicy::new(
        Duration::from_millis(0),
        Duration::from_millis(0),
        Some(1),
    ))
    .enable_domain(ProtocolDomain::Market)
}

fn market_topology() -> SessionTopology {
    SessionTopology::default().with_route(SessionRoute {
        label: "market".to_string(),
        target: SessionTarget::Shared,
        domains: vec![ProtocolDomain::Market],
        endpoint: SessionRouteEndpoint::WebSocket {
            url: "ws://market.example".to_string(),
            connect: Default::default(),
        },
    })
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
