use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, AuthContext, AuthProvider, CommitScope, ContractError, ContractFuture,
    EndpointConfig, ProtocolDomain, RawFrame, ReconnectPolicy, Revision, Runtime, RuntimeHandle,
    SessionBootstrap, SessionConfig, SessionPhase, SessionRoute, SessionRouteConnector,
    SessionRouteEndpoint, SessionRun, SessionRuntime, SessionRuntimeDeps, SessionTarget,
    SessionTopology, SessionTopologyResolver, StatePath, Transport,
};

#[derive(Clone)]
enum RecvBehavior {
    Frame(RawFrame),
    Error(ContractError),
}

#[derive(Clone)]
struct ControlledTransport {
    behavior: RecvBehavior,
}

impl Transport for ControlledTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        let behavior = self.behavior.clone();
        Box::pin(async move {
            match behavior {
                RecvBehavior::Frame(frame) => Ok(frame),
                RecvBehavior::Error(err) => Err(err),
            }
        })
    }

    fn send(&mut self, _frame: tqsdk_runtime_contract::OutboundFrame) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
enum ConnectOutcome {
    Connected(RecvBehavior),
    Error(ContractError),
}

struct ControlledConnector {
    outcomes: Arc<Mutex<VecDeque<ConnectOutcome>>>,
    connected_labels: Arc<Mutex<Vec<String>>>,
}

impl ControlledConnector {
    fn new(behaviors: Vec<RecvBehavior>) -> Self {
        Self::with_outcomes(
            behaviors
                .into_iter()
                .map(ConnectOutcome::Connected)
                .collect::<Vec<_>>(),
        )
    }

    fn with_outcomes(outcomes: Vec<ConnectOutcome>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into())),
            connected_labels: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn connected_labels(&self) -> Vec<String> {
        self.connected_labels.lock().unwrap().clone()
    }
}

impl SessionRouteConnector for ControlledConnector {
    fn connect_route<'a>(
        &'a self,
        route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>> {
        let outcomes = Arc::clone(&self.outcomes);
        let connected_labels = Arc::clone(&self.connected_labels);
        let label = route.label.clone();
        Box::pin(async move {
            connected_labels.lock().unwrap().push(label);
            let outcome =
                outcomes
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(ConnectOutcome::Connected(RecvBehavior::Frame(
                        RawFrame::Pong,
                    )));

            match outcome {
                ConnectOutcome::Connected(behavior) => {
                    Ok(Box::new(ControlledTransport { behavior }) as Box<dyn Transport>)
                }
                ConnectOutcome::Error(err) => Err(err),
            }
        })
    }
}

#[test]
fn session_runtime_turns_transport_close_into_reconnect_signal_and_commits() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let mut run = connect_run(RecvBehavior::Frame(RawFrame::Close));

    let outcome =
        block_on(runtime.pump_route_once(&mut run, "market", vec![], CommitScope::RealtimeUpdate))
            .unwrap();

    assert!(outcome.reconnect_required);
    assert_eq!(outcome.commits.len(), 2);
    assert_eq!(outcome.commits[0].scope, CommitScope::SessionTransition);
    assert_eq!(
        outcome.commits[0].changes.path_hits,
        vec![StatePath::new(["system", "internal", "transport-close"])]
    );
    assert_eq!(outcome.commits[1].scope, CommitScope::SessionTransition);
    assert_eq!(
        outcome.commits[1].changes.path_hits,
        vec![StatePath::new(["system", "session", "lifecycle"])]
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "transport-close", "route"]),
        Some(&json!("market"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("reconnecting"))
    );
}

#[test]
fn session_runtime_turns_transport_recv_errors_into_reconnect_signal_and_commits() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let mut run = connect_run(RecvBehavior::Error(ContractError::auth(
        "websocket recv failed: connection reset",
    )));

    let outcome =
        block_on(runtime.pump_route_once(&mut run, "market", vec![], CommitScope::RealtimeUpdate))
            .unwrap();

    assert!(outcome.reconnect_required);
    assert_eq!(outcome.commits.len(), 2);
    assert_eq!(
        outcome.commits[0].changes.path_hits,
        vec![StatePath::new(["system", "internal", "transport-error"])]
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "transport-error", "route"]),
        Some(&json!("market"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "transport-error", "message"]),
        Some(&json!(
            "auth error: websocket recv failed: connection reset"
        ))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("reconnecting"))
    );
}

#[test]
fn session_runtime_drive_route_once_recovers_after_transport_close_without_duplicate_reconnecting_commit()
 {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = ControlledConnector::new(vec![
        RecvBehavior::Frame(RawFrame::Close),
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
    let start_revision = Revision::new(log.head_revision().unwrap().get() + 1);
    let mut cursor = handle.cursor_from(start_revision);

    let outcome = block_on(runtime.drive_route_once(
        &mut run,
        "market",
        vec![],
        CommitScope::RealtimeUpdate,
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
    assert!(outcome.dispatches.is_empty());
    assert_eq!(outcome.commits.len(), 5);
    assert_eq!(
        outcome.commits[0].changes.path_hits,
        vec![StatePath::new(["system", "internal", "transport-close"])]
    );
    assert_eq!(outcome.commits[1].scope, CommitScope::SessionTransition);
    assert_eq!(
        outcome.commits[2].changes.path_hits,
        vec![StatePath::new(["system", "session", "reconnect"])]
    );
    assert_eq!(outcome.commits[3].scope, CommitScope::SessionTransition);
    assert_eq!(outcome.commits[4].scope, CommitScope::ResyncRecovery);
    assert_eq!(
        connector.connected_labels(),
        vec!["market".to_string(), "market".to_string()]
    );
    assert_eq!(run.bootstrap.phase, SessionPhase::Running);
    assert_eq!(run.connected.routes.len(), 1);
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

    assert_eq!(
        log.next(&mut cursor).unwrap().scope,
        CommitScope::SessionTransition
    );
    assert_eq!(
        log.next(&mut cursor).unwrap().scope,
        CommitScope::SessionTransition
    );
    assert_eq!(
        log.next(&mut cursor).unwrap().scope,
        CommitScope::SessionTransition
    );
    assert_eq!(
        log.next(&mut cursor).unwrap().scope,
        CommitScope::SessionTransition
    );
    assert_eq!(
        log.next(&mut cursor).unwrap().scope,
        CommitScope::ResyncRecovery
    );
    assert_eq!(log.next(&mut cursor), None);
}

#[test]
fn session_runtime_drive_route_once_recovers_after_transport_error() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = ControlledConnector::new(vec![
        RecvBehavior::Error(ContractError::auth("websocket recv failed: broken pipe")),
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

    let outcome = block_on(runtime.drive_route_once(
        &mut run,
        "market",
        vec![],
        CommitScope::RealtimeUpdate,
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
        vec![StatePath::new(["system", "internal", "transport-error"])]
    );
    assert_eq!(
        outcome.commits[2].changes.path_hits,
        vec![StatePath::new(["system", "session", "reconnect"])]
    );
    assert_eq!(outcome.commits[4].scope, CommitScope::ResyncRecovery);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "transport-error", "message"]),
        Some(&json!("auth error: websocket recv failed: broken pipe"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("running"))
    );
}

#[test]
fn session_runtime_retries_recovery_with_reconnect_policy_until_connect_succeeds() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = ControlledConnector::with_outcomes(vec![
        ConnectOutcome::Connected(RecvBehavior::Frame(RawFrame::Close)),
        ConnectOutcome::Error(ContractError::auth("websocket reconnect failed: attempt 1")),
        ConnectOutcome::Connected(RecvBehavior::Frame(RawFrame::Pong)),
    ]);
    let adapters = adapter_registry();
    let config = session_config().with_reconnect(ReconnectPolicy::new(
        Duration::from_secs(1),
        Duration::from_secs(8),
        Some(3),
    ));

    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &MarketTopologyResolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let outcome = block_on(runtime.drive_route_once(
        &mut run,
        "market",
        vec![],
        CommitScope::RealtimeUpdate,
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
    assert_eq!(run.bootstrap.phase, SessionPhase::Running);
    assert_eq!(run.connected.routes.len(), 1);
    assert_eq!(connector.connected_labels().len(), 3);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "attempt"]),
        Some(&json!(2))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "scheduled_backoff_ms"]),
        Some(&json!(2000))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "max_attempts"]),
        Some(&json!(3))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "exhausted"]),
        Some(&json!(false))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "internal", "session-recovery-error", "attempt"]),
        Some(&json!(1))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("running"))
    );
}

#[test]
fn session_runtime_closes_session_when_reconnect_attempts_are_exhausted() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let connector = ControlledConnector::with_outcomes(vec![
        ConnectOutcome::Connected(RecvBehavior::Frame(RawFrame::Close)),
        ConnectOutcome::Error(ContractError::auth("websocket reconnect failed: attempt 1")),
        ConnectOutcome::Error(ContractError::auth("websocket reconnect failed: attempt 2")),
    ]);
    let adapters = adapter_registry();
    let config = session_config().with_reconnect(ReconnectPolicy::new(
        Duration::from_secs(1),
        Duration::from_secs(8),
        Some(2),
    ));

    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &MarketTopologyResolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let err = block_on(runtime.drive_route_once(
        &mut run,
        "market",
        vec![],
        CommitScope::RealtimeUpdate,
        SessionRuntimeDeps::new(
            &TestAuthProvider,
            &MarketTopologyResolver,
            &connector,
            &config,
            &adapters,
        ),
    ))
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "auth error: websocket reconnect failed: attempt 2"
    );
    assert_eq!(connector.connected_labels().len(), 3);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "attempt"]),
        Some(&json!(2))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "scheduled_backoff_ms"]),
        Some(&json!(2000))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "reconnect", "exhausted"]),
        Some(&json!(true))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("closed"))
    );
}

fn connect_run(behavior: RecvBehavior) -> SessionRun {
    let topology = SessionTopology::default().with_route(SessionRoute {
        label: "market".to_string(),
        target: SessionTarget::Shared,
        domains: vec![ProtocolDomain::Market],
        endpoint: SessionRouteEndpoint::WebSocket {
            url: "ws://market.example".to_string(),
            connect: Default::default(),
        },
    });

    let connected = block_on(
        SessionBootstrap::new()
            .connect_topology(&topology, &ControlledConnector::new(vec![behavior])),
    )
    .unwrap();

    SessionRun {
        bootstrap: tqsdk_runtime_contract::BootstrapResult::new(
            tqsdk_runtime_contract::AuthContext::new("token"),
            vec![ProtocolDomain::Market],
        )
        .with_topology(topology),
        connected,
    }
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
    .enable_domain(ProtocolDomain::Market)
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
