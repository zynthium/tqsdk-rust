use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, CommitScope, ContractError, ContractFuture, ProtocolDomain, RawFrame, Runtime,
    RuntimeHandle, SessionBootstrap, SessionRoute, SessionRouteConnector, SessionRouteEndpoint, SessionRun,
    SessionRuntime, SessionTarget, SessionTopology, StatePath, Transport,
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

struct ControlledConnector {
    behavior: RecvBehavior,
}

impl SessionRouteConnector for ControlledConnector {
    fn connect_route<'a>(
        &'a self,
        _route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>> {
        let transport = ControlledTransport {
            behavior: self.behavior.clone(),
        };
        Box::pin(async move { Ok(Box::new(transport) as Box<dyn Transport>) })
    }
}

#[test]
fn session_runtime_turns_transport_close_into_reconnect_signal_and_commits() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let mut run = connect_run(RecvBehavior::Frame(RawFrame::Close));

    let outcome = block_on(runtime.pump_route_once(
        &mut run,
        "market",
        vec![],
        CommitScope::RealtimeUpdate,
    ))
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

    let outcome = block_on(runtime.pump_route_once(
        &mut run,
        "market",
        vec![],
        CommitScope::RealtimeUpdate,
    ))
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
        Some(&json!("auth error: websocket recv failed: connection reset"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "lifecycle", "phase"]),
        Some(&json!("reconnecting"))
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
        SessionBootstrap::new().connect_topology(&topology, &ControlledConnector { behavior }),
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
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
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

static NOOP_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(noop_clone, noop, noop, noop);
