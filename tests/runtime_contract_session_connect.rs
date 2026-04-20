use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

mod support;

use support::websocket::{ClientFrame, TestWebSocketServer};
use tqsdk_runtime_contract::{
    AccountId, ProtocolDomain, SessionBootstrap, SessionRoute, SessionRouteEndpoint, SessionTarget,
    SessionTopology, WebSocketConnectOptions, WebSocketRouteConnector,
};

#[test]
fn session_bootstrap_connects_websocket_routes_from_topology() {
    let market_server = TestWebSocketServer::spawn(|mut socket| {
        assert_eq!(
            socket.request().header("authorization"),
            Some("Bearer test-token"),
        );
        match socket.recv().unwrap() {
            ClientFrame::Close => {}
            other => panic!("expected close frame, got {other:?}"),
        }
    })
    .unwrap();

    let trade_server = TestWebSocketServer::spawn(|mut socket| {
        assert_eq!(
            socket.request().header("authorization"),
            Some("Bearer test-token"),
        );
        match socket.recv().unwrap() {
            ClientFrame::Close => {}
            other => panic!("expected close frame, got {other:?}"),
        }
    })
    .unwrap();

    let topology = SessionTopology::default()
        .with_route(SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System, ProtocolDomain::Market],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: market_server.url("/md"),
                connect: WebSocketConnectOptions::default()
                    .with_header("Authorization", "Bearer test-token"),
            },
        })
        .with_route(SessionRoute {
            label: "trade:simnow".to_string(),
            target: SessionTarget::Account(AccountId::new("simnow")),
            domains: vec![ProtocolDomain::Trade],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: trade_server.url("/trade"),
                connect: WebSocketConnectOptions::default()
                    .with_header("Authorization", "Bearer test-token"),
            },
        });

    let connector = WebSocketRouteConnector;
    let mut connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &connector)).unwrap();

    assert_eq!(connected.routes.len(), 2);
    assert_eq!(connected.routes[0].route.label, "market");
    assert_eq!(connected.routes[1].route.label, "trade:simnow");

    block_on(connected.close_all()).unwrap();
    market_server.join();
    trade_server.join();
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
