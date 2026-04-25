use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

mod support;

use support::websocket::{ClientFrame, TestWebSocketServer};
use tqsdk_core::{
    DefaultRouteConnector, OutboundFrame, ProtocolDomain, SessionBootstrap, SessionRoute,
    SessionRouteEndpoint, SessionTarget, SessionTopology, WebSocketConnectOptions,
};

#[test]
fn transport_orchestration_methods_do_not_box_futures() {
    let source = include_str!("../src/transport.rs");
    let blocks = [
        (
            "ConnectedSessionRoute",
            source
                .split("impl ConnectedSessionRoute {")
                .nth(1)
                .and_then(|tail| tail.split("#[derive(Default)]").next()),
        ),
        (
            "ConnectedTopology",
            source
                .split("impl ConnectedTopology {")
                .nth(1)
                .and_then(|tail| tail.split("#[doc(hidden)]").next()),
        ),
        (
            "SessionBootstrap",
            source
                .split("impl SessionBootstrap {")
                .nth(1)
                .and_then(|tail| tail.split("#[cfg(test)]").next()),
        ),
    ];

    for (name, block) in blocks {
        let block = block.unwrap_or_else(|| panic!("{name} impl block should be present"));
        assert!(
            !block.contains("Box::pin"),
            "{name} orchestration methods should use native async futures"
        );
    }
}

#[test]
fn default_route_connector_supports_non_websocket_route_endpoints() {
    let topology = SessionTopology::default()
        .with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example/graphql".to_string(),
            },
        })
        .with_route(SessionRoute {
            label: "replay".to_string(),
            target: SessionTarget::Replay(tqsdk_core::ReplaySessionId::new("rb-1")),
            domains: vec![ProtocolDomain::Replay],
            endpoint: SessionRouteEndpoint::Replay {
                label: "replay-driver".to_string(),
            },
        })
        .with_route(SessionRoute {
            label: "internal".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System],
            endpoint: SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            },
        });

    let connector = DefaultRouteConnector::default();
    let mut connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &connector)).unwrap();

    assert_eq!(connected.routes.len(), 3);
    assert_eq!(connected.routes[0].route.label, "query");
    assert_eq!(connected.routes[1].route.label, "replay");
    assert_eq!(connected.routes[2].route.label, "internal");

    let query_send = block_on(
        connected.routes[0]
            .transport
            .send_boxed(OutboundFrame::Ping),
    )
    .unwrap_err();
    assert_eq!(
        query_send.to_string(),
        "validation error: http route transport does not support frame send"
    );

    let replay_recv = block_on(connected.routes[1].transport.recv_boxed()).unwrap_err();
    assert_eq!(
        replay_recv.to_string(),
        "validation error: replay route transport does not support frame recv"
    );

    let internal_send = block_on(
        connected.routes[2]
            .transport
            .send_boxed(OutboundFrame::Ping),
    )
    .unwrap_err();
    assert_eq!(
        internal_send.to_string(),
        "validation error: internal route transport does not support frame send"
    );

    block_on(connected.close_all()).unwrap();
}

#[test]
fn default_route_connector_delegates_websocket_routes() {
    run_on_tokio(async {
        let server = TestWebSocketServer::spawn(|mut socket| {
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
                    url: server.url("/md"),
                    connect: WebSocketConnectOptions::default()
                        .with_header("Authorization", "Bearer test-token"),
                },
            })
            .with_route(SessionRoute {
                label: "internal".to_string(),
                target: SessionTarget::Shared,
                domains: vec![ProtocolDomain::System],
                endpoint: SessionRouteEndpoint::Internal {
                    label: "system-driver".to_string(),
                },
            });

        let connector = DefaultRouteConnector::default();
        let mut connected = SessionBootstrap::new()
            .connect_topology(&topology, &connector)
            .await
            .unwrap();

        assert_eq!(connected.routes.len(), 2);
        assert_eq!(connected.routes[0].route.label, "market");
        assert_eq!(connected.routes[1].route.label, "internal");

        connected.close_all().await.unwrap();
        server.join();
    });
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
