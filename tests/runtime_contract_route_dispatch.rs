use std::future::Future;
use std::net::TcpListener;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, DefaultRouteConnector, HttpMethod, MarketCommand, OutboundDispatch,
    ProtocolDomain, ReplayCommand, Runtime, RuntimeCommand, RuntimeHandle, SchemaCommand, SchemaId,
    SessionBootstrap, SessionRoute, SessionRouteEndpoint, SessionTarget, SessionTopology, Symbol,
    SystemCommand, WebSocketConnectOptions,
};
use tungstenite::handshake::server::{Request, Response};
use tungstenite::{Message, accept_hdr};

#[test]
fn runtime_handle_drain_dispatches_resolves_command_domains() {
    let handle = runtime_with_default_adapters();

    let market_id = block_on(handle.submit(RuntimeCommand::Market(
        MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();
    let schema_id = block_on(
        handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("instrument"),
            path: "/schema/instrument.json".to_string(),
        })),
    )
    .unwrap();

    let dispatches = handle.drain_dispatches().unwrap();

    assert_eq!(
        dispatches,
        vec![
            OutboundDispatch {
                command_id: market_id,
                domain: ProtocolDomain::Market,
                request: tqsdk_runtime_contract::OutboundRequest::Transport(
                    tqsdk_runtime_contract::OutboundFrame::Text(
                        json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
                    ),
                ),
            },
            OutboundDispatch {
                command_id: market_id,
                domain: ProtocolDomain::Market,
                request: tqsdk_runtime_contract::OutboundRequest::Transport(
                    tqsdk_runtime_contract::OutboundFrame::Text(
                        json!({"aid": "peek_message"}).to_string(),
                    ),
                ),
            },
            OutboundDispatch {
                command_id: schema_id,
                domain: ProtocolDomain::Schema,
                request: tqsdk_runtime_contract::OutboundRequest::Http(
                    tqsdk_runtime_contract::HttpRequest {
                        method: HttpMethod::Get,
                        path: Some("/schema/instrument.json".to_string()),
                        body: None,
                    },
                ),
            },
        ]
    );
}

#[test]
#[allow(clippy::result_large_err)]
fn connected_topology_dispatches_transport_and_queues_non_transport_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut socket = accept_hdr(stream, |request: &Request, response: Response| {
            assert_eq!(
                request
                    .headers()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok()),
                Some("Bearer test-token"),
            );
            Ok(response)
        })
        .unwrap();

        let first = socket.read().unwrap();
        let second = socket.read().unwrap();
        assert_eq!(
            first,
            Message::Text(json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string())
        );
        assert_eq!(
            second,
            Message::Text(json!({"aid": "peek_message"}).to_string())
        );
        let _ = socket.close(None);
    });

    let topology = SessionTopology::default()
        .with_route(SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Market],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: format!("ws://{addr}/md"),
                connect: WebSocketConnectOptions::default()
                    .with_header("Authorization", "Bearer test-token"),
            },
        })
        .with_route(SessionRoute {
            label: "schema".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Schema],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://schema.example".to_string(),
            },
        })
        .with_route(SessionRoute {
            label: "replay".to_string(),
            target: SessionTarget::Replay(tqsdk_runtime_contract::ReplaySessionId::new("rb-1")),
            domains: vec![ProtocolDomain::Replay],
            endpoint: SessionRouteEndpoint::Replay {
                label: "replay-driver".to_string(),
            },
        })
        .with_route(SessionRoute {
            label: "system".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System],
            endpoint: SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            },
        });

    let mut connected = block_on(
        SessionBootstrap::new().connect_topology(&topology, &DefaultRouteConnector::default()),
    )
    .unwrap();
    let handle = runtime_with_default_adapters();

    let market_id = block_on(handle.submit(RuntimeCommand::Market(
        MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();
    let schema_id = block_on(
        handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("instrument"),
            path: "/schema/instrument.json".to_string(),
        })),
    )
    .unwrap();
    let replay_id = block_on(handle.submit(RuntimeCommand::Replay(ReplayCommand::Step))).unwrap();
    let system_id =
        block_on(handle.submit(RuntimeCommand::System(SystemCommand::RefreshAuth))).unwrap();

    let dispatches = handle.drain_dispatches().unwrap();
    let receipts = dispatches
        .into_iter()
        .map(|dispatch| block_on(connected.dispatch(dispatch)).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        receipts
            .iter()
            .map(|receipt| (
                receipt.command_id,
                receipt.route_label.as_str(),
                receipt.domain
            ))
            .collect::<Vec<_>>(),
        vec![
            (market_id, "market", ProtocolDomain::Market),
            (market_id, "market", ProtocolDomain::Market),
            (schema_id, "schema", ProtocolDomain::Schema),
            (replay_id, "replay", ProtocolDomain::Replay),
            (system_id, "system", ProtocolDomain::System),
        ]
    );

    assert!(connected.routes[0].drain_pending_requests().is_empty());
    assert_eq!(
        connected.routes[1].drain_pending_requests(),
        vec![OutboundDispatch {
            command_id: schema_id,
            domain: ProtocolDomain::Schema,
            request: tqsdk_runtime_contract::OutboundRequest::Http(
                tqsdk_runtime_contract::HttpRequest {
                    method: HttpMethod::Get,
                    path: Some("/schema/instrument.json".to_string()),
                    body: None,
                }
            ),
        }]
    );
    assert_eq!(
        connected.routes[2].drain_pending_requests(),
        vec![OutboundDispatch {
            command_id: replay_id,
            domain: ProtocolDomain::Replay,
            request: tqsdk_runtime_contract::OutboundRequest::Replay(
                tqsdk_runtime_contract::ReplayRequest { action: "step" },
            ),
        }]
    );
    assert_eq!(
        connected.routes[3].drain_pending_requests(),
        vec![OutboundDispatch {
            command_id: system_id,
            domain: ProtocolDomain::System,
            request: tqsdk_runtime_contract::OutboundRequest::Internal(
                tqsdk_runtime_contract::InternalRequest {
                    label: "refresh-auth",
                },
            ),
        }]
    );

    block_on(connected.close_all()).unwrap();
    server.join().unwrap();
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

static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
