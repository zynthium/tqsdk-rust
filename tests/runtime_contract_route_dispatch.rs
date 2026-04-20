use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

mod support;

use serde_json::json;
use support::websocket::{ClientFrame, TestWebSocketServer};
use tqsdk_runtime_contract::{
    AccountId, AdapterRegistry, DefaultRouteConnector, HttpMethod, MarketCommand, OutboundDispatch,
    OutboundFrame, ProtocolDomain, RawFrame, ReplayCommand, Runtime, RuntimeCommand, RuntimeHandle,
    SchemaCommand, SchemaId, SessionBootstrap, SessionRoute, SessionRouteConnector,
    SessionRouteEndpoint, SessionTarget, SessionTopology, Symbol, SystemCommand, TradeCommand,
    Transport, WebSocketConnectOptions,
};

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
                account_id: None,
                request: tqsdk_runtime_contract::OutboundRequest::Transport(
                    tqsdk_runtime_contract::OutboundFrame::Text(
                        json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
                    ),
                ),
            },
            OutboundDispatch {
                command_id: market_id,
                domain: ProtocolDomain::Market,
                account_id: None,
                request: tqsdk_runtime_contract::OutboundRequest::Transport(
                    tqsdk_runtime_contract::OutboundFrame::Text(
                        json!({"aid": "peek_message"}).to_string(),
                    ),
                ),
            },
            OutboundDispatch {
                command_id: schema_id,
                domain: ProtocolDomain::Schema,
                account_id: None,
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
    run_on_tokio(async {
        let server = TestWebSocketServer::spawn(|mut socket| {
            assert_eq!(
                socket.request().header("authorization"),
                Some("Bearer test-token"),
            );
            assert_eq!(
                socket.recv().unwrap(),
                ClientFrame::Text(
                    json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string()
                )
            );
            assert_eq!(
                socket.recv().unwrap(),
                ClientFrame::Text(json!({"aid": "peek_message"}).to_string())
            );
            socket.send_close().unwrap();
        })
        .unwrap();

        let topology = SessionTopology::default()
            .with_route(SessionRoute {
                label: "market".to_string(),
                target: SessionTarget::Shared,
                domains: vec![ProtocolDomain::Market],
                endpoint: SessionRouteEndpoint::WebSocket {
                    url: server.url("/md"),
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

        let mut connected = SessionBootstrap::new()
            .connect_topology(&topology, &DefaultRouteConnector::default())
            .await
            .unwrap();
        let handle = runtime_with_default_adapters();

        let market_id = handle
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new("SHFE.au2602")],
            }))
            .await
            .unwrap();
        let schema_id = handle
            .submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
                schema_id: SchemaId::new("instrument"),
                path: "/schema/instrument.json".to_string(),
            }))
            .await
            .unwrap();
        let replay_id = handle
            .submit(RuntimeCommand::Replay(ReplayCommand::Step))
            .await
            .unwrap();
        let system_id = handle
            .submit(RuntimeCommand::System(SystemCommand::RefreshAuth))
            .await
            .unwrap();

        let dispatches = handle.drain_dispatches().unwrap();
        let mut receipts = Vec::new();
        for dispatch in dispatches {
            receipts.push(connected.dispatch(dispatch).await.unwrap());
        }

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
                account_id: None,
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
                account_id: None,
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
                account_id: None,
                request: tqsdk_runtime_contract::OutboundRequest::Internal(
                    tqsdk_runtime_contract::InternalRequest {
                        label: "refresh-auth",
                    },
                ),
            }]
        );

        connected.close_all().await.unwrap();
        server.join();
    });
}

#[test]
fn connected_topology_routes_trade_dispatches_to_matching_account_route() {
    run_on_tokio(async {
        let connector = RecordingRouteConnector::default();
        let topology = SessionTopology::default()
            .with_route(SessionRoute {
                label: "trade:sim-a".to_string(),
                target: SessionTarget::Account(AccountId::new("sim-a")),
                domains: vec![ProtocolDomain::Trade],
                endpoint: SessionRouteEndpoint::WebSocket {
                    url: "ws://trade-a.example".to_string(),
                    connect: WebSocketConnectOptions::default(),
                },
            })
            .with_route(SessionRoute {
                label: "trade:sim-b".to_string(),
                target: SessionTarget::Account(AccountId::new("sim-b")),
                domains: vec![ProtocolDomain::Trade],
                endpoint: SessionRouteEndpoint::WebSocket {
                    url: "ws://trade-b.example".to_string(),
                    connect: WebSocketConnectOptions::default(),
                },
            });

        let mut connected = SessionBootstrap::new()
            .connect_topology(&topology, &connector)
            .await
            .unwrap();
        let handle = runtime_with_default_adapters();

        let command_a = handle
            .submit(RuntimeCommand::Trade(TradeCommand::QueryAccountInfo {
                account_id: AccountId::new("sim-a"),
            }))
            .await
            .unwrap();
        let command_b = handle
            .submit(RuntimeCommand::Trade(TradeCommand::QueryAccountInfo {
                account_id: AccountId::new("sim-b"),
            }))
            .await
            .unwrap();

        let mut receipts = Vec::new();
        for dispatch in handle.drain_dispatches().unwrap() {
            receipts.push(connected.dispatch(dispatch).await.unwrap());
        }

        assert_eq!(
            receipts
                .iter()
                .map(|receipt| (receipt.command_id, receipt.route_label.as_str()))
                .collect::<Vec<_>>(),
            vec![(command_a, "trade:sim-a"), (command_b, "trade:sim-b")]
        );
        assert_eq!(
            connector.sent_frames(),
            vec![
                (
                    "trade:sim-a".to_string(),
                    json!({"aid": "qry_account_info", "user_id": "sim-a"}).to_string(),
                ),
                (
                    "trade:sim-b".to_string(),
                    json!({"aid": "qry_account_info", "user_id": "sim-b"}).to_string(),
                ),
            ]
        );
    });
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}

#[derive(Clone, Default)]
struct RecordingRouteConnector {
    sent_frames: Arc<Mutex<Vec<(String, String)>>>,
}

impl RecordingRouteConnector {
    fn sent_frames(&self) -> Vec<(String, String)> {
        self.sent_frames.lock().unwrap().clone()
    }
}

impl SessionRouteConnector for RecordingRouteConnector {
    fn connect_route<'a>(
        &'a self,
        route: &'a SessionRoute,
    ) -> tqsdk_runtime_contract::ContractFuture<'a, Box<dyn Transport>> {
        let label = route.label.clone();
        let sent_frames = Arc::clone(&self.sent_frames);
        Box::pin(async move {
            Ok(Box::new(RecordingTransport { label, sent_frames }) as Box<dyn Transport>)
        })
    }
}

struct RecordingTransport {
    label: String,
    sent_frames: Arc<Mutex<Vec<(String, String)>>>,
}

impl Transport for RecordingTransport {
    fn connect(&mut self) -> tqsdk_runtime_contract::ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&mut self) -> tqsdk_runtime_contract::ContractFuture<'_, RawFrame> {
        Box::pin(async { Ok(RawFrame::Pong) })
    }

    fn send(
        &mut self,
        frame: OutboundFrame,
    ) -> tqsdk_runtime_contract::ContractFuture<'_, ()> {
        let label = self.label.clone();
        let sent_frames = Arc::clone(&self.sent_frames);
        Box::pin(async move {
            let text = match frame {
                OutboundFrame::Text(text) => text,
                other => panic!("expected trade text frame, got {other:?}"),
            };
            sent_frames.lock().unwrap().push((label, text));
            Ok(())
        })
    }

    fn close(&mut self) -> tqsdk_runtime_contract::ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
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
