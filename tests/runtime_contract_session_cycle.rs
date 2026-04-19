use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_runtime_contract::{
    AccountId, AdapterRegistry, BootstrapResult, CommitScope, ContractError, ContractFuture,
    DefaultRouteConnector, IoEvent, MarketCommand, OrderId, OutboundFrame, ProtocolDomain,
    RawFrame, Runtime, RuntimeCommand, RuntimeHandle, RuntimeInput, SchemaCommand, SchemaId,
    SessionBootstrap, SessionRoute, SessionRouteConnector, SessionRouteEndpoint, SessionRun,
    SessionRuntime, SessionTarget, SessionTopology, Symbol, TradeCommand, TradeDirection,
    TradeInsertOrderCommand, TradeOffset, TradePriceType, TradeTimeCondition, TradeVolumeCondition,
    Transport,
};

#[derive(Clone)]
struct QueuedTransport {
    recv_frames: Arc<Mutex<VecDeque<RawFrame>>>,
    sent_frames: Arc<Mutex<Vec<OutboundFrame>>>,
}

impl QueuedTransport {
    fn new(recv_frames: Vec<RawFrame>, sent_frames: Arc<Mutex<Vec<OutboundFrame>>>) -> Self {
        Self {
            recv_frames: Arc::new(Mutex::new(recv_frames.into())),
            sent_frames,
        }
    }
}

impl Transport for QueuedTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        let frame = self
            .recv_frames
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(RawFrame::Pong);
        Box::pin(async move { Ok(frame) })
    }

    fn send(&mut self, frame: OutboundFrame) -> ContractFuture<'_, ()> {
        self.sent_frames.lock().unwrap().push(frame);
        Box::pin(async { Ok(()) })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct TestRouteConnector {
    sent_frames: Arc<Mutex<Vec<OutboundFrame>>>,
    recv_frames: Vec<RawFrame>,
}

impl SessionRouteConnector for TestRouteConnector {
    fn connect_route<'a>(
        &'a self,
        _route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>> {
        let transport =
            QueuedTransport::new(self.recv_frames.clone(), Arc::clone(&self.sent_frames));
        Box::pin(async move { Ok(Box::new(transport) as Box<dyn Transport>) })
    }
}

#[derive(Default)]
struct FailingSendTransport;

impl Transport for FailingSendTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        Box::pin(async { Ok(RawFrame::Pong) })
    }

    fn send(&mut self, _frame: OutboundFrame) -> ContractFuture<'_, ()> {
        Box::pin(async { Err(ContractError::auth("websocket send failed: broken pipe")) })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct FailingSendConnector;

impl SessionRouteConnector for FailingSendConnector {
    fn connect_route<'a>(
        &'a self,
        _route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>> {
        Box::pin(async { Ok(Box::new(FailingSendTransport) as Box<dyn Transport>) })
    }
}

#[test]
fn session_runtime_flushes_and_ingests_transport_route_inputs() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let sent_frames = Arc::new(Mutex::new(Vec::new()));
    let connector = TestRouteConnector {
        sent_frames: Arc::clone(&sent_frames),
        recv_frames: vec![RawFrame::Text(
            json!({
                "aid": "rtn_data",
                "data": [{
                    "quotes": {
                        "SHFE.au2602": {
                            "last_price": 618.5,
                            "ask_price1": 619.0
                        }
                    }
                }]
            })
            .to_string(),
        )],
    };

    let topology = SessionTopology::default().with_route(SessionRoute {
        label: "market".to_string(),
        target: SessionTarget::Shared,
        domains: vec![ProtocolDomain::Market],
        endpoint: SessionRouteEndpoint::WebSocket {
            url: "ws://market.example".to_string(),
            connect: Default::default(),
        },
    });

    let connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &connector)).unwrap();
    let mut run = SessionRun {
        bootstrap: BootstrapResult::new(
            tqsdk_runtime_contract::AuthContext::new("token"),
            vec![ProtocolDomain::Market],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(handle.submit(RuntimeCommand::Market(
        MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(
        sent_frames.lock().unwrap().clone(),
        vec![
            OutboundFrame::Text(
                json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string()
            ),
            OutboundFrame::Text(json!({"aid": "peek_message"}).to_string()),
        ]
    );

    let commit = block_on(runtime.recv_route_and_ingest(
        &mut run,
        "market",
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();

    assert_eq!(commit.caused_by, vec![command_id]);
    assert_eq!(commit.scope, CommitScope::RealtimeUpdate);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(618.5))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["quotes", "SHFE.au2602", "ask_price1"]),
        Some(&json!(619.0))
    );
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("partially_applied"))
    );
}

#[test]
fn session_runtime_trade_order_diff_marks_transport_command_acked() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let sent_frames = Arc::new(Mutex::new(Vec::new()));
    let connector = TestRouteConnector {
        sent_frames: Arc::clone(&sent_frames),
        recv_frames: vec![RawFrame::Text(
            json!({
                "aid": "rtn_data",
                "data": [{
                    "trade": {
                        "simnow": {
                            "orders": {
                                "order-1": {
                                    "status": "ALIVE",
                                    "volume_left": 2
                                }
                            }
                        }
                    }
                }]
            })
            .to_string(),
        )],
    };

    let topology = SessionTopology::default().with_route(SessionRoute {
        label: "trade".to_string(),
        target: SessionTarget::Account(AccountId::new("simnow")),
        domains: vec![ProtocolDomain::Trade],
        endpoint: SessionRouteEndpoint::WebSocket {
            url: "ws://trade.example".to_string(),
            connect: Default::default(),
        },
    });

    let connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &connector)).unwrap();
    let mut run = SessionRun {
        bootstrap: BootstrapResult::new(
            tqsdk_runtime_contract::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(
        handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new("order-1"),
                symbol: Symbol::new("SHFE.au2602"),
                direction: TradeDirection::Buy,
                offset: Some(TradeOffset::Open),
                volume: 2,
                price_type: TradePriceType::Limit,
                limit_price: Some(json!(618.5)),
                time_condition: TradeTimeCondition::Gfd,
                volume_condition: TradeVolumeCondition::Any,
            },
        ))),
    )
    .unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 1);

    let commit = block_on(runtime.recv_route_and_ingest(
        &mut run,
        "trade",
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();

    assert_eq!(commit.caused_by, vec![command_id]);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("acked"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "route"
        ]),
        Some(&json!("trade"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "order_status"
        ]),
        Some(&json!("ALIVE"))
    );
}

#[test]
fn session_runtime_trade_order_finish_diff_marks_transport_command_completed() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let sent_frames = Arc::new(Mutex::new(Vec::new()));
    let connector = TestRouteConnector {
        sent_frames: Arc::clone(&sent_frames),
        recv_frames: vec![
            RawFrame::Text(
                json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            "simnow": {
                                "orders": {
                                    "order-1": {
                                        "status": "ALIVE",
                                        "volume_left": 2
                                    }
                                }
                            }
                        }
                    }]
                })
                .to_string(),
            ),
            RawFrame::Text(
                json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            "simnow": {
                                "orders": {
                                    "order-1": {
                                        "status": "FINISHED",
                                        "volume_left": 0,
                                        "exchange_order_id": "EX123",
                                        "last_msg": "全部成交"
                                    }
                                }
                            }
                        }
                    }]
                })
                .to_string(),
            ),
        ],
    };

    let topology = SessionTopology::default().with_route(SessionRoute {
        label: "trade".to_string(),
        target: SessionTarget::Account(AccountId::new("simnow")),
        domains: vec![ProtocolDomain::Trade],
        endpoint: SessionRouteEndpoint::WebSocket {
            url: "ws://trade.example".to_string(),
            connect: Default::default(),
        },
    });

    let connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &connector)).unwrap();
    let mut run = SessionRun {
        bootstrap: BootstrapResult::new(
            tqsdk_runtime_contract::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(
        handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new("order-1"),
                symbol: Symbol::new("SHFE.au2602"),
                direction: TradeDirection::Buy,
                offset: Some(TradeOffset::Open),
                volume: 2,
                price_type: TradePriceType::Limit,
                limit_price: Some(json!(618.5)),
                time_condition: TradeTimeCondition::Gfd,
                volume_condition: TradeVolumeCondition::Any,
            },
        ))),
    )
    .unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 1);

    block_on(runtime.recv_route_and_ingest(
        &mut run,
        "trade",
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();

    let commit = block_on(runtime.recv_route_and_ingest(
        &mut run,
        "trade",
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();

    assert_eq!(commit.caused_by, vec![command_id]);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "order_status"
        ]),
        Some(&json!("FINISHED"))
    );
}

#[test]
fn session_runtime_trade_reject_diff_marks_transport_command_rejected() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let sent_frames = Arc::new(Mutex::new(Vec::new()));
    let connector = TestRouteConnector {
        sent_frames: Arc::clone(&sent_frames),
        recv_frames: vec![RawFrame::Text(
            json!({
                "aid": "rtn_data",
                "data": [{
                    "trade": {
                        "simnow": {
                            "orders": {
                                "order-1": {
                                    "status": "FINISHED",
                                    "volume_left": 1,
                                    "last_msg": "开仓资金不足"
                                }
                            }
                        }
                    }
                }]
            })
            .to_string(),
        )],
    };

    let topology = SessionTopology::default().with_route(SessionRoute {
        label: "trade".to_string(),
        target: SessionTarget::Account(AccountId::new("simnow")),
        domains: vec![ProtocolDomain::Trade],
        endpoint: SessionRouteEndpoint::WebSocket {
            url: "ws://trade.example".to_string(),
            connect: Default::default(),
        },
    });

    let connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &connector)).unwrap();
    let mut run = SessionRun {
        bootstrap: BootstrapResult::new(
            tqsdk_runtime_contract::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(
        handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new("order-1"),
                symbol: Symbol::new("SHFE.au2602"),
                direction: TradeDirection::Buy,
                offset: Some(TradeOffset::Open),
                volume: 1,
                price_type: TradePriceType::Limit,
                limit_price: Some(json!(618.5)),
                time_condition: TradeTimeCondition::Gfd,
                volume_condition: TradeVolumeCondition::Any,
            },
        ))),
    )
    .unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 1);

    let commit = block_on(runtime.recv_route_and_ingest(
        &mut run,
        "trade",
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();

    assert_eq!(commit.caused_by, vec![command_id]);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("rejected"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "last_msg"
        ]),
        Some(&json!("开仓资金不足"))
    );
}

#[test]
fn session_runtime_ingests_queued_non_transport_route_inputs() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let topology = SessionTopology::default().with_route(SessionRoute {
        label: "instrument-schema".to_string(),
        target: SessionTarget::Shared,
        domains: vec![ProtocolDomain::Schema],
        endpoint: SessionRouteEndpoint::Http {
            url: "https://schema.example".to_string(),
        },
    });

    let connected = block_on(
        SessionBootstrap::new().connect_topology(&topology, &DefaultRouteConnector::default()),
    )
    .unwrap();
    let mut run = SessionRun {
        bootstrap: BootstrapResult::new(
            tqsdk_runtime_contract::AuthContext::new("token"),
            vec![ProtocolDomain::Schema],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(
        handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("instrument-schema"),
            path: "/schema/instrument.json".to_string(),
        })),
    )
    .unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].route_label, "instrument-schema");

    run.connected
        .route_mut("instrument-schema")
        .unwrap()
        .queue_input(RuntimeInput::Io(IoEvent {
            route: "instrument-schema".to_string(),
            domains: vec![ProtocolDomain::Schema],
            payload: tqsdk_runtime_contract::InputPayload::Json(json!({
                "nodes": {
                    "quote": {
                        "fields": ["last_price", "ask_price1"]
                    }
                }
            })),
        }));

    let commit = runtime
        .ingest_queued_inputs(&mut run, vec![command_id], CommitScope::InitialReady)
        .unwrap()
        .unwrap();

    assert_eq!(commit.caused_by, vec![command_id]);
    assert_eq!(commit.scope, CommitScope::InitialReady);
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["schema", "instrument-schema", "nodes", "quote", "fields"]),
        Some(&json!(["last_price", "ask_price1"]))
    );
}

#[test]
fn session_runtime_flush_outbound_marks_commands_as_sent() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let sent_frames = Arc::new(Mutex::new(Vec::new()));
    let connector = TestRouteConnector {
        sent_frames: Arc::clone(&sent_frames),
        recv_frames: vec![RawFrame::Pong],
    };

    let topology = SessionTopology::default()
        .with_route(SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Market],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: "ws://market.example".to_string(),
                connect: Default::default(),
            },
        })
        .with_route(SessionRoute {
            label: "instrument-schema".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Schema],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://schema.example".to_string(),
            },
        });

    let connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &connector)).unwrap();
    let mut run = SessionRun {
        bootstrap: BootstrapResult::new(
            tqsdk_runtime_contract::AuthContext::new("token"),
            vec![ProtocolDomain::Market, ProtocolDomain::Schema],
        )
        .with_topology(topology),
        connected,
    };

    let market_id = block_on(handle.submit(RuntimeCommand::Market(
        MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();
    let schema_id = block_on(
        handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("instrument-schema"),
            path: "/schema/instrument.json".to_string(),
        })),
    )
    .unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 3);

    let market_segment = market_id.get().to_string();
    let schema_segment = schema_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", market_segment.as_str(), "status"]),
        Some(&json!("sent"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            market_segment.as_str(),
            "detail",
            "route"
        ]),
        Some(&json!("market"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", schema_segment.as_str(), "status"]),
        Some(&json!("sent"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            schema_segment.as_str(),
            "detail",
            "route"
        ]),
        Some(&json!("instrument-schema"))
    );
}

#[test]
fn session_runtime_flush_outbound_marks_commands_failed_when_transport_send_errors() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let topology = SessionTopology::default().with_route(SessionRoute {
        label: "market".to_string(),
        target: SessionTarget::Shared,
        domains: vec![ProtocolDomain::Market],
        endpoint: SessionRouteEndpoint::WebSocket {
            url: "ws://market.example".to_string(),
            connect: Default::default(),
        },
    });

    let connected =
        block_on(SessionBootstrap::new().connect_topology(&topology, &FailingSendConnector))
            .unwrap();
    let mut run = SessionRun {
        bootstrap: BootstrapResult::new(
            tqsdk_runtime_contract::AuthContext::new("token"),
            vec![ProtocolDomain::Market],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(handle.submit(RuntimeCommand::Market(
        MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();

    let err = block_on(runtime.flush_outbound(&mut run)).unwrap_err();
    assert_eq!(
        err.to_string(),
        "auth error: websocket send failed: broken pipe"
    );

    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("failed"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "route"
        ]),
        Some(&json!("market"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "message"
        ]),
        Some(&json!("auth error: websocket send failed: broken pipe"))
    );
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
