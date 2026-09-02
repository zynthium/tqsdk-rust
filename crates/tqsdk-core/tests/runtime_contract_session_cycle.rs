#![allow(clippy::manual_async_fn)]

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_core::internal::{DefaultRouteConnector, DynTransport, SessionBootstrap};
use tqsdk_core::internal::{SessionRun, SessionRuntime};
use tqsdk_core::{
    AccountId, AdapterRegistry, BootstrapResult, CommitScope, ContractError, IoEvent,
    MarketCommand, OrderId, OutboundFrame, ProtocolDomain, RawFrame, Result as CoreResult, Runtime,
    RuntimeCommand, RuntimeHandle, RuntimeInput, SchemaCommand, SchemaId, SessionRoute,
    SessionRouteConnector, SessionRouteEndpoint, SessionTarget, SessionTopology, Symbol,
    TradeAccountType, TradeCommand, TradeDirection, TradeInsertOrderCommand, TradeLoginCommand,
    TradeOffset, TradePreInsertOrderCommand, TradePriceType, TradeTimeCondition,
    TradeVolumeCondition, Transport,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = CoreResult<T>> + Send + 'a>>;

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
    fn connect(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }

    fn recv(&mut self) -> impl Future<Output = CoreResult<RawFrame>> + Send + '_ {
        let frame = self
            .recv_frames
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(RawFrame::Pong);
        async move { Ok(frame) }
    }

    fn send(&mut self, frame: OutboundFrame) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        self.sent_frames.lock().unwrap().push(frame);
        async { Ok(()) }
    }

    fn close(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
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
    ) -> BoxFuture<'a, Box<dyn DynTransport>> {
        let transport =
            QueuedTransport::new(self.recv_frames.clone(), Arc::clone(&self.sent_frames));
        Box::pin(async move { Ok(Box::new(transport) as Box<dyn DynTransport>) })
    }
}

#[derive(Default)]
struct FailingSendTransport;

impl Transport for FailingSendTransport {
    fn connect(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }

    fn recv(&mut self) -> impl Future<Output = CoreResult<RawFrame>> + Send + '_ {
        async { Ok(RawFrame::Pong) }
    }

    fn send(&mut self, _frame: OutboundFrame) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Err(ContractError::auth("websocket send failed: broken pipe")) }
    }

    fn close(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }
}

struct FailingSendConnector;

impl SessionRouteConnector for FailingSendConnector {
    fn connect_route<'a>(
        &'a self,
        _route: &'a SessionRoute,
    ) -> BoxFuture<'a, Box<dyn DynTransport>> {
        Box::pin(async { Ok(Box::new(FailingSendTransport) as Box<dyn DynTransport>) })
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
            tqsdk_core::AuthContext::new("token"),
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
            tqsdk_core::AuthContext::new("token"),
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
            tqsdk_core::AuthContext::new("token"),
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
            tqsdk_core::AuthContext::new("token"),
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
fn session_runtime_cancel_order_finish_diff_marks_transport_command_completed() {
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
                                    "last_msg": "已撤单"
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
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(
        handle.submit(RuntimeCommand::Trade(TradeCommand::CancelOrder {
            account_id: AccountId::new("simnow"),
            order_id: OrderId::new("order-1"),
        })),
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
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "last_msg"
        ]),
        Some(&json!("已撤单"))
    );
}

#[test]
fn session_runtime_trade_login_snapshot_marks_transport_command_completed() {
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
                            "session": {
                                "trading_day": "20260420"
                            },
                            "trade_more_data": false
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
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(handle.submit(RuntimeCommand::Trade(TradeCommand::Login(
        TradeLoginCommand {
            account_id: AccountId::new("simnow"),
            broker_id: "9999".to_string(),
            password: "secret".to_string(),
            client_mac_address: None,
            account_type: TradeAccountType::Future,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        },
    ))))
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
        Some(&json!("completed"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "trade_more_data"
        ]),
        Some(&json!(false))
    );
}

#[test]
fn session_runtime_trade_confirm_settlement_sent_status_keeps_command_account_id() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let sent_frames = Arc::new(Mutex::new(Vec::new()));
    let connector = TestRouteConnector {
        sent_frames: Arc::clone(&sent_frames),
        recv_frames: vec![],
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
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(handle.submit(RuntimeCommand::Trade(
        TradeCommand::ConfirmSettlement {
            account_id: AccountId::new("simnow"),
        },
    )))
    .unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 1);

    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("sent"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "account_id"
        ]),
        Some(&json!("simnow"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "aid"
        ]),
        Some(&json!("confirm_settlement"))
    );
}

#[test]
fn session_runtime_trade_settlement_reply_marks_transport_command_completed() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let sent_frames = Arc::new(Mutex::new(Vec::new()));
    let connector = TestRouteConnector {
        sent_frames: Arc::clone(&sent_frames),
        recv_frames: vec![RawFrame::Text(
            json!({
                "aid": "qry_settlement_info",
                "user_name": "simnow",
                "trading_day": "20260420",
                "settlement_info": "line-1\nline-2"
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
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(handle.submit(RuntimeCommand::Trade(
        TradeCommand::QuerySettlementInfo {
            account_id: AccountId::new("simnow"),
            trading_day: 20260420,
        },
    )))
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
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["trade", "simnow", "his_settlements", "20260420", "content"]),
        Some(&json!("line-1\nline-2"))
    );

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
            "trading_day"
        ]),
        Some(&json!("20260420"))
    );
}

#[test]
fn session_runtime_trade_account_info_diff_marks_transport_command_completed() {
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
                            "accounts": {
                                "CNY": {
                                    "balance": 1000000.0,
                                    "available": 990000.0,
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
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(handle.submit(RuntimeCommand::Trade(
        TradeCommand::QueryAccountInfo {
            account_id: AccountId::new("simnow"),
        },
    )))
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
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["trade", "simnow", "accounts", "CNY", "balance"]),
        Some(&json!(1000000.0))
    );

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
            "currency"
        ]),
        Some(&json!("CNY"))
    );
}

#[test]
fn session_runtime_trade_risk_management_rule_diff_marks_transport_command_completed() {
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
                            "risk_management_rule": {
                                "SSE": {
                                    "exchange_id": "SSE",
                                    "enable": true,
                                    "self_trade": {
                                        "count_limit": 3
                                    }
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
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(handle.submit(RuntimeCommand::Trade(
        TradeCommand::SetRiskManagementRule {
            account_id: AccountId::new("simnow"),
            rule: json!({
                "exchange_id": "SSE",
                "enable": true,
                "self_trade": {
                    "count_limit": 3
                }
            }),
        },
    )))
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
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["trade", "simnow", "risk_management_rule", "SSE", "enable"]),
        Some(&json!(true))
    );

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
            "exchange_id"
        ]),
        Some(&json!("SSE"))
    );
}

#[test]
fn session_runtime_trade_pre_insert_order_diff_marks_transport_command_completed() {
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
                            "pre_insert_orders": {
                                "pre-1": {
                                    "exchange_id": "SHFE",
                                    "instrument_id": "au2602",
                                    "direction": "BUY",
                                    "pre_margin": 1234.5
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
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(
        handle.submit(RuntimeCommand::Trade(TradeCommand::PreInsertOrder(
            TradePreInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new("pre-1"),
                symbol: Symbol::new("SHFE.au2602"),
                direction: TradeDirection::Buy,
                offset: Some(TradeOffset::Open),
                volume: 1,
                price_type: TradePriceType::Limit,
                limit_price: Some(json!(0.0)),
                time_condition: TradeTimeCondition::Gfd,
                volume_condition: TradeVolumeCondition::Any,
                hedge_flag: "SPECULATION".to_string(),
                contingent_condition: "IMMEDIATELY".to_string(),
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
    assert_eq!(
        handle.latest_snapshot().get([
            "trade",
            "simnow",
            "pre_insert_orders",
            "pre-1",
            "pre_margin"
        ]),
        Some(&json!(1234.5))
    );

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
            "pre_margin"
        ]),
        Some(&json!(1234.5))
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
            tqsdk_core::AuthContext::new("token"),
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
            payload: tqsdk_core::InputPayload::Json(json!({
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
            tqsdk_core::AuthContext::new("token"),
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
            tqsdk_core::AuthContext::new("token"),
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

#[test]
fn session_runtime_trade_order_status_preserves_seed_detail_over_dispatch_json() {
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
                                "ORDER_SEED": {
                                    "order_id": "ORDER_SEED",
                                    "status": "ALIVE",
                                    "exchange_order_id": "EX_SEED",
                                    "volume_left": 1
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
        label: "trade:simnow".to_string(),
        target: SessionTarget::Shared,
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
            tqsdk_core::AuthContext::new("token"),
            vec![ProtocolDomain::Trade],
        )
        .with_topology(topology),
        connected,
    };

    let command_id = block_on(
        handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new("ORDER_SEED"),
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

    let _receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    let commit = block_on(runtime.recv_route_and_ingest(
        &mut run,
        "trade:simnow",
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();

    assert_eq!(commit.caused_by, vec![command_id]);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "order_id",
        ]),
        Some(&json!("ORDER_SEED"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "exchange_order_id",
        ]),
        Some(&json!("EX_SEED"))
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
