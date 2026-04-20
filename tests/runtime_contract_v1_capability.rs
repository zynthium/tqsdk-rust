use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_runtime_contract::{
    AccountId, AdapterRegistry, AuthContext, AuthEvent, AuthId, AuthProvider, CommitScope,
    ContractFuture, EndpointConfig, InputPayload, IoEvent, MarketCommand, OrderId,
    OutboundDispatch, OutboundFrame, ProtocolDomain, QueryCommand, QueryId, RawFrame,
    ReplayCommand, ReplayEvent, ReplaySessionId, RouteRequestExecutor, Runtime, RuntimeCommand,
    RuntimeHandle, RuntimeInput, SchemaCommand, SchemaId, SessionBootstrap, SessionConfig,
    SessionRoute, SessionRouteConnector, SessionRouteEndpoint, SessionRuntime, SessionTarget,
    SessionTopology, SessionTopologyResolver, SystemCommand, TradeAccountType, TradeCommand,
    TradeDirection, TradeInsertOrderCommand, TradeLoginCommand, TradeOffset,
    TradePreInsertOrderCommand, TradePriceType, TradeTimeCondition, TradeVolumeCondition,
    Transport,
};

struct CapabilityAuthProvider;

impl AuthProvider for CapabilityAuthProvider {
    fn authenticate(&self) -> ContractFuture<'_, AuthContext> {
        Box::pin(async {
            Ok(AuthContext::new("test-token")
                .with_auth_id(AuthId::new("auth-v1"))
                .with_feature("market")
                .with_feature("trade")
                .with_feature("query")
                .with_feature("schema")
                .with_feature("replay")
                .with_feature("system"))
        })
    }
}

struct StaticTopologyResolver {
    topology: SessionTopology,
    expected_domains: Vec<ProtocolDomain>,
}

impl SessionTopologyResolver for StaticTopologyResolver {
    fn resolve_topology<'a>(
        &'a self,
        _auth: &'a AuthContext,
        _config: &'a SessionConfig,
        enabled_domains: &'a [ProtocolDomain],
    ) -> ContractFuture<'a, SessionTopology> {
        let topology = self.topology.clone();
        let expected_domains = self.expected_domains.clone();
        Box::pin(async move {
            assert_eq!(enabled_domains, expected_domains.as_slice());
            Ok(topology)
        })
    }
}

struct QueuedTransport {
    label: String,
    recv_frames: VecDeque<RawFrame>,
    sent_frames: Arc<Mutex<Vec<(String, OutboundFrame)>>>,
}

impl QueuedTransport {
    fn new(
        label: impl Into<String>,
        recv_frames: Vec<RawFrame>,
        sent_frames: Arc<Mutex<Vec<(String, OutboundFrame)>>>,
    ) -> Self {
        Self {
            label: label.into(),
            recv_frames: recv_frames.into(),
            sent_frames,
        }
    }
}

impl Transport for QueuedTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        let frame = self.recv_frames.pop_front().unwrap_or(RawFrame::Pong);
        Box::pin(async move { Ok(frame) })
    }

    fn send(&mut self, frame: OutboundFrame) -> ContractFuture<'_, ()> {
        self.sent_frames
            .lock()
            .unwrap()
            .push((self.label.clone(), frame));
        Box::pin(async { Ok(()) })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct PassiveTransport;

impl Transport for PassiveTransport {
    fn connect(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
        Box::pin(async {
            Err(tqsdk_runtime_contract::ContractError::validation(
                "passive transport cannot recv",
            ))
        })
    }

    fn send(&mut self, _frame: OutboundFrame) -> ContractFuture<'_, ()> {
        Box::pin(async {
            Err(tqsdk_runtime_contract::ContractError::validation(
                "passive transport cannot send",
            ))
        })
    }

    fn close(&mut self) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Default)]
struct CapabilityConnector {
    recv_frames: Arc<Mutex<BTreeMap<String, Vec<RawFrame>>>>,
    sent_frames: Arc<Mutex<Vec<(String, OutboundFrame)>>>,
}

impl CapabilityConnector {
    fn with_recv_frames(self, route_label: impl Into<String>, frames: Vec<RawFrame>) -> Self {
        self.recv_frames
            .lock()
            .unwrap()
            .insert(route_label.into(), frames);
        self
    }

    fn sent_frames(&self) -> Vec<(String, OutboundFrame)> {
        self.sent_frames.lock().unwrap().clone()
    }
}

impl SessionRouteConnector for CapabilityConnector {
    fn connect_route<'a>(
        &'a self,
        route: &'a SessionRoute,
    ) -> ContractFuture<'a, Box<dyn Transport>> {
        let route_label = route.label.clone();
        let recv_frames = Arc::clone(&self.recv_frames);
        let sent_frames = Arc::clone(&self.sent_frames);
        Box::pin(async move {
            match &route.endpoint {
                SessionRouteEndpoint::WebSocket { .. } => {
                    let frames = recv_frames
                        .lock()
                        .unwrap()
                        .remove(&route_label)
                        .unwrap_or_default();
                    Ok(
                        Box::new(QueuedTransport::new(route_label, frames, sent_frames))
                            as Box<dyn Transport>,
                    )
                }
                SessionRouteEndpoint::Http { .. }
                | SessionRouteEndpoint::Replay { .. }
                | SessionRouteEndpoint::Internal { .. } => {
                    Ok(Box::new(PassiveTransport) as Box<dyn Transport>)
                }
            }
        })
    }
}

type SeenDispatches = Arc<Mutex<Vec<(String, Vec<OutboundDispatch>)>>>;

#[derive(Clone, Default)]
struct RecordingExecutor {
    responses: BTreeMap<String, Vec<RuntimeInput>>,
    seen: SeenDispatches,
}

impl RecordingExecutor {
    fn with_response(mut self, route_label: impl Into<String>, inputs: Vec<RuntimeInput>) -> Self {
        self.responses.insert(route_label.into(), inputs);
        self
    }

    fn seen(&self) -> Vec<(String, Vec<OutboundDispatch>)> {
        self.seen.lock().unwrap().clone()
    }
}

impl RouteRequestExecutor for RecordingExecutor {
    fn execute<'a>(
        &'a self,
        route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> ContractFuture<'a, Vec<RuntimeInput>> {
        let inputs = self
            .responses
            .get(&route.label)
            .cloned()
            .unwrap_or_default();
        let seen = Arc::clone(&self.seen);
        let route_label = route.label.clone();
        let recorded_requests = requests.clone();
        Box::pin(async move {
            seen.lock().unwrap().push((route_label, recorded_requests));
            Ok(inputs)
        })
    }
}

#[test]
fn market_multi_frame_command_keeps_origin_detail_in_runtime_snapshot() {
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
    let resolver = StaticTopologyResolver {
        topology,
        expected_domains: vec![ProtocolDomain::Market],
    };
    let connector = CapabilityConnector::default();
    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .enable_domain(ProtocolDomain::Market);
    let adapters = adapter_registry();
    let mut run = block_on(runtime.establish(
        &CapabilityAuthProvider,
        &resolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();

    let command_id = block_on(handle.submit(RuntimeCommand::Market(
        MarketCommand::SubscribeQuotes {
            symbols: vec![tqsdk_runtime_contract::Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 2);

    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "aid"
        ]),
        Some(&json!("subscribe_quote"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "symbols"
        ]),
        Some(&json!(["SHFE.au2602"]))
    );
}

#[test]
fn minimal_core_api_covers_v1_capability_matrix() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
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
            label: "trade".to_string(),
            target: SessionTarget::Account(AccountId::new("simnow")),
            domains: vec![ProtocolDomain::Trade],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: "ws://trade.example".to_string(),
                connect: Default::default(),
            },
        })
        .with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example/graphql".to_string(),
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
            target: SessionTarget::Replay(ReplaySessionId::new("rb-1")),
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
    let resolver = StaticTopologyResolver {
        topology,
        expected_domains: vec![
            ProtocolDomain::Market,
            ProtocolDomain::Trade,
            ProtocolDomain::Query,
            ProtocolDomain::Schema,
            ProtocolDomain::Replay,
            ProtocolDomain::System,
        ],
    };
    let connector = CapabilityConnector::default()
        .with_recv_frames(
            "market",
            vec![RawFrame::Text(
                json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": {
                                "last_price": 618.5,
                                "ask_price1": 619.0,
                            }
                        },
                        "charts": {
                            "chart-1": {
                                "left_id": 40,
                                "right_id": 42,
                                "more_data": false,
                            }
                        },
                        "trading_status": {
                            "SHFE.au2602": {
                                "symbol": "SHFE.au2602",
                                "trade_status": "CONTINOUS",
                            }
                        }
                    }]
                })
                .to_string(),
            )],
        )
        .with_recv_frames(
            "trade",
            vec![RawFrame::Text(
                json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            "simnow": {
                                "session": {
                                    "user_id": "simnow",
                                    "trading_day": "20260420",
                                },
                                "trade_more_data": false,
                                "orders": {
                                    "order-1": {
                                        "status": "ALIVE",
                                        "volume_left": 2,
                                    }
                                },
                                "pre_insert_orders": {
                                    "pre-1": {
                                        "exchange_id": "SHFE",
                                        "instrument_id": "au2602",
                                        "direction": "BUY",
                                        "pre_margin": 1234.5,
                                    }
                                },
                                "positions": {
                                    "SHFE.au2602": {
                                        "pos": 2,
                                    }
                                },
                                "trades": {
                                    "trade-1": {
                                        "order_id": "order-1",
                                        "trade_price": 618.5,
                                    }
                                }
                            }
                        }
                    }]
                })
                .to_string(),
            )],
        );
    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .enable_domain(ProtocolDomain::Market)
        .enable_domain(ProtocolDomain::Trade)
        .enable_domain(ProtocolDomain::Query)
        .enable_domain(ProtocolDomain::Schema)
        .enable_domain(ProtocolDomain::Replay)
        .enable_domain(ProtocolDomain::System);
    let adapters = adapter_registry();
    let mut run = block_on(runtime.establish(
        &CapabilityAuthProvider,
        &resolver,
        &connector,
        &config,
        &adapters,
    ))
    .unwrap();
    let executor = RecordingExecutor::default()
        .with_response(
            "query",
            vec![RuntimeInput::Io(IoEvent {
                route: "query".to_string(),
                domains: vec![ProtocolDomain::Query],
                payload: InputPayload::Json(json!({
                    "query_id": "quotes-page-1",
                    "data": {
                        "items": [{"instrument_id": "au2602"}],
                        "has_more": false,
                    },
                    "errors": [],
                })),
            })],
        )
        .with_response(
            "schema",
            vec![RuntimeInput::Io(IoEvent {
                route: "schema".to_string(),
                domains: vec![ProtocolDomain::Schema],
                payload: InputPayload::Json(json!({
                    "nodes": {
                        "quote": {
                            "fields": ["last_price", "ask_price1"],
                        }
                    }
                })),
            })],
        )
        .with_response(
            "replay",
            vec![RuntimeInput::Replay(ReplayEvent {
                label: "step",
                session_id: Some(ReplaySessionId::new("rb-1")),
                payload: Some(json!({
                    "cursor": {
                        "seq": 1,
                        "state": "stepped",
                    }
                })),
            })],
        )
        .with_response(
            "system",
            vec![RuntimeInput::Auth(AuthEvent {
                label: "refreshed",
                payload: Some(json!({
                    "auth_id": "auth-2",
                    "features": ["trade", "query", "schema"],
                })),
            })],
        );

    let market_id = block_on(handle.submit(RuntimeCommand::Market(
        MarketCommand::SubscribeQuotes {
            symbols: vec![tqsdk_runtime_contract::Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();
    let trade_login_id = block_on(handle.submit(RuntimeCommand::Trade(TradeCommand::Login(
        TradeLoginCommand {
            account_id: AccountId::new("simnow"),
            broker_id: "9999".to_string(),
            password: "secret".to_string(),
            account_type: TradeAccountType::Future,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        },
    ))))
    .unwrap();
    let trade_insert_id = block_on(handle.submit(RuntimeCommand::Trade(
        TradeCommand::InsertOrder(TradeInsertOrderCommand {
            account_id: AccountId::new("simnow"),
            order_id: OrderId::new("order-1"),
            symbol: tqsdk_runtime_contract::Symbol::new("SHFE.au2602"),
            direction: TradeDirection::Buy,
            offset: Some(TradeOffset::Open),
            volume: 2,
            price_type: TradePriceType::Limit,
            limit_price: Some(json!(618.5)),
            time_condition: TradeTimeCondition::Gfd,
            volume_condition: TradeVolumeCondition::Any,
        }),
    )))
    .unwrap();
    let trade_pre_insert_id = block_on(handle.submit(RuntimeCommand::Trade(
        TradeCommand::PreInsertOrder(TradePreInsertOrderCommand {
            account_id: AccountId::new("simnow"),
            order_id: OrderId::new("pre-1"),
            symbol: tqsdk_runtime_contract::Symbol::new("SHFE.au2602"),
            direction: TradeDirection::Buy,
            offset: Some(TradeOffset::Open),
            volume: 1,
            price_type: TradePriceType::Limit,
            limit_price: Some(json!(0.0)),
            time_condition: TradeTimeCondition::Gfd,
            volume_condition: TradeVolumeCondition::Any,
            hedge_flag: "SPECULATION".to_string(),
            contingent_condition: "IMMEDIATELY".to_string(),
        }),
    )))
    .unwrap();
    let query_id = block_on(handle.submit(RuntimeCommand::Query(QueryCommand::Fetch {
        query_id: QueryId::new("quotes-page-1"),
        query: "query Quotes { symbols { instrument_id } }".to_string(),
        variables: None,
    })))
    .unwrap();
    let schema_id = block_on(
        handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("instrument-schema"),
            path: "/schema/instrument.json".to_string(),
        })),
    )
    .unwrap();
    let replay_id = block_on(handle.submit(RuntimeCommand::Replay(ReplayCommand::Step))).unwrap();
    let system_id =
        block_on(handle.submit(RuntimeCommand::System(SystemCommand::RefreshAuth))).unwrap();

    let receipts = block_on(runtime.flush_outbound(&mut run)).unwrap();
    assert_eq!(receipts.len(), 9);

    let market_commit = block_on(runtime.recv_route_and_ingest(
        &mut run,
        "market",
        vec![market_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(market_commit.scope, CommitScope::RealtimeUpdate);

    let trade_commit = block_on(runtime.recv_route_and_ingest(
        &mut run,
        "trade",
        vec![trade_login_id, trade_insert_id, trade_pre_insert_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(trade_commit.scope, CommitScope::RealtimeUpdate);

    let query_outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        "query",
        &executor,
        vec![query_id],
        CommitScope::QueryRefresh,
    ))
    .unwrap();
    assert_eq!(query_outcome.commits.len(), 1);

    let schema_outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        "schema",
        &executor,
        vec![schema_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap();
    assert_eq!(schema_outcome.commits.len(), 1);

    let replay_outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        "replay",
        &executor,
        vec![replay_id],
        CommitScope::ReplayStep,
    ))
    .unwrap();
    assert_eq!(replay_outcome.commits.len(), 1);

    let system_outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        "system",
        &executor,
        vec![system_id],
        CommitScope::SessionTransition,
    ))
    .unwrap();
    assert_eq!(system_outcome.commits.len(), 1);

    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "auth", "context", "auth_id"]),
        Some(&json!("auth-v1"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "session", "topology", "routes"]),
        Some(&json!([
            {
                "label": "market",
                "target": {"kind": "shared"},
                "domains": ["market"],
                "endpoint": {"kind": "websocket", "url": "ws://market.example"},
            },
            {
                "label": "trade",
                "target": {"kind": "account", "account_id": "simnow"},
                "domains": ["trade"],
                "endpoint": {"kind": "websocket", "url": "ws://trade.example"},
            },
            {
                "label": "query",
                "target": {"kind": "shared"},
                "domains": ["query"],
                "endpoint": {"kind": "http", "url": "https://query.example/graphql"},
            },
            {
                "label": "schema",
                "target": {"kind": "shared"},
                "domains": ["schema"],
                "endpoint": {"kind": "http", "url": "https://schema.example"},
            },
            {
                "label": "replay",
                "target": {"kind": "replay", "session_id": "rb-1"},
                "domains": ["replay"],
                "endpoint": {"kind": "replay", "label": "replay-driver"},
            },
            {
                "label": "system",
                "target": {"kind": "shared"},
                "domains": ["system"],
                "endpoint": {"kind": "internal", "label": "system-driver"},
            }
        ]))
    );

    assert_eq!(
        handle
            .latest_snapshot()
            .get(["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(618.5))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["charts", "chart-1", "more_data"]),
        Some(&json!(false))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["trading_status", "SHFE.au2602", "trade_status"]),
        Some(&json!("CONTINOUS"))
    );

    let market_segment = market_id.get().to_string();
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            market_segment.as_str(),
            "detail",
            "aid"
        ]),
        Some(&json!("subscribe_quote"))
    );

    let login_segment = trade_login_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", login_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            login_segment.as_str(),
            "detail",
            "trade_more_data"
        ]),
        Some(&json!(false))
    );

    let insert_segment = trade_insert_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", insert_segment.as_str(), "status"]),
        Some(&json!("acked"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            insert_segment.as_str(),
            "detail",
            "order_status"
        ]),
        Some(&json!("ALIVE"))
    );

    let pre_insert_segment = trade_pre_insert_id.get().to_string();
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            pre_insert_segment.as_str(),
            "status"
        ]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            pre_insert_segment.as_str(),
            "detail",
            "pre_margin"
        ]),
        Some(&json!(1234.5))
    );

    let query_segment = query_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", query_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["query", "quotes-page-1", "items"]),
        Some(&json!([{ "instrument_id": "au2602" }]))
    );

    let schema_segment = schema_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", schema_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["schema", "instrument-schema", "nodes", "quote", "fields"]),
        Some(&json!(["last_price", "ask_price1"]))
    );

    let replay_segment = replay_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", replay_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["replay", "rb-1", "cursor", "seq"]),
        Some(&json!(1))
    );

    let system_segment = system_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", system_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "auth", "refreshed", "auth_id"]),
        Some(&json!("auth-2"))
    );

    assert_eq!(executor.seen().len(), 4);
    assert_eq!(
        connector.sent_frames().len(),
        5,
        "market sends subscribe+peek, trade sends login+insert+pre-insert",
    );

    let mut cursor = handle.cursor_from(tqsdk_runtime_contract::Revision::new(1));
    let mut observed_paths = Vec::new();
    while let Some(commit) = log.next(&mut cursor) {
        observed_paths.extend(
            commit
                .changes
                .path_hits
                .iter()
                .map(|path| path.segments().join("/")),
        );
    }

    assert!(
        observed_paths
            .iter()
            .any(|path| path == "system/session/topology")
    );
    assert!(
        observed_paths
            .iter()
            .any(|path| path == "quotes/SHFE.au2602")
    );
    assert!(
        observed_paths
            .iter()
            .any(|path| path == "trade/simnow/orders/order-1")
    );
    assert!(
        observed_paths
            .iter()
            .any(|path| path == "trade/simnow/pre_insert_orders/pre-1")
    );
    assert!(
        observed_paths
            .iter()
            .any(|path| path == "query/quotes-page-1")
    );
    assert!(
        observed_paths
            .iter()
            .any(|path| path == "schema/instrument-schema/nodes/quote")
    );
    assert!(
        observed_paths
            .iter()
            .any(|path| path == "replay/rb-1/cursor")
    );
    assert!(
        observed_paths
            .iter()
            .any(|path| path == "system/auth/refreshed")
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
