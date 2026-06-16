use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::{Duration, Instant};
#[cfg(feature = "tq-auth")]
use tqsdk_core::TradeAccountType;
use tqsdk_core::internal::{DynRouteConnectFuture, DynTransport, SessionBootstrap};
use tqsdk_core::internal::{RouteRequestExecutor, SessionRuntime};
use tqsdk_core::{
    AccountId, AdapterRegistry, AuthContext, AuthId, AuthProvider, CommandId, CommandStatus,
    CommitScope, EndpointConfig, FieldMutation, InputPayload, IoEvent, MarketCommand,
    MutationSource, NormalizedMutation, ObjectKey, OrderId, OutboundDispatch, OutboundFrame,
    OutboundRequest, ProtocolAdapter, ProtocolDomain, QueryCommand, QueryId, RawFrame,
    ReplaySessionId, Result as CoreResult, Runtime, RuntimeCommand, RuntimeHandle, RuntimeInput,
    SessionConfig, SessionRoute, SessionRouteConnector, SessionRouteEndpoint, SessionTarget,
    SessionTopology, SessionTopologyResolver, StatePath, Symbol, TradeCommand, TradeDirection,
    TradeInsertOrderCommand, TradeOffset, TradePriceType, TradeTimeCondition, TradeVolumeCondition,
    Transport,
};

#[cfg(feature = "live")]
use super::SessionClientContext;
use super::{
    SessionClient, SessionInternalExecutor, SessionIoComponents, SessionIoState, SessionProgress,
    SessionReplayExecutor, SharedAuthProvider, SharedRouteConnector, SharedRouteExecutor,
    SharedTopologyResolver, market_interest::MarketInterestRegistry,
};
use crate::testing::ManualSession;
#[derive(Clone, Default)]
struct TestAuthProvider {
    auth_id: Option<String>,
    features: Vec<String>,
}

impl TestAuthProvider {
    fn with_features(features: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            auth_id: None,
            features: features.into_iter().map(str::to_string).collect(),
        }
    }

    #[cfg(feature = "tq-auth")]
    fn with_auth_id(auth_id: impl Into<String>) -> Self {
        Self {
            auth_id: Some(auth_id.into()),
            features: Vec::new(),
        }
    }
}

impl AuthProvider for TestAuthProvider {
    fn authenticate(&self) -> impl Future<Output = CoreResult<AuthContext>> + Send + '_ {
        let mut auth = AuthContext::new("test-token");
        if let Some(auth_id) = &self.auth_id {
            auth = auth.with_auth_id(AuthId::new(auth_id.clone()));
        }
        for feature in &self.features {
            auth = auth.with_feature(feature.clone());
        }
        async move { Ok(auth) }
    }
}

#[derive(Clone)]
struct StaticTopologyResolver {
    topology: SessionTopology,
}

impl SessionTopologyResolver for StaticTopologyResolver {
    fn resolve_topology<'a>(
        &'a self,
        _auth: &'a AuthContext,
        _config: &'a SessionConfig,
        _enabled_domains: &'a [ProtocolDomain],
    ) -> Pin<Box<dyn Future<Output = CoreResult<SessionTopology>> + Send + 'a>> {
        let topology = self.topology.clone();
        Box::pin(async move { Ok(topology) })
    }
}

#[derive(Default, Clone)]
struct QueueTransport {
    sent: Arc<Mutex<Vec<OutboundFrame>>>,
    recv_queue: Arc<Mutex<VecDeque<RawFrame>>>,
}

impl QueueTransport {
    fn with_frame(frame: RawFrame) -> Self {
        let transport = Self::default();
        transport.recv_queue.lock().unwrap().push_back(frame);
        transport
    }
}

impl Transport for QueueTransport {
    fn connect(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }

    fn recv(&mut self) -> impl Future<Output = CoreResult<RawFrame>> + Send + '_ {
        let recv_queue = Arc::clone(&self.recv_queue);
        async move {
            let frame = recv_queue
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(RawFrame::Pong);
            Ok(frame)
        }
    }

    fn send(&mut self, frame: OutboundFrame) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        let sent = Arc::clone(&self.sent);
        async move {
            sent.lock().unwrap().push(frame);
            Ok(())
        }
    }

    fn close(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }
}

#[derive(Clone)]
struct QueueConnector {
    transport: QueueTransport,
}

impl SessionRouteConnector for QueueConnector {
    fn connect_route<'a>(&'a self, _route: &'a SessionRoute) -> DynRouteConnectFuture<'a> {
        let transport = self.transport.clone();
        Box::pin(async move { Ok(Box::new(transport) as Box<dyn DynTransport>) })
    }
}

#[derive(Clone, Default)]
struct QueryResultTransport {
    sent: Arc<Mutex<Vec<OutboundFrame>>>,
    emit_ping_first: bool,
    emitted_ping: Arc<Mutex<bool>>,
    emitted_result: Arc<Mutex<bool>>,
}

impl QueryResultTransport {
    fn new(emit_ping_first: bool) -> Self {
        Self {
            emit_ping_first,
            ..Self::default()
        }
    }
}

impl Transport for QueryResultTransport {
    fn connect(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }

    fn recv(&mut self) -> impl Future<Output = CoreResult<RawFrame>> + Send + '_ {
        let sent = Arc::clone(&self.sent);
        let emitted_ping = Arc::clone(&self.emitted_ping);
        let emitted_result = Arc::clone(&self.emitted_result);
        let emit_ping_first = self.emit_ping_first;
        async move {
            if emit_ping_first && !*emitted_ping.lock().unwrap() {
                *emitted_ping.lock().unwrap() = true;
                return Ok(RawFrame::Ping);
            }

            if !*emitted_result.lock().unwrap() {
                let Some(query_id) = sent.lock().unwrap().iter().find_map(outbound_query_id) else {
                    return Ok(RawFrame::Pong);
                };
                *emitted_result.lock().unwrap() = true;
                return Ok(RawFrame::Text(
                    json!({
                        "aid": "rtn_data",
                        "data": [{
                            "symbols": {
                                query_id: {
                                    "result": {
                                        "quotes": ["SHFE.au2602"]
                                    }
                                }
                            }
                        }]
                    })
                    .to_string(),
                ));
            }

            Ok(RawFrame::Pong)
        }
    }

    fn send(&mut self, frame: OutboundFrame) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        let sent = Arc::clone(&self.sent);
        async move {
            sent.lock().unwrap().push(frame);
            Ok(())
        }
    }

    fn close(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }
}

#[derive(Clone)]
struct QueryResultConnector {
    transport: QueryResultTransport,
}

impl SessionRouteConnector for QueryResultConnector {
    fn connect_route<'a>(&'a self, _route: &'a SessionRoute) -> DynRouteConnectFuture<'a> {
        let transport = self.transport.clone();
        Box::pin(async move { Ok(Box::new(transport) as Box<dyn DynTransport>) })
    }
}

#[derive(Clone, Default)]
struct RecordingExecutor {
    responses: Arc<Mutex<BTreeMap<String, Vec<RuntimeInput>>>>,
    query_values: Arc<Mutex<BTreeMap<String, Value>>>,
}

impl RecordingExecutor {
    fn with_response(self, route_label: impl Into<String>, inputs: Vec<RuntimeInput>) -> Self {
        self.responses
            .lock()
            .unwrap()
            .insert(route_label.into(), inputs);
        self
    }

    fn with_query_value(self, route_label: impl Into<String>, value: Value) -> Self {
        self.query_values
            .lock()
            .unwrap()
            .insert(route_label.into(), value);
        self
    }
}

impl RouteRequestExecutor for RecordingExecutor {
    fn execute<'a>(
        &'a self,
        route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> Pin<Box<dyn Future<Output = CoreResult<Vec<RuntimeInput>>> + Send + 'a>> {
        let fixed_inputs = self.responses.lock().unwrap().get(&route.label).cloned();
        let query_value = self.query_values.lock().unwrap().get(&route.label).cloned();
        let inputs = fixed_inputs
            .or_else(|| build_query_inputs(route, &requests, query_value))
            .unwrap_or_default();
        Box::pin(async move { Ok(inputs) })
    }
}

fn build_query_inputs(
    route: &SessionRoute,
    requests: &[OutboundDispatch],
    value: Option<Value>,
) -> Option<Vec<RuntimeInput>> {
    let value = value?;
    let query_id = requests
        .iter()
        .find_map(|dispatch| match &dispatch.request {
            OutboundRequest::Http(request) => request
                .body
                .as_ref()
                .and_then(|body| body.get("query_id"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            OutboundRequest::Query(request) => Some(request.query_id.as_str().to_string()),
            OutboundRequest::Transport(_)
            | OutboundRequest::Internal(_)
            | OutboundRequest::Replay(_) => None,
        })?;
    Some(vec![RuntimeInput::Io(IoEvent {
        route: route.label.clone(),
        domains: route.domains.clone(),
        payload: InputPayload::Json(json!({
            "query_id": query_id,
            "data": value,
        })),
    })])
}

fn outbound_query_id(frame: &OutboundFrame) -> Option<String> {
    let text = match frame {
        OutboundFrame::Text(text) => text,
        OutboundFrame::Binary(_) | OutboundFrame::Ping | OutboundFrame::Close => return None,
    };
    serde_json::from_str::<Value>(text)
        .ok()?
        .get("query_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

#[tokio::test(flavor = "current_thread")]
async fn live_client_drive_route_once_establishes_market_session_and_ingests_quote() {
    let handle = runtime_with_default_adapters();
    let transport = QueueTransport::with_frame(RawFrame::Text(
        json!({
            "aid": "rtn_data",
            "data": [{
                "quotes": {
                    "SHFE.au2602": {
                        "instrument_id": "au2602",
                        "last_price": 618.5
                    }
                }
            }]
        })
        .to_string(),
    ));
    let sent = Arc::clone(&transport.sent);
    let client = test_live_client(
        handle.clone(),
        SessionTopology::default().with_route(SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Market],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: "wss://market.example".to_string(),
                connect: tqsdk_core::WebSocketConnectOptions::default(),
            },
        }),
        transport,
        Arc::new(RecordingExecutor::default()),
    );

    client
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![tqsdk_core::Symbol::new("SHFE.au2602")],
        }))
        .await
        .unwrap();

    assert!(
        client
            .drive_route_once(Some(Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap()
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(618.5))
    );
    assert!(
        sent.lock()
            .unwrap()
            .iter()
            .any(|frame| matches!(frame, OutboundFrame::Text(text) if text == super::PEEK_MESSAGE))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn live_client_drive_pending_once_executes_http_routes() {
    let handle = runtime_with_default_adapters();
    let executor: SharedRouteExecutor = Arc::new(RecordingExecutor::default().with_response(
        "query",
        vec![RuntimeInput::Io(IoEvent {
            route: "query".to_string(),
            domains: vec![ProtocolDomain::Query],
            payload: InputPayload::Json(json!({
                "query_id": "query-1",
                "data": { "quotes": ["SHFE.au2602"] }
            })),
        })],
    ));
    let client = test_live_client(
        handle.clone(),
        SessionTopology::default().with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example".to_string(),
            },
        }),
        QueueTransport::default(),
        executor,
    );

    handle
        .submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: QueryId::new("query-1"),
            query: "query { quotes }".to_string(),
            variables: None,
        }))
        .await
        .unwrap();

    assert!(client.flush_outbound().await.unwrap());
    assert!(client.drive_pending_once().await.unwrap());
    assert_eq!(
        handle.latest_snapshot().get(["query", "query-1", "quotes"]),
        Some(&json!(["SHFE.au2602"]))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn live_client_progress_once_reports_flush_then_pending_for_http_query() {
    let handle = runtime_with_default_adapters();
    let executor: SharedRouteExecutor = Arc::new(RecordingExecutor::default().with_response(
        "query",
        vec![RuntimeInput::Io(IoEvent {
            route: "query".to_string(),
            domains: vec![ProtocolDomain::Query],
            payload: InputPayload::Json(json!({
                "query_id": "query-1",
                "data": { "quotes": ["SHFE.au2602"] }
            })),
        })],
    ));
    let client = test_live_client(
        handle.clone(),
        SessionTopology::default().with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example".to_string(),
            },
        }),
        QueueTransport::default(),
        executor,
    );

    handle
        .submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: QueryId::new("query-1"),
            query: "query { quotes }".to_string(),
            variables: None,
        }))
        .await
        .unwrap();

    assert_eq!(
        client.progress_once(None).await.unwrap(),
        SessionProgress::FlushedOutbound
    );
    assert_eq!(
        client.progress_once(None).await.unwrap(),
        SessionProgress::DrovePending
    );
    assert_eq!(
        handle.latest_snapshot().get(["query", "query-1", "quotes"]),
        Some(&json!(["SHFE.au2602"]))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn live_client_progress_once_reports_route_progress_for_websocket_input() {
    let handle = runtime_with_default_adapters();
    let client = test_live_client(
        handle.clone(),
        SessionTopology::default().with_route(SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Market],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: "wss://market.example".to_string(),
                connect: tqsdk_core::WebSocketConnectOptions::default(),
            },
        }),
        QueueTransport::with_frame(RawFrame::Text(
            json!({
                "aid": "rtn_data",
                "data": [{
                    "quotes": {
                        "SHFE.au2602": {
                            "instrument_id": "au2602",
                            "last_price": 618.5
                        }
                    }
                }]
            })
            .to_string(),
        )),
        Arc::new(RecordingExecutor::default()),
    );

    assert_eq!(
        client
            .progress_once(Some(Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap(),
        SessionProgress::DroveRoute
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(618.5))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn live_client_progress_once_reports_idle_when_no_route_has_work() {
    let client = test_live_client(
        runtime_with_default_adapters(),
        SessionTopology::default().with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example".to_string(),
            },
        }),
        QueueTransport::default(),
        Arc::new(RecordingExecutor::default()),
    );

    assert_eq!(
        client.progress_once(None).await.unwrap(),
        SessionProgress::Idle
    );
}

#[tokio::test(flavor = "current_thread")]
async fn wait_command_completed_drives_http_query_command_to_completion() {
    let handle = runtime_with_default_adapters();
    let executor: SharedRouteExecutor = Arc::new(
        RecordingExecutor::default()
            .with_query_value("query", json!({ "quotes": ["SHFE.au2602"] })),
    );
    let client = test_live_client(
        handle.clone(),
        SessionTopology::default().with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example".to_string(),
            },
        }),
        QueueTransport::default(),
        executor,
    );

    let command_id = client
        .submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: QueryId::new("query-1"),
            query: "query { quotes }".to_string(),
            variables: None,
        }))
        .await
        .unwrap();

    client.wait_command_completed(command_id).await.unwrap();

    assert_eq!(
        client.command_status(command_id).unwrap(),
        Some("completed".to_string())
    );
    assert_eq!(
        client.command_status_typed(command_id).unwrap(),
        Some(CommandStatus::Completed)
    );
    assert_eq!(
        handle.latest_snapshot().get(["query", "query-1", "quotes"]),
        Some(&json!(["SHFE.au2602"]))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn wait_command_completed_accepts_insert_order_ack() {
    let handle = runtime_with_default_adapters();
    let client = ManualSession::from_runtime(handle.clone()).into_client();
    for (index, offset) in [
        TradeOffset::Open,
        TradeOffset::Close,
        TradeOffset::CloseToday,
    ]
    .into_iter()
    .enumerate()
    {
        let command_id = handle
            .submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
                TradeInsertOrderCommand {
                    account_id: AccountId::new("sim"),
                    order_id: OrderId::new(format!("order-{index}")),
                    symbol: Symbol::new("SHFE.ao2609"),
                    direction: TradeDirection::Buy,
                    offset: Some(offset),
                    volume: 1,
                    price_type: TradePriceType::Limit,
                    limit_price: Some(json!(2800.0)),
                    time_condition: TradeTimeCondition::Gfd,
                    volume_condition: TradeVolumeCondition::Any,
                },
            )))
            .await
            .unwrap();

        handle
            .record_command_status(
                command_id,
                CommandStatus::Sent,
                Some(json!({"aid": "insert_order"})),
                CommitScope::RealtimeUpdate,
            )
            .unwrap();
        handle
            .record_command_status(
                command_id,
                CommandStatus::Acked,
                Some(json!({"aid": "insert_order"})),
                CommitScope::RealtimeUpdate,
            )
            .unwrap();

        client.wait_command_completed(command_id).await.unwrap();
    }
}

#[test]
fn command_status_typed_rejects_unknown_status_strings() {
    let mut registry = AdapterRegistry::new();
    registry.register_adapter(CommandStatusFixtureAdapter {
        command_id: CommandId::new(99),
        status: "mystery".to_string(),
    });
    let handle = RuntimeHandle::with_adapters(registry);
    let client = ManualSession::from_runtime(handle.clone()).into_client();

    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "command-status-fixture".to_string(),
                domains: vec![ProtocolDomain::System],
                payload: InputPayload::Json(json!({})),
            }),
            vec![],
            CommitScope::SessionTransition,
        )
        .unwrap()
        .expect("fixture command status mutation should publish a commit");

    assert_eq!(
        client.command_status(CommandId::new(99)).unwrap(),
        Some("mystery".to_string())
    );
    let error = client
        .command_status_typed(CommandId::new(99))
        .expect_err("unknown status must be rejected");
    assert_eq!(
        error.to_string(),
        "invalid session facade state: unknown command status"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn wait_command_completed_errors_when_command_cannot_reach_terminal_state() {
    let handle = runtime_with_default_adapters();
    let client = ManualSession::from_runtime(handle.clone()).into_client();
    let command_id = handle
        .submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: QueryId::new("query-1"),
            query: "query { quotes }".to_string(),
            variables: None,
        }))
        .await
        .unwrap();

    let error = client.wait_command_completed(command_id).await.unwrap_err();

    assert_eq!(
        error.to_string(),
        "invalid session facade state: command did not reach a terminal state"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn query_graphql_value_drives_query_route_and_returns_state_value() {
    let handle = runtime_with_default_adapters();
    let executor: SharedRouteExecutor = Arc::new(
        RecordingExecutor::default()
            .with_query_value("query", json!({ "quotes": ["SHFE.au2602"] })),
    );
    let client = test_live_client(
        handle,
        SessionTopology::default().with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example".to_string(),
            },
        }),
        QueueTransport::default(),
        executor,
    );

    let value = client
        .query_graphql_value("query { quotes }", None)
        .await
        .unwrap();

    assert_eq!(value, json!({ "quotes": ["SHFE.au2602"] }));
}

#[tokio::test(flavor = "current_thread")]
async fn query_graphql_value_serializes_concurrent_value_queries() {
    let handle = runtime_with_default_adapters();
    let executor: SharedRouteExecutor = Arc::new(
        RecordingExecutor::default()
            .with_query_value("query", json!({ "quotes": ["SHFE.au2602"] })),
    );
    let client = test_live_client(
        handle,
        SessionTopology::default().with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example".to_string(),
            },
        }),
        QueueTransport::default(),
        executor,
    );

    let left = client.clone();
    let right = client.clone();
    let (left_value, right_value) = tokio::join!(
        left.query_graphql_value("query { left }", None),
        right.query_graphql_value("query { right }", None)
    );

    assert_eq!(left_value.unwrap(), json!({ "quotes": ["SHFE.au2602"] }));
    assert_eq!(right_value.unwrap(), json!({ "quotes": ["SHFE.au2602"] }));
}

#[test]
fn cloned_session_clients_share_direct_query_lock() {
    let client = ManualSession::from_runtime(runtime_with_default_adapters()).into_client();
    let cloned = client.clone();

    assert!(Arc::ptr_eq(&client.query_lock, &cloned.query_lock));
}

#[tokio::test(flavor = "current_thread")]
async fn query_graphql_value_works_over_market_websocket_when_query_is_cohosted() {
    let handle = runtime_with_default_adapters();
    let client = test_live_client_with_components(
        handle,
        SessionTopology::default().with_route(SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Market, ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: "wss://market.example".to_string(),
                connect: tqsdk_core::WebSocketConnectOptions::default(),
            },
        }),
        SessionIoComponents {
            auth_provider: Arc::new(TestAuthProvider::default()),
            topology_resolver: Arc::new(StaticTopologyResolver {
                topology: SessionTopology::default().with_route(SessionRoute {
                    label: "market".to_string(),
                    target: SessionTarget::Shared,
                    domains: vec![ProtocolDomain::Market, ProtocolDomain::Query],
                    endpoint: SessionRouteEndpoint::WebSocket {
                        url: "wss://market.example".to_string(),
                        connect: tqsdk_core::WebSocketConnectOptions::default(),
                    },
                }),
            }),
            route_connector: Arc::new(QueryResultConnector {
                transport: QueryResultTransport::new(false),
            }),
            http_executor: Arc::new(RecordingExecutor::default()),
            internal_executor: Arc::new(SessionInternalExecutor::new(Arc::new(
                TestAuthProvider::default(),
            ))),
            replay_executor: Arc::new(SessionReplayExecutor),
        },
    );

    let value = client
        .query_graphql_value("query { quotes }", None)
        .await
        .unwrap();

    assert_eq!(value, json!({ "result": { "quotes": ["SHFE.au2602"] } }));
}

#[tokio::test(flavor = "current_thread")]
async fn query_graphql_value_tolerates_server_ping_before_query_result() {
    let handle = runtime_with_default_adapters();
    let client = test_live_client_with_components(
        handle,
        SessionTopology::default().with_route(SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Market, ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: "wss://market.example".to_string(),
                connect: tqsdk_core::WebSocketConnectOptions::default(),
            },
        }),
        SessionIoComponents {
            auth_provider: Arc::new(TestAuthProvider::default()),
            topology_resolver: Arc::new(StaticTopologyResolver {
                topology: SessionTopology::default().with_route(SessionRoute {
                    label: "market".to_string(),
                    target: SessionTarget::Shared,
                    domains: vec![ProtocolDomain::Market, ProtocolDomain::Query],
                    endpoint: SessionRouteEndpoint::WebSocket {
                        url: "wss://market.example".to_string(),
                        connect: tqsdk_core::WebSocketConnectOptions::default(),
                    },
                }),
            }),
            route_connector: Arc::new(QueryResultConnector {
                transport: QueryResultTransport::new(true),
            }),
            http_executor: Arc::new(RecordingExecutor::default()),
            internal_executor: Arc::new(SessionInternalExecutor::new(Arc::new(
                TestAuthProvider::default(),
            ))),
            replay_executor: Arc::new(SessionReplayExecutor),
        },
    );

    let value = client
        .query_graphql_value("query { quotes }", None)
        .await
        .unwrap();

    assert_eq!(value, json!({ "result": { "quotes": ["SHFE.au2602"] } }));
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_schema_value_drives_schema_route_and_returns_state_value() {
    let handle = runtime_with_default_adapters();
    let executor: SharedRouteExecutor = Arc::new(RecordingExecutor::default().with_response(
        "schema",
        vec![RuntimeInput::Io(IoEvent {
            route: "schema".to_string(),
            domains: vec![ProtocolDomain::Schema],
            payload: InputPayload::Json(json!({
                "schema_id": "instrument-schema",
                "data": {
                    "nodes": {
                        "quote": {
                            "fields": ["last_price", "ask_price1"]
                        }
                    }
                }
            })),
        })],
    ));
    let client = test_live_client(
        handle,
        SessionTopology::default().with_route(SessionRoute {
            label: "schema".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Schema],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://schema.example".to_string(),
            },
        }),
        QueueTransport::default(),
        executor,
    );

    let value = client
        .refresh_schema_value("instrument-schema", "/schema/instrument.json")
        .await
        .unwrap();

    assert_eq!(
        value,
        json!({
            "nodes": {
                "quote": {
                    "fields": ["last_price", "ask_price1"]
                }
            }
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_schema_value_waits_for_fresh_command_completion_instead_of_returning_cache() {
    let handle = runtime_with_default_adapters();
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "schema".to_string(),
                domains: vec![ProtocolDomain::Schema],
                payload: InputPayload::Json(json!({
                    "schema_id": "instrument-schema",
                    "data": { "version": 1 }
                })),
            }),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )
        .unwrap();
    let executor: SharedRouteExecutor = Arc::new(RecordingExecutor::default().with_response(
        "schema",
        vec![RuntimeInput::Io(IoEvent {
            route: "schema".to_string(),
            domains: vec![ProtocolDomain::Schema],
            payload: InputPayload::Json(json!({
                "schema_id": "instrument-schema",
                "data": { "version": 2 }
            })),
        })],
    ));
    let client = test_live_client(
        handle,
        SessionTopology::default().with_route(SessionRoute {
            label: "schema".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Schema],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://schema.example".to_string(),
            },
        }),
        QueueTransport::default(),
        executor,
    );

    let value = client
        .refresh_schema_value("instrument-schema", "/schema/instrument.json")
        .await
        .unwrap();

    assert_eq!(value, json!({ "version": 2 }));
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_auth_value_drives_system_route_and_returns_auth_payload() {
    let handle = runtime_with_default_adapters();
    let client = test_live_client(
        handle,
        SessionTopology::default().with_route(SessionRoute {
            label: "system".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System],
            endpoint: SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            },
        }),
        QueueTransport::default(),
        Arc::new(RecordingExecutor::default()),
    );

    let value = client.refresh_auth_value().await.unwrap();

    assert_eq!(value.get("access_token"), Some(&json!("test-token")));
}

#[tokio::test(flavor = "current_thread")]
async fn replay_value_helpers_drive_replay_route_and_return_current_state() {
    let handle = runtime_with_default_adapters();
    let client = test_live_client(
        handle,
        SessionTopology::default().with_route(SessionRoute {
            label: "replay".to_string(),
            target: SessionTarget::Replay(ReplaySessionId::new("rb-test")),
            domains: vec![ProtocolDomain::Replay],
            endpoint: SessionRouteEndpoint::Replay {
                label: "rb-test".to_string(),
            },
        }),
        QueueTransport::default(),
        Arc::new(RecordingExecutor::default()),
    );

    let stepped = client.replay_step_value("rb-test").await.unwrap();
    assert_eq!(stepped, json!({ "state": "stepped" }));

    let reset = client.replay_reset_value("rb-test").await.unwrap();
    assert_eq!(reset, json!({ "state": "reset" }));
}

#[tokio::test(flavor = "current_thread")]
async fn query_value_helper_requires_enabled_query_route() {
    let client = ManualSession::from_runtime(runtime_with_default_adapters()).into_client();

    let error = client
        .query_graphql_value("query { ping }", None)
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "invalid session facade state: query value helper requires an enabled query route"
    );
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "live")]
async fn query_value_helper_rejects_non_stock_websocket_query_without_http_override() {
    let client = crate::builder::SessionClientBuilder::new("demo-user", "demo-pass")
        .futures_market()
        .enable_query()
        .build()
        .expect("builder should construct a thin session client");

    let error = client
        .query_graphql_value("query { ping }", None)
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "invalid session facade state: websocket query helpers require stock market_target when query_url is not configured"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn replay_value_helpers_require_explicit_replay_route() {
    let client = ManualSession::from_runtime(runtime_with_default_adapters()).into_client();

    let error = client.replay_step_value("rb-test").await.unwrap_err();

    assert_eq!(
        error.to_string(),
        "invalid session facade state: replay value helper requires an enabled replay route"
    );
}

#[test]
#[cfg(feature = "live")]
fn built_client_retains_builder_auth_and_endpoints() {
    let client = crate::builder::SessionClientBuilder::new("demo-user", "demo-pass")
        .query_url("https://query.example.com/graphql")
        .schema_url("https://schema.example.com/latest.json")
        .replay_url("wss://replay.example.com/feed")
        .build()
        .expect("builder should construct a thin session client");

    assert_eq!(client.auth_user(), "demo-user");
    assert_eq!(client.auth_pass(), "demo-pass");
    assert_eq!(
        client.endpoints().query_url.as_deref(),
        Some("https://query.example.com/graphql")
    );
    assert_eq!(
        client.endpoints().schema_url.as_deref(),
        Some("https://schema.example.com/latest.json")
    );
    assert_eq!(
        client.endpoints().replay_url.as_deref(),
        Some("wss://replay.example.com/feed")
    );
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "live")]
async fn built_client_enable_query_enables_query_domain_without_query_url() {
    let client = crate::builder::SessionClientBuilder::new("demo-user", "demo-pass")
        .enable_query()
        .build()
        .expect("builder should enable live query domain without requiring query_url");

    assert_eq!(client.endpoints().query_url, None);

    let io = client
        .io
        .as_ref()
        .expect("live client should retain io state");
    let io = io.lock().await;
    let enabled = io.config.enabled_domains();

    assert!(enabled.contains(&ProtocolDomain::Market));
    assert!(enabled.contains(&ProtocolDomain::System));
    assert!(enabled.contains(&ProtocolDomain::Query));
}

#[tokio::test(flavor = "current_thread")]
async fn has_feature_reads_auth_context_features() {
    let client = test_live_client_with_auth(
        runtime_with_default_adapters(),
        SessionTopology::default().with_route(SessionRoute {
            label: "system".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System],
            endpoint: SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            },
        }),
        QueueTransport::default(),
        Arc::new(RecordingExecutor::default()),
        Arc::new(TestAuthProvider::with_features(["futr", "opt"])),
    );

    assert!(client.has_feature("futr").await.unwrap());
    assert!(!client.has_feature("sec").await.unwrap());
}

#[cfg(feature = "tq-auth")]
#[tokio::test(flavor = "current_thread")]
async fn tqkq_login_helpers_derive_login_from_established_auth_context() {
    let client = test_live_client_with_auth(
        runtime_with_default_adapters(),
        SessionTopology::default().with_route(SessionRoute {
            label: "system".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System],
            endpoint: SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            },
        }),
        QueueTransport::default(),
        Arc::new(RecordingExecutor::default()),
        Arc::new(TestAuthProvider::with_auth_id("auth-1")),
    );

    let futures = client.tqkq_login_command_numbered(7).await.unwrap();
    assert_eq!(futures.broker_id, "快期模拟");
    assert_eq!(futures.account_id.as_str(), "auth-1007");
    assert_eq!(futures.password, "shinnytech007");
    assert_eq!(futures.account_type, TradeAccountType::Future);

    let stock = client.tqkq_stock_login_command().await.unwrap();
    assert_eq!(stock.broker_id, "快期股票模拟");
    assert_eq!(stock.account_id.as_str(), "auth-1-sim-securities");
    assert_eq!(stock.password, "auth-1");
    assert_eq!(stock.account_type, TradeAccountType::Spot);
}

#[tokio::test(flavor = "current_thread")]
async fn check_md_grants_allows_futures_with_futr_feature() {
    let client = test_live_client_with_auth(
        runtime_with_default_adapters(),
        SessionTopology::default().with_route(SessionRoute {
            label: "system".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System],
            endpoint: SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            },
        }),
        QueueTransport::default(),
        Arc::new(RecordingExecutor::default()),
        Arc::new(TestAuthProvider::with_features(["futr"])),
    );

    client
        .check_md_grants(&["SHFE.au2606", "SHFE.au2606C720"])
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn check_md_grants_rejects_stock_without_sec_feature() {
    let client = test_live_client_with_auth(
        runtime_with_default_adapters(),
        SessionTopology::default().with_route(SessionRoute {
            label: "system".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System],
            endpoint: SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            },
        }),
        QueueTransport::default(),
        Arc::new(RecordingExecutor::default()),
        Arc::new(TestAuthProvider::with_features(["opt"])),
    );

    let error = client
        .check_md_grants(&["SSE.510300", "SSE.10010989"])
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "auth error: your account does not support stock market data for SSE.510300"
    );
}

fn test_live_client(
    handle: RuntimeHandle,
    topology: SessionTopology,
    transport: QueueTransport,
    http_executor: SharedRouteExecutor,
) -> SessionClient {
    test_live_client_with_auth(
        handle,
        topology,
        transport,
        http_executor,
        Arc::new(TestAuthProvider::default()),
    )
}

fn test_live_client_with_auth(
    handle: RuntimeHandle,
    topology: SessionTopology,
    transport: QueueTransport,
    http_executor: SharedRouteExecutor,
    auth_provider: SharedAuthProvider,
) -> SessionClient {
    let topology_resolver: SharedTopologyResolver = Arc::new(StaticTopologyResolver {
        topology: topology.clone(),
    });
    let route_connector: SharedRouteConnector = Arc::new(QueueConnector { transport });
    let internal_executor: SharedRouteExecutor =
        Arc::new(SessionInternalExecutor::new(auth_provider.clone()));
    let replay_executor: SharedRouteExecutor = Arc::new(SessionReplayExecutor);

    test_live_client_with_components(
        handle,
        topology,
        SessionIoComponents {
            auth_provider,
            topology_resolver,
            route_connector,
            http_executor,
            internal_executor,
            replay_executor,
        },
    )
}

fn test_live_client_with_components(
    handle: RuntimeHandle,
    topology: SessionTopology,
    components: SessionIoComponents,
) -> SessionClient {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let mut endpoints = EndpointConfig::new("https://auth.example");
    let mut enabled_domains = Vec::new();
    for route in &topology.routes {
        for domain in &route.domains {
            if !enabled_domains.contains(domain) {
                enabled_domains.push(*domain);
            }
        }
        match &route.endpoint {
            SessionRouteEndpoint::WebSocket { url, .. }
                if route.domains.contains(&ProtocolDomain::Market) =>
            {
                endpoints = endpoints.with_market_url(url.clone());
            }
            SessionRouteEndpoint::WebSocket { url, .. }
                if route.domains.contains(&ProtocolDomain::Trade) =>
            {
                endpoints = endpoints.with_trade_url(url.clone());
            }
            SessionRouteEndpoint::Http { url }
                if route.domains.contains(&ProtocolDomain::Query) =>
            {
                endpoints = endpoints.with_query_url(url.clone());
            }
            SessionRouteEndpoint::Http { url }
                if route.domains.contains(&ProtocolDomain::Schema) =>
            {
                endpoints = endpoints.with_schema_url(url.clone());
            }
            SessionRouteEndpoint::Replay { label } => {
                endpoints = endpoints.with_replay_url(label.clone());
            }
            SessionRouteEndpoint::WebSocket { .. }
            | SessionRouteEndpoint::Http { .. }
            | SessionRouteEndpoint::Internal { .. } => {}
        }
    }
    let mut config = SessionConfig::new(endpoints.clone());
    for domain in enabled_domains {
        config = config.enable_domain(domain);
    }

    SessionClient {
        handle: handle.clone(),
        reader: handle.reader(),
        runtime: SessionRuntime::new(handle, SessionBootstrap::new()),
        query_lock: Arc::new(TokioMutex::new(())),
        market_interests: Arc::new(TokioMutex::new(MarketInterestRegistry::default())),
        order_intents: Arc::new(Mutex::new(std::collections::HashMap::new())),
        #[cfg(feature = "services")]
        service_http: reqwest::Client::new(),
        #[cfg(feature = "services")]
        trading_calendar_holiday_cache: Arc::new(TokioMutex::new(None)),
        #[cfg(feature = "live")]
        context: SessionClientContext::new(
            "demo-user".to_string(),
            "demo-pass".to_string(),
            endpoints,
        ),
        io: Some(Arc::new(TokioMutex::new(SessionIoState::new(
            components, adapters, config,
        )))),
    }
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}

struct CommandStatusFixtureAdapter {
    command_id: CommandId,
    status: String,
}

impl ProtocolAdapter for CommandStatusFixtureAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::System
    }

    fn accepts_command(&self, _cmd: &RuntimeCommand) -> bool {
        false
    }

    fn encode(&mut self, _cmd: &RuntimeCommand) -> CoreResult<Vec<OutboundRequest>> {
        Ok(Vec::new())
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(
            input,
            RuntimeInput::Io(IoEvent { route, .. }) if route == "command-status-fixture"
        )
    }

    fn decode(&mut self, _input: &RuntimeInput) -> CoreResult<Vec<NormalizedMutation>> {
        let command_segment = self.command_id.get().to_string();
        Ok(vec![NormalizedMutation {
            path: StatePath::new(vec![
                "runtime".to_string(),
                "commands".to_string(),
                command_segment,
            ]),
            object: Some(ObjectKey::Command {
                command_id: self.command_id,
            }),
            fields: vec![
                FieldMutation {
                    field: "domain".to_string(),
                    value: json!("trade"),
                },
                FieldMutation {
                    field: "status".to_string(),
                    value: json!(self.status),
                },
            ],
            source: MutationSource::SessionControl,
        }])
    }
}
