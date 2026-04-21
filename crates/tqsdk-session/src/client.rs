#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout};
use tqsdk_core::{
    AdapterRegistry, AuthEvent, AuthProvider, CommandId, CommitScope, DefaultRouteConnector,
    InternalEvent, OutboundDispatch, OutboundFrame, QueryCommand, QueryId, ReplayEvent,
    ReqwestHttpExecutor, RouteRequestExecutor, Runtime, RuntimeCommand, RuntimeHandle,
    RuntimeReader, SchemaCommand, SchemaId, SessionBootstrap, SessionConfig, SessionRoute,
    SessionRouteConnector, SessionRouteEndpoint, SessionRun, SessionRuntime, SessionRuntimeDeps,
    SessionTarget, SessionTopologyResolver, TqAuthProvider, TradeSessionTarget,
};

use crate::config::SessionFacadeConfig;
use crate::direct_query::SessionDirectQuery;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
const PEEK_MESSAGE: &str = r#"{"aid":"peek_message"}"#;

type SharedAuthProvider = Arc<dyn AuthProvider>;
type SharedTopologyResolver = Arc<dyn SessionTopologyResolver>;
type SharedRouteConnector = Arc<dyn SessionRouteConnector>;
type SharedRouteExecutor = Arc<dyn RouteRequestExecutor>;

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) struct SessionClientContext {
    auth_user: String,
    auth_pass: String,
    pub(crate) endpoints: tqsdk_core::EndpointConfig,
}

impl SessionClientContext {
    pub(crate) fn new(
        auth_user: String,
        auth_pass: String,
        endpoints: tqsdk_core::EndpointConfig,
    ) -> Self {
        Self {
            auth_user,
            auth_pass,
            endpoints,
        }
    }
}

struct SessionIoState {
    auth_provider: SharedAuthProvider,
    topology_resolver: SharedTopologyResolver,
    route_connector: SharedRouteConnector,
    http_executor: SharedRouteExecutor,
    internal_executor: SharedRouteExecutor,
    replay_executor: SharedRouteExecutor,
    adapters: AdapterRegistry,
    config: SessionConfig,
    run: Option<SessionRun>,
    next_pending_route: usize,
    next_websocket_route: usize,
}

struct SessionIoComponents {
    auth_provider: SharedAuthProvider,
    topology_resolver: SharedTopologyResolver,
    route_connector: SharedRouteConnector,
    http_executor: SharedRouteExecutor,
    internal_executor: SharedRouteExecutor,
    replay_executor: SharedRouteExecutor,
}

impl SessionIoState {
    fn new(
        components: SessionIoComponents,
        adapters: AdapterRegistry,
        config: SessionConfig,
    ) -> Self {
        Self {
            auth_provider: components.auth_provider,
            topology_resolver: components.topology_resolver,
            route_connector: components.route_connector,
            http_executor: components.http_executor,
            internal_executor: components.internal_executor,
            replay_executor: components.replay_executor,
            adapters,
            config,
            run: None,
            next_pending_route: 0,
            next_websocket_route: 0,
        }
    }

    fn next_pending_route_label(&mut self) -> Option<String> {
        next_route_label(&self.run, &mut self.next_pending_route, false)
    }

    fn next_websocket_route_label(&mut self) -> Option<String> {
        next_route_label(&self.run, &mut self.next_websocket_route, true)
    }
}

#[derive(Clone)]
pub struct SessionClient {
    handle: RuntimeHandle,
    reader: RuntimeReader,
    runtime: SessionRuntime,
    facade_config: SessionFacadeConfig,
    #[cfg_attr(not(test), allow(dead_code))]
    context: SessionClientContext,
    io: Option<Arc<Mutex<SessionIoState>>>,
}

impl SessionClient {
    fn next_query_id() -> QueryId {
        QueryId::new(format!(
            "query-{}",
            NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    pub(crate) fn new_live(
        handle: RuntimeHandle,
        facade_config: SessionFacadeConfig,
        context: SessionClientContext,
        config: SessionConfig,
        trade_targets: Vec<TradeSessionTarget>,
    ) -> crate::error::Result<Self> {
        let reader = handle.reader();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        let provider = Arc::new(
            TqAuthProvider::new(tqsdk_core::PasswordCredentials::new(
                context.auth_user.clone(),
                context.auth_pass.clone(),
            ))
            .with_auth_url(
                context
                    .endpoints
                    .auth_url
                    .clone()
                    .unwrap_or_else(|| "https://auth.shinnytech.com".to_string()),
            ),
        );
        let auth_provider: SharedAuthProvider = provider.clone();
        let topology_resolver: SharedTopologyResolver = provider;
        let route_connector: SharedRouteConnector = Arc::new(DefaultRouteConnector::default());
        let http_executor: SharedRouteExecutor = Arc::new(ReqwestHttpExecutor::new()?);
        let internal_executor: SharedRouteExecutor =
            Arc::new(SessionInternalExecutor::new(auth_provider.clone()));
        let replay_executor: SharedRouteExecutor = Arc::new(SessionReplayExecutor);
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();

        let _ = trade_targets;

        Ok(Self {
            handle,
            reader,
            runtime,
            facade_config,
            context,
            io: Some(Arc::new(Mutex::new(SessionIoState::new(
                SessionIoComponents {
                    auth_provider,
                    topology_resolver,
                    route_connector,
                    http_executor,
                    internal_executor,
                    replay_executor,
                },
                adapters,
                config,
            )))),
        })
    }

    fn new_without_io(
        handle: RuntimeHandle,
        facade_config: SessionFacadeConfig,
        context: SessionClientContext,
    ) -> Self {
        let reader = handle.reader();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        Self {
            handle,
            reader,
            runtime,
            facade_config,
            context,
            io: None,
        }
    }

    pub fn handle(&self) -> &RuntimeHandle {
        &self.handle
    }

    pub fn reader(&self) -> &RuntimeReader {
        &self.reader
    }

    pub fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }

    pub fn reader_clone(&self) -> RuntimeReader {
        self.reader.clone()
    }

    pub fn runtime_clone(&self) -> SessionRuntime {
        self.runtime.clone()
    }

    pub async fn submit(&self, command: RuntimeCommand) -> crate::error::Result<CommandId> {
        Ok(self.handle.submit(command).await?)
    }

    pub fn drain_dispatches(&self) -> crate::error::Result<Vec<OutboundDispatch>> {
        Ok(self.handle.drain_dispatches()?)
    }

    pub async fn ensure_established(&self) -> crate::error::Result<bool> {
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        if io.run.is_some() {
            return Ok(false);
        }

        let run = self
            .runtime
            .establish(
                io.auth_provider.as_ref(),
                io.topology_resolver.as_ref(),
                io.route_connector.as_ref(),
                &io.config,
                &io.adapters,
            )
            .await?;
        io.run = Some(run);
        prime_all_websocket_routes(&mut io).await?;
        Ok(true)
    }

    pub async fn flush_outbound(&self) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        let Some(run) = io.run.as_mut() else {
            return Ok(false);
        };
        let receipts = self.runtime.flush_outbound(run).await?;
        Ok(!receipts.is_empty())
    }

    pub async fn drive_pending_once(&self) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        let Some(route_label) = io.next_pending_route_label() else {
            return Ok(false);
        };
        let Some(route) = io.run.as_ref().and_then(|run| {
            run.connected
                .routes
                .iter()
                .find(|route| route.route.label == route_label)
                .map(|route| route.route.clone())
        }) else {
            return Ok(false);
        };
        let Some(executor) = (match route.endpoint {
            SessionRouteEndpoint::Http { .. } => Some(io.http_executor.clone()),
            SessionRouteEndpoint::Internal { .. } => Some(io.internal_executor.clone()),
            SessionRouteEndpoint::Replay { .. } => Some(io.replay_executor.clone()),
            SessionRouteEndpoint::WebSocket { .. } => None,
        }) else {
            return Ok(false);
        };
        let Some(run) = io.run.as_mut() else {
            return Ok(false);
        };
        let outcome = self
            .runtime
            .drive_pending_route_once(
                run,
                route_label.as_str(),
                executor.as_ref(),
                Vec::new(),
                CommitScope::RealtimeUpdate,
            )
            .await?;
        Ok(!outcome.requests.is_empty() || !outcome.commits.is_empty())
    }

    pub async fn drive_route_once(&self, deadline: Option<Instant>) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        let Some(route_label) = io.next_websocket_route_label() else {
            return Ok(false);
        };
        prime_route(&mut io, route_label.as_str()).await?;

        let SessionIoState {
            auth_provider,
            topology_resolver,
            route_connector,
            adapters,
            config,
            run,
            ..
        } = &mut *io;
        let Some(run) = run.as_mut() else {
            return Ok(false);
        };
        let deps = SessionRuntimeDeps::new(
            auth_provider.as_ref(),
            topology_resolver.as_ref(),
            route_connector.as_ref(),
            config,
            adapters,
        );
        let future = self.runtime.drive_route_once(
            run,
            route_label.as_str(),
            Vec::new(),
            CommitScope::RealtimeUpdate,
            deps,
        );
        let outcome = if let Some(deadline) = deadline {
            let budget = deadline.saturating_duration_since(Instant::now());
            if budget.is_zero() {
                return Ok(false);
            }
            match timeout(budget, future).await {
                Ok(result) => result?,
                Err(_) => return Ok(false),
            }
        } else {
            future.await?
        };

        Ok(!outcome.dispatches.is_empty() || !outcome.commits.is_empty() || outcome.recovered)
    }

    pub fn query_result(&self, query_id: &str) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["query", query_id])
            .map_err(Into::into)
    }

    pub fn schema_value(&self, schema_id: &str) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["schema", schema_id])
            .map_err(Into::into)
    }

    async fn drive_until_value<F>(&self, mut load: F) -> crate::error::Result<Value>
    where
        F: FnMut(&Self) -> crate::error::Result<Option<Value>>,
    {
        if let Some(value) = load(self)? {
            return Ok(value);
        }

        loop {
            let mut progress = false;

            progress |= self.flush_outbound().await?;
            if let Some(value) = load(self)? {
                return Ok(value);
            }

            progress |= self.drive_pending_once().await?;
            if let Some(value) = load(self)? {
                return Ok(value);
            }

            progress |= self.drive_route_once(None).await?;
            if let Some(value) = load(self)? {
                return Ok(value);
            }

            if !progress {
                return Err(crate::error::SessionFacadeError::InvalidState(
                    "direct query did not produce a result",
                ));
            }
        }
    }

    pub async fn query_graphql(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: Self::next_query_id(),
            query: query.to_owned(),
            variables,
        }))
        .await
    }

    pub async fn refresh_schema(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new(schema_id),
            path: path.to_owned(),
        }))
        .await
    }

    pub async fn query_graphql_value(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<Value> {
        let query_id = Self::next_query_id();
        self.submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: query_id.clone(),
            query: query.to_owned(),
            variables,
        }))
        .await?;

        self.drive_until_value(|client| client.query_result(query_id.as_str()))
            .await
    }

    pub async fn refresh_schema_value(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<Value> {
        self.refresh_schema(schema_id, path).await?;
        self.drive_until_value(|client| client.schema_value(schema_id))
            .await
    }

    pub fn facade_config(&self) -> &SessionFacadeConfig {
        &self.facade_config
    }

    #[doc(hidden)]
    pub fn new_for_test_with_handle(
        handle: RuntimeHandle,
        facade_config: SessionFacadeConfig,
    ) -> Self {
        Self::new_without_io(
            handle,
            facade_config,
            SessionClientContext::new(
                String::new(),
                String::new(),
                tqsdk_core::EndpointConfig::default(),
            ),
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn auth_user(&self) -> &str {
        &self.context.auth_user
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn auth_pass(&self) -> &str {
        &self.context.auth_pass
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn endpoints(&self) -> &tqsdk_core::EndpointConfig {
        &self.context.endpoints
    }
}

impl SessionDirectQuery for SessionClient {
    async fn query_graphql(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<CommandId> {
        SessionClient::query_graphql(self, query, variables).await
    }

    async fn refresh_schema(&self, schema_id: &str, path: &str) -> crate::error::Result<CommandId> {
        SessionClient::refresh_schema(self, schema_id, path).await
    }

    async fn query_graphql_value(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<Value> {
        SessionClient::query_graphql_value(self, query, variables).await
    }

    async fn refresh_schema_value(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<Value> {
        SessionClient::refresh_schema_value(self, schema_id, path).await
    }
}

#[derive(Clone)]
struct SessionInternalExecutor {
    auth_provider: SharedAuthProvider,
}

impl SessionInternalExecutor {
    fn new(auth_provider: SharedAuthProvider) -> Self {
        Self { auth_provider }
    }
}

impl RouteRequestExecutor for SessionInternalExecutor {
    fn execute<'a>(
        &'a self,
        _route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> tqsdk_core::ContractFuture<'a, Vec<tqsdk_core::RuntimeInput>> {
        Box::pin(async move {
            let mut inputs = Vec::with_capacity(requests.len());
            for request in requests {
                match request.request {
                    tqsdk_core::OutboundRequest::Internal(internal)
                        if internal.label == "refresh-auth" =>
                    {
                        let auth = self.auth_provider.authenticate().await?;
                        inputs.push(tqsdk_core::RuntimeInput::Auth(AuthEvent {
                            label: "refreshed",
                            payload: Some(json!({
                                "access_token": auth.access_token(),
                                "auth_id": auth.auth_id().map(|id| id.as_str()),
                                "features": auth.features(),
                            })),
                        }));
                    }
                    tqsdk_core::OutboundRequest::Internal(internal) => {
                        inputs.push(tqsdk_core::RuntimeInput::Internal(InternalEvent {
                            label: internal.label,
                            payload: None,
                        }));
                    }
                    other => {
                        return Err(tqsdk_core::ContractError::validation(format!(
                            "internal executor received unsupported request: {other:?}"
                        )));
                    }
                }
            }
            Ok(inputs)
        })
    }
}

#[derive(Clone, Default)]
struct SessionReplayExecutor;

impl RouteRequestExecutor for SessionReplayExecutor {
    fn execute<'a>(
        &'a self,
        route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> tqsdk_core::ContractFuture<'a, Vec<tqsdk_core::RuntimeInput>> {
        Box::pin(async move {
            let session_id = match &route.target {
                SessionTarget::Replay(session_id) => Some(session_id.clone()),
                SessionTarget::Shared | SessionTarget::Account(_) => None,
            };
            let mut inputs = Vec::with_capacity(requests.len());
            for request in requests {
                let tqsdk_core::OutboundRequest::Replay(replay) = request.request else {
                    return Err(tqsdk_core::ContractError::validation(
                        "replay executor received non-replay request",
                    ));
                };
                let payload = match replay.action {
                    "step" => Some(json!({ "state": "stepped" })),
                    "reset" => Some(json!({ "state": "reset" })),
                    _ => Some(json!({ "state": replay.action })),
                };
                inputs.push(tqsdk_core::RuntimeInput::Replay(ReplayEvent {
                    label: replay.action,
                    session_id: session_id.clone(),
                    payload,
                }));
            }
            Ok(inputs)
        })
    }
}

fn next_route_label(
    run: &Option<SessionRun>,
    cursor: &mut usize,
    websocket: bool,
) -> Option<String> {
    let routes = &run.as_ref()?.connected.routes;
    if routes.is_empty() {
        return None;
    }

    for offset in 0..routes.len() {
        let index = (*cursor + offset) % routes.len();
        let is_websocket = matches!(
            routes[index].route.endpoint,
            SessionRouteEndpoint::WebSocket { .. }
        );
        if is_websocket == websocket {
            *cursor = (index + 1) % routes.len();
            return Some(routes[index].route.label.clone());
        }
    }

    None
}

async fn prime_all_websocket_routes(io: &mut SessionIoState) -> crate::error::Result<()> {
    let Some(run) = io.run.as_mut() else {
        return Ok(());
    };
    let labels = run
        .connected
        .routes
        .iter()
        .filter_map(|route| match route.route.endpoint {
            SessionRouteEndpoint::WebSocket { .. } => Some(route.route.label.clone()),
            SessionRouteEndpoint::Http { .. }
            | SessionRouteEndpoint::Replay { .. }
            | SessionRouteEndpoint::Internal { .. } => None,
        })
        .collect::<Vec<_>>();
    for label in labels {
        run.connected
            .send_route_frame(&label, OutboundFrame::Text(PEEK_MESSAGE.to_string()))
            .await?;
    }
    Ok(())
}

async fn prime_route(io: &mut SessionIoState, route_label: &str) -> crate::error::Result<()> {
    let Some(run) = io.run.as_mut() else {
        return Ok(());
    };
    run.connected
        .send_route_frame(route_label, OutboundFrame::Text(PEEK_MESSAGE.to_string()))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use tokio::sync::Mutex as TokioMutex;
    use tokio::time::{Duration, Instant};
    use tqsdk_core::{
        AdapterRegistry, AuthContext, AuthProvider, ContractFuture, EndpointConfig, InputPayload,
        IoEvent, MarketCommand, OutboundDispatch, OutboundFrame, OutboundRequest, ProtocolDomain,
        QueryCommand, QueryId, RawFrame, RouteRequestExecutor, Runtime, RuntimeCommand,
        RuntimeHandle, RuntimeInput, SessionBootstrap, SessionConfig, SessionRoute,
        SessionRouteConnector, SessionRouteEndpoint, SessionRuntime, SessionTarget,
        SessionTopology, SessionTopologyResolver, Transport,
    };

    use super::{
        SessionClient, SessionClientContext, SessionInternalExecutor, SessionIoComponents,
        SessionIoState, SessionReplayExecutor, SharedAuthProvider, SharedRouteConnector,
        SharedRouteExecutor, SharedTopologyResolver,
    };
    use crate::config::SessionFacadeConfig;

    #[derive(Clone)]
    struct TestAuthProvider;

    impl AuthProvider for TestAuthProvider {
        fn authenticate(&self) -> ContractFuture<'_, AuthContext> {
            Box::pin(async { Ok(AuthContext::new("test-token")) })
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
        ) -> ContractFuture<'a, SessionTopology> {
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
        fn connect(&mut self) -> ContractFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn recv(&mut self) -> ContractFuture<'_, RawFrame> {
            let recv_queue = Arc::clone(&self.recv_queue);
            Box::pin(async move {
                let frame = recv_queue
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(RawFrame::Pong);
                Ok(frame)
            })
        }

        fn send(&mut self, frame: OutboundFrame) -> ContractFuture<'_, ()> {
            let sent = Arc::clone(&self.sent);
            Box::pin(async move {
                sent.lock().unwrap().push(frame);
                Ok(())
            })
        }

        fn close(&mut self) -> ContractFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Clone)]
    struct QueueConnector {
        transport: QueueTransport,
    }

    impl SessionRouteConnector for QueueConnector {
        fn connect_route<'a>(
            &'a self,
            _route: &'a SessionRoute,
        ) -> ContractFuture<'a, Box<dyn Transport>> {
            let transport = self.transport.clone();
            Box::pin(async move { Ok(Box::new(transport) as Box<dyn Transport>) })
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
        ) -> ContractFuture<'a, Vec<RuntimeInput>> {
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
                .drive_route_once(Some(Instant::now() + Duration::from_millis(20)))
                .await
                .unwrap()
        );
        assert_eq!(
            handle
                .latest_snapshot()
                .get(["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(618.5))
        );
        assert!(sent.lock().unwrap().iter().any(
            |frame| matches!(frame, OutboundFrame::Text(text) if text == super::PEEK_MESSAGE)
        ));
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

    #[test]
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

    fn test_live_client(
        handle: RuntimeHandle,
        topology: SessionTopology,
        transport: QueueTransport,
        http_executor: SharedRouteExecutor,
    ) -> SessionClient {
        let auth_provider: SharedAuthProvider = Arc::new(TestAuthProvider);
        let topology_resolver: SharedTopologyResolver =
            Arc::new(StaticTopologyResolver { topology });
        let route_connector: SharedRouteConnector = Arc::new(QueueConnector { transport });
        let internal_executor: SharedRouteExecutor =
            Arc::new(SessionInternalExecutor::new(auth_provider.clone()));
        let replay_executor: SharedRouteExecutor = Arc::new(SessionReplayExecutor);
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
            .enable_domain(ProtocolDomain::Market)
            .enable_domain(ProtocolDomain::Query);

        SessionClient {
            handle: handle.clone(),
            reader: handle.reader(),
            runtime: SessionRuntime::new(handle, SessionBootstrap::new()),
            facade_config: SessionFacadeConfig::default(),
            context: SessionClientContext::new(
                "demo-user".to_string(),
                "demo-pass".to_string(),
                EndpointConfig::new("https://auth.example"),
            ),
            io: Some(Arc::new(TokioMutex::new(SessionIoState::new(
                SessionIoComponents {
                    auth_provider,
                    topology_resolver,
                    route_connector,
                    http_executor,
                    internal_executor,
                    replay_executor,
                },
                adapters,
                config,
            )))),
        }
    }

    fn runtime_with_default_adapters() -> RuntimeHandle {
        let mut registry = AdapterRegistry::new();
        registry.register_default_adapters();
        RuntimeHandle::with_adapters(registry)
    }
}
