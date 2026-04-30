#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
#[cfg(any(test, feature = "live"))]
use std::future::Future;
#[cfg(any(test, feature = "live"))]
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::Value;
#[cfg(any(test, feature = "live"))]
use serde_json::json;
use tokio::sync::Mutex;
#[cfg(feature = "live")]
use tqsdk_core::internal::DefaultRouteConnector;
use tqsdk_core::internal::{DynAuthProvider, SessionBootstrap};
use tqsdk_core::internal::{RouteRequestExecutor, SessionRun, SessionRuntime};
use tqsdk_core::{
    AdapterRegistry, AuthContext, CommandId, OutboundDispatch, OutboundFrame, Quote, RuntimeHandle,
    RuntimeReader, SessionConfig, SessionRouteConnector, SessionRouteEndpoint,
    SessionTopologyResolver, TradeSessionTarget,
};
#[cfg(any(test, feature = "live"))]
use tqsdk_core::{AuthEvent, InternalEvent, ReplayEvent, SessionRoute, SessionTarget};
#[cfg(feature = "services")]
use tqsdk_core::{EdbIndexData, SymbolRanking, SymbolSettlement, TradingCalendarDay};

use crate::direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, FinanceOptionLevelQuery, OptionLevelQuotes,
    OptionQueryFilter, SessionMetadataQuery, SessionRawQuery,
};
#[cfg(feature = "services")]
use crate::direct_query::{EdbDataAlign, EdbDataFill, SessionServiceQuery, SymbolRankingType};
#[cfg(feature = "live")]
use crate::http_executor::ReqwestHttpExecutor;
use crate::order_intent::{OrderIntentRecord, OrderIntentRegistration};
#[cfg(feature = "services")]
use crate::services::SessionServiceEndpoints;
#[cfg(feature = "tq-auth")]
use crate::tq_auth::{PasswordCredentials, TqAuthProvider};
mod auth;
mod commands;
mod io;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
const PEEK_MESSAGE: &str = r#"{"aid":"peek_message"}"#;
const WEBSOCKET_COMMAND_POLL_BUDGET: Duration = Duration::from_millis(250);
const WEBSOCKET_COMMAND_MAX_WAIT: Duration = Duration::from_secs(60);

type SharedAuthProvider = Arc<dyn DynAuthProvider>;
type SharedTopologyResolver = Arc<dyn SessionTopologyResolver>;
type SharedRouteConnector = Arc<dyn SessionRouteConnector>;
type SharedRouteExecutor = Arc<dyn RouteRequestExecutor>;

#[derive(Debug, Clone)]
pub(crate) struct SessionClientContext {
    #[cfg(feature = "live")]
    auth_user: String,
    #[cfg(feature = "live")]
    auth_pass: String,
    #[cfg(feature = "live")]
    pub(crate) endpoints: tqsdk_core::EndpointConfig,
    #[cfg(feature = "services")]
    service_endpoints: SessionServiceEndpoints,
}

impl SessionClientContext {
    pub(crate) fn new(
        auth_user: String,
        auth_pass: String,
        endpoints: tqsdk_core::EndpointConfig,
    ) -> Self {
        #[cfg(not(feature = "live"))]
        let _ = (auth_user, auth_pass, endpoints);

        Self {
            #[cfg(feature = "live")]
            auth_user,
            #[cfg(feature = "live")]
            auth_pass,
            #[cfg(feature = "live")]
            endpoints,
            #[cfg(feature = "services")]
            service_endpoints: SessionServiceEndpoints::default(),
        }
    }

    #[cfg(all(test, feature = "services"))]
    pub(crate) fn new_with_services(
        auth_user: impl Into<String>,
        auth_pass: impl Into<String>,
        endpoints: tqsdk_core::EndpointConfig,
        service_endpoints: SessionServiceEndpoints,
    ) -> Self {
        let auth_user = auth_user.into();
        let auth_pass = auth_pass.into();
        #[cfg(not(feature = "live"))]
        let _ = (&auth_user, &auth_pass, &endpoints);

        Self {
            #[cfg(feature = "live")]
            auth_user,
            #[cfg(feature = "live")]
            auth_pass,
            #[cfg(feature = "live")]
            endpoints,
            #[cfg(feature = "services")]
            service_endpoints,
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
    cached_auth: Option<AuthContext>,
    next_pending_route: usize,
    next_websocket_route: usize,
}

#[cfg(any(test, feature = "live"))]
struct SessionIoComponents {
    auth_provider: SharedAuthProvider,
    topology_resolver: SharedTopologyResolver,
    route_connector: SharedRouteConnector,
    http_executor: SharedRouteExecutor,
    internal_executor: SharedRouteExecutor,
    replay_executor: SharedRouteExecutor,
}

impl SessionIoState {
    #[cfg(any(test, feature = "live"))]
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
            cached_auth: None,
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

async fn recover_run(
    io: &mut SessionIoState,
    runtime: &SessionRuntime,
) -> crate::error::Result<()> {
    let run = runtime
        .recover(
            io.auth_provider.as_ref(),
            io.topology_resolver.as_ref(),
            io.route_connector.as_ref(),
            &io.config,
            &io.adapters,
        )
        .await?;
    io.run = Some(run);
    prime_all_websocket_routes(io).await?;
    Ok(())
}

async fn prime_route_with_recover(
    io: &mut SessionIoState,
    runtime: &SessionRuntime,
    route_label: &str,
) -> crate::error::Result<()> {
    match prime_route(io, route_label).await {
        Ok(()) => Ok(()),
        Err(crate::error::SessionFacadeError::Core(tqsdk_core::ContractError::Transport(_))) => {
            recover_run(io, runtime).await?;
            prime_route(io, route_label).await
        }
        Err(err) => Err(err),
    }
}

/// Outcome of one substrate-level session progress step.
///
/// This models only transport/runtime progression. It does not consume commit
/// cursors and does not impose `wait_update()` semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionProgress {
    Idle,
    FlushedOutbound,
    DrovePending,
    DroveRoute,
}

impl SessionProgress {
    /// Returns true when the session made any observable transport/runtime progress.
    #[must_use]
    pub fn is_progress(self) -> bool {
        !matches!(self, Self::Idle)
    }
}

#[derive(Clone)]
/// Reusable async session facade shared by higher-level consumption styles.
///
/// [`SessionClient`] owns the live runtime handle plus one-shot direct-query,
/// schema, replay, auth, and metadata/service helpers. It does not impose a
/// `wait_update()` or stream/callback consumption model.
pub struct SessionClient {
    handle: RuntimeHandle,
    reader: RuntimeReader,
    runtime: SessionRuntime,
    order_intents: Arc<StdMutex<HashMap<(String, String), OrderIntentRecord>>>,
    #[cfg(feature = "services")]
    service_http: reqwest::Client,
    #[cfg(any(feature = "services", all(test, feature = "live")))]
    context: SessionClientContext,
    io: Option<Arc<Mutex<SessionIoState>>>,
}

impl SessionClient {
    #[cfg(feature = "live")]
    pub(crate) fn new_live(
        handle: RuntimeHandle,
        context: SessionClientContext,
        config: SessionConfig,
        trade_targets: Vec<TradeSessionTarget>,
    ) -> crate::error::Result<Self> {
        let reader = handle.reader();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        let provider = Arc::new(
            TqAuthProvider::new(PasswordCredentials::new(
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
            order_intents: Arc::new(StdMutex::new(HashMap::new())),
            #[cfg(feature = "services")]
            service_http: reqwest::Client::new(),
            #[cfg(any(feature = "services", all(test, feature = "live")))]
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

    #[cfg(not(feature = "live"))]
    pub(crate) fn new_live(
        _handle: RuntimeHandle,
        _context: SessionClientContext,
        _config: SessionConfig,
        _trade_targets: Vec<TradeSessionTarget>,
    ) -> crate::error::Result<Self> {
        Err(crate::error::SessionFacadeError::InvalidState(
            "live session support requires the `live` feature",
        ))
    }

    fn new_without_io(handle: RuntimeHandle, context: SessionClientContext) -> Self {
        #[cfg(not(any(feature = "services", all(test, feature = "live"))))]
        let _ = context;

        let reader = handle.reader();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        Self {
            handle,
            reader,
            runtime,
            order_intents: Arc::new(StdMutex::new(HashMap::new())),
            #[cfg(feature = "services")]
            service_http: reqwest::Client::new(),
            #[cfg(any(feature = "services", all(test, feature = "live")))]
            context,
            io: None,
        }
    }

    #[must_use]
    pub fn handle(&self) -> &RuntimeHandle {
        &self.handle
    }

    #[must_use]
    pub fn reader(&self) -> &RuntimeReader {
        &self.reader
    }

    #[must_use]
    pub fn reader_clone(&self) -> RuntimeReader {
        self.reader.clone()
    }

    pub(crate) fn drain_manual_dispatches(&self) -> crate::error::Result<Vec<OutboundDispatch>> {
        if self.io.is_some() {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "drain_dispatches is only available for test/manual sessions without live IO; use progress_once() to drive live sessions",
            ));
        }

        Ok(self.handle.drain_dispatches()?)
    }

    pub fn remember_order_intent(
        &self,
        record: OrderIntentRecord,
    ) -> crate::error::Result<OrderIntentRegistration> {
        let mut order_intents = self.order_intents.lock().map_err(|_| {
            crate::error::SessionFacadeError::InvalidState("order intent ledger lock poisoned")
        })?;
        let key = record.key();

        if let Some(existing) = order_intents.get(&key) {
            if !existing.request_matches(&record) {
                return Err(crate::error::SessionFacadeError::InvalidState(
                    "client order intent already registered with different order fields",
                ));
            }
            return Ok(OrderIntentRegistration::Existing(existing.clone()));
        }

        order_intents.insert(key, record.clone());
        Ok(OrderIntentRegistration::Registered(record))
    }

    pub fn update_order_intent_command(
        &self,
        account_id: &str,
        client_order_id: &str,
        command_id: CommandId,
    ) -> crate::error::Result<()> {
        let mut order_intents = self.order_intents.lock().map_err(|_| {
            crate::error::SessionFacadeError::InvalidState("order intent ledger lock poisoned")
        })?;
        if let Some(record) =
            order_intents.get_mut(&(account_id.to_owned(), client_order_id.to_owned()))
        {
            record.set_command_id(command_id);
        }
        Ok(())
    }

    pub fn forget_order_intent(
        &self,
        account_id: &str,
        client_order_id: &str,
    ) -> crate::error::Result<Option<OrderIntentRecord>> {
        let mut order_intents = self.order_intents.lock().map_err(|_| {
            crate::error::SessionFacadeError::InvalidState("order intent ledger lock poisoned")
        })?;
        Ok(order_intents.remove(&(account_id.to_owned(), client_order_id.to_owned())))
    }

    pub fn order_intent(
        &self,
        account_id: &str,
        client_order_id: &str,
    ) -> crate::error::Result<Option<OrderIntentRecord>> {
        let order_intents = self.order_intents.lock().map_err(|_| {
            crate::error::SessionFacadeError::InvalidState("order intent ledger lock poisoned")
        })?;
        Ok(order_intents
            .get(&(account_id.to_owned(), client_order_id.to_owned()))
            .cloned())
    }

    #[cfg(feature = "services")]
    pub(crate) fn service_http(&self) -> &reqwest::Client {
        &self.service_http
    }

    #[cfg(feature = "services")]
    pub(crate) fn service_endpoints(&self) -> &SessionServiceEndpoints {
        &self.context.service_endpoints
    }

    pub(crate) fn new_manual_with_handle(handle: RuntimeHandle) -> Self {
        Self::new_without_io(
            handle,
            SessionClientContext::new(
                String::new(),
                String::new(),
                tqsdk_core::EndpointConfig::default(),
            ),
        )
    }

    #[cfg(all(test, feature = "live"))]
    pub(crate) fn auth_user(&self) -> &str {
        &self.context.auth_user
    }

    #[cfg(all(test, feature = "live"))]
    pub(crate) fn auth_pass(&self) -> &str {
        &self.context.auth_pass
    }

    #[cfg(all(test, feature = "live"))]
    pub(crate) fn endpoints(&self) -> &tqsdk_core::EndpointConfig {
        &self.context.endpoints
    }
}

impl SessionRawQuery for SessionClient {
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

impl SessionMetadataQuery for SessionClient {
    async fn query_symbol_info(&self, symbols: &[&str]) -> crate::error::Result<Vec<Quote>> {
        SessionClient::query_symbol_info(self, symbols).await
    }

    async fn query_instrument_specs(
        &self,
        symbols: &[&str],
    ) -> crate::error::Result<Vec<crate::InstrumentSpec>> {
        SessionClient::query_instrument_specs(self, symbols).await
    }

    async fn query_quotes(
        &self,
        ins_class: Option<&str>,
        exchange_id: Option<&str>,
        product_id: Option<&str>,
        expired: Option<bool>,
        has_night: Option<bool>,
    ) -> crate::error::Result<Vec<String>> {
        SessionClient::query_quotes(self, ins_class, exchange_id, product_id, expired, has_night)
            .await
    }

    async fn query_cont_quotes(
        &self,
        exchange_id: Option<&str>,
        product_id: Option<&str>,
        has_night: Option<bool>,
    ) -> crate::error::Result<Vec<String>> {
        SessionClient::query_cont_quotes(self, exchange_id, product_id, has_night).await
    }

    async fn query_options(
        &self,
        underlying_symbol: &str,
        filter: &OptionQueryFilter,
    ) -> crate::error::Result<Vec<String>> {
        SessionClient::query_options(self, underlying_symbol, filter).await
    }

    async fn query_atm_options(
        &self,
        underlying_symbol: &str,
        query: &AtmOptionQuery,
    ) -> crate::error::Result<Vec<Option<String>>> {
        SessionClient::query_atm_options(self, underlying_symbol, query).await
    }

    async fn query_all_level_options(
        &self,
        underlying_symbol: &str,
        query: &AllLevelOptionQuery,
    ) -> crate::error::Result<OptionLevelQuotes> {
        SessionClient::query_all_level_options(self, underlying_symbol, query).await
    }

    async fn query_all_level_finance_options(
        &self,
        underlying_symbol: &str,
        query: &FinanceOptionLevelQuery,
    ) -> crate::error::Result<OptionLevelQuotes> {
        SessionClient::query_all_level_finance_options(self, underlying_symbol, query).await
    }
}

#[cfg(feature = "services")]
impl SessionServiceQuery for SessionClient {
    async fn get_trading_calendar(
        &self,
        start_dt: chrono::NaiveDate,
        end_dt: chrono::NaiveDate,
    ) -> crate::error::Result<Vec<TradingCalendarDay>> {
        SessionClient::get_trading_calendar(self, start_dt, end_dt).await
    }

    async fn query_symbol_settlement(
        &self,
        symbols: &[&str],
        days: usize,
        start_dt: Option<chrono::NaiveDate>,
    ) -> crate::error::Result<Vec<SymbolSettlement>> {
        SessionClient::query_symbol_settlement(self, symbols, days, start_dt).await
    }

    async fn query_symbol_ranking(
        &self,
        symbol: &str,
        ranking_type: SymbolRankingType,
        days: usize,
        start_dt: Option<chrono::NaiveDate>,
        broker: Option<&str>,
    ) -> crate::error::Result<Vec<SymbolRanking>> {
        SessionClient::query_symbol_ranking(self, symbol, ranking_type, days, start_dt, broker)
            .await
    }

    async fn query_edb_data(
        &self,
        ids: &[i32],
        start_dt: chrono::NaiveDate,
        end_dt: chrono::NaiveDate,
        align: Option<EdbDataAlign>,
        fill: Option<EdbDataFill>,
    ) -> crate::error::Result<Vec<EdbIndexData>> {
        SessionClient::query_edb_data(self, ids, start_dt, end_dt, align, fill).await
    }
}

#[derive(Clone)]
#[cfg(any(test, feature = "live"))]
struct SessionInternalExecutor {
    auth_provider: SharedAuthProvider,
}

#[cfg(any(test, feature = "live"))]
impl SessionInternalExecutor {
    fn new(auth_provider: SharedAuthProvider) -> Self {
        Self { auth_provider }
    }
}

#[cfg(any(test, feature = "live"))]
impl RouteRequestExecutor for SessionInternalExecutor {
    fn execute<'a>(
        &'a self,
        _route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> Pin<Box<dyn Future<Output = tqsdk_core::Result<Vec<tqsdk_core::RuntimeInput>>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut inputs = Vec::with_capacity(requests.len());
            for request in requests {
                match request.request {
                    tqsdk_core::OutboundRequest::Internal(internal)
                        if internal.label == "refresh-auth" =>
                    {
                        let auth = self.auth_provider.authenticate_boxed().await?;
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
#[cfg(any(test, feature = "live"))]
struct SessionReplayExecutor;

#[cfg(any(test, feature = "live"))]
impl RouteRequestExecutor for SessionReplayExecutor {
    fn execute<'a>(
        &'a self,
        route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> Pin<Box<dyn Future<Output = tqsdk_core::Result<Vec<tqsdk_core::RuntimeInput>>> + Send + 'a>>
    {
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
#[allow(clippy::manual_async_fn)]
mod tests;
