#![cfg_attr(not(test), forbid(unsafe_code))]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout};
use tqsdk_core::{
    AdapterRegistry, AuthContext, AuthEvent, CommandId, CommitScope, DefaultRouteConnector,
    DynAuthProvider, EdbIndexData, InternalEvent, OutboundDispatch, OutboundFrame, QueryCommand,
    QueryId, Quote, ReplayCommand, ReplayEvent, RouteRequestExecutor, Runtime, RuntimeCommand,
    RuntimeHandle, RuntimeReader, SchemaCommand, SchemaId, SessionBootstrap, SessionConfig,
    SessionRoute, SessionRouteConnector, SessionRouteEndpoint, SessionRun, SessionRuntime,
    SessionRuntimeDeps, SessionTarget, SessionTopologyResolver, SymbolRanking, SymbolSettlement,
    SystemCommand, TradeLoginCommand, TradeSessionTarget, TradingCalendarDay,
};

use crate::direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, EdbDataAlign, EdbDataFill, FinanceOptionLevelQuery,
    OptionLevelQuotes, OptionQueryFilter, SessionMetadataQuery, SessionRawQuery,
    SessionServiceQuery, SymbolRankingType,
};
use crate::http_executor::ReqwestHttpExecutor;
use crate::services::SessionServiceEndpoints;
use crate::tq_auth::{PasswordCredentials, TqAuthProvider};
use crate::tqkq::TqKqAccountConfig;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);
const PEEK_MESSAGE: &str = r#"{"aid":"peek_message"}"#;
const WEBSOCKET_COMMAND_POLL_BUDGET: Duration = Duration::from_millis(250);
const WEBSOCKET_COMMAND_MAX_WAIT: Duration = Duration::from_secs(60);
const LIMITED_INDEX_SYMBOLS: &[&str] = &["SSE.000016", "SSE.000300", "SSE.000905", "SSE.000852"];

type SharedAuthProvider = Arc<dyn DynAuthProvider>;
type SharedTopologyResolver = Arc<dyn SessionTopologyResolver>;
type SharedRouteConnector = Arc<dyn SessionRouteConnector>;
type SharedRouteExecutor = Arc<dyn RouteRequestExecutor>;

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) struct SessionClientContext {
    auth_user: String,
    auth_pass: String,
    pub(crate) endpoints: tqsdk_core::EndpointConfig,
    service_endpoints: SessionServiceEndpoints,
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
            service_endpoints: SessionServiceEndpoints::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_services(
        auth_user: impl Into<String>,
        auth_pass: impl Into<String>,
        endpoints: tqsdk_core::EndpointConfig,
        service_endpoints: SessionServiceEndpoints,
    ) -> Self {
        Self {
            auth_user: auth_user.into(),
            auth_pass: auth_pass.into(),
            endpoints,
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
    service_http: reqwest::Client,
    #[cfg_attr(not(test), allow(dead_code))]
    context: SessionClientContext,
    io: Option<Arc<Mutex<SessionIoState>>>,
}

impl SessionClient {
    fn validate_query_payload(value: &Value) -> crate::error::Result<()> {
        if let Some(error) = value.get("error").and_then(Value::as_str) {
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::validation(format!("graphql query failed: {error}")),
            ));
        }
        if let Some(errors) = value.get("errors").and_then(Value::as_array)
            && !errors.is_empty()
        {
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::validation(format!(
                    "graphql query failed: {}",
                    Value::Array(errors.clone())
                )),
            ));
        }
        Ok(())
    }

    fn next_query_id() -> QueryId {
        QueryId::new(format!(
            "query-{}",
            NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

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
            service_http: reqwest::Client::new(),
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

    fn new_without_io(handle: RuntimeHandle, context: SessionClientContext) -> Self {
        let reader = handle.reader();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        Self {
            handle,
            reader,
            runtime,
            service_http: reqwest::Client::new(),
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
    pub fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }

    #[must_use]
    pub fn reader_clone(&self) -> RuntimeReader {
        self.reader.clone()
    }

    #[must_use]
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
        self.flush_outbound_locked(&mut io).await
    }

    pub async fn drive_pending_once(&self) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        self.drive_pending_once_locked(&mut io).await
    }

    async fn drive_pending_route_label_once(
        &self,
        route_label: &str,
    ) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
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
                route_label,
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
        self.drive_route_once_locked(&mut io, deadline).await
    }

    /// Performs one substrate-level progress step across outbound flush,
    /// pending-route execution, and one websocket-route drive attempt.
    ///
    /// Callers should still drain commit cursors themselves if they need
    /// commit-first semantics. This helper only advances the live session.
    pub async fn progress_once(
        &self,
        deadline: Option<Instant>,
    ) -> crate::error::Result<SessionProgress> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(SessionProgress::Idle);
        };
        let mut io = io.lock().await;

        if self.flush_outbound_locked(&mut io).await? {
            return Ok(SessionProgress::FlushedOutbound);
        }
        if self.drive_pending_once_locked(&mut io).await? {
            return Ok(SessionProgress::DrovePending);
        }
        if self.drive_route_once_locked(&mut io, deadline).await? {
            return Ok(SessionProgress::DroveRoute);
        }

        Ok(SessionProgress::Idle)
    }

    async fn drive_route_label_once(
        &self,
        route_label: &str,
        deadline: Option<Instant>,
        caused_by: Vec<CommandId>,
    ) -> crate::error::Result<bool> {
        self.ensure_established().await?;
        let Some(io) = self.io.as_ref() else {
            return Ok(false);
        };
        let mut io = io.lock().await;
        if io
            .run
            .as_ref()
            .is_none_or(|run| !run.connected.has_route(route_label))
        {
            return Ok(false);
        }
        prime_route_with_recover(&mut io, &self.runtime, route_label).await?;

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
            route_label,
            caused_by,
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

    pub fn command_state(&self, command_id: CommandId) -> crate::error::Result<Option<Value>> {
        let command_segment = command_id.get().to_string();
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["runtime", "commands", command_segment.as_str()])
            .map_err(Into::into)
    }

    pub fn command_status(&self, command_id: CommandId) -> crate::error::Result<Option<String>> {
        Ok(self.command_state(command_id)?.and_then(|command| {
            command
                .get("status")
                .and_then(Value::as_str)
                .map(str::to_owned)
        }))
    }

    /// Drives the substrate until the specified command reaches a completed
    /// terminal status.
    ///
    /// This helper only advances transport/runtime state for the submitted
    /// command. It does not impose `wait_update()` semantics or consume commit
    /// cursors on behalf of the caller.
    pub async fn wait_command_completed(&self, command_id: CommandId) -> crate::error::Result<()> {
        let started_at = Instant::now();
        loop {
            if self.command_completed(command_id)? {
                return Ok(());
            }

            let mut progress = false;

            progress |= self.flush_outbound().await?;
            if self.command_completed(command_id)? {
                return Ok(());
            }

            if let Some(route_label) = self.command_route_label(command_id)? {
                progress |= self
                    .drive_pending_route_label_once(route_label.as_str())
                    .await?;
                if self.command_completed(command_id)? {
                    return Ok(());
                }

                let websocket_progress = self
                    .drive_route_label_once(
                        route_label.as_str(),
                        Some(Instant::now() + WEBSOCKET_COMMAND_POLL_BUDGET),
                        vec![command_id],
                    )
                    .await?;
                progress |= websocket_progress;
                if !websocket_progress && started_at.elapsed() < WEBSOCKET_COMMAND_MAX_WAIT {
                    progress = true;
                }
            } else {
                progress |= self.drive_pending_once().await?;
                if self.command_completed(command_id)? {
                    return Ok(());
                }

                progress |= self.drive_route_once(None).await?;
            }
            if self.command_completed(command_id)? {
                return Ok(());
            }

            if !progress {
                return Err(crate::error::SessionFacadeError::InvalidState(
                    "command did not reach a terminal state",
                ));
            }
        }
    }

    fn command_route_label(&self, command_id: CommandId) -> crate::error::Result<Option<String>> {
        Ok(self.command_state(command_id)?.and_then(|command| {
            command
                .get("detail")
                .and_then(|detail| detail.get("route"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        }))
    }

    pub fn schema_value(&self, schema_id: &str) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["schema", schema_id])
            .map_err(Into::into)
    }

    pub fn auth_context(&self) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["system", "auth", "context"])
            .map_err(Into::into)
    }

    pub fn refreshed_auth(&self) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["system", "auth", "refreshed"])
            .map_err(Into::into)
    }

    pub async fn tqkq_login_command(&self) -> crate::error::Result<TradeLoginCommand> {
        self.tqkq_login_command_with_number(None).await
    }

    pub async fn tqkq_login_command_numbered(
        &self,
        number: u8,
    ) -> crate::error::Result<TradeLoginCommand> {
        self.tqkq_login_command_with_number(Some(number)).await
    }

    pub async fn tqkq_stock_login_command(&self) -> crate::error::Result<TradeLoginCommand> {
        self.tqkq_stock_login_command_with_number(None).await
    }

    pub async fn tqkq_stock_login_command_numbered(
        &self,
        number: u8,
    ) -> crate::error::Result<TradeLoginCommand> {
        self.tqkq_stock_login_command_with_number(Some(number))
            .await
    }

    pub async fn has_feature(&self, feature: &str) -> crate::error::Result<bool> {
        let auth = self.service_auth_context(false).await?;
        Ok(has_auth_feature(auth.features(), feature))
    }

    pub async fn check_md_grants(&self, symbols: &[&str]) -> crate::error::Result<()> {
        let auth = self.service_auth_context(false).await?;
        check_md_grants_for_features(auth.features(), symbols)
    }

    pub fn replay_state(&self, replay_id: &str) -> crate::error::Result<Option<Value>> {
        let guard = self.reader.read();
        guard
            .decode_path::<Value>(&["replay", replay_id])
            .map_err(Into::into)
    }

    fn command_completed(&self, command_id: CommandId) -> crate::error::Result<bool> {
        let Some(command) = self.command_state(command_id)? else {
            return Ok(false);
        };
        match command.get("status").and_then(Value::as_str) {
            Some("completed") => Ok(true),
            Some("rejected" | "failed" | "cancelled") => {
                Err(crate::error::SessionFacadeError::InvalidState(
                    "command reached a non-completed terminal status",
                ))
            }
            Some(_) | None => Ok(false),
        }
    }

    async fn require_query_value_route(&self) -> crate::error::Result<()> {
        let Some(io) = self.io.as_ref() else {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "query value helper requires an enabled query route",
            ));
        };
        let io = io.lock().await;
        if !io
            .config
            .enabled_domains()
            .contains(&tqsdk_core::ProtocolDomain::Query)
        {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "query value helper requires an enabled query route",
            ));
        }
        if io.config.endpoints.query_url.is_none() && !io.config.market_target.stock {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "websocket query helpers require stock market_target when query_url is not configured",
            ));
        }
        Ok(())
    }

    async fn require_replay_value_route(&self) -> crate::error::Result<()> {
        if let Some(io) = self.io.as_ref()
            && io
                .lock()
                .await
                .config
                .enabled_domains()
                .contains(&tqsdk_core::ProtocolDomain::Replay)
        {
            Ok(())
        } else {
            Err(crate::error::SessionFacadeError::InvalidState(
                "replay value helper requires an enabled replay route",
            ))
        }
    }

    async fn tqkq_login_command_with_number(
        &self,
        number: Option<u8>,
    ) -> crate::error::Result<TradeLoginCommand> {
        let auth_id = self.established_auth_id().await?;
        let config = if let Some(number) = number {
            TqKqAccountConfig::future_numbered(auth_id.as_str(), number)?
        } else {
            TqKqAccountConfig::future(auth_id.as_str())
        };
        Ok(config.login_command())
    }

    async fn tqkq_stock_login_command_with_number(
        &self,
        number: Option<u8>,
    ) -> crate::error::Result<TradeLoginCommand> {
        let auth_id = self.established_auth_id().await?;
        let config = if let Some(number) = number {
            TqKqAccountConfig::stock_numbered(auth_id.as_str(), number)?
        } else {
            TqKqAccountConfig::stock(auth_id.as_str())
        };
        Ok(config.login_command())
    }

    async fn established_auth_id(&self) -> crate::error::Result<String> {
        self.ensure_established().await?;
        let auth = self
            .auth_context()?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "session established without a system auth context payload",
            ))?;
        auth.get("auth_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "system auth context is missing auth_id",
            ))
    }

    async fn flush_outbound_locked(&self, io: &mut SessionIoState) -> crate::error::Result<bool> {
        if io.run.is_none() {
            return Ok(false);
        }
        let receipts = match self
            .runtime
            .flush_outbound(io.run.as_mut().expect("run checked above"))
            .await
        {
            Ok(receipts) => receipts,
            Err(tqsdk_core::ContractError::Transport(_)) => {
                recover_run(io, &self.runtime).await?;
                self.runtime
                    .flush_outbound(io.run.as_mut().expect("run recovered"))
                    .await?
            }
            Err(err) => return Err(err.into()),
        };
        Ok(!receipts.is_empty())
    }

    async fn drive_pending_once_locked(
        &self,
        io: &mut SessionIoState,
    ) -> crate::error::Result<bool> {
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

    async fn drive_route_once_locked(
        &self,
        io: &mut SessionIoState,
        deadline: Option<Instant>,
    ) -> crate::error::Result<bool> {
        let Some(route_label) = io.next_websocket_route_label() else {
            return Ok(false);
        };
        prime_route_with_recover(io, &self.runtime, route_label.as_str()).await?;

        let SessionIoState {
            auth_provider,
            topology_resolver,
            route_connector,
            adapters,
            config,
            run,
            ..
        } = io;
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
        self.require_query_value_route().await?;
        let query_id = Self::next_query_id();
        let command_id = self
            .submit(RuntimeCommand::Query(QueryCommand::Fetch {
                query_id: query_id.clone(),
                query: query.to_owned(),
                variables,
            }))
            .await?;

        self.wait_command_completed(command_id).await?;
        let value = self.query_result(query_id.as_str())?.ok_or(
            crate::error::SessionFacadeError::InvalidState(
                "query command completed without a result payload",
            ),
        )?;
        Self::validate_query_payload(&value)?;
        Ok(value)
    }

    pub async fn refresh_schema_value(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<Value> {
        let command_id = self
            .submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
                schema_id: SchemaId::new(schema_id),
                path: path.to_owned(),
            }))
            .await?;

        self.wait_command_completed(command_id).await?;
        self.schema_value(schema_id)?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "schema refresh completed without a schema payload",
            ))
    }

    pub async fn refresh_auth(&self) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::System(SystemCommand::RefreshAuth))
            .await
    }

    pub async fn refresh_auth_value(&self) -> crate::error::Result<Value> {
        let command_id = self.refresh_auth().await?;
        self.wait_command_completed(command_id).await?;
        self.refreshed_auth()?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "auth refresh completed without a refreshed auth payload",
            ))
    }

    pub async fn replay_step(&self) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Replay(ReplayCommand::Step))
            .await
    }

    pub async fn replay_step_value(&self, replay_id: &str) -> crate::error::Result<Value> {
        self.require_replay_value_route().await?;
        let command_id = self.replay_step().await?;
        self.wait_command_completed(command_id).await?;
        self.replay_state(replay_id)?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "replay step completed without a replay state payload",
            ))
    }

    pub async fn replay_reset(&self) -> crate::error::Result<CommandId> {
        self.submit(RuntimeCommand::Replay(ReplayCommand::Reset))
            .await
    }

    pub async fn replay_reset_value(&self, replay_id: &str) -> crate::error::Result<Value> {
        self.require_replay_value_route().await?;
        let command_id = self.replay_reset().await?;
        self.wait_command_completed(command_id).await?;
        self.replay_state(replay_id)?
            .ok_or(crate::error::SessionFacadeError::InvalidState(
                "replay reset completed without a replay state payload",
            ))
    }

    pub(crate) fn service_http(&self) -> &reqwest::Client {
        &self.service_http
    }

    pub(crate) fn service_endpoints(&self) -> &SessionServiceEndpoints {
        &self.context.service_endpoints
    }

    pub(crate) async fn service_auth_context(
        &self,
        force_refresh: bool,
    ) -> crate::error::Result<AuthContext> {
        let Some(io) = self.io.as_ref() else {
            return Err(crate::error::SessionFacadeError::InvalidState(
                "direct service helpers require a live session client",
            ));
        };

        let auth_provider = {
            let io = io.lock().await;
            if !force_refresh && let Some(auth) = io.cached_auth.as_ref() {
                return Ok(auth.clone());
            }
            io.auth_provider.clone()
        };

        let auth = auth_provider.authenticate_boxed().await?;
        io.lock().await.cached_auth = Some(auth.clone());
        Ok(auth)
    }

    #[doc(hidden)]
    pub fn new_for_test_with_handle(handle: RuntimeHandle) -> Self {
        Self::new_without_io(
            handle,
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
struct SessionReplayExecutor;

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

fn has_auth_feature(features: &[String], feature: &str) -> bool {
    features.iter().any(|item| item == feature)
}

fn check_md_grants_for_features(features: &[String], symbols: &[&str]) -> crate::error::Result<()> {
    for symbol in symbols {
        let prefix = symbol.split('.').next().unwrap_or_default();

        if LIMITED_INDEX_SYMBOLS.contains(symbol) {
            if has_auth_feature(features, "sec") || has_auth_feature(features, "lmt_idx") {
                continue;
            }
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::auth(format!(
                    "your account does not support market data for {symbol}"
                )),
            ));
        }

        if matches!(
            prefix,
            "CFFEX" | "SHFE" | "DCE" | "CZCE" | "INE" | "GFEX" | "SSWE" | "KQ" | "KQD"
        ) {
            if has_auth_feature(features, "futr") {
                continue;
            }
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::auth(format!(
                    "your account does not support futures market data for {symbol}"
                )),
            ));
        }

        if prefix == "CSI" || matches!(prefix, "SSE" | "SZSE") {
            if has_auth_feature(features, "sec") {
                continue;
            }
            return Err(crate::error::SessionFacadeError::from(
                tqsdk_core::ContractError::auth(format!(
                    "your account does not support stock market data for {symbol}"
                )),
            ));
        }

        return Err(crate::error::SessionFacadeError::from(
            tqsdk_core::ContractError::auth(format!(
                "unsupported market-data symbol namespace for {symbol}"
            )),
        ));
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::manual_async_fn)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use tokio::sync::Mutex as TokioMutex;
    use tokio::time::{Duration, Instant};
    use tqsdk_core::{
        AdapterRegistry, AuthContext, AuthId, AuthProvider, CommitScope, DynRouteConnectFuture,
        DynTransport, EndpointConfig, InputPayload, IoEvent, MarketCommand, OutboundDispatch,
        OutboundFrame, OutboundRequest, ProtocolDomain, QueryCommand, QueryId, RawFrame,
        ReplaySessionId, Result as CoreResult, RouteRequestExecutor, Runtime, RuntimeCommand,
        RuntimeHandle, RuntimeInput, SessionBootstrap, SessionConfig, SessionRoute,
        SessionRouteConnector, SessionRouteEndpoint, SessionRuntime, SessionTarget,
        SessionTopology, SessionTopologyResolver, TradeAccountType, Transport,
    };

    use super::{
        SessionClient, SessionClientContext, SessionInternalExecutor, SessionIoComponents,
        SessionIoState, SessionProgress, SessionReplayExecutor, SharedAuthProvider,
        SharedRouteConnector, SharedRouteExecutor, SharedTopologyResolver,
    };
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

        fn send(
            &mut self,
            frame: OutboundFrame,
        ) -> impl Future<Output = CoreResult<()>> + Send + '_ {
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
                    let Some(query_id) = sent.lock().unwrap().iter().find_map(outbound_query_id)
                    else {
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

        fn send(
            &mut self,
            frame: OutboundFrame,
        ) -> impl Future<Output = CoreResult<()>> + Send + '_ {
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
                .progress_once(Some(Instant::now() + Duration::from_millis(20)))
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
            handle.latest_snapshot().get(["query", "query-1", "quotes"]),
            Some(&json!(["SHFE.au2602"]))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_command_completed_errors_when_command_cannot_reach_terminal_state() {
        let handle = runtime_with_default_adapters();
        let client = SessionClient::new_for_test_with_handle(handle.clone());
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
        let client = SessionClient::new_for_test_with_handle(runtime_with_default_adapters());

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
        let client = SessionClient::new_for_test_with_handle(runtime_with_default_adapters());

        let error = client.replay_step_value("rb-test").await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid session facade state: replay value helper requires an enabled replay route"
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

    #[tokio::test(flavor = "current_thread")]
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
            service_http: reqwest::Client::new(),
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
}
