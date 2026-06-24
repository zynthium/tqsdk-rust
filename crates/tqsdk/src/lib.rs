#![cfg_attr(not(test), forbid(unsafe_code))]
//! User-facing facade crate for `tqsdk-rust`.
//!
//! This crate gives ordinary users one dependency and one prelude while keeping
//! the underlying `core` / `session` / `wait` / `stream` / `task` / `data`
//! boundaries available under [`advanced`].

use std::env;
use std::fmt;

/// Common imports for strategy-oriented users.
pub mod prelude {
    pub use crate::{Error, LOCAL_BACKTEST_ACCOUNT_ID, Result, TargetPos, Tq, TqBuilder};
    pub use tqsdk_wait::{AccountRef, PositionRef, QuoteRef, QuoteSet, WaitStep};
}

/// Explicit access to the underlying crates for advanced users.
pub mod advanced {
    pub mod core {
        pub use tqsdk_core::{Kline, Quote, Tick, TradeAccountType, TradeDirection, TradeOffset};
    }

    pub mod data {
        pub use tqsdk_data::{
            DataClient, DataError, KlineDataSeries, KlineDataSeriesRequest, TickDataSeries,
            TickDataSeriesRequest,
        };
    }

    pub mod runtime {
        pub use tqsdk_core::{CommitResult, RuntimeHandle, RuntimeReader, UpdateCursor};
    }

    pub mod session {
        pub type SessionClient = tqsdk_session::SessionClient;
        pub type SessionClientBuilder = tqsdk_session::SessionClientBuilder;
        pub type SessionFacadeError = tqsdk_session::SessionFacadeError;
    }

    pub mod stream {
        pub type TqStream = tqsdk_stream::TqStream;
        pub type TqStreamBuilder = tqsdk_stream::TqStreamBuilder;
    }

    pub mod task {
        pub use tqsdk_task::{
            LOCAL_BACKTEST_ACCOUNT_ID, OffsetPriority, PriceMode, ReplayMarketEvent,
            ReplayMarketPayload, ReplayMarketPayloadKind, ReplayMarketSource, StrategyBacktest,
            StrategyBacktestBalancePoint, StrategyBacktestEquityPoint, StrategyBacktestSummary,
            StrategyReplaySourceBuilder, TargetPosConfig, TargetPosTask,
            TargetPosTaskExecutionReport, TaskError, TaskHost, VolumeSplitPolicy,
        };
    }

    pub mod wait {
        pub use tqsdk_wait::{
            AccountRef, KlineHandle, KlineWindow, OrderPrice, OrderRef, OrderTicket,
            OrderTicketState, PositionRef, QuoteRef, QuoteSet, TickHandle, TickWindow, TqApi,
            TqApiBuilder, TradingStatusRef, WaitFacadeError, WaitStep,
        };
    }
}

/// Result type for the user-facing facade.
pub type Result<T> = std::result::Result<T, Error>;

/// Default account id used by [`Tq::local_backtest`].
pub const LOCAL_BACKTEST_ACCOUNT_ID: &str = tqsdk_task::LOCAL_BACKTEST_ACCOUNT_ID;

/// Error type for the user-facing facade.
#[derive(Debug)]
pub enum Error {
    MissingAuth,
    MissingAuthEnv {
        name: &'static str,
        source: env::VarError,
    },
    EmptyAuthEnv {
        name: &'static str,
    },
    Session(Box<tqsdk_session::SessionFacadeError>),
    Wait(Box<tqsdk_wait::WaitFacadeError>),
    Task(Box<tqsdk_task::TaskError>),
    Data(Box<tqsdk_data::DataError>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAuth => write!(
                f,
                "missing auth credentials; call auth(user, pass) or auth_env() before connect()"
            ),
            Self::MissingAuthEnv { name, .. } => {
                write!(f, "missing required environment variable {name}")
            }
            Self::EmptyAuthEnv { name } => {
                write!(f, "environment variable {name} must not be empty")
            }
            Self::Session(error) => write!(f, "{error}"),
            Self::Wait(error) => write!(f, "{error}"),
            Self::Task(error) => write!(f, "{error}"),
            Self::Data(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingAuth => None,
            Self::MissingAuthEnv { source, .. } => Some(source),
            Self::EmptyAuthEnv { .. } => None,
            Self::Session(error) => Some(&**error),
            Self::Wait(error) => Some(&**error),
            Self::Task(error) => Some(&**error),
            Self::Data(error) => Some(&**error),
        }
    }
}

impl From<tqsdk_session::SessionFacadeError> for Error {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(Box::new(error))
    }
}

impl From<tqsdk_wait::WaitFacadeError> for Error {
    fn from(error: tqsdk_wait::WaitFacadeError) -> Self {
        Self::Wait(Box::new(error))
    }
}

impl From<tqsdk_task::TaskError> for Error {
    fn from(error: tqsdk_task::TaskError) -> Self {
        Self::Task(Box::new(error))
    }
}

impl From<tqsdk_data::DataError> for Error {
    fn from(error: tqsdk_data::DataError) -> Self {
        Self::Data(Box::new(error))
    }
}

/// Strategy-oriented entrypoint.
///
/// `Tq` owns a wait-style API plus task host so common strategy loops can use
/// one object for `next()`, live refs, history access, and target-position tasks.
///
/// In local-backtest mode the inner driver switches to [`tqsdk_task::StrategyBacktest`]
/// while keeping the same public surface (`next()`, `quote()`, etc.).
pub struct Tq {
    inner: TqInner,
}

enum TqInner {
    Live(Box<tqsdk_task::TaskHost>),
    LocalBacktest(Box<tqsdk_task::StrategyBacktest>),
}

impl Tq {
    /// Create a new builder. This is the recommended entry point.
    #[allow(clippy::new_ret_no_self)]
    #[must_use]
    pub fn new() -> TqBuilder {
        TqBuilder::new()
    }

    /// Alias for [`Tq::new()`] — kept for backward compatibility.
    #[must_use]
    pub fn futures() -> TqBuilder {
        TqBuilder::new()
    }

    #[must_use]
    pub fn from_api(api: tqsdk_wait::TqApi) -> Self {
        Self {
            inner: TqInner::Live(Box::new(tqsdk_task::TaskHost::new(api))),
        }
    }

    fn from_local_backtest(backtest: tqsdk_task::StrategyBacktest) -> Self {
        Self {
            inner: TqInner::LocalBacktest(Box::new(backtest)),
        }
    }

    // ── Internal accessors (live mode only) ──

    fn task_host_live(&self) -> Option<&tqsdk_task::TaskHost> {
        match &self.inner {
            TqInner::Live(host) => Some(host),
            TqInner::LocalBacktest(_) => None,
        }
    }

    fn api_mut_any(&mut self) -> &mut tqsdk_wait::TqApi {
        match &mut self.inner {
            TqInner::Live(host) => host.api_mut(),
            TqInner::LocalBacktest(bt) => bt.strategy_mut().task_host_mut().api_mut(),
        }
    }

    fn api_any(&self) -> &tqsdk_wait::TqApi {
        match &self.inner {
            TqInner::Live(host) => host.api(),
            TqInner::LocalBacktest(bt) => bt.strategy().task_host().api(),
        }
    }

    // ── Public accessors ──

    /// Returns a reference to the underlying [`tqsdk_wait::TqApi`].
    ///
    /// Available in both live and local-backtest modes.
    #[must_use]
    pub fn api(&self) -> &tqsdk_wait::TqApi {
        self.api_any()
    }

    /// Returns a mutable reference to the underlying [`tqsdk_wait::TqApi`].
    #[must_use]
    pub fn api_mut(&mut self) -> &mut tqsdk_wait::TqApi {
        self.api_mut_any()
    }

    /// Returns the [`tqsdk_task::TaskHost`] (live mode only).
    ///
    /// Returns `None` in local-backtest mode.
    #[must_use]
    pub fn task_host(&self) -> Option<&tqsdk_task::TaskHost> {
        self.task_host_live()
    }

    /// Returns a reference to the [`tqsdk_session::SessionClient`] (live mode only).
    ///
    /// # Panics
    ///
    /// Panics if called in local-backtest mode (no session exists).
    #[must_use]
    pub fn session(&self) -> &tqsdk_session::SessionClient {
        self.api_any().session()
    }

    /// Create a [`tqsdk_data::DataClient`] from the underlying session.
    ///
    /// Only meaningful in live mode where a real session exists.
    #[must_use]
    pub fn history(&self) -> tqsdk_data::DataClient {
        tqsdk_data::DataClient::from_session(self.session().clone())
    }

    #[cfg(feature = "live")]
    pub async fn tqkq_account_id(&self) -> Result<String> {
        let login = self.session().tqkq_login_command().await?;
        Ok(login.account_id.as_str().to_owned())
    }

    #[cfg(feature = "live")]
    pub async fn tqkq_account_id_numbered(&self, number: u8) -> Result<String> {
        let login = self.session().tqkq_login_command_numbered(number).await?;
        Ok(login.account_id.as_str().to_owned())
    }

    #[cfg(feature = "live")]
    pub async fn target_pos_tqkq(&mut self, symbol: &str) -> Result<TargetPos> {
        let account_id = self.tqkq_account_id().await?;
        self.target_pos(&account_id, symbol)
    }

    #[cfg(feature = "live")]
    pub async fn target_pos_tqkq_numbered(
        &mut self,
        number: u8,
        symbol: &str,
    ) -> Result<TargetPos> {
        let account_id = self.tqkq_account_id_numbered(number).await?;
        self.target_pos(&account_id, symbol)
    }

    // ── Core loop ──

    /// Advance one step. Returns `false` when there are no more events
    /// (backtest finished or session closed).
    pub async fn next(&mut self) -> Result<bool> {
        match &mut self.inner {
            TqInner::Live(host) => host.wait_update(None).await.map_err(Error::from),
            TqInner::LocalBacktest(bt) => {
                let Some(mut ctx) = bt.next().await? else {
                    return Ok(false);
                };
                ctx.finish_sim_step()?;
                Ok(true)
            }
        }
    }

    /// Advance one step with a deadline (live mode). In local-backtest mode
    /// the deadline is ignored.
    pub async fn wait_update(&mut self, deadline: Option<tokio::time::Instant>) -> Result<bool> {
        match &mut self.inner {
            TqInner::Live(host) => host.wait_update(deadline).await.map_err(Error::from),
            TqInner::LocalBacktest(_) => self.next().await,
        }
    }

    // ── Market data ──

    pub async fn quote(&mut self, symbol: &str) -> Result<tqsdk_wait::QuoteRef> {
        self.api_mut_any().quote(symbol).await.map_err(Error::from)
    }

    pub async fn quotes<I, S>(&mut self, symbols: I) -> Result<tqsdk_wait::QuoteSet>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.api_mut_any()
            .quotes(symbols)
            .await
            .map_err(Error::from)
    }

    #[must_use]
    pub fn account(&self, account_id: &str) -> tqsdk_wait::AccountRef {
        self.api_any().account(account_id)
    }

    #[must_use]
    pub fn position(&self, account_id: &str, symbol: &str) -> tqsdk_wait::PositionRef {
        self.api_any().position(account_id, symbol)
    }

    pub fn target_pos(&mut self, account_id: &str, symbol: &str) -> Result<TargetPos> {
        match &mut self.inner {
            TqInner::Live(host) => {
                let task = host.target_pos(account_id, symbol).build()?;
                Ok(TargetPos::new(task))
            }
            TqInner::LocalBacktest(backtest) => {
                let task = backtest
                    .strategy_mut()
                    .task_host_mut()
                    .target_pos(account_id, symbol)
                    .build()?;
                Ok(TargetPos::new(task))
            }
        }
    }

    /// Returns `true` if this `Tq` is in local-backtest mode.
    #[must_use]
    pub fn is_local_backtest(&self) -> bool {
        matches!(self.inner, TqInner::LocalBacktest(_))
    }

    /// Returns the backtest summary (local-backtest mode only).
    pub fn backtest_summary(&self) -> Option<tqsdk_task::StrategyBacktestSummary> {
        match &self.inner {
            TqInner::LocalBacktest(bt) => Some(bt.summary()),
            TqInner::Live(_) => None,
        }
    }
}

/// Builder for [`Tq`].
#[derive(Debug)]
pub struct TqBuilder {
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
    market_url: Option<String>,
    backtest: Option<BacktestConfig>,
    quote_symbols: Vec<String>,
    price_ticks: std::collections::HashMap<String, f64>,
    default_price_tick: Option<f64>,
}

impl TqBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            auth: None,
            query_enabled: false,
            trade_targets: Vec::new(),
            market_url: None,
            backtest: None,
            quote_symbols: Vec::new(),
            price_ticks: std::collections::HashMap::new(),
            default_price_tick: None,
        }
    }

    /// Alias for [`TqBuilder::new()`].
    #[must_use]
    pub fn futures() -> Self {
        Self::new()
    }

    #[must_use]
    pub fn market_relay(mut self, relay_url: impl Into<String>) -> Self {
        self.market_url = Some(relay_url.into());
        self
    }

    /// Enter server-side backtest mode (≈ Python `TqBacktest`).
    ///
    /// The strategy body (`next()` / `quote()` / etc.) stays identical to live.
    #[must_use]
    pub fn backtest(mut self, start_ns: i64, end_ns: i64) -> Self {
        self.backtest = Some(BacktestConfig::Server { start_ns, end_ns });
        self
    }

    /// Enter local-backtest mode using the provided replay cache.
    ///
    /// Uses [`tqsdk_task::TqSim`] for matching. The strategy body stays identical to live.
    #[must_use]
    pub fn local_backtest(mut self, replay: tqsdk_task::ReplayMarketSource) -> Self {
        self.backtest = Some(BacktestConfig::Local { replay });
        self
    }

    /// Enter local-backtest mode from owned kline history series.
    ///
    /// This is a convenience wrapper around [`tqsdk_task::StrategyReplaySourceBuilder`].
    pub fn local_backtest_klines(
        self,
        series: impl IntoIterator<Item = tqsdk_data::KlineDataSeries>,
    ) -> Result<Self> {
        let mut builder = tqsdk_task::StrategyReplaySourceBuilder::new();
        for series in series {
            builder = builder.kline_series(series, "history-kline")?;
        }
        Ok(self.local_backtest(builder.build()))
    }

    /// Enter local-backtest mode from owned tick history series.
    ///
    /// This is a convenience wrapper around [`tqsdk_task::StrategyReplaySourceBuilder`].
    pub fn local_backtest_ticks(
        self,
        series: impl IntoIterator<Item = tqsdk_data::TickDataSeries>,
    ) -> Result<Self> {
        let mut builder = tqsdk_task::StrategyReplaySourceBuilder::new();
        for series in series {
            builder = builder.tick_series(series, "history-tick")?;
        }
        Ok(self.local_backtest(builder.build()))
    }

    /// Pre-declare a symbol for local backtest.
    #[must_use]
    pub fn quote_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.quote_symbols.push(symbol.into());
        self
    }

    /// Pre-declare a price tick for local backtest (required if replay contains klines).
    #[must_use]
    pub fn price_tick(mut self, symbol: impl Into<String>, tick: f64) -> Self {
        self.price_ticks.insert(symbol.into(), tick);
        self
    }

    /// Set fallback price tick for local-backtest kline quote synthesis.
    ///
    /// Per-symbol [`TqBuilder::price_tick`] overrides this fallback.
    #[must_use]
    pub fn default_price_tick(mut self, tick: f64) -> Self {
        self.default_price_tick = Some(tick);
        self
    }

    #[must_use]
    pub fn auth(mut self, user: impl Into<String>, pass: impl Into<String>) -> Self {
        self.auth = Some(Auth {
            user: user.into(),
            pass: pass.into(),
        });
        self
    }

    pub fn auth_env(self) -> Result<Self> {
        let user = read_env("TQ_AUTH_USER")?;
        let pass = read_env("TQ_AUTH_PASS")?;
        Ok(self.auth(user, pass))
    }

    #[must_use]
    pub fn enable_query(mut self) -> Self {
        self.query_enabled = true;
        self
    }

    #[must_use]
    pub fn trade_target(
        mut self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> Self {
        self.trade_targets.push(TradeTarget::Custom {
            broker_id: broker_id.into(),
            account_id: account_id.into(),
            trade_url: None,
        });
        self
    }

    #[must_use]
    pub fn trade_target_with_url(
        mut self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
        trade_url: impl Into<String>,
    ) -> Self {
        self.trade_targets.push(TradeTarget::Custom {
            broker_id: broker_id.into(),
            account_id: account_id.into(),
            trade_url: Some(trade_url.into()),
        });
        self
    }

    #[must_use]
    pub fn trade_target_tqkq(mut self) -> Self {
        self.trade_targets.push(TradeTarget::TqKq);
        self
    }

    #[must_use]
    pub fn trade_target_tqkq_numbered(mut self, number: u8) -> Self {
        self.trade_targets.push(TradeTarget::TqKqNumbered(number));
        self
    }

    pub async fn connect(self) -> Result<Tq> {
        let Self {
            auth,
            query_enabled,
            trade_targets,
            market_url,
            backtest,
            quote_symbols,
            price_ticks,
            default_price_tick,
        } = self;

        match backtest {
            Some(BacktestConfig::Local { replay }) => {
                connect_local_backtest(replay, quote_symbols, price_ticks, default_price_tick).await
            }
            backtest => {
                connect_wait_facade(auth, query_enabled, trade_targets, market_url, backtest).await
            }
        }
    }
}

impl Default for TqBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Thin ergonomic wrapper around [`tqsdk_task::TargetPosTask`].
#[derive(Clone)]
pub struct TargetPos {
    inner: tqsdk_task::TargetPosTask,
}

impl TargetPos {
    #[must_use]
    pub fn new(inner: tqsdk_task::TargetPosTask) -> Self {
        Self { inner }
    }

    #[must_use]
    pub fn inner(&self) -> &tqsdk_task::TargetPosTask {
        &self.inner
    }

    #[must_use]
    pub fn into_inner(self) -> tqsdk_task::TargetPosTask {
        self.inner
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    #[must_use]
    pub fn current_target_volume(&self) -> Option<i64> {
        self.inner.current_target_volume()
    }

    #[must_use]
    pub fn last_error(&self) -> Option<tqsdk_task::TaskError> {
        self.inner.last_error()
    }

    #[must_use]
    pub fn execution_report(&self) -> tqsdk_task::TargetPosTaskExecutionReport {
        self.inner.execution_report()
    }

    pub fn set(&self, volume: i64) -> Result<()> {
        self.inner.set_target_volume(volume).map_err(Error::from)
    }

    pub fn close(&self) -> Result<()> {
        self.set(0)
    }
}

#[derive(Debug, Clone)]
struct Auth {
    user: String,
    pass: String,
}

enum BacktestConfig {
    Server {
        start_ns: i64,
        end_ns: i64,
    },
    Local {
        replay: tqsdk_task::ReplayMarketSource,
    },
}

impl std::fmt::Debug for BacktestConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server { start_ns, end_ns } => f
                .debug_struct("Server")
                .field("start_ns", start_ns)
                .field("end_ns", end_ns)
                .finish(),
            Self::Local { .. } => f.debug_struct("Local").finish_non_exhaustive(),
        }
    }
}

async fn connect_local_backtest(
    replay: tqsdk_task::ReplayMarketSource,
    quote_symbols: Vec<String>,
    price_ticks: std::collections::HashMap<String, f64>,
    default_price_tick: Option<f64>,
) -> Result<Tq> {
    let mut builder = tqsdk_task::StrategyBacktest::builder(replay);
    if let Some(default_price_tick) = default_price_tick {
        builder = builder.default_price_tick(default_price_tick);
    }
    for symbol in &quote_symbols {
        builder = builder.quote(symbol);
    }
    for (symbol, tick) in &price_ticks {
        builder = builder.price_tick(symbol, *tick);
    }
    let backtest = builder.build().await?;
    Ok(Tq::from_local_backtest(backtest))
}

async fn connect_wait_facade(
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
    market_url: Option<String>,
    backtest: Option<BacktestConfig>,
) -> Result<Tq> {
    let session_builder = session_builder(auth, query_enabled, trade_targets, market_url)?;
    let mut wait_builder = tqsdk_wait::TqApiBuilder::from_session_builder(session_builder);
    if let Some(BacktestConfig::Server { start_ns, end_ns }) = backtest {
        wait_builder = wait_builder.futures_backtest(start_ns, end_ns)?;
    }
    let api = wait_builder.build().await?;
    Ok(Tq::from_api(api))
}

fn session_builder(
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
    market_url: Option<String>,
) -> Result<tqsdk_session::SessionClientBuilder> {
    let auth = auth.ok_or(Error::MissingAuth)?;
    let mut builder =
        tqsdk_session::SessionClientBuilder::new(&auth.user, &auth.pass).futures_market();
    if let Some(market_url) = market_url {
        builder = builder.market_relay(market_url);
    }
    if query_enabled {
        builder = builder.enable_query();
    }
    for target in trade_targets {
        builder = target.apply(builder);
    }
    Ok(builder)
}

#[derive(Debug, Clone)]
enum TradeTarget {
    Custom {
        broker_id: String,
        account_id: String,
        trade_url: Option<String>,
    },
    TqKq,
    TqKqNumbered(u8),
}

impl TradeTarget {
    fn apply(
        self,
        builder: tqsdk_session::SessionClientBuilder,
    ) -> tqsdk_session::SessionClientBuilder {
        match self {
            Self::Custom {
                broker_id,
                account_id,
                trade_url: Some(trade_url),
            } => builder.trade_target_with_url(broker_id, account_id, trade_url),
            Self::Custom {
                broker_id,
                account_id,
                trade_url: None,
            } => builder.trade_target(broker_id, account_id),
            Self::TqKq => builder.trade_target_tqkq(),
            Self::TqKqNumbered(number) => builder.trade_target_tqkq_numbered(number),
        }
    }
}

fn read_env(name: &'static str) -> Result<String> {
    let value = env::var(name).map_err(|source| Error::MissingAuthEnv { name, source })?;
    parse_env_value(name, value)
}

fn parse_env_value(name: &'static str, value: String) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::EmptyAuthEnv { name });
    }
    Ok(trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{Error, parse_env_value};

    #[test]
    fn parse_env_value_trims_non_empty_credentials() {
        assert_eq!(
            parse_env_value("TQ_AUTH_USER", "  demo-user  ".to_string()).unwrap(),
            "demo-user"
        );
    }

    #[test]
    fn parse_env_value_rejects_empty_credentials() {
        assert!(matches!(
            parse_env_value("TQ_AUTH_PASS", "   ".to_string()),
            Err(Error::EmptyAuthEnv {
                name: "TQ_AUTH_PASS"
            })
        ));
    }
}

#[cfg(test)]
mod builder_contract_tests {
    use super::{Error, TqBuilder};

    #[tokio::test]
    async fn local_backtest_connect_does_not_require_auth() {
        let replay = tqsdk_task::ReplayMarketSource::new(Vec::new());

        let result = TqBuilder::new().local_backtest(replay).connect().await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn live_connect_requires_auth() {
        let result = TqBuilder::new().connect().await;

        assert!(matches!(result, Err(Error::MissingAuth)));
    }
}
