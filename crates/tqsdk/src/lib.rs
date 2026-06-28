#![cfg_attr(not(test), forbid(unsafe_code))]
//! User-facing facade crate for `tqsdk-rust`.
//!
//! This crate gives ordinary users one dependency and one prelude while keeping
//! the underlying `core` / `session` / `wait` / `task` / `data`
//! boundaries available under [`advanced`].

use std::env;
use std::fmt;
use std::sync::{Arc, Mutex};
#[cfg(all(feature = "services", feature = "live"))]
use std::time::Duration;

#[cfg(all(feature = "services", feature = "live"))]
use chrono::NaiveDate;

/// Common imports for strategy-oriented users.
pub mod prelude {
    pub use crate::{
        BacktestBuilder, BacktestCachePolicy, BacktestDataReport, BacktestTickCache, Error,
        LOCAL_BACKTEST_ACCOUNT_ID, PreparedBacktest, Result, TargetPos, Tq, TqBuilder,
    };
    pub use tqsdk_wait::{AccountRef, PositionRef, QuoteRef, QuoteSet, WaitStep};
}

/// Explicit access to the underlying crates for advanced users.
pub mod advanced {
    pub mod core {
        pub use tqsdk_core::{Kline, Quote, Tick, TradeAccountType, TradeDirection, TradeOffset};
    }

    pub mod data {
        pub use tqsdk_data::{
            BacktestTickCache, DataClient, DataError, HistoricalContUnderlyingRow,
            HistoricalContUnderlyingSegment, KlineDataSeries, KlineDataSeriesRequest,
            TickDataSeries, TickDataSeriesRequest, TradingCalendarRow,
            historical_cont_underlying_segments,
        };
    }

    pub mod runtime {
        pub use tqsdk_core::{CommitResult, RuntimeHandle, RuntimeReader, UpdateCursor};
    }

    pub mod session {
        pub use tqsdk_session::{InstrumentClass, InstrumentSpec, SymbolInfo};
        #[cfg(all(feature = "services", feature = "live"))]
        pub use tqsdk_session::{ServerReplayBuilder, ServerReplaySession};

        pub type SessionClient = tqsdk_session::SessionClient;
        pub type SessionClientBuilder = tqsdk_session::SessionClientBuilder;
        pub type SessionFacadeError = tqsdk_session::SessionFacadeError;
    }

    pub mod task {
        pub mod backtest {
            pub use tqsdk_task::backtest::*;
        }

        pub mod replay {
            pub use tqsdk_task::replay::*;
        }

        pub mod sim {
            pub use tqsdk_task::sim::*;
        }

        pub use tqsdk_task::{
            LOCAL_BACKTEST_ACCOUNT_ID, OffsetPriority, PriceMode, ReplayMarketEvent,
            ReplayMarketPayload, ReplayMarketPayloadKind, ReplayMarketSource, StrategyBacktest,
            StrategyBacktestBalancePoint, StrategyBacktestClosedProfitPoint,
            StrategyBacktestDailyBalanceReturn, StrategyBacktestDailyEquityReturn,
            StrategyBacktestDailyReturnWindow, StrategyBacktestEquityPoint,
            StrategyBacktestPerformanceMetrics, StrategyBacktestPerformanceReport,
            StrategyBacktestRollingRatioPoint, StrategyBacktestSummary,
            StrategyReplaySourceBuilder, TargetPosConfig, TargetPosTask,
            TargetPosTaskExecutionEvent, TargetPosTaskExecutionReport, TargetPosTaskOrderReport,
            TargetPosTaskReachedTarget, TargetPosTaskTradeFill, TaskError, TaskHost,
            VolumeSplitPolicy,
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

mod backtest_remote;
mod local_backtest;

pub use tqsdk_data::{BacktestCachePolicy, BacktestTickCache};

/// Result type for the user-facing facade.
pub type Result<T> = std::result::Result<T, Error>;

/// Default account id used by local simulated backtests.
pub const LOCAL_BACKTEST_ACCOUNT_ID: &str = tqsdk_task::sim::LOCAL_BACKTEST_ACCOUNT_ID;

#[cfg(all(feature = "services", feature = "live"))]
const SERVER_REPLAY_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(all(feature = "services", feature = "live"))]
const SERVER_REPLAY_TERMINATE_TIMEOUT: Duration = Duration::from_secs(5);

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
/// In local-backtest mode the inner driver switches to [`tqsdk_task::backtest::StrategyBacktest`]
/// while keeping the same public surface (`next()`, `quote()`, etc.).
pub struct Tq {
    inner: TqInner,
    default_account_id: DefaultAccountId,
    #[cfg(feature = "live")]
    server_side_backtest: bool,
    #[cfg(all(feature = "services", feature = "live"))]
    server_replay: Option<tqsdk_session::ServerReplaySession>,
    #[cfg(all(feature = "services", feature = "live"))]
    server_replay_heartbeat: Option<tokio::task::JoinHandle<()>>,
}

enum TqInner {
    Live(Box<tqsdk_task::TaskHost>),
    LocalBacktest(Box<tqsdk_task::backtest::StrategyBacktest>),
}

enum DefaultAccountId {
    None,
    Single(String),
    #[cfg(feature = "live")]
    Ambiguous,
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
            default_account_id: DefaultAccountId::None,
            #[cfg(feature = "live")]
            server_side_backtest: false,
            #[cfg(all(feature = "services", feature = "live"))]
            server_replay: None,
            #[cfg(all(feature = "services", feature = "live"))]
            server_replay_heartbeat: None,
        }
    }

    fn from_api_with_server_backtest(api: tqsdk_wait::TqApi) -> Self {
        Self {
            inner: TqInner::Live(Box::new(tqsdk_task::TaskHost::new(api))),
            default_account_id: DefaultAccountId::None,
            #[cfg(feature = "live")]
            server_side_backtest: true,
            #[cfg(all(feature = "services", feature = "live"))]
            server_replay: None,
            #[cfg(all(feature = "services", feature = "live"))]
            server_replay_heartbeat: None,
        }
    }

    #[cfg(all(feature = "services", feature = "live"))]
    fn from_api_with_server_replay(
        api: tqsdk_wait::TqApi,
        server_replay: tqsdk_session::ServerReplaySession,
    ) -> Self {
        let server_replay_heartbeat = Some(spawn_server_replay_heartbeat(&server_replay));
        Self {
            inner: TqInner::Live(Box::new(tqsdk_task::TaskHost::new(api))),
            default_account_id: DefaultAccountId::None,
            #[cfg(feature = "live")]
            server_side_backtest: true,
            server_replay: Some(server_replay),
            server_replay_heartbeat,
        }
    }

    fn from_local_backtest(backtest: tqsdk_task::backtest::StrategyBacktest) -> Self {
        Self {
            inner: TqInner::LocalBacktest(Box::new(backtest)),
            default_account_id: DefaultAccountId::Single(LOCAL_BACKTEST_ACCOUNT_ID.to_string()),
            #[cfg(feature = "live")]
            server_side_backtest: false,
            #[cfg(all(feature = "services", feature = "live"))]
            server_replay: None,
            #[cfg(all(feature = "services", feature = "live"))]
            server_replay_heartbeat: None,
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

    #[must_use]
    pub fn default_account_id_opt(&self) -> Option<&str> {
        match &self.default_account_id {
            DefaultAccountId::Single(account_id) => Some(account_id.as_str()),
            DefaultAccountId::None => None,
            #[cfg(feature = "live")]
            DefaultAccountId::Ambiguous => None,
        }
    }

    pub fn default_account_id(&self) -> Result<&str> {
        match &self.default_account_id {
            DefaultAccountId::Single(account_id) => Ok(account_id.as_str()),
            DefaultAccountId::None => Err(missing_default_account()),
            #[cfg(feature = "live")]
            DefaultAccountId::Ambiguous => Err(ambiguous_default_account()),
        }
    }

    #[cfg(all(feature = "services", feature = "live"))]
    #[must_use]
    pub fn server_replay_session(&self) -> Option<&tqsdk_session::ServerReplaySession> {
        self.server_replay.as_ref()
    }

    #[cfg(all(test, feature = "services", feature = "live"))]
    fn server_replay_heartbeat_active(&self) -> bool {
        self.server_replay_heartbeat
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    #[cfg(all(feature = "services", feature = "live"))]
    pub async fn set_replay_speed(&self, speed: f64) -> Result<()> {
        self.require_server_replay_session()?
            .set_speed(speed)
            .await
            .map_err(Error::from)
    }

    #[cfg(all(feature = "services", feature = "live"))]
    pub async fn send_replay_heartbeat(&self) -> Result<()> {
        self.require_server_replay_session()?
            .heartbeat()
            .await
            .map_err(Error::from)
    }

    #[cfg(all(feature = "services", feature = "live"))]
    pub async fn terminate_server_replay(&mut self) -> Result<()> {
        let session = self.server_replay.take().ok_or_else(|| {
            Error::from(tqsdk_session::SessionFacadeError::InvalidState(
                "server replay control requires server_replay mode",
            ))
        })?;
        self.abort_server_replay_heartbeat();
        let result = session.terminate().await;
        if result.is_err() {
            self.server_replay = Some(session);
        }
        result.map_err(Error::from)
    }

    #[cfg(all(feature = "services", feature = "live"))]
    fn abort_server_replay_heartbeat(&mut self) {
        if let Some(handle) = self.server_replay_heartbeat.take() {
            handle.abort();
        }
    }

    #[cfg(all(feature = "services", feature = "live"))]
    fn require_server_replay_session(&self) -> Result<&tqsdk_session::ServerReplaySession> {
        self.server_replay_session().ok_or_else(|| {
            Error::from(tqsdk_session::SessionFacadeError::InvalidState(
                "server replay control requires server_replay mode",
            ))
        })
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

    #[cfg(feature = "live")]
    pub async fn login_trade_account(
        &mut self,
        broker_id: &str,
        account_id: &str,
        password: &str,
    ) -> Result<tqsdk_wait::AccountRef> {
        if self.server_side_backtest {
            return Err(server_side_trade_login_error());
        }
        let account = self
            .api_mut_any()
            .login_trade_account(
                broker_id,
                account_id,
                password,
                tqsdk_core::TradeAccountType::Future,
                None,
            )
            .await?;
        self.note_trade_login(account_id);
        Ok(account)
    }

    #[cfg(feature = "live")]
    pub async fn login_tqkq_account(&mut self) -> Result<tqsdk_wait::AccountRef> {
        let login = self.session().tqkq_login_command().await?;
        self.login_trade_account(
            login.broker_id.as_str(),
            login.account_id.as_str(),
            login.password.as_str(),
        )
        .await
    }

    #[cfg(feature = "live")]
    pub async fn login_tqkq_account_numbered(
        &mut self,
        number: u8,
    ) -> Result<tqsdk_wait::AccountRef> {
        let login = self.session().tqkq_login_command_numbered(number).await?;
        self.login_trade_account(
            login.broker_id.as_str(),
            login.account_id.as_str(),
            login.password.as_str(),
        )
        .await
    }

    #[cfg(feature = "live")]
    fn note_trade_login(&mut self, account_id: &str) {
        match &mut self.default_account_id {
            DefaultAccountId::None => {
                self.default_account_id = DefaultAccountId::Single(account_id.to_owned());
            }
            DefaultAccountId::Single(existing) if existing == account_id => {}
            DefaultAccountId::Single(_) | DefaultAccountId::Ambiguous => {
                self.default_account_id = DefaultAccountId::Ambiguous;
            }
        }
    }

    #[cfg(all(test, feature = "live"))]
    fn note_trade_login_for_test(&mut self, account_id: &str) {
        self.note_trade_login(account_id);
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

    pub fn account_default(&self) -> Result<tqsdk_wait::AccountRef> {
        Ok(self.account(self.default_account_id()?))
    }

    #[must_use]
    pub fn position(&self, account_id: &str, symbol: &str) -> tqsdk_wait::PositionRef {
        self.api_any().position(account_id, symbol)
    }

    pub fn position_default(&self, symbol: &str) -> Result<tqsdk_wait::PositionRef> {
        Ok(self.position(self.default_account_id()?, symbol))
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

    pub fn target_pos_default(&mut self, symbol: &str) -> Result<TargetPos> {
        let account_id = self.default_account_id()?.to_owned();
        self.target_pos(account_id.as_str(), symbol)
    }

    /// Returns `true` if this `Tq` is in local-backtest mode.
    #[must_use]
    pub fn is_local_backtest(&self) -> bool {
        matches!(self.inner, TqInner::LocalBacktest(_))
    }

    /// Returns the backtest summary (local-backtest mode only).
    pub fn backtest_summary(&self) -> Option<tqsdk_task::backtest::StrategyBacktestSummary> {
        match &self.inner {
            TqInner::LocalBacktest(bt) => Some(bt.summary()),
            TqInner::Live(_) => None,
        }
    }

    /// Returns a balance-based backtest performance snapshot (local-backtest mode only).
    pub fn backtest_performance_metrics(
        &self,
    ) -> Option<tqsdk_task::backtest::StrategyBacktestPerformanceMetrics> {
        self.backtest_summary()
            .map(|summary| summary.performance_metrics())
    }

    /// Returns a typed backtest performance report (local-backtest mode only).
    pub fn backtest_performance_report(
        &self,
        rolling_window_len: usize,
    ) -> Option<tqsdk_task::backtest::StrategyBacktestPerformanceReport> {
        self.backtest_summary()
            .map(|summary| summary.performance_report(rolling_window_len))
    }
}

#[cfg(all(feature = "services", feature = "live"))]
impl Drop for Tq {
    fn drop(&mut self) {
        self.abort_server_replay_heartbeat();
        if let Some(session) = self.server_replay.take() {
            spawn_server_replay_terminate(session);
        }
    }
}

#[cfg(all(feature = "services", feature = "live"))]
fn spawn_server_replay_heartbeat(
    replay_session: &tqsdk_session::ServerReplaySession,
) -> tokio::task::JoinHandle<()> {
    let replay_session = replay_session.clone();
    tokio::spawn(async move {
        let _ = replay_session.set_speed(1.0).await;
        loop {
            tokio::time::sleep(SERVER_REPLAY_HEARTBEAT_INTERVAL).await;
            let _ = replay_session.heartbeat().await;
        }
    })
}

#[cfg(all(feature = "services", feature = "live"))]
fn spawn_server_replay_terminate(session: tqsdk_session::ServerReplaySession) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                let _ = tokio::time::timeout(SERVER_REPLAY_TERMINATE_TIMEOUT, session.terminate())
                    .await;
            });
        });
        return;
    };
    handle.spawn(async move {
        let _ = tokio::time::timeout(SERVER_REPLAY_TERMINATE_TIMEOUT, session.terminate()).await;
    });
}

#[cfg(feature = "live")]
async fn apply_auto_trade_login(tq: &mut Tq, login: AutoTradeLogin) -> Result<()> {
    match login {
        AutoTradeLogin::Futures {
            broker_id,
            account_id,
            password,
        } => {
            tq.login_trade_account(broker_id.as_str(), account_id.as_str(), password.as_str())
                .await?;
        }
        AutoTradeLogin::TqKq { number: None } => {
            tq.login_tqkq_account().await?;
        }
        AutoTradeLogin::TqKq {
            number: Some(number),
        } => {
            tq.login_tqkq_account_numbered(number).await?;
        }
    }
    Ok(())
}

/// Builder for cache-backed local backtests.
pub struct BacktestBuilder {
    base: TqBuilder,
    start_ns: i64,
    end_ns: i64,
    cache: Option<tqsdk_data::BacktestTickCache>,
    cache_policy: BacktestCachePolicy,
    symbols: Vec<String>,
}

/// Cache-prepared local backtest that can be connected without remote access.
pub struct PreparedBacktest {
    builder: BacktestBuilder,
    data_report: BacktestDataReport,
    mode: PreparedBacktestMode,
}

enum PreparedBacktestMode {
    CacheHit,
    RemoteCaching { symbols: Vec<String> },
}

/// Minimal data preparation report for a cache-backed local backtest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestDataReport {
    pub requested_range: (i64, i64),
    pub cache_policy: BacktestCachePolicy,
    pub cache_dir: std::path::PathBuf,
    pub resolved_symbols: usize,
    pub remote_used: bool,
}

impl BacktestBuilder {
    /// Set the cache policy for data preparation.
    #[must_use]
    pub fn cache(mut self, policy: BacktestCachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    /// Set the persistent tick cache used to prepare this backtest.
    #[must_use]
    pub fn cache_store(mut self, cache: tqsdk_data::BacktestTickCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Open and set the persistent tick cache directory used by this backtest.
    pub fn cache_dir(mut self, root_dir: impl AsRef<std::path::Path>) -> Result<Self> {
        self.cache = Some(tqsdk_data::BacktestTickCache::open(root_dir)?);
        Ok(self)
    }

    /// Require all backtest ticks to already exist in the persistent cache.
    #[must_use]
    pub fn cache_only(self) -> Self {
        self.cache(BacktestCachePolicy::CacheOnly)
    }

    /// Use remote data for missing tick ranges.
    #[must_use]
    pub fn remote_on_miss(self) -> Self {
        self.cache(BacktestCachePolicy::RemoteOnMiss)
    }

    /// Disable the persistent cache path.
    #[must_use]
    pub fn disabled_cache(self) -> Self {
        self.cache(BacktestCachePolicy::Disabled)
    }

    /// Refresh the full requested tick range before running the backtest.
    #[must_use]
    pub fn refresh(self) -> Self {
        self.cache(BacktestCachePolicy::Refresh)
    }

    /// Add a symbol whose tick history must be present in the cache.
    #[must_use]
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        let symbol = symbol.into();
        if !self.symbols.iter().any(|existing| existing == &symbol) {
            self.symbols.push(symbol);
        }
        self
    }

    /// Validate cache coverage and prepare the local replay inputs.
    pub async fn prepare(self) -> Result<PreparedBacktest> {
        if self.end_ns <= self.start_ns {
            return Err(data_validation(
                "backtest end_ns must be greater than start_ns",
            ));
        }
        if self.symbols.is_empty() {
            return Err(data_validation(
                "cache-backed backtest requires at least one symbol in phase 1",
            ));
        }
        if self.symbols.iter().any(String::is_empty) {
            return Err(data_validation(
                "cache-backed backtest symbol must not be empty",
            ));
        }
        let cache = self
            .cache
            .as_ref()
            .ok_or_else(|| data_validation("backtest cache is required in phase 1"))?;
        let mut missing_symbols = Vec::new();
        for symbol in &self.symbols {
            let coverage = cache.coverage(symbol, self.start_ns, self.end_ns)?;
            if !coverage.is_complete() {
                missing_symbols.push(coverage);
            }
        }

        let mode = match self.cache_policy {
            BacktestCachePolicy::CacheOnly => {
                if let Some(coverage) = missing_symbols.first() {
                    return Err(data_validation(format!(
                        "backtest cache coverage is incomplete for {}: {:?}",
                        coverage.symbol, coverage.missing_ranges
                    )));
                }
                PreparedBacktestMode::CacheHit
            }
            BacktestCachePolicy::RemoteOnMiss => {
                if missing_symbols.is_empty() {
                    PreparedBacktestMode::CacheHit
                } else {
                    if self.base.auth.is_none() {
                        return Err(data_validation("remote backtest cache fill requires auth"));
                    }
                    PreparedBacktestMode::RemoteCaching {
                        symbols: self.symbols.clone(),
                    }
                }
            }
            BacktestCachePolicy::Refresh => {
                if self.base.auth.is_none() {
                    return Err(data_validation("remote backtest cache fill requires auth"));
                }
                PreparedBacktestMode::RemoteCaching {
                    symbols: self.symbols.clone(),
                }
            }
            BacktestCachePolicy::Disabled => {
                return Err(data_validation(format!(
                    "backtest cache policy {:?} is not supported in phase 1",
                    self.cache_policy
                )));
            }
        };

        let data_report = BacktestDataReport {
            requested_range: (self.start_ns, self.end_ns),
            cache_policy: self.cache_policy,
            cache_dir: cache.history_cache().root_dir().to_path_buf(),
            resolved_symbols: self.symbols.len(),
            remote_used: matches!(mode, PreparedBacktestMode::RemoteCaching { .. }),
        };
        Ok(PreparedBacktest {
            builder: self,
            data_report,
            mode,
        })
    }

    /// Prepare and connect the cache-backed local backtest.
    pub async fn connect(self) -> Result<Tq> {
        self.prepare().await?.connect().await
    }
}

impl PreparedBacktest {
    /// Return the cache preparation report.
    #[must_use]
    pub fn data_report(&self) -> &BacktestDataReport {
        &self.data_report
    }

    /// Connect the prepared local backtest without remote access.
    pub async fn connect(self) -> Result<Tq> {
        let PreparedBacktest {
            builder,
            data_report: _,
            mode,
        } = self;
        let BacktestBuilder {
            base,
            start_ns,
            end_ns,
            cache,
            cache_policy: _,
            symbols,
        } = builder;
        let cache = cache.ok_or_else(|| data_validation("prepared backtest cache missing"))?;
        match mode {
            PreparedBacktestMode::CacheHit => {
                let requests = symbols
                    .iter()
                    .map(|symbol| tqsdk_data::TickDataSeriesRequest::new(symbol, start_ns, end_ns))
                    .collect::<Vec<_>>();
                let stream = tqsdk_task::HistoryTickReplayStream::new(
                    cache.history_cache().clone(),
                    requests,
                )?;
                base.replay_backtest_stream(Box::new(stream))
                    .connect()
                    .await
            }
            PreparedBacktestMode::RemoteCaching { symbols } => {
                let auth = base.auth.clone().ok_or(Error::MissingAuth)?;
                let stream = backtest_remote::RemoteBacktestCachingStream::connect(
                    auth.user, auth.pass, start_ns, end_ns, symbols, cache,
                )
                .await?;
                base.replay_backtest_stream(Box::new(stream))
                    .connect()
                    .await
            }
        }
    }
}

/// Builder for [`Tq`].
#[derive(Debug)]
pub struct TqBuilder {
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
    #[cfg(feature = "live")]
    auto_trade_login: Option<AutoTradeLogin>,
    market_url: Option<String>,
    replay_url: Option<String>,
    backtest: Option<BacktestConfig>,
    local_backtest_recipe: local_backtest::LocalBacktestRecipe,
}

impl TqBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            auth: None,
            query_enabled: false,
            trade_targets: Vec::new(),
            #[cfg(feature = "live")]
            auto_trade_login: None,
            market_url: None,
            replay_url: None,
            backtest: None,
            local_backtest_recipe: local_backtest::LocalBacktestRecipe::default(),
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

    #[must_use]
    pub fn replay_url(mut self, replay_url: impl Into<String>) -> Self {
        self.replay_url = Some(replay_url.into());
        self
    }

    /// Enter server-side single-day replay mode (≈ Python `TqReplay(date)`).
    ///
    /// The replay server replays one trading day of market data through the
    /// normal live `next()` / `quote()` strategy body. Creating the official
    /// replay session still requires auth during [`TqBuilder::connect`].
    #[cfg(all(feature = "services", feature = "live"))]
    pub fn server_replay(mut self, replay_date: NaiveDate) -> Result<Self> {
        tqsdk_session::ServerReplayBuilder::new("validation", "validation", replay_date)?;
        self.backtest = Some(BacktestConfig::ServerReplay { replay_date });
        Ok(self)
    }

    /// Prepare a cache-backed local backtest for `[start_ns, end_ns)`.
    ///
    /// Phase 1 requires an explicit cache/cache directory and explicit symbols.
    #[must_use]
    pub fn backtest(self, start_ns: i64, end_ns: i64) -> BacktestBuilder {
        BacktestBuilder {
            base: self,
            start_ns,
            end_ns,
            cache: None,
            cache_policy: BacktestCachePolicy::default(),
            symbols: Vec::new(),
        }
    }

    /// Enter official server-side backtest mode (≈ Python `TqBacktest`).
    ///
    /// The strategy body (`next()` / `quote()` / etc.) stays identical to live.
    #[must_use]
    pub fn server_backtest(mut self, start_ns: i64, end_ns: i64) -> Self {
        self.backtest = Some(BacktestConfig::Server { start_ns, end_ns });
        self
    }

    /// Enter local-backtest mode using a custom in-memory replay source.
    ///
    /// Uses [`tqsdk_task::sim::TqSim`] for matching. The strategy body stays identical to live.
    #[must_use]
    pub fn replay_backtest(mut self, replay: tqsdk_task::replay::ReplayMarketSource) -> Self {
        self.backtest = Some(BacktestConfig::Local { replay });
        self
    }

    /// Enter local-backtest mode using a custom async market stream.
    #[must_use]
    pub fn replay_backtest_stream(
        mut self,
        stream: Box<dyn tqsdk_task::BacktestMarketStream>,
    ) -> Self {
        self.backtest = Some(BacktestConfig::LocalStream {
            stream: Arc::new(Mutex::new(Some(stream))),
        });
        self
    }

    /// Pre-declare a quote symbol for local replay/backtest.
    #[must_use]
    pub fn quote_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.local_backtest_recipe = self.local_backtest_recipe.quote_symbol(symbol);
        self
    }

    /// Pre-declare a price tick for local replay/backtest (required if replay contains klines).
    #[must_use]
    pub fn price_tick(mut self, symbol: impl Into<String>, tick: f64) -> Self {
        self.local_backtest_recipe = self.local_backtest_recipe.price_tick(symbol, tick);
        self
    }

    #[must_use]
    pub fn instrument_spec(mut self, spec: tqsdk_session::InstrumentSpec) -> Self {
        self.local_backtest_recipe = self.local_backtest_recipe.instrument_spec(spec);
        self
    }

    #[must_use]
    pub fn instrument_specs(
        mut self,
        specs: impl IntoIterator<Item = tqsdk_session::InstrumentSpec>,
    ) -> Self {
        self.local_backtest_recipe = self.local_backtest_recipe.instrument_specs(specs);
        self
    }

    /// Set fallback price tick for local replay/backtest kline quote synthesis.
    ///
    /// Per-symbol [`TqBuilder::price_tick`] overrides this fallback.
    #[must_use]
    pub fn default_price_tick(mut self, tick: f64) -> Self {
        self.local_backtest_recipe = self.local_backtest_recipe.default_price_tick(tick);
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

    #[must_use]
    #[cfg(feature = "live")]
    pub fn tqkq_sim(mut self) -> Self {
        self.trade_targets.push(TradeTarget::TqKq);
        self.auto_trade_login = Some(AutoTradeLogin::TqKq { number: None });
        self
    }

    #[must_use]
    #[cfg(feature = "live")]
    pub fn tqkq_sim_numbered(mut self, number: u8) -> Self {
        self.trade_targets.push(TradeTarget::TqKqNumbered(number));
        self.auto_trade_login = Some(AutoTradeLogin::TqKq {
            number: Some(number),
        });
        self
    }

    #[must_use]
    #[cfg(feature = "live")]
    pub fn trade_account(
        mut self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let broker_id = broker_id.into();
        let account_id = account_id.into();
        let password = password.into();
        self.trade_targets.push(TradeTarget::Custom {
            broker_id: broker_id.clone(),
            account_id: account_id.clone(),
            trade_url: None,
        });
        self.auto_trade_login = Some(AutoTradeLogin::Futures {
            broker_id,
            account_id,
            password,
        });
        self
    }

    #[must_use]
    #[cfg(feature = "live")]
    pub fn trade_account_with_url(
        mut self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
        password: impl Into<String>,
        trade_url: impl Into<String>,
    ) -> Self {
        let broker_id = broker_id.into();
        let account_id = account_id.into();
        let password = password.into();
        self.trade_targets.push(TradeTarget::Custom {
            broker_id: broker_id.clone(),
            account_id: account_id.clone(),
            trade_url: Some(trade_url.into()),
        });
        self.auto_trade_login = Some(AutoTradeLogin::Futures {
            broker_id,
            account_id,
            password,
        });
        self
    }

    #[cfg(feature = "live")]
    pub fn trade_account_env(self) -> Result<Self> {
        let broker_id = read_env("TQ_TRADE_BROKER_ID")?;
        let account_id = read_env("TQ_TRADE_ACCOUNT_ID")?;
        let password = read_env("TQ_TRADE_PASSWORD")?;
        Ok(self.trade_account(broker_id, account_id, password))
    }

    pub async fn connect(self) -> Result<Tq> {
        let Self {
            auth,
            query_enabled,
            trade_targets,
            #[cfg(feature = "live")]
            auto_trade_login,
            market_url,
            replay_url,
            backtest,
            local_backtest_recipe,
        } = self;

        let is_server_side_backtest = backtest
            .as_ref()
            .is_some_and(BacktestConfig::is_server_side);
        if is_server_side_backtest && !trade_targets.is_empty() {
            return Err(server_side_trade_login_error());
        }
        #[cfg(feature = "live")]
        if is_server_side_backtest && auto_trade_login.is_some() {
            return Err(server_side_trade_login_error());
        }

        match backtest {
            Some(BacktestConfig::Local { replay }) => local_backtest_recipe.connect(replay).await,
            Some(BacktestConfig::LocalStream { stream }) => {
                let stream = take_local_backtest_stream(stream)?;
                local_backtest_recipe.connect_stream(stream).await
            }
            backtest => {
                let tq = connect_wait_facade(
                    auth,
                    query_enabled,
                    trade_targets,
                    market_url,
                    replay_url,
                    backtest,
                )
                .await?;
                #[cfg(feature = "live")]
                {
                    let mut tq = tq;
                    if let Some(auto_trade_login) = auto_trade_login {
                        apply_auto_trade_login(&mut tq, auto_trade_login).await?;
                    }
                    Ok(tq)
                }
                #[cfg(not(feature = "live"))]
                {
                    Ok(tq)
                }
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

    #[must_use]
    pub fn execution_events_since(
        &self,
        start: usize,
    ) -> (usize, Vec<tqsdk_task::TargetPosTaskExecutionEvent>) {
        self.inner.execution_events_since(start)
    }

    #[must_use]
    pub fn execution_trades_since(
        &self,
        start: usize,
    ) -> (usize, Vec<tqsdk_task::TargetPosTaskTradeFill>) {
        self.inner.execution_trades_since(start)
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

#[cfg(feature = "live")]
#[derive(Clone)]
enum AutoTradeLogin {
    Futures {
        broker_id: String,
        account_id: String,
        password: String,
    },
    TqKq {
        number: Option<u8>,
    },
}

#[cfg(feature = "live")]
impl std::fmt::Debug for AutoTradeLogin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Futures {
                broker_id,
                account_id,
                ..
            } => f
                .debug_struct("Futures")
                .field("broker_id", broker_id)
                .field("account_id", account_id)
                .field("password", &"[REDACTED]")
                .finish(),
            Self::TqKq { number } => f.debug_struct("TqKq").field("number", number).finish(),
        }
    }
}

enum BacktestConfig {
    Server {
        start_ns: i64,
        end_ns: i64,
    },
    #[cfg(all(feature = "services", feature = "live"))]
    ServerReplay {
        replay_date: NaiveDate,
    },
    Local {
        replay: tqsdk_task::replay::ReplayMarketSource,
    },
    LocalStream {
        stream: SharedBacktestMarketStream,
    },
}

type BacktestMarketStreamBox = Box<dyn tqsdk_task::BacktestMarketStream>;
type SharedBacktestMarketStream = Arc<Mutex<Option<BacktestMarketStreamBox>>>;

impl std::fmt::Debug for BacktestConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server { start_ns, end_ns } => f
                .debug_struct("Server")
                .field("start_ns", start_ns)
                .field("end_ns", end_ns)
                .finish(),
            #[cfg(all(feature = "services", feature = "live"))]
            Self::ServerReplay { replay_date } => f
                .debug_struct("ServerReplay")
                .field("replay_date", replay_date)
                .finish(),
            Self::Local { .. } => f.debug_struct("Local").finish_non_exhaustive(),
            Self::LocalStream { .. } => f.debug_struct("LocalStream").finish_non_exhaustive(),
        }
    }
}

impl BacktestConfig {
    fn is_server_side(&self) -> bool {
        match self {
            Self::Server { .. } => true,
            #[cfg(all(feature = "services", feature = "live"))]
            Self::ServerReplay { .. } => true,
            Self::Local { .. } | Self::LocalStream { .. } => false,
        }
    }
}

async fn connect_wait_facade(
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
    market_url: Option<String>,
    replay_url: Option<String>,
    backtest: Option<BacktestConfig>,
) -> Result<Tq> {
    #[cfg(all(feature = "services", feature = "live"))]
    let mut server_replay_session = None;
    #[cfg(all(feature = "services", feature = "live"))]
    let (market_url, replay_url) =
        if let Some(BacktestConfig::ServerReplay { replay_date }) = &backtest {
            let auth_ref = auth.as_ref().ok_or(Error::MissingAuth)?;
            let replay_session = tqsdk_session::ServerReplayBuilder::new(
                auth_ref.user.as_str(),
                auth_ref.pass.as_str(),
                *replay_date,
            )?
            .create()
            .await?;
            let endpoints = server_replay_endpoints(market_url, replay_url, &replay_session);
            server_replay_session = Some(replay_session);
            endpoints
        } else {
            (market_url, replay_url)
        };

    let session_builder =
        session_builder(auth, query_enabled, trade_targets, market_url, replay_url)?;
    let mut wait_builder = tqsdk_wait::TqApiBuilder::from_session_builder(session_builder);
    let server_side_backtest = backtest
        .as_ref()
        .is_some_and(BacktestConfig::is_server_side);
    match &backtest {
        Some(BacktestConfig::Server { start_ns, end_ns }) => {
            wait_builder = wait_builder.futures_backtest(*start_ns, *end_ns)?;
        }
        #[cfg(all(feature = "services", feature = "live"))]
        Some(BacktestConfig::ServerReplay { .. }) => {}
        Some(BacktestConfig::Local { .. }) | Some(BacktestConfig::LocalStream { .. }) | None => {}
    }
    let api = wait_builder.build().await?;
    #[cfg(all(feature = "services", feature = "live"))]
    if let Some(server_replay) = server_replay_session {
        return Ok(Tq::from_api_with_server_replay(api, server_replay));
    }
    if server_side_backtest {
        return Ok(Tq::from_api_with_server_backtest(api));
    }
    Ok(Tq::from_api(api))
}

#[cfg(all(feature = "services", feature = "live"))]
fn server_replay_endpoints(
    _market_url: Option<String>,
    _replay_url: Option<String>,
    replay_session: &tqsdk_session::ServerReplaySession,
) -> (Option<String>, Option<String>) {
    (
        Some(replay_session.market_url().to_string()),
        Some(replay_session.session_url().to_string()),
    )
}

fn session_builder(
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
    market_url: Option<String>,
    replay_url: Option<String>,
) -> Result<tqsdk_session::SessionClientBuilder> {
    let auth = auth.ok_or(Error::MissingAuth)?;
    let mut builder =
        tqsdk_session::SessionClientBuilder::new(&auth.user, &auth.pass).futures_market();
    if let Some(market_url) = market_url {
        builder = builder.market_relay(market_url);
    }
    if let Some(replay_url) = replay_url {
        builder = builder.replay_url(replay_url);
    }
    if query_enabled {
        builder = builder.enable_query();
    }
    for target in trade_targets {
        builder = target.apply(builder);
    }
    Ok(builder)
}

fn data_validation(message: impl Into<String>) -> Error {
    Error::Data(Box::new(tqsdk_data::DataError::Validation(message.into())))
}

fn take_local_backtest_stream(
    stream: SharedBacktestMarketStream,
) -> Result<BacktestMarketStreamBox> {
    let mut guard = stream
        .lock()
        .map_err(|_| data_validation("local backtest stream lock poisoned"))?;
    guard
        .take()
        .ok_or_else(|| data_validation("local backtest stream was already consumed"))
}

fn missing_default_account() -> Error {
    Error::from(tqsdk_session::SessionFacadeError::InvalidState(
        "default account is not configured; use backtest(...).cache(...), replay_backtest(...), tqkq_sim(), trade_account(...), or login_trade_account(...)",
    ))
}

#[cfg(feature = "live")]
fn ambiguous_default_account() -> Error {
    Error::from(tqsdk_session::SessionFacadeError::InvalidState(
        "default account is ambiguous after multiple trade account logins; use explicit account-specific helpers or configure a single automatic trade login",
    ))
}

fn server_side_trade_login_error() -> Error {
    Error::from(tqsdk_session::SessionFacadeError::InvalidState(
        "server-side backtest/replay cannot be combined with trade targets or automatic trade account login; use backtest(...).cache(...) or replay_backtest(...) for simulated fills",
    ))
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
    use chrono::NaiveDate;
    use serde_json::json;

    use super::{
        Auth, AutoTradeLogin, BacktestConfig, Error, LOCAL_BACKTEST_ACCOUNT_ID, Tq, TqBuilder,
        session_builder,
    };

    #[tokio::test]
    async fn replay_backtest_connect_does_not_require_auth() {
        let replay = tqsdk_task::replay::ReplayMarketSource::new(Vec::new());

        let result = TqBuilder::new().replay_backtest(replay).connect().await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn replay_backtest_connect_sets_default_account_id() {
        let replay = tqsdk_task::replay::ReplayMarketSource::new(Vec::new());

        let tq = TqBuilder::new()
            .replay_backtest(replay)
            .connect()
            .await
            .expect("replay backtest should connect without auth");

        assert_eq!(tq.default_account_id().unwrap(), LOCAL_BACKTEST_ACCOUNT_ID);
    }

    #[tokio::test]
    async fn server_backtest_rejects_auto_trade_login_before_network() {
        let error = TqBuilder::new()
            .auth("demo-user", "demo-pass")
            .server_backtest(1_000, 2_000)
            .trade_account("9999", "acct-1", "secret")
            .connect()
            .await
            .err()
            .expect("server-side backtest must not auto-login a trade account");

        assert_eq!(
            error.to_string(),
            "invalid session facade state: server-side backtest/replay cannot be combined with trade targets or automatic trade account login; use backtest(...).cache(...) or replay_backtest(...) for simulated fills"
        );
    }

    #[test]
    fn tqkq_sim_sets_trade_target_and_auto_login() {
        let builder = TqBuilder::new().tqkq_sim_numbered(7);

        assert_eq!(builder.trade_targets.len(), 1);
        assert!(matches!(
            builder.trade_targets[0],
            super::TradeTarget::TqKqNumbered(7)
        ));
        assert!(matches!(
            builder.auto_trade_login,
            Some(AutoTradeLogin::TqKq { number: Some(7) })
        ));
    }

    #[test]
    fn trade_account_sets_target_and_redacts_debug_password() {
        let builder = TqBuilder::new().trade_account("9999", "acct-1", "secret");

        assert_eq!(builder.trade_targets.len(), 1);
        assert!(matches!(
            &builder.trade_targets[0],
            super::TradeTarget::Custom {
                broker_id,
                account_id,
                trade_url: None,
            } if broker_id == "9999" && account_id == "acct-1"
        ));
        let debug = format!("{:?}", builder.auto_trade_login);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
    }

    #[tokio::test]
    async fn second_trade_login_makes_default_account_ambiguous() {
        let mut tq = Tq::from_api(
            tqsdk_wait::TqApiBuilder::new("demo-user", "demo-pass")
                .build()
                .await
                .expect("session client should build without network"),
        );

        tq.note_trade_login_for_test("acct-1");
        assert_eq!(tq.default_account_id().unwrap(), "acct-1");

        tq.note_trade_login_for_test("acct-2");

        assert!(tq.default_account_id_opt().is_none());
        assert_eq!(
            tq.default_account_id().unwrap_err().to_string(),
            "invalid session facade state: default account is ambiguous after multiple trade account logins; use explicit account-specific helpers or configure a single automatic trade login"
        );
    }

    #[tokio::test]
    async fn live_connect_requires_auth() {
        let result = TqBuilder::new().connect().await;

        assert!(matches!(result, Err(Error::MissingAuth)));
    }

    #[test]
    fn session_builder_applies_replay_url() {
        let builder = session_builder(
            Some(Auth {
                user: "demo-user".to_string(),
                pass: "demo-pass".to_string(),
            }),
            false,
            Vec::new(),
            None,
            Some("replay-driver".to_string()),
        )
        .unwrap();

        assert_eq!(
            builder.endpoints().replay_url.as_deref(),
            Some("replay-driver")
        );
    }

    #[cfg(all(feature = "services", feature = "live"))]
    #[test]
    fn server_replay_builder_sets_replay_config() {
        let replay_date = NaiveDate::from_ymd_opt(2026, 6, 25).expect("valid date");
        let builder = TqBuilder::new()
            .server_replay(replay_date)
            .expect("weekday replay date should be valid");

        assert!(matches!(
            builder.backtest,
            Some(BacktestConfig::ServerReplay { replay_date: date }) if date == replay_date
        ));
    }

    #[cfg(all(feature = "services", feature = "live"))]
    #[test]
    fn server_replay_builder_rejects_weekend_dates() {
        let weekend = NaiveDate::from_ymd_opt(2026, 6, 27).expect("valid date");

        let error = TqBuilder::new()
            .server_replay(weekend)
            .expect_err("weekend replay date should be rejected");

        assert_eq!(
            error.to_string(),
            "validation error: replay_date must be a weekday trading date"
        );
    }

    #[cfg(all(feature = "services", feature = "live"))]
    #[test]
    fn server_replay_session_sets_market_and_replay_endpoints() {
        let replay_date = NaiveDate::from_ymd_opt(2026, 6, 25).expect("valid date");
        let session = tqsdk_session::ServerReplaySession::from_create_session_payload(
            replay_date,
            &json!({
                "ip": "127.0.0.1",
                "session_port": 18888,
                "gateway_web_port": 27777,
                "session": "session-1"
            }),
        )
        .expect("valid session response");

        let (market_url, replay_url) = super::server_replay_endpoints(None, None, &session);
        let builder = session_builder(
            Some(Auth {
                user: "demo-user".to_string(),
                pass: "demo-pass".to_string(),
            }),
            false,
            Vec::new(),
            market_url,
            replay_url,
        )
        .unwrap();

        assert_eq!(
            builder.endpoints().market_url.as_deref(),
            Some("ws://127.0.0.1:27777/t/rmd/front/mobile")
        );
        assert_eq!(
            builder.endpoints().replay_url.as_deref(),
            Some("http://127.0.0.1:18888/t/rmd/replay/session/session-1")
        );
    }

    #[cfg(all(feature = "services", feature = "live"))]
    #[tokio::test]
    async fn tq_retains_server_replay_session_handle() {
        let api = tqsdk_wait::TqApiBuilder::new("demo-user", "demo-pass")
            .build()
            .await
            .expect("session client should build without network");
        let replay_date = NaiveDate::from_ymd_opt(2026, 6, 25).expect("valid date");
        let session = tqsdk_session::ServerReplaySession::from_create_session_payload(
            replay_date,
            &json!({
                "ip": "127.0.0.1",
                "session_port": 18888,
                "gateway_web_port": 27777,
                "session": "session-1"
            }),
        )
        .expect("valid session response");

        let tq = Tq::from_api_with_server_replay(api, session);

        assert!(tq.server_replay_heartbeat_active());
        assert_eq!(
            tq.server_replay_session()
                .expect("server replay session should be retained")
                .session_id(),
            "session-1"
        );
    }

    #[cfg(all(feature = "services", feature = "live"))]
    #[tokio::test]
    async fn replay_control_requires_server_replay_mode() {
        let api = tqsdk_wait::TqApiBuilder::new("demo-user", "demo-pass")
            .build()
            .await
            .expect("session client should build without network");
        let tq = Tq::from_api(api);

        let error = tq
            .set_replay_speed(1.0)
            .await
            .expect_err("live mode should not have replay control");

        assert_eq!(
            error.to_string(),
            "invalid session facade state: server replay control requires server_replay mode"
        );
    }

    #[cfg(all(feature = "services", feature = "live"))]
    #[tokio::test]
    async fn terminate_server_replay_keeps_session_on_transport_error() {
        let api = tqsdk_wait::TqApiBuilder::new("demo-user", "demo-pass")
            .build()
            .await
            .expect("session client should build without network");
        let replay_date = NaiveDate::from_ymd_opt(2026, 6, 25).expect("valid date");
        let session = tqsdk_session::ServerReplaySession::from_create_session_payload(
            replay_date,
            &json!({
                "ip": "127.0.0.1",
                "session_port": 18888,
                "gateway_web_port": 27777,
                "session": "session-1"
            }),
        )
        .expect("valid session response");

        let mut tq = Tq::from_api_with_server_replay(api, session);
        assert!(tq.server_replay_heartbeat_active());
        assert!(tq.server_replay_session().is_some());

        let error = tq
            .terminate_server_replay()
            .await
            .expect_err("terminate requires an authenticated control client");

        assert!(error.to_string().contains(
            "server replay session was not created with an authenticated control client"
        ));
        assert!(
            tq.server_replay_session().is_some(),
            "session should be kept for retry"
        );
        assert!(
            !tq.server_replay_heartbeat_active(),
            "heartbeat should be stopped on terminate attempt"
        );
    }
}
