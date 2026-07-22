#![cfg_attr(not(test), forbid(unsafe_code))]
//! User-facing facade crate for `tqsdk-rust`.
//!
//! This crate gives ordinary users one dependency and one prelude while keeping
//! the underlying `core` / `session` / `wait` / `task` / `data`
//! boundaries available under [`advanced`].

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{Datelike, FixedOffset, NaiveDate, TimeZone, Timelike, Utc, Weekday};

/// Common imports for strategy-oriented users.
pub mod prelude {
    pub use crate::{
        BacktestBuilder, BacktestCachePolicy, BacktestCacheWarmupAction, BacktestCacheWarmupReport,
        BacktestCacheWarmupSymbolReport, BacktestDataReport, BacktestRemoteFillCancellation,
        BacktestRemoteFillConfig, BacktestRemoteFillInspectionProgress, BacktestRemoteFillPhase,
        BacktestRemoteFillProgress, BacktestRemoteFillTelemetry, BacktestTickCache,
        BacktestTickCachePurgeReport, BacktestTickCacheStatus, Error, LOCAL_BACKTEST_ACCOUNT_ID,
        MarketCachePolicy, PreparedBacktest, RecordTicksFlushReport, RecordTicksHealth,
        RecordTicksReport, RecordTicksSymbolFlushReport, RecordTicksSymbolHealth, RemoteFillPlan,
        RemoteFillPlanSymbol, Result, TargetPos, Tq, TqBuilder,
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
            BacktestTickCache, BacktestTickCachePurgeReport, BacktestTickCacheStatus, DataClient,
            DataError, HistoricalContUnderlyingRow, HistoricalContUnderlyingSegment,
            KlineDataSeries, KlineDataSeriesRequest, LiveTickCacheWriter, TickDataSeries,
            TickDataSeriesRequest, TradingCalendarRow, historical_cont_underlying_segments,
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

mod backtest_history_remote;
mod backtest_remote;
mod live_tick_recorder;
mod local_backtest;

pub use backtest_remote::{
    BacktestRemoteFillCancellation, BacktestRemoteFillConfig, BacktestRemoteFillInspectionProgress,
    BacktestRemoteFillPhase, BacktestRemoteFillProgress, BacktestRemoteFillProgressHandler,
    BacktestRemoteFillTelemetry, BacktestRemoteFillTelemetryHandler, RemoteFillPlan,
    RemoteFillPlanSymbol,
};
pub use live_tick_recorder::{
    RecordTicksFlushReport, RecordTicksHealth, RecordTicksReport, RecordTicksSymbolFlushReport,
    RecordTicksSymbolHealth,
};
pub use tqsdk_data::{
    BacktestCachePolicy, BacktestTickCache, BacktestTickCachePurgeReport, BacktestTickCacheStatus,
};

/// Result type for the user-facing facade.
pub type Result<T> = std::result::Result<T, Error>;

/// Default account id used by local simulated backtests.
pub const LOCAL_BACKTEST_ACCOUNT_ID: &str = tqsdk_task::sim::LOCAL_BACKTEST_ACCOUNT_ID;

#[cfg(all(feature = "services", feature = "live"))]
const SERVER_REPLAY_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(all(feature = "services", feature = "live"))]
const SERVER_REPLAY_TERMINATE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_BACKTEST_WARMUP_BATCH_SIZE: usize = 20;
const BACKTEST_SYNTH_KLINE_MAX_NS: i64 = 60_000_000_000;

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
    tick_recorder: Option<live_tick_recorder::LiveTickRecorder>,
    tick_record_report: Option<RecordTicksReport>,
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
            tick_recorder: None,
            tick_record_report: None,
            #[cfg(feature = "live")]
            server_side_backtest: false,
            #[cfg(all(feature = "services", feature = "live"))]
            server_replay: None,
            #[cfg(all(feature = "services", feature = "live"))]
            server_replay_heartbeat: None,
        }
    }

    fn from_api_with_remote_backtest(api: tqsdk_wait::TqApi) -> Self {
        Self {
            inner: TqInner::Live(Box::new(tqsdk_task::TaskHost::new(api))),
            default_account_id: DefaultAccountId::None,
            tick_recorder: None,
            tick_record_report: None,
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
            tick_recorder: None,
            tick_record_report: None,
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
            tick_recorder: None,
            tick_record_report: None,
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

    fn flush_tick_recorder(&mut self) -> Result<()> {
        if let Some(recorder) = self.tick_recorder.as_mut() {
            recorder.flush()?;
        }
        Ok(())
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

    /// Return the active live tick recording report, if live recording is enabled.
    #[must_use]
    pub fn record_ticks_report(&self) -> Option<&RecordTicksReport> {
        self.tick_record_report.as_ref()
    }

    /// Return live tick recording health, including gap status and latest flush details.
    #[must_use]
    pub fn record_ticks_health(&self) -> Option<&RecordTicksHealth> {
        self.tick_recorder
            .as_ref()
            .map(|recorder| recorder.health())
    }

    /// Build a cache policy from the active live tick recording health.
    ///
    /// This is intended for explicit post-run gap handling: pass the returned
    /// policy to `Tq::futures().auth_env()?.market_cache(policy).backtest(...).remote_on_miss()`
    /// to inspect or fill missing recorded ranges.
    #[must_use]
    pub fn recorded_market_cache_policy(&self) -> Option<MarketCachePolicy> {
        self.record_ticks_health()
            .map(MarketCachePolicy::from_record_ticks_health)
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
            return Err(remote_backtest_trade_login_error());
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
        let updated = match &mut self.inner {
            TqInner::Live(host) => host.wait_update(None).await.map_err(Error::from),
            TqInner::LocalBacktest(bt) => {
                if let Some(mut ctx) = bt.next().await? {
                    ctx.finish_sim_step()?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }?;
        if updated {
            self.flush_tick_recorder()?;
        }
        Ok(updated)
    }

    /// Advance one step with a deadline (live mode). In local-backtest mode
    /// the deadline is ignored.
    pub async fn wait_update(&mut self, deadline: Option<tokio::time::Instant>) -> Result<bool> {
        if matches!(&self.inner, TqInner::LocalBacktest(_)) {
            return self.next().await;
        }
        let updated = match &mut self.inner {
            TqInner::Live(host) => {
                let updated = host.wait_update(deadline).await.map_err(Error::from)?;
                if updated {
                    self.flush_tick_recorder()?;
                }
                updated
            }
            TqInner::LocalBacktest(_) => self.next().await?,
        };
        Ok(updated)
    }

    // ── Market data ──

    pub async fn record_ticks<I, S>(
        &mut self,
        cache_dir: impl AsRef<std::path::Path>,
        symbols: I,
    ) -> Result<RecordTicksReport>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if self.tick_recorder.is_some() {
            return Err(tqsdk_wait::WaitFacadeError::InvalidState(
                "record_ticks is already active",
            )
            .into());
        }
        if matches!(&self.inner, TqInner::LocalBacktest(_)) {
            return Err(tqsdk_wait::WaitFacadeError::InvalidState(
                "record_ticks is only available in live/session mode",
            )
            .into());
        }

        let (recorder, report) =
            live_tick_recorder::LiveTickRecorder::start(self.api_mut_any(), cache_dir, symbols)
                .await?;
        self.tick_recorder = Some(recorder);
        self.tick_record_report = Some(report.clone());
        Ok(report)
    }

    /// Apply a market-cache policy to this running facade.
    ///
    /// In the current phase this starts explicit live tick recording into the
    /// shared backtest tick cache. Empty policies are accepted as a no-op.
    pub async fn start_market_cache(
        &mut self,
        policy: MarketCachePolicy,
    ) -> Result<Option<RecordTicksReport>> {
        let symbols = self.resolve_market_cache_policy_symbols(&policy).await?;
        if symbols.is_empty() {
            return Ok(None);
        }
        self.record_ticks(policy.cache_dir(), symbols)
            .await
            .map(Some)
    }

    async fn resolve_market_cache_policy_symbols(
        &self,
        policy: &MarketCachePolicy,
    ) -> Result<Vec<String>> {
        let mut symbols = policy.tick_symbols.clone();
        if let Some(expression) = policy.universe_expression() {
            if !matches!(self.inner, TqInner::Live(_)) {
                return Err(data_validation(
                    "market cache universe recording requires live/session mode",
                ));
            }
            let resolved =
                resolve_universe_with_session(expression, self.session().clone()).await?;
            for symbol in resolved {
                push_unique_string(&mut symbols, symbol);
            }
        }
        Ok(symbols)
    }

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

    pub async fn quotes_universe(
        &mut self,
        expression: impl AsRef<str>,
    ) -> Result<tqsdk_wait::QuoteSet> {
        let expression = tqsdk_data::UniverseExpression::parse(expression.as_ref())?;
        let symbols = if expression.is_static_symbol_only() {
            tqsdk_data::resolve_static_symbols_with_expression(&expression)?
        } else {
            if !matches!(self.inner, TqInner::Live(_)) {
                return Err(data_validation(
                    "dynamic quotes_universe expression requires live/session mode",
                ));
            }
            resolve_universe_with_session(&expression, self.session().clone()).await?
        };
        self.quotes(symbols).await
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

/// Shared market-cache policy for live recording and cache-backed local backtests.
///
/// The policy is intentionally tick-first: live mode records the configured tick
/// symbols into the same persistent cache that local backtests can replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCachePolicy {
    cache_dir: PathBuf,
    tick_symbols: Vec<String>,
    universe_expression: Option<tqsdk_data::UniverseExpression>,
}

impl MarketCachePolicy {
    #[must_use]
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            tick_symbols: Vec::new(),
            universe_expression: None,
        }
    }

    #[must_use]
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    #[must_use]
    pub fn tick_symbols(&self) -> &[String] {
        &self.tick_symbols
    }

    #[must_use]
    pub fn universe_expression(&self) -> Option<&tqsdk_data::UniverseExpression> {
        self.universe_expression.as_ref()
    }

    #[must_use]
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        push_unique_string(&mut self.tick_symbols, symbol.into());
        self
    }

    #[must_use]
    pub fn record_ticks<I, S>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for symbol in symbols {
            push_unique_string(&mut self.tick_symbols, symbol.into());
        }
        self
    }

    /// Record tick cache rows for symbols resolved from a futures universe expression.
    pub fn record_universe(mut self, expression: impl AsRef<str>) -> Result<Self> {
        self.universe_expression =
            Some(tqsdk_data::UniverseExpression::parse(expression.as_ref())?);
        Ok(self)
    }

    #[must_use]
    pub fn from_record_ticks_health(health: &RecordTicksHealth) -> Self {
        Self {
            cache_dir: health.cache_dir.clone(),
            tick_symbols: health
                .symbols
                .iter()
                .map(|symbol| symbol.symbol.clone())
                .collect(),
            universe_expression: None,
        }
    }
}

/// Builder for strategy backtests.
///
/// Backtests use the shared persistent history cache by default. Explicit
/// [`BacktestBuilder::cache_dir`], [`BacktestBuilder::cache_store`], or
/// [`TqBuilder::market_cache`] configuration overrides that default. Use
/// [`BacktestBuilder::disabled_cache`] to request the official server-side
/// backtest stream without local persistence. Historical `KQ.m@...` main
/// contracts resolve their dated physical-underlying segments through
/// `tqsdk-data`; each segment uses that physical contract as its tick-cache
/// key while replay keeps the main-contract symbol.
pub struct BacktestBuilder {
    base: TqBuilder,
    start_ns: i64,
    end_ns: i64,
    cache: Option<tqsdk_data::BacktestTickCache>,
    cache_policy: BacktestCachePolicy,
    symbols: Vec<String>,
    kline_specs: Vec<BacktestKlineSpec>,
    tick_specs: Vec<BacktestTickSpec>,
    universe_expression: Option<tqsdk_data::UniverseExpression>,
    warmup_batch_size: usize,
    remote_fill_config: Option<BacktestRemoteFillConfig>,
    remote_fill_progress: Option<BacktestRemoteFillProgressHandler>,
    remote_fill_telemetry: Option<BacktestRemoteFillTelemetryHandler>,
    remote_fill_cancellation: Option<BacktestRemoteFillCancellation>,
    remote_fill_lock_wait: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BacktestKlineSource {
    SynthesizedFromTick,
    NativeKline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BacktestKlineSpec {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BacktestTickSpec {
    symbol: String,
    view_width: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BacktestPlannedInputs {
    tick_symbols: Vec<String>,
    native_klines: Vec<BacktestKlineSpec>,
    synthetic_klines: Vec<BacktestKlineSpec>,
    auto_quote_klines: Vec<BacktestKlineSpec>,
}

/// Cache-prepared local backtest that can be connected without remote access.
pub struct PreparedBacktest {
    builder: BacktestBuilder,
    data_report: BacktestDataReport,
    mode: PreparedBacktestMode,
    remote_fill_lock: Option<tqsdk_data::BacktestTickCacheOperationLock>,
}

enum PreparedBacktestMode {
    CacheHit {
        inputs: PreparedBacktestInputs,
    },
    RemoteCaching {
        inputs: PreparedBacktestInputs,
        tick_fill_requests: Vec<backtest_remote::RemoteBacktestCacheFillRequest>,
        kline_fill_requests: Vec<backtest_history_remote::BacktestKlineFillRequest>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PreparedBacktestInputs {
    tick_sources: Vec<tqsdk_task::HistoryBacktestTickSource>,
    native_klines: Vec<BacktestKlineSpec>,
    synthetic_klines: Vec<BacktestKlineSpec>,
}

/// Minimal data preparation report for a cache-backed local backtest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestDataReport {
    pub requested_range: (i64, i64),
    pub cache_policy: BacktestCachePolicy,
    pub cache_dir: std::path::PathBuf,
    pub resolved_symbols: usize,
    pub remote_used: bool,
    pub tick_symbols: usize,
    pub native_kline_series: usize,
    pub synthetic_kline_series: usize,
    pub remote_tick_used: bool,
    pub remote_kline_used: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestCacheWarmupAction {
    SkippedComplete,
    MissingCacheOnly,
    FilledRemote,
    RefreshedRemote,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestCacheWarmupSymbolReport {
    /// Physical tick-cache symbol for this warmed range. For a `KQ.m@...`
    /// request this is its resolved concrete underlying contract.
    pub symbol: String,
    pub action: BacktestCacheWarmupAction,
    pub before: BacktestTickCacheStatus,
    pub after: BacktestTickCacheStatus,
    pub rows_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestCacheWarmupReport {
    pub requested_range: (i64, i64),
    pub cache_policy: BacktestCachePolicy,
    pub cache_dir: std::path::PathBuf,
    /// Logical symbols after explicit and universe-expression resolution.
    ///
    /// `symbols` below reports the physical tick-cache symbols; they can differ
    /// for a historical `KQ.m@...` main-contract request.
    pub logical_symbols: Vec<String>,
    /// Compatibility field for callers that still record a warmup batch hint.
    ///
    /// Remote cache fill is scheduled by the internal bounded remote scheduler;
    /// tune `TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY` for network parallelism.
    pub batch_size: usize,
    pub symbols_total: usize,
    pub symbols_skipped: usize,
    pub symbols_missing: usize,
    pub symbols_filled: usize,
    pub rows_written: usize,
    pub remote_used: bool,
    pub symbols: Vec<BacktestCacheWarmupSymbolReport>,
}

fn backtest_kline_source(duration_ns: i64) -> Result<BacktestKlineSource> {
    if duration_ns <= 0 {
        return Err(data_validation(
            "backtest kline duration must be greater than zero",
        ));
    }
    if duration_ns <= BACKTEST_SYNTH_KLINE_MAX_NS {
        Ok(BacktestKlineSource::SynthesizedFromTick)
    } else {
        Ok(BacktestKlineSource::NativeKline)
    }
}

fn duration_to_ns(duration: Duration) -> Result<i64> {
    let seconds =
        i64::try_from(duration.as_secs()).map_err(|_| data_validation("duration is too large"))?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|ns| ns.checked_add(i64::from(duration.subsec_nanos())))
        .ok_or_else(|| data_validation("duration is too large"))
}

fn plan_backtest_inputs(
    quote_symbols: &[String],
    tick_specs: &[BacktestTickSpec],
    kline_specs: &[BacktestKlineSpec],
) -> Result<BacktestPlannedInputs> {
    let mut tick_symbols = BTreeSet::new();
    let mut native_klines = Vec::new();
    let mut native_keys = BTreeSet::new();
    let mut synthetic_klines = Vec::new();
    let mut synthetic_keys = BTreeSet::new();

    for spec in tick_specs {
        validate_backtest_symbol(&spec.symbol)?;
        if spec.view_width == 0 {
            return Err(data_validation(
                "backtest tick view_width must be greater than zero",
            ));
        }
        tick_symbols.insert(spec.symbol.clone());
    }

    for spec in kline_specs {
        validate_backtest_symbol(&spec.symbol)?;
        if spec.view_width == 0 {
            return Err(data_validation(
                "backtest kline view_width must be greater than zero",
            ));
        }
        match backtest_kline_source(spec.duration_ns)? {
            BacktestKlineSource::SynthesizedFromTick => {
                tick_symbols.insert(spec.symbol.clone());
                push_unique_kline(&mut synthetic_klines, &mut synthetic_keys, spec.clone());
            }
            BacktestKlineSource::NativeKline => {
                push_unique_kline(&mut native_klines, &mut native_keys, spec.clone());
            }
        }
    }

    let mut auto_quote_klines = Vec::new();
    let mut auto_keys = BTreeSet::new();
    for symbol in quote_symbols {
        validate_backtest_symbol(symbol)?;
        if tick_symbols.contains(symbol) {
            continue;
        }
        let smallest_kline = kline_specs
            .iter()
            .filter(|spec| spec.symbol == *symbol)
            .map(|spec| spec.duration_ns)
            .min();
        if smallest_kline.is_none_or(|duration_ns| duration_ns > BACKTEST_SYNTH_KLINE_MAX_NS) {
            let spec = BacktestKlineSpec {
                symbol: symbol.clone(),
                duration_ns: BACKTEST_SYNTH_KLINE_MAX_NS,
                view_width: 2,
            };
            tick_symbols.insert(symbol.clone());
            push_unique_kline(&mut auto_quote_klines, &mut auto_keys, spec);
        }
    }

    Ok(BacktestPlannedInputs {
        tick_symbols: tick_symbols.into_iter().collect(),
        native_klines,
        synthetic_klines,
        auto_quote_klines,
    })
}

fn push_unique_kline(
    target: &mut Vec<BacktestKlineSpec>,
    keys: &mut BTreeSet<(String, i64)>,
    spec: BacktestKlineSpec,
) {
    if keys.insert((spec.symbol.clone(), spec.duration_ns)) {
        target.push(spec);
    }
}

fn validate_backtest_symbol(symbol: &str) -> Result<()> {
    if symbol.is_empty() {
        return Err(data_validation("backtest symbol must not be empty"));
    }
    Ok(())
}

fn continuous_tick_sources(
    symbol: &str,
    start_ns: i64,
    end_ns: i64,
    segments: &[tqsdk_data::HistoricalContUnderlyingSegment],
) -> Result<Vec<tqsdk_task::HistoryBacktestTickSource>> {
    if !is_main_continuous_contract(symbol) {
        return Err(data_validation(format!(
            "continuous tick sources require a KQ.m@ symbol, got {symbol}"
        )));
    }
    if end_ns <= start_ns {
        return Err(data_validation(
            "continuous tick source end_ns must be greater than start_ns",
        ));
    }

    let mut sources = Vec::new();
    for segment in segments {
        if segment.symbol != symbol {
            return Err(data_validation(format!(
                "continuous segment symbol {} does not match requested {symbol}",
                segment.symbol
            )));
        }
        if segment.underlying.is_empty() {
            return Err(data_validation(format!(
                "continuous segment for {symbol} has an empty underlying contract"
            )));
        }

        let segment_start =
            trading_day_start_ns(parse_continuous_segment_date(&segment.start_date)?)?;
        let segment_end = trading_day_end_ns(parse_continuous_segment_date(&segment.end_date)?)?;
        if segment_end <= segment_start {
            return Err(data_validation(format!(
                "continuous segment for {symbol} has an invalid date range {} to {}",
                segment.start_date, segment.end_date
            )));
        }

        let source_start = start_ns.max(segment_start);
        let source_end = end_ns.min(segment_end);
        if source_start < source_end {
            sources.push(tqsdk_task::HistoryBacktestTickSource {
                replay_symbol: symbol.to_string(),
                cache_symbol: segment.underlying.clone(),
                start_ns: source_start,
                end_ns: source_end,
            });
        }
    }

    if sources.is_empty() {
        return Err(data_validation(format!(
            "continuous-contract mapping does not cover requested backtest range for {symbol}"
        )));
    }
    sources.sort_by(|left, right| {
        left.start_ns
            .cmp(&right.start_ns)
            .then_with(|| left.end_ns.cmp(&right.end_ns))
            .then_with(|| left.cache_symbol.cmp(&right.cache_symbol))
    });
    Ok(sources)
}

fn is_main_continuous_contract(symbol: &str) -> bool {
    symbol.starts_with("KQ.m@")
}

fn parse_continuous_segment_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|error| {
        data_validation(format!("invalid continuous segment date {value}: {error}"))
    })
}

fn trading_day_start_ns(trading_day: NaiveDate) -> Result<i64> {
    let mut calendar_day = trading_day
        .pred_opt()
        .ok_or_else(|| data_validation("continuous segment trading day predates chrono range"))?;
    while matches!(calendar_day.weekday(), Weekday::Sat | Weekday::Sun) {
        calendar_day = calendar_day.pred_opt().ok_or_else(|| {
            data_validation("continuous segment trading day predates chrono range")
        })?;
    }
    cst_datetime_ns(calendar_day, 18)
}

fn trading_day_end_ns(trading_day: NaiveDate) -> Result<i64> {
    cst_datetime_ns(trading_day, 18)
}

fn cst_datetime_ns(date: NaiveDate, hour: u32) -> Result<i64> {
    let datetime = date
        .and_hms_opt(hour, 0, 0)
        .ok_or_else(|| data_validation("failed to build continuous segment CST datetime"))?;
    let cst = FixedOffset::east_opt(8 * 60 * 60).expect("China Standard Time offset must be valid");
    let timestamp = cst
        .from_local_datetime(&datetime)
        .single()
        .ok_or_else(|| data_validation("failed to resolve continuous segment CST datetime"))?
        .timestamp();
    timestamp
        .checked_mul(1_000_000_000)
        .ok_or_else(|| data_validation("continuous segment timestamp overflowed"))
}

fn continuous_mapping_query_window(start_ns: i64, end_ns: i64) -> Result<(usize, NaiveDate)> {
    if end_ns <= start_ns {
        return Err(data_validation(
            "continuous mapping end_ns must be greater than start_ns",
        ));
    }
    let start_date = trading_day_from_timestamp_ns(start_ns)?;
    let last_timestamp_ns = end_ns
        .checked_sub(1)
        .ok_or_else(|| data_validation("continuous mapping end_ns underflowed"))?;
    let end_date = trading_day_from_timestamp_ns(last_timestamp_ns)?;
    let calendar_days = end_date.signed_duration_since(start_date).num_days();
    let days = usize::try_from(calendar_days)
        .map_err(|_| data_validation("continuous mapping date range is invalid"))?
        .checked_add(1)
        .ok_or_else(|| data_validation("continuous mapping day count overflowed"))?;
    Ok((days, end_date))
}

fn trading_day_from_timestamp_ns(timestamp_ns: i64) -> Result<NaiveDate> {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let nanos = timestamp_ns.rem_euclid(1_000_000_000) as u32;
    let utc = Utc
        .timestamp_opt(seconds, nanos)
        .single()
        .ok_or_else(|| data_validation("failed to resolve continuous mapping timestamp"))?;
    let cst = FixedOffset::east_opt(8 * 60 * 60).expect("China Standard Time offset must be valid");
    let local = utc.with_timezone(&cst);
    let mut trading_day = local.date_naive();
    if local.hour() >= 18 {
        trading_day = trading_day
            .succ_opt()
            .ok_or_else(|| data_validation("continuous mapping trading day overflowed"))?;
    }
    while matches!(trading_day.weekday(), Weekday::Sat | Weekday::Sun) {
        trading_day = trading_day
            .succ_opt()
            .ok_or_else(|| data_validation("continuous mapping trading day overflowed"))?;
    }
    Ok(trading_day)
}

async fn resolve_backtest_tick_sources(
    symbols: &[String],
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<tqsdk_task::HistoryBacktestTickSource>> {
    let mapping_window = symbols
        .iter()
        .any(|symbol| is_main_continuous_contract(symbol))
        .then(|| continuous_mapping_query_window(start_ns, end_ns))
        .transpose()?;
    let data_client = mapping_window
        .as_ref()
        .map(|_| tqsdk_data::DataClient::new());
    let mut sources = Vec::new();

    for symbol in symbols {
        validate_backtest_symbol(symbol)?;
        if let Some((days, end_date)) = mapping_window
            && is_main_continuous_contract(symbol)
        {
            let segments = data_client
                .as_ref()
                .expect("main continuous contract requires a data client")
                .query_his_cont_underlying_segments(symbol, days, Some(end_date))
                .await?;
            sources.extend(continuous_tick_sources(
                symbol, start_ns, end_ns, &segments,
            )?);
        } else {
            sources.push(tqsdk_task::HistoryBacktestTickSource {
                replay_symbol: symbol.clone(),
                cache_symbol: symbol.clone(),
                start_ns,
                end_ns,
            });
        }
    }

    Ok(sources)
}

fn reject_continuous_native_kline_specs(specs: &[BacktestKlineSpec]) -> Result<()> {
    if let Some(spec) = specs
        .iter()
        .find(|spec| is_main_continuous_contract(&spec.symbol))
    {
        return Err(data_validation(format!(
            "cache-backed continuous-contract native kline {} duration {} is unsupported; use duration <= 60s so it can be synthesized from shared physical ticks",
            spec.symbol, spec.duration_ns
        )));
    }
    Ok(())
}

fn physical_tick_ranges(
    sources: &[tqsdk_task::HistoryBacktestTickSource],
) -> Vec<(String, i64, i64)> {
    let mut ranges_by_symbol = BTreeMap::<String, Vec<(i64, i64)>>::new();
    for source in sources {
        ranges_by_symbol
            .entry(source.cache_symbol.clone())
            .or_default()
            .push((source.start_ns, source.end_ns));
    }

    let mut merged = Vec::new();
    for (symbol, mut ranges) in ranges_by_symbol {
        ranges.sort_unstable();
        for (start_ns, end_ns) in ranges {
            match merged.last_mut() {
                Some((last_symbol, _, last_end_ns))
                    if *last_symbol == symbol && start_ns <= *last_end_ns =>
                {
                    *last_end_ns = (*last_end_ns).max(end_ns);
                }
                _ => merged.push((symbol.clone(), start_ns, end_ns)),
            }
        }
    }
    merged
}

impl BacktestBuilder {
    fn validate_range(&self) -> Result<()> {
        if self.end_ns <= self.start_ns {
            return Err(data_validation(
                "backtest end_ns must be greater than start_ns",
            ));
        }
        Ok(())
    }

    fn into_remote_backtest(mut self) -> TqBuilder {
        // No-cache backtest mode is intentionally remote-only and must not
        // start live tick cache recording even if a shared market cache policy
        // was configured before `.backtest(...)`.
        self.base.market_cache = None;
        self.base.with_remote_backtest(self.start_ns, self.end_ns)
    }

    async fn apply_market_cache_policy(&mut self) -> Result<()> {
        let Some(policy) = self.base.market_cache.clone() else {
            return Ok(());
        };
        if self.cache.is_none() {
            self.cache = Some(tqsdk_data::BacktestTickCache::open(policy.cache_dir())?);
        }
        for symbol in policy.tick_symbols() {
            push_unique_string(&mut self.symbols, symbol.clone());
        }
        if let Some(expression) = policy.universe_expression() {
            let resolved = resolve_backtest_universe(expression, self.base.auth.as_ref()).await?;
            for symbol in resolved {
                push_unique_string(&mut self.symbols, symbol);
            }
        }
        Ok(())
    }

    fn apply_default_cache_if_needed(&mut self) -> Result<()> {
        if self.cache.is_some() {
            return Ok(());
        }
        let cache = tqsdk_data::BacktestTickCache::open(tqsdk_data::default_history_cache_dir())?;
        self.cache = Some(cache);
        Ok(())
    }

    fn remote_fill_runtime(&self) -> backtest_remote::RemoteBacktestFillRuntime {
        backtest_remote::RemoteBacktestFillRuntime::new(
            self.remote_fill_config,
            self.remote_fill_progress.clone(),
            self.remote_fill_telemetry.clone(),
            self.remote_fill_cancellation.clone(),
        )
    }

    async fn acquire_remote_fill_lock(
        &self,
        cache: &tqsdk_data::BacktestTickCache,
    ) -> Result<tqsdk_data::BacktestTickCacheOperationLock> {
        if self
            .remote_fill_cancellation
            .as_ref()
            .is_some_and(BacktestRemoteFillCancellation::is_cancelled)
        {
            return Err(data_validation("remote backtest cache fill cancelled"));
        }
        let Some(wait) = self.remote_fill_lock_wait else {
            return cache.try_acquire_remote_fill_lock().map_err(Error::from);
        };
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if self
                .remote_fill_cancellation
                .as_ref()
                .is_some_and(BacktestRemoteFillCancellation::is_cancelled)
            {
                return Err(data_validation("remote backtest cache fill cancelled"));
            }
            match cache.try_acquire_remote_fill_lock() {
                Ok(lock) => return Ok(lock),
                Err(tqsdk_data::DataError::CacheBusy { .. })
                    if tokio::time::Instant::now() < deadline =>
                {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(remaining.min(Duration::from_millis(200))).await;
                }
                Err(error) => return Err(Error::from(error)),
            }
        }
    }

    fn resolved_cache(&self) -> Result<tqsdk_data::BacktestTickCache> {
        if let Some(cache) = &self.cache {
            return Ok(cache.clone());
        }
        if let Some(policy) = &self.base.market_cache {
            return tqsdk_data::BacktestTickCache::open(policy.cache_dir()).map_err(Error::from);
        }
        tqsdk_data::BacktestTickCache::open(tqsdk_data::default_history_cache_dir())
            .map_err(Error::from)
    }

    fn planned_inputs(&self) -> Result<BacktestPlannedInputs> {
        let mut tick_specs = self.tick_specs.clone();
        for symbol in &self.symbols {
            tick_specs.push(BacktestTickSpec {
                symbol: symbol.clone(),
                view_width: 1,
            });
        }
        let mut quote_symbols = self.symbols.clone();
        for spec in &self.kline_specs {
            push_unique_string(&mut quote_symbols, spec.symbol.clone());
        }
        plan_backtest_inputs(&quote_symbols, &tick_specs, &self.kline_specs)
    }

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
        let cache = tqsdk_data::BacktestTickCache::open(root_dir)?;
        self.cache = Some(cache);
        Ok(self)
    }

    /// Set the compatibility warmup batch-size hint reported by [`BacktestCacheWarmupReport`].
    ///
    /// Missing cache ranges are submitted to the internal bounded remote scheduler in one pass;
    /// this method no longer serially chunks remote fills. Use
    /// `TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY` to tune remote fill parallelism.
    #[must_use]
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.warmup_batch_size = batch_size.max(1);
        self
    }

    /// Override remote tick-fill scheduling for this backtest operation.
    ///
    /// Use [`BacktestRemoteFillConfig::from_environment`] to retain the
    /// existing environment configuration and selectively override it.
    #[must_use]
    pub fn remote_fill_config(mut self, config: BacktestRemoteFillConfig) -> Self {
        self.remote_fill_config = Some(config);
        self
    }

    /// Observe low-frequency remote cache-fill lifecycle events.
    ///
    /// The handler is not installed by default and should return quickly.
    #[must_use]
    pub fn on_remote_fill_progress(
        mut self,
        handler: impl Fn(&BacktestRemoteFillProgress) + Send + Sync + 'static,
    ) -> Self {
        self.remote_fill_progress = Some(Arc::new(handler));
        self
    }

    /// Observe resolved plans and low-frequency remote cache-fill telemetry.
    ///
    /// The handler receives an `Inspecting` snapshot after each physical cache
    /// range is checked, with cumulative hit and gap counts. After coverage is
    /// fully inspected (and, for remote fill modes, after the root fill lock is
    /// held), it receives `PlanReady`, then per-physical-symbol lifecycle
    /// snapshots. The handler is not installed by default and must return
    /// quickly; in particular it must not perform terminal I/O or block cache
    /// inspection or remote filling.
    #[must_use]
    pub fn on_remote_fill_telemetry(
        mut self,
        handler: impl Fn(&BacktestRemoteFillTelemetry) + Send + Sync + 'static,
    ) -> Self {
        self.remote_fill_telemetry = Some(Arc::new(handler));
        self
    }

    /// Install cooperative cancellation for a remote cache fill.
    ///
    /// Cancellation persists accepted partial rows but does not claim coverage
    /// for the interrupted range.
    #[must_use]
    pub fn remote_fill_cancellation(
        mut self,
        cancellation: BacktestRemoteFillCancellation,
    ) -> Self {
        self.remote_fill_cancellation = Some(cancellation);
        self
    }

    /// Wait up to `wait` for another remote cache fill owner to release the root lock.
    ///
    /// Without this opt-in the cache lock is fail-fast.
    #[must_use]
    pub fn remote_fill_lock_wait(mut self, wait: Duration) -> Self {
        self.remote_fill_lock_wait = Some(wait);
        self
    }

    /// Inspect persistent tick cache coverage for explicitly configured symbols.
    pub fn inspect_cache(&self) -> Result<Vec<BacktestTickCacheStatus>> {
        if self.symbols.is_empty() {
            return Err(data_validation(
                "backtest cache inspection requires at least one explicit symbol",
            ));
        }
        let cache = self.resolved_cache()?;
        self.symbols
            .iter()
            .map(|symbol| {
                cache
                    .inspect(symbol, self.start_ns, self.end_ns)
                    .map_err(Error::from)
            })
            .collect()
    }

    /// Remove persistent tick cache files for explicitly configured symbols.
    pub fn purge_cache_symbols(&self) -> Result<Vec<BacktestTickCachePurgeReport>> {
        if self.symbols.is_empty() {
            return Err(data_validation(
                "backtest cache purge requires at least one explicit symbol",
            ));
        }
        let cache = self.resolved_cache()?;
        self.symbols
            .iter()
            .map(|symbol| cache.purge_symbol_ticks(symbol).map_err(Error::from))
            .collect()
    }

    pub async fn warmup(mut self) -> Result<BacktestCacheWarmupReport> {
        self.validate_range()?;
        self.apply_market_cache_policy().await?;
        self.apply_default_cache_if_needed()?;
        if let Some(expression) = &self.universe_expression {
            let resolved = resolve_backtest_universe(expression, self.base.auth.as_ref()).await?;
            for symbol in resolved {
                push_unique_string(&mut self.symbols, symbol);
            }
        }
        if self.symbols.is_empty() && self.tick_specs.is_empty() && self.kline_specs.is_empty() {
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
            .clone()
            .ok_or_else(|| data_validation("backtest default cache was not applied"))?;
        let cache_dir = cache.cache_dir().to_path_buf();
        let planned = self.planned_inputs()?;
        reject_continuous_native_kline_specs(&planned.native_klines)?;
        let tick_sources =
            resolve_backtest_tick_sources(&planned.tick_symbols, self.start_ns, self.end_ns)
                .await?;
        let physical_ranges = physical_tick_ranges(&tick_sources);
        let mut logical_symbols = planned.tick_symbols.clone();
        logical_symbols.sort();
        logical_symbols.dedup();

        if matches!(self.cache_policy, BacktestCachePolicy::Disabled) {
            return Err(data_validation(format!(
                "backtest cache policy {:?} is not supported in phase 1",
                self.cache_policy
            )));
        }

        let refresh = matches!(self.cache_policy, BacktestCachePolicy::Refresh);
        if refresh && self.base.auth.is_none() {
            return Err(data_validation("remote backtest cache fill requires auth"));
        }
        let _remote_fill_lock = if matches!(
            self.cache_policy,
            BacktestCachePolicy::RemoteOnMiss | BacktestCachePolicy::Refresh
        ) {
            Some(self.acquire_remote_fill_lock(&cache).await?)
        } else {
            None
        };
        if refresh {
            for (symbol, _, _) in &physical_ranges {
                cache.purge_symbol_ticks(symbol)?;
            }
        }

        let remote_fill_runtime = self.remote_fill_runtime();
        let total_ranges = physical_ranges.len();
        let mut checked_ranges = 0usize;
        let mut complete_ranges = 0usize;
        let mut incomplete_ranges = 0usize;
        let mut before_by_range = BTreeMap::new();
        let mut fill_requests = Vec::new();
        for (symbol, start_ns, end_ns) in &physical_ranges {
            let before = cache.inspect(symbol, *start_ns, *end_ns)?;
            let is_complete = before.is_complete();
            checked_ranges = checked_ranges.saturating_add(1);
            if is_complete {
                complete_ranges = complete_ranges.saturating_add(1);
            } else {
                incomplete_ranges = incomplete_ranges.saturating_add(1);
            }
            remote_fill_runtime.emit_inspection(
                symbol,
                (*start_ns, *end_ns),
                backtest_remote::BacktestRemoteFillInspectionProgress::new(
                    total_ranges,
                    checked_ranges,
                    complete_ranges,
                    incomplete_ranges,
                ),
            );
            if refresh || !is_complete {
                fill_requests.extend(fill_requests_from_status(&before));
            }
            before_by_range.insert((symbol.clone(), *start_ns, *end_ns), before);
        }

        remote_fill_runtime.emit_plan(build_remote_fill_plan(
            (self.start_ns, self.end_ns),
            logical_symbols.clone(),
            &physical_ranges,
            &before_by_range,
            &fill_requests,
            remote_fill_runtime.config(),
        )?);

        if matches!(self.cache_policy, BacktestCachePolicy::CacheOnly) {
            let mut symbols = Vec::new();
            for (symbol, start_ns, end_ns) in &physical_ranges {
                let before = before_by_range
                    .get(&(symbol.clone(), *start_ns, *end_ns))
                    .cloned()
                    .ok_or_else(|| data_validation("warmup physical range status missing"))?;
                let action = if before.is_complete() {
                    BacktestCacheWarmupAction::SkippedComplete
                } else {
                    BacktestCacheWarmupAction::MissingCacheOnly
                };
                symbols.push(BacktestCacheWarmupSymbolReport {
                    symbol: symbol.clone(),
                    action,
                    before: before.clone(),
                    after: before,
                    rows_written: 0,
                });
            }
            return Ok(build_warmup_report(
                WarmupReportContext {
                    requested_range: (self.start_ns, self.end_ns),
                    cache_policy: self.cache_policy,
                    cache_dir,
                    logical_symbols: logical_symbols.clone(),
                    batch_size: self.warmup_batch_size,
                    remote_used: false,
                },
                symbols,
            ));
        }

        if !fill_requests.is_empty() && self.base.auth.is_none() {
            return Err(data_validation("remote backtest cache fill requires auth"));
        }

        let mut rows_by_symbol = BTreeMap::new();
        let remote_used = !fill_requests.is_empty();
        if !fill_requests.is_empty() {
            let auth = self.base.auth.clone().ok_or(Error::MissingAuth)?;
            let fill_report = backtest_remote::fill_backtest_tick_cache(
                auth.user,
                auth.pass,
                fill_requests,
                cache.clone(),
                remote_fill_runtime,
            )
            .await?;
            for (symbol, rows) in fill_report.rows_by_symbol {
                *rows_by_symbol.entry(symbol).or_insert(0) += rows;
            }
        }

        let mut symbols = Vec::new();
        let mut reported_rows_by_symbol = BTreeSet::new();
        for (symbol, start_ns, end_ns) in &physical_ranges {
            let before = before_by_range
                .get(&(symbol.clone(), *start_ns, *end_ns))
                .cloned()
                .ok_or_else(|| data_validation("warmup physical range status missing"))?;
            let rows_written = if reported_rows_by_symbol.insert(symbol.clone()) {
                rows_by_symbol.get(symbol).copied().unwrap_or_default()
            } else {
                0
            };
            let after = cache.inspect(symbol, *start_ns, *end_ns)?;
            let action = if before.is_complete() && !refresh {
                BacktestCacheWarmupAction::SkippedComplete
            } else if refresh {
                BacktestCacheWarmupAction::RefreshedRemote
            } else {
                BacktestCacheWarmupAction::FilledRemote
            };
            symbols.push(BacktestCacheWarmupSymbolReport {
                symbol: symbol.clone(),
                action,
                before,
                after,
                rows_written,
            });
        }

        Ok(build_warmup_report(
            WarmupReportContext {
                requested_range: (self.start_ns, self.end_ns),
                cache_policy: self.cache_policy,
                cache_dir,
                logical_symbols,
                batch_size: self.warmup_batch_size,
                remote_used,
            },
            symbols,
        ))
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
        push_unique_string(&mut self.symbols, symbol.into());
        self
    }

    /// Declare a kline serial for cache-backed local backtest.
    pub fn kline(
        mut self,
        symbol: impl AsRef<str>,
        duration: Duration,
        view_width: usize,
    ) -> Result<Self> {
        let spec = BacktestKlineSpec {
            symbol: symbol.as_ref().to_string(),
            duration_ns: duration_to_ns(duration)?,
            view_width,
        };
        if !self.kline_specs.iter().any(|existing| existing == &spec) {
            self.kline_specs.push(spec);
        }
        Ok(self)
    }

    /// Declare a tick serial for cache-backed local backtest.
    #[must_use]
    pub fn tick(mut self, symbol: impl AsRef<str>, view_width: usize) -> Self {
        let spec = BacktestTickSpec {
            symbol: symbol.as_ref().to_string(),
            view_width,
        };
        if !self.tick_specs.iter().any(|existing| existing == &spec) {
            self.tick_specs.push(spec);
        }
        self
    }

    /// Pre-declare a price tick for cache-backed local backtest Kline quote synthesis.
    #[must_use]
    pub fn price_tick(mut self, symbol: impl Into<String>, tick: f64) -> Self {
        self.base = self.base.price_tick(symbol, tick);
        self
    }

    /// Pre-declare instrument metadata for cache-backed local backtest Kline quote synthesis.
    #[must_use]
    pub fn instrument_spec(mut self, spec: tqsdk_session::InstrumentSpec) -> Self {
        self.base = self.base.instrument_spec(spec);
        self
    }

    /// Pre-declare multiple instrument metadata entries for cache-backed local backtest.
    #[must_use]
    pub fn instrument_specs(
        mut self,
        specs: impl IntoIterator<Item = tqsdk_session::InstrumentSpec>,
    ) -> Self {
        self.base = self.base.instrument_specs(specs);
        self
    }

    /// Set fallback price tick for cache-backed local backtest Kline quote synthesis.
    ///
    /// Per-symbol [`BacktestBuilder::price_tick`] overrides this fallback.
    #[must_use]
    pub fn default_price_tick(mut self, tick: f64) -> Self {
        self.base = self.base.default_price_tick(tick);
        self
    }

    /// Add futures symbols resolved from the shared relay-compatible selector grammar.
    pub fn universe(mut self, expression: impl AsRef<str>) -> Result<Self> {
        self.universe_expression =
            Some(tqsdk_data::UniverseExpression::parse(expression.as_ref())?);
        Ok(self)
    }

    /// Validate cache coverage and prepare the local replay inputs.
    pub async fn prepare(mut self) -> Result<PreparedBacktest> {
        self.validate_range()?;
        self.apply_market_cache_policy().await?;
        self.apply_default_cache_if_needed()?;
        if let Some(expression) = &self.universe_expression {
            let resolved = resolve_backtest_universe(expression, self.base.auth.as_ref()).await?;
            for symbol in resolved {
                push_unique_string(&mut self.symbols, symbol);
            }
        }
        if self.symbols.is_empty() && self.tick_specs.is_empty() && self.kline_specs.is_empty() {
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
            .ok_or_else(|| data_validation("backtest default cache was not applied"))?;
        let planned = self.planned_inputs()?;
        reject_continuous_native_kline_specs(&planned.native_klines)?;
        let mut synthetic_klines = planned.synthetic_klines.clone();
        synthetic_klines.extend(planned.auto_quote_klines.clone());
        let prepared_inputs = PreparedBacktestInputs {
            tick_sources: resolve_backtest_tick_sources(
                &planned.tick_symbols,
                self.start_ns,
                self.end_ns,
            )
            .await?,
            native_klines: planned.native_klines.clone(),
            synthetic_klines,
        };
        let remote_fill_lock = if matches!(
            self.cache_policy,
            BacktestCachePolicy::RemoteOnMiss | BacktestCachePolicy::Refresh
        ) {
            Some(self.acquire_remote_fill_lock(cache).await?)
        } else {
            None
        };
        if matches!(self.cache_policy, BacktestCachePolicy::Refresh) {
            if self.base.auth.is_none() {
                return Err(data_validation("remote backtest cache fill requires auth"));
            }
            for (symbol, _, _) in physical_tick_ranges(&prepared_inputs.tick_sources) {
                cache.purge_symbol_ticks(symbol)?;
            }
            let history = tqsdk_data::HistorySeriesCache::open(cache.cache_dir())?;
            for spec in &planned.native_klines {
                history.purge_kline_series(&spec.symbol, spec.duration_ns)?;
            }
        }
        let history = tqsdk_data::HistorySeriesCache::open(cache.cache_dir())?;
        let mut missing_tick_symbols = Vec::new();
        let mut tick_fill_requests = Vec::new();
        for (symbol, range_start_ns, range_end_ns) in
            physical_tick_ranges(&prepared_inputs.tick_sources)
        {
            let coverage = cache.coverage(symbol, range_start_ns, range_end_ns)?;
            if !coverage.is_complete() {
                tick_fill_requests.extend(fill_requests_from_coverage(&coverage));
                missing_tick_symbols.push(coverage);
            }
        }
        let mut missing_kline_series = Vec::new();
        let mut kline_fill_requests = Vec::new();
        for spec in &planned.native_klines {
            let coverage = history.kline_coverage(
                &spec.symbol,
                spec.duration_ns,
                self.start_ns,
                self.end_ns,
            )?;
            if !coverage.is_complete() {
                for (start_ns, end_ns) in &coverage.missing_ranges {
                    kline_fill_requests.push(
                        backtest_history_remote::BacktestKlineFillRequest::new(
                            spec.symbol.clone(),
                            spec.duration_ns,
                            *start_ns,
                            *end_ns,
                        ),
                    );
                }
                missing_kline_series.push((spec.clone(), coverage.missing_ranges));
            }
        }

        let mode = match self.cache_policy {
            BacktestCachePolicy::CacheOnly => {
                if let Some(coverage) = missing_tick_symbols.first() {
                    return Err(data_validation(format!(
                        "backtest cache coverage is incomplete for {}: {:?}",
                        coverage.symbol, coverage.missing_ranges
                    )));
                }
                if let Some((spec, missing_ranges)) = missing_kline_series.first() {
                    return Err(data_validation(format!(
                        "backtest native kline cache coverage is incomplete for {} duration {}: {:?}",
                        spec.symbol, spec.duration_ns, missing_ranges
                    )));
                }
                PreparedBacktestMode::CacheHit {
                    inputs: prepared_inputs.clone(),
                }
            }
            BacktestCachePolicy::RemoteOnMiss => {
                if missing_tick_symbols.is_empty() && missing_kline_series.is_empty() {
                    PreparedBacktestMode::CacheHit {
                        inputs: prepared_inputs.clone(),
                    }
                } else {
                    if self.base.auth.is_none() {
                        return Err(data_validation("remote backtest cache fill requires auth"));
                    }
                    PreparedBacktestMode::RemoteCaching {
                        inputs: prepared_inputs.clone(),
                        tick_fill_requests,
                        kline_fill_requests,
                    }
                }
            }
            BacktestCachePolicy::Refresh => {
                if self.base.auth.is_none() {
                    return Err(data_validation("remote backtest cache fill requires auth"));
                }
                PreparedBacktestMode::RemoteCaching {
                    inputs: prepared_inputs.clone(),
                    tick_fill_requests,
                    kline_fill_requests,
                }
            }
            BacktestCachePolicy::Disabled => {
                return Err(data_validation(format!(
                    "backtest cache policy {:?} is not supported in phase 1",
                    self.cache_policy
                )));
            }
        };

        let remote_tick_used = matches!(
            &mode,
            PreparedBacktestMode::RemoteCaching {
                tick_fill_requests,
                ..
            } if !tick_fill_requests.is_empty()
        );
        let remote_kline_used = matches!(
            &mode,
            PreparedBacktestMode::RemoteCaching {
                kline_fill_requests,
                ..
            } if !kline_fill_requests.is_empty()
        );
        let resolved_symbols = planned
            .tick_symbols
            .iter()
            .cloned()
            .chain(
                prepared_inputs
                    .native_klines
                    .iter()
                    .map(|spec| spec.symbol.clone()),
            )
            .chain(
                prepared_inputs
                    .synthetic_klines
                    .iter()
                    .map(|spec| spec.symbol.clone()),
            )
            .collect::<BTreeSet<_>>()
            .len();
        let data_report = BacktestDataReport {
            requested_range: (self.start_ns, self.end_ns),
            cache_policy: self.cache_policy,
            cache_dir: cache.cache_dir().to_path_buf(),
            resolved_symbols,
            remote_used: remote_tick_used || remote_kline_used,
            tick_symbols: planned.tick_symbols.len(),
            native_kline_series: prepared_inputs.native_klines.len(),
            synthetic_kline_series: prepared_inputs.synthetic_klines.len(),
            remote_tick_used,
            remote_kline_used,
        };
        let remote_fill_lock = if matches!(&mode, PreparedBacktestMode::RemoteCaching { .. }) {
            remote_fill_lock
        } else {
            None
        };
        Ok(PreparedBacktest {
            builder: self,
            data_report,
            mode,
            remote_fill_lock,
        })
    }

    /// Connect the backtest.
    ///
    /// Connect the cache-backed local replay path. Use
    /// [`BacktestBuilder::disabled_cache`] to force the official server-side
    /// backtest stream without local persistence.
    pub async fn connect(self) -> Result<Tq> {
        self.validate_range()?;
        if matches!(self.cache_policy, BacktestCachePolicy::Disabled) {
            return self.into_remote_backtest().connect().await;
        }
        self.prepare().await?.connect().await
    }
}

fn fill_requests_from_status(
    status: &BacktestTickCacheStatus,
) -> Vec<backtest_remote::RemoteBacktestCacheFillRequest> {
    fill_requests_from_ranges(status.symbol.as_str(), &status.missing_ranges)
}

fn build_remote_fill_plan(
    requested_range: (i64, i64),
    logical_symbols: Vec<String>,
    physical_ranges: &[(String, i64, i64)],
    before_by_range: &BTreeMap<(String, i64, i64), BacktestTickCacheStatus>,
    fill_requests: &[backtest_remote::RemoteBacktestCacheFillRequest],
    config: BacktestRemoteFillConfig,
) -> Result<backtest_remote::RemoteFillPlan> {
    let logical_batches = backtest_remote::remote_fill_logical_batch_count(
        fill_requests.to_vec(),
        config.symbol_batch_size,
    )?;
    let mut by_symbol = BTreeMap::<String, (Vec<(i64, i64)>, Vec<(i64, i64)>)>::new();
    for (symbol, start_ns, end_ns) in physical_ranges {
        let status = before_by_range
            .get(&(symbol.clone(), *start_ns, *end_ns))
            .ok_or_else(|| data_validation("remote fill plan coverage status missing"))?;
        let entry = by_symbol.entry(symbol.clone()).or_default();
        entry.0.push((*start_ns, *end_ns));
        entry.1.extend(status.missing_ranges.iter().copied());
    }
    let physical_symbols = by_symbol
        .into_iter()
        .map(|(symbol, (requested_ranges, missing_ranges))| {
            backtest_remote::RemoteFillPlanSymbol::new(symbol, requested_ranges, missing_ranges)
        })
        .collect();
    Ok(backtest_remote::RemoteFillPlan::new(
        requested_range,
        logical_symbols,
        physical_symbols,
        logical_batches,
    ))
}

fn fill_requests_from_coverage(
    coverage: &tqsdk_data::BacktestTickCoverage,
) -> Vec<backtest_remote::RemoteBacktestCacheFillRequest> {
    fill_requests_from_ranges(coverage.symbol.as_str(), &coverage.missing_ranges)
}

fn fill_requests_from_ranges(
    symbol: &str,
    ranges: &[(i64, i64)],
) -> Vec<backtest_remote::RemoteBacktestCacheFillRequest> {
    ranges
        .iter()
        .map(|(start_ns, end_ns)| {
            backtest_remote::RemoteBacktestCacheFillRequest::new(symbol, *start_ns, *end_ns)
        })
        .collect()
}

struct WarmupReportContext {
    requested_range: (i64, i64),
    cache_policy: BacktestCachePolicy,
    cache_dir: std::path::PathBuf,
    logical_symbols: Vec<String>,
    batch_size: usize,
    remote_used: bool,
}

fn build_warmup_report(
    context: WarmupReportContext,
    symbols: Vec<BacktestCacheWarmupSymbolReport>,
) -> BacktestCacheWarmupReport {
    let symbols_skipped = symbols
        .iter()
        .filter(|symbol| symbol.action == BacktestCacheWarmupAction::SkippedComplete)
        .count();
    let symbols_missing = symbols
        .iter()
        .filter(|symbol| symbol.action == BacktestCacheWarmupAction::MissingCacheOnly)
        .count();
    let symbols_filled = symbols
        .iter()
        .filter(|symbol| {
            matches!(
                symbol.action,
                BacktestCacheWarmupAction::FilledRemote
                    | BacktestCacheWarmupAction::RefreshedRemote
            )
        })
        .count();
    let rows_written = symbols.iter().map(|symbol| symbol.rows_written).sum();
    BacktestCacheWarmupReport {
        requested_range: context.requested_range,
        cache_policy: context.cache_policy,
        cache_dir: context.cache_dir,
        logical_symbols: context.logical_symbols,
        batch_size: context.batch_size,
        symbols_total: symbols.len(),
        symbols_skipped,
        symbols_missing,
        symbols_filled,
        rows_written,
        remote_used: context.remote_used,
        symbols,
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
            remote_fill_lock: _remote_fill_lock,
        } = self;
        let BacktestBuilder {
            base,
            start_ns,
            end_ns,
            cache,
            cache_policy: _,
            symbols: _,
            kline_specs,
            tick_specs,
            universe_expression: _,
            warmup_batch_size: _,
            remote_fill_config,
            remote_fill_progress,
            remote_fill_telemetry,
            remote_fill_cancellation,
            remote_fill_lock_wait: _,
        } = builder;
        let remote_fill_runtime = backtest_remote::RemoteBacktestFillRuntime::new(
            remote_fill_config,
            remote_fill_progress,
            remote_fill_telemetry,
            remote_fill_cancellation,
        );
        let cache = cache.ok_or_else(|| data_validation("prepared backtest cache missing"))?;
        let mut base = base;
        for spec in &kline_specs {
            base = base.kline_symbol(
                spec.symbol.clone(),
                Duration::from_nanos(spec.duration_ns as u64),
                spec.view_width,
            );
        }
        for spec in &tick_specs {
            base = base.tick_symbol(spec.symbol.clone(), spec.view_width);
        }
        match mode {
            PreparedBacktestMode::CacheHit { inputs } => {
                let stream = history_backtest_stream(cache.cache_dir(), start_ns, end_ns, inputs)?;
                base.replay_backtest_stream(Box::new(stream))
                    .connect()
                    .await
            }
            PreparedBacktestMode::RemoteCaching {
                inputs,
                tick_fill_requests,
                kline_fill_requests,
            } => {
                let auth = base.auth.clone().ok_or(Error::MissingAuth)?;
                if !tick_fill_requests.is_empty() {
                    backtest_remote::fill_backtest_tick_cache(
                        auth.user.clone(),
                        auth.pass.clone(),
                        tick_fill_requests,
                        cache.clone(),
                        remote_fill_runtime.clone(),
                    )
                    .await?;
                }
                if !kline_fill_requests.is_empty() {
                    let report = backtest_history_remote::fill_backtest_kline_cache(
                        &auth,
                        cache.cache_dir(),
                        kline_fill_requests,
                    )
                    .await?;
                    let _ = report.rows_by_series.len();
                }
                let stream = history_backtest_stream(cache.cache_dir(), start_ns, end_ns, inputs)?;
                base.replay_backtest_stream(Box::new(stream))
                    .connect()
                    .await
            }
        }
    }
}

fn history_backtest_stream(
    cache_dir: &Path,
    start_ns: i64,
    end_ns: i64,
    inputs: PreparedBacktestInputs,
) -> Result<tqsdk_task::HistoryBacktestReplayStream> {
    let PreparedBacktestInputs {
        tick_sources,
        native_klines,
        synthetic_klines,
    } = inputs;
    let mut synthetic_kline_sources = Vec::new();
    for spec in synthetic_klines {
        let source_count = tick_sources
            .iter()
            .filter(|source| source.replay_symbol == spec.symbol)
            .count();
        if source_count == 0 {
            return Err(data_validation(format!(
                "backtest synthetic kline {} has no tick source",
                spec.symbol
            )));
        }
        synthetic_kline_sources.extend(
            tick_sources
                .iter()
                .filter(|source| source.replay_symbol == spec.symbol)
                .cloned()
                .map(
                    |tick_source| tqsdk_task::HistoryBacktestSyntheticKlineSource {
                        tick_source,
                        duration_ns: spec.duration_ns,
                    },
                ),
        );
    }
    tqsdk_task::HistoryBacktestReplayStream::new_projected(
        tqsdk_task::HistoryBacktestProjectedReplayRequest {
            cache: tqsdk_data::HistorySeriesCache::open(cache_dir)?,
            start_ns,
            end_ns,
            tick_sources,
            native_klines: native_klines
                .into_iter()
                .map(|spec| tqsdk_task::HistoryBacktestKlineRequest {
                    symbol: spec.symbol,
                    duration_ns: spec.duration_ns,
                })
                .collect(),
            synthetic_kline_sources,
        },
    )
    .map_err(Error::from)
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
    market_cache: Option<MarketCachePolicy>,
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
            market_cache: None,
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

    /// Configure a shared market-cache policy for this facade.
    ///
    /// Live/session modes start tick recording during [`TqBuilder::connect`].
    /// Cache-backed local backtests use the same policy as their default cache
    /// directory and symbol set unless explicit builder calls override them.
    #[must_use]
    pub fn market_cache(mut self, policy: MarketCachePolicy) -> Self {
        self.market_cache = Some(policy);
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
    /// The shared `tqsdk-data` default history cache root is used unless an
    /// explicit cache directory, cache store, or market cache policy is set.
    #[must_use]
    pub fn backtest(self, start_ns: i64, end_ns: i64) -> BacktestBuilder {
        BacktestBuilder {
            base: self,
            start_ns,
            end_ns,
            cache: None,
            cache_policy: BacktestCachePolicy::default(),
            symbols: Vec::new(),
            kline_specs: Vec::new(),
            tick_specs: Vec::new(),
            universe_expression: None,
            warmup_batch_size: DEFAULT_BACKTEST_WARMUP_BATCH_SIZE,
            remote_fill_config: None,
            remote_fill_progress: None,
            remote_fill_telemetry: None,
            remote_fill_cancellation: None,
            remote_fill_lock_wait: None,
        }
    }

    fn with_remote_backtest(mut self, start_ns: i64, end_ns: i64) -> Self {
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

    /// Pre-declare a kline serial for local replay/backtest.
    #[must_use]
    pub fn kline_symbol(
        mut self,
        symbol: impl Into<String>,
        duration: Duration,
        view_width: usize,
    ) -> Self {
        self.local_backtest_recipe = self
            .local_backtest_recipe
            .kline_symbol(symbol, duration, view_width);
        self
    }

    /// Pre-declare a tick serial for local replay/backtest.
    #[must_use]
    pub fn tick_symbol(mut self, symbol: impl Into<String>, view_width: usize) -> Self {
        self.local_backtest_recipe = self.local_backtest_recipe.tick_symbol(symbol, view_width);
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
            market_cache,
            backtest,
            local_backtest_recipe,
        } = self;
        let is_server_side_backtest = backtest
            .as_ref()
            .is_some_and(BacktestConfig::is_server_side);
        if is_server_side_backtest && !trade_targets.is_empty() {
            return Err(remote_backtest_trade_login_error());
        }
        #[cfg(feature = "live")]
        if is_server_side_backtest && auto_trade_login.is_some() {
            return Err(remote_backtest_trade_login_error());
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
                let mut tq = tq;
                #[cfg(feature = "live")]
                {
                    if let Some(auto_trade_login) = auto_trade_login {
                        apply_auto_trade_login(&mut tq, auto_trade_login).await?;
                    }
                    if let Some(policy) = market_cache {
                        tq.start_market_cache(policy).await?;
                    }
                }
                #[cfg(not(feature = "live"))]
                {
                    let _ = market_cache;
                }
                Ok(tq)
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
    let remote_backtest = backtest
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
    if remote_backtest {
        return Ok(Tq::from_api_with_remote_backtest(api));
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

fn push_unique_string(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

async fn resolve_backtest_universe(
    expression: &tqsdk_data::UniverseExpression,
    auth: Option<&Auth>,
) -> Result<Vec<String>> {
    if expression.is_static_symbol_only() {
        return Ok(tqsdk_data::resolve_static_symbols_with_expression(
            expression,
        )?);
    }

    let auth = auth.ok_or_else(|| data_validation("dynamic backtest universe requires auth"))?;
    let client =
        tqsdk_data::session_client_builder_for_futures_discovery(&auth.user, &auth.pass).build()?;
    let mut resolver = tqsdk_data::SessionFuturesUniverseResolver::new(client);
    if tqsdk_data::expression_requires_activity_quotes(expression) {
        let activity_client =
            tqsdk_session::SessionClientBuilder::new(auth.user.clone(), auth.pass.clone())
                .futures_market()
                .build()?;
        resolver = resolver.with_activity_client(activity_client);
    }
    Ok(tqsdk_data::resolve_futures_universe_symbols(expression, &mut resolver).await?)
}

async fn resolve_universe_with_session(
    expression: &tqsdk_data::UniverseExpression,
    session: tqsdk_session::SessionClient,
) -> Result<Vec<String>> {
    if expression.is_static_symbol_only() {
        return Ok(tqsdk_data::resolve_static_symbols_with_expression(
            expression,
        )?);
    }

    let mut resolver = tqsdk_data::SessionFuturesUniverseResolver::new(session.clone());
    if tqsdk_data::expression_requires_activity_quotes(expression) {
        resolver = resolver.with_activity_client(session);
    }
    Ok(tqsdk_data::resolve_futures_universe_symbols(expression, &mut resolver).await?)
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
        "default account is not configured; use backtest(...).cache_dir(...), replay_backtest(...), tqkq_sim(), trade_account(...), or login_trade_account(...)",
    ))
}

#[cfg(feature = "live")]
fn ambiguous_default_account() -> Error {
    Error::from(tqsdk_session::SessionFacadeError::InvalidState(
        "default account is ambiguous after multiple trade account logins; use explicit account-specific helpers or configure a single automatic trade login",
    ))
}

fn remote_backtest_trade_login_error() -> Error {
    Error::from(tqsdk_session::SessionFacadeError::InvalidState(
        "server-side backtest/replay cannot be combined with trade targets or automatic trade account login; use backtest(...).cache_dir(...) / market_cache(...) or replay_backtest(...) for simulated fills",
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
    use std::path::PathBuf;

    use super::{BacktestTickCacheStatus, Error, fill_requests_from_status, parse_env_value};

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

    #[test]
    fn fill_requests_use_only_sparse_cache_missing_ranges() {
        let status = BacktestTickCacheStatus {
            backend_format: "tqbn",
            cache_dir: PathBuf::from("/tmp/cache"),
            series_path: PathBuf::from("/tmp/cache/series"),
            series_path_exists: true,
            symbol: "SHFE.rb2601".to_string(),
            range_start_ns: 1_000,
            range_end_ns: 7_000,
            cached_ranges: vec![(1_000, 2_000), (4_000, 5_000)],
            missing_ranges: vec![(2_000, 4_000), (5_000, 7_000)],
        };

        let requests = fill_requests_from_status(&status);

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].symbol, "SHFE.rb2601");
        assert_eq!((requests[0].start_ns, requests[0].end_ns), (2_000, 4_000));
        assert_eq!(requests[1].symbol, "SHFE.rb2601");
        assert_eq!((requests[1].start_ns, requests[1].end_ns), (5_000, 7_000));
    }
}

#[cfg(test)]
mod builder_contract_tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use chrono::{NaiveDate, TimeZone};
    use serde_json::json;
    use tqsdk_core::Tick;
    use tqsdk_data::{BacktestTickCache, HistoricalContUnderlyingSegment, HistorySeriesCache};
    use tqsdk_task::{BacktestMarketStream, HistoryBacktestTickSource};

    use super::{
        Auth, AutoTradeLogin, BacktestConfig, BacktestKlineSource, BacktestKlineSpec,
        BacktestTickSpec, Error, LOCAL_BACKTEST_ACCOUNT_ID, PreparedBacktestInputs,
        PreparedBacktestMode, Tq, TqBuilder, backtest_kline_source,
        continuous_mapping_query_window, continuous_tick_sources, duration_to_ns,
        history_backtest_stream, physical_tick_ranges, plan_backtest_inputs,
        reject_continuous_native_kline_specs, session_builder,
    };

    #[test]
    fn backtest_kline_source_uses_synthetic_path_through_one_minute() {
        assert_eq!(
            backtest_kline_source(duration_to_ns(std::time::Duration::from_secs(1)).unwrap())
                .unwrap(),
            BacktestKlineSource::SynthesizedFromTick
        );
        assert_eq!(
            backtest_kline_source(duration_to_ns(std::time::Duration::from_secs(59)).unwrap())
                .unwrap(),
            BacktestKlineSource::SynthesizedFromTick
        );
        assert_eq!(
            backtest_kline_source(duration_to_ns(std::time::Duration::from_secs(60)).unwrap())
                .unwrap(),
            BacktestKlineSource::SynthesizedFromTick
        );
        assert_eq!(
            backtest_kline_source(duration_to_ns(std::time::Duration::from_secs(61)).unwrap())
                .unwrap(),
            BacktestKlineSource::NativeKline
        );
        assert!(backtest_kline_source(0).is_err());
    }

    #[test]
    fn backtest_quote_fallback_plans_tick_and_kline_sources() {
        let tick_only = plan_backtest_inputs(
            &[],
            &[BacktestTickSpec {
                symbol: "SHFE.rb2601".to_string(),
                view_width: 100,
            }],
            &[],
        )
        .unwrap();
        assert_eq!(tick_only.tick_symbols, vec!["SHFE.rb2601".to_string()]);
        assert!(tick_only.auto_quote_klines.is_empty());

        let thirty_seconds = duration_to_ns(std::time::Duration::from_secs(30)).unwrap();
        let one_minute = duration_to_ns(std::time::Duration::from_secs(60)).unwrap();
        let five_minutes = duration_to_ns(std::time::Duration::from_secs(300)).unwrap();

        let short_kline_only = plan_backtest_inputs(
            &["SHFE.rb2601".to_string()],
            &[],
            &[BacktestKlineSpec {
                symbol: "SHFE.rb2601".to_string(),
                duration_ns: thirty_seconds,
                view_width: 200,
            }],
        )
        .unwrap();
        assert_eq!(
            short_kline_only.tick_symbols,
            vec!["SHFE.rb2601".to_string()]
        );
        assert_eq!(short_kline_only.synthetic_klines.len(), 1);
        assert_eq!(
            short_kline_only.synthetic_klines[0].duration_ns,
            thirty_seconds
        );
        assert!(short_kline_only.auto_quote_klines.is_empty());

        let minute_kline_only = plan_backtest_inputs(
            &["SHFE.rb2601".to_string()],
            &[],
            &[BacktestKlineSpec {
                symbol: "SHFE.rb2601".to_string(),
                duration_ns: one_minute,
                view_width: 200,
            }],
        )
        .unwrap();
        assert_eq!(
            minute_kline_only.synthetic_klines[0].duration_ns,
            one_minute
        );
        assert!(minute_kline_only.native_klines.is_empty());

        let long_kline_only = plan_backtest_inputs(
            &["SHFE.rb2601".to_string()],
            &[],
            &[BacktestKlineSpec {
                symbol: "SHFE.rb2601".to_string(),
                duration_ns: five_minutes,
                view_width: 200,
            }],
        )
        .unwrap();
        assert_eq!(long_kline_only.native_klines.len(), 1);
        assert_eq!(long_kline_only.native_klines[0].duration_ns, five_minutes);
        assert_eq!(long_kline_only.auto_quote_klines.len(), 1);
        assert_eq!(long_kline_only.auto_quote_klines[0].duration_ns, one_minute);

        let quote_only = plan_backtest_inputs(&["DCE.i2601".to_string()], &[], &[]).unwrap();
        assert_eq!(quote_only.tick_symbols, vec!["DCE.i2601".to_string()]);
        assert_eq!(quote_only.auto_quote_klines.len(), 1);
        assert_eq!(quote_only.auto_quote_klines[0].duration_ns, one_minute);

        let mixed_klines = plan_backtest_inputs(
            &["SHFE.rb2601".to_string()],
            &[],
            &[
                BacktestKlineSpec {
                    symbol: "SHFE.rb2601".to_string(),
                    duration_ns: thirty_seconds,
                    view_width: 200,
                },
                BacktestKlineSpec {
                    symbol: "SHFE.rb2601".to_string(),
                    duration_ns: five_minutes,
                    view_width: 200,
                },
            ],
        )
        .unwrap();
        assert_eq!(mixed_klines.synthetic_klines.len(), 1);
        assert_eq!(mixed_klines.native_klines.len(), 1);
        assert!(mixed_klines.auto_quote_klines.is_empty());
    }

    #[test]
    fn continuous_tick_sources_clip_main_contract_segments_to_backtest_window() {
        let main = "KQ.m@SHFE.au";
        let start = cst_datetime_ns(2026, 5, 15, 21, 0, 0);
        let end = cst_datetime_ns(2026, 5, 19, 10, 0, 0);
        let segments = [
            HistoricalContUnderlyingSegment {
                symbol: main.to_string(),
                underlying: "SHFE.au2608".to_string(),
                start_date: "2026-05-18".to_string(),
                end_date: "2026-05-18".to_string(),
                trading_days: 1,
            },
            HistoricalContUnderlyingSegment {
                symbol: main.to_string(),
                underlying: "SHFE.au2610".to_string(),
                start_date: "2026-05-19".to_string(),
                end_date: "2026-05-19".to_string(),
                trading_days: 1,
            },
        ];

        let sources = continuous_tick_sources(main, start, end, &segments).unwrap();

        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].replay_symbol, main);
        assert_eq!(sources[0].cache_symbol, "SHFE.au2608");
        assert_eq!(sources[0].start_ns, start);
        assert_eq!(sources[0].end_ns, cst_datetime_ns(2026, 5, 18, 18, 0, 0));
        assert_eq!(sources[1].replay_symbol, main);
        assert_eq!(sources[1].cache_symbol, "SHFE.au2610");
        assert_eq!(sources[1].start_ns, cst_datetime_ns(2026, 5, 18, 18, 0, 0));
        assert_eq!(sources[1].end_ns, end);
    }

    #[test]
    fn physical_tick_ranges_merge_main_and_concrete_contract_overlap() {
        let sources = [
            HistoryBacktestTickSource {
                replay_symbol: "KQ.m@SHFE.au".to_string(),
                cache_symbol: "SHFE.au2608".to_string(),
                start_ns: 100,
                end_ns: 200,
            },
            HistoryBacktestTickSource {
                replay_symbol: "SHFE.au2608".to_string(),
                cache_symbol: "SHFE.au2608".to_string(),
                start_ns: 50,
                end_ns: 150,
            },
            HistoryBacktestTickSource {
                replay_symbol: "KQ.m@SHFE.au".to_string(),
                cache_symbol: "SHFE.au2610".to_string(),
                start_ns: 200,
                end_ns: 300,
            },
        ];

        assert_eq!(
            physical_tick_ranges(&sources),
            vec![
                ("SHFE.au2608".to_string(), 50, 200),
                ("SHFE.au2610".to_string(), 200, 300),
            ]
        );
    }

    #[test]
    fn continuous_mapping_query_window_uses_cst_trading_days() {
        let start = cst_datetime_ns(2026, 5, 15, 21, 0, 0);
        let end = cst_datetime_ns(2026, 5, 19, 10, 0, 0);

        let (days, end_date) = continuous_mapping_query_window(start, end).unwrap();

        assert_eq!(days, 2);
        assert_eq!(end_date, NaiveDate::from_ymd_opt(2026, 5, 19).unwrap());
    }

    #[test]
    fn continuous_contract_native_kline_requires_tick_synthesis() {
        let error = reject_continuous_native_kline_specs(&[BacktestKlineSpec {
            symbol: "KQ.m@SHFE.au".to_string(),
            duration_ns: 61_000_000_000,
            view_width: 20,
        }])
        .unwrap_err();

        assert!(error.to_string().contains("duration <= 60s"));
    }

    #[tokio::test]
    async fn facade_history_stream_replays_main_contract_from_physical_tick_cache() {
        let dir = temp_cache_dir("facade-projected-main-contract");
        let main = "KQ.m@SHFE.au";
        let physical = "SHFE.au2608";
        BacktestTickCache::open(&dir)
            .unwrap()
            .store_ticks(physical, 1_000, 2_000, [tick(1, 1_000, 500.0, 1)])
            .unwrap();

        let mut stream = history_backtest_stream(
            &dir,
            1_000,
            2_000,
            PreparedBacktestInputs {
                tick_sources: vec![HistoryBacktestTickSource {
                    replay_symbol: main.to_string(),
                    cache_symbol: physical.to_string(),
                    start_ns: 1_000,
                    end_ns: 2_000,
                }],
                native_klines: Vec::new(),
                synthetic_klines: Vec::new(),
            },
        )
        .unwrap();

        let event = stream.next_event().await.unwrap().unwrap();
        assert_eq!(event.symbol(), main);
        assert_eq!(event.underlying_symbol(), Some(physical));
        assert!(stream.next_event().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn backtest_kline_sixty_seconds_prepares_synthetic_from_ticks() {
        let dir = temp_cache_dir("facade-synthetic-kline");
        BacktestTickCache::open(&dir)
            .unwrap()
            .store_ticks(
                "SHFE.rb2601",
                0,
                120_000_000_000,
                [tick(1, 1_000, 101.0, 10)],
            )
            .unwrap();

        let prepared = TqBuilder::new()
            .backtest(0, 120_000_000_000)
            .cache_dir(&dir)
            .unwrap()
            .cache_only()
            .kline("SHFE.rb2601", Duration::from_secs(60), 20)
            .unwrap()
            .prepare()
            .await
            .unwrap();

        assert!(!prepared.data_report.remote_used);
        assert!(!prepared.data_report.remote_tick_used);
        assert!(!prepared.data_report.remote_kline_used);
        assert_eq!(prepared.data_report.tick_symbols, 1);
        assert_eq!(prepared.data_report.native_kline_series, 0);
        assert_eq!(prepared.data_report.synthetic_kline_series, 1);

        match prepared.mode {
            PreparedBacktestMode::CacheHit { inputs } => {
                assert_eq!(inputs.tick_sources.len(), 1);
                assert_eq!(inputs.tick_sources[0].replay_symbol, "SHFE.rb2601");
                assert_eq!(inputs.tick_sources[0].cache_symbol, "SHFE.rb2601");
                assert!(inputs.native_klines.is_empty());
                assert_eq!(inputs.synthetic_klines.len(), 1);
                assert_eq!(inputs.synthetic_klines[0].duration_ns, 60_000_000_000);
            }
            PreparedBacktestMode::RemoteCaching { .. } => {
                panic!("complete tick cache should not request remote fill")
            }
        }

        let kline_coverage = HistorySeriesCache::open(&dir)
            .unwrap()
            .kline_coverage("SHFE.rb2601", 60_000_000_000, 0, 120_000_000_000)
            .unwrap();
        assert!(
            !kline_coverage.is_complete(),
            "synthetic klines must not be persisted as native history cache"
        );
    }

    #[tokio::test]
    async fn backtest_kline_above_sixty_seconds_prepares_native_remote_fill() {
        let dir = temp_cache_dir("facade-native-kline");
        BacktestTickCache::open(&dir)
            .unwrap()
            .store_ticks(
                "SHFE.rb2601",
                0,
                122_000_000_000,
                [tick(1, 1_000, 101.0, 10)],
            )
            .unwrap();

        let prepared = TqBuilder::new()
            .auth("demo-user", "demo-pass")
            .backtest(0, 122_000_000_000)
            .cache_dir(&dir)
            .unwrap()
            .remote_on_miss()
            .kline("SHFE.rb2601", Duration::from_secs(61), 20)
            .unwrap()
            .prepare()
            .await
            .unwrap();

        assert!(prepared.data_report.remote_used);
        assert!(!prepared.data_report.remote_tick_used);
        assert!(prepared.data_report.remote_kline_used);
        assert_eq!(prepared.data_report.tick_symbols, 1);
        assert_eq!(prepared.data_report.native_kline_series, 1);
        assert_eq!(prepared.data_report.synthetic_kline_series, 1);

        match prepared.mode {
            PreparedBacktestMode::RemoteCaching {
                inputs,
                tick_fill_requests,
                kline_fill_requests,
            } => {
                assert!(tick_fill_requests.is_empty());
                assert_eq!(kline_fill_requests.len(), 1);
                assert_eq!(kline_fill_requests[0].symbol, "SHFE.rb2601");
                assert_eq!(kline_fill_requests[0].duration_ns, 61_000_000_000);
                assert_eq!(inputs.native_klines.len(), 1);
                assert_eq!(inputs.native_klines[0].duration_ns, 61_000_000_000);
                assert_eq!(inputs.synthetic_klines.len(), 1);
                assert_eq!(inputs.synthetic_klines[0].duration_ns, 60_000_000_000);
            }
            PreparedBacktestMode::CacheHit { .. } => {
                panic!("missing native kline cache should request remote fill")
            }
        }
    }

    fn tick(id: i64, datetime: i64, last_price: f64, volume: i64) -> Tick {
        Tick {
            id,
            datetime,
            last_price,
            volume,
            ..Tick::default()
        }
    }

    fn cst_datetime_ns(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> i64 {
        chrono::FixedOffset::east_opt(8 * 60 * 60)
            .unwrap()
            .with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
            .timestamp()
            * 1_000_000_000
    }

    fn temp_cache_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("tqsdk-{prefix}-{nanos}"))
    }

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(name);
            // Tests hold a process-local mutex while this override is active.
            unsafe {
                std::env::set_var(name, value);
            }
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(previous) = &self.previous {
                    std::env::set_var(self.name, previous);
                } else {
                    std::env::remove_var(self.name);
                }
            }
        }
    }

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
    async fn backtest_disabled_cache_rejects_auto_trade_login_before_network() {
        let error = TqBuilder::new()
            .auth("demo-user", "demo-pass")
            .trade_account("9999", "acct-1", "secret")
            .backtest(1_000, 2_000)
            .disabled_cache()
            .connect()
            .await
            .err()
            .expect("server-side backtest must not auto-login a trade account");

        assert_eq!(
            error.to_string(),
            "invalid session facade state: server-side backtest/replay cannot be combined with trade targets or automatic trade account login; use backtest(...).cache_dir(...) / market_cache(...) or replay_backtest(...) for simulated fills"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backtest_cache_only_uses_default_cache_before_network() {
        static HISTORY_CACHE_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

        let _lock = HISTORY_CACHE_ENV_LOCK.lock().await;
        let cache_dir = temp_cache_dir("facade-default-cache-only");
        let _env = EnvVarGuard::set("TQSDK_HISTORY_CACHE_DIR", &cache_dir);
        let error = TqBuilder::new()
            .backtest(1_000, 2_000)
            .symbol("SHFE.rb2601")
            .cache_only()
            .connect()
            .await
            .err()
            .expect("cache_only default cache without data must fail before auth or network");

        assert!(
            error
                .to_string()
                .contains("backtest cache coverage is incomplete for SHFE.rb2601"),
            "unexpected error: {error}"
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
