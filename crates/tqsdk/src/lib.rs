#![cfg_attr(not(test), forbid(unsafe_code))]
//! User-facing facade crate for `tqsdk-rust`.
//!
//! This crate gives ordinary users one dependency and one prelude while keeping
//! the underlying `core` / `session` / `wait` / `stream` / `task` / `data`
//! boundaries available under [`advanced`].

use std::env;
use std::fmt;
use std::time::Duration;

use chrono::{FixedOffset, NaiveDate, TimeZone, Utc};

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
            DataClient, DataError, HistoricalContUnderlyingRow, HistoricalContUnderlyingSegment,
            KlineDataSeries, KlineDataSeriesRequest, TickDataSeries, TickDataSeriesRequest,
            TradingCalendarRow, historical_cont_underlying_segments,
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

    pub mod stream {
        pub type TqStream = tqsdk_stream::TqStream;
        pub type TqStreamBuilder = tqsdk_stream::TqStreamBuilder;
    }

    pub mod task {
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

/// Result type for the user-facing facade.
pub type Result<T> = std::result::Result<T, Error>;

/// Default account id used by [`Tq::local_backtest`].
pub const LOCAL_BACKTEST_ACCOUNT_ID: &str = tqsdk_task::LOCAL_BACKTEST_ACCOUNT_ID;

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;
const TRADING_DAY_START_OFFSET_NS: i64 = 6 * 60 * 60 * NANOS_PER_SECOND;
const TRADING_DAY_END_OFFSET_NS: i64 = 18 * 60 * 60 * NANOS_PER_SECOND;
const CST_OFFSET_SECONDS: i32 = 8 * 60 * 60;
const CST_1990_01_01_NS: i64 = 631_123_200_000_000_000;
#[cfg(all(feature = "services", feature = "live"))]
const SERVER_REPLAY_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

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
    #[cfg(all(feature = "services", feature = "live"))]
    server_replay: Option<tqsdk_session::ServerReplaySession>,
    #[cfg(all(feature = "services", feature = "live"))]
    server_replay_heartbeat: Option<tokio::task::JoinHandle<()>>,
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
            server_replay: Some(server_replay),
            server_replay_heartbeat,
        }
    }

    fn from_local_backtest(backtest: tqsdk_task::StrategyBacktest) -> Self {
        Self {
            inner: TqInner::LocalBacktest(Box::new(backtest)),
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
        let session = self.require_server_replay_session()?.clone();
        self.abort_server_replay_heartbeat();
        session.terminate().await.map_err(Error::from)
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

    /// Returns a balance-based backtest performance snapshot (local-backtest mode only).
    pub fn backtest_performance_metrics(
        &self,
    ) -> Option<tqsdk_task::StrategyBacktestPerformanceMetrics> {
        self.backtest_summary()
            .map(|summary| summary.performance_metrics())
    }

    /// Returns a typed backtest performance report (local-backtest mode only).
    pub fn backtest_performance_report(
        &self,
        rolling_window_len: usize,
    ) -> Option<tqsdk_task::StrategyBacktestPerformanceReport> {
        self.backtest_summary()
            .map(|summary| summary.performance_report(rolling_window_len))
    }
}

#[cfg(all(feature = "services", feature = "live"))]
impl Drop for Tq {
    fn drop(&mut self) {
        self.abort_server_replay_heartbeat();
    }
}

#[cfg(all(feature = "services", feature = "live"))]
fn spawn_server_replay_heartbeat(
    replay_session: &tqsdk_session::ServerReplaySession,
) -> tokio::task::JoinHandle<()> {
    let replay_session = replay_session.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SERVER_REPLAY_HEARTBEAT_INTERVAL).await;
            let _ = replay_session.heartbeat().await;
        }
    })
}

/// Builder for [`Tq`].
#[derive(Debug)]
pub struct TqBuilder {
    auth: Option<Auth>,
    query_enabled: bool,
    trade_targets: Vec<TradeTarget>,
    market_url: Option<String>,
    replay_url: Option<String>,
    backtest: Option<BacktestConfig>,
    quote_symbols: Vec<String>,
    price_ticks: std::collections::HashMap<String, f64>,
    instrument_specs: Vec<tqsdk_session::InstrumentSpec>,
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
            replay_url: None,
            backtest: None,
            quote_symbols: Vec::new(),
            price_ticks: std::collections::HashMap::new(),
            instrument_specs: Vec::new(),
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

    /// Enter local-backtest mode from owned kline history series under a replay symbol.
    ///
    /// This is useful when underlying contract history should drive a synthetic
    /// symbol such as a continuous-contract code.
    pub fn local_backtest_klines_as(
        self,
        replay_symbol: impl AsRef<str>,
        series: impl IntoIterator<Item = tqsdk_data::KlineDataSeries>,
    ) -> Result<Self> {
        let replay_symbol = replay_symbol.as_ref().to_owned();
        let mut builder = tqsdk_task::StrategyReplaySourceBuilder::new();
        for series in series {
            builder = builder.kline_series_as(series, replay_symbol.as_str(), "history-kline")?;
        }
        Ok(self.local_backtest(builder.build()))
    }

    /// Fetch kline history and enter local-backtest mode from the owned series.
    pub async fn local_backtest_kline_history(
        self,
        data: &tqsdk_data::DataClient,
        request: tqsdk_data::KlineDataSeriesRequest,
    ) -> Result<Self> {
        let series = data.get_kline_data_series(request).await?;
        self.local_backtest_klines([series])
    }

    /// Fetch multiple kline history requests and enter local-backtest mode.
    pub async fn local_backtest_kline_histories(
        self,
        data: &tqsdk_data::DataClient,
        requests: impl IntoIterator<Item = tqsdk_data::KlineDataSeriesRequest>,
    ) -> Result<Self> {
        let mut series = Vec::new();
        for request in requests {
            series.push(data.get_kline_data_series(request).await?);
        }
        self.local_backtest_klines(series)
    }

    /// Fetch multiple kline history requests and replay them under one symbol.
    ///
    /// Use this for explicitly segmented continuous-contract backtests after the
    /// caller has chosen the date/time windows for each underlying segment.
    pub async fn local_backtest_kline_histories_as(
        self,
        data: &tqsdk_data::DataClient,
        replay_symbol: impl AsRef<str>,
        requests: impl IntoIterator<Item = tqsdk_data::KlineDataSeriesRequest>,
    ) -> Result<Self> {
        let mut series = Vec::new();
        for request in requests {
            series.push(data.get_kline_data_series(request).await?);
        }
        self.local_backtest_klines_as(replay_symbol, series)
    }

    /// Fetch kline history and replay it under a caller-provided symbol.
    pub async fn local_backtest_kline_history_as(
        self,
        data: &tqsdk_data::DataClient,
        replay_symbol: impl AsRef<str>,
        request: tqsdk_data::KlineDataSeriesRequest,
    ) -> Result<Self> {
        self.local_backtest_kline_histories_as(data, replay_symbol, [request])
            .await
    }

    /// Fetch one-minute kline history and enter local-backtest mode.
    pub async fn local_backtest_minute_history(
        self,
        data: &tqsdk_data::DataClient,
        symbol: impl Into<String>,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Self> {
        self.local_backtest_kline_history(
            data,
            tqsdk_data::KlineDataSeriesRequest::new(
                symbol,
                Duration::from_secs(60),
                start_datetime_ns,
                end_datetime_ns,
            ),
        )
        .await
    }

    /// Fetch one-minute kline history and replay it under a caller-provided symbol.
    pub async fn local_backtest_minute_history_as(
        self,
        data: &tqsdk_data::DataClient,
        replay_symbol: impl AsRef<str>,
        symbol: impl Into<String>,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Self> {
        self.local_backtest_kline_history_as(
            data,
            replay_symbol,
            tqsdk_data::KlineDataSeriesRequest::new(
                symbol,
                Duration::from_secs(60),
                start_datetime_ns,
                end_datetime_ns,
            ),
        )
        .await
    }

    /// Fetch one-minute kline history for pre-declared quote symbols.
    ///
    /// This is the explicit local-backtest counterpart to Python TqBacktest's
    /// minute-line quote fallback: declare symbols with [`TqBuilder::quote_symbol`],
    /// then call this helper to fetch `[start_datetime_ns, end_datetime_ns)` minute
    /// histories for those symbols without hidden subscriptions.
    pub async fn local_backtest_quote_minute_history(
        self,
        data: &tqsdk_data::DataClient,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Self> {
        let requests = declared_quote_minute_history_requests(
            &self.quote_symbols,
            start_datetime_ns,
            end_datetime_ns,
        )?;
        self.local_backtest_kline_histories(data, requests).await
    }

    /// Fetch one-minute underlying histories for a main continuous contract.
    ///
    /// This queries the historical continuous-contract mapping, fetches the
    /// matching underlying minute series for each contiguous segment, and
    /// replays every segment under `symbol` while preserving quote
    /// `underlying_symbol` metadata. The time range is `[start_datetime_ns,
    /// end_datetime_ns)`.
    ///
    /// Kline quote synthesis still needs a `price_tick`, supplied via
    /// [`TqBuilder::instrument_spec`], [`TqBuilder::instrument_specs`],
    /// [`TqBuilder::price_tick`], or [`TqBuilder::default_price_tick`].
    pub async fn local_backtest_continuous_minute_history(
        self,
        data: &tqsdk_data::DataClient,
        symbol: impl AsRef<str>,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Self> {
        let symbol = symbol.as_ref().to_owned();
        if symbol.is_empty() {
            return Err(data_validation("continuous symbol must not be empty"));
        }
        if end_datetime_ns <= start_datetime_ns {
            return Err(data_validation(
                "end_datetime_ns must be greater than start_datetime_ns",
            ));
        }
        let trading_start = trading_day_from_timestamp_ns(start_datetime_ns)?;
        let end_inclusive_ns = end_datetime_ns.checked_sub(1).ok_or_else(|| {
            data_validation("end_datetime_ns is too small to compute an inclusive end")
        })?;
        let trading_end = trading_day_from_timestamp_ns(end_inclusive_ns)?;
        let trading_days = data.query_trading_days(trading_start, trading_end).await?;
        if trading_days.is_empty() {
            return self
                .local_backtest_klines_as(symbol, Vec::<tqsdk_data::KlineDataSeries>::new());
        }

        let segments = data
            .query_his_cont_underlying_segments(&symbol, trading_days.len(), Some(trading_end))
            .await?;
        let requests = continuous_minute_history_requests(
            &symbol,
            start_datetime_ns,
            end_datetime_ns,
            &segments,
        )?;
        self.local_backtest_kline_histories_as(data, symbol, requests)
            .await
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

    /// Enter local-backtest mode from owned tick history series under a replay symbol.
    ///
    /// This keeps the strategy-facing symbol stable while preserving each
    /// series' original symbol as quote `underlying_symbol` metadata.
    pub fn local_backtest_ticks_as(
        self,
        replay_symbol: impl AsRef<str>,
        series: impl IntoIterator<Item = tqsdk_data::TickDataSeries>,
    ) -> Result<Self> {
        let replay_symbol = replay_symbol.as_ref().to_owned();
        let mut builder = tqsdk_task::StrategyReplaySourceBuilder::new();
        for series in series {
            builder = builder.tick_series_as(series, replay_symbol.as_str(), "history-tick")?;
        }
        Ok(self.local_backtest(builder.build()))
    }

    /// Fetch tick history and enter local-backtest mode from the owned series.
    pub async fn local_backtest_tick_history(
        self,
        data: &tqsdk_data::DataClient,
        request: tqsdk_data::TickDataSeriesRequest,
    ) -> Result<Self> {
        let series = data.get_tick_data_series(request).await?;
        self.local_backtest_ticks([series])
    }

    /// Fetch multiple tick history requests and replay them under one symbol.
    pub async fn local_backtest_tick_histories_as(
        self,
        data: &tqsdk_data::DataClient,
        replay_symbol: impl AsRef<str>,
        requests: impl IntoIterator<Item = tqsdk_data::TickDataSeriesRequest>,
    ) -> Result<Self> {
        let mut series = Vec::new();
        for request in requests {
            series.push(data.get_tick_data_series(request).await?);
        }
        self.local_backtest_ticks_as(replay_symbol, series)
    }

    /// Fetch tick history and replay it under a caller-provided symbol.
    pub async fn local_backtest_tick_history_as(
        self,
        data: &tqsdk_data::DataClient,
        replay_symbol: impl AsRef<str>,
        request: tqsdk_data::TickDataSeriesRequest,
    ) -> Result<Self> {
        self.local_backtest_tick_histories_as(data, replay_symbol, [request])
            .await
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

    #[must_use]
    pub fn instrument_spec(mut self, spec: tqsdk_session::InstrumentSpec) -> Self {
        self.instrument_specs.push(spec);
        self
    }

    #[must_use]
    pub fn instrument_specs(
        mut self,
        specs: impl IntoIterator<Item = tqsdk_session::InstrumentSpec>,
    ) -> Self {
        self.instrument_specs.extend(specs);
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
            replay_url,
            backtest,
            quote_symbols,
            price_ticks,
            instrument_specs,
            default_price_tick,
        } = self;

        match backtest {
            Some(BacktestConfig::Local { replay }) => {
                connect_local_backtest(
                    replay,
                    quote_symbols,
                    price_ticks,
                    instrument_specs,
                    default_price_tick,
                )
                .await
            }
            backtest => {
                connect_wait_facade(
                    auth,
                    query_enabled,
                    trade_targets,
                    market_url,
                    replay_url,
                    backtest,
                )
                .await
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
            #[cfg(all(feature = "services", feature = "live"))]
            Self::ServerReplay { replay_date } => f
                .debug_struct("ServerReplay")
                .field("replay_date", replay_date)
                .finish(),
            Self::Local { .. } => f.debug_struct("Local").finish_non_exhaustive(),
        }
    }
}

async fn connect_local_backtest(
    replay: tqsdk_task::ReplayMarketSource,
    quote_symbols: Vec<String>,
    price_ticks: std::collections::HashMap<String, f64>,
    instrument_specs: Vec<tqsdk_session::InstrumentSpec>,
    default_price_tick: Option<f64>,
) -> Result<Tq> {
    let mut builder = tqsdk_task::StrategyBacktest::builder(replay);
    if let Some(default_price_tick) = default_price_tick {
        builder = builder.default_price_tick(default_price_tick);
    }
    builder = builder.instrument_specs(instrument_specs);
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
    match backtest {
        Some(BacktestConfig::Server { start_ns, end_ns }) => {
            wait_builder = wait_builder.futures_backtest(start_ns, end_ns)?;
        }
        #[cfg(all(feature = "services", feature = "live"))]
        Some(BacktestConfig::ServerReplay { .. }) => {}
        Some(BacktestConfig::Local { .. }) | None => {}
    }
    let api = wait_builder.build().await?;
    #[cfg(all(feature = "services", feature = "live"))]
    if let Some(server_replay) = server_replay_session {
        return Ok(Tq::from_api_with_server_replay(api, server_replay));
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

fn continuous_minute_history_requests(
    symbol: &str,
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    segments: &[tqsdk_data::HistoricalContUnderlyingSegment],
) -> Result<Vec<tqsdk_data::KlineDataSeriesRequest>> {
    if symbol.is_empty() {
        return Err(data_validation("continuous symbol must not be empty"));
    }
    if end_datetime_ns <= start_datetime_ns {
        return Err(data_validation(
            "end_datetime_ns must be greater than start_datetime_ns",
        ));
    }

    let mut requests: Vec<tqsdk_data::KlineDataSeriesRequest> = Vec::new();
    for segment in segments {
        if segment.symbol != symbol {
            return Err(data_validation(format!(
                "continuous segment symbol {} does not match requested {symbol}",
                segment.symbol
            )));
        }
        if segment.underlying.is_empty() {
            continue;
        }

        let segment_start_date = parse_segment_date(&segment.start_date)?;
        let segment_end_date = parse_segment_date(&segment.end_date)?;
        let segment_start_ns = trading_day_start_time_ns(segment_start_date)?;
        let segment_end_ns = trading_day_end_time_ns(segment_end_date)?;
        let request_start = start_datetime_ns.max(segment_start_ns);
        let request_end = end_datetime_ns.min(segment_end_ns);
        if request_start < request_end {
            requests.push(tqsdk_data::KlineDataSeriesRequest::new(
                segment.underlying.clone(),
                Duration::from_secs(60),
                request_start,
                request_end,
            ));
        }
    }

    Ok(requests)
}

fn declared_quote_minute_history_requests(
    symbols: &[String],
    start_datetime_ns: i64,
    end_datetime_ns: i64,
) -> Result<Vec<tqsdk_data::KlineDataSeriesRequest>> {
    if symbols.is_empty() {
        return Err(data_validation(
            "local_backtest_quote_minute_history requires at least one quote_symbol",
        ));
    }
    if end_datetime_ns <= start_datetime_ns {
        return Err(data_validation(
            "end_datetime_ns must be greater than start_datetime_ns",
        ));
    }

    let mut requests: Vec<tqsdk_data::KlineDataSeriesRequest> = Vec::new();
    for symbol in symbols {
        if symbol.is_empty() {
            return Err(data_validation("quote_symbol must not be empty"));
        }
        if requests.iter().any(|request| request.symbol() == symbol) {
            continue;
        }
        requests.push(tqsdk_data::KlineDataSeriesRequest::new(
            symbol.clone(),
            Duration::from_secs(60),
            start_datetime_ns,
            end_datetime_ns,
        ));
    }
    Ok(requests)
}

fn trading_day_from_timestamp_ns(timestamp_ns: i64) -> Result<NaiveDate> {
    let elapsed = timestamp_ns
        .checked_sub(CST_1990_01_01_NS)
        .ok_or_else(|| data_validation("timestamp is before supported trading-day base"))?;
    let mut days = elapsed.div_euclid(NANOS_PER_DAY);
    if elapsed.rem_euclid(NANOS_PER_DAY) >= TRADING_DAY_END_OFFSET_NS {
        days += 1;
    }
    let week_day = days.rem_euclid(7);
    if week_day >= 5 {
        days += 7 - week_day;
    }
    let trading_day_ns = CST_1990_01_01_NS
        .checked_add(days.checked_mul(NANOS_PER_DAY).ok_or_else(|| {
            data_validation("trading-day timestamp overflowed while scaling days")
        })?)
        .ok_or_else(|| data_validation("trading-day timestamp overflowed"))?;
    timestamp_ns_to_cst_date(trading_day_ns)
}

fn trading_day_start_time_ns(trading_day: NaiveDate) -> Result<i64> {
    let mut start_time = cst_midnight_ns(trading_day)?
        .checked_sub(TRADING_DAY_START_OFFSET_NS)
        .ok_or_else(|| data_validation("trading-day start timestamp underflowed"))?;
    let elapsed = start_time
        .checked_sub(CST_1990_01_01_NS)
        .ok_or_else(|| data_validation("trading-day start is before supported base"))?;
    let week_day = elapsed.div_euclid(NANOS_PER_DAY).rem_euclid(7);
    if week_day >= 5 {
        start_time = start_time
            .checked_sub((week_day - 4) * NANOS_PER_DAY)
            .ok_or_else(|| data_validation("weekend-adjusted trading-day start underflowed"))?;
    }
    Ok(start_time)
}

fn trading_day_end_time_ns(trading_day: NaiveDate) -> Result<i64> {
    cst_midnight_ns(trading_day)?
        .checked_add(TRADING_DAY_END_OFFSET_NS)
        .ok_or_else(|| data_validation("trading-day end timestamp overflowed"))
}

fn parse_segment_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|error| data_validation(format!("invalid segment date {value}: {error}")))
}

fn cst_midnight_ns(date: NaiveDate) -> Result<i64> {
    let midnight = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| data_validation("failed to build CST midnight"))?;
    let cst = cst_offset();
    let local = cst
        .from_local_datetime(&midnight)
        .single()
        .ok_or_else(|| data_validation("failed to resolve CST midnight"))?;
    local
        .timestamp()
        .checked_mul(NANOS_PER_SECOND)
        .ok_or_else(|| data_validation("CST midnight timestamp overflowed"))
}

fn timestamp_ns_to_cst_date(timestamp_ns: i64) -> Result<NaiveDate> {
    let seconds = timestamp_ns.div_euclid(NANOS_PER_SECOND);
    let nanos = timestamp_ns.rem_euclid(NANOS_PER_SECOND) as u32;
    let utc = Utc
        .timestamp_opt(seconds, nanos)
        .single()
        .ok_or_else(|| data_validation("failed to resolve timestamp"))?;
    Ok(utc.with_timezone(&cst_offset()).date_naive())
}

fn cst_offset() -> FixedOffset {
    FixedOffset::east_opt(CST_OFFSET_SECONDS).expect("CST offset must be valid")
}

fn data_validation(message: impl Into<String>) -> Error {
    Error::Data(Box::new(tqsdk_data::DataError::Validation(message.into())))
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
        Auth, BacktestConfig, Error, Tq, TqBuilder, continuous_minute_history_requests,
        declared_quote_minute_history_requests, session_builder, trading_day_from_timestamp_ns,
        trading_day_start_time_ns,
    };

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

    #[test]
    fn declared_quote_minute_history_requests_dedupes_declared_symbols() {
        let start = cst_datetime_ns(2026, 5, 18, 9, 0, 0);
        let end = cst_datetime_ns(2026, 5, 18, 10, 0, 0);
        let symbols = vec![
            "SHFE.rb2601".to_string(),
            "SHFE.rb2601".to_string(),
            "DCE.i2601".to_string(),
        ];

        let requests = declared_quote_minute_history_requests(&symbols, start, end).unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].symbol(), "SHFE.rb2601");
        assert_eq!(requests[0].duration(), std::time::Duration::from_secs(60));
        assert_eq!(requests[0].start_datetime_ns(), start);
        assert_eq!(requests[0].end_datetime_ns(), end);
        assert_eq!(requests[1].symbol(), "DCE.i2601");
        assert_eq!(requests[1].duration(), std::time::Duration::from_secs(60));
    }

    #[test]
    fn continuous_minute_history_requests_clip_underlying_segments_to_backtest_window() {
        let start = cst_datetime_ns(2026, 5, 15, 21, 0, 0);
        let end = cst_datetime_ns(2026, 5, 20, 10, 0, 0);
        let segments = [
            tqsdk_data::HistoricalContUnderlyingSegment {
                symbol: "KQ.m@SHFE.rb".to_string(),
                underlying: "SHFE.rb2601".to_string(),
                start_date: "2026-05-15".to_string(),
                end_date: "2026-05-18".to_string(),
                trading_days: 2,
            },
            tqsdk_data::HistoricalContUnderlyingSegment {
                symbol: "KQ.m@SHFE.rb".to_string(),
                underlying: "SHFE.rb2605".to_string(),
                start_date: "2026-05-19".to_string(),
                end_date: "2026-05-20".to_string(),
                trading_days: 2,
            },
        ];

        let requests =
            continuous_minute_history_requests("KQ.m@SHFE.rb", start, end, &segments).unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].symbol(), "SHFE.rb2601");
        assert_eq!(requests[0].duration(), std::time::Duration::from_secs(60));
        assert_eq!(requests[0].start_datetime_ns(), start);
        assert_eq!(
            requests[0].end_datetime_ns(),
            cst_datetime_ns(2026, 5, 18, 18, 0, 0)
        );
        assert_eq!(requests[1].symbol(), "SHFE.rb2605");
        assert_eq!(
            requests[1].start_datetime_ns(),
            cst_datetime_ns(2026, 5, 18, 18, 0, 0)
        );
        assert_eq!(requests[1].end_datetime_ns(), end);
    }

    #[test]
    fn continuous_minute_history_uses_official_style_trading_day_boundaries() {
        let friday_night = cst_datetime_ns(2026, 5, 15, 21, 0, 0);
        let trading_day = trading_day_from_timestamp_ns(friday_night).unwrap();
        assert_eq!(trading_day, NaiveDate::from_ymd_opt(2026, 5, 18).unwrap());
        assert_eq!(
            trading_day_start_time_ns(trading_day).unwrap(),
            cst_datetime_ns(2026, 5, 15, 18, 0, 0)
        );
    }

    #[test]
    fn continuous_minute_history_rejects_mismatched_segments() {
        let start = cst_datetime_ns(2026, 5, 18, 9, 0, 0);
        let end = cst_datetime_ns(2026, 5, 18, 10, 0, 0);
        let segments = [tqsdk_data::HistoricalContUnderlyingSegment {
            symbol: "KQ.m@SHFE.au".to_string(),
            underlying: "SHFE.au2601".to_string(),
            start_date: "2026-05-18".to_string(),
            end_date: "2026-05-18".to_string(),
            trading_days: 1,
        }];

        let result = continuous_minute_history_requests("KQ.m@SHFE.rb", start, end, &segments);

        assert!(matches!(result, Err(Error::Data(_))));
    }

    fn cst_datetime_ns(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> i64 {
        use chrono::TimeZone;

        chrono::FixedOffset::east_opt(8 * 60 * 60)
            .unwrap()
            .with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
            .timestamp()
            * 1_000_000_000
    }
}
