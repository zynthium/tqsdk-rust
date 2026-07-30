//! Remote cache-fill coordination for the backtest-history query path.
//!
//! The module is deliberately crate-private: durable cache APIs stay on their
//! existing facades while the public query client later composes this
//! coordinator with planning and chunk delivery.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use chrono::Utc;
use fs2::FileExt;
use tokio::sync::Notify;
use tqsdk_core::Kline;
use tqsdk_session::{
    ServerBacktestHistoryChart, ServerBacktestHistoryEvent, ServerBacktestHistoryKind,
    ServerBacktestHistoryRequest, ServerBacktestMarketKind,
};

use crate::backtest_tick_cache::{
    BacktestTickCache, BacktestTickFill, backtest_tick_trading_day_for_timestamp_ns,
    backtest_tick_trading_day_range,
};
use crate::minute_kline_cache::{MinuteKlineCache, MinuteKlineCacheSnapshot};
use crate::{DataError, Result};

use super::report::{BacktestHistoryPhase, BacktestHistoryTelemetryEvent};
use super::request::{
    BacktestHistoryClientConfig, BacktestHistoryCredentials, BacktestHistoryPolicy,
    BacktestHistoryRequestId,
};
use super::telemetry::TelemetryHub;

const TICK_WRITE_BUFFER_ROWS: usize = 8_192;
const MINUTE_FILL_MAX_SPAN_NS: i64 = 10_000 * 60_000_000_000;
const CROSS_PROCESS_RECHECK_INTERVAL: Duration = Duration::from_millis(250);
const REMOTE_FILL_RETRY_ATTEMPTS: usize = 3;

static NEXT_CHART_ID: AtomicU64 = AtomicU64::new(1);

type ServerHistorySourceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ServerBacktestHistoryEvent>>> + Send + 'a>>;
type OpenServerHistorySourceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn ServerHistorySource>>> + Send + 'a>>;

/// Cache family whose coverage is being remotely materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FillFamily {
    Tick,
    CanonicalMinute,
}

impl FillFamily {
    fn lock_directory(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::CanonicalMinute => "minute",
        }
    }

    fn server_kind(self) -> ServerBacktestHistoryKind {
        match self {
            Self::Tick => ServerBacktestHistoryKind::Tick,
            Self::CanonicalMinute => ServerBacktestHistoryKind::CanonicalMinute,
        }
    }
}

/// One missing durable-cache slice the planner asks the fill coordinator to
/// establish. The planner has already resolved logical-to-physical ownership.
#[derive(Debug, Clone)]
pub(crate) struct BacktestHistoryFillRequest {
    pub(crate) family: FillFamily,
    pub(crate) cache_symbol: String,
    pub(crate) range: (i64, i64),
    pub(crate) minute_snapshot: Option<MinuteKlineCacheSnapshot>,
    pub(crate) provisional_as_of_ns: Option<i64>,
    pub(crate) request_id: Option<BacktestHistoryRequestId>,
    pub(crate) telemetry_symbol: String,
}

impl BacktestHistoryFillRequest {
    pub(crate) fn tick(
        cache_symbol: impl Into<String>,
        range: (i64, i64),
        provisional_as_of_ns: Option<i64>,
        request_id: Option<BacktestHistoryRequestId>,
        telemetry_symbol: impl Into<String>,
    ) -> Self {
        Self {
            family: FillFamily::Tick,
            cache_symbol: cache_symbol.into(),
            range,
            minute_snapshot: None,
            provisional_as_of_ns,
            request_id,
            telemetry_symbol: telemetry_symbol.into(),
        }
    }

    pub(crate) fn canonical_minute(
        cache_symbol: impl Into<String>,
        range: (i64, i64),
        snapshot: MinuteKlineCacheSnapshot,
        request_id: Option<BacktestHistoryRequestId>,
        telemetry_symbol: impl Into<String>,
    ) -> Self {
        Self {
            family: FillFamily::CanonicalMinute,
            cache_symbol: cache_symbol.into(),
            range,
            minute_snapshot: Some(snapshot),
            provisional_as_of_ns: None,
            request_id,
            telemetry_symbol: telemetry_symbol.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.cache_symbol.trim().is_empty() {
            return Err(DataError::Validation(
                "backtest history fill cache symbol must not be empty".to_string(),
            ));
        }
        if self.range.0 >= self.range.1 {
            return Err(DataError::Validation(format!(
                "backtest history fill range must satisfy start < end: [{}, {})",
                self.range.0, self.range.1
            )));
        }
        match self.family {
            FillFamily::Tick => {
                if let Some(as_of_ns) = self.provisional_as_of_ns {
                    if as_of_ns < self.range.1 {
                        return Err(DataError::Validation(
                            "provisional tick fill range must not extend beyond its as-of timestamp"
                                .to_string(),
                        ));
                    }
                }
            }
            FillFamily::CanonicalMinute => {
                if self.minute_snapshot.is_none() {
                    return Err(DataError::Validation(
                        "canonical-minute fill requires a cache snapshot".to_string(),
                    ));
                }
                if self.provisional_as_of_ns.is_some() {
                    return Err(DataError::Validation(
                        "canonical-minute fill does not support provisional coverage".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn compatibility(&self) -> FillCompatibility {
        FillCompatibility {
            provisional_as_of_ns: self.provisional_as_of_ns,
            minute_snapshot: self.minute_snapshot.clone(),
        }
    }

    fn with_range(&self, range: (i64, i64)) -> Self {
        Self {
            range,
            ..self.clone()
        }
    }

    fn server_request(&self) -> ServerBacktestHistoryRequest {
        let chart_number = NEXT_CHART_ID.fetch_add(1, Ordering::Relaxed);
        ServerBacktestHistoryRequest {
            market_kind: ServerBacktestMarketKind::Futures,
            start_ns: self.range.0,
            end_ns: self.range.1,
            charts: vec![ServerBacktestHistoryChart {
                chart_id: format!(
                    "backtest-history-{}-{chart_number}",
                    self.family.lock_directory()
                ),
                symbol: self.cache_symbol.clone(),
                kind: self.family.server_kind(),
            }],
        }
    }
}

/// Outcome used by the planner to populate request-level coverage reports.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BacktestHistoryFillOutcome {
    pub(crate) remote_filled_ranges: Vec<(i64, i64)>,
    pub(crate) remote_used: bool,
}

/// A minimal source facade over the session-owned history stream. It makes
/// fake scripted streams possible without widening the public data API.
pub(crate) trait ServerHistorySource: Send {
    fn next_event<'a>(&'a mut self) -> ServerHistorySourceFuture<'a>;
}

/// Factory for an official server-backtest history source.
pub(crate) trait ServerHistorySourceFactory: Send + Sync {
    fn open<'a>(
        &'a self,
        credentials: BacktestHistoryCredentials,
        request: ServerBacktestHistoryRequest,
    ) -> OpenServerHistorySourceFuture<'a>;
}

pub(crate) fn default_server_history_source_factory() -> Arc<dyn ServerHistorySourceFactory> {
    #[cfg(all(feature = "live", feature = "services"))]
    {
        Arc::new(SessionServerHistorySourceFactory)
    }
    #[cfg(not(all(feature = "live", feature = "services")))]
    {
        Arc::new(UnavailableServerHistorySourceFactory)
    }
}

#[cfg(all(feature = "live", feature = "services"))]
struct SessionServerHistorySourceFactory;

#[cfg(all(feature = "live", feature = "services"))]
impl ServerHistorySourceFactory for SessionServerHistorySourceFactory {
    fn open<'a>(
        &'a self,
        credentials: BacktestHistoryCredentials,
        request: ServerBacktestHistoryRequest,
    ) -> OpenServerHistorySourceFuture<'a> {
        Box::pin(async move {
            let (user, pass) = credentials.into_parts();
            let session = tqsdk_session::SessionClientBuilder::new(user, pass)
                .futures_backtest_market()
                .build()?;
            let stream = tqsdk_session::ServerBacktestHistoryStream::open(session, request).await?;
            Ok(Box::new(SessionServerHistorySource { stream }) as Box<dyn ServerHistorySource>)
        })
    }
}

#[cfg(all(feature = "live", feature = "services"))]
struct SessionServerHistorySource {
    stream: tqsdk_session::ServerBacktestHistoryStream,
}

#[cfg(all(feature = "live", feature = "services"))]
impl ServerHistorySource for SessionServerHistorySource {
    fn next_event<'a>(&'a mut self) -> ServerHistorySourceFuture<'a> {
        Box::pin(async move { self.stream.next_event(None).await.map_err(Into::into) })
    }
}

#[cfg(not(all(feature = "live", feature = "services")))]
struct UnavailableServerHistorySourceFactory;

#[cfg(not(all(feature = "live", feature = "services")))]
impl ServerHistorySourceFactory for UnavailableServerHistorySourceFactory {
    fn open<'a>(
        &'a self,
        _credentials: BacktestHistoryCredentials,
        _request: ServerBacktestHistoryRequest,
    ) -> OpenServerHistorySourceFuture<'a> {
        Box::pin(async { Err(DataError::RemoteBacktestHistoryFillUnavailable) })
    }
}

/// Cache-miss coordinator shared by all requests originating from one query
/// client. Shared fills are also registered process-wide by cache root/family/
/// symbol so independent clients do not duplicate an overlap.
#[derive(Clone)]
pub(crate) struct RemoteFillCoordinator {
    config: Arc<BacktestHistoryClientConfig>,
    telemetry: TelemetryHub,
}

impl RemoteFillCoordinator {
    pub(crate) fn new(config: Arc<BacktestHistoryClientConfig>, telemetry: TelemetryHub) -> Self {
        Self { config, telemetry }
    }

    pub(crate) async fn ensure_coverage(
        &self,
        request: BacktestHistoryFillRequest,
    ) -> Result<BacktestHistoryFillOutcome> {
        request.validate()?;
        self.emit(
            &request,
            BacktestHistoryPhase::Inspect,
            0,
            "inspecting cache coverage",
        );
        let missing_ranges = self.missing_ranges(&request)?;
        if missing_ranges.is_empty() {
            return Ok(BacktestHistoryFillOutcome::default());
        }
        if self.config.policy == BacktestHistoryPolicy::CacheOnly {
            return Err(DataError::InvalidState(
                "backtest history cache coverage is incomplete",
            ));
        }

        let mut subscriptions = Vec::new();
        for missing_range in missing_ranges.iter().copied() {
            for slice in self.split_fill_range(&request, missing_range)? {
                subscriptions.extend(self.subscribe(slice)?);
            }
        }
        for subscription in subscriptions {
            subscription.wait().await?;
        }

        let still_missing = self.missing_ranges(&request)?;
        if !still_missing.is_empty() {
            return Err(DataError::InvalidState(
                "backtest history remote fill ended without final cache coverage",
            ));
        }
        Ok(BacktestHistoryFillOutcome {
            remote_filled_ranges: missing_ranges,
            remote_used: true,
        })
    }

    fn split_fill_range(
        &self,
        request: &BacktestHistoryFillRequest,
        range: (i64, i64),
    ) -> Result<Vec<BacktestHistoryFillRequest>> {
        if request.family == FillFamily::Tick {
            return Ok(vec![request.with_range(range)]);
        }
        let mut slices = Vec::new();
        let mut start_ns = range.0;
        while start_ns < range.1 {
            let end_ns = start_ns
                .checked_add(MINUTE_FILL_MAX_SPAN_NS)
                .unwrap_or(i64::MAX)
                .min(range.1);
            if end_ns <= start_ns {
                return Err(DataError::InvalidState(
                    "canonical-minute fill slice did not advance",
                ));
            }
            slices.push(request.with_range((start_ns, end_ns)));
            start_ns = end_ns;
        }
        Ok(slices)
    }

    fn subscribe(&self, request: BacktestHistoryFillRequest) -> Result<Vec<FillSubscription>> {
        let key = FillSeriesKey {
            canonical_cache_root: canonical_cache_root(self.config.cache_dir.as_path())?,
            family: request.family,
            cache_symbol: request.cache_symbol.clone(),
        };
        let compatibility = request.compatibility();
        let mut covered = Vec::new();
        let mut subscriptions = Vec::new();
        let mut pending_starts = Vec::new();
        let registry = fill_registry();
        {
            let mut registry = registry
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let active = registry.entry(key.clone()).or_default();
            let mut retained = Vec::new();
            for weak in active.drain(..) {
                let Some(shared) = weak.upgrade() else {
                    continue;
                };
                if shared.compatibility == compatibility {
                    if let Some(overlap) = intersect_ranges(shared.range, request.range) {
                        covered.push(overlap);
                        subscriptions.push(FillSubscription::new(Arc::clone(&shared)));
                    }
                }
                retained.push(Arc::downgrade(&shared));
            }
            *active = retained;

            for range in subtract_ranges(request.range, covered) {
                let shared = Arc::new(SharedFill::new(range, compatibility.clone()));
                subscriptions.push(FillSubscription::new(Arc::clone(&shared)));
                active.push(Arc::downgrade(&shared));
                pending_starts.push((shared, request.with_range(range)));
            }
        }

        for (shared, request) in pending_starts {
            let coordinator = self.clone();
            let key = key.clone();
            tokio::spawn(async move {
                let result = coordinator.run_shared_fill(&request, &shared).await;
                let terminal = result.map_err(|error| error.to_string());
                shared.complete(terminal);
                remove_shared_fill(&key, &shared);
            });
        }
        Ok(subscriptions)
    }

    async fn run_shared_fill(
        &self,
        request: &BacktestHistoryFillRequest,
        shared: &SharedFill,
    ) -> Result<()> {
        loop {
            self.ensure_not_cancelled(shared)?;
            if self.missing_ranges(request)?.is_empty() {
                return Ok(());
            }
            match SeriesFillLease::try_acquire(
                self.config.cache_dir.as_path(),
                request.family,
                request.cache_symbol.as_str(),
            )? {
                Some(_lease) => {
                    // Another process can have finished between the first
                    // inspection and acquiring its series lease.
                    if self.missing_ranges(request)?.is_empty() {
                        return Ok(());
                    }
                    return self.fill_under_lease(request, shared).await;
                }
                None => {
                    self.emit(
                        request,
                        BacktestHistoryPhase::WaitForFill,
                        0,
                        "another process owns this cache-series fill; waiting for coverage",
                    );
                    tokio::time::sleep(CROSS_PROCESS_RECHECK_INTERVAL).await;
                }
            }
        }
    }

    async fn fill_under_lease(
        &self,
        request: &BacktestHistoryFillRequest,
        shared: &SharedFill,
    ) -> Result<()> {
        match request.family {
            FillFamily::Tick => self.fill_ticks(request, shared).await,
            FillFamily::CanonicalMinute => self.fill_minutes(request, shared).await,
        }
    }

    async fn fill_ticks(
        &self,
        request: &BacktestHistoryFillRequest,
        shared: &SharedFill,
    ) -> Result<()> {
        if request.provisional_as_of_ns.is_none() {
            ensure_final_tick_range_is_closed(request.range)?;
        }
        let cache = BacktestTickCache::open(self.config.cache_dir.as_path())?;
        let mut fill = BacktestTickFill::new(
            request.cache_symbol.clone(),
            request.range.0,
            request.range.1,
        );
        let mut pending_rows = Vec::with_capacity(TICK_WRITE_BUFFER_ROWS);
        let mut completed_rows = 0usize;

        self.consume_with_retries(request, shared, |event| match event {
            ServerBacktestHistoryEvent::Ticks { symbol, rows, .. } => {
                if symbol != request.cache_symbol {
                    return Err(DataError::InvalidResponse(format!(
                        "server Tick fill returned unexpected symbol {symbol}"
                    )));
                }
                for row in rows {
                    if fill.push(row.clone())? {
                        pending_rows.push(row);
                        if pending_rows.len() >= TICK_WRITE_BUFFER_ROWS {
                            self.ensure_not_cancelled(shared)?;
                            cache.append_partial_ticks(
                                request.cache_symbol.as_str(),
                                pending_rows.drain(..),
                            )?;
                        }
                        completed_rows = completed_rows.saturating_add(1);
                    }
                }
                self.emit(
                    request,
                    BacktestHistoryPhase::Fill,
                    completed_rows,
                    "writing partial Tick rows",
                );
                Ok(false)
            }
            ServerBacktestHistoryEvent::ChartCompleted { symbol, .. } => {
                if symbol != request.cache_symbol {
                    return Err(DataError::InvalidResponse(format!(
                        "server Tick fill completed unexpected symbol {symbol}"
                    )));
                }
                Ok(false)
            }
            ServerBacktestHistoryEvent::StreamCompleted => Ok(true),
            ServerBacktestHistoryEvent::CanonicalMinutes { .. } => Err(DataError::InvalidResponse(
                "server Tick fill returned canonical-minute rows".to_string(),
            )),
        })
        .await?;

        self.ensure_not_cancelled(shared)?;
        if !pending_rows.is_empty() {
            cache.append_partial_ticks(request.cache_symbol.as_str(), pending_rows.drain(..))?;
        }
        let report = fill.finish_after_idle(0)?;
        if !report.complete {
            return Err(DataError::InvalidResponse(
                report
                    .gap_summary
                    .unwrap_or_else(|| "server Tick fill contained a discontinuity".to_string()),
            ));
        }
        self.ensure_not_cancelled(shared)?;
        match request.provisional_as_of_ns {
            Some(as_of_ns) => cache.mark_provisional_without_inspection(
                request.cache_symbol.as_str(),
                request.range.0,
                request.range.1,
                as_of_ns,
                report.unique_rows,
                report.id_range,
            )?,
            None => cache.mark_complete_without_inspection(
                request.cache_symbol.as_str(),
                request.range.0,
                request.range.1,
                report.unique_rows,
                report.id_range,
            )?,
        }
        self.emit_terminal(
            request,
            completed_rows,
            "Tick fill reached an explicit server terminal and committed coverage",
        );
        Ok(())
    }

    async fn fill_minutes(
        &self,
        request: &BacktestHistoryFillRequest,
        shared: &SharedFill,
    ) -> Result<()> {
        ensure_final_tick_range_is_closed(request.range)?;
        let snapshot = request.minute_snapshot.as_ref().ok_or_else(|| {
            DataError::InvalidState("canonical-minute fill was missing its cache snapshot")
        })?;
        let cache = MinuteKlineCache::open(self.config.cache_dir.as_path())?;
        let mut rows_by_datetime = BTreeMap::<i64, Kline>::new();

        self.consume_with_retries(request, shared, |event| match event {
            ServerBacktestHistoryEvent::CanonicalMinutes { symbol, rows, .. } => {
                if symbol != request.cache_symbol {
                    return Err(DataError::InvalidResponse(format!(
                        "server canonical-minute fill returned unexpected symbol {symbol}"
                    )));
                }
                for row in rows {
                    if row.datetime >= request.range.0 && row.datetime < request.range.1 {
                        rows_by_datetime.insert(row.datetime, row);
                    }
                }
                self.emit(
                    request,
                    BacktestHistoryPhase::Fill,
                    rows_by_datetime.len(),
                    "buffering canonical-minute rows until the server terminal",
                );
                Ok(false)
            }
            ServerBacktestHistoryEvent::ChartCompleted { symbol, .. } => {
                if symbol != request.cache_symbol {
                    return Err(DataError::InvalidResponse(format!(
                        "server canonical-minute fill completed unexpected symbol {symbol}"
                    )));
                }
                Ok(false)
            }
            ServerBacktestHistoryEvent::StreamCompleted => Ok(true),
            ServerBacktestHistoryEvent::Ticks { .. } => Err(DataError::InvalidResponse(
                "server canonical-minute fill returned Tick rows".to_string(),
            )),
        })
        .await?;

        self.ensure_not_cancelled(shared)?;
        let rows = rows_by_datetime.into_values().collect::<Vec<_>>();
        cache.store_final_range(
            request.cache_symbol.as_str(),
            request.range.0,
            request.range.1,
            snapshot,
            rows.as_slice(),
        )?;
        self.emit_terminal(
            request,
            rows.len(),
            "canonical-minute fill reached an explicit server terminal and committed coverage",
        );
        Ok(())
    }

    async fn consume_with_retries<F>(
        &self,
        request: &BacktestHistoryFillRequest,
        shared: &SharedFill,
        mut consume: F,
    ) -> Result<()>
    where
        F: FnMut(ServerBacktestHistoryEvent) -> Result<bool>,
    {
        let provider = self.config.auth_provider.as_ref().ok_or_else(|| {
            DataError::Validation(
                "remote backtest history fill requires auth_env() or auth_provider()".to_string(),
            )
        })?;
        let mut last_error = None;
        for attempt in 1..=REMOTE_FILL_RETRY_ATTEMPTS {
            self.ensure_not_cancelled(shared)?;
            if attempt > 1 {
                self.emit(
                    request,
                    BacktestHistoryPhase::Retry,
                    0,
                    format!("retrying official server-backtest source (attempt {attempt})"),
                );
            }
            let credentials = provider.load().await?;
            let mut source = match self
                .config
                .source_factory
                .open(credentials, request.server_request())
                .await
            {
                Ok(source) => source,
                Err(error) => {
                    if attempt < REMOTE_FILL_RETRY_ATTEMPTS && is_retryable(&error) {
                        last_error = Some(error);
                        tokio::time::sleep(retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(error);
                }
            };
            loop {
                self.ensure_not_cancelled(shared)?;
                let cancellation = shared.state.terminal.notified();
                let source_event = source.next_event();
                tokio::pin!(cancellation);
                tokio::pin!(source_event);
                let next_event = tokio::select! {
                    result = &mut source_event => result,
                    _ = &mut cancellation => {
                        self.ensure_not_cancelled(shared)?;
                        continue;
                    }
                };
                match next_event {
                    Ok(Some(event)) => {
                        if consume(event)? {
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        let error = DataError::InvalidResponse(
                            "server backtest history source ended without StreamCompleted"
                                .to_string(),
                        );
                        if attempt < REMOTE_FILL_RETRY_ATTEMPTS && is_retryable(&error) {
                            last_error = Some(error);
                            break;
                        }
                        return Err(error);
                    }
                    Err(error) => {
                        if attempt < REMOTE_FILL_RETRY_ATTEMPTS && is_retryable(&error) {
                            last_error = Some(error);
                            break;
                        }
                        return Err(error);
                    }
                }
            }
            tokio::time::sleep(retry_delay(attempt)).await;
        }
        Err(last_error.unwrap_or_else(|| {
            DataError::InvalidState("server backtest history source exhausted its retry budget")
        }))
    }

    fn missing_ranges(&self, request: &BacktestHistoryFillRequest) -> Result<Vec<(i64, i64)>> {
        match request.family {
            FillFamily::Tick => Ok(BacktestTickCache::open_read_only(
                self.config.cache_dir.as_path(),
            )
            .coverage(
                request.cache_symbol.as_str(),
                request.range.0,
                request.range.1,
            )?
            .missing_ranges),
            FillFamily::CanonicalMinute => {
                let snapshot = request.minute_snapshot.as_ref().ok_or_else(|| {
                    DataError::InvalidState("canonical-minute fill was missing its cache snapshot")
                })?;
                Ok(
                    MinuteKlineCache::open_read_only(self.config.cache_dir.as_path())
                        .coverage(
                            request.cache_symbol.as_str(),
                            request.range.0,
                            request.range.1,
                            snapshot,
                        )?
                        .missing_ranges,
                )
            }
        }
    }

    fn ensure_not_cancelled(&self, shared: &SharedFill) -> Result<()> {
        if shared.is_cancelled() {
            return Err(DataError::InvalidState(
                "backtest history shared fill was cancelled before final coverage was committed",
            ));
        }
        Ok(())
    }

    fn emit(
        &self,
        request: &BacktestHistoryFillRequest,
        phase: BacktestHistoryPhase,
        completed_rows: usize,
        message: impl Into<String>,
    ) {
        self.telemetry.emit(BacktestHistoryTelemetryEvent {
            request_id: request.request_id,
            symbol: request.telemetry_symbol.clone(),
            phase,
            completed_rows,
            message: message.into(),
        });
    }

    fn emit_terminal(
        &self,
        request: &BacktestHistoryFillRequest,
        completed_rows: usize,
        message: impl Into<String>,
    ) {
        self.telemetry.emit_terminal(BacktestHistoryTelemetryEvent {
            request_id: request.request_id,
            symbol: request.telemetry_symbol.clone(),
            phase: BacktestHistoryPhase::Fill,
            completed_rows,
            message: message.into(),
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FillCompatibility {
    provisional_as_of_ns: Option<i64>,
    minute_snapshot: Option<MinuteKlineCacheSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FillSeriesKey {
    canonical_cache_root: PathBuf,
    family: FillFamily,
    cache_symbol: String,
}

static FILL_REGISTRY: OnceLock<Mutex<BTreeMap<FillSeriesKey, Vec<Weak<SharedFill>>>>> =
    OnceLock::new();

fn fill_registry() -> &'static Mutex<BTreeMap<FillSeriesKey, Vec<Weak<SharedFill>>>> {
    FILL_REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

struct SharedFill {
    range: (i64, i64),
    compatibility: FillCompatibility,
    state: SharedFillState,
    result: Mutex<Option<std::result::Result<(), String>>>,
}

struct SharedFillState {
    consumers: AtomicUsize,
    cancel_requested: AtomicBool,
    terminal: Notify,
}

impl SharedFill {
    fn new(range: (i64, i64), compatibility: FillCompatibility) -> Self {
        Self {
            range,
            compatibility,
            state: SharedFillState {
                consumers: AtomicUsize::new(0),
                cancel_requested: AtomicBool::new(false),
                terminal: Notify::new(),
            },
            result: Mutex::new(None),
        }
    }

    fn subscribe(self: &Arc<Self>) -> FillConsumerGuard {
        self.state.consumers.fetch_add(1, Ordering::AcqRel);
        FillConsumerGuard {
            shared: Arc::clone(self),
        }
    }

    fn is_cancelled(&self) -> bool {
        self.state.cancel_requested.load(Ordering::Acquire)
    }

    fn complete(&self, result: std::result::Result<(), String>) {
        let mut stored = self
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if stored.is_none() {
            *stored = Some(result);
        }
        drop(stored);
        self.state.terminal.notify_waiters();
    }

    async fn wait(&self) -> Result<()> {
        loop {
            let notified = self.state.terminal.notified();
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .cloned()
            {
                return result.map_err(DataError::InvalidResponse);
            }
            notified.await;
        }
    }
}

struct FillSubscription {
    shared: Arc<SharedFill>,
    _consumer: FillConsumerGuard,
}

impl FillSubscription {
    fn new(shared: Arc<SharedFill>) -> Self {
        let consumer = shared.subscribe();
        Self {
            shared,
            _consumer: consumer,
        }
    }

    async fn wait(self) -> Result<()> {
        self.shared.wait().await
    }
}

struct FillConsumerGuard {
    shared: Arc<SharedFill>,
}

impl Drop for FillConsumerGuard {
    fn drop(&mut self) {
        let previous = self.shared.state.consumers.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "fill consumer count must not underflow");
        if previous == 1 {
            self.shared
                .state
                .cancel_requested
                .store(true, Ordering::Release);
            self.shared.state.terminal.notify_waiters();
        }
    }
}

fn remove_shared_fill(key: &FillSeriesKey, shared: &Arc<SharedFill>) {
    let mut registry = fill_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(active) = registry.get_mut(key) else {
        return;
    };
    active.retain(|weak| {
        weak.upgrade()
            .is_some_and(|candidate| !Arc::ptr_eq(&candidate, shared))
    });
    if active.is_empty() {
        registry.remove(key);
    }
}

/// Cross-process advisory lock held per cache root/family/symbol. The lock
/// file is coordination-only and deliberately persists after release.
struct SeriesFillLease {
    file: File,
}

impl SeriesFillLease {
    fn try_acquire(root: &Path, family: FillFamily, symbol: &str) -> Result<Option<Self>> {
        let directory = root
            .join(".backtest-history-fill-locks")
            .join(family.lock_directory());
        fs::create_dir_all(directory.as_path())?;
        let path = directory.join(format!("{}.lock", escape_path_component(symbol)));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { file })),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for SeriesFillLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn canonical_cache_root(path: &Path) -> Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if path.is_absolute() {
                Ok(path.to_path_buf())
            } else {
                Ok(std::env::current_dir()?.join(path))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn ensure_final_tick_range_is_closed(range: (i64, i64)) -> Result<()> {
    let now_ns = Utc::now().timestamp_nanos_opt().ok_or_else(|| {
        DataError::InvalidResponse(
            "current timestamp overflowed while checking fill finality".to_string(),
        )
    })?;
    let current_day = backtest_tick_trading_day_for_timestamp_ns(now_ns)?;
    let current_range = backtest_tick_trading_day_range(current_day)?;
    if range.1 > current_range.start_ns {
        return Err(DataError::InvalidState(
            "current or future trading-day backtest history cannot be committed as final",
        ));
    }
    Ok(())
}

fn intersect_ranges(left: (i64, i64), right: (i64, i64)) -> Option<(i64, i64)> {
    let start = left.0.max(right.0);
    let end = left.1.min(right.1);
    (start < end).then_some((start, end))
}

fn subtract_ranges(request: (i64, i64), covered: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    let mut covered = covered
        .into_iter()
        .filter_map(|range| intersect_ranges(request, range))
        .collect::<Vec<_>>();
    covered.sort_unstable();
    let mut cursor = request.0;
    let mut missing = Vec::new();
    for (start, end) in covered {
        if start > cursor {
            missing.push((cursor, start));
        }
        cursor = cursor.max(end);
    }
    if cursor < request.1 {
        missing.push((cursor, request.1));
    }
    missing
}

fn escape_path_component(value: &str) -> String {
    let mut escaped = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_') {
            escaped.push(byte as char);
        } else {
            escaped.push('%');
            escaped.push_str(&format!("{byte:02X}"));
        }
    }
    escaped
}

fn is_retryable(error: &DataError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    [
        "connection",
        "connect",
        "timeout",
        "timed out",
        "token",
        "endpoint",
        "temporar",
        "transport",
        "dns",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(
        u64::try_from(attempt)
            .unwrap_or(u64::MAX)
            .saturating_mul(250)
            .min(2_000),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::{Duration as ChronoDuration, Utc};
    use tqsdk_core::Tick;

    use super::*;
    use crate::backtest_history::request::BacktestHistoryAuthProvider;
    use crate::backtest_history::telemetry::TelemetryHub;

    #[tokio::test]
    async fn complete_coverage_does_not_open_a_source_or_load_authentication() {
        let root = temporary_root("fill-complete-coverage");
        let range = closed_range();
        let cache = BacktestTickCache::open(&root).unwrap();
        cache
            .store_ticks(
                "SHFE.au2608",
                range.0,
                range.1,
                [tick(1, range.0.saturating_add(1))],
            )
            .unwrap();
        let opens = Arc::new(AtomicUsize::new(0));
        let auth_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = coordinator(
            root,
            Arc::new(ScriptedFactory::new(
                Arc::clone(&opens),
                Vec::new(),
                Duration::ZERO,
            )),
            Arc::new(CountingAuth::new(Arc::clone(&auth_calls))),
        );

        let outcome = coordinator
            .ensure_coverage(BacktestHistoryFillRequest::tick(
                "SHFE.au2608",
                range,
                None,
                Some(1),
                "SHFE.au2608",
            ))
            .await
            .unwrap();

        assert!(!outcome.remote_used);
        assert_eq!(opens.load(Ordering::SeqCst), 0);
        assert_eq!(auth_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn overlapping_tick_requests_share_one_remote_fill() {
        let root = temporary_root("fill-single-flight");
        let range = closed_range();
        let opens = Arc::new(AtomicUsize::new(0));
        let auth_calls = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(ScriptedFactory::new(
            Arc::clone(&opens),
            vec![
                ServerBacktestHistoryEvent::Ticks {
                    chart_id: "tick".to_string(),
                    symbol: "SHFE.au2608".to_string(),
                    rows: vec![tick(1, range.0.saturating_add(1))],
                },
                ServerBacktestHistoryEvent::StreamCompleted,
            ],
            Duration::from_millis(50),
        ));
        let coordinator = coordinator(
            root.clone(),
            factory,
            Arc::new(CountingAuth::new(Arc::clone(&auth_calls))),
        );
        let request =
            BacktestHistoryFillRequest::tick("SHFE.au2608", range, None, Some(1), "SHFE.au2608");

        let (first, second) = tokio::join!(
            coordinator.ensure_coverage(request.clone()),
            coordinator.ensure_coverage(request)
        );
        first.unwrap();
        second.unwrap();

        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(auth_calls.load(Ordering::SeqCst), 1);
        assert!(
            BacktestTickCache::open_read_only(root)
                .coverage("SHFE.au2608", range.0, range.1)
                .unwrap()
                .is_complete()
        );
    }

    #[tokio::test]
    async fn series_lease_waits_for_another_process_then_rechecks_coverage() {
        let root = temporary_root("fill-series-lease");
        let range = closed_range();
        let lease = SeriesFillLease::try_acquire(&root, FillFamily::Tick, "SHFE.au2608")
            .unwrap()
            .expect("first process should acquire the series lease");
        let opens = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(ScriptedFactory::new(
            Arc::clone(&opens),
            vec![ServerBacktestHistoryEvent::StreamCompleted],
            Duration::ZERO,
        ));
        let coordinator = coordinator(
            root.clone(),
            factory,
            Arc::new(CountingAuth::new(Arc::new(AtomicUsize::new(0)))),
        );
        let task = tokio::spawn({
            let coordinator = coordinator.clone();
            async move {
                coordinator
                    .ensure_coverage(BacktestHistoryFillRequest::tick(
                        "SHFE.au2608",
                        range,
                        None,
                        Some(1),
                        "SHFE.au2608",
                    ))
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(opens.load(Ordering::SeqCst), 0);
        drop(lease);
        task.await.unwrap().unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_keeps_partial_ticks_without_final_coverage() {
        let root = temporary_root("fill-cancellation");
        let range = closed_range();
        let opens = Arc::new(AtomicUsize::new(0));
        let coordinator = coordinator(
            root.clone(),
            Arc::new(NeverFactory {
                opens: Arc::clone(&opens),
            }),
            Arc::new(CountingAuth::new(Arc::new(AtomicUsize::new(0)))),
        );
        let task = tokio::spawn({
            let coordinator = coordinator.clone();
            async move {
                coordinator
                    .ensure_coverage(BacktestHistoryFillRequest::tick(
                        "SHFE.au2608",
                        range,
                        None,
                        Some(1),
                        "SHFE.au2608",
                    ))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while opens.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source should have opened");
        task.abort();
        let _ = task.await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(
            !BacktestTickCache::open_read_only(root)
                .coverage("SHFE.au2608", range.0, range.1)
                .unwrap()
                .is_complete()
        );
    }

    #[tokio::test]
    async fn minute_rows_become_final_only_after_stream_terminal() {
        let root = temporary_root("minute-terminal");
        let range = closed_range();
        let snapshot = MinuteKlineCacheSnapshot::cst_v1();
        let opens = Arc::new(AtomicUsize::new(0));
        let coordinator = coordinator(
            root.clone(),
            Arc::new(ScriptedFactory::new(
                Arc::clone(&opens),
                vec![
                    ServerBacktestHistoryEvent::CanonicalMinutes {
                        chart_id: "minute".to_string(),
                        symbol: "KQ.i@SHFE.au".to_string(),
                        rows: vec![kline(1, range.0.saturating_add(60_000_000_000))],
                    },
                    ServerBacktestHistoryEvent::StreamCompleted,
                ],
                Duration::ZERO,
            )),
            Arc::new(CountingAuth::new(Arc::new(AtomicUsize::new(0)))),
        );

        coordinator
            .ensure_coverage(BacktestHistoryFillRequest::canonical_minute(
                "KQ.i@SHFE.au",
                range,
                snapshot.clone(),
                Some(1),
                "KQ.i@SHFE.au",
            ))
            .await
            .unwrap();

        assert!(
            MinuteKlineCache::open_read_only(root)
                .coverage("KQ.i@SHFE.au", range.0, range.1, &snapshot)
                .unwrap()
                .is_complete()
        );
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    fn coordinator(
        root: PathBuf,
        source_factory: Arc<dyn ServerHistorySourceFactory>,
        auth_provider: Arc<dyn BacktestHistoryAuthProvider>,
    ) -> RemoteFillCoordinator {
        RemoteFillCoordinator::new(
            Arc::new(BacktestHistoryClientConfig {
                cache_dir: root,
                policy: BacktestHistoryPolicy::RemoteOnMiss,
                logical_concurrency: 1,
                blocking_workers: 1,
                per_symbol_buffer_bytes: 1024,
                collect_limit_bytes: 1024,
                auth_provider: Some(auth_provider),
                source_factory,
            }),
            TelemetryHub::new(),
        )
    }

    fn closed_range() -> (i64, i64) {
        let timestamp = (Utc::now() - ChronoDuration::days(10))
            .timestamp_nanos_opt()
            .expect("past timestamp should fit i64 nanoseconds");
        let day = backtest_tick_trading_day_for_timestamp_ns(timestamp).unwrap();
        let range = backtest_tick_trading_day_range(day).unwrap();
        (range.start_ns, range.end_ns)
    }

    fn temporary_root(name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .expect("test timestamp should fit i64 nanoseconds");
        let root = std::env::temp_dir().join(format!("tqsdk-backtest-fill-{name}-{suffix}"));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn tick(id: i64, datetime: i64) -> Tick {
        Tick {
            id,
            datetime,
            ..Tick::default()
        }
    }

    fn kline(id: i64, datetime: i64) -> Kline {
        Kline {
            id,
            datetime,
            ..Kline::default()
        }
    }

    struct CountingAuth {
        calls: Arc<AtomicUsize>,
    }

    impl CountingAuth {
        fn new(calls: Arc<AtomicUsize>) -> Self {
            Self { calls }
        }
    }

    impl BacktestHistoryAuthProvider for CountingAuth {
        fn load<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Result<BacktestHistoryCredentials>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(BacktestHistoryCredentials::new("test-user", "test-pass"))
            })
        }
    }

    struct ScriptedFactory {
        opens: Arc<AtomicUsize>,
        events: Vec<ServerBacktestHistoryEvent>,
        first_event_delay: Duration,
    }

    impl ScriptedFactory {
        fn new(
            opens: Arc<AtomicUsize>,
            events: Vec<ServerBacktestHistoryEvent>,
            first_event_delay: Duration,
        ) -> Self {
            Self {
                opens,
                events,
                first_event_delay,
            }
        }
    }

    impl ServerHistorySourceFactory for ScriptedFactory {
        fn open<'a>(
            &'a self,
            _credentials: BacktestHistoryCredentials,
            _request: ServerBacktestHistoryRequest,
        ) -> OpenServerHistorySourceFuture<'a> {
            Box::pin(async move {
                self.opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(ScriptedSource {
                    events: self.events.clone().into(),
                    first_event_delay: self.first_event_delay,
                    delayed: false,
                }) as Box<dyn ServerHistorySource>)
            })
        }
    }

    struct ScriptedSource {
        events: VecDeque<ServerBacktestHistoryEvent>,
        first_event_delay: Duration,
        delayed: bool,
    }

    impl ServerHistorySource for ScriptedSource {
        fn next_event<'a>(&'a mut self) -> ServerHistorySourceFuture<'a> {
            Box::pin(async move {
                if !self.delayed {
                    self.delayed = true;
                    tokio::time::sleep(self.first_event_delay).await;
                }
                Ok(self.events.pop_front())
            })
        }
    }

    struct NeverFactory {
        opens: Arc<AtomicUsize>,
    }

    impl ServerHistorySourceFactory for NeverFactory {
        fn open<'a>(
            &'a self,
            _credentials: BacktestHistoryCredentials,
            _request: ServerBacktestHistoryRequest,
        ) -> OpenServerHistorySourceFuture<'a> {
            Box::pin(async move {
                self.opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(NeverSource) as Box<dyn ServerHistorySource>)
            })
        }
    }

    struct NeverSource;

    impl ServerHistorySource for NeverSource {
        fn next_event<'a>(&'a mut self) -> ServerHistorySourceFuture<'a> {
            Box::pin(std::future::pending())
        }
    }
}
