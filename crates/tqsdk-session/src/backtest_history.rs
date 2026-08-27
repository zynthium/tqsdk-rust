#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tqsdk_core::{Chart, Kline, MarketChartCommand, RuntimeReader, Symbol, Tick, UpdateCursor};

use crate::{MarketChartLease, Result, SessionClient, SessionFacadeError};

/// The only Kline duration persisted by the backtest cache hierarchy.
pub const SERVER_BACKTEST_CANONICAL_MINUTE_NS: i64 = 60_000_000_000;
/// Native daily server-backtest chart duration.
pub const SERVER_BACKTEST_CANONICAL_DAILY_NS: i64 = 86_400_000_000_000;
const SERVER_BACKTEST_HISTORY_PAGE_WIDTH: usize = 10_000;

/// Market family selected for an official server-backtest history stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerBacktestMarketKind {
    Futures,
    Stock,
}

/// Low-level server-backtest history source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerBacktestHistoryKind {
    Tick,
    CanonicalMinute,
    CanonicalDaily,
}

/// One logical chart read by a [`ServerBacktestHistoryStream`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerBacktestHistoryChart {
    pub chart_id: String,
    pub symbol: String,
    pub kind: ServerBacktestHistoryKind,
}

/// A bounded server-backtest range and its independent charts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerBacktestHistoryRequest {
    pub market_kind: ServerBacktestMarketKind,
    pub start_ns: i64,
    pub end_ns: i64,
    pub charts: Vec<ServerBacktestHistoryChart>,
}

/// One event emitted by [`ServerBacktestHistoryStream`].
#[derive(Debug, Clone)]
pub enum ServerBacktestHistoryEvent {
    Ticks {
        chart_id: String,
        symbol: String,
        rows: Vec<Tick>,
    },
    CanonicalMinutes {
        chart_id: String,
        symbol: String,
        rows: Vec<Kline>,
    },
    CanonicalDaily {
        chart_id: String,
        symbol: String,
        rows: Vec<Kline>,
    },
    ChartCompleted {
        chart_id: String,
        symbol: String,
    },
    StreamCompleted,
}

/// Session-owned official server-backtest Tick and canonical-minute stream.
///
/// This type owns pagination and chart leases, but not cache coverage, cache
/// files, aggregation, or request policy. Those belong to `tqsdk-data`.
pub struct ServerBacktestHistoryStream {
    session: SessionClient,
    reader: RuntimeReader,
    cursor: UpdateCursor,
    request: ServerBacktestHistoryRequest,
    charts: Vec<HistoryChartState>,
    next_chart_index: usize,
    pending_events: VecDeque<ServerBacktestHistoryEvent>,
    stream_completed: bool,
    cleanup: StreamCleanup,
}

#[derive(Debug)]
struct HistoryChartState {
    chart: ServerBacktestHistoryChart,
    page_number: usize,
    current_page_chart_id: String,
    next_left_kline_id: Option<i64>,
    last_emitted_id: Option<i64>,
    last_page_right_id: Option<i64>,
    completed: bool,
}

impl HistoryChartState {
    fn new(chart: ServerBacktestHistoryChart) -> Self {
        Self {
            current_page_chart_id: chart.chart_id.clone(),
            chart,
            page_number: 0,
            next_left_kline_id: None,
            last_emitted_id: None,
            last_page_right_id: None,
            completed: false,
        }
    }

    fn duration_ns(&self) -> i64 {
        match self.chart.kind {
            ServerBacktestHistoryKind::Tick => 0,
            ServerBacktestHistoryKind::CanonicalMinute => SERVER_BACKTEST_CANONICAL_MINUTE_NS,
            ServerBacktestHistoryKind::CanonicalDaily => SERVER_BACKTEST_CANONICAL_DAILY_NS,
        }
    }

    fn page_chart_id(&self) -> String {
        if self.page_number == 0 {
            self.chart.chart_id.clone()
        } else {
            format!(
                "{}--server-history-page-{}",
                self.chart.chart_id, self.page_number
            )
        }
    }
}

enum ReadyPage {
    Ticks {
        right_id: i64,
        terminal: bool,
        rows: Vec<Tick>,
    },
    CanonicalMinutes {
        right_id: i64,
        terminal: bool,
        rows: Vec<Kline>,
    },
    CanonicalDaily {
        right_id: i64,
        terminal: bool,
        rows: Vec<Kline>,
    },
}

impl ReadyPage {
    fn right_id(&self) -> i64 {
        match self {
            Self::Ticks { right_id, .. }
            | Self::CanonicalMinutes { right_id, .. }
            | Self::CanonicalDaily { right_id, .. } => *right_id,
        }
    }

    fn terminal(&self) -> bool {
        match self {
            Self::Ticks { terminal, .. }
            | Self::CanonicalMinutes { terminal, .. }
            | Self::CanonicalDaily { terminal, .. } => *terminal,
        }
    }
}

#[derive(Clone)]
struct StreamCleanup {
    cancellation_requested: Arc<AtomicBool>,
    cancellation: Arc<Notify>,
    leases: Arc<Mutex<BTreeMap<String, MarketChartLease>>>,
    finished: Arc<AtomicBool>,
    result: Arc<Mutex<Option<Result<()>>>>,
    finished_notification: Arc<Notify>,
}

impl StreamCleanup {
    fn new() -> Self {
        Self {
            cancellation_requested: Arc::new(AtomicBool::new(false)),
            cancellation: Arc::new(Notify::new()),
            leases: Arc::new(Mutex::new(BTreeMap::new())),
            finished: Arc::new(AtomicBool::new(false)),
            result: Arc::new(Mutex::new(None)),
            finished_notification: Arc::new(Notify::new()),
        }
    }

    fn start_coordinator(&self) {
        let cancellation = Arc::clone(&self.cancellation);
        let leases = Arc::clone(&self.leases);
        let finished = Arc::clone(&self.finished);
        let result = Arc::clone(&self.result);
        let finished_notification = Arc::clone(&self.finished_notification);
        let _cleanup_task = tokio::spawn(async move {
            cancellation.notified().await;
            *result.lock().await = Some(close_leases(leases).await);
            finished.store(true, Ordering::Release);
            finished_notification.notify_waiters();
        });
    }

    async fn insert(&self, chart_id: String, lease: MarketChartLease) {
        self.leases.lock().await.insert(chart_id, lease);
    }

    async fn close(&self, chart_id: &str) -> Result<()> {
        let lease = self.leases.lock().await.remove(chart_id);
        if let Some(lease) = lease {
            lease.close().await?;
        }
        Ok(())
    }

    fn request_cancellation(&self) {
        if !self.cancellation_requested.swap(true, Ordering::AcqRel) {
            self.cancellation.notify_one();
        }
    }

    async fn close_and_wait(&self) -> Result<()> {
        self.request_cancellation();
        loop {
            let finished = self.finished_notification.notified();
            if self.finished.load(Ordering::Acquire) {
                break;
            }
            finished.await;
        }
        self.result.lock().await.take().unwrap_or_else(|| {
            Err(validation_error(
                "server-backtest history stream cleanup finished without a result",
            ))
        })
    }
}

async fn close_leases(leases: Arc<Mutex<BTreeMap<String, MarketChartLease>>>) -> Result<()> {
    let leases = std::mem::take(&mut *leases.lock().await);
    let mut first_error = None;
    for lease in leases.into_values() {
        if let Err(error) = lease.close().await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(())
}

impl ServerBacktestHistoryStream {
    /// Opens chart leases for an official server-backtest history request.
    pub async fn open(
        session: SessionClient,
        request: ServerBacktestHistoryRequest,
    ) -> Result<Self> {
        validate_request(&session, &request)?;
        let reader = session.reader_clone();
        let cleanup = StreamCleanup::new();
        cleanup.start_coordinator();
        let mut stream = Self {
            session,
            cursor: reader.cursor(),
            reader,
            charts: request
                .charts
                .iter()
                .cloned()
                .map(HistoryChartState::new)
                .collect(),
            request,
            next_chart_index: 0,
            pending_events: VecDeque::new(),
            stream_completed: false,
            cleanup,
        };

        for chart_index in 0..stream.charts.len() {
            stream.open_page(chart_index).await?;
        }
        stream.cursor = stream.reader.cursor();
        Ok(stream)
    }

    /// Releases every chart lease and waits until cleanup has completed.
    ///
    /// Call this before reusing the underlying shared session for another
    /// server-backtest stream. Dropping the stream still requests best-effort
    /// asynchronous cleanup when an explicit close is not possible.
    pub async fn close(self) -> Result<()> {
        self.cleanup.close_and_wait().await
    }

    /// Advances the shared session until one history event is available.
    ///
    /// Returns `Ok(None)` only when `deadline` expires or after the terminal
    /// [`ServerBacktestHistoryEvent::StreamCompleted`] event has been returned.
    pub async fn next_event(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<Option<ServerBacktestHistoryEvent>> {
        loop {
            if let Some(event) = self.poll_ready_events().await? {
                return Ok(Some(event));
            }

            if self.stream_completed {
                return Ok(None);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(None);
            }

            if self.reader.next(&mut self.cursor).is_some() {
                continue;
            }

            if self.session.progress_once(deadline).await?.is_progress() {
                continue;
            }

            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(None);
            }

            // This stream is the sole driver for its SessionClient. Waiting
            // for a RuntimeReader notification here can therefore deadlock:
            // the notification is produced only by a later progress_once()
            // call, but no call is made while this task is waiting. Yield so
            // other Tokio work may run, then drive the session again.
            tokio::task::yield_now().await;
        }
    }

    async fn poll_ready_events(&mut self) -> Result<Option<ServerBacktestHistoryEvent>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }

        let chart_count = self.charts.len();
        for offset in 0..chart_count {
            let chart_index = (self.next_chart_index + offset) % chart_count;
            if self.charts[chart_index].completed {
                continue;
            }
            let Some(page) = self.read_ready_page(chart_index)? else {
                continue;
            };
            self.next_chart_index = (chart_index + 1) % chart_count;
            let event = self.consume_ready_page(chart_index, page).await?;
            if let Some(event) = event {
                return Ok(Some(event));
            }
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(Some(event));
            }
        }

        if self.charts.iter().all(|chart| chart.completed) {
            self.stream_completed = true;
            self.cleanup.request_cancellation();
            return Ok(Some(ServerBacktestHistoryEvent::StreamCompleted));
        }
        Ok(None)
    }

    async fn consume_ready_page(
        &mut self,
        chart_index: usize,
        page: ReadyPage,
    ) -> Result<Option<ServerBacktestHistoryEvent>> {
        let old_last_page_right_id = self.charts[chart_index].last_page_right_id;
        let right_id = page.right_id();
        let page_terminal = page.terminal();
        let mut reached_requested_end = false;
        let event = match page {
            ReadyPage::Ticks {
                terminal: _, rows, ..
            } => {
                let (chart_id, symbol, rows) =
                    self.take_new_ticks(chart_index, rows, &mut reached_requested_end);
                (!rows.is_empty()).then_some(ServerBacktestHistoryEvent::Ticks {
                    chart_id,
                    symbol,
                    rows,
                })
            }
            ReadyPage::CanonicalMinutes {
                terminal: _, rows, ..
            } => {
                let (chart_id, symbol, rows) =
                    self.take_new_minutes(chart_index, rows, &mut reached_requested_end);
                (!rows.is_empty()).then_some(ServerBacktestHistoryEvent::CanonicalMinutes {
                    chart_id,
                    symbol,
                    rows,
                })
            }
            ReadyPage::CanonicalDaily {
                terminal: _, rows, ..
            } => {
                let (chart_id, symbol, rows) =
                    self.take_new_minutes(chart_index, rows, &mut reached_requested_end);
                (!rows.is_empty()).then_some(ServerBacktestHistoryEvent::CanonicalDaily {
                    chart_id,
                    symbol,
                    rows,
                })
            }
        };

        let advanced = self.charts[chart_index]
            .last_emitted_id
            .is_some_and(|last_id| old_last_page_right_id.is_none_or(|old| last_id > old));
        let terminal = page_terminal
            || reached_requested_end
            || !advanced
            || old_last_page_right_id.is_some_and(|old| right_id <= old);
        self.charts[chart_index].last_page_right_id = Some(right_id);

        if terminal {
            self.complete_chart(chart_index).await?;
        } else {
            self.charts[chart_index].next_left_kline_id = Some(right_id);
            self.charts[chart_index].page_number =
                self.charts[chart_index].page_number.saturating_add(1);
            self.cleanup
                .close(self.charts[chart_index].current_page_chart_id.as_str())
                .await?;
            self.open_page(chart_index).await?;
        }
        Ok(event)
    }

    fn take_new_ticks(
        &mut self,
        chart_index: usize,
        rows: Vec<Tick>,
        reached_requested_end: &mut bool,
    ) -> (String, String, Vec<Tick>) {
        let state = &mut self.charts[chart_index];
        let mut emitted = Vec::new();
        for row in rows {
            if state
                .last_emitted_id
                .is_some_and(|last_id| row.id <= last_id)
            {
                continue;
            }
            state.last_emitted_id = Some(row.id);
            if row.datetime >= self.request.end_ns {
                *reached_requested_end = true;
                continue;
            }
            if row.datetime >= self.request.start_ns {
                emitted.push(row);
            }
        }
        (
            state.chart.chart_id.clone(),
            state.chart.symbol.clone(),
            emitted,
        )
    }

    fn take_new_minutes(
        &mut self,
        chart_index: usize,
        rows: Vec<Kline>,
        reached_requested_end: &mut bool,
    ) -> (String, String, Vec<Kline>) {
        let state = &mut self.charts[chart_index];
        let mut emitted = Vec::new();
        for row in rows {
            if state
                .last_emitted_id
                .is_some_and(|last_id| row.id <= last_id)
            {
                continue;
            }
            state.last_emitted_id = Some(row.id);
            if row.datetime >= self.request.end_ns {
                *reached_requested_end = true;
                continue;
            }
            if row.datetime >= self.request.start_ns {
                emitted.push(row);
            }
        }
        (
            state.chart.chart_id.clone(),
            state.chart.symbol.clone(),
            emitted,
        )
    }

    async fn complete_chart(&mut self, chart_index: usize) -> Result<()> {
        if self.charts[chart_index].completed {
            return Ok(());
        }
        let chart_id = self.charts[chart_index].chart.chart_id.clone();
        let symbol = self.charts[chart_index].chart.symbol.clone();
        let page_chart_id = self.charts[chart_index].current_page_chart_id.clone();
        self.charts[chart_index].completed = true;
        self.cleanup.close(page_chart_id.as_str()).await?;
        self.pending_events
            .push_back(ServerBacktestHistoryEvent::ChartCompleted { chart_id, symbol });
        Ok(())
    }

    async fn open_page(&mut self, chart_index: usize) -> Result<()> {
        let state = &self.charts[chart_index];
        let chart_id = state.page_chart_id();
        let command = MarketChartCommand {
            chart_id: chart_id.clone(),
            symbols: vec![Symbol::new(state.chart.symbol.as_str())],
            duration_ns: state.duration_ns(),
            view_width: SERVER_BACKTEST_HISTORY_PAGE_WIDTH,
            left_kline_id: state.next_left_kline_id,
            focus_datetime_ns: (state.page_number == 0).then_some(self.request.start_ns),
            focus_position: (state.page_number == 0).then_some(0),
        };
        let lease = self.session.ensure_chart(command).await?;
        self.cleanup.insert(chart_id.clone(), lease).await;
        self.charts[chart_index].current_page_chart_id = chart_id;
        Ok(())
    }

    fn read_ready_page(&self, chart_index: usize) -> Result<Option<ReadyPage>> {
        let state = &self.charts[chart_index];
        let market = self.reader.read_market_state();
        let Some(chart) =
            market.decode_path::<Chart>(&["charts", state.current_page_chart_id.as_str()])?
        else {
            return Ok(None);
        };
        if !chart.ready || chart.more_data || !page_state_matches(&chart, state, &self.request) {
            return Ok(None);
        }
        if market
            .get_path(&["mdhis_more_data"])
            .and_then(Value::as_bool)
            != Some(false)
        {
            return Ok(None);
        }
        let series = match state.chart.kind {
            ServerBacktestHistoryKind::Tick => {
                market.get_path(&["ticks", state.chart.symbol.as_str()])
            }
            ServerBacktestHistoryKind::CanonicalMinute => {
                market.get_path(&["klines", state.chart.symbol.as_str(), "60000000000"])
            }
            ServerBacktestHistoryKind::CanonicalDaily => {
                market.get_path(&["klines", state.chart.symbol.as_str(), "86400000000000"])
            }
        };
        let last_id = series
            .and_then(|value| value.get("last_id"))
            .and_then(Value::as_i64);
        let data = series.and_then(|value| value.get("data"));

        if chart.left_id == -1 && chart.right_id == -1 {
            if last_id == Some(-1) && data.is_some_and(Value::is_object_and_empty) {
                return Ok(Some(match state.chart.kind {
                    ServerBacktestHistoryKind::Tick => ReadyPage::Ticks {
                        right_id: -1,
                        terminal: true,
                        rows: Vec::new(),
                    },
                    ServerBacktestHistoryKind::CanonicalMinute => ReadyPage::CanonicalMinutes {
                        right_id: -1,
                        terminal: true,
                        rows: Vec::new(),
                    },
                    ServerBacktestHistoryKind::CanonicalDaily => ReadyPage::CanonicalDaily {
                        right_id: -1,
                        terminal: true,
                        rows: Vec::new(),
                    },
                }));
            }
            return Ok(None);
        }
        if chart.left_id < 0 || chart.right_id < chart.left_id {
            return Err(validation_error(
                "server-backtest chart returned invalid page bounds",
            ));
        }
        let Some(last_id) = last_id else {
            return Ok(None);
        };
        let effective_right_id = chart.right_id.min(last_id);
        let terminal = last_id < chart.right_id;
        let Some(data) = data.and_then(Value::as_object) else {
            return Ok(None);
        };
        match state.chart.kind {
            ServerBacktestHistoryKind::Tick => {
                let rows = decode_page_rows::<Tick>(data, chart.left_id, effective_right_id)?;
                if rows.is_empty() && !terminal {
                    return Err(validation_error(
                        "server-backtest Tick chart was ready without its page rows",
                    ));
                }
                Ok(Some(ReadyPage::Ticks {
                    right_id: effective_right_id,
                    terminal,
                    rows,
                }))
            }
            ServerBacktestHistoryKind::CanonicalMinute => {
                let rows = decode_page_rows::<Kline>(data, chart.left_id, effective_right_id)?;
                if rows.is_empty() && !terminal {
                    return Err(validation_error(
                        "server-backtest canonical-minute chart was ready without its page rows",
                    ));
                }
                Ok(Some(ReadyPage::CanonicalMinutes {
                    right_id: effective_right_id,
                    terminal,
                    rows,
                }))
            }
            ServerBacktestHistoryKind::CanonicalDaily => {
                let rows = decode_page_rows::<Kline>(data, chart.left_id, effective_right_id)?;
                if rows.is_empty() && !terminal {
                    return Err(validation_error(
                        "server-backtest canonical-daily chart was ready without page rows",
                    ));
                }
                Ok(Some(ReadyPage::CanonicalDaily {
                    right_id: effective_right_id,
                    terminal,
                    rows,
                }))
            }
        }
    }
}

impl Drop for ServerBacktestHistoryStream {
    fn drop(&mut self) {
        self.cleanup.request_cancellation();
    }
}

fn validate_request(session: &SessionClient, request: &ServerBacktestHistoryRequest) -> Result<()> {
    if request.start_ns >= request.end_ns {
        return Err(validation_error(
            "server-backtest history range requires start_ns < end_ns",
        ));
    }
    if request.charts.is_empty() {
        return Err(validation_error(
            "server-backtest history request requires at least one chart",
        ));
    }
    let target = session.market_target();
    let expected_stock = matches!(request.market_kind, ServerBacktestMarketKind::Stock);
    if !target.backtest || target.stock != expected_stock {
        return Err(validation_error(
            "server-backtest history request does not match the session market target",
        ));
    }

    let mut chart_ids = BTreeSet::new();
    for chart in &request.charts {
        if chart.chart_id.is_empty() || chart.chart_id.trim() != chart.chart_id {
            return Err(validation_error(
                "server-backtest history chart_id must be non-empty and trimmed",
            ));
        }
        if chart.symbol.is_empty() || chart.symbol.trim() != chart.symbol {
            return Err(validation_error(
                "server-backtest history symbol must be non-empty and trimmed",
            ));
        }
        if !chart_ids.insert(chart.chart_id.as_str()) {
            return Err(validation_error(
                "server-backtest history chart_id values must be unique",
            ));
        }
    }
    Ok(())
}

fn page_state_matches(
    chart: &Chart,
    state: &HistoryChartState,
    request: &ServerBacktestHistoryRequest,
) -> bool {
    let expected_ins_list = state.chart.symbol.as_str();
    let expected_duration = state.duration_ns();
    chart.state.get("ins_list").and_then(Value::as_str) == Some(expected_ins_list)
        && chart.state.get("duration").and_then(Value::as_i64) == Some(expected_duration)
        && chart.state.get("view_width").and_then(Value::as_u64)
            == Some(SERVER_BACKTEST_HISTORY_PAGE_WIDTH as u64)
        && if state.page_number == 0 {
            chart.state.get("focus_datetime").and_then(Value::as_i64) == Some(request.start_ns)
                && chart.state.get("focus_position").and_then(Value::as_u64) == Some(0)
        } else {
            chart.state.get("left_kline_id").and_then(Value::as_i64) == state.next_left_kline_id
        }
}

fn decode_page_rows<T>(
    data: &serde_json::Map<String, Value>,
    left_id: i64,
    right_id: i64,
) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned + HasHistoryRowId,
{
    let mut rows = data
        .iter()
        .filter_map(|(id, value)| {
            let id = id.parse::<i64>().ok()?;
            (left_id <= id && id <= right_id).then_some((id, value))
        })
        .map(|(id, value)| {
            serde_json::from_value::<T>(value.clone()).map_err(|error| {
                validation_error(format!(
                    "server-backtest history row {id} could not be decoded: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by_key(HasHistoryRowId::row_id);
    if rows.len() > SERVER_BACKTEST_HISTORY_PAGE_WIDTH {
        return Err(validation_error(
            "server-backtest history page exceeded the 10,000-row event bound",
        ));
    }
    Ok(rows)
}

trait HasHistoryRowId {
    fn row_id(&self) -> i64;
}

impl HasHistoryRowId for Tick {
    fn row_id(&self) -> i64 {
        self.id
    }
}

impl HasHistoryRowId for Kline {
    fn row_id(&self) -> i64 {
        self.id
    }
}

trait JsonValueExt {
    fn is_object_and_empty(&self) -> bool;
}

impl JsonValueExt for Value {
    fn is_object_and_empty(&self) -> bool {
        self.as_object().is_some_and(serde_json::Map::is_empty)
    }
}

fn validation_error(message: impl Into<String>) -> SessionFacadeError {
    tqsdk_core::ContractError::validation(message).into()
}
