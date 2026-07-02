#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use tqsdk_core::{
    Chart, CommitScope, InputPayload, IoEvent, MarketChartCommand, ObjectKey, ProtocolDomain,
    RuntimeInput, RuntimeReader, SharedCommitResult, Symbol,
};
use tqsdk_session::SessionClient;

use crate::error::{Result, WaitFacadeError};

const BACKTEST_PAGE_VIEW_WIDTH: usize = 10_000;
pub(crate) const BACKTEST_TICK_ROW_MARKER_FIELD: &str = "_wait_backtest_tick_row_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestMarketKind {
    Futures,
    Stock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqBacktest {
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    market_kind: BacktestMarketKind,
}

impl TqBacktest {
    pub fn new(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::futures(start_datetime_ns, end_datetime_ns)
    }

    pub fn futures(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::with_market_kind(
            start_datetime_ns,
            end_datetime_ns,
            BacktestMarketKind::Futures,
        )
    }

    pub fn stock(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::with_market_kind(
            start_datetime_ns,
            end_datetime_ns,
            BacktestMarketKind::Stock,
        )
    }

    fn with_market_kind(
        start_datetime_ns: i64,
        end_datetime_ns: i64,
        market_kind: BacktestMarketKind,
    ) -> crate::error::Result<Self> {
        if start_datetime_ns >= end_datetime_ns {
            return Err(crate::error::WaitFacadeError::InvalidState(
                "backtest start_datetime_ns must be less than end_datetime_ns",
            ));
        }
        Ok(Self {
            start_datetime_ns,
            end_datetime_ns,
            market_kind,
        })
    }

    #[must_use]
    pub fn start_datetime_ns(&self) -> i64 {
        self.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.end_datetime_ns
    }

    #[must_use]
    pub fn market_kind(&self) -> BacktestMarketKind {
        self.market_kind
    }
}

#[derive(Debug)]
pub(crate) enum BacktestCommitAction {
    Expose(SharedCommitResult),
    Synthetic(BacktestSyntheticCommit),
    Suppressed,
}

#[derive(Debug)]
pub(crate) struct BacktestSyntheticCommit {
    pub(crate) commit: SharedCommitResult,
    pub(crate) current_dt: Option<i64>,
}

#[derive(Debug, Default)]
pub(crate) struct BacktestPump {
    next_page_seq: u64,
    tick_serials: BTreeMap<String, BacktestTickSerial>,
    internal_tick_charts: BTreeMap<String, String>,
    mode: BacktestPumpMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum BacktestPumpMode {
    #[default]
    Strategy,
    CacheFill,
}

impl BacktestPump {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_mode(mode: BacktestPumpMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }

    pub(crate) fn new_cache_fill() -> Self {
        Self::with_mode(BacktestPumpMode::CacheFill)
    }

    pub(crate) fn tick_serial_exhausted(&self, chart_id: &str) -> Option<bool> {
        self.tick_serials
            .get(chart_id)
            .map(|serial| serial.exhausted)
    }

    pub(crate) async fn ensure_tick_serial(
        &mut self,
        session: &SessionClient,
        backtest: &TqBacktest,
        symbol: &str,
        view_width: usize,
        chart_id: &str,
    ) -> Result<()> {
        if self.tick_serials.contains_key(chart_id) {
            return Ok(());
        }

        let focus_position = match self.mode {
            BacktestPumpMode::Strategy => BACKTEST_PAGE_VIEW_WIDTH,
            BacktestPumpMode::CacheFill => 0,
        };
        let internal_chart_id = self
            .request_tick_page(
                session,
                symbol,
                TickPageRequest::Focus {
                    datetime_ns: backtest.start_datetime_ns(),
                    position: focus_position,
                },
            )
            .await?;
        let serial = BacktestTickSerial {
            symbol: symbol.to_string(),
            view_width,
            user_chart_id: chart_id.to_string(),
            awaiting_page: true,
            current_page_left_id: None,
            current_page_right_id: None,
            next_emit_id: None,
            first_emitted_id: None,
            last_emitted_id: None,
            last_loaded_right_id: None,
            exhausted: false,
        };
        self.internal_tick_charts
            .insert(internal_chart_id, chart_id.to_string());
        self.tick_serials.insert(chart_id.to_string(), serial);

        synthesize_ready_tick_commit(session, symbol, chart_id)?;
        Ok(())
    }

    pub(crate) async fn emit_pending_tick(
        &mut self,
        session: &SessionClient,
        reader: &RuntimeReader,
        backtest: &TqBacktest,
    ) -> Result<Option<BacktestSyntheticCommit>> {
        if self.mode == BacktestPumpMode::CacheFill {
            return self
                .emit_pending_tick_cache_fill(session, reader, backtest)
                .await;
        }

        let chart_ids = self.tick_serials.keys().cloned().collect::<Vec<_>>();
        let mut best: Option<TickPumpCandidate> = None;

        for chart_id in chart_ids {
            let candidate = {
                let Some(serial) = self.tick_serials.get(&chart_id) else {
                    continue;
                };
                serial.peek_emit_candidate(reader, backtest)?
            };
            if let Some(candidate) = candidate {
                if best
                    .as_ref()
                    .is_none_or(|best_candidate| candidate.precedes(best_candidate))
                {
                    best = Some(candidate);
                }
                continue;
            }

            let decision = {
                let Some(serial) = self.tick_serials.get_mut(&chart_id) else {
                    continue;
                };
                serial.next_decision(reader, backtest)?
            };

            match decision {
                TickPumpDecision::Emit {
                    symbol,
                    user_chart_id,
                    row_id,
                    row,
                    datetime,
                    first_visible_id,
                } => {
                    return synthesize_tick_commit(
                        session,
                        &symbol,
                        &user_chart_id,
                        row_id,
                        row,
                        datetime,
                        first_visible_id,
                    );
                }
                TickPumpDecision::RequestNextPage {
                    symbol,
                    user_chart_id,
                    left_id,
                } => {
                    self.request_next_tick_page(session, &symbol, &user_chart_id, left_id)
                        .await?;
                    if self.mode == BacktestPumpMode::Strategy {
                        return Ok(None);
                    }
                }
                TickPumpDecision::None => {
                    if self
                        .tick_serials
                        .get(&chart_id)
                        .is_some_and(|serial| serial.awaiting_page && !serial.exhausted)
                        && self.mode == BacktestPumpMode::Strategy
                    {
                        return Ok(None);
                    }
                }
            }
        }

        let Some(candidate) = best else {
            return Ok(None);
        };
        if let Some(serial) = self.tick_serials.get_mut(&candidate.user_chart_id) {
            serial.consume_emit_candidate(candidate.row_id);
        }
        synthesize_tick_commit(
            session,
            &candidate.symbol,
            &candidate.user_chart_id,
            candidate.row_id,
            candidate.row,
            candidate.datetime,
            candidate.first_visible_id,
        )
    }

    async fn emit_pending_tick_cache_fill(
        &mut self,
        session: &SessionClient,
        reader: &RuntimeReader,
        backtest: &TqBacktest,
    ) -> Result<Option<BacktestSyntheticCommit>> {
        let chart_ids = self.tick_serials.keys().cloned().collect::<Vec<_>>();

        for chart_id in chart_ids {
            let decision = {
                let Some(serial) = self.tick_serials.get_mut(&chart_id) else {
                    continue;
                };
                serial.next_decision(reader, backtest)?
            };

            match decision {
                TickPumpDecision::Emit {
                    symbol,
                    user_chart_id,
                    row_id,
                    row,
                    datetime,
                    first_visible_id,
                } => {
                    return synthesize_tick_commit(
                        session,
                        &symbol,
                        &user_chart_id,
                        row_id,
                        row,
                        datetime,
                        first_visible_id,
                    );
                }
                TickPumpDecision::RequestNextPage {
                    symbol,
                    user_chart_id,
                    left_id,
                } => {
                    self.request_next_tick_page(session, &symbol, &user_chart_id, left_id)
                        .await?;
                }
                TickPumpDecision::None => {}
            }
        }

        Ok(None)
    }

    pub(crate) async fn handle_commit(
        &mut self,
        commit: SharedCommitResult,
        session: &SessionClient,
        reader: &RuntimeReader,
        backtest: &TqBacktest,
    ) -> Result<BacktestCommitAction> {
        let internal_chart_ids = self.touched_internal_tick_charts(&commit);
        if internal_chart_ids.is_empty() {
            return Ok(BacktestCommitAction::Expose(commit));
        }

        for internal_chart_id in internal_chart_ids {
            let Some(user_chart_id) = self.internal_tick_charts.get(&internal_chart_id).cloned()
            else {
                continue;
            };

            if let Some(serial) = self.tick_serials.get_mut(&user_chart_id) {
                trace_backtest_tick(format_args!(
                    "load_page internal={internal_chart_id} user={user_chart_id}"
                ));
                serial.load_page(reader, &internal_chart_id)?;
            }
        }

        if let Some(commit) = self.emit_pending_tick(session, reader, backtest).await? {
            return Ok(BacktestCommitAction::Synthetic(commit));
        }

        Ok(BacktestCommitAction::Suppressed)
    }

    fn touched_internal_tick_charts(&self, commit: &tqsdk_core::CommitResult) -> Vec<String> {
        let mut chart_ids = BTreeSet::new();
        for object in &commit.changes.object_hits {
            if let ObjectKey::Chart { chart_id } = object {
                let chart_id = chart_id.as_str();
                if self.internal_tick_charts.contains_key(chart_id) {
                    chart_ids.insert(chart_id.to_string());
                }
            }
        }

        for path in &commit.changes.path_hits {
            let segments = path.segments();
            if let [root, chart_id, ..] = segments
                && root == "charts"
                && self.internal_tick_charts.contains_key(chart_id)
            {
                chart_ids.insert(chart_id.clone());
            }
        }

        let touched_symbols = touched_tick_symbols(commit);
        let global_history_state_changed = commit
            .changes
            .path_hits
            .iter()
            .any(|path| matches!(path.segments(), [field] if field == "mdhis_more_data"));

        if !touched_symbols.is_empty() || global_history_state_changed {
            for (internal_chart_id, user_chart_id) in &self.internal_tick_charts {
                let Some(serial) = self.tick_serials.get(user_chart_id) else {
                    continue;
                };
                if !serial.awaiting_page {
                    continue;
                }
                if global_history_state_changed || touched_symbols.contains(serial.symbol.as_str())
                {
                    chart_ids.insert(internal_chart_id.clone());
                }
            }
        }

        chart_ids.into_iter().collect()
    }

    async fn request_tick_page(
        &mut self,
        session: &SessionClient,
        symbol: &str,
        request: TickPageRequest,
    ) -> Result<String> {
        self.next_page_seq += 1;
        let chart_id = format!(
            "wait-backtest-tick-{}-{}",
            sanitize_chart_token(symbol),
            self.next_page_seq
        );

        let mut command = MarketChartCommand {
            chart_id: chart_id.clone(),
            symbols: vec![Symbol::new(symbol)],
            duration_ns: 0,
            view_width: BACKTEST_PAGE_VIEW_WIDTH,
            left_kline_id: None,
            focus_datetime_ns: None,
            focus_position: None,
        };
        match request {
            TickPageRequest::Focus {
                datetime_ns,
                position,
            } => {
                command.focus_datetime_ns = Some(datetime_ns);
                command.focus_position = Some(position);
            }
            TickPageRequest::LeftId(left_id) => {
                command.left_kline_id = Some(left_id);
            }
        }

        session
            .ensure_chart(command)
            .await
            .map_err(WaitFacadeError::Session)?;

        Ok(chart_id)
    }

    async fn request_next_tick_page(
        &mut self,
        session: &SessionClient,
        symbol: &str,
        user_chart_id: &str,
        left_id: i64,
    ) -> Result<()> {
        self.internal_tick_charts
            .retain(|_, mapped_user_chart_id| mapped_user_chart_id != user_chart_id);
        let internal_chart_id = self
            .request_tick_page(session, symbol, TickPageRequest::LeftId(left_id))
            .await?;
        if let Some(serial) = self.tick_serials.get_mut(user_chart_id) {
            serial.awaiting_page = true;
        }
        self.internal_tick_charts
            .insert(internal_chart_id, user_chart_id.to_string());
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum TickPageRequest {
    Focus { datetime_ns: i64, position: usize },
    LeftId(i64),
}

#[derive(Debug)]
struct BacktestTickSerial {
    symbol: String,
    view_width: usize,
    user_chart_id: String,
    awaiting_page: bool,
    current_page_left_id: Option<i64>,
    current_page_right_id: Option<i64>,
    next_emit_id: Option<i64>,
    first_emitted_id: Option<i64>,
    last_emitted_id: Option<i64>,
    last_loaded_right_id: Option<i64>,
    exhausted: bool,
}

impl BacktestTickSerial {
    fn peek_emit_candidate(
        &self,
        reader: &RuntimeReader,
        backtest: &TqBacktest,
    ) -> Result<Option<TickPumpCandidate>> {
        if self.exhausted || self.awaiting_page {
            return Ok(None);
        }

        let Some(mut id) = self.next_emit_id else {
            return Ok(None);
        };
        let Some(right_id) = self.current_page_right_id else {
            return Ok(None);
        };

        while id <= right_id {
            let id_key = id.to_string();
            let row = {
                let market = reader.read_market_state();
                market
                    .get_path(&["ticks", self.symbol.as_str(), "data", id_key.as_str()])
                    .cloned()
            };
            let Some(row) = row else {
                id = match id.checked_add(1) {
                    Some(next) => next,
                    None => break,
                };
                continue;
            };

            let datetime = row.get("datetime").and_then(Value::as_i64).ok_or(
                WaitFacadeError::InvalidState("backtest tick row missing datetime"),
            )?;
            if datetime < backtest.start_datetime_ns() {
                id = match id.checked_add(1) {
                    Some(next) => next,
                    None => break,
                };
                continue;
            }
            if datetime >= backtest.end_datetime_ns() {
                return Ok(None);
            }

            let first_emitted_id = self.first_emitted_id.unwrap_or(id);
            let width_left_id = id.saturating_sub(self.view_width.saturating_sub(1) as i64);
            let first_visible_id = width_left_id.max(first_emitted_id);
            return Ok(Some(TickPumpCandidate {
                symbol: self.symbol.clone(),
                user_chart_id: self.user_chart_id.clone(),
                row_id: id,
                row,
                datetime,
                first_visible_id,
            }));
        }

        Ok(None)
    }

    fn consume_emit_candidate(&mut self, row_id: i64) {
        self.first_emitted_id.get_or_insert(row_id);
        self.last_emitted_id = Some(row_id);
        self.next_emit_id = row_id.checked_add(1);
    }

    fn load_page(&mut self, reader: &RuntimeReader, internal_chart_id: &str) -> Result<()> {
        let market = reader.read_market_state();
        let Some(chart) = market.decode_path::<Chart>(&["charts", internal_chart_id])? else {
            return Ok(());
        };
        trace_backtest_tick(format_args!(
            "chart {internal_chart_id} ready={} more_data={} left={} right={} last_loaded={:?} last_emitted={:?}",
            chart.ready,
            chart.more_data,
            chart.left_id,
            chart.right_id,
            self.last_loaded_right_id,
            self.last_emitted_id
        ));
        if !chart.ready {
            return Ok(());
        }
        let serial_last_id = tick_last_id(&market, &self.symbol);
        let mdhis_more_data = market_mdhis_more_data(&market);
        let rows_ready = tick_rows_ready(&market, &self.symbol, &chart, serial_last_id);
        if chart.more_data || mdhis_more_data || !rows_ready {
            trace_backtest_tick(format_args!(
                "wait page chart_more_data={} mdhis_more_data={} rows_ready={} serial_last_id={serial_last_id:?}",
                chart.more_data, mdhis_more_data, rows_ready
            ));
            return Ok(());
        }

        self.awaiting_page = false;
        self.current_page_left_id = Some(chart.left_id);
        self.current_page_right_id = Some(chart.right_id);
        if self
            .last_loaded_right_id
            .is_some_and(|last_right_id| chart.right_id <= last_right_id)
        {
            self.exhausted = true;
            self.next_emit_id = None;
            return Ok(());
        }
        self.last_loaded_right_id = Some(chart.right_id);

        if chart.left_id <= chart.right_id {
            let next_after_last = self
                .last_emitted_id
                .and_then(|id| id.checked_add(1))
                .unwrap_or(chart.left_id);
            self.next_emit_id = Some(next_after_last.max(chart.left_id));
        } else {
            self.exhausted = true;
            self.next_emit_id = None;
        }

        Ok(())
    }

    fn next_decision(
        &mut self,
        reader: &RuntimeReader,
        backtest: &TqBacktest,
    ) -> Result<TickPumpDecision> {
        if self.exhausted || self.awaiting_page {
            return Ok(TickPumpDecision::None);
        }

        let Some(mut id) = self.next_emit_id else {
            return self.next_page_or_finish();
        };
        let Some(right_id) = self.current_page_right_id else {
            return self.next_page_or_finish();
        };

        while id <= right_id {
            self.next_emit_id = id.checked_add(1);

            let id_key = id.to_string();
            let row = {
                let market = reader.read_market_state();
                market
                    .get_path(&["ticks", self.symbol.as_str(), "data", id_key.as_str()])
                    .cloned()
            };
            let Some(row) = row else {
                id = match id.checked_add(1) {
                    Some(next) => next,
                    None => break,
                };
                continue;
            };

            let datetime = row.get("datetime").and_then(Value::as_i64).ok_or(
                WaitFacadeError::InvalidState("backtest tick row missing datetime"),
            )?;
            if datetime < backtest.start_datetime_ns() {
                if id % 1_000 == 0 {
                    trace_backtest_tick(format_args!(
                        "skip before start id={id} datetime={datetime} start={}",
                        backtest.start_datetime_ns()
                    ));
                }
                id = match id.checked_add(1) {
                    Some(next) => next,
                    None => break,
                };
                continue;
            }
            if datetime >= backtest.end_datetime_ns() {
                self.exhausted = true;
                self.next_emit_id = None;
                return Ok(TickPumpDecision::None);
            }

            let first_emitted_id = *self.first_emitted_id.get_or_insert(id);
            self.last_emitted_id = Some(id);
            let width_left_id = id.saturating_sub(self.view_width.saturating_sub(1) as i64);
            let first_visible_id = width_left_id.max(first_emitted_id);

            trace_backtest_tick(format_args!(
                "emit id={id} datetime={datetime} first_visible_id={first_visible_id}"
            ));
            return Ok(TickPumpDecision::Emit {
                symbol: self.symbol.clone(),
                user_chart_id: self.user_chart_id.clone(),
                row_id: id,
                row,
                datetime,
                first_visible_id,
            });
        }

        self.next_page_or_finish()
    }

    fn next_page_or_finish(&mut self) -> Result<TickPumpDecision> {
        if let Some(right_id) = self.current_page_right_id {
            let left_id = right_id;
            trace_backtest_tick(format_args!(
                "request next page left_id={left_id} symbol={}",
                self.symbol
            ));
            self.current_page_left_id = None;
            self.current_page_right_id = None;
            self.next_emit_id = None;
            return Ok(TickPumpDecision::RequestNextPage {
                symbol: self.symbol.clone(),
                user_chart_id: self.user_chart_id.clone(),
                left_id,
            });
        }

        self.exhausted = true;
        self.next_emit_id = None;
        Ok(TickPumpDecision::None)
    }
}

fn trace_backtest_tick(args: std::fmt::Arguments<'_>) {
    if std::env::var_os("TQSDK_WAIT_BACKTEST_TRACE").is_some() {
        eprintln!("[tqsdk-wait backtest tick] {args}");
    }
}

fn market_mdhis_more_data(market: &tqsdk_core::MarketStateReadGuard<'_>) -> bool {
    market
        .get_path(&["mdhis_more_data"])
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn tick_last_id(market: &tqsdk_core::MarketStateReadGuard<'_>, symbol: &str) -> Option<i64> {
    market
        .get_path(&["ticks", symbol, "last_id"])
        .and_then(Value::as_i64)
}

fn tick_rows_ready(
    market: &tqsdk_core::MarketStateReadGuard<'_>,
    symbol: &str,
    chart: &Chart,
    last_id: Option<i64>,
) -> bool {
    if chart.left_id > chart.right_id {
        return true;
    }

    if last_id.is_some_and(|last_id| last_id >= chart.right_id) {
        return true;
    }

    let right_id = chart.right_id.to_string();
    market
        .get_path(&["ticks", symbol, "data", right_id.as_str()])
        .is_some()
}

fn touched_tick_symbols(commit: &tqsdk_core::CommitResult) -> BTreeSet<&str> {
    let mut symbols = BTreeSet::new();

    for object in &commit.changes.object_hits {
        if let ObjectKey::Tick { symbol, .. } = object {
            symbols.insert(symbol.as_str());
        }
    }

    for hit in &commit.changes.field_hits {
        if let ObjectKey::Tick { symbol, .. } = &hit.object {
            symbols.insert(symbol.as_str());
        }
    }

    for path in &commit.changes.path_hits {
        if let [root, symbol, ..] = path.segments()
            && root == "ticks"
        {
            symbols.insert(symbol.as_str());
        }
    }

    symbols
}

#[derive(Debug)]
struct TickPumpCandidate {
    symbol: String,
    user_chart_id: String,
    row_id: i64,
    row: Value,
    datetime: i64,
    first_visible_id: i64,
}

impl TickPumpCandidate {
    fn precedes(&self, other: &Self) -> bool {
        self.datetime < other.datetime
            || (self.datetime == other.datetime && self.user_chart_id < other.user_chart_id)
    }
}

#[derive(Debug)]
enum TickPumpDecision {
    Emit {
        symbol: String,
        user_chart_id: String,
        row_id: i64,
        row: Value,
        datetime: i64,
        first_visible_id: i64,
    },
    RequestNextPage {
        symbol: String,
        user_chart_id: String,
        left_id: i64,
    },
    None,
}

fn synthesize_ready_tick_commit(
    session: &SessionClient,
    symbol: &str,
    chart_id: &str,
) -> Result<Option<SharedCommitResult>> {
    session
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": symbol,
                                    "duration": 0,
                                },
                                "left_id": -1,
                                "right_id": -1,
                                "ready": true,
                                "more_data": false,
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .map_err(Into::into)
}

fn synthesize_tick_commit(
    session: &SessionClient,
    symbol: &str,
    chart_id: &str,
    row_id: i64,
    row: Value,
    datetime: i64,
    first_visible_id: i64,
) -> Result<Option<BacktestSyntheticCommit>> {
    let row_key = row_id.to_string();
    let prune_key = row_id
        .saturating_sub(BACKTEST_PAGE_VIEW_WIDTH as i64)
        .to_string();
    let commit = session
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": symbol,
                                    "duration": 0,
                                },
                                "left_id": first_visible_id,
                                "right_id": row_id,
                                "ready": true,
                                "more_data": false,
                                (BACKTEST_TICK_ROW_MARKER_FIELD): row_id,
                            }
                        },
                        "ticks": {
                            symbol: {
                                "last_id": row_id,
                                "data": {
                                    row_key: row,
                                    prune_key: Value::Null,
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .map_err(WaitFacadeError::from)?;

    Ok(commit.map(|commit| BacktestSyntheticCommit {
        commit,
        current_dt: Some(datetime),
    }))
}

fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tqsdk_core::{
        AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
        ProtocolDomain, RuntimeHandle, RuntimeInput,
    };
    use tqsdk_session::testing::ManualSession;

    use super::*;

    #[tokio::test]
    async fn backtest_tick_pump_emits_earliest_timestamp_across_serials() {
        let handle = runtime_with_default_adapters();
        seed_tick(&handle, "ZZZ.late", 1, 2_000);
        seed_tick(&handle, "AAA.early", 10, 1_000);
        let reader = handle.reader();
        let session = ManualSession::from_runtime(handle).into_client();
        let backtest = TqBacktest::futures(0, 10_000).expect("valid backtest range");

        let mut pump = BacktestPump::new();
        pump.tick_serials.insert(
            "a-chart".to_string(),
            ready_serial("ZZZ.late", "a-chart", 1, 1),
        );
        pump.tick_serials.insert(
            "b-chart".to_string(),
            ready_serial("AAA.early", "b-chart", 10, 10),
        );

        let synthetic = pump
            .emit_pending_tick(&session, &reader, &backtest)
            .await
            .expect("emit should succeed")
            .expect("one synthetic tick should be emitted");

        assert_eq!(synthetic.current_dt, Some(1_000));
        assert_eq!(emitted_marker(&reader, "b-chart"), Some(10));
        assert_eq!(emitted_marker(&reader, "a-chart"), None);
    }

    #[tokio::test]
    async fn backtest_tick_pump_continues_in_global_timestamp_order_after_each_emit() {
        let handle = runtime_with_default_adapters();
        seed_tick(&handle, "AAA.first_then_last", 1, 1_000);
        seed_tick(&handle, "AAA.first_then_last", 2, 3_000);
        seed_tick(&handle, "BBB.middle", 10, 2_000);
        seed_tick(&handle, "BBB.middle", 11, 4_000);
        let reader = handle.reader();
        let session = ManualSession::from_runtime(handle).into_client();
        let backtest = TqBacktest::futures(0, 10_000).expect("valid backtest range");

        let mut pump = BacktestPump::new();
        pump.tick_serials.insert(
            "a-chart".to_string(),
            ready_serial("AAA.first_then_last", "a-chart", 1, 2),
        );
        pump.tick_serials.insert(
            "b-chart".to_string(),
            ready_serial("BBB.middle", "b-chart", 10, 11),
        );

        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(1_000)
        );
        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(2_000)
        );
        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(3_000)
        );
    }

    #[tokio::test]
    async fn backtest_tick_pump_skips_before_start_rows_before_selecting_earliest_serial() {
        let handle = runtime_with_default_adapters();
        seed_tick(&handle, "AAA.before_start_then_late", 1, 500);
        seed_tick(&handle, "AAA.before_start_then_late", 2, 3_000);
        seed_tick(&handle, "BBB.in_range", 10, 2_000);
        seed_tick(&handle, "BBB.in_range", 11, 4_000);
        let reader = handle.reader();
        let session = ManualSession::from_runtime(handle).into_client();
        let backtest = TqBacktest::futures(1_000, 10_000).expect("valid backtest range");

        let mut pump = BacktestPump::new();
        pump.tick_serials.insert(
            "a-chart".to_string(),
            ready_serial("AAA.before_start_then_late", "a-chart", 1, 2),
        );
        pump.tick_serials.insert(
            "b-chart".to_string(),
            ready_serial("BBB.in_range", "b-chart", 10, 11),
        );

        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(2_000)
        );
        assert_eq!(emitted_marker(&reader, "b-chart"), Some(10));
        assert_eq!(emitted_marker(&reader, "a-chart"), None);

        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(3_000)
        );
        assert_eq!(emitted_marker(&reader, "a-chart"), Some(2));
    }

    #[tokio::test]
    async fn backtest_tick_pump_requests_next_page_before_emitting_later_ready_serial() {
        let handle = runtime_with_default_adapters();
        seed_tick(&handle, "AAA.needs_next_page", 1, 1_000);
        seed_tick(&handle, "BBB.future_ready", 10, 5_000);
        let reader = handle.reader();
        let session = ManualSession::from_runtime(handle).into_client();
        let backtest = TqBacktest::futures(0, 10_000).expect("valid backtest range");

        let mut pump = BacktestPump::new();
        pump.tick_serials.insert(
            "a-chart".to_string(),
            ready_serial("AAA.needs_next_page", "a-chart", 1, 1),
        );
        pump.tick_serials.insert(
            "b-chart".to_string(),
            ready_serial("BBB.future_ready", "b-chart", 10, 10),
        );

        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(1_000)
        );

        let synthetic = pump
            .emit_pending_tick(&session, &reader, &backtest)
            .await
            .expect("next page request should succeed");

        assert!(synthetic.is_none());
        assert_eq!(emitted_marker(&reader, "b-chart"), None);
        let a_serial = pump
            .tick_serials
            .get("a-chart")
            .expect("a-chart serial should still exist");
        assert!(a_serial.awaiting_page);
    }

    #[tokio::test]
    async fn backtest_tick_pump_cache_fill_mode_emits_ready_serial_while_requesting_next_page() {
        let handle = runtime_with_default_adapters();
        seed_tick(&handle, "AAA.needs_next_page", 1, 1_000);
        seed_tick(&handle, "BBB.future_ready", 10, 5_000);
        let reader = handle.reader();
        let session = ManualSession::from_runtime(handle).into_client();
        let backtest = TqBacktest::futures(0, 10_000).expect("valid backtest range");

        let mut pump = BacktestPump::new_cache_fill();
        pump.tick_serials.insert(
            "a-chart".to_string(),
            ready_serial("AAA.needs_next_page", "a-chart", 1, 1),
        );
        pump.tick_serials.insert(
            "b-chart".to_string(),
            ready_serial("BBB.future_ready", "b-chart", 10, 10),
        );

        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(1_000)
        );

        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(5_000)
        );
        assert_eq!(emitted_marker(&reader, "b-chart"), Some(10));
        let a_serial = pump
            .tick_serials
            .get("a-chart")
            .expect("a-chart serial should still exist");
        assert!(a_serial.awaiting_page);
    }

    #[tokio::test]
    async fn backtest_tick_pump_uses_chart_id_tie_break_for_equal_timestamps() {
        let handle = runtime_with_default_adapters();
        seed_tick(&handle, "ZZZ.same_time", 20, 1_000);
        seed_tick(&handle, "AAA.same_time", 10, 1_000);
        seed_tick(&handle, "AAA.same_time", 11, 2_000);
        let reader = handle.reader();
        let session = ManualSession::from_runtime(handle).into_client();
        let backtest = TqBacktest::futures(0, 10_000).expect("valid backtest range");

        let mut pump = BacktestPump::new();
        pump.tick_serials.insert(
            "b-chart".to_string(),
            ready_serial("ZZZ.same_time", "b-chart", 20, 20),
        );
        pump.tick_serials.insert(
            "a-chart".to_string(),
            ready_serial("AAA.same_time", "a-chart", 10, 11),
        );

        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(1_000)
        );
        assert_eq!(emitted_marker(&reader, "a-chart"), Some(10));
        assert_eq!(emitted_marker(&reader, "b-chart"), None);

        assert_eq!(
            emit_current_dt(&mut pump, &session, &reader, &backtest).await,
            Some(1_000)
        );
        assert_eq!(emitted_marker(&reader, "b-chart"), Some(20));
    }

    #[tokio::test]
    async fn backtest_tick_pump_focus_position_matches_mode() {
        let backtest = TqBacktest::futures(1_000, 2_000).expect("valid backtest range");

        let strategy_session = ManualSession::from_runtime(runtime_with_default_adapters());
        let mut strategy_pump = BacktestPump::new();
        strategy_pump
            .ensure_tick_serial(
                strategy_session.client(),
                &backtest,
                "SHFE.ag2608",
                256,
                "strategy-chart",
            )
            .await
            .expect("strategy tick serial should be requested");
        let strategy_body = set_chart_body(&strategy_session);
        assert_eq!(strategy_body.get("focus_datetime"), Some(&json!(1_000)));
        assert_eq!(
            strategy_body.get("focus_position"),
            Some(&json!(BACKTEST_PAGE_VIEW_WIDTH))
        );

        let cache_fill_session = ManualSession::from_runtime(runtime_with_default_adapters());
        let mut cache_fill_pump = BacktestPump::new_cache_fill();
        cache_fill_pump
            .ensure_tick_serial(
                cache_fill_session.client(),
                &backtest,
                "SHFE.ag2608",
                256,
                "cache-fill-chart",
            )
            .await
            .expect("cache fill tick serial should be requested");
        let cache_fill_body = set_chart_body(&cache_fill_session);
        assert_eq!(cache_fill_body.get("focus_datetime"), Some(&json!(1_000)));
        assert_eq!(cache_fill_body.get("focus_position"), Some(&json!(0)));
    }

    #[test]
    fn tick_rows_ready_requires_right_edge_but_allows_sparse_rows() {
        let handle = runtime_with_default_adapters();
        seed_tick(&handle, "SHFE.missing_right", 1, 1_000);
        seed_tick(&handle, "SHFE.partial", 1, 1_000);
        seed_tick(&handle, "SHFE.partial", 3, 3_000);
        let reader = handle.reader();
        let market = reader.read_market_state();
        let chart = Chart {
            left_id: 1,
            right_id: 3,
            more_data: false,
            ready: true,
            state: Default::default(),
            epoch: None,
        };

        assert!(!tick_rows_ready(
            &market,
            "SHFE.missing_right",
            &chart,
            Some(2)
        ));
        assert!(tick_rows_ready(&market, "SHFE.partial", &chart, Some(2)));
        assert!(tick_rows_ready(&market, "SHFE.partial", &chart, Some(3)));
    }

    #[test]
    fn tick_next_page_reuses_current_right_edge_boundary() {
        let mut serial = ready_serial("SHFE.ag2608", "chart", 1, 3);

        let decision = serial
            .next_page_or_finish()
            .expect("next page decision should succeed");

        match decision {
            TickPumpDecision::RequestNextPage { left_id, .. } => {
                assert_eq!(left_id, 3);
            }
            other => panic!("expected next page request, got {other:?}"),
        }
    }

    fn set_chart_body(session: &ManualSession) -> serde_json::Value {
        session
            .drain_dispatches()
            .expect("dispatches should drain")
            .into_iter()
            .find_map(|dispatch| match dispatch.request {
                OutboundRequest::Transport(OutboundFrame::Text(text)) => {
                    let body: serde_json::Value =
                        serde_json::from_str(&text).expect("market frame should be json");
                    (body.get("aid") == Some(&json!("set_chart"))).then_some(body)
                }
                _ => None,
            })
            .expect("set_chart dispatch should be present")
    }

    fn runtime_with_default_adapters() -> RuntimeHandle {
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        RuntimeHandle::with_adapters(adapters)
    }

    async fn emit_current_dt(
        pump: &mut BacktestPump,
        session: &SessionClient,
        reader: &RuntimeReader,
        backtest: &TqBacktest,
    ) -> Option<i64> {
        pump.emit_pending_tick(session, reader, backtest)
            .await
            .expect("emit should succeed")
            .expect("one synthetic tick should be emitted")
            .current_dt
    }

    fn emitted_marker(reader: &RuntimeReader, chart_id: &str) -> Option<i64> {
        let market = reader.read_market_state();
        market
            .get_path(&["charts", chart_id, BACKTEST_TICK_ROW_MARKER_FIELD])
            .and_then(Value::as_i64)
    }

    fn ready_serial(
        symbol: &str,
        user_chart_id: &str,
        left_id: i64,
        right_id: i64,
    ) -> BacktestTickSerial {
        BacktestTickSerial {
            symbol: symbol.to_string(),
            view_width: 10_000,
            user_chart_id: user_chart_id.to_string(),
            awaiting_page: false,
            current_page_left_id: Some(left_id),
            current_page_right_id: Some(right_id),
            next_emit_id: Some(left_id),
            first_emitted_id: None,
            last_emitted_id: None,
            last_loaded_right_id: Some(right_id),
            exhausted: false,
        }
    }

    fn seed_tick(handle: &RuntimeHandle, symbol: &str, id: i64, datetime: i64) {
        let id_key = id.to_string();
        handle
            .ingest(
                RuntimeInput::Io(IoEvent {
                    route: "market".to_string(),
                    domains: vec![ProtocolDomain::Market],
                    payload: InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "ticks": {
                                symbol: {
                                    "last_id": id,
                                    "data": {
                                        id_key: {
                                            "id": id,
                                            "datetime": datetime,
                                            "last_price": 1.0,
                                            "ask_price1": 1.2,
                                            "bid_price1": 0.8
                                        }
                                    }
                                }
                            }
                        }]
                    })),
                }),
                vec![],
                CommitScope::RealtimeUpdate,
            )
            .expect("seed tick ingest should succeed")
            .expect("seed tick ingest should produce a commit");
    }
}
