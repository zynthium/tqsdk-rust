#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::BTreeMap;

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
}

impl BacktestPump {
    pub(crate) fn new() -> Self {
        Self::default()
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

        let internal_chart_id = self
            .request_tick_page(
                session,
                symbol,
                TickPageRequest::Focus(backtest.start_datetime_ns()),
            )
            .await?;
        let serial = BacktestTickSerial {
            symbol: symbol.to_string(),
            view_width,
            user_chart_id: chart_id.to_string(),
            awaiting_page: true,
            current_page_left_id: None,
            current_page_right_id: None,
            current_page_more_data: false,
            next_emit_id: None,
            first_emitted_id: None,
            last_emitted_id: None,
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
                    view_width,
                    row_id,
                    row,
                    datetime,
                    first_visible_id,
                } => {
                    return synthesize_tick_commit(
                        session,
                        &symbol,
                        &user_chart_id,
                        view_width,
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
                    let internal_chart_id = self
                        .request_tick_page(session, &symbol, TickPageRequest::LeftId(left_id))
                        .await?;
                    if let Some(serial) = self.tick_serials.get_mut(&user_chart_id) {
                        serial.awaiting_page = true;
                    }
                    self.internal_tick_charts
                        .insert(internal_chart_id, user_chart_id);
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
        let Some(internal_chart_id) = self.touched_internal_tick_chart(&commit) else {
            return Ok(BacktestCommitAction::Expose(commit));
        };
        let Some(user_chart_id) = self.internal_tick_charts.get(&internal_chart_id).cloned() else {
            return Ok(BacktestCommitAction::Expose(commit));
        };

        if let Some(serial) = self.tick_serials.get_mut(&user_chart_id) {
            serial.load_page(reader, &internal_chart_id)?;
        }

        if let Some(commit) = self.emit_pending_tick(session, reader, backtest).await? {
            return Ok(BacktestCommitAction::Synthetic(commit));
        }

        Ok(BacktestCommitAction::Suppressed)
    }

    fn touched_internal_tick_chart(&self, commit: &tqsdk_core::CommitResult) -> Option<String> {
        for object in &commit.changes.object_hits {
            if let ObjectKey::Chart { chart_id } = object {
                let chart_id = chart_id.as_str();
                if self.internal_tick_charts.contains_key(chart_id) {
                    return Some(chart_id.to_string());
                }
            }
        }

        for path in &commit.changes.path_hits {
            let segments = path.segments();
            if let [root, chart_id, ..] = segments
                && root == "charts"
                && self.internal_tick_charts.contains_key(chart_id)
            {
                return Some(chart_id.clone());
            }
        }

        None
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
            TickPageRequest::Focus(datetime_ns) => {
                command.focus_datetime_ns = Some(datetime_ns);
                command.focus_position = Some(BACKTEST_PAGE_VIEW_WIDTH);
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
}

#[derive(Debug, Clone, Copy)]
enum TickPageRequest {
    Focus(i64),
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
    current_page_more_data: bool,
    next_emit_id: Option<i64>,
    first_emitted_id: Option<i64>,
    last_emitted_id: Option<i64>,
    exhausted: bool,
}

impl BacktestTickSerial {
    fn load_page(&mut self, reader: &RuntimeReader, internal_chart_id: &str) -> Result<()> {
        let market = reader.read_market_state();
        let Some(chart) = market.decode_path::<Chart>(&["charts", internal_chart_id])? else {
            return Ok(());
        };
        if !chart.ready {
            return Ok(());
        }

        self.awaiting_page = false;
        self.current_page_left_id = Some(chart.left_id);
        self.current_page_right_id = Some(chart.right_id);
        self.current_page_more_data = chart.more_data;

        if chart.left_id <= chart.right_id {
            let next_after_last = self
                .last_emitted_id
                .and_then(|id| id.checked_add(1))
                .unwrap_or(chart.left_id);
            self.next_emit_id = Some(next_after_last.max(chart.left_id));
        } else if chart.more_data {
            self.next_emit_id = None;
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

            return Ok(TickPumpDecision::Emit {
                symbol: self.symbol.clone(),
                user_chart_id: self.user_chart_id.clone(),
                view_width: self.view_width,
                row_id: id,
                row,
                datetime,
                first_visible_id,
            });
        }

        self.next_page_or_finish()
    }

    fn next_page_or_finish(&mut self) -> Result<TickPumpDecision> {
        if self.current_page_more_data
            && let Some(right_id) = self.current_page_right_id
            && let Some(left_id) = right_id.checked_add(1)
        {
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

#[derive(Debug)]
enum TickPumpDecision {
    Emit {
        symbol: String,
        user_chart_id: String,
        view_width: usize,
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
    _view_width: usize,
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
