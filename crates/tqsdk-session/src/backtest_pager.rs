//! Wire pagination shared by the history source and wait-style backtest.
use tqsdk_core::{Chart, MarketChartCommand, Symbol};

/// Page width used by the official Python backtest generator.
pub const BACKTEST_PAGE_WIDTH: usize = 8_964;

/// Two alternating chart windows, matching Python's `_gen_serial` requests.
///
/// This is protocol state only: consumption, cache ranges and persistence remain
/// with the caller. Advancing does not imply that a row was consumed.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct BacktestChartPager {
    ids: [String; 2],
    slot: usize,
    ins_list: String,
    command: MarketChartCommand,
}

impl BacktestChartPager {
    pub fn new(chart_id: String, symbols: Vec<Symbol>, duration_ns: i64, start_ns: i64) -> Self {
        let ins_list = symbols
            .iter()
            .map(Symbol::as_str)
            .collect::<Vec<_>>()
            .join(",");
        Self {
            ins_list,
            ids: [chart_id.clone(), format!("{chart_id}--backtest-b")],
            slot: 0,
            command: MarketChartCommand {
                chart_id,
                symbols,
                duration_ns,
                view_width: BACKTEST_PAGE_WIDTH,
                left_kline_id: None,
                focus_datetime_ns: Some(start_ns),
                focus_position: Some(BACKTEST_PAGE_WIDTH),
            },
        }
    }

    pub fn command(&self) -> &MarketChartCommand {
        &self.command
    }

    pub fn chart_ids(&self) -> &[String; 2] {
        &self.ids
    }

    pub fn advance(&mut self, right_id: i64) {
        self.slot ^= 1;
        self.command.chart_id.clone_from(&self.ids[self.slot]);
        self.command.left_kline_id = Some(right_id);
        self.command.focus_datetime_ns = None;
        self.command.focus_position = None;
    }

    /// Reject delayed responses for an earlier use of the same chart ID.
    pub fn matches(&self, chart: &Chart) -> bool {
        use serde_json::Value;
        let command = &self.command;
        chart.state.get("ins_list").and_then(Value::as_str) == Some(self.ins_list.as_str())
            && chart.state.get("duration").and_then(Value::as_i64) == Some(command.duration_ns)
            && chart.state.get("view_width").and_then(Value::as_u64)
                == Some(BACKTEST_PAGE_WIDTH as u64)
            && match command.left_kline_id {
                Some(left) => {
                    chart.state.get("left_kline_id").and_then(Value::as_i64) == Some(left)
                }
                None => {
                    chart.state.get("focus_datetime").and_then(Value::as_i64)
                        == command.focus_datetime_ns
                        && chart.state.get("focus_position").and_then(Value::as_u64)
                            == Some(BACKTEST_PAGE_WIDTH as u64)
                }
            }
    }
}
