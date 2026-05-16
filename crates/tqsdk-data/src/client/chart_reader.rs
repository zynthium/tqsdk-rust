use std::time::Duration;

use tqsdk_core::{Chart, Kline, MarketChartCommand, MarketCommand, RuntimeCommand, Tick};

use crate::error::{DataError, Result};

use super::chart_ids::{next_history_chart_sequence, sanitize_chart_token};
use super::{KlineDataPage, MARKET_POLL_BUDGET, TickDataPage, contract_error_into_data};

pub(super) async fn wait_for_ready_chart(
    session: &tqsdk_session::SessionClient,
    reader: &tqsdk_core::RuntimeReader,
    chart_id: &str,
    expected: &ExpectedChartState,
    command_id: tqsdk_core::CommandId,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if chart_is_ready(reader, chart_id, expected)? {
            return Ok(());
        }

        if let Some(status) = session.command_status(command_id)?
            && matches!(status.as_str(), "rejected" | "failed" | "cancelled")
        {
            return Err(DataError::InvalidResponse(format!(
                "set chart command reached terminal status {status}"
            )));
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(DataError::Timeout(timeout));
        }

        let mut progress = false;
        progress |= session.flush_outbound().await?;
        progress |= session.drive_pending_once().await?;
        progress |= session
            .drive_route_once(Some(
                (tokio::time::Instant::now() + MARKET_POLL_BUDGET).min(deadline),
            ))
            .await?;

        if progress {
            continue;
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(DataError::Timeout(timeout));
        }

        tokio::time::sleep(remaining.min(Duration::from_millis(1))).await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExpectedChartState {
    ins_list: String,
    duration_ns: i64,
    view_width: usize,
    left_kline_id: Option<i64>,
    focus_datetime_ns: Option<i64>,
    focus_position: Option<usize>,
}

impl ExpectedChartState {
    pub(super) fn from_command(command: &MarketChartCommand) -> Self {
        Self {
            ins_list: command
                .symbols
                .iter()
                .map(|symbol| symbol.as_str())
                .collect::<Vec<_>>()
                .join(","),
            duration_ns: command.duration_ns,
            view_width: command.view_width,
            left_kline_id: command.left_kline_id,
            focus_datetime_ns: command.focus_datetime_ns,
            focus_position: command.focus_position,
        }
    }
}

fn chart_is_ready(
    reader: &tqsdk_core::RuntimeReader,
    chart_id: &str,
    expected: &ExpectedChartState,
) -> Result<bool> {
    let market = reader.read_market_state();
    let Some(chart) = market
        .decode_path::<Chart>(&["charts", chart_id])
        .map_err(contract_error_into_data)?
    else {
        return Ok(false);
    };
    // For history windows, `more_data` means there are more pages to paginate, not
    // that the current chart snapshot is unreadable.
    Ok(chart.ready && chart_state_matches(&chart, expected))
}

fn chart_state_matches(chart: &Chart, expected: &ExpectedChartState) -> bool {
    state_str(chart, "ins_list") == Some(expected.ins_list.as_str())
        && state_i64(chart, "duration") == Some(expected.duration_ns)
        && state_usize(chart, "view_width") == Some(expected.view_width)
        && match expected.left_kline_id {
            Some(value) => state_i64(chart, "left_kline_id") == Some(value),
            None => true,
        }
        && match (expected.focus_datetime_ns, expected.focus_position) {
            (Some(datetime), Some(position)) => {
                state_i64(chart, "focus_datetime") == Some(datetime)
                    && state_usize(chart, "focus_position") == Some(position)
            }
            _ => true,
        }
}

fn state_str<'a>(chart: &'a Chart, key: &str) -> Option<&'a str> {
    chart.state.get(key).and_then(serde_json::Value::as_str)
}

fn state_i64(chart: &Chart, key: &str) -> Option<i64> {
    chart.state.get(key).and_then(serde_json::Value::as_i64)
}

fn state_usize(chart: &Chart, key: &str) -> Option<usize> {
    chart
        .state
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

pub(super) fn read_ready_kline_data_page(
    reader: &tqsdk_core::RuntimeReader,
    symbol: &str,
    duration_ns: i64,
    view_width: usize,
    chart_id: &str,
) -> Result<Option<KlineDataPage>> {
    let market = reader.read_market_state();
    let Some(chart) = market
        .decode_path::<Chart>(&["charts", chart_id])
        .map_err(contract_error_into_data)?
    else {
        return Ok(None);
    };
    if !chart.ready {
        return Ok(None);
    }

    let duration_key = duration_ns.to_string();
    let data_path = ["klines", symbol, duration_key.as_str(), "data"];
    let mut ids = market
        .get_path(&data_path)
        .and_then(|value| value.as_object())
        .map(|data| {
            data.keys()
                .filter_map(|key| key.parse::<i64>().ok())
                .filter(|id| chart.left_id <= *id && *id <= chart.right_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ids.sort_unstable();
    if ids.len() > view_width {
        ids.drain(0..ids.len() - view_width);
    }

    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let id_key = id.to_string();
        if let Some(row) = market
            .decode_path::<Kline>(&[
                "klines",
                symbol,
                duration_key.as_str(),
                "data",
                id_key.as_str(),
            ])
            .map_err(contract_error_into_data)?
        {
            rows.push(row);
        }
    }

    Ok(Some(KlineDataPage::new(
        symbol.to_string(),
        duration_ns,
        view_width,
        chart.left_id,
        chart.right_id,
        chart.more_data,
        rows,
    )))
}

pub(super) fn read_ready_tick_data_page(
    reader: &tqsdk_core::RuntimeReader,
    symbol: &str,
    view_width: usize,
    chart_id: &str,
) -> Result<Option<TickDataPage>> {
    let market = reader.read_market_state();
    let Some(chart) = market
        .decode_path::<Chart>(&["charts", chart_id])
        .map_err(contract_error_into_data)?
    else {
        return Ok(None);
    };
    if !chart.ready {
        return Ok(None);
    }

    let mut ids = market
        .get_path(&["ticks", symbol, "data"])
        .and_then(|value| value.as_object())
        .map(|data| {
            data.keys()
                .filter_map(|key| key.parse::<i64>().ok())
                .filter(|id| chart.left_id <= *id && *id <= chart.right_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ids.sort_unstable();
    if ids.len() > view_width {
        ids.drain(0..ids.len() - view_width);
    }

    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        let id_key = id.to_string();
        if let Some(row) = market
            .decode_path::<Tick>(&["ticks", symbol, "data", id_key.as_str()])
            .map_err(contract_error_into_data)?
        {
            rows.push(row);
        }
    }

    Ok(Some(TickDataPage::new(
        symbol.to_string(),
        view_width,
        chart.left_id,
        chart.right_id,
        chart.more_data,
        rows,
    )))
}

pub(super) fn next_kline_page_chart_id(symbol: &str, duration_ns: i64) -> String {
    let sequence = next_history_chart_sequence();
    format!(
        "data-kline-page-{}-{duration_ns}-{sequence}",
        sanitize_chart_token(symbol)
    )
}

pub(super) fn next_tick_page_chart_id(symbol: &str) -> String {
    let sequence = next_history_chart_sequence();
    format!("data-tick-page-{}-{sequence}", sanitize_chart_token(symbol))
}

pub(super) async fn cancel_chart_best_effort(
    session: &tqsdk_session::SessionClient,
    chart_id: String,
) {
    let _ = session
        .submit(RuntimeCommand::Market(MarketCommand::CancelChart {
            chart_id,
        }))
        .await;
}
