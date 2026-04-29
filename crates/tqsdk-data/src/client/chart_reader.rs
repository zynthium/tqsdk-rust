use std::sync::atomic::Ordering;
use std::time::Duration;

use tqsdk_core::{Chart, Kline, MarketCommand, RuntimeCommand, Tick};

use crate::error::{DataError, Result};

use super::{
    KlineDataPage, NEXT_HISTORY_CHART_ID, TickDataPage, contract_error_into_data,
    sanitize_chart_token, MARKET_POLL_BUDGET,
};

pub(super) async fn wait_for_ready_chart(
    session: &tqsdk_session::SessionClient,
    reader: &tqsdk_core::RuntimeReader,
    chart_id: &str,
    command_id: tqsdk_core::CommandId,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if chart_is_ready(reader, chart_id)? {
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

fn chart_is_ready(reader: &tqsdk_core::RuntimeReader, chart_id: &str) -> Result<bool> {
    let snapshot = reader.read();
    let Some(chart) = snapshot
        .decode_path::<Chart>(&["charts", chart_id])
        .map_err(contract_error_into_data)?
    else {
        return Ok(false);
    };
    // For history windows, `more_data` means there are more pages to paginate, not
    // that the current chart snapshot is unreadable.
    Ok(chart.ready)
}

pub(super) fn read_ready_kline_data_page(
    reader: &tqsdk_core::RuntimeReader,
    symbol: &str,
    duration_ns: i64,
    view_width: usize,
    chart_id: &str,
) -> Result<Option<KlineDataPage>> {
    let snapshot = reader.read();
    let Some(chart) = snapshot
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
    let mut ids = snapshot
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
        if let Some(row) = snapshot
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
    let snapshot = reader.read();
    let Some(chart) = snapshot
        .decode_path::<Chart>(&["charts", chart_id])
        .map_err(contract_error_into_data)?
    else {
        return Ok(None);
    };
    if !chart.ready {
        return Ok(None);
    }

    let mut ids = snapshot
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
        if let Some(row) = snapshot
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
    let sequence = NEXT_HISTORY_CHART_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "data-kline-page-{}-{duration_ns}-{sequence}",
        sanitize_chart_token(symbol)
    )
}

pub(super) fn next_tick_page_chart_id(symbol: &str) -> String {
    let sequence = NEXT_HISTORY_CHART_ID.fetch_add(1, Ordering::Relaxed);
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
