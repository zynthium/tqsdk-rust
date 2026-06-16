#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::engine::{DownstreamFrame, RelayEngine};
use crate::error::RelayResult;
use crate::upstream::{UpstreamMarketEvent, UpstreamSourceUpdate, UpstreamTickSource};

pub async fn pump_once<S>(
    engine: &mut RelayEngine,
    source: &mut S,
) -> RelayResult<Vec<DownstreamFrame>>
where
    S: UpstreamTickSource + Send,
{
    let update = source.next_update().await;
    record_upstream_source_diagnostics(engine, source);
    let Some(update) = update else {
        return Ok(Vec::new());
    };
    ingest_update(engine, update)
}

pub async fn pump_available<S>(
    engine: &mut RelayEngine,
    source: &mut S,
) -> RelayResult<Vec<DownstreamFrame>>
where
    S: UpstreamTickSource + Send,
{
    let mut frames = Vec::new();
    loop {
        let update = source.next_update().await;
        record_upstream_source_diagnostics(engine, source);
        let Some(update) = update else {
            return Ok(frames);
        };
        frames.extend(ingest_update(engine, update)?);
    }
}

fn ingest_update(
    engine: &mut RelayEngine,
    update: UpstreamSourceUpdate,
) -> RelayResult<Vec<DownstreamFrame>> {
    match update {
        UpstreamSourceUpdate::Event(event) => ingest_event(engine, event),
        UpstreamSourceUpdate::Progress => Ok(Vec::new()),
    }
}

fn ingest_event(
    engine: &mut RelayEngine,
    event: UpstreamMarketEvent,
) -> RelayResult<Vec<DownstreamFrame>> {
    match event {
        UpstreamMarketEvent::Tick(tick) => engine.ingest_tick(tick.symbol, tick.row),
        UpstreamMarketEvent::Quote(quote) => engine.ingest_quote(quote.symbol, quote.quote),
        UpstreamMarketEvent::TradingStatus(status) => {
            let status = *status;
            engine.ingest_trading_status(status.symbol, status.trading_status)
        }
    }
}

fn record_upstream_source_diagnostics<S>(engine: &mut RelayEngine, source: &mut S)
where
    S: UpstreamTickSource,
{
    let progress = source.take_progress();
    let invalid_rows = source.take_invalid_tick_rows();
    let invalid_rows_by_symbol = source.take_invalid_tick_rows_by_symbol();
    let last_error = source.take_last_invalid_tick_row_error();
    engine.record_upstream_progress(progress);
    engine.record_upstream_invalid_tick_rows_by_symbol(
        invalid_rows,
        invalid_rows_by_symbol,
        last_error,
    );
}
