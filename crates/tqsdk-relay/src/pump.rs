#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::engine::{DownstreamFrame, RelayEngine};
use crate::error::RelayResult;
use crate::upstream::{UpstreamMarketEvent, UpstreamTickSource};

pub async fn pump_once<S>(
    engine: &mut RelayEngine,
    source: &mut S,
) -> RelayResult<Vec<DownstreamFrame>>
where
    S: UpstreamTickSource + Send,
{
    let event = source.next_event().await;
    record_upstream_source_diagnostics(engine, source);
    let Some(event) = event else {
        return Ok(Vec::new());
    };
    ingest_event(engine, event)
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
        let event = source.next_event().await;
        record_upstream_source_diagnostics(engine, source);
        let Some(event) = event else {
            return Ok(frames);
        };
        frames.extend(ingest_event(engine, event)?);
    }
}

fn ingest_event(
    engine: &mut RelayEngine,
    event: UpstreamMarketEvent,
) -> RelayResult<Vec<DownstreamFrame>> {
    match event {
        UpstreamMarketEvent::Tick(tick) => engine.ingest_tick(tick.symbol, tick.row),
        UpstreamMarketEvent::Quote(quote) => engine.ingest_quote(quote.symbol, quote.quote),
    }
}

fn record_upstream_source_diagnostics<S>(engine: &mut RelayEngine, source: &mut S)
where
    S: UpstreamTickSource,
{
    let invalid_rows = source.take_invalid_tick_rows();
    let last_error = source.take_last_invalid_tick_row_error();
    engine.record_upstream_invalid_tick_rows(invalid_rows, last_error);
}
