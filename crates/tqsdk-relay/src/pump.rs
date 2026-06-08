#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::engine::{DownstreamFrame, RelayEngine};
use crate::error::RelayResult;
use crate::upstream::UpstreamTickSource;

pub async fn pump_once<S>(
    engine: &mut RelayEngine,
    source: &mut S,
) -> RelayResult<Vec<DownstreamFrame>>
where
    S: UpstreamTickSource,
{
    let tick = source.next_tick().await;
    record_upstream_source_diagnostics(engine, source);
    let Some(tick) = tick else {
        return Ok(Vec::new());
    };
    engine.ingest_tick(tick.symbol, tick.row)
}

pub async fn pump_available<S>(
    engine: &mut RelayEngine,
    source: &mut S,
) -> RelayResult<Vec<DownstreamFrame>>
where
    S: UpstreamTickSource,
{
    let mut frames = Vec::new();
    loop {
        let tick = source.next_tick().await;
        record_upstream_source_diagnostics(engine, source);
        let Some(tick) = tick else {
            return Ok(frames);
        };
        frames.extend(engine.ingest_tick(tick.symbol, tick.row)?);
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
