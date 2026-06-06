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
    let Some(tick) = source.next_tick().await else {
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
        let Some(tick) = source.next_tick().await else {
            return Ok(frames);
        };
        frames.extend(engine.ingest_tick(tick.symbol, tick.row)?);
    }
}
