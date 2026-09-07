//! Offline, allocation-free trading-time queries on an immutable generation.
use std::time::Duration;
use tqsdk_data::{
    TradingTimeDirection, TradingTimeline, TradingTimelineIdentity, TradingTimelineInterval,
    TradingTimelineKnownRange, TradingTimelineSnapshot,
};

fn main() -> tqsdk_data::Result<()> {
    const SECOND: i64 = 1_000_000_000;
    // Frozen illustrative wall-clock intervals; live code loads a confirmed
    // product snapshot through TradingTimelineStore::open_read_only().load_active().
    let snapshot = TradingTimelineSnapshot::new(
        TradingTimelineIdentity::new(
            "INE",
            "sc",
            "KQ.i@INE.sc",
            "fixture-rule",
            "fixture-evidence",
        )?,
        vec![TradingTimelineKnownRange::new(0, 100 * SECOND)?],
        vec![
            TradingTimelineInterval::new(0, 20 * SECOND)?,
            TradingTimelineInterval::new(80 * SECOND, 100 * SECOND)?,
        ],
    )?;
    let timeline = TradingTimeline::from_snapshot(snapshot)?;
    let end = timeline.shift(
        10 * SECOND,
        Duration::from_secs(20),
        TradingTimeDirection::Forward,
    )?;
    assert_eq!(end, 90 * SECOND);
    assert_eq!(
        timeline.trading_duration_between(10 * SECOND, end)?,
        Duration::from_secs(20)
    );
    // Use [10 * SECOND, end) with the existing history range API. Sparse
    // instruments may return fewer bars: no physical-row expansion is performed.
    Ok(())
}
