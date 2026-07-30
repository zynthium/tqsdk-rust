//! Shared session-aware Kline aggregation for cache-backed queries and replay.

mod minute;
mod session;
mod tick;

pub use minute::{MinuteKlineAggregationUpdate, MinuteKlineAggregator};
pub use session::{KlineSessionPosition, KlineSessionTemplate, KlineSessionWindow};
pub use tick::{TickKlineAggregationUpdate, TickKlineAggregator};

/// The sole durable Kline period used by the backtest cache.
pub const CANONICAL_MINUTE_KLINE_NS: i64 = crate::MINUTE_KLINE_DURATION_NS;
