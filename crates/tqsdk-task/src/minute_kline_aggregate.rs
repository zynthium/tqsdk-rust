//! Compatibility re-exports for the shared data-layer aggregation core.

pub use tqsdk_data::{
    CANONICAL_MINUTE_KLINE_NS, KlineSessionTemplate as MinuteKlineSessionTemplate,
    KlineSessionWindow as MinuteKlineSessionWindow, MinuteKlineAggregationUpdate,
    MinuteKlineAggregator,
};
