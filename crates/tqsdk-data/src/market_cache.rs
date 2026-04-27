#![cfg_attr(not(test), forbid(unsafe_code))]

use serde::{Deserialize, Serialize};
use tqsdk_core::{Kline, Quote, Tick};

use crate::{DataError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data")]
pub enum MarketCachePayload {
    Quote(Box<Quote>),
    Kline { duration_ns: i64, row: Kline },
    Tick(Tick),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCacheEvent {
    pub source: String,
    pub symbol: String,
    pub received_at_ns: i64,
    pub exchange_time_ns: Option<i64>,
    pub payload: MarketCachePayload,
}

impl MarketCacheEvent {
    pub fn quote(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        quote: Quote,
    ) -> Result<Self> {
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Quote(Box::new(quote)),
        )
    }

    pub fn kline(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        duration_ns: i64,
        row: Kline,
    ) -> Result<Self> {
        if duration_ns <= 0 {
            return Err(DataError::Validation(
                "kline duration must be positive".into(),
            ));
        }
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Kline { duration_ns, row },
        )
    }

    pub fn tick(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        tick: Tick,
    ) -> Result<Self> {
        Self::new(
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            MarketCachePayload::Tick(tick),
        )
    }

    fn new(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        exchange_time_ns: Option<i64>,
        payload: MarketCachePayload,
    ) -> Result<Self> {
        let source = source.into();
        let symbol = symbol.into();
        if source.trim().is_empty() {
            return Err(DataError::Validation(
                "cache event source must not be empty".into(),
            ));
        }
        if symbol.trim().is_empty() {
            return Err(DataError::Validation(
                "cache event symbol must not be empty".into(),
            ));
        }
        if received_at_ns < 0 {
            return Err(DataError::Validation(
                "received_at_ns must be non-negative".into(),
            ));
        }
        if exchange_time_ns.is_some_and(|time| time < 0) {
            return Err(DataError::Validation(
                "exchange_time_ns must be non-negative".into(),
            ));
        }
        Ok(Self {
            source,
            symbol,
            received_at_ns,
            exchange_time_ns,
            payload,
        })
    }

    #[must_use]
    pub fn event_time_ns(&self) -> i64 {
        self.exchange_time_ns.unwrap_or(self.received_at_ns)
    }
}
