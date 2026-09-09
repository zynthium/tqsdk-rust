#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::OnceLock;

use tqsdk_core::Quote;

use crate::protocol::RelayTickRow;

const DEFAULT_MAX_CACHED_SYMBOLS: usize = 512;
const DEFAULT_MAX_RETAINED_BYTES: usize = 512 * 1024 * 1024;
const QUOTE_TEXT_RESERVATION_BYTES: usize = 1024;
const ENTRY_METADATA_RESERVATION_BYTES: usize = 128;

/// Explicit logical memory reservation for the in-memory relay cache.
///
/// The reservation includes every fixed-capacity tick ring, cache keys, quote
/// projection, and a conservative map-entry allowance. It is a hard admission
/// bound, not a process RSS measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketCacheLimits {
    pub max_symbols: usize,
    pub max_retained_bytes: usize,
}

impl Default for MarketCacheLimits {
    fn default() -> Self {
        Self::defaults()
    }
}

impl MarketCacheLimits {
    #[must_use]
    pub const fn defaults() -> Self {
        Self {
            max_symbols: DEFAULT_MAX_CACHED_SYMBOLS,
            max_retained_bytes: DEFAULT_MAX_RETAINED_BYTES,
        }
    }

    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            max_symbols: usize::MAX,
            max_retained_bytes: usize::MAX,
        }
    }

    #[must_use]
    pub fn minimum_retained_bytes(tick_capacity: usize) -> usize {
        MarketCache::entry_reservation(tick_capacity, "", true)
    }
}

/// Result of one cache admission attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketCacheWriteReport {
    pub stored: bool,
    pub evicted_symbols: usize,
    pub retained_bytes: usize,
}

/// Cached quote representation. Tick-originated quotes retain their native
/// timestamp until a compatibility `Quote` is actually observed.
#[derive(Debug, Clone)]
enum CachedQuote {
    Direct(Quote),
    Tick(CachedTickQuote),
}

#[derive(Debug, Clone)]
struct CachedTickQuote {
    row: RelayTickRow,
    formatted_datetime: OnceLock<String>,
    projected: OnceLock<Quote>,
}

impl CachedQuote {
    fn from_tick(row: RelayTickRow) -> Self {
        Self::Tick(CachedTickQuote {
            row,
            formatted_datetime: OnceLock::new(),
            projected: OnceLock::new(),
        })
    }

    fn from_tick_with_datetime(row: RelayTickRow, formatted_datetime: OnceLock<String>) -> Self {
        Self::Tick(CachedTickQuote {
            row,
            formatted_datetime,
            projected: OnceLock::new(),
        })
    }

    fn quote(&self, symbol: &str) -> &Quote {
        match self {
            Self::Direct(quote) => quote,
            Self::Tick(tick) => {
                let datetime = tick
                    .formatted_datetime
                    .get_or_init(|| quote_datetime_from_tick_ns(tick.row.datetime));
                tick.projected
                    .get_or_init(|| project_quote(symbol, &tick.row, datetime.clone()))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketCache {
    tick_capacity: usize,
    kline_capacity: usize,
    limits: MarketCacheLimits,
    ticks: HashMap<String, VecDeque<RelayTickRow>>,
    quotes: HashMap<String, CachedQuote>,
    lru: VecDeque<String>,
    retained_bytes: usize,
}

impl MarketCache {
    #[must_use]
    pub fn new(tick_capacity: usize, kline_capacity: usize) -> Self {
        Self::with_limits(
            tick_capacity,
            kline_capacity,
            MarketCacheLimits::unbounded(),
        )
    }

    #[must_use]
    pub fn with_limits(
        tick_capacity: usize,
        kline_capacity: usize,
        limits: MarketCacheLimits,
    ) -> Self {
        assert!(tick_capacity > 0, "tick_capacity must be greater than zero");
        assert!(
            kline_capacity > 0,
            "kline_capacity must be greater than zero"
        );
        assert!(
            limits.max_symbols > 0,
            "max_symbols must be greater than zero"
        );
        assert!(
            limits.max_retained_bytes > 0,
            "max_retained_bytes must be greater than zero"
        );
        assert!(
            MarketCacheLimits::minimum_retained_bytes(tick_capacity) <= limits.max_retained_bytes,
            "max_retained_bytes must fit one tick ring"
        );
        Self {
            tick_capacity,
            kline_capacity,
            limits,
            ticks: HashMap::new(),
            quotes: HashMap::new(),
            lru: VecDeque::new(),
            retained_bytes: 0,
        }
    }

    pub fn push_tick(
        &mut self,
        symbol: impl Into<String>,
        row: RelayTickRow,
    ) -> MarketCacheWriteReport {
        let symbol = symbol.into();
        let report = self.admit(&symbol, true);
        if !report.stored {
            return report;
        }
        let tick_capacity = self.tick_capacity;
        let ring = self
            .ticks
            .entry(symbol.clone())
            .or_insert_with(|| VecDeque::with_capacity(tick_capacity));
        ring.push_back(row.clone());
        while ring.len() > self.tick_capacity {
            ring.pop_front();
        }
        let cached_quote = match self.quotes.remove(&symbol) {
            Some(CachedQuote::Tick(previous)) if previous.row.datetime == row.datetime => {
                CachedQuote::from_tick_with_datetime(row, previous.formatted_datetime)
            }
            _ => CachedQuote::from_tick(row),
        };
        self.quotes.insert(symbol, cached_quote);
        report
    }

    pub fn push_quote(
        &mut self,
        symbol: impl Into<String>,
        quote: Quote,
    ) -> MarketCacheWriteReport {
        let symbol = symbol.into();
        let quote = relay_quote_projection(&symbol, &quote);
        let report = self.admit(&symbol, self.ticks.contains_key(&symbol));
        if !report.stored {
            return report;
        }
        self.quotes.insert(symbol, CachedQuote::Direct(quote));
        report
    }

    #[must_use]
    pub fn ticks(&self, symbol: &str) -> Vec<RelayTickRow> {
        self.ticks
            .get(symbol)
            .map(|rows| rows.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn tick_ring(&self, symbol: &str) -> Option<&VecDeque<RelayTickRow>> {
        self.ticks.get(symbol)
    }

    #[must_use]
    pub fn quote_ref(&self, symbol: &str) -> Option<&Quote> {
        self.quotes.get(symbol).map(|quote| quote.quote(symbol))
    }

    /// Compatibility snapshot helper. Hot paths should use [`Self::quote_ref`].
    #[must_use]
    pub fn quote(&self, symbol: &str) -> Option<Quote> {
        self.quote_ref(symbol).cloned()
    }

    #[must_use]
    pub fn kline_capacity(&self) -> usize {
        self.kline_capacity
    }

    #[must_use]
    pub fn limits(&self) -> MarketCacheLimits {
        self.limits
    }

    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    #[must_use]
    pub fn cached_symbols(&self) -> usize {
        self.lru.len()
    }

    fn admit(&mut self, symbol: &str, has_tick: bool) -> MarketCacheWriteReport {
        let existing = self.lru.iter().any(|cached| cached == symbol);
        let previous_reservation = if existing {
            self.entry_reservation_for(symbol, self.ticks.contains_key(symbol))
        } else {
            0
        };
        let next_reservation = self.entry_reservation_for(symbol, has_tick);
        if next_reservation > self.limits.max_retained_bytes {
            return MarketCacheWriteReport {
                stored: false,
                evicted_symbols: 0,
                retained_bytes: self.retained_bytes,
            };
        }

        self.lru.retain(|cached| cached != symbol);
        let mut evicted_symbols = 0;
        while self.lru.len().saturating_add(usize::from(!existing)) > self.limits.max_symbols
            || self
                .retained_bytes
                .saturating_sub(previous_reservation)
                .saturating_add(next_reservation)
                > self.limits.max_retained_bytes
        {
            let Some(victim) = self.lru.pop_front() else {
                self.lru.push_back(symbol.to_owned());
                return MarketCacheWriteReport {
                    stored: false,
                    evicted_symbols,
                    retained_bytes: self.retained_bytes,
                };
            };
            self.evict_symbol(&victim);
            evicted_symbols = evicted_symbols.saturating_add(1);
        }

        self.retained_bytes = self
            .retained_bytes
            .saturating_sub(previous_reservation)
            .saturating_add(next_reservation);
        self.lru.push_back(symbol.to_owned());
        MarketCacheWriteReport {
            stored: true,
            evicted_symbols,
            retained_bytes: self.retained_bytes,
        }
    }

    fn evict_symbol(&mut self, symbol: &str) {
        let has_tick = self.ticks.contains_key(symbol);
        if !has_tick && !self.quotes.contains_key(symbol) {
            return;
        }
        self.retained_bytes = self
            .retained_bytes
            .saturating_sub(self.entry_reservation_for(symbol, has_tick));
        self.ticks.remove(symbol);
        self.quotes.remove(symbol);
    }

    fn entry_reservation_for(&self, symbol: &str, has_tick: bool) -> usize {
        Self::entry_reservation(self.tick_capacity, symbol, has_tick)
    }

    fn entry_reservation(tick_capacity: usize, symbol: &str, has_tick: bool) -> usize {
        let map_entries = 2_usize.saturating_add(usize::from(has_tick));
        let key_bytes = size_of::<String>().saturating_add(symbol.len());
        let metadata =
            map_entries.saturating_mul(key_bytes.saturating_add(ENTRY_METADATA_RESERVATION_BYTES));
        let quote =
            size_of::<CachedQuote>().saturating_add(QUOTE_TEXT_RESERVATION_BYTES.saturating_mul(2));
        let ring = has_tick.then(|| {
            size_of::<VecDeque<RelayTickRow>>()
                .saturating_add(tick_capacity.saturating_mul(size_of::<RelayTickRow>()))
        });
        metadata
            .saturating_add(quote)
            .saturating_add(ring.unwrap_or_default())
    }
}

fn project_quote(symbol: &str, row: &RelayTickRow, datetime: String) -> Quote {
    Quote {
        instrument_id: symbol.to_string(),
        last_price: row.last_price,
        volume: row.volume,
        open_interest: row.open_interest,
        datetime,
        ..Quote::default()
    }
}

fn relay_quote_projection(symbol: &str, quote: &Quote) -> Quote {
    Quote {
        datetime: bounded_text(&quote.datetime, ""),
        instrument_id: bounded_text(&quote.instrument_id, symbol),
        last_price: quote.last_price,
        volume: quote.volume,
        open_interest: quote.open_interest,
        ..Quote::default()
    }
}

fn bounded_text(value: &str, fallback: &str) -> String {
    if value.len() <= QUOTE_TEXT_RESERVATION_BYTES {
        return value.to_owned();
    }
    if fallback.len() <= QUOTE_TEXT_RESERVATION_BYTES {
        return fallback.to_owned();
    }
    fallback
        .char_indices()
        .take_while(|(index, character)| {
            index.saturating_add(character.len_utf8()) <= QUOTE_TEXT_RESERVATION_BYTES
        })
        .map(|(_, character)| character)
        .collect()
}

fn quote_datetime_from_tick_ns(datetime_ns: i64) -> String {
    let secs = datetime_ns.div_euclid(1_000_000_000);
    let nanos = datetime_ns.rem_euclid(1_000_000_000) as u32;
    let Some(utc) = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos) else {
        return datetime_ns.to_string();
    };
    let Some(china_offset) = chrono::FixedOffset::east_opt(8 * 3600) else {
        return datetime_ns.to_string();
    };
    utc.with_timezone(&china_offset)
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_quote_projection_is_lazy_until_read() {
        let mut cache = MarketCache::new(4, 16);
        cache.push_tick(
            "SHFE.au2602",
            RelayTickRow {
                id: 1,
                datetime: 1_713_660_000_000_000_000,
                last_price: 610.0,
                volume: 10,
                open_interest: 100,
            },
        );

        assert!(matches!(
            cache.quotes.get("SHFE.au2602"),
            Some(CachedQuote::Tick(tick)) if tick.projected.get().is_none()
        ));

        let quote = cache.quote_ref("SHFE.au2602").expect("cached quote");
        assert_eq!(quote.instrument_id, "SHFE.au2602");
        assert_eq!(quote.last_price, 610.0);

        assert!(matches!(
            cache.quotes.get("SHFE.au2602"),
            Some(CachedQuote::Tick(tick)) if tick.projected.get().is_some()
        ));

        cache.push_tick(
            "SHFE.au2602",
            RelayTickRow {
                id: 2,
                datetime: 1_713_660_000_000_000_000,
                last_price: 611.0,
                volume: 11,
                open_interest: 101,
            },
        );
        assert!(matches!(
            cache.quotes.get("SHFE.au2602"),
            Some(CachedQuote::Tick(tick))
                if tick.formatted_datetime.get().is_some() && tick.projected.get().is_none()
        ));
        assert_eq!(
            cache
                .quote_ref("SHFE.au2602")
                .expect("updated quote")
                .last_price,
            611.0
        );
    }
}
