#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{HashMap, VecDeque};

use tqsdk_core::Quote;

use crate::protocol::RelayTickRow;

#[derive(Debug, Clone)]
pub struct MarketCache {
    tick_capacity: usize,
    kline_capacity: usize,
    ticks: HashMap<String, VecDeque<RelayTickRow>>,
    quotes: HashMap<String, Quote>,
}

impl MarketCache {
    #[must_use]
    pub fn new(tick_capacity: usize, kline_capacity: usize) -> Self {
        assert!(tick_capacity > 0, "tick_capacity must be greater than zero");
        assert!(
            kline_capacity > 0,
            "kline_capacity must be greater than zero"
        );
        Self {
            tick_capacity,
            kline_capacity,
            ticks: HashMap::new(),
            quotes: HashMap::new(),
        }
    }

    pub fn push_tick(&mut self, symbol: impl Into<String>, row: RelayTickRow) {
        let symbol = symbol.into();
        let quote = project_quote(&symbol, &row);
        let ring = self.ticks.entry(symbol.clone()).or_default();
        ring.push_back(row.clone());
        while ring.len() > self.tick_capacity {
            ring.pop_front();
        }
        self.quotes.insert(symbol, quote);
    }

    pub fn push_quote(&mut self, symbol: impl Into<String>, mut quote: Quote) {
        let symbol = symbol.into();
        if quote.instrument_id.is_empty() {
            quote.instrument_id = symbol.clone();
        }
        self.quotes.insert(symbol, quote);
    }

    #[must_use]
    pub fn ticks(&self, symbol: &str) -> Vec<RelayTickRow> {
        self.ticks
            .get(symbol)
            .map(|rows| rows.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn quote(&self, symbol: &str) -> Option<Quote> {
        self.quotes.get(symbol).cloned()
    }

    #[must_use]
    pub fn kline_capacity(&self) -> usize {
        self.kline_capacity
    }
}

fn project_quote(symbol: &str, row: &RelayTickRow) -> Quote {
    Quote {
        instrument_id: symbol.to_string(),
        last_price: row.last_price,
        volume: row.volume,
        open_interest: row.open_interest,
        datetime: row.datetime.to_string(),
        ..Quote::default()
    }
}
