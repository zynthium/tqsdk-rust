#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;

use crate::error::{RelayError, RelayResult};
use crate::protocol::RelayTickRow;

#[derive(Debug, Clone, PartialEq)]
pub struct UpstreamTick {
    pub symbol: String,
    pub row: RelayTickRow,
}

pub trait UpstreamTickSource {
    fn next_tick(&mut self) -> impl std::future::Future<Output = Option<UpstreamTick>> + Send + '_;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamTickChart {
    chart_id: String,
    symbols: Vec<String>,
    view_width: usize,
}

impl UpstreamTickChart {
    pub fn new<I, S>(
        chart_id: impl Into<String>,
        symbols: I,
        view_width: usize,
    ) -> RelayResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let chart_id = chart_id.into();
        if chart_id.trim().is_empty() {
            return Err(RelayError::invalid_config(
                "upstream tick chart_id must not be empty",
            ));
        }
        if view_width == 0 {
            return Err(RelayError::invalid_config(
                "upstream tick view_width must be greater than zero",
            ));
        }
        let mut symbols: Vec<String> = symbols
            .into_iter()
            .map(|symbol| symbol.as_ref().trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return Err(RelayError::invalid_config(
                "upstream tick chart requires at least one symbol",
            ));
        }
        Ok(Self {
            chart_id,
            symbols,
            view_width,
        })
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    #[must_use]
    pub const fn duration_ns(&self) -> i64 {
        0
    }

    #[must_use]
    pub const fn view_width(&self) -> usize {
        self.view_width
    }
}

#[derive(Debug, Default)]
pub struct FakeUpstreamTickSource {
    ticks: VecDeque<UpstreamTick>,
}

impl FakeUpstreamTickSource {
    pub fn push(&mut self, tick: UpstreamTick) {
        self.ticks.push_back(tick);
    }
}

impl UpstreamTickSource for FakeUpstreamTickSource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        self.ticks.pop_front()
    }
}
