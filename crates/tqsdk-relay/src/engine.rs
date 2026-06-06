#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{Value, json};

use crate::bootstrap::{BootstrapQueue, BootstrapRequest};
use crate::cache::MarketCache;
use crate::error::RelayResult;
use crate::interest::{ClientId, InterestRegistry, SourceKey};
use crate::kline::KlineSynthesis;
use crate::observability::{HealthSnapshot, MetricsSnapshot, RelaySourceStatus};
use crate::protocol::{DownstreamCommand, RelayMarketFrame, RelayTickRow};

#[derive(Debug, Clone, PartialEq)]
pub struct DownstreamFrame {
    pub client_id: ClientId,
    pub payload: Value,
}

#[derive(Debug)]
pub struct RelayEngine {
    cache: MarketCache,
    interests: InterestRegistry,
    bootstrap: BootstrapQueue,
    klines: HashMap<SourceKey, KlineSynthesis>,
    upstream_status: RelaySourceStatus,
    ticks_ingested: u64,
}

impl RelayEngine {
    #[must_use]
    pub fn new_memory_only(tick_capacity: usize, kline_capacity: usize) -> Self {
        Self {
            cache: MarketCache::new(tick_capacity, kline_capacity),
            interests: InterestRegistry::default(),
            bootstrap: BootstrapQueue::new(4, Duration::from_millis(250)),
            klines: HashMap::new(),
            upstream_status: RelaySourceStatus::Connecting,
            ticks_ingested: 0,
        }
    }

    pub fn handle_command(
        &mut self,
        client_id: ClientId,
        command: DownstreamCommand,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        match command {
            DownstreamCommand::SubscribeQuote { symbols } => {
                self.interests.set_quotes(client_id, symbols);
                Ok(Vec::new())
            }
            DownstreamCommand::SetChart(command) => {
                let source = self.interests.set_chart(client_id, command);
                self.bootstrap.enqueue(BootstrapRequest {
                    source,
                    start_id: i64::MIN,
                    end_id: i64::MAX,
                });
                Ok(Vec::new())
            }
            DownstreamCommand::PeekMessage => Ok(Vec::new()),
        }
    }

    pub fn ingest_tick(
        &mut self,
        symbol: impl AsRef<str>,
        row: RelayTickRow,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let symbol = symbol.as_ref();
        self.ticks_ingested = self.ticks_ingested.saturating_add(1);
        self.upstream_status = RelaySourceStatus::Up;
        self.cache.push_tick(symbol, row.clone());
        let mut frames = self.quote_frames(symbol);
        frames.extend(self.kline_frames(symbol, row)?);
        Ok(frames)
    }

    pub fn remove_client(&mut self, client_id: ClientId) {
        self.interests.remove_client(client_id);
    }

    #[must_use]
    pub fn interests(&self) -> &InterestRegistry {
        &self.interests
    }

    #[must_use]
    pub fn bootstrap_pending_len(&self) -> usize {
        self.bootstrap.len()
    }

    #[must_use]
    pub fn health_snapshot(&self) -> HealthSnapshot {
        HealthSnapshot {
            ready: true,
            upstream_status: self.upstream_status,
            downstream_clients: self.interests.client_count(),
        }
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            downstream_clients: self.interests.client_count(),
            quote_subscriptions: self.interests.total_quote_subscriptions(),
            chart_subscriptions: self.interests.total_chart_subscriptions(),
            ticks_ingested: self.ticks_ingested,
            bootstrap_pending: self.bootstrap.len(),
            bootstrap_inflight: self.bootstrap.inflight(),
        }
    }

    fn quote_frames(&self, symbol: &str) -> Vec<DownstreamFrame> {
        let Some(quote) = self.cache.quote(symbol) else {
            return Vec::new();
        };
        let payload = RelayMarketFrame::rtn_data(vec![RelayMarketFrame::RtnData(vec![json!({
            "quotes": {
                symbol: {
                    "instrument_id": quote.instrument_id,
                    "datetime": quote.datetime,
                    "last_price": quote.last_price,
                    "volume": quote.volume,
                    "open_interest": quote.open_interest
                }
            }
        })])])
        .into_value();

        self.interests
            .quote_clients(symbol)
            .into_iter()
            .map(|client_id| DownstreamFrame {
                client_id,
                payload: payload.clone(),
            })
            .collect()
    }

    fn kline_frames(
        &mut self,
        symbol: &str,
        row: RelayTickRow,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let sources = self.interests.sources_for_symbol(symbol);
        let mut frames = Vec::new();
        for source in sources {
            if source.duration_ns <= 0 {
                continue;
            }
            let completed_rows = {
                let synthesizer = self
                    .klines
                    .entry(source.clone())
                    .or_insert_with(|| KlineSynthesis::new(symbol.to_string(), source.duration_ns));
                synthesizer.push_tick(row.clone())?
            };
            for completed in completed_rows {
                let kline_payload =
                    RelayMarketFrame::rtn_data(vec![RelayMarketFrame::kline_update(
                        symbol,
                        source.duration_ns,
                        completed.clone(),
                    )])
                    .into_value();
                for client_id in self.interests.chart_clients(&source) {
                    frames.push(DownstreamFrame {
                        client_id,
                        payload: kline_payload.clone(),
                    });
                    if let Some(chart_id) = self.interests.downstream_chart_id(client_id, &source) {
                        frames.push(DownstreamFrame {
                            client_id,
                            payload: chart_payload(chart_id, completed.id),
                        });
                    }
                }
            }
        }
        Ok(frames)
    }
}

fn chart_payload(chart_id: &str, right_id: i64) -> Value {
    json!({
        "aid": "rtn_data",
        "data": [
            {
                "charts": {
                    chart_id: {
                        "left_id": right_id,
                        "right_id": right_id,
                        "more_data": false,
                        "ready": true
                    }
                }
            }
        ]
    })
}
