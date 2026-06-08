#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::bootstrap::{BootstrapQueue, BootstrapRequest};
use crate::cache::MarketCache;
use crate::error::RelayResult;
use crate::interest::{ClientId, InterestRegistry, SourceKey};
use crate::kline::KlineSynthesis;
use crate::observability::{
    DEFAULT_DATA_STALE_AFTER_SECS, HealthSnapshot, MetricsSnapshot, RelaySourceStatus,
};
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
    upstream_symbols: usize,
    upstream_ins_list_chars: usize,
    upstream_ins_list_warn_chars: Option<usize>,
    upstream_ins_list_max_chars: Option<usize>,
    upstream_ins_list_over_warn: bool,
    last_universe_refresh_unix_secs: Option<u64>,
    last_universe_refresh_error: Option<String>,
    last_tick_unix_secs: Option<u64>,
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
            upstream_symbols: 0,
            upstream_ins_list_chars: 0,
            upstream_ins_list_warn_chars: None,
            upstream_ins_list_max_chars: None,
            upstream_ins_list_over_warn: false,
            last_universe_refresh_unix_secs: None,
            last_universe_refresh_error: None,
            last_tick_unix_secs: None,
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
                    source: source.clone(),
                    start_id: i64::MIN,
                    end_id: i64::MAX,
                });
                self.replay_cached_kline_frames(client_id, &source)
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
        self.record_data_activity_at(current_unix_secs());
        self.cache.push_tick(symbol, row.clone());
        let mut frames = self.quote_frames(symbol);
        frames.extend(self.kline_frames(symbol, row)?);
        Ok(frames)
    }

    pub fn remove_client(&mut self, client_id: ClientId) {
        self.interests.remove_client(client_id);
        let interests = &self.interests;
        self.bootstrap
            .retain_pending(|request| interests.chart_interest_count(&request.source) > 0);
    }

    pub fn mark_upstream_degraded(&mut self) {
        self.upstream_status = RelaySourceStatus::Degraded;
    }

    pub fn record_universe_refresh_success(
        &mut self,
        upstream_symbols: usize,
        upstream_ins_list_chars: usize,
        warn_chars: Option<usize>,
        max_chars: Option<usize>,
        unix_secs: u64,
    ) {
        self.upstream_symbols = upstream_symbols;
        self.upstream_ins_list_chars = upstream_ins_list_chars;
        self.upstream_ins_list_warn_chars = warn_chars;
        self.upstream_ins_list_max_chars = max_chars;
        self.upstream_ins_list_over_warn =
            warn_chars.is_some_and(|warn_chars| upstream_ins_list_chars > warn_chars);
        self.last_universe_refresh_unix_secs = Some(unix_secs);
        self.last_universe_refresh_error = None;
    }

    pub fn record_universe_refresh_error(&mut self, message: impl Into<String>, unix_secs: u64) {
        self.last_universe_refresh_unix_secs = Some(unix_secs);
        self.last_universe_refresh_error = Some(message.into());
    }

    pub fn record_data_activity_at(&mut self, unix_secs: u64) {
        self.last_tick_unix_secs = Some(unix_secs);
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
        self.health_snapshot_at(current_unix_secs())
    }

    #[must_use]
    pub fn health_snapshot_at(&self, now_unix_secs: u64) -> HealthSnapshot {
        let process_started = true;
        let downstream_listening = true;
        let upstream_connected = self.upstream_status == RelaySourceStatus::Up;
        let universe_ready = self.last_universe_refresh_unix_secs.is_some()
            && self.last_universe_refresh_error.is_none();
        let data_fresh = self.last_tick_unix_secs.is_some_and(|last_tick_unix_secs| {
            now_unix_secs.saturating_sub(last_tick_unix_secs) <= DEFAULT_DATA_STALE_AFTER_SECS
        });
        let market_data_ready = upstream_connected && universe_ready && data_fresh;
        HealthSnapshot {
            ready: process_started && downstream_listening,
            market_data_ready,
            process_started,
            downstream_listening,
            upstream_status: self.upstream_status,
            upstream_connected,
            universe_ready,
            data_fresh,
            downstream_clients: self.interests.client_count(),
            upstream_symbols: self.upstream_symbols,
            ticks_ingested: self.ticks_ingested,
            last_universe_refresh_unix_secs: self.last_universe_refresh_unix_secs,
            last_universe_refresh_error: self.last_universe_refresh_error.clone(),
            last_tick_unix_secs: self.last_tick_unix_secs,
            data_stale_after_secs: DEFAULT_DATA_STALE_AFTER_SECS,
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
            upstream_symbols: self.upstream_symbols,
            upstream_ins_list_chars: self.upstream_ins_list_chars,
            upstream_ins_list_warn_chars: self.upstream_ins_list_warn_chars,
            upstream_ins_list_max_chars: self.upstream_ins_list_max_chars,
            upstream_ins_list_over_warn: self.upstream_ins_list_over_warn,
            last_universe_refresh_unix_secs: self.last_universe_refresh_unix_secs,
            last_universe_refresh_error: self.last_universe_refresh_error.clone(),
            last_tick_unix_secs: self.last_tick_unix_secs,
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

    fn replay_cached_kline_frames(
        &mut self,
        client_id: ClientId,
        source: &SourceKey,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        if source.duration_ns <= 0 || self.klines.contains_key(source) {
            return Ok(Vec::new());
        }
        let Some(symbol) = source.symbols.first() else {
            return Ok(Vec::new());
        };
        let ticks = self.cache.ticks(symbol);
        if ticks.is_empty() {
            return Ok(Vec::new());
        }

        let mut synthesis = KlineSynthesis::new(symbol.clone(), source.duration_ns);
        let mut completed_rows = Vec::new();
        for tick in ticks {
            completed_rows.extend(synthesis.push_tick(tick)?);
        }
        self.klines.insert(source.clone(), synthesis);

        let mut frames = Vec::new();
        for completed in completed_rows {
            frames.push(DownstreamFrame {
                client_id,
                payload: RelayMarketFrame::rtn_data(vec![RelayMarketFrame::kline_update(
                    symbol,
                    source.duration_ns,
                    completed.clone(),
                )])
                .into_value(),
            });
            if let Some(chart_id) = self.interests.downstream_chart_id(client_id, source) {
                frames.push(DownstreamFrame {
                    client_id,
                    payload: chart_payload(chart_id, completed.id),
                });
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

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}
