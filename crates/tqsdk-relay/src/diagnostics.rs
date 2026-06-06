#![cfg_attr(not(test), forbid(unsafe_code))]

use serde::Serialize;

use crate::config::{FuturesUniverseRefreshSchedule, RelayConfig};
use crate::universe::FuturesProductFilter;
use crate::upstream::UpstreamTickChart;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RelayStartupReport {
    pub event: &'static str,
    pub dry_run: bool,
    pub upstream_source: String,
    pub downstream_listen: String,
    pub metrics_listen: String,
    pub refresh_schedule: String,
    pub futures_metadata_batch_size: usize,
    pub upstream_symbols: usize,
    pub upstream_ins_list_chars: usize,
    pub upstream_ins_list_warn_chars: Option<usize>,
    pub upstream_ins_list_max_chars: Option<usize>,
    pub upstream_ins_list_over_warn: bool,
    pub upstream_ins_list_over_max: bool,
    pub suggested_relay_instances: Option<usize>,
}

impl RelayStartupReport {
    #[must_use]
    pub fn from_config_and_chart(config: &RelayConfig, chart: Option<&UpstreamTickChart>) -> Self {
        let upstream_ins_list_chars = chart.map_or(0, UpstreamTickChart::ins_list_chars);
        Self {
            event: "relay_startup",
            dry_run: config.dry_run,
            upstream_source: upstream_source(config),
            downstream_listen: config.downstream_listen.clone(),
            metrics_listen: config.metrics_listen.clone(),
            refresh_schedule: refresh_schedule(config.futures_universe_refresh),
            futures_metadata_batch_size: config.futures_metadata_batch_size,
            upstream_symbols: chart.map_or(0, |chart| chart.symbols().len()),
            upstream_ins_list_chars,
            upstream_ins_list_warn_chars: config.upstream_ins_list_limits.warn_chars,
            upstream_ins_list_max_chars: config.upstream_ins_list_limits.max_chars,
            upstream_ins_list_over_warn: config
                .upstream_ins_list_limits
                .over_warn(upstream_ins_list_chars),
            upstream_ins_list_over_max: config
                .upstream_ins_list_limits
                .max_chars
                .is_some_and(|max_chars| upstream_ins_list_chars > max_chars),
            suggested_relay_instances: config
                .upstream_ins_list_limits
                .suggested_shards(upstream_ins_list_chars),
        }
    }

    #[must_use]
    pub fn log_line(&self) -> String {
        serde_json::to_string(self).expect("startup report is JSON serializable")
    }
}

fn upstream_source(config: &RelayConfig) -> String {
    if !config.futures_symbols.is_empty() {
        return "static-symbols".to_string();
    }
    match &config.futures_product_filter {
        FuturesProductFilter::None => "none".to_string(),
        FuturesProductFilter::All => "products:all".to_string(),
        FuturesProductFilter::Products(products) => {
            format!("products:{}", products.len())
        }
    }
}

fn refresh_schedule(schedule: FuturesUniverseRefreshSchedule) -> String {
    match schedule {
        FuturesUniverseRefreshSchedule::Daily(time) => {
            let seconds = time.seconds_after_midnight();
            let hour = seconds / 3600;
            let minute = (seconds % 3600) / 60;
            let second = seconds % 60;
            format!("daily:{hour:02}:{minute:02}:{second:02}")
        }
        FuturesUniverseRefreshSchedule::Interval(interval) => {
            format!("interval:{}s", interval.as_secs())
        }
    }
}
