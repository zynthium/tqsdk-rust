#![cfg_attr(not(test), forbid(unsafe_code))]

use serde::Serialize;

use crate::config::{FuturesUniverseRefreshSchedule, RelayConfig, RelayRuntimeConfig};
use crate::upstream::{UpstreamTickChart, upstream_subscription_ins_list_chars};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RelayStartupReport {
    pub event: &'static str,
    pub dry_run: bool,
    pub upstream_source: String,
    pub downstream_listen: String,
    pub metrics_listen: String,
    pub refresh_schedule: String,
    pub futures_metadata_batch_size: usize,
    pub futures_universe_expression: Option<String>,
    pub futures_universe_include_clauses: Option<usize>,
    pub futures_universe_exclude_clauses: Option<usize>,
    pub futures_universe_final_symbols: Option<usize>,
    pub upstream_symbols: usize,
    pub upstream_tick_view_width: usize,
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
        match chart {
            Some(chart) => Self::from_config_and_charts(config, std::slice::from_ref(chart)),
            None => Self::from_config_and_charts(config, &[]),
        }
    }

    #[must_use]
    pub fn from_config_and_charts(config: &RelayConfig, charts: &[UpstreamTickChart]) -> Self {
        let upstream_ins_list_chars = upstream_subscription_ins_list_chars(charts);
        Self {
            event: "relay_startup",
            dry_run: config.dry_run,
            upstream_source: upstream_source(config),
            downstream_listen: config.downstream_listen.clone(),
            metrics_listen: config.metrics_listen.clone(),
            refresh_schedule: refresh_schedule(config.futures_universe_refresh),
            futures_metadata_batch_size: config.futures_metadata_batch_size,
            futures_universe_expression: config
                .futures_universe_expression
                .as_ref()
                .map(ToString::to_string),
            futures_universe_include_clauses: config
                .futures_universe_expression
                .as_ref()
                .map(|expression| expression.include_clause_count()),
            futures_universe_exclude_clauses: config
                .futures_universe_expression
                .as_ref()
                .map(|expression| expression.exclude_clause_count()),
            futures_universe_final_symbols: config
                .futures_universe_expression
                .as_ref()
                .map(|_| charts.iter().map(|chart| chart.symbols().len()).sum()),
            upstream_symbols: charts.iter().map(|chart| chart.symbols().len()).sum(),
            upstream_tick_view_width: charts.first().map_or(
                config.upstream_tick_view_width,
                UpstreamTickChart::view_width,
            ),
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
    pub fn from_runtime_config_and_charts(
        config: &RelayRuntimeConfig,
        charts: &[UpstreamTickChart],
    ) -> Self {
        let mut report = Self::from_config_and_charts(config.relay_config(), charts);
        let has_files = !config.futures_universe_symbol_files().is_empty();
        if let Some(spec) = config.futures_universe_spec() {
            report.upstream_source = if has_files {
                "universe-v2+files"
            } else {
                "universe-v2"
            }
            .to_string();
            report.futures_universe_expression = Some(spec.canonical_text().to_string());
            report.futures_universe_include_clauses = Some(spec.includes().len());
            report.futures_universe_exclude_clauses =
                Some(spec.excludes().len() + spec.global_filters().len());
        } else if has_files {
            report.upstream_source = if config.relay_config().futures_universe_expression.is_some()
            {
                "universe-expression+files"
            } else {
                "universe-files"
            }
            .to_string();
        }
        report.futures_universe_final_symbols = config
            .has_upstream_futures_source()
            .then(|| charts.iter().map(|chart| chart.symbols().len()).sum());
        report
    }

    #[must_use]
    pub fn log_line(&self) -> String {
        serde_json::to_string(self).expect("startup report is JSON serializable")
    }
}

fn upstream_source(config: &RelayConfig) -> String {
    if config.futures_universe_expression.is_some() {
        return "universe-expression".to_string();
    }
    "none".to_string()
}

fn refresh_schedule(schedule: FuturesUniverseRefreshSchedule) -> String {
    let seconds = schedule.refresh_at().seconds_after_midnight();
    let hour = seconds / 3600;
    let minute = (seconds % 3600) / 60;
    let second = seconds % 60;
    format!("daily:{hour:02}:{minute:02}:{second:02}")
}
