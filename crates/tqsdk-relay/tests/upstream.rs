use tqsdk_relay::{RelayConfig, UpstreamTickChart};

#[test]
fn config_accepts_explicit_futures_universe() {
    let config = RelayConfig {
        futures_symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
        ..RelayConfig::default()
    };

    config.validate().unwrap();
    assert_eq!(config.futures_symbols.len(), 2);
}

#[test]
fn config_rejects_empty_futures_symbol() {
    let config = RelayConfig {
        futures_symbols: vec!["SHFE.au2602".to_string(), " ".to_string()],
        ..RelayConfig::default()
    };

    let err = config.validate().unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: futures_symbols must not contain empty symbols"
    );
}

#[test]
fn upstream_tick_chart_uses_duration_zero_and_sorted_symbols() {
    let chart = UpstreamTickChart::new(
        "relay-upstream-all-futures-ticks",
        ["DCE.m2609", "SHFE.au2602"],
        10_000,
    )
    .unwrap();

    assert_eq!(chart.chart_id(), "relay-upstream-all-futures-ticks");
    assert_eq!(chart.duration_ns(), 0);
    assert_eq!(chart.view_width(), 10_000);
    assert_eq!(
        chart.symbols(),
        &["DCE.m2609".to_string(), "SHFE.au2602".to_string()]
    );
}
