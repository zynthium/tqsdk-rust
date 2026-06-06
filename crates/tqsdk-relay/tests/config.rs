use std::time::Duration;

use tqsdk_relay::{BootstrapConfig, RelayConfig, RelayError};

#[test]
fn default_config_is_memory_only_and_local() {
    let config = RelayConfig::default();

    assert_eq!(config.downstream_listen, "127.0.0.1:7788");
    assert_eq!(config.metrics_listen, "127.0.0.1:7789");
    assert_eq!(config.tick_ring_capacity, 200_000);
    assert_eq!(config.kline_ring_capacity, 10_000);
    assert_eq!(config.bootstrap.max_concurrent_remote_charts, 4);
    assert_eq!(
        config.bootstrap.min_remote_request_interval,
        Duration::from_millis(250)
    );
    assert_eq!(
        config.bootstrap.per_series_cooldown,
        Duration::from_secs(30)
    );
    assert!(config.futures_symbols.is_empty());
    assert!(config.disk_cache_dir.is_none());
    assert!(config.best_effort_duration_tag);
    assert!(config.validate().is_ok());
}

#[test]
fn debug_redacts_upstream_auth_pass() {
    let config = RelayConfig {
        upstream_auth_pass: Some("super-secret-password".to_string()),
        ..RelayConfig::default()
    };

    let debug = format!("{config:?}");

    assert!(debug.contains("upstream_auth_pass"));
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("super-secret-password"));
}

#[test]
fn config_rejects_empty_upstream_market_url() {
    let config = RelayConfig {
        upstream_market_url: String::new(),
        ..RelayConfig::default()
    };

    let err = config.validate().unwrap_err();

    assert!(matches!(err, RelayError::InvalidConfig(_)));
    assert_eq!(
        err.to_string(),
        "invalid relay config: upstream_market_url must not be empty"
    );
}

#[test]
fn config_rejects_zero_ring_capacity() {
    let config = RelayConfig {
        tick_ring_capacity: 0,
        ..RelayConfig::default()
    };

    let err = config.validate().unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: tick_ring_capacity must be greater than zero"
    );
}

#[test]
fn config_rejects_zero_min_remote_request_interval() {
    let config = RelayConfig {
        bootstrap: BootstrapConfig {
            min_remote_request_interval: Duration::ZERO,
            ..BootstrapConfig::default()
        },
        ..RelayConfig::default()
    };

    let err = config.validate().unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: bootstrap.min_remote_request_interval must be greater than zero"
    );
}

#[test]
fn config_rejects_zero_per_series_cooldown() {
    let config = RelayConfig {
        bootstrap: BootstrapConfig {
            per_series_cooldown: Duration::ZERO,
            ..BootstrapConfig::default()
        },
        ..RelayConfig::default()
    };

    let err = config.validate().unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: bootstrap.per_series_cooldown must be greater than zero"
    );
}
