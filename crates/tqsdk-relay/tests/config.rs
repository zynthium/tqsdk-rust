use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tqsdk_relay::{
    BootstrapConfig, FuturesProductCode, FuturesProductFilter, RelayConfig, RelayError,
};

#[test]
fn default_config_is_memory_only_and_local() {
    let config = RelayConfig::default();

    assert_eq!(config.downstream_listen, "127.0.0.1:7788");
    assert_eq!(config.metrics_listen, "127.0.0.1:7789");
    assert_eq!(config.futures_universe_refresh, Duration::from_secs(86_400));
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
fn config_loads_env_overrides_without_touching_sdk_defaults() {
    let config = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_UPSTREAM_MARKET_URL" => Some("ws://127.0.0.1:9001/market".to_string()),
        "TQSDK_RELAY_DOWNSTREAM_LISTEN" => Some("127.0.0.1:17788".to_string()),
        "TQSDK_RELAY_METRICS_LISTEN" => Some("127.0.0.1:17789".to_string()),
        "TQSDK_RELAY_FUTURES_SYMBOLS" => Some(" SHFE.au2602, DCE.m2609 ".to_string()),
        _ => None,
    })
    .unwrap();

    assert_eq!(config.upstream_market_url, "ws://127.0.0.1:9001/market");
    assert_eq!(config.downstream_listen, "127.0.0.1:17788");
    assert_eq!(config.metrics_listen, "127.0.0.1:17789");
    assert_eq!(
        config.futures_symbols,
        vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()]
    );
}

#[test]
fn config_loads_all_futures_products_from_env() {
    let config = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_FUTURES_PRODUCTS" => Some("ALL".to_string()),
        _ => None,
    })
    .unwrap();

    assert_eq!(config.futures_product_filter, FuturesProductFilter::All);
    assert!(config.futures_symbols.is_empty());
}

#[test]
fn config_loads_futures_product_codes_from_env() {
    let config = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_FUTURES_PRODUCTS" => Some("SHFE.au,DCE.m,MA".to_string()),
        _ => None,
    })
    .unwrap();

    assert_eq!(
        config.futures_product_filter,
        FuturesProductFilter::Products(vec![
            FuturesProductCode::new(Some("SHFE"), "au").unwrap(),
            FuturesProductCode::new(Some("DCE"), "m").unwrap(),
            FuturesProductCode::new(None, "MA").unwrap(),
        ])
    );
}

#[test]
fn config_loads_auth_and_futures_universe_refresh_from_env() {
    let config = RelayConfig::from_env_vars(|key| match key {
        "TQ_AUTH_USER" => Some(" demo-user ".to_string()),
        "TQ_AUTH_PASS" => Some(" demo-pass ".to_string()),
        "TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_SECS" => Some("86400".to_string()),
        _ => None,
    })
    .unwrap();

    assert_eq!(config.upstream_auth_user.as_deref(), Some("demo-user"));
    assert_eq!(config.upstream_auth_pass.as_deref(), Some("demo-pass"));
    assert_eq!(config.futures_universe_refresh, Duration::from_secs(86_400));
}

#[test]
fn config_env_rejects_empty_futures_symbol_entries() {
    let err = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_FUTURES_SYMBOLS" => Some("SHFE.au2602, ,DCE.m2609".to_string()),
        _ => None,
    })
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: futures_symbols must not contain empty symbols"
    );
}

#[test]
fn config_loads_futures_symbols_from_file_env() {
    let path = temp_symbols_file("futures-symbols");
    fs::write(&path, "SHFE.au2602\nDCE.m2609\n").unwrap();

    let config = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_FUTURES_SYMBOLS_FILE" => Some(path.display().to_string()),
        _ => None,
    })
    .unwrap();

    assert_eq!(
        config.futures_symbols,
        vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()]
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn config_rejects_inline_and_file_futures_symbols_together() {
    let err = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_FUTURES_SYMBOLS" => Some("SHFE.au2602".to_string()),
        "TQSDK_RELAY_FUTURES_SYMBOLS_FILE" => Some("symbols.txt".to_string()),
        _ => None,
    })
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: set only one of TQSDK_RELAY_FUTURES_SYMBOLS or TQSDK_RELAY_FUTURES_SYMBOLS_FILE"
    );
}

#[test]
fn config_rejects_futures_symbols_and_products_together() {
    let err = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_FUTURES_SYMBOLS" => Some("SHFE.au2602".to_string()),
        "TQSDK_RELAY_FUTURES_PRODUCTS" => Some("ALL".to_string()),
        _ => None,
    })
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: set only one futures universe source"
    );
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
fn config_rejects_zero_futures_universe_refresh() {
    let config = RelayConfig {
        futures_universe_refresh: Duration::ZERO,
        ..RelayConfig::default()
    };

    let err = config.validate().unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: futures_universe_refresh must be greater than zero"
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

fn temp_symbols_file(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-relay-{name}-{}-{nanos}.txt",
        std::process::id()
    ))
}
