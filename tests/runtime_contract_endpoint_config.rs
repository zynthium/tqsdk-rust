use std::sync::Mutex;

use tqsdk_runtime_contract::{EndpointConfig, MarketSessionTarget};

static ENV_MUTEX: Mutex<()> = Mutex::new(());

#[test]
fn endpoint_config_from_env_reads_tqsdk_rs_named_env_vars() {
    let _guard = ENV_MUTEX.lock().unwrap();
    let keys = [
        "TQ_AUTH_URL",
        "TQ_MD_URL",
        "TQ_TD_URL",
        "TQ_QUERY_URL",
        "TQ_REPLAY_URL",
        "TQ_SCHEMA_URL",
    ];
    let saved = snapshot_env(&keys);

    unsafe {
        std::env::set_var("TQ_AUTH_URL", "https://auth.env");
        std::env::set_var("TQ_MD_URL", "wss://md.env");
        std::env::set_var("TQ_TD_URL", "wss://td.env");
        std::env::set_var("TQ_QUERY_URL", "https://query.env/graphql");
        std::env::set_var("TQ_REPLAY_URL", "replay-driver");
        std::env::set_var("TQ_SCHEMA_URL", "https://schema.env");
    }

    let config = EndpointConfig::from_env();
    assert_eq!(config.auth_url.as_deref(), Some("https://auth.env"));
    assert_eq!(config.market_url.as_deref(), Some("wss://md.env"));
    assert_eq!(config.trade_url.as_deref(), Some("wss://td.env"));
    assert_eq!(
        config.query_url.as_deref(),
        Some("https://query.env/graphql")
    );
    assert_eq!(config.replay_url.as_deref(), Some("replay-driver"));
    assert_eq!(config.schema_url.as_deref(), Some("https://schema.env"));

    restore_env(saved);
}

#[test]
fn endpoint_config_default_and_market_target_match_runtime_defaults() {
    let _guard = ENV_MUTEX.lock().unwrap();
    let keys = [
        "TQ_AUTH_URL",
        "TQ_MD_URL",
        "TQ_TD_URL",
        "TQ_QUERY_URL",
        "TQ_REPLAY_URL",
        "TQ_SCHEMA_URL",
    ];
    let saved = snapshot_env(&keys);
    clear_env(&keys);

    let config = EndpointConfig::default();
    assert_eq!(
        config.auth_url.as_deref(),
        Some("https://auth.shinnytech.com")
    );
    assert_eq!(config.market_url, None);
    assert_eq!(config.trade_url, None);
    assert_eq!(config.query_url, None);
    assert_eq!(config.replay_url, None);
    assert_eq!(config.schema_url, None);

    let target = MarketSessionTarget::default();
    assert!(target.stock);
    assert!(!target.backtest);

    restore_env(saved);
}

fn snapshot_env(keys: &[&str]) -> Vec<(String, Option<String>)> {
    keys.iter()
        .map(|key| (key.to_string(), std::env::var(key).ok()))
        .collect()
}

fn clear_env(keys: &[&str]) {
    for key in keys {
        unsafe {
            std::env::remove_var(key);
        }
    }
}

fn restore_env(saved: Vec<(String, Option<String>)>) {
    for (key, value) in saved {
        match value {
            Some(value) => unsafe {
                std::env::set_var(&key, value);
            },
            None => unsafe {
                std::env::remove_var(&key);
            },
        }
    }
}
