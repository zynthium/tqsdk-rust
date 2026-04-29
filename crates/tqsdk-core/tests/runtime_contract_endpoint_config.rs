use std::sync::Mutex;

use tqsdk_core::{EndpointConfig, MarketSessionTarget};

static ENV_MUTEX: Mutex<()> = Mutex::new(());

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_MUTEX
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn endpoint_config_from_env_reads_only_official_runtime_override_env_vars() {
    let _guard = env_lock();
    let keys = [
        "TQ_AUTH_URL",
        "TQ_MD_URL",
        "TQ_TD_URL",
        "TQ_QUERY_URL",
        "TQ_REPLAY_URL",
        "TQ_SCHEMA_URL",
        "TQ_INS_URL",
        "TQ_CHINESE_HOLIDAY_URL",
    ];
    let saved = snapshot_env(&keys);

    // SAFETY: endpoint config tests hold ENV_MUTEX while mutating process-wide
    // environment variables, so no other test in this module observes partial
    // updates during the scoped setup.
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
    assert_eq!(config.query_url, None);
    assert_eq!(config.replay_url, None);
    assert_eq!(config.schema_url, None);

    restore_env(saved);
}

#[test]
fn endpoint_config_from_env_ignores_non_runtime_or_misaligned_env_vars() {
    let _guard = env_lock();
    let keys = [
        "TQ_AUTH_URL",
        "TQ_MD_URL",
        "TQ_TD_URL",
        "TQ_QUERY_URL",
        "TQ_REPLAY_URL",
        "TQ_SCHEMA_URL",
        "TQ_INS_URL",
        "TQ_CHINESE_HOLIDAY_URL",
    ];
    let saved = snapshot_env(&keys);
    clear_env(&keys);

    // SAFETY: endpoint config tests hold ENV_MUTEX while mutating process-wide
    // environment variables, so these ignored keys are scoped to this serialized
    // test section.
    unsafe {
        std::env::set_var("TQ_INS_URL", "https://ins.env/graphql");
        std::env::set_var(
            "TQ_CHINESE_HOLIDAY_URL",
            "https://files.env/metadata/shinny_chinese_holiday.json",
        );
    }

    let config = EndpointConfig::from_env();
    assert_eq!(config.query_url, None);
    assert_eq!(config.replay_url, None);
    assert_eq!(config.schema_url, None);

    restore_env(saved);
}

#[test]
fn endpoint_config_default_and_market_target_match_runtime_defaults() {
    let _guard = env_lock();
    let keys = [
        "TQ_AUTH_URL",
        "TQ_MD_URL",
        "TQ_TD_URL",
        "TQ_QUERY_URL",
        "TQ_REPLAY_URL",
        "TQ_SCHEMA_URL",
        "TQ_INS_URL",
        "TQ_CHINESE_HOLIDAY_URL",
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
        // SAFETY: callers hold ENV_MUTEX, serializing process-wide environment
        // mutation for the duration of each endpoint config test.
        unsafe {
            std::env::remove_var(key);
        }
    }
}

fn restore_env(saved: Vec<(String, Option<String>)>) {
    for (key, value) in saved {
        match value {
            // SAFETY: callers hold ENV_MUTEX, so restoring saved variables
            // cannot race with another endpoint config test in this module.
            Some(value) => unsafe {
                std::env::set_var(&key, value);
            },
            // SAFETY: callers hold ENV_MUTEX, so removing variables absent in
            // the saved snapshot is serialized with all other test environment
            // mutation here.
            None => unsafe {
                std::env::remove_var(&key);
            },
        }
    }
}
