#![allow(
    dead_code,
    reason = "shared helpers are compiled independently by each integration-test target"
)]

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use tqsdk_data::{
    BACKTEST_HISTORY_METADATA_SCHEMA_VERSION, BacktestHistoryMarketKind,
    BacktestHistoryMetadataSnapshot, BacktestHistoryPhysicalSegment, BacktestHistoryTradingDay,
    KlineSessionTemplate,
};

pub fn temp_dir(name: &str) -> PathBuf {
    let root = unique_path(name);
    std::fs::create_dir_all(&root).expect("test cache directory should be created");
    root
}

pub fn missing_path(name: &str) -> PathBuf {
    unique_path(name)
}

fn unique_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("tqsdk-backtest-history-{name}-{nanos}"))
}

pub fn snapshot(
    logical_symbol: &str,
    captured_at_ns: i64,
    physical_segments: Vec<BacktestHistoryPhysicalSegment>,
) -> BacktestHistoryMetadataSnapshot {
    BacktestHistoryMetadataSnapshot {
        schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
        market_kind: BacktestHistoryMarketKind::Futures,
        logical_symbol: logical_symbol.to_string(),
        captured_at_ns,
        trading_days: vec![
            BacktestHistoryTradingDay {
                date: "2026-01-05".to_string(),
                is_trading_day: true,
                start_ns: 1_767_572_800_000_000_000,
                end_ns: 1_767_659_200_000_000_000,
            },
            BacktestHistoryTradingDay {
                date: "2026-01-06".to_string(),
                is_trading_day: true,
                start_ns: 1_767_659_200_000_000_000,
                end_ns: 1_767_745_600_000_000_000,
            },
        ],
        session: KlineSessionTemplate::cst_trading_day(),
        physical_segments,
        snapshot_hash: String::new(),
    }
}

pub fn segment(symbol: &str, start_ns: i64, end_ns: i64) -> BacktestHistoryPhysicalSegment {
    BacktestHistoryPhysicalSegment {
        physical_symbol: symbol.to_string(),
        start_ns,
        end_ns,
    }
}

pub fn metadata_symbol_dir(root: &std::path::Path, symbol: &str) -> PathBuf {
    root.join("backtest-history-metadata-v1")
        .join(escape_path_component(symbol))
}

fn escape_path_component(value: &str) -> String {
    let mut escaped = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_') {
            escaped.push(byte as char);
        } else {
            escaped.push('%');
            escaped.push_str(&format!("{byte:02X}"));
        }
    }
    escaped
}
