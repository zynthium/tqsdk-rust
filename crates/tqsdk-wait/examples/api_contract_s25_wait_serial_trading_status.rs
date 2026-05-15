//! Scenario: Wait 行情序列与交易状态
//!
//! User goal:
//! - 单策略作者在 `wait_update()` 推进点读取实时交易状态
//! - 同时持有 K 线 serial 和 tick serial 的当前窗口
//! - 只在对象变化后加载 typed window / status 并继续策略逻辑
//! - Primary user layer: 单策略作者
//! - Intended crate path: `tqsdk-wait`
//! - Lower-level escape hatch: `tqsdk-session + RuntimeReader`
//! - Non-goal: 历史下载、DataFrame/polars、direct query metadata
//!
//! API contract:
//! - `TqApi::get_trading_status` 返回 diff-backed live trading status ref
//! - `TqApi::get_kline_serial` 返回 diff-backed realtime K 线窗口 ref
//! - `TqApi::get_tick_serial` 返回 diff-backed realtime tick 窗口 ref
//! - `TqApi::wait_update` 是用户可见状态推进点
//! - `TqApi::is_changing` 判断对象是否在最近一次 commit 中变化
//! - `TqApi::is_changing_fields` 判断对象字段是否在最近一次 commit 中变化
//! - `TqApi::is_serial_ready` 判断 serial window 是否已经初始化
//! - `KlineWindow::completed_rows` / `last_completed` 用于跳过最新可变尾 bar
//!
//! Forbidden:
//! - GraphQL / metadata direct query
//! - `DataClient` downloader
//! - `RuntimeCommand`
//! - `StatePath`
//! - `serde_json::Value`
//!
//! Regression signal:
//! - 实时 serial window 必须改用 data downloader 或 DataFrame/polars 才能读取
//! - trading status 被挪到 session direct query 或 metadata API
//! - 用户必须手写 runtime command、state path 或 JSON path 才能判断变化
//! - `is_changing` / `is_changing_fields` 无法覆盖 serial 或 trading status refs
//!
//! Review questions:
//! - 当前 API 是否自然表达 wait 风格实时交易状态与序列窗口？
//! - 实时窗口是否保持在 wait continuous consumption，而不是回流到 data download？
//! - 底层逃生舱是否仍然只是 `tqsdk-session + RuntimeReader`，而不是扩大 wait public surface？

use std::time::Duration;

use tqsdk_wait::TqApiBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn read_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let kline_seconds = read_env_u64("TQ_KLINE_SECONDS", 60);
    let serial_length = read_env_usize("TQ_SERIAL_LENGTH", 32);

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;
    let trading_status = api.get_trading_status(&symbol).await?;
    let kline_serial = api
        .get_kline_serial(&symbol, Duration::from_secs(kline_seconds), serial_length)
        .await?;
    let tick_serial = api.get_tick_serial(&symbol, serial_length).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while tokio::time::Instant::now() < deadline {
        if !api.wait_update(Some(deadline)).await? {
            continue;
        }

        if api.is_changing(&trading_status)?
            || api.is_changing_fields(&trading_status, &["trade_status"])?
        {
            let status = trading_status.load(&api)?;
            println!(
                "trading_status symbol={} epoch={:?} trade_status={}",
                status.symbol, status.epoch, status.trade_status
            );
        }

        if api.is_serial_ready(&kline_serial)?
            && (api.is_changing(&kline_serial)?
                || api.is_changing_fields(&kline_serial, &["close"])?)
        {
            let window = kline_serial.load(&api)?;
            let mutable_tail = window.last();
            let last_completed = window.last_completed();
            println!(
                "kline_window symbol={} duration_ns={} len={} completed={} last_completed_id={:?} last_completed_close={:?} mutable_tail_id={:?}",
                window.symbol(),
                window.duration_ns(),
                window.len(),
                window.completed_rows().len(),
                last_completed.map(|row| row.id),
                last_completed.map(|row| row.close),
                mutable_tail.map(|row| row.id)
            );
        }

        if api.is_serial_ready(&tick_serial)?
            && (api.is_changing(&tick_serial)?
                || api.is_changing_fields(&tick_serial, &["last_price"])?)
        {
            let window = tick_serial.load(&api)?;
            let last = window.last();
            println!(
                "tick_window symbol={} len={} last_id={:?} last_price={:?}",
                window.symbol(),
                window.len(),
                last.map(|row| row.id),
                last.map(|row| row.last_price)
            );
        }
    }

    Ok(())
}
