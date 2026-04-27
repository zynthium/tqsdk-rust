//! Scenario: 合约信息查询与标准化
//!
//! User goal:
//! - 查询合约乘数、tick size、交易所、品种、到期日
//! - 获得 typed metadata
//! - 用结果做下单校验和展示
//!
//! API contract:
//! - 使用 `tqsdk-session` 的 one-shot metadata public API
//! - 返回 typed `InstrumentSpec`/metadata fields
//! - 不要求 live subscription 或 `wait_update()`
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - live quote subscription
//! - `StatePath`
//! - provider 内部 session / protocol type
//! - `serde_json::Value`
//!
//! Regression signal:
//! - metadata 查询必须走 wait/stream facade
//! - 合约规格需要从字符串 payload 手动解析
//! - direct query 归属回流到 wait/stream
//!
//! Review questions:
//! - 当前 API 是否自然表达合约信息查询？
//! - 是否保持 direct query crate 归属？
//! - typed metadata 字段是否足够？

use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());

    let session = SessionClientBuilder::new(user, pass)
        .enable_query()
        .build()?;
    let spec = session
        .query_instrument_specs(&[symbol.as_str()])
        .await?
        .into_iter()
        .next()
        .ok_or("query_instrument_specs returned no rows")?;

    println!(
        "symbol={} exchange={} product={} class={:?} tick={} multiplier={} expire={:?}",
        spec.symbol.as_str(),
        spec.exchange_id,
        spec.product_id,
        spec.class,
        spec.price_tick,
        spec.volume_multiple,
        spec.expire_datetime_ns
    );

    Ok(())
}
