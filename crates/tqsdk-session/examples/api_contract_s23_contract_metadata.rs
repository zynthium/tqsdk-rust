//! Scenario: 合约信息查询与标准化
//!
//! User goal:
//! - 查询合约乘数、tick size、交易所、品种、到期日
//! - 查询官方合约信息表字段，例如交易时间段、涨跌停、昨结算和开仓限额
//! - 获得 typed metadata
//! - 用结果做下单校验和展示
//!
//! API contract:
//! - 使用 `tqsdk-session` 的 one-shot metadata public API
//! - `query_symbol_info` 返回 typed `SymbolInfo`
//! - `query_instrument_specs` 返回更窄的 typed `InstrumentSpec`
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
//! - metadata 查询必须走 wait 或自建消费层 facade
//! - 合约信息或规格需要从字符串 payload 手动解析
//! - direct query 归属回流到 wait 或自建消费层
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
    let info = session
        .query_symbol_info(&[symbol.as_str()])
        .await?
        .into_iter()
        .next()
        .ok_or("query_symbol_info returned no rows")?;
    let spec = session
        .query_instrument_specs(&[symbol.as_str()])
        .await?
        .into_iter()
        .next()
        .ok_or("query_instrument_specs returned no rows")?;

    println!(
        "info symbol={} class={} day={:?} night={:?} open_limit={:?} pre_settlement={:?}",
        info.instrument_id,
        info.ins_class,
        info.trading_time.day,
        info.trading_time.night,
        info.open_limit,
        info.pre_settlement
    );
    println!(
        "symbol={} exchange={} product={} class={:?} tick={} multiplier={} expire={:?}",
        spec.symbol.as_str(),
        spec.exchange_id,
        spec.product_id,
        spec.class,
        spec.price_tick,
        spec.volume_multiple,
        spec.expire_datetime_secs
    );

    Ok(())
}
