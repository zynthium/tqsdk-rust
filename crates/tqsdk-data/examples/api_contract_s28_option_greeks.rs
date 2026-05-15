//! Scenario: Data 期权 Greeks 研究查询
//!
//! User goal:
//! - 一次性查询多个期权合约的 Greeks
//! - 可显式传入波动率、无风险利率和查询超时
//! - 获得 owned typed rows 用于研究分析
//!
//! API contract:
//! - Primary user layer: 研究 / 数据用户
//! - Intended crate path: `tqsdk-data`
//! - Lower-level escape hatch: 需要原始实时 quote 时使用 wait/stream live market API
//! - `query_option_greeks` 是 data research query，不公开内部 snapshot helper
//! - Greeks 能力留在 data，不回流到 session/wait/stream
//!
//! Forbidden:
//! - `TqApi::quote` as pre-step
//! - direct `RuntimeCommand`
//! - provider internal quote snapshot helper
//! - `Arc<Mutex<_>>` for temporary subscriptions
//! - trading order / risk-control workflow
//! - DataFrame/polars
//!
//! Regression signal:
//! - Greeks 查询要求用户先创建 live quote ref
//! - 内部临时 snapshot helper 变成通用 public API
//! - Greeks 被移动到 session direct query 或 wait/stream facade
//! - 研究用户需要手写临时订阅状态管理
//!
//! Review questions:
//! - Greeks research query 是否能通过 data crate 的 owned typed API 自然表达？
//! - 原始实时 quote 需求是否仍明确导向 wait/stream，而不是复用内部 helper？
//! - session 是否只作为共享连接 substrate，而不是 Greeks API 归属地？

use std::time::Duration;

use tqsdk_data::{DataClient, OptionGreeksRequest};
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn parse_symbols() -> Vec<String> {
    std::env::var("TQ_OPTION_SYMBOLS")
        .unwrap_or_else(|_| "SHFE.au2606C720".to_string())
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_volatilities(symbol_count: usize) -> Vec<f64> {
    std::env::var("TQ_OPTION_VOLS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter_map(|vol| vol.parse::<f64>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|volatilities| volatilities.len() == symbol_count)
        .unwrap_or_else(|| vec![0.3; symbol_count])
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbols = parse_symbols();
    let volatilities = parse_volatilities(symbols.len());
    let risk_free_rate = std::env::var("TQ_RISK_FREE_RATE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.025);

    let session = SessionClientBuilder::new(user, pass)
        .enable_query()
        .futures_market()
        .build()?;
    let client = DataClient::from_session(session);
    let request = OptionGreeksRequest::new(symbols)
        .with_volatilities(volatilities)
        .with_risk_free_rate(risk_free_rate)
        .with_timeout(Duration::from_secs(30));
    let result = client.query_option_greeks(request).await?;

    for row in result.iter() {
        println!(
            "{} {} dt={} option_px={} underlying={} underlying_px={} vol={} delta={} gamma={} theta={} vega={} rho={}",
            row.symbol,
            row.option_class,
            row.quote_datetime,
            row.option_last_price,
            row.underlying_symbol,
            row.underlying_last_price,
            row.volatility,
            row.delta,
            row.gamma,
            row.theta,
            row.vega,
            row.rho
        );
    }
    println!("greeks_rows={}", result.iter().count());

    Ok(())
}
