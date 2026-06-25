//! Scenario: Session metadata 与 service query pack
//!
//! User goal:
//! - 低层 / 高频用户直接使用 session direct-query 能力
//! - 一次性查询合约列表、主连、期权、交易日历、结算价、排名和 EDB
//! - 不进入 wait live refs、自建消费层或 data/research 工作流
//!
//! API contract:
//! - Primary user layer: 低层 / 高频用户；direct-query 用户
//! - Intended crate path: `tqsdk-session`
//! - Lower-level escape hatch: `SessionRawQuery::query_graphql_value`
//! - 使用 `SessionClientBuilder::enable_query()` 启用官方 query domain
//! - 覆盖 `SessionClient` 的 metadata/service one-shot request/response API
//!
//! Forbidden:
//! - `TqApi` live subscription
//! - live consumer facade
//! - `DataClient`
//! - provider 内部 session type
//! - 需要用户解析的 `serde_json::Value`
//!
//! Regression signal:
//! - metadata/service direct query 被复制到 wait 或自建消费层
//! - 用户必须通过 live ref、历史下载或 DataFrame/polars 读取这些一次性结果
//! - 合约、日历、结算价、排名或 EDB 结果退化为用户手动解析 JSON
//!
//! Review questions:
//! - metadata/service query pack 是否完整归属 `tqsdk-session`？
//! - `SessionClient` 的 typed one-shot API 是否覆盖 direct-query 用户主流程？
//! - raw GraphQL escape hatch 是否仍只是低层补洞，而不是主路径？
//!
//! Non-goal:
//! - wait live refs 或自建 live refs
//! - 历史下载
//! - Greeks
//! - DataFrame/polars

use chrono::{Duration, Utc};
use tqsdk_session::{
    AllLevelOptionQuery, AtmOptionQuery, EdbDataAlign, EdbDataFill, FinanceOptionLevelQuery,
    OptionQueryFilter, SessionClientBuilder, SymbolRankingType,
};

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn option_count(levels: &tqsdk_session::OptionLevelQuotes) -> usize {
    levels.in_money.len() + levels.at_money.len() + levels.out_of_money.len()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = read_env_or("TQ_TEST_SYMBOL", "SHFE.au2602");
    let exchange = read_env_or("TQ_TEST_EXCHANGE", "SHFE");
    let product = read_env_or("TQ_TEST_PRODUCT", "au");
    let option_underlying = read_env_or("TQ_OPTION_UNDERLYING", "SHFE.au2602");
    let finance_option_underlying = read_env_or("TQ_FINANCE_OPTION_UNDERLYING", "SSE.510300");

    let session = SessionClientBuilder::new(user, pass)
        .enable_query()
        .build()?;

    let quotes = session
        .query_quotes(
            Some("FUTURE"),
            Some(exchange.as_str()),
            Some(product.as_str()),
            Some(false),
            None,
        )
        .await?;
    let cont_quotes = session
        .query_cont_quotes(Some(exchange.as_str()), Some(product.as_str()), None)
        .await?;
    let options = session
        .query_options(option_underlying.as_str(), &OptionQueryFilter::new())
        .await?;
    let atm_options = session
        .query_atm_options(
            option_underlying.as_str(),
            &AtmOptionQuery::new(500.0, [-1, 0, 1], "CALL"),
        )
        .await?;
    let all_level_options = session
        .query_all_level_options(
            option_underlying.as_str(),
            &AllLevelOptionQuery::new(500.0, "CALL"),
        )
        .await?;
    let finance_options = session
        .query_all_level_finance_options(
            finance_option_underlying.as_str(),
            &FinanceOptionLevelQuery::new(500.0, "CALL", [0]),
        )
        .await?;

    let end_dt = Utc::now().date_naive();
    let start_dt = end_dt - Duration::days(7);
    let calendar = session.get_trading_calendar(start_dt, end_dt).await?;
    let settlement = session
        .query_symbol_settlement(&[symbol.as_str()], 5, None)
        .await?;
    let ranking = session
        .query_symbol_ranking(symbol.as_str(), SymbolRankingType::Volume, 5, None, None)
        .await?;
    let edb = session
        .query_edb_data(
            &[100001],
            start_dt,
            end_dt,
            Some(EdbDataAlign::Day),
            Some(EdbDataFill::Forward),
        )
        .await?;

    println!("query_quotes={}", quotes.len());
    println!("query_cont_quotes={}", cont_quotes.len());
    println!("query_options={}", options.len());
    println!("query_atm_options={}", atm_options.len());
    println!(
        "query_all_level_options={}",
        option_count(&all_level_options)
    );
    println!(
        "query_all_level_finance_options={}",
        option_count(&finance_options)
    );
    println!("get_trading_calendar={}", calendar.len());
    println!("query_symbol_settlement={}", settlement.len());
    println!("query_symbol_ranking={}", ranking.len());
    println!("query_edb_data={}", edb.len());

    Ok(())
}
