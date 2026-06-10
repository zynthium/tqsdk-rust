use std::time::Duration;

#[cfg(feature = "services")]
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{Value, json};
use tokio::time::Instant;
#[cfg(feature = "tq-auth")]
use tqsdk_core::{Account, TradeCommand};
use tqsdk_core::{MarketCommand, QueryCommand, QueryId, Quote, RuntimeCommand, Symbol};
use tqsdk_session::{
    AllLevelOptionQuery, AtmOptionQuery, FinanceOptionLevelQuery, OptionQueryFilter,
    SessionClientBuilder,
};
#[cfg(feature = "services")]
use tqsdk_session::{EdbDataAlign, EdbDataFill, SymbolRankingType};

const LIVE_SYMBOL_INFO_QUERY: &str = r#"query($instrument_id:[String]){
  multi_symbol_info(instrument_id: $instrument_id) {
    ... on basic {
      instrument_id
      class
      price_tick
    }
  }
}"#;

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and query access"]
async fn live_query_symbol_info_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .enable_query()
        .build()
        .expect("live session should build");
    let infos = session
        .query_symbol_info(&[symbol.as_str()])
        .await
        .expect("query_symbol_info should succeed");
    let info = infos
        .into_iter()
        .next()
        .expect("query_symbol_info should return at least one row");

    assert!(!info.instrument_id.as_str().is_empty());
    assert!(!info.ins_class.is_empty());
    assert!(info.price_tick.is_some_and(f64::is_finite));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, stock market access, and validates raw query command waiting"]
async fn live_query_command_wait_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_QUERY_SYMBOL").unwrap_or_else(|| "SSE.000300".to_string());
    assert!(
        is_stock_symbol(symbol.as_str()),
        "TQ_QUERY_SYMBOL must be a stock symbol when query rides the official stock websocket"
    );

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .stock_market()
        .enable_query()
        .build()
        .expect("live session should build");
    let query_id = QueryId::new("live-symbol-info");
    let command_id = session
        .submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: query_id.clone(),
            query: LIVE_SYMBOL_INFO_QUERY.to_string(),
            variables: Some(json!({ "instrument_id": [symbol] })),
        }))
        .await
        .expect("raw query command should submit");

    session
        .wait_command_completed(command_id)
        .await
        .expect("raw query command should complete");

    let payload = session
        .query_result(query_id.as_str())
        .expect("query_result should decode")
        .expect("query command should produce a result payload");
    let instrument = first_symbol_info(payload).expect("query payload should contain one symbol");

    assert!(
        !instrument
            .get("instrument_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .is_empty()
    );
    assert!(
        !instrument
            .get("class")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .is_empty()
    );
    assert!(
        instrument
            .get("price_tick")
            .and_then(Value::as_f64)
            .is_some_and(f64::is_finite)
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, stock market query access, and exercises raw/control-plane requests"]
async fn live_raw_and_control_plane_requests_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_QUERY_SYMBOL").unwrap_or_else(|| "SSE.000300".to_string());
    assert!(
        is_stock_symbol(symbol.as_str()),
        "TQ_QUERY_SYMBOL must be a stock symbol when query rides the official stock websocket"
    );

    let session = SessionClientBuilder::new(auth_user.clone(), auth_pass.clone())
        .stock_market()
        .enable_query()
        .build()
        .expect("live session should build");
    let payload = session
        .query_graphql_value(
            LIVE_SYMBOL_INFO_QUERY,
            Some(json!({ "instrument_id": [symbol.as_str()] })),
        )
        .await
        .expect("query_graphql_value should succeed");
    let instrument = first_symbol_info(payload).expect("query payload should contain one symbol");
    assert_eq!(
        instrument
            .get("instrument_id")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        symbol
    );
    assert!(
        instrument
            .get("price_tick")
            .and_then(Value::as_f64)
            .is_some_and(f64::is_finite)
    );

    let refreshed = session
        .refresh_auth_value()
        .await
        .expect("refresh_auth_value should succeed");
    assert!(
        refreshed
            .get("access_token")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
    );
    assert!(
        refreshed
            .get("features")
            .and_then(Value::as_array)
            .is_some()
    );

    let schema_path =
        read_env("TQ_SCHEMA_SMOKE_PATH").unwrap_or_else(|| "broker-list.json".to_string());
    let schema = SessionClientBuilder::new(auth_user, auth_pass)
        .schema_url("https://files.shinnytech.com/")
        .build()
        .expect("schema session should build")
        .refresh_schema_value("live-schema-smoke", schema_path.as_str())
        .await
        .expect("refresh_schema_value should fetch a file-backed schema/metadata payload");
    assert!(
        schema.as_object().is_some_and(|object| !object.is_empty()),
        "schema payload should be a non-empty object"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and stock market query access"]
async fn live_metadata_query_pack_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let stock_symbol = read_env("TQ_QUERY_SYMBOL").unwrap_or_else(|| "SSE.000300".to_string());
    assert!(
        is_stock_symbol(stock_symbol.as_str()),
        "TQ_QUERY_SYMBOL must be a stock symbol when query rides the official stock websocket"
    );
    let future_symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());
    let exchange = read_env("TQ_TEST_EXCHANGE").unwrap_or_else(|| "SHFE".to_string());
    let product = read_env("TQ_TEST_PRODUCT").unwrap_or_else(|| "ao".to_string());
    let option_underlying =
        read_env("TQ_OPTION_UNDERLYING").unwrap_or_else(|| "SHFE.au2606".to_string());
    let finance_underlying =
        read_env("TQ_FINANCE_OPTION_UNDERLYING").unwrap_or_else(|| "SSE.510300".to_string());
    let underlying_price = read_f64_env("TQ_OPTION_UNDERLYING_PRICE").unwrap_or(500.0);
    let finance_underlying_price = read_f64_env("TQ_FINANCE_OPTION_UNDERLYING_PRICE")
        .unwrap_or_else(|| underlying_price.max(1.0));

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .stock_market()
        .enable_query()
        .build()
        .expect("live session should build");

    let symbols = vec![future_symbol.as_str(), stock_symbol.as_str()];
    let infos = session
        .query_symbol_info(&symbols)
        .await
        .expect("query_symbol_info should succeed");
    assert_eq!(infos.len(), symbols.len());
    assert!(
        infos
            .iter()
            .all(|info| !info.instrument_id.as_str().is_empty())
    );

    let specs = session
        .query_instrument_specs(&symbols)
        .await
        .expect("query_instrument_specs should succeed");
    assert_eq!(specs.len(), symbols.len());
    assert!(specs.iter().all(|spec| spec.price_tick.is_finite()));

    let quotes = session
        .query_quotes(
            Some("FUTURE"),
            Some(exchange.as_str()),
            Some(product.as_str()),
            Some(false),
            None,
        )
        .await
        .expect("query_quotes should succeed");
    assert!(
        quotes
            .iter()
            .any(|symbol| symbol.starts_with(exchange.as_str())),
        "query_quotes should return at least one symbol for {exchange}.{product}"
    );

    let cont_quotes = session
        .query_cont_quotes(Some(exchange.as_str()), Some(product.as_str()), None)
        .await
        .expect("query_cont_quotes should succeed");
    assert!(
        cont_quotes
            .iter()
            .all(|symbol| symbol.starts_with(exchange.as_str())),
        "query_cont_quotes should only return matching underlying symbols for {exchange}.{product}"
    );

    let mut filter = OptionQueryFilter::new();
    filter.expired = Some(false);
    let options = session
        .query_options(option_underlying.as_str(), &filter)
        .await
        .expect("query_options should succeed");
    assert!(
        !options.is_empty(),
        "query_options should return live option symbols for {option_underlying}"
    );

    let atm_options = session
        .query_atm_options(
            option_underlying.as_str(),
            &AtmOptionQuery::new(underlying_price, [-1, 0, 1], "CALL"),
        )
        .await
        .expect("query_atm_options should succeed");
    assert_eq!(atm_options.len(), 3);
    assert!(
        atm_options.iter().any(Option::is_some),
        "query_atm_options should resolve at least one nearby option"
    );

    let all_level_options = session
        .query_all_level_options(
            option_underlying.as_str(),
            &AllLevelOptionQuery::new(underlying_price, "CALL"),
        )
        .await
        .expect("query_all_level_options should succeed");
    assert!(
        option_level_count(&all_level_options) > 0,
        "query_all_level_options should return at least one option"
    );

    let finance_options = session
        .query_all_level_finance_options(
            finance_underlying.as_str(),
            &FinanceOptionLevelQuery::new(finance_underlying_price, "CALL", [0]),
        )
        .await
        .expect("query_all_level_finance_options should succeed");
    assert!(
        option_level_count(&finance_options) > 0,
        "query_all_level_finance_options should return at least one option"
    );
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "services")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and service query access"]
async fn live_service_query_pack_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());
    let edb_id = read_i32_env("TQ_EDB_ID").unwrap_or(100001);

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .build()
        .expect("live session should build");
    let end_dt = Utc::now().date_naive();
    let start_dt = end_dt - ChronoDuration::days(7);

    let calendar = session
        .get_trading_calendar(start_dt, end_dt)
        .await
        .expect("get_trading_calendar should succeed");
    let start_dt_text = start_dt.format("%Y-%m-%d").to_string();
    assert_eq!(calendar.len(), 8);
    assert_eq!(
        calendar.first().map(|day| day.date.as_str()),
        Some(start_dt_text.as_str())
    );
    assert!(calendar.iter().any(|day| !day.date.is_empty()));

    let settlement = session
        .query_symbol_settlement(&[symbol.as_str()], 5, None)
        .await
        .expect("query_symbol_settlement should succeed");
    assert!(
        settlement.iter().all(|row| row.symbol == symbol),
        "query_symbol_settlement should only return requested symbols"
    );

    let ranking = session
        .query_symbol_ranking(symbol.as_str(), SymbolRankingType::Volume, 5, None, None)
        .await
        .expect("query_symbol_ranking should succeed");
    assert!(
        ranking.iter().all(|row| row.symbol == symbol),
        "query_symbol_ranking should only return requested symbols"
    );

    let edb = match session
        .query_edb_data(
            &[edb_id],
            start_dt,
            end_dt,
            Some(EdbDataAlign::Day),
            Some(EdbDataFill::Forward),
        )
        .await
    {
        Ok(edb) => edb,
        Err(error) if is_edb_permission_error(&error) && read_env("TQ_REQUIRE_EDB").is_none() => {
            eprintln!("skipping EDB assertion because this account lacks EDB access: {error}");
            return;
        }
        Err(error) => panic!("query_edb_data should succeed: {error}"),
    };
    assert_eq!(edb.len(), calendar.len());
    assert!(edb.iter().all(|row| row.values.contains_key(&edb_id)));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and market access"]
async fn live_quote_progress_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());

    let session = build_market_session(auth_user, auth_pass, symbol.as_str());
    session
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new(symbol.clone())],
        }))
        .await
        .expect("SubscribeQuotes should submit successfully");

    let quote = wait_for_quote_update(&session, symbol.as_str(), Duration::from_secs(30))
        .await
        .expect("quote should become ready");

    assert!(!quote.instrument_id.is_empty());
    assert!(!quote.datetime.is_empty());
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "tq-auth")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and uses the official built-in TqKq account"]
async fn live_tqkq_trade_login_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO").expect("account number should parse");

    let builder = SessionClientBuilder::new(auth_user, auth_pass);
    let session = if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .expect("live session should build");

    let trade_login = if let Some(number) = account_number {
        session
            .tqkq_login_command_numbered(number)
            .await
            .expect("numbered tqkq login should resolve")
    } else {
        session
            .tqkq_login_command()
            .await
            .expect("tqkq login should resolve")
    };
    let account_id = trade_login.account_id.as_str().to_string();

    session
        .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
        .await
        .expect("TradeLoginCommand should submit successfully");

    let account =
        wait_for_trade_account_ready(&session, account_id.as_str(), Duration::from_secs(30))
            .await
            .expect("trade account should become ready");

    assert_eq!(account.user_id, account_id);
    assert_eq!(account.currency, "CNY");
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(feature = "tq-auth")]
fn read_u8_env(name: &str) -> Result<Option<u8>, String> {
    let Some(raw) = read_env(name) else {
        return Ok(None);
    };
    raw.parse::<u8>()
        .map(Some)
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn build_market_session(
    auth_user: String,
    auth_pass: String,
    symbol: &str,
) -> tqsdk_session::SessionClient {
    let builder = SessionClientBuilder::new(auth_user, auth_pass);
    if is_stock_symbol(symbol) {
        builder.stock_market()
    } else {
        builder.futures_market()
    }
    .build()
    .expect("live session should build")
}

fn is_stock_symbol(symbol: &str) -> bool {
    symbol.starts_with("SSE.") || symbol.starts_with("SZSE.") || symbol.starts_with("BSE.")
}

fn first_symbol_info(payload: Value) -> Option<Value> {
    let payload = payload.get("result").unwrap_or(&payload);
    payload
        .get("multi_symbol_info")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .cloned()
}

fn option_level_count(levels: &tqsdk_session::OptionLevelQuotes) -> usize {
    levels.in_money.len() + levels.at_money.len() + levels.out_of_money.len()
}

fn read_f64_env(name: &str) -> Option<f64> {
    read_env(name).and_then(|value| value.parse().ok())
}

#[cfg(feature = "services")]
fn read_i32_env(name: &str) -> Option<i32> {
    read_env(name).and_then(|value| value.parse().ok())
}

#[cfg(feature = "services")]
fn is_edb_permission_error(error: &tqsdk_session::SessionFacadeError) -> bool {
    let message = error.to_string();
    message.contains("edb query failed") && message.contains("tqsdk-buy")
}

async fn wait_for_quote_update(
    session: &tqsdk_session::SessionClient,
    symbol: &str,
    timeout: Duration,
) -> Result<Quote, String> {
    let reader = session.reader().clone();
    let mut cursor = reader.cursor();
    let deadline = Instant::now() + timeout;

    loop {
        while reader.next(&mut cursor).is_some() {
            if let Some(quote) = reader
                .read()
                .decode_path::<Quote>(&["quotes", symbol])
                .map_err(|error| error.to_string())?
                && !quote.datetime.is_empty()
            {
                return Ok(quote);
            }
        }

        let now = Instant::now();
        if now >= deadline {
            return Err("timed out waiting for quote snapshot".to_string());
        }

        let progress = session
            .progress_once(Some(now + Duration::from_millis(250)))
            .await
            .map_err(|error| error.to_string())?;

        if !progress.is_progress() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(feature = "tq-auth")]
async fn wait_for_trade_account_ready(
    session: &tqsdk_session::SessionClient,
    account_id: &str,
    timeout: Duration,
) -> Result<Account, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(account) = session
            .reader()
            .read()
            .decode_path::<Account>(&["trade", account_id, "accounts", "CNY"])
            .map_err(|error| error.to_string())?
        {
            return Ok(account);
        }

        let now = Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade account snapshot".to_string());
        }

        let progress = session
            .progress_once(Some(now + Duration::from_millis(250)))
            .await
            .map_err(|error| error.to_string())?;

        if !progress.is_progress() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
