use serde_json::{Value, json};
use tqsdk_core::{QueryCommand, QueryId, RuntimeCommand};
use tqsdk_session::SessionClientBuilder;

const QUERY_SYMBOL_INFO: &str = r#"query($instrument_id:[String]){
  multi_symbol_info(instrument_id: $instrument_id) {
    ... on basic {
      instrument_id
      class
      price_tick
    }
  }
}"#;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_QUERY_SYMBOL").unwrap_or_else(|_| "SSE.000300".to_string());

    if !is_stock_symbol(symbol.as_str()) {
        return Err("query_command_wait uses the official stock websocket query route, so TQ_QUERY_SYMBOL must be a stock symbol".into());
    }

    let session = SessionClientBuilder::new(user, pass)
        .stock_market()
        .enable_query()
        .build()?;
    let query_id = QueryId::new("example-symbol-info");
    let command_id = session
        .submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: query_id.clone(),
            query: QUERY_SYMBOL_INFO.to_string(),
            variables: Some(json!({ "instrument_id": [symbol] })),
        }))
        .await?;

    session.wait_command_completed(command_id).await?;

    let payload = session
        .query_result(query_id.as_str())?
        .ok_or("query completed without a result payload")?;
    let instrument = first_symbol_info(payload).ok_or("query payload missing multi_symbol_info")?;

    println!(
        "{} {} tick={}",
        instrument
            .get("instrument_id")
            .and_then(Value::as_str)
            .unwrap_or(""),
        instrument
            .get("class")
            .and_then(Value::as_str)
            .unwrap_or(""),
        instrument
            .get("price_tick")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
    );

    Ok(())
}

fn first_symbol_info(payload: Value) -> Option<Value> {
    let payload = payload.get("result").unwrap_or(&payload);
    payload
        .get("multi_symbol_info")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .cloned()
}

fn is_stock_symbol(symbol: &str) -> bool {
    symbol.starts_with("SSE.") || symbol.starts_with("SZSE.") || symbol.starts_with("BSE.")
}
