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

    let infos = session.query_symbol_info(&[symbol.as_str()]).await?;
    let info = infos
        .into_iter()
        .next()
        .ok_or("query_symbol_info returned no rows")?;

    println!(
        "{} {} tick={:?} volume_multiple={:?} day={:?} night={:?}",
        info.instrument_id,
        info.ins_class,
        info.price_tick,
        info.volume_multiple,
        info.trading_time.day,
        info.trading_time.night
    );

    Ok(())
}
