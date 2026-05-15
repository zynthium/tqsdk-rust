use tqsdk_session::SessionClientBuilder;
use tqsdk_wait::TqApiBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let wait_once = std::env::var_os("TQ_WAIT_ONCE").is_some();

    let session_builder = SessionClientBuilder::new(user, pass).enable_query();
    let mut api = TqApiBuilder::from_session_builder(session_builder)
        .build()
        .await?;

    let metadata = api.session().query_symbol_info(&[symbol.as_str()]).await?;
    let instrument = metadata
        .first()
        .ok_or("query_symbol_info returned no rows")?;
    println!(
        "metadata {} {} tick={}",
        instrument.instrument_id, instrument.ins_class, instrument.price_tick
    );

    let quote = api.quote(&symbol).await?;

    loop {
        let Some(step) = api.step().await? else {
            continue;
        };

        if step.is_changing(&quote) {
            let snapshot = quote.load()?;
            println!("{} {}", snapshot.datetime, snapshot.last_price);
            if wait_once {
                break;
            }
        }
    }

    Ok(())
}
