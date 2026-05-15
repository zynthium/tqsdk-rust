use tqsdk_wait::TqApiBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let wait_once = std::env::var_os("TQ_WAIT_ONCE").is_some();
    let mut api = TqApiBuilder::new(user, pass).build().await?;
    let quote = api.quote("SHFE.au2602").await?;

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
