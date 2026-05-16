use std::time::Duration;

use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let mut api = TqApiBuilder::new(user, pass).build().await?;
    let quote = api.quote("{{SYMBOL}}").await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while let Some(step) = api.step_until(Some(deadline)).await? {
        if step.is_changing(&quote) {
            let snapshot = quote.load()?;
            println!("{} {}", snapshot.datetime, snapshot.last_price);
        }
    }

    Ok(())
}
