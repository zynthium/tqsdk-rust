use std::error::Error;

use tqsdk_data::{DataClient, OptionGreeksRequest};
use tqsdk_session::SessionClientBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let auth_user = std::env::var("TQ_AUTH_USER")?;
    let auth_pass = std::env::var("TQ_AUTH_PASS")?;
    let symbols = std::env::var("TQ_OPTION_SYMBOLS")
        .unwrap_or_else(|_| "SHFE.au2606C720".to_string())
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .enable_query()
        .build()?;
    let client = DataClient::from_session(session);
    let result = client
        .query_option_greeks(OptionGreeksRequest::new(symbols))
        .await?;

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

    Ok(())
}
