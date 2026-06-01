use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    let mut tq = Tq::futures().auth_env()?.connect().await?;
    let quote = tq.quote("{{SYMBOL}}").await?;

    while tq.next().await? {
        let snapshot = quote.load()?;
        println!("{} {}", snapshot.datetime, snapshot.last_price);
    }

    Ok(())
}
