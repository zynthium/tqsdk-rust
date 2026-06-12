#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk::Result<()> {
    tracing_subscriber::fmt::init();
    println!("Connecting to Relay for Market Data and SIM account for Trading...");

    let mut tq = Tq::futures()
        .market_relay("ws://127.0.0.1:7788/")
        .auth_env()?
        .trade_target_tqkq()
        .connect()
        .await?;

    let symbol = "KQ.m@SHFE.au";

    // 获取模拟交易的账号 ID
    let account_id = tq.tqkq_account_id().await?;

    // 获取目标持仓管理器，以及账户和持仓状态
    let target = tq.target_pos_tqkq(symbol).await?;
    let account = tq.account(&account_id);
    let position = tq.position(&account_id, symbol);

    let mut target_set = false;
    let mut closing = false;
    let mut initial_printed = false;

    // 事件循环
    while tq.next().await? {
        if !initial_printed {
            if let Ok(acc) = account.load() {
                println!("Initial Account Balance: {}", acc.balance);
                initial_printed = true;

                // If it's the weekend or off-hours, we might not get quotes
                println!("Waiting for the first quote of {}...", symbol);
                println!("(Note: If the market is currently closed, no live quotes will arrive.)");
            }
        }

        // As a demonstration of trade execution, we don't wait for quotes.
        // We will just place an order right away since the market might be closed right now.
        if initial_printed && !target_set {
            println!(
                "[{}] Executing test order. Opening a long position of 1 lot.",
                symbol
            );
            target.set(1)?;
            target_set = true;
        }

        let Ok(pos) = position.load() else { continue };

        if target_set && pos.pos_long == 1 && !closing {
            println!(
                "[{}] Order filled. Current pos: 1. Now closing position...",
                symbol
            );
            target.set(0)?;
            closing = true;
        }

        if closing && pos.pos_long == 0 {
            if let Ok(acc) = account.load() {
                println!("Test finished. Final Account Balance: {}", acc.balance);
            }
            break;
        }
    }

    Ok(())
}
