//! Scenario: 高频交易柜台低延迟 profile
//!
//! User goal:
//! - 在一个可审计的低延迟循环里消费行情、同 revision 读取 market/trade 状态、
//!   运行 typed risk gate 并提交订单。
//! - hot path 使用 `tqsdk-session + RuntimeReader` 分区读面，不进入 data/history cache。
//! - 慢日志和落盘通过 stream sidecar managed sink 隔离，不阻塞行情/下单循环。
//!
//! API contract:
//! - `TradingDeskProfile` 是 task 层薄 profile，构建于 shared `SessionClient`。
//! - `next_market_event(deadline)` 消费同一 runtime commit/cursor 语义。
//! - `read_market_trade_state()` 返回同 revision 的 market + trade 分区读 guard。
//! - `precheck_order(&state, intent, client_order_id)` 在该 guard 上运行
//!   `RiskEngine::check_report_on_state` / `project_order_on_state`。
//! - `submit_prechecked_order(...)` 通过 typed task order intent 注册 session
//!   client order id，并提交 runtime trade command。
//! - `TradingDeskOrderTicket::status(&desk)` 返回 typed order state，不要求解析字符串。
//! - `TradingLatencyProbe` / cycle / report 是 typed API，缺 marker 返回 `None`。
//! - `tqsdk-stream` sidecar 可以用 managed sink + WAL/journal 承载慢消费者隔离，
//!   但 stream sink 不进入 trading desk public profile。
//!
//! Forbidden:
//! - hot path import 或调用 `tqsdk-data` / history mmap cache。
//! - hot loop 每 tick 做 full snapshot clone。
//! - 字符串判断 command/order/trade status。
//! - 慢日志/落盘 future await 在行情/下单主循环。
//! - 用户手写 channel、unbounded queue 或 `Arc<Mutex<_>>` 隔离慢消费者。
//!
//! Regression signal:
//! - 低延迟用户必须回到 `TqApi::wait_update()` 或手写 provider 私有 order packet。
//! - 风控和下单读取不同 revision。
//! - 慢 sink 导致 hot loop lag 或丢失风险不可见。
//! - 延迟只能靠日志字符串定位。
//!
//! Review questions:
//! - profile 是否仍是 session/reader hot-path 薄层，而不是策略平台或 OMS？
//! - stream sidecar 是否只作为 slow-consumer isolation，而不进入 task profile public API？
//! - 订单状态和 latency report 是否保持 typed contract？

use std::time::Duration;

use tqsdk_core::{RuntimeCommand, SharedCommitResult, TradeCommand, TradeDirection, TradeOffset};
use tqsdk_session::SessionClientBuilder;
use tqsdk_stream::{
    StreamCommitJournal, StreamSinkFuture, StreamSinkProfile, StreamSinkRetryPolicy,
    StreamSinkWalFsyncPolicy, TqStream,
};
use tqsdk_task::{RiskEngine, TaskOrderIntent, TradingDeskProfile, TradingLatencyProbe};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_DESK_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let account_id = std::env::var("TQ_DESK_ACCOUNT_ID").unwrap_or_else(|_| "TQKQ".into());
    let limit_price = std::env::var("TQ_DESK_LIMIT_PRICE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(480.0);
    let allow_order = std::env::var("TQ_DESK_ALLOW_ORDER").ok().as_deref() == Some("1");

    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .trade_target_tqkq()
        .build()?;
    let login = session.tqkq_login_command().await?;
    let login_command = session
        .submit(RuntimeCommand::Trade(TradeCommand::Login(login.clone())))
        .await?;
    session.wait_command_completed(login_command).await?;

    let stream_sidecar = TqStream::with_commit_channel_capacity(session.clone(), 16_384)?;
    let wal_path = std::env::temp_dir().join("tqsdk-s31-desk-sink.jsonl");
    let journal_path = std::env::temp_dir().join("tqsdk-s31-desk-journal.jsonl");
    let sink_options = StreamSinkProfile::reliable_jsonl(wal_path.clone(), journal_path.clone())
        .retry_policy(StreamSinkRetryPolicy::limited(3)?)
        .fsync_policy(StreamSinkWalFsyncPolicy::EveryRecord)
        .into_options();
    let audit_sink = stream_sidecar.spawn_commit_sink_with_options(
        "desk-audit",
        write_audit_commit,
        sink_options,
    )?;

    let risk = RiskEngine::new()
        .max_order_volume(1)
        .max_net_position(3)
        .max_price_deviation(20.0);
    let mut desk = TradingDeskProfile::builder(session.clone())
        .subscribe_quotes([symbol.as_str()])
        .risk_engine(risk)
        .latency_probe(TradingLatencyProbe::enabled())
        .build()
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while let Some(event) = desk.next_market_event(Some(deadline)).await? {
        let mut latency = event.into_latency_cycle();
        let state = desk.read_market_trade_state();
        let quote = state
            .market_state()
            .quote(&tqsdk_core::Symbol::new(symbol.clone()))?;
        let position = state.trade_state().position(
            &tqsdk_core::AccountId::new(login.account_id.as_str()),
            &tqsdk_core::Symbol::new(symbol.clone()),
        )?;

        if let Some(cycle) = &mut latency {
            cycle.mark_decision();
        }
        let should_submit = allow_order && quote.is_some() && position.is_some();
        if should_submit {
            let intent = TaskOrderIntent {
                account_id: account_id.clone(),
                symbol: symbol.clone(),
                direction: TradeDirection::Buy,
                offset: Some(TradeOffset::Open),
                volume: 1,
                limit_price: Some(limit_price),
            };
            let prechecked = desk.precheck_order(&state, intent, "s31-desk-order-001")?;
            if let Some(cycle) = &mut latency {
                cycle.mark_risk();
            }
            drop(state);

            let ticket = desk.submit_prechecked_order(prechecked).await?;
            if let Some(cycle) = &mut latency {
                cycle.mark_submit();
            }
            let status = ticket.status(&desk)?;
            if let Some(cycle) = &mut latency {
                cycle.mark_ack();
                if let Some(report) = cycle.report() {
                    println!(
                        "latency rev={} total={:?} submit_to_ack={:?}",
                        report.revision().get(),
                        report.total(),
                        report.submit_to_ack()
                    );
                }
            }
            println!(
                "desk ticket client_order_id={} status={:?}",
                ticket.client_order_id(),
                status.state()
            );
            break;
        }
        drop(state);

        if tokio::time::Instant::now() >= deadline {
            break;
        }
    }

    let sink_report = audit_sink.shutdown().await?;
    println!(
        "desk audit sink={} processed={} flushed={}",
        sink_report.name(),
        sink_report.stats().processed_commits(),
        sink_report.flushed()
    );
    let journal_records = StreamCommitJournal::new().read_jsonl(&journal_path)?;
    println!("desk audit journal records={}", journal_records.len());

    Ok(())
}

fn write_audit_commit(commit: SharedCommitResult) -> StreamSinkFuture {
    Box::pin(async move {
        println!("audit revision={}", commit.revision.get());
        Ok(())
    })
}
