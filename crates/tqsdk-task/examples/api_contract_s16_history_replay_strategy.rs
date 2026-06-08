//! Scenario: 历史行情回放
//!
//! User goal:
//! - 历史行情按时间顺序驱动同一套策略逻辑
//! - DataClient 拉取的历史 K线直接进入标准 strategy context
//! - 回放策略可以复用 typed 下单与 fake broker 验证成交
//!
//! API contract:
//! - history series 到 task-owned replay event 的转换是 `tqsdk-task` public API
//! - 多个 history/replay event series 可通过 public replay source builder 合并
//! - history/replay source 是 public strategy replay driver，不是用户手写 runtime for-loop
//! - replay event 输出标准 kline/tick/quote 状态读取面
//! - replay context 暴露 deterministic replay time 和可恢复 checkpoint
//! - replay speed policy 是 public API，默认可选择最快回放
//! - replay checkpoint 可保存到 durable store 并恢复，不要求用户手写 JSON
//! - 策略无需区分 live market event 和 replay market event 的状态读取 API
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己把历史 K线改造成 runtime DIFF JSON
//! - `ReplayCommand` 或 provider 内部 protocol type 泄漏到策略逻辑
//! - `serde_json::Value`
//! - 多套 event schema
//!
//! Regression signal:
//! - 历史/cache 回放不能复用实时策略的 `StrategyContext`
//! - 回放推进和策略状态读取各自维护 revision
//! - 用户需要自己处理排序、runtime ingest 或后台任务
//!
//! Review questions:
//! - 当前 API 是否自然表达历史回放驱动策略？
//! - 是否存在状态一致性风险？
//! - replay time/checkpoint 是否作为 public contract 保持可读？
//! - replay speed policy 是否不要求用户手写 sleep / task 编排？
//! - checkpoint persistence 是否不泄漏 serde_json 或内部 runtime 状态？
//! - 多序列 replay 是否不要求用户手动排序和拼接？

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tqsdk_data::{DataClient, KlineDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;
use tqsdk_task::testing::{FakeBroker, FakeMarket};
use tqsdk_task::{StrategyReplay, StrategyReplayCheckpointStore, StrategyReplaySpeed};

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let duration = Duration::from_secs(60);
    let end = Utc::now();
    let start = end - ChronoDuration::hours(4);

    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .build()?;
    let client = DataClient::from_session(session);
    let series = client
        .get_kline_data_series(KlineDataSeriesRequest::new(
            symbol.clone(),
            duration,
            start
                .timestamp_nanos_opt()
                .ok_or("invalid start timestamp")?,
            end.timestamp_nanos_opt().ok_or("invalid end timestamp")?,
        ))
        .await?;
    let replay = StrategyReplay::source_builder()
        .kline_series(series, "history")?
        .build();
    let checkpoint_store =
        std::env::var("TQ_REPLAY_CHECKPOINT_FILE").map(StrategyReplayCheckpointStore::json_file);

    let mut strategy_builder = StrategyReplay::builder(replay)
        .market(
            FakeMarket::new()
                .account("sim", 100_000.0)
                .position("sim", symbol.as_str(), 0),
        )
        .broker(FakeBroker::new().fill_all())
        .account("sim")
        .kline(symbol.as_str(), duration, 64)
        .speed(StrategyReplaySpeed::FASTEST);
    if let Ok(store) = &checkpoint_store {
        strategy_builder = strategy_builder.resume_from_store(store)?;
    }
    let mut strategy = strategy_builder.build().await?;

    while let Some(mut ctx) = strategy.next().await? {
        let event = ctx.event();
        let checkpoint = ctx.checkpoint();
        println!(
            "replay source={} symbol={} event_time_ns={} replay_time_ns={} next_event_index={}",
            event.source(),
            event.symbol(),
            event.event_time_ns(),
            ctx.replay_time_ns(),
            checkpoint.next_event_index()
        );

        let last_row = ctx.kline(symbol.as_str(), duration)?.last().cloned();
        let position = ctx.position("sim", symbol.as_str())?;

        if let Some(row) = last_row
            && row.close.is_finite()
            && row.close > row.open
            && position.pos_long == 0
        {
            ctx.orders("sim")
                .buy_open(symbol.as_str(), 1)
                .limit(row.close)
                .send_once("history-replay-entry-1")
                .await?;

            let report = ctx.finish_test_step().await?;
            println!(
                "orders={} trades={} pos_long={}",
                report.orders().len(),
                report.trades().len(),
                report.position("sim", symbol.as_str())?.pos_long
            );
            if let Ok(store) = &checkpoint_store {
                store.save(ctx.checkpoint())?;
            }
            break;
        }

        if let Ok(store) = &checkpoint_store {
            store.save(ctx.checkpoint())?;
        }
    }

    Ok(())
}
