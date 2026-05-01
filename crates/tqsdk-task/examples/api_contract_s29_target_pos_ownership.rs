//! Scenario: TargetPosTask ownership
//!
//! User goal:
//! - Primary user layer: 执行工具用户；单策略作者
//! - Intended crate path: `tqsdk-task`
//! - 用 `TaskHost` 托管同一个 wait-style 推进点和目标持仓任务 ownership
//! - 同账户同合约只能有一个 `TargetPosTask` 或 `TargetPosScheduler` owner
//! - 需要直接下单时使用 `TaskHost::orders(...)` 或 wait 层 `OrderTicket`
//!
//! API contract:
//! - `TqApiBuilder::new(user, pass).futures_market().trade_target_tqkq().build().await?`
//!   可构建 futures market + TQKQ trade 的 wait facade
//! - `TaskHost::new(api)` 是执行工具入口
//! - `TaskHost::target_pos(...)` 创建 `TargetPosTask`，`TargetPosTask::set_target_volume(...)`
//!   表达目标持仓
//! - `TaskHost::check_manual_order_allowed(...)` 在任务持有同账户同合约 ownership 时拒绝手动下单
//! - 重复创建同账户同合约 owner 会失败，包括 task/task 与 task/scheduler 冲突
//! - `TaskHost::target_pos_scheduler(...)` 创建 `TargetPosScheduler`
//! - `TaskHost::wait_update(...)` 是 task 与 scheduler 的统一推进点
//! - `TargetPosTask::execution_events_since(...)` 返回 `(usize, Vec<...>)` 游标式增量事件，
//!   `execution_report()` 提供聚合报告
//!
//! Forbidden:
//! - 绕过 `TaskHost` 直接在任务运行时手动插单
//! - 在 `tqsdk-core` 中实现 TargetPos
//! - 跨账户 TargetPos orchestration
//! - 自动 hedge/flatten/补单策略
//! - durable audit/resume
//!
//! Regression signal:
//! - 同账户同合约可以同时启动多个目标持仓 owner
//! - `TaskHost::check_manual_order_allowed(...)` 在任务运行时仍允许手动下单
//! - scheduler 需要独立推进循环，无法通过 `TaskHost::wait_update(...)` 推进
//! - 用户必须解析内部 command/order 字符串才能读取执行事件
//!
//! Review questions:
//! - ownership 是否自然落在 `tqsdk-task`，而不是 core/session/wait？
//! - lower-level escape hatch 是否足够清晰：直接下单走 `TaskHost::orders(...)` 或 wait 层 `OrderTicket`？
//! - non-goal 是否足够明确：自动 hedge / flatten、跨账户 TargetPos 编排和 durable audit/resume 不进入核心 SDK？

use std::error::Error;
use std::time::Duration;

use tqsdk_core::{AccountId, RuntimeCommand, TradeCommand};
use tqsdk_task::{TargetPosScheduleStep, TaskError, TaskHost};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = read_optional_env("TQ_TASK_SYMBOL").unwrap_or_else(|| "SHFE.au2602".to_string());
    let allow_orders = env_flag("TQ_TASK_ALLOW_ORDERS");

    let api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target_tqkq()
        .build()
        .await?;
    let mut host = TaskHost::new(api);

    let mut account_id = read_optional_env("TQ_TASK_ACCOUNT_ID").unwrap_or_else(|| "TQKQ".into());

    if allow_orders {
        let trade_login = host.api().session().tqkq_login_command().await?;
        if std::env::var_os("TQ_TASK_ACCOUNT_ID").is_some() {
            ensure_tqkq_account(account_id.as_str(), &trade_login.account_id)?;
        } else {
            account_id = trade_login.account_id.as_str().to_string();
        }
        host.api()
            .session()
            .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
            .await?;
        wait_for_trade_account_ready(&mut host, account_id.as_str()).await?;
    }

    let task = host
        .target_pos(account_id.as_str(), symbol.as_str())
        .build()?;

    assert_ownership_conflict(
        host.target_pos(account_id.as_str(), symbol.as_str())
            .build(),
        "duplicate TargetPosTask owner",
    )?;
    assert_manual_order_blocked(
        host.check_manual_order_allowed(account_id.as_str(), symbol.as_str()),
        "manual order while TargetPosTask owns symbol",
    )?;
    assert_ownership_conflict(
        host.target_pos(account_id.as_str(), symbol.as_str())
            .build(),
        "duplicate TargetPosTask owner",
    )?;
    let mut task_event_cursor = 0_usize;
    if allow_orders {
        let target_volume = read_i64_env("TQ_TARGET_VOLUME")?;
        task.set_target_volume(target_volume)?;

        run_target_pos_loop(
            &mut host,
            &task,
            target_volume,
            symbol.as_str(),
            &mut task_event_cursor,
        )
        .await?;
    } else {
        let (next_cursor, events) = task.execution_events_since(task_event_cursor);
        task_event_cursor = next_cursor;
        println!(
            "dry_run ownership verified account_id={} symbol={} task_events={}",
            account_id,
            symbol,
            events.len()
        );
        println!("task_report={:?}", task.execution_report());
    }

    task.cancel().await?;
    drain_until_task_finished(&mut host, &task, &mut task_event_cursor).await?;

    assert_ownership_conflict(
        host.target_pos_scheduler(account_id.as_str(), symbol.as_str())
            .steps(vec![TargetPosScheduleStep::pause(Duration::from_millis(1))])
            .build(),
        "scheduler owner after TargetPosTask release",
    )?;

    let scheduler = host
        .target_pos_scheduler(account_id.as_str(), symbol.as_str())
        .steps(vec![TargetPosScheduleStep::pause(Duration::from_millis(1))])
        .build()?;
    assert_manual_order_blocked(
        host.check_manual_order_allowed(account_id.as_str(), symbol.as_str()),
        "manual order while TargetPosScheduler owns symbol",
    )?;
    let _updated = host.wait_update(Some(tokio::time::Instant::now())).await?;
    let (_scheduler_cursor, scheduler_events) = scheduler.execution_events_since(0);
    println!(
        "scheduler ownership verified finished={} events={} report={:?}",
        scheduler.is_finished(),
        scheduler_events.len(),
        scheduler.execution_report()
    );
    scheduler.wait_finished().await?;

    Ok(())
}

async fn run_target_pos_loop(
    host: &mut TaskHost,
    task: &tqsdk_task::TargetPosTask,
    target_volume: i64,
    symbol: &str,
    event_cursor: &mut usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for TargetPosTask to finish".into());
        }

        let _updated = host.wait_update(Some(now + Duration::from_secs(5))).await?;
        emit_task_events(task, event_cursor);

        if let Some(error) = task.last_error() {
            return Err(format!("TargetPosTask failed: {error}").into());
        }
        if task.is_finished() {
            println!("target task finished symbol={symbol} target_volume={target_volume}");
            break;
        }
    }

    Ok(())
}

async fn drain_until_task_finished(
    host: &mut TaskHost,
    task: &tqsdk_task::TargetPosTask,
    event_cursor: &mut usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !task.is_finished() {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for TargetPosTask ownership release".into());
        }
        let _updated = host.wait_update(Some(now + Duration::from_secs(1))).await?;
        emit_task_events(task, event_cursor);
    }
    task.wait_finished().await?;
    Ok(())
}

async fn wait_for_trade_account_ready(
    host: &mut TaskHost,
    account_id: &str,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let account = host.api().get_account(account_id);

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade account snapshot".into());
        }

        let _updated = host.wait_update(Some(now + Duration::from_secs(5))).await?;
        if let Some(snapshot) = account.snapshot(host.api())? {
            println!(
                "trade account ready user_id={} currency={} available={}",
                snapshot.user_id, snapshot.currency, snapshot.available
            );
            return Ok(());
        }
    }
}

fn emit_task_events(task: &tqsdk_task::TargetPosTask, cursor: &mut usize) {
    let (next_cursor, events) = task.execution_events_since(*cursor);
    *cursor = next_cursor;
    for event in events {
        println!("task_event={event:?}");
    }
}

fn assert_ownership_conflict<T>(
    result: tqsdk_task::Result<T>,
    context: &str,
) -> Result<(), Box<dyn Error>> {
    match result {
        Err(TaskError::OwnershipConflict { .. }) => Ok(()),
        Err(error) => Err(format!("{context}: expected ownership conflict, got {error}").into()),
        Ok(_) => Err(format!("{context}: expected ownership conflict").into()),
    }
}

fn assert_manual_order_blocked(
    result: tqsdk_task::Result<()>,
    context: &str,
) -> Result<(), Box<dyn Error>> {
    match result {
        Err(TaskError::ManualOrderBlocked { .. }) => Ok(()),
        Err(error) => Err(format!("{context}: expected manual order guard, got {error}").into()),
        Ok(()) => Err(format!("{context}: expected manual order guard").into()),
    }
}

fn ensure_tqkq_account(
    account_id: &str,
    login_account_id: &AccountId,
) -> Result<(), Box<dyn Error>> {
    if account_id == login_account_id.as_str() {
        return Ok(());
    }

    Err(format!(
        "TQ_TASK_ACCOUNT_ID={account_id} does not match TQKQ account {}; this contract only logs in the builder-selected TQKQ account",
        login_account_id.as_str()
    )
    .into())
}

fn read_env(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| format!("missing environment variable: {name}").into())
}

fn read_optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_i64_env(name: &str) -> Result<i64, Box<dyn Error>> {
    Ok(read_env(name)?.parse()?)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| value == "1")
}
