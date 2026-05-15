//! Scenario: 零门槛行情订阅
//!
//! User goal:
//! - 创建 SDK client
//! - 订阅一个合约
//! - 打印最新 quote
//!
//! API contract:
//! - 只使用 `tqsdk-wait` 面向终端用户的 public API
//! - 不引用 provider 私有模块
//! - 不手动处理协议连接、心跳、重连
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `serde_json::Value`
//! - provider 内部 session / protocol type
//! - 手写 Tokio 后台任务编排
//!
//! Regression signal:
//! - 示例明显变长
//! - 必须理解底层协议字段
//! - 必须手动管理异步任务或共享状态
//!
//! Review questions:
//! - 当前 API 是否自然表达该场景？
//! - 是否暴露内部细节？
//! - 是否存在状态一致性或性能风险？

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

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;
    let quote = api.quote(&symbol).await?;

    loop {
        let Some(step) = api.step().await? else {
            continue;
        };

        if step.is_changing(&quote) {
            let snapshot = quote.load()?;
            println!(
                "symbol={} datetime={} last_price={}",
                snapshot.instrument_id, snapshot.datetime, snapshot.last_price
            );

            if wait_once {
                break;
            }
        }
    }

    Ok(())
}
