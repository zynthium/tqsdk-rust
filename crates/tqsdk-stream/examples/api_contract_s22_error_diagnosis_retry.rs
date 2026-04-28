//! Scenario: 错误诊断与重试（typed retry policy foundation）
//!
//! User goal:
//! - 区分连接错误、登录错误、业务拒单、交易错误
//! - 对可重试错误读取 typed retry hint
//! - 对 transport / HTTP 等底层错误执行一致的 retry decision / backoff loop
//! - 对不可重试错误给出明确诊断
//!
//! API contract:
//! - public error enum 有稳定分类和 retry hint
//! - trade reject 与 transport failure 不混在一个字符串里
//! - retry policy 可以把 typed diagnostic 转换成 typed decision
//! - retry runner 可处理底层 fallible operation 的 backoff loop
//! - 业务拒单仍通过 typed order/risk surface 判断，不被 transport retry 吞掉
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 字符串 contains 判断错误类别
//! - provider 私有 error type
//! - 业务代码手写指数退避任务
//! - `serde_json::Value` 作为诊断主入口
//!
//! Regression signal:
//! - 登录失败和网络断开只能返回同一种 opaque error
//! - 业务拒单被当作 transport retry
//! - retry 后 command/order correlation 丢失
//!
//! Review questions:
//! - 当前 API 是否自然表达错误诊断？
//! - 错误分类是否支持安全重试？
//! - 需要 API 微调还是 error model 局部重构？
//!
//! Current API note:
//! 本示例验证低层 transport/session/stream 诊断。业务拒单仍应通过
//! typed order/risk surface 判断；`StreamRetryPolicy` 只负责 stream-facing
//! fallible operation 的 retry decision / backoff runner，不接管 order intent
//! idempotency 或业务拒单审计。

use std::time::Duration;

use tqsdk_core::ContractError;
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::{StreamErrorKind, StreamFacadeError, StreamRetryPolicy};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let error = StreamFacadeError::Session(SessionFacadeError::from(ContractError::transport(
        "websocket recv failed",
    )));
    let diagnostic = error.diagnostic();
    let retry_policy = StreamRetryPolicy::new()
        .max_attempts(3)?
        .base_delay(Duration::from_millis(200))
        .max_delay(Duration::from_secs(2));

    let decision = retry_policy.decide(1, &error);
    if diagnostic.kind == StreamErrorKind::Transport && decision.should_retry() {
        println!(
            "retry decision={decision:?} diagnostic={}",
            diagnostic.message
        );
    }

    let mut attempts = 0;
    let report = retry_policy
        .base_delay(Duration::ZERO)
        .run(|attempt| {
            attempts = attempt;
            async move {
                if attempt < 2 {
                    Err(StreamFacadeError::Session(SessionFacadeError::from(
                        ContractError::http("temporary query timeout"),
                    )))
                } else {
                    Ok("query-ok")
                }
            }
        })
        .await?;

    println!(
        "result={} attempts={} retries={} last_attempt={}",
        report.value(),
        report.attempts(),
        report.retry_count(),
        attempts
    );

    Ok(())
}
