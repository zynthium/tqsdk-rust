//! Scenario: 错误诊断与重试
//!
//! User goal:
//! - 区分连接错误、登录错误、业务拒单、交易错误
//! - 对可重试错误执行重试
//! - 对不可重试错误给出明确诊断
//!
//! API contract:
//! - public error enum 有稳定分类和 retry hint
//! - trade reject 与 transport failure 不混在一个字符串里
//! - retry hint 可读取；完整 retry policy orchestration 保留为 gap
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
//! typed order/risk surface 判断，完整 retry policy orchestration 仍是 gap。

use tqsdk_core::{ContractError, RetryHint};
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::{StreamErrorKind, StreamFacadeError};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let error = StreamFacadeError::Session(SessionFacadeError::from(ContractError::transport(
        "websocket recv failed",
    )));
    let diagnostic = error.diagnostic();

    match diagnostic.retry_hint {
        RetryHint::RetryAfterReconnect => {
            if diagnostic.kind == StreamErrorKind::Transport {
                println!("retry after reconnect: {}", diagnostic.message);
            }
        }
        RetryHint::DoNotRetry => {
            println!("do not retry: {}", diagnostic.message);
        }
        RetryHint::RetryWithBackoff => {
            println!("retry with backoff: {}", diagnostic.message);
        }
    }

    Ok(())
}
