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
//! - retry policy 可配置并能审计
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
//! 当前有 `SessionFacadeError`、`WaitFacadeError`、`StreamFacadeError` 和
//! trade session event，但诊断/retry hint 还没有形成终端用户级统一 contract。
//!
//! 理想用户代码草案：
//! ```ignore
//! match order.send().await {
//!     Ok(ticket) => ticket.wait_terminal(&mut api).await?,
//!     Err(error) if error.is_retryable_transport() => retry_policy.retry(error).await?,
//!     Err(error) if error.is_business_reject() => return Err(error.explain()),
//!     Err(error) => return Err(error.into()),
//! }
//! ```

fn main() {}
