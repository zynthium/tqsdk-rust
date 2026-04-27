//! Scenario: 错误诊断与重试（剩余 gap：retry orchestration）
//!
//! User goal:
//! - 区分连接错误、登录错误、业务拒单、交易错误
//! - 对可重试错误执行一致的重试策略
//! - 对不可重试错误给出明确诊断和审计记录
//!
//! API contract:
//! - low-level error kind / retry hint 已由 core/session/stream 提供
//! - retry policy orchestration 尚未形成 public API
//! - business reject 不应和 transport failure 混在一起
//! - 不要求用户手写指数退避任务
//!
//! Forbidden:
//! - 字符串 contains 判断错误类别
//! - provider 私有 error type
//! - retry 后 command/order correlation 丢失
//! - `serde_json::Value` 作为诊断主入口
//!
//! Regression signal:
//! - 登录失败和网络断开只能返回同一种 opaque error
//! - 业务拒单被当作 transport retry
//! - retry policy 只能散落在策略代码里
//!
//! Review questions:
//! - retry policy 应落在 session、stream 还是 task/daemon tooling？
//! - order intent retry 如何保留 idempotency 和 audit trail？
//! - business reject 与 transport retry 的统一报告如何表达？
//!
//! Current API note:
//! `ContractError::{kind, retry_hint}`、`SessionFacadeError::diagnostic()` 和
//! `StreamFacadeError::diagnostic()` 已覆盖低层诊断。业务拒单应通过 typed
//! order/risk surface 判断；本文件只保留统一 retry policy orchestration gap。
//!
//! 理想用户代码草案：
//! ```ignore
//! let retry = RetryPolicy::new()
//!     .transport_reconnect()
//!     .http_backoff(3)
//!     .do_not_retry_business_reject()
//!     .audit_to(audit_sink);
//!
//! retry.run_order_intent(intent, async || {
//!     api.limit_order("sim", "SHFE.au2602", Buy, Open, 1)
//!         .limit(480.0)
//!         .client_intent(intent)
//!         .send_once()
//!         .await
//! }).await?;
//! ```

fn main() {}
