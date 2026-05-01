//! Archived gap sketch: 2026-05-01
//!
//! This scenario is now naturally expressible by the formal compiled example
//! `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`.
//! The remaining business/order retry audit scope was explicitly downgraded to
//! user-side execution tooling, so this sketch is kept only as historical
//! context.
//!
//! Scenario: 错误诊断与重试（降级能力：business/order retry audit）
//!
//! User goal:
//! - 区分连接错误、登录错误、业务拒单、交易错误
//! - 对可重试错误执行一致的重试策略
//! - 对不可重试错误给出明确诊断和审计记录
//!
//! API contract:
//! - low-level error kind / retry hint 已由 core/session/stream 提供
//! - stream-facing retry decision / backoff runner 已由 `tqsdk-stream` 提供
//! - business reject 不应和 transport failure 混在一起
//! - order intent retry audit 不属于 stream/core 诊断层核心 API
//! - 不要求用户在 stream-facing operation 中手写指数退避任务
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
//! - low-level diagnostics / retry hint 回退为字符串判断
//!
//! Review questions:
//! - order/business retry audit 应落在 wait、task 还是 daemon tooling？
//! - order intent retry 如何保留 idempotency 和 audit trail？
//! - business reject 与 transport retry 的统一报告如何表达？
//!
//! Current API note:
//! `ContractError::{kind, retry_hint}`、`SessionFacadeError::diagnostic()` 和
//! `StreamFacadeError::diagnostic()` 已覆盖低层诊断。`StreamRetryPolicy` 已覆盖
//! stream-facing retry decision / backoff runner。业务拒单应通过 typed order/risk
//! surface 判断；order/business retry audit 已降级为用户执行审计系统职责，不再
//! 作为 `tqsdk-stream` 核心 public API 继续推进。
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
