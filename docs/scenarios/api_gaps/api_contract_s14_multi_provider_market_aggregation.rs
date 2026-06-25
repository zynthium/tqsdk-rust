//! Scenario: 多 provider 行情聚合
//!
//! User goal:
//! - 接入多个行情源
//! - 统一输出标准 quote/tick 事件
//! - 能选择主源、备用源、去重和时间戳策略
//!
//! API contract:
//! - provider aggregation 是显式 public abstraction
//! - 输出事件使用 SDK 标准 typed market event
//! - provider failure 不破坏单一用户事件语义
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己 spawn 多个 provider task
//! - 用户自己建 channel 合并事件
//! - provider 私有 protocol type 泄漏到策略层
//! - 多棵互不相关的状态树直接暴露给业务代码
//!
//! Regression signal:
//! - 每增加一个 provider 都要重写策略事件循环
//! - 去重/优先级/故障切换散落在用户代码
//! - 不同 provider 的 quote schema 不能统一
//!
//! Review questions:
//! - 当前 API 是否能自然表达多 provider 聚合？
//! - 是否需要跨 session 的 aggregated reader/revision contract？
//! - 这是局部 facade 扩展还是架构级新增能力？
//! - 该能力是否已经超出官方 Python SDK 的核心能力边界？
//!
//! Active gap status:
//! 仍保留在 `api_gaps/`。该能力未被当前 public API 支持，但也不是近期核心
//! SDK 目标；本文件只作为多 provider 基础设施边界样本，防止能力下沉到
//! core/session 或污染单 provider 用户路径。
//!
//! API gap:
//! 当前 public API 基本围绕单 `SessionClient` / 单 runtime reader。`tqsdk-core`
//! 有 aggregated reader 相关底座，但没有终端用户可用的 provider aggregation
//! facade。
//!
//! Boundary decision:
//! 官方 `tqsdk-python` 没有把多行情源聚合作为核心 public API。该场景当前降级为
//! 非核心用户层 / 基础设施能力，只保留 desired API sketch；不得作为近期
//! `tqsdk-rust` 核心 public API 目标推进，也不得下沉到 `tqsdk-core` /
//! `tqsdk-session`。
//!
//! 理想用户代码草案：
//! ```ignore
//! let mut market = MarketAggregator::new()
//!     .primary(SessionClientBuilder::new(user, pass).futures_market())
//!     .fallback(CustomProvider::new(...))
//!     .dedupe_by_exchange_timestamp()
//!     .quote("SHFE.au2602")
//!     .build()
//!     .await?;
//!
//! while let Some(event) = market.next().await.transpose()? {
//!     println!("provider={} price={}", event.provider, event.quote.last_price);
//! }
//! ```

fn main() {}
