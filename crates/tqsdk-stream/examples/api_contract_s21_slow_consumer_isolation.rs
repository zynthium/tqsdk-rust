//! Scenario: 慢消费者隔离
//!
//! User goal:
//! - 写库 / 日志不能拖慢核心行情循环
//! - 慢消费者 lag 可见
//! - 核心策略消费者不受影响
//!
//! API contract:
//! - fan-out/backpressure 策略是 public config
//! - 每个 consumer 的 lag/drop policy 可独立配置
//! - 慢消费者错误不会影响 session driver
//! - 不要求用户自建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户手写 mpsc/broadcast channel 隔离写库
//! - 写库 future 直接 await 在核心行情循环里
//! - provider 私有 driver handle
//! - 手写 Tokio 后台任务编排
//!
//! Regression signal:
//! - 一个日志消费者 lag 导致策略消费者也丢事件
//! - lag 只能表现为 stream 关闭或卡住
//! - 用户必须自己 spawn 任务保护核心循环
//!
//! Review questions:
//! - 当前 API 是否自然表达慢消费者隔离？
//! - hot path 是否有性能风险？
//! - 应通过 stream config 微调还是新增 sink abstraction？
//!
//! Current API note:
//! 当前 `tqsdk-stream` 使用 bounded broadcast，并把 `Lagged` 显式暴露；
//! 这是正确底座，但还没有面向写库/日志这类 sink 的用户级隔离 API。
//!
//! 理想用户代码草案：
//! ```ignore
//! let stream = TqStreamBuilder::new(user, pass)
//!     .consumer("strategy", ConsumerPolicy::lossless())
//!     .consumer("warehouse", ConsumerPolicy::drop_oldest(10_000))
//!     .build()
//!     .await?;
//! stream.market_events().pipe("warehouse", SqlSink::new(pool)).await?;
//! ```

fn main() {}
