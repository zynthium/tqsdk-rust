//! Scenario: 历史行情回放
//!
//! User goal:
//! - 历史数据按时间顺序驱动同一套策略逻辑
//! - 回放速度可控
//! - 回放事件与实时事件类型一致
//!
//! API contract:
//! - history replay 是 public replay driver，不是用户手写 for-loop
//! - quote/tick/kline replay 输出标准 market event
//! - 策略无需区分 live event 和 replay event
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己把历史 K线改造成实时事件
//! - `ReplayCommand` 泄漏到策略逻辑
//! - `serde_json::Value`
//! - 多套 event schema
//!
//! Regression signal:
//! - 历史回放不能复用实时策略入口
//! - 回放推进和状态读取各自维护 revision
//! - 用户需要自己处理排序、时钟和暂停
//!
//! Review questions:
//! - 当前 API 是否自然表达历史回放驱动策略？
//! - 是否存在状态一致性风险？
//! - 应由 `tqsdk-data` 还是新的 replay/strategy facade 承接？
//!
//! Current API note:
//! `tqsdk-task::StrategyHost` 已提供策略侧应对齐的 context 形状：
//! 策略步骤通过同一个 context 读取 quote/position/account 并提交 typed
//! task order。
//!
//! Remaining API gap:
//! `tqsdk-data` 能拉取历史序列，`tqsdk-session` 有 replay control-plane
//! helper，但还没有把历史数据转成 `StrategyHost` 同构 update/context 的
//! public replay driver。后续 `HistoryReplay` 应复用这一策略 context，而不是
//! 让用户手写历史 K线到实时事件的转换循环。
//!
//! 理想用户代码草案：
//! ```ignore
//! let replay = HistoryReplay::new()
//!     .kline("SHFE.au2602", Duration::from_secs(60), start, end)
//!     .speed(ReplaySpeed::Fastest)
//!     .build()
//!     .await?;
//! run_strategy(replay, MyStrategy::default()).await?;
//! ```

fn main() {}
