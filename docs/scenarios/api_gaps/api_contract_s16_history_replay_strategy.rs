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
//! `tqsdk-task::StrategyReplay` 已能消费 `tqsdk-data::MarketCacheReplay`
//! 的有序 quote/kline/tick cache event，并将它们推进到正常 runtime market
//! commit。回放策略通过同一个 `StrategyContext` 读取 quote/kline/tick、
//! account/position，并提交 typed task order。
//! `KlineDataSeries::into_market_cache_replay` 与
//! `TickDataSeries::into_market_cache_replay` 已提供 history series 到 cache
//! replay 的直接 adapter。
//! `StrategyReplayCheckpoint` / `StrategyReplayBuilder::resume_from` 已提供
//! 内存级 replay checkpoint 和 deterministic replay clock。
//! `StrategyReplaySpeed` / `StrategyReplayBuilder::speed` 已提供最快、
//! real-time 和 scaled replay speed policy。
//! `StrategyReplayCheckpointStore` / `StrategyReplayBuilder::resume_from_store`
//! 已提供 JSON file-backed durable checkpoint persistence foundation。
//! `StrategyReplaySourceBuilder` 已提供多序列 event source 合并入口。
//!
//! Remaining API gap:
//! `tqsdk-task::StrategyReplay` 已提供 history/cache replay -> strategy context
//! foundation。剩余 gap 是面向生产策略部署的统一 live/sim/replay
//! environment abstraction。
//!
//! Boundary decision:
//! 官方 `tqsdk-python` 的回测 / 复盘能力服务策略回放与研究，不承诺生产级 daemon
//! reconnect orchestration。`tqsdk-rust` 的 S16 核心边界止于历史/cache event
//! 按时间顺序驱动同一策略 context、speed policy 和 checkpoint foundation；
//! 生产部署、跨进程恢复和 daemon lifecycle 由 S15/S20 的薄 primitive 或用户层系统
//! 组合，不继续扩大 S16。
//!
//! 理想用户代码草案：
//! ```ignore
//! let replay = StrategyReplay::source_builder()
//!     .events(kline_series.into_market_cache_events("history")?)
//!     .events(tick_series.into_market_cache_events("history")?)
//!     .build();
//! let checkpoint_store = StrategyReplayCheckpointStore::json_file("replay.checkpoint.json");
//! run_strategy(replay, MyStrategy::default()).await?;
//! ```

fn main() {}
