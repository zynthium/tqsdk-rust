//! Scenario: 启动后状态恢复
//!
//! User goal:
//! - 登录后恢复订阅
//! - 同步订单 / 成交 / 持仓 / 资金
//! - 在第一轮业务决策前得到一致初始截面
//!
//! API contract:
//! - SDK 提供明确的 startup recovery barrier
//! - market subscriptions 与 trade state sync 都有 typed ready signal
//! - 用户不需要知道 route/pending-route/replay 细节
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `SessionRuntime::recover`
//! - `RuntimeCommand`
//! - 手写多阶段 ready flag
//! - 业务代码自建状态恢复 cache
//!
//! Regression signal:
//! - 策略必须在多个 stream 中猜测“是否恢复完成”
//! - 启动后第一笔下单可能基于不完整持仓
//! - 订阅恢复和交易状态恢复没有同一个 barrier
//!
//! Review questions:
//! - 当前 API 是否自然表达启动恢复？
//! - 是否有状态一致性或资金安全风险？
//! - 缺口应由 facade 微调还是架构新增 recovery surface？
//!
//! API gap:
//! 当前底层 runtime 有 recover/resync 能力，但 public wait/stream facade 没有
//! “恢复完成后再交给策略”的 typed barrier。
//!
//! 理想用户代码草案：
//! ```ignore
//! let stream = TqStreamBuilder::new(user, pass)
//!     .futures_market()
//!     .trade_target_tqkq()
//!     .build()
//!     .await?;
//! let account = stream.login_default_trade_account().await?;
//! let recovered = stream
//!     .recover_state()
//!     .quotes(["SHFE.au2602", "SHFE.ag2602"])
//!     .trade_account(account.id())
//!     .await?;
//! assert!(recovered.market_ready && recovered.trade_ready);
//! ```

fn main() {}
