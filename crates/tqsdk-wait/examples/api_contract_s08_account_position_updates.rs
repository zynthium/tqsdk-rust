//! Scenario: 账户 / 资金 / 持仓查询
//!
//! User goal:
//! - 读取账户资金快照
//! - 读取某合约持仓快照
//! - 接收后续资金 / 持仓增量变化
//!
//! API contract:
//! - 账户和持仓是 typed live refs
//! - 初始 ready 和后续 change checks 共享同一 `wait_update()` 截面
//! - 不要求用户读取底层 state path
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `serde_json::Value`
//! - `StatePath`
//! - provider 内部 session / protocol type
//! - 手写账户状态 cache
//!
//! Regression signal:
//! - 用户必须通过字符串路径读 `trade/{account_id}/...`
//! - 账户 ready 需要手写底层 command
//! - 增量和快照来自不同状态源
//!
//! Review questions:
//! - 当前 API 是否自然表达账户/持仓 live ref？
//! - 是否暴露内部路径？
//! - 是否存在状态一致性风险？
//!
//! Current API note:
//! `TqApi::get_account()` 和 `TqApi::get_position()` 能表达 typed refs；
//! 主要缺口在交易登录/账户 ready 仍偏底层，导致完整用户代码样板偏高。

fn main() {}
