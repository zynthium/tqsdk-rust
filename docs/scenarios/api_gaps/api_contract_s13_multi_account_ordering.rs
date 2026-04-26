//! Scenario: 多账户下单
//!
//! User goal:
//! - 同一策略按比例向多个账户下单
//! - 每个账户状态隔离
//! - 汇总执行结果
//!
//! API contract:
//! - 多账户是 typed account group，而不是字符串 account_id 列表
//! - 每个账户订单、成交、持仓和错误隔离可追踪
//! - 支持比例、最小手数、失败处理策略
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 在业务代码里循环多个 `insert_order`
//! - 用共享 `HashMap` 拼账户执行状态
//! - 字符串判断订单状态或错误类型
//! - `RuntimeCommand::Trade`
//!
//! Regression signal:
//! - 一个账户拒单导致其他账户 outcome 无法解释
//! - 比例拆单、尾差和风控散落在用户代码
//! - 多账户状态相互污染
//!
//! Review questions:
//! - 当前 API 是否自然表达多账户执行？
//! - 是否有状态隔离和资金安全风险？
//! - 应通过 task 局部扩展还是新增 portfolio execution API？
//!
//! API gap:
//! builder 能声明多个 trade target，但 task/wait API 没有面向终端用户的
//! multi-account execution group、比例拆单和 per-account outcome。
//!
//! 理想用户代码草案：
//! ```ignore
//! let accounts = host.accounts()
//!     .add("sim-a", Ratio::new(7, 10))
//!     .add("sim-b", Ratio::new(3, 10))
//!     .build()
//!     .await?;
//! let outcome = host
//!     .multi_account_order(accounts)
//!     .buy_open("SHFE.au2602", 10)
//!     .limit(480.0)
//!     .send()
//!     .await?
//!     .wait_finished(&mut host)
//!     .await?;
//! ```

fn main() {}
