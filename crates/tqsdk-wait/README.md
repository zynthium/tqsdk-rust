# `tqsdk-wait`

`tqsdk-wait` 是建立在 `tqsdk-core + tqsdk-session` 之上的 Python 风格单推进点 facade。

它的职责很窄：

- 提供单 owner 的 `TqApi`
- 提供真实驱动 live session 的 `wait_update()` 语义和最近一次用户可见 commit 的变化解释
- 提供基于状态树的轻量对象引用与窗口视图
- 提供 trade 命令的 wait 风格薄包装

它明确不负责：

- GraphQL / HTTP direct query
- schema / metadata direct facade
- downloader / `TargetPosTask` / callback / stream
- 本地订单 overlay 或第二棵状态树

这些一次性接口的正确归属始终是 `tqsdk-session`，即使后续面向高性能用户增加 `tqsdk-stream` 也不改变这一点。

用户真正会持有和传递的 `Ref` / `Window` 类型都直接从 crate 根导出，不需要再通过 `refs::*` / `views::*` 子模块路径访问。

## 依赖方式

Cargo 包名是 `tqsdk-wait`，代码里的 crate 路径是 `tqsdk_wait`。

正式发布到 crates.io 前，workspace 外项目可以先使用 Git dependency：

```toml
[dependencies]
tqsdk-wait = { git = "https://github.com/zynthium/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

在本仓库内做 crate 间开发时使用 `path = "../tqsdk-wait"`；正式发布后把 Git
dependency 换成版本号即可。默认 feature 包含 live session 与 service query 支持。

## 当前公开面

当前 MVP 已包含：

- `TqApiBuilder`
- `TqApi`
- `QuoteRef`
- `TradingStatusRef`
- `KlineSerialRef`
- `TickSerialRef`
- `AccountRef`
- `PositionRef`
- `PreInsertOrderRef`
- `OrderRef`
- `TradeRef`
- `RiskManagementRuleRef`
- `RiskManagementDataRef`
- `SettlementInfoRef`
- `NotificationRef`
- `SecurityAccountRef`
- `SecurityPositionRef`
- `SecurityOrderRef`
- `SecurityTradeRef`
- `KlineWindow`
- `TickWindow`
- `ClientOrderId`
- `OrderPrice`
- `LimitOrderIntent`
- `OrderTicket`
- `OrderTicketState`
- `testing::WaitTestDriver`
- `wait_update(deadline).await`
- `is_changing(...)`
- `is_changing_fields(...)`
- `is_serial_ready(...)`
- `get_quote(...).await`
- `quote_snapshot(...).await`
- `startup_recovery()`
- `get_trading_status(...).await`
- `get_kline_serial(...).await`
- `get_tick_serial(...).await`
- `KlineWindow::{rows, into_rows, first, last, last_completed, completed_rows}`
- `TickWindow::{rows, into_rows, first, last}`
- `get_account(...)`
- `get_position(...)`
- `get_pre_insert_order(...)`
- `get_order(...)`
- `get_trade(...)`
- `get_risk_management_rule(...)`
- `get_risk_management_data(...)`
- `get_settlement_info(...)`
- `get_notification(...)`
- `get_security_account(...)`
- `get_security_position(...)`
- `get_security_order(...)`
- `get_security_trade(...)`
- `login_trade_account(...).await`
- `insert_order(..., OrderPrice).await`
- `insert_limit_order(...).await`
- `limit_order(...).client_intent(...).send_once().await`
- `cancel_order(...).await`
- `OrderRef::cancel(...).await`
- `OrderRef::cancel_remaining(...).await`
- `OrderRef::wait_partially_filled(...).await`
- `OrderRef::wait_partially_filled_until(...).await`
- `OrderRef::wait_terminal(...).await`
- `OrderRef::wait_terminal_until(...).await`
- `OrderTicket::cancel_remaining(...).await`
- `OrderTicket::wait_partially_filled(...).await`
- `OrderTicket::wait_partially_filled_until(...).await`
- `confirm_settlement(...).await`
- `session()`
- `into_session()`

`testing::WaitTestDriver` 只用于 deterministic fixture：持有 wait guard 以刻画
并发 `wait_update()` 防线，或把已生成 commit 放回下一次 `wait_update()` 前置队列。
普通用户代码不应把它当成运行时控制 API。

## 设计边界

- market / trade 对象都只是状态树上的轻量 `Ref`
- serial 数据先暴露为 Rust 原生窗口视图，而不是 DataFrame 兼容层
- `get_kline_serial` / `get_tick_serial` 对齐 Python live serial 心智：创建并持有滚动窗口引用，数据来自同一棵 runtime state tree；它不读写 `tqsdk-data` 的 Python-compatible mmap 历史缓存，也不承担历史下载职责
- `insert_order` / `insert_limit_order` / `cancel_order` / `confirm_settlement` 只提交到底层 command contract，不做本地伪造状态；其中 `insert_order` 使用 `OrderPrice` 明确表达 `any/best/five_level/limit` 语义，而不是接受 `serde_json::Value` 或魔法字符串
- `limit_order(...).client_intent(...).send_once()` 会把用户稳定 intent id 映射为 runtime `order_id`，并通过底层 `SessionClient` 的 session-scoped intent ledger 防止相同 intent 在同一 session 内重复提交；完整断线重连对账仍属于后续 session/runtime 一致性能力
- direct query / schema refresh / metadata 查询继续放在 `tqsdk-session`
- 如需在 wait facade 上直接落回这层 substrate，可通过 `api.session()` 访问底层 `SessionClient`
- `tqsdk-stream` 也只承载这同一批 diff-backed 对象的另一种消费形状，而不会接管 direct query

## 示例

```rust
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let mut api = TqApiBuilder::new(user, pass).build().await?;
    let quote = api.get_quote("SHFE.au2602").await?;

    loop {
        if !api.wait_update(None).await? {
            continue;
        }

        if api.is_changing(&quote)? {
            let snapshot = quote.load(&api)?;
            println!("{} {}", snapshot.datetime, snapshot.last_price);
        }
    }
}
```

完整可编译示例见 [examples/quote_wait.rs](examples/quote_wait.rs)。

如果只需要一次 typed 行情快照，而不想手写 `wait_update()` 循环，可以使用
`TqApi::quote_snapshot(symbol, deadline).await`。该 helper 会复用同一个底层
session 订阅 quote、等待带 `datetime` 的 ready snapshot，并保留用户可见的
`last_commit()` / `is_changing()` 截面解释不被内部等待破坏。契约示例见
[examples/api_contract_s03_quote_snapshot.rs](examples/api_contract_s03_quote_snapshot.rs)。

实时交易状态、K 线 serial 和 tick serial 属于 wait continuous consumption：
用户通过 `get_trading_status`、`get_kline_serial`、`get_tick_serial` 获取 live ref，
再在 `wait_update()` 后用 `is_changing()` / `is_changing_fields()` 判断是否加载当前
typed status 或窗口。K 线 / Tick window 按对应 chart 的 `left_id` / `right_id`
投影 rows，不从全局缓存中截取最新 N 条；row-only diff 也会让对应 serial ref
报告变化。`is_serial_ready()` 可用于确认窗口初始化状态，K 线窗口的
`completed_rows()` / `last_completed()` 用于跳过最新可变尾 bar。这不是
`tqsdk-data` 的历史下载或 mmap 缓存，也不是 `tqsdk-session` 的 metadata direct
query。契约示例见
[examples/api_contract_s25_wait_serial_trading_status.rs](examples/api_contract_s25_wait_serial_trading_status.rs)。

较少见的 trade/system live refs 也属于 wait facade：`NotificationRef`、
`SettlementInfoRef`、`RiskManagementRuleRef`、`RiskManagementDataRef` 都通过同一
runtime state tree 和 `is_changing()` 观察。`confirm_settlement` 是 wait 风格
trade command wrapper，默认示例运行不会提交确认命令。契约示例见
[examples/api_contract_s26_trade_system_refs.rs](examples/api_contract_s26_trade_system_refs.rs)。

证券 account/position/order/trade refs 使用独立 typed refs，同样属于 wait 的
diff-backed live state surface，不是 session direct query。契约示例见
[examples/api_contract_s26_security_trade_refs.rs](examples/api_contract_s26_security_trade_refs.rs)。

如果策略启动前需要同时确认行情订阅和交易初始同步完成，可以使用
`TqApi::startup_recovery()` 构造 typed barrier。它会提交 quote 订阅，并等待
指定交易账户的 account 对象和官方 `trade_more_data=false` 标记同时出现，业务代码
不需要手写多阶段 ready flag。契约示例见
[examples/api_contract_s09_startup_state_recovery.rs](examples/api_contract_s09_startup_state_recovery.rs)。

交易登录优先走 `TqApi::login_trade_account(...)`：builder 负责配置 trade route，
该 helper 负责提交 typed login request 并等待账户对象 ready，业务代码不需要构造
`TradeLoginCommand`。

如果需要直接提交市价 / 对手价 / 五档 IOC / 限价单，优先使用 typed `OrderPrice`：

```rust
use tqsdk_wait::OrderPrice;

api.insert_order(
    "sim",
    "SHFE.au2602",
    tqsdk_core::TradeDirection::Buy,
    Some(tqsdk_core::TradeOffset::Open),
    1,
    OrderPrice::best(),
)
.await?;
```

普通限价单如果需要稳定的下单意图 id，优先使用 intent builder：

```rust
let ticket = api
    .limit_order("sim", "SHFE.au2602")
    .client_intent("strategy-a-open-001")
    .buy_open(1)
    .at(618.0)
    .send_once()
    .await?;
```

`OrderTicket` 内部仍只是指向 runtime state tree 的 `OrderRef`，不会创建本地订单
overlay。`was_submitted()` 可以区分本次调用是否真的提交了新命令，便于 retry 代码
避免把同一个 intent 发送两次。intent ledger 由底层 `SessionClient` 持有，所以同一
session 被重新包装成新的 `TqApi` 后仍能识别已提交 intent；它不是跨进程持久化存储。
`OrderTicket::status()` / `wait_reconnect_safe_terminal*()` 会把 command ledger
和 order lifecycle 合并成 typed `OrderTicketState`，业务代码不需要解析
command status 字符串或 `order.status` 字符串。契约示例见
[examples/api_contract_s10_reconnect_order_consistency.rs](examples/api_contract_s10_reconnect_order_consistency.rs)。

订单撤单和状态等待优先走 `OrderRef` helper：`cancel_remaining()` 会保留订单归属
上下文，`wait_partially_filled()` / `wait_terminal()` 直接返回 typed `Order`，
业务代码不需要解析 `status` 字符串或手写 terminal-state 轮询。使用 stable
client intent 下单时，`OrderTicket` 也提供同名 partial-fill 和 cancel-remain
helper，避免用户在 reconnect-safe 路径上绕回内部 order handle。契约示例见
[examples/api_contract_s07_cancel_partial_fill.rs](examples/api_contract_s07_cancel_partial_fill.rs)。

如果要证明 wait facade 可以复用同一个底层 session 做 direct query，而不需要额外建第二个 client，可参考 [examples/quote_wait_with_session_query.rs](examples/quote_wait_with_session_query.rs)。

## Builder 边界

`TqApiBuilder` 只补一层和 wait facade 直接相关的便利配置，例如：

- `legacy compatibility: market_target(...)`
- `stock_market()`
- `futures_market()`
- `stock_backtest_market()`
- `futures_backtest_market()`
- `trade_target(...)`
- `trade_target_tqkq()`
- `trade_target_tqkq_numbered(<1..99>)`
- `trade_target_tqkq_stock()`
- `trade_target_tqkq_stock_numbered(<1..99>)`
- `trade_target_with_url(...)`
- `replay_url(...)`

优先使用这些命名方法，而不是直接写 `market_target(bool, bool)` 这种裸布尔组合。
`market_target(...)` 仅保留兼容用途，不应作为新的推荐入口。

如果需要更细的 session 级配置，例如 direct query、schema 或其他未来扩展项，应先配置 `tqsdk_session::SessionClientBuilder`，再通过 `TqApiBuilder::from_session_builder(...)` 包装成 wait facade。

如果已经持有 `TqApi`，但某个路径上又需要一次性 direct query / schema / metadata 调用，不需要再额外建立第二个 client，可以直接通过 `api.session()` 复用同一个底层 session。

如果 trade 侧默认就走官方内置模拟账户，实际登录命令也建议直接复用底层 session helper：

- `api.session().tqkq_login_command().await`
- `api.session().tqkq_login_command_numbered(<1..99>).await`
- `api.session().tqkq_stock_login_command().await`
- `api.session().tqkq_stock_login_command_numbered(<1..99>).await`
