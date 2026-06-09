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
- `step().await`
- `step_until(deadline).await`
- `WaitStep::{revision, current_dt, is_changing, is_changing_fields}`
- `quote(...).await`
- `quotes(...).await`
- `trading_status(...).await`
- `kline(...).await`
- `kline_ready(...).await`
- `tick(...).await`
- `tick_ready(...).await`
- `QuoteRef::{snapshot, load, changed_snapshot}`
- `KlineHandle::{is_ready, has_rows, row, window, rows, completed_rows, last, last_completed, rows_since, changed_rows}`
- `TickHandle::{is_ready, has_rows, row, window, rows, last, rows_since, changed_rows}`
- `startup_recovery()`
- `TqBacktest::{new, futures, stock}`
- `TqApiBuilder::{backtest, futures_backtest, stock_backtest}`
- `KlineWindow::{rows, into_rows, first, last, last_completed, completed_rows}`
- `TickWindow::{rows, into_rows, first, last}`
- `account(...)`
- `position(...)`
- `pre_insert_order(...)`
- `order(...)`
- `trade(...)`
- `risk_management_rule(...)`
- `risk_management_data(...)`
- `settlement_info(...)`
- `notification(...)`
- `security_account(...)`
- `security_position(...)`
- `security_order(...)`
- `security_trade(...)`
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

`TqBacktest` 和 `TqApiBuilder::{futures_backtest,stock_backtest}` 对应官方 Python
`TqApi(backtest=TqBacktest(...))` 的 wait 心智：同一段策略主体继续依赖
`quote` / `kline` handles 和 `step()` 推进，live 与 backtest 的差异只放在 builder
配置里。需要本地 `TqSim` 账户撮合和不连接真实服务的确定性回测时，使用
`tqsdk-task::StrategyBacktest` 搭配 `tqsdk-task::ReplayMarketSource`，不要把它塞进
wait facade。

## 设计边界

- market / trade 对象都只是状态树上的轻量 `Ref`
- 常见多合约实时行情入口使用 `quotes(...).await`，一次提交批量 quote 订阅并返回
  symbol-indexed `QuoteSet`；单合约 `quote(...)` 仍保留为便利入口。订阅意图由底层
  `SessionClient` 去重，避免 wait / stream 在同一 session 内重复提交或互相取消。
- 如果一个策略循环需要订阅大量合约甚至全市场，但消费模型仍然是单 owner 稳定截面，默认入口仍应是 wait facade 的 `quotes(...).await`。no-scan changed symbols / changed snapshots 属于 wait facade 的已接受后续优化方向，用来避免每轮扫描所有订阅合约；`tqsdk-stream` 不应成为单消费者 quote throughput 的默认答案。
- serial 数据先暴露为 Rust 原生窗口视图，而不是 DataFrame 兼容层
- `kline` / `tick` 对齐 Rust handle 心智：只提交 / 复用 `SetChart` 并返回滚动窗口 handle，不强制等待首批 rows；需要等待 chart 初始化时使用 `kline_ready` / `tick_ready`。数据来自同一棵 runtime state tree；它不读写 `tqsdk-data` 的 Python-compatible mmap 历史缓存，也不承担历史下载职责
- 多合约 K 线使用 `kline_multi([...], duration, data_length)`，一个 `chart_id` 对应逗号拼接的 `ins_list`。和 Python TqSdk 一样，多合约 K 线启动请求使用 `view_width=10000` 让服务端补齐足够窗口；客户端返回 `MultiKlineWindow`，以第一个合约为主合约，并通过主合约 K 线分区下的 `binding/{secondary}/{primary_id}` 把副合约行对齐到同一行。缺少任一副合约 binding 或 row 的主合约行不会进入窗口。
- Tick serial 不支持多合约。`tick("A,B", ...)` 会直接报错；需要多个合约 tick 时应分别创建多个单合约 `tick(...)` handle 或使用 stream/event 管线。
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
    let quotes = api.quotes(["SHFE.au2602", "DCE.m2609"]).await?;

    loop {
        let Some(step) = api.step().await? else { continue };
        for quote in quotes.iter() {
            if let Some(snapshot) = quote.changed_snapshot(&step)? {
                println!("{} {}", snapshot.instrument_id, snapshot.last_price);
            }
        }

    }
}
```

完整可编译示例见
[examples/api_contract_s34_batch_quote_subscription.rs](examples/api_contract_s34_batch_quote_subscription.rs)。

live/backtest same-body loop 的可编译契约示例见
[examples/api_contract_s36_wait_live_backtest_same_body.rs](examples/api_contract_s36_wait_live_backtest_same_body.rs)。

如果只需要一次 typed 行情快照，可以在示例本地封装一个很薄的 helper：先通过
`TqApi::quote(symbol).await` 创建 handle，再用 `step_until(deadline).await` 等待
`WaitStep::is_changing(&quote)` 并读取 `quote.load()`。该模式仍复用同一个底层
session 和 `RuntimeReader`，不会绕过 commit 边界。契约示例见
[examples/api_contract_s03_quote_snapshot.rs](examples/api_contract_s03_quote_snapshot.rs)。

实时交易状态、K 线 serial 和 tick serial 属于 wait continuous consumption：
用户通过 `trading_status`、`kline`、`tick` 获取 live handle；`kline` / `tick`
不阻塞等待 chart ready，严格等待路径使用 `kline_ready` / `tick_ready`。
多合约 K 线使用 `kline_multi` 获取 `MultiKlineHandle`；Tick 没有 multi 入口。
再在 `step()` / `step_until(...)` 后用 `WaitStep::is_changing()` /
`WaitStep::is_changing_fields()` 判断是否加载当前
typed status 或窗口。K 线 / Tick window 按对应 chart 的 `left_id` / `right_id`
投影 rows，不从全局缓存中截取最新 N 条；row-only diff 也会让对应 serial ref
报告变化。`KlineHandle::last()` / `TickHandle::last()` 是对当前窗口尾部的 owned
快照便利方法，`rows_since(last_seen_id)` 适合用户自己维护 row id 游标；
`row(id)` 和 `changed_rows(&WaitStep)` 只解码目标 row id，后者只解释当前已消费
commit 并返回本轮变化涉及的当前窗口 rows。`KlineHandle::is_ready()` /
`TickHandle::is_ready()` 可用于确认窗口初始化状态，窗口是否已有 rows 由
`has_rows()` 或 `window()?.is_empty()` 判断。
`MultiKlineHandle::window()` 返回按主合约 id 排序且截断到 `data_length` 的
`MultiKlineWindow`；每个 `MultiKlineRow` 通过 `get(symbol)` 读取主/副合约行。
K 线窗口或 handle 的 `completed_rows()` / `last_completed()` 用于跳过最新可变尾 bar。这不是
`tqsdk-data` 的历史下载或 mmap 缓存，也不是 `tqsdk-session` 的 metadata direct
query。契约示例见
[examples/api_contract_s25_wait_serial_trading_status.rs](examples/api_contract_s25_wait_serial_trading_status.rs)。

较少见的 trade/system live refs 也属于 wait facade：`NotificationRef`、
`SettlementInfoRef`、`RiskManagementRuleRef`、`RiskManagementDataRef` 都通过同一
runtime state tree 和 `WaitStep::is_changing()` 观察。`confirm_settlement` 是 wait 风格
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
