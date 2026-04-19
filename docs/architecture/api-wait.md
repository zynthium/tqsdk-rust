# `tqsdk-api-wait` 专题设计

## 文档定位
本文档是 `tqsdk-rs` 在分层架构路线中的 `V2 tqsdk-api-wait` 专题设计，专门细化：

- `wait_update()` 在 Rust 中的等价语义
- `snapshot()`、`is_changing()`、`Revision` 可见性如何协同
- 初始化、超时、重连在 `wait` 范式下如何定义
- `TqApi`、`QuoteView`、`QuoteSnapshot` 等 V2 公共接口应长什么样

相关文档：

- [总架构入口](README.md)
- [runtime-core 总览](runtime-core/overview.md)
- [协议交互](runtime-core/protocol-flow.md)
- [数据契约](runtime-core/data-contracts.md)
- [类型约束](runtime-core/type-system.md)
- [验收与测试矩阵](validation.md)

## 本页覆盖范围
本页专注于 `tqsdk-api-wait` 这一层，主要覆盖：

- 用户端 `TqApi` 入口设计
- `QuoteView`、`QuoteSnapshot` 等公共类型
- `wait_update()` / `snapshot()` / `is_changing()` 的协同语义
- wait 范式下的时序、状态机与 Python TqSdk 接口对照

## 核心目标与非目标
### 核心目标
- 兼容 Python TqSdk 最重要的策略语义，尤其是 `wait_update()` 的一致性边界、对象视图稳定性、`is_changing()` 的判定直觉。
- 在不破坏语义的前提下，保留 Rust 的并发安全、性能与工程可维护性。
- 让熟悉 TqSdk 的用户能理解“Rust 版为什么 API 不完全一样，但语义仍然可靠”。

### 非目标
- 首版不追求 Python TqSdk 所有表层 API 的 1:1 对齐。
- 首版不追求一次性覆盖全部账户类型、工具链和回测能力。
- 本文档不再承担总架构蓝图角色，runtime kernel 细节以 `docs/architecture/` 下各专题页为准。

## 原版 TqSdk 的关键语义
### `wait_update()` 的真实职责
在 Python TqSdk 中，`wait_update()` 不只是“等待下一条消息”，而是一次完整的系统推进边界。一次调用通常会串联：

- 推进内部异步任务
- 发送累计的订阅、下单、撤单等请求
- 接收来自服务端的新消息
- 合并 diff 到本地状态
- 在提交完成后让用户看到一个新的稳定数据截面

因此 Rust 版不能把 `wait_update().await` 简化成单纯 `Notify` 包装器，它代表的是“系统完成了一轮已提交状态”。

### 数据一致性语义
TqSdk 的策略模型默认依赖这样一个事实：两次 `wait_update()` 之间，用户读取到的行情、K 线、订单、持仓、账户等对象在逻辑上属于同一轮状态，不会在读取过程中被局部更新打断。

### `is_changing()` 的语义
`is_changing()` 更接近：本次 `wait_update()` 返回所代表的这轮提交，是否触及了某个对象或字段。它是按轮次判定的问题，而不是单靠当前对象版本号就能完整表达的问题。

## 兼容性契约
### `wait_update()` 的等价定义
Rust 版建议将 `wait_update().await -> bool` 定义为：

- 返回 `true` 表示系统已经完成至少一轮新的状态提交
- 返回后，所有对外可见视图都对应同一轮已提交状态
- 返回 `false` 表示超时或本轮未形成新的用户可见提交

### 必须显式约定的 4 个子语义
1. 初始截面是否算更新
   - 首个可读状态不一定等价于首次 `is_changing() == true`
2. 订阅完成与字段齐全的关系
   - 对象句柄可创建，不等于字段已完整可读
3. 超时与半提交
   - `wait_update(timeout)` 返回 `false` 时，不应把半完成状态暴露给用户
4. 重连后的状态切换
   - 重连后的首次可见状态必须以新的完整提交轮次对外出现

### `is_changing()` 的契约要求
- 支持对象或字段粒度判断本轮提交是否命中
- 对 K 线新 bar、同 bar 多次补丁、多合约对齐等场景有一致语义
- 能区分“初始截面可见”与“本轮发生变化”
- 在未来支持同步和异步访问风格时，语义不飘移

## 对外 API 设计
### 设计原则
- 保留 TqSdk 的使用心智：围绕 `TqApi`、`wait_update()`、行情对象、账户对象、订单对象写策略
- 采用 Rust 风格只读视图：返回 `View + Snapshot`，而不是共享可变对象
- 用异步替代阻塞：`wait_update()`、下单、撤单等操作使用 `async/await`
- 兼容性优先于表层相似：优先保证语义正确

### 核心入口：`TqApi`
```rust
pub struct TqApi {
    // 内部共享状态句柄
}

impl TqApi {
    pub async fn connect_live(url: &str, auth: Auth) -> Result<Self>;
    pub async fn connect_sim(url: &str, sim: TqSim, auth: Auth) -> Result<Self>;
    pub async fn backtest(config: BacktestConfig) -> Result<Self>;

    pub async fn wait_update(&self, timeout: Option<Duration>) -> bool;

    pub fn quote(&self, symbol: &str) -> QuoteView;
    pub fn tick_serial(&self, symbol: &str, len: usize) -> TickSerialView;
    pub fn kline_serial(&self, symbol: &str, duration: Duration, len: usize) -> KlineSerialView;
    pub fn account(&self) -> AccountView;
    pub fn position(&self, symbol: &str) -> PositionView;
    pub fn order(&self, order_id: &str) -> OrderView;
    pub fn orders(&self) -> OrdersView;

    pub fn is_changing<T: ChangeTarget>(&self, target: &T, field: Option<&str>) -> bool;

    pub async fn insert_order(&self, req: InsertOrderRequest) -> Result<OrderHandle>;
    pub async fn cancel_order(&self, order_id: &str) -> Result<()>;
}
```

关键判断：

- `TqApi` 应是可 clone 的轻量句柄，而不是 `&mut self` 驱动的大状态机对象
- `is_changing()` 继续保留在 `TqApi` 上，以保持 TqSdk 心智模型

### 行情对象：`QuoteView + QuoteSnapshot`
```rust
#[derive(Clone)]
pub struct QuoteView {
    // api handle + symbol
}

#[derive(Clone, Debug)]
pub struct QuoteSnapshot {
    pub revision: Revision,
    pub symbol: String,
    pub datetime: Option<String>,
    pub last_price: Option<f64>,
    pub ask_price1: Option<f64>,
    pub bid_price1: Option<f64>,
    pub ask_volume1: Option<i64>,
    pub bid_volume1: Option<i64>,
}

impl QuoteView {
    pub fn snapshot(&self) -> Option<QuoteSnapshot>;
}
```

用户使用方式：

```rust
let api = TqApi::connect_live(url, auth).await?;
let quote = api.quote("SHFE.cu2406");

loop {
    if !api.wait_update(None).await {
        continue;
    }

    if api.is_changing(&quote, Some("last_price")) {
        if let Some(snap) = quote.snapshot() {
            println!("{:?}", snap.last_price);
        }
    }
}
```

### 账户、持仓、订单视图
- `AccountView`
- `PositionView`
- `OrderView`
- `OrdersView`

都建议统一成 `View + Snapshot` 风格，以保证：

- 行情与交易状态读取方式一致
- 同一轮 `wait_update()` 后，用户能在同一逻辑截面中读取 quote、position、account、order
- `is_changing()` 可统一作用于这些对象

### 交易 API
```rust
pub struct InsertOrderRequest {
    pub symbol: String,
    pub direction: Direction,
    pub offset: Offset,
    pub volume: i64,
    pub price: Price,
}

pub enum Price {
    Market,
    Limit(f64),
}

pub struct OrderHandle {
    pub order_id: String,
}
```

### 回测 API
```rust
pub struct BacktestConfig {
    pub start: chrono::DateTime<chrono::Utc>,
    pub end: chrono::DateTime<chrono::Utc>,
}
```

## 核心公共类型
### `ChangeTarget`
```rust
pub trait ChangeTarget {
    fn change_key(&self) -> ChangeKey;
}
```

### `TqApi`
`TqApi` 是用户侧唯一核心入口。它对用户是轻量共享句柄，对内部则是 runtime kernel 与状态仓的访问门面。

### `QuoteView` 与 `QuoteSnapshot`
- `QuoteView` 是轻量引用对象，可以长期持有
- `QuoteSnapshot` 是值对象，代表一次显式读取的稳定快照
- `snapshot()` 返回 `Option`，用于表达“句柄已创建，但数据尚未初始化完成”

### `InsertOrderRequest`
请求对象优于长位置参数列表，更利于未来扩展多账户、条件单和风控字段。

### `OrderHandle`
下单直接返回轻量句柄，后续配合 `OrderView::snapshot()` 获取完整状态。

### `BacktestConfig`
回测入口配置对象，用于保持回测与实盘在上层编程模型上的一致性。

## wait 范式的时序与状态机
### 一次成功更新的标准时序
1. runtime 收到一个或多个输入
2. runtime 组装本轮待提交补丁
3. 调用 `StateStore::commit(...)`
4. `StateStore` 合并数据、计算真实变更、生成新的 `ChangeSet`
5. 若存在用户可见变化，则推进 `Revision`
6. commit 完成后，`UpdateWaiter` 发布新的 revision
7. `wait_update()` 被唤醒并返回 `true`
8. 用户调用 `snapshot()` 读取该 revision 下的稳定状态
9. 用户调用 `is_changing()` 查询该 revision 对应的 `ChangeSet`

### `wait_update()` 的状态机
```text
Idle
  -> WaitingForRevision
  -> WokenByCommittedRevision
  -> VisibleRevisionAdvanced
  -> Return(true)

Idle
  -> WaitingForRevision
  -> Timeout
  -> Return(false)
```

这里区分：

- 内部最新 revision：runtime 最近一次完成提交后的 revision
- 调用方可见 revision：当前 `TqApi` 已对用户暴露的 revision

建议语义：

- `wait_update(true)` 成功返回时推进调用方可见 revision
- `wait_update(false)` 不推进调用方可见 revision
- `is_changing()` 永远只针对最近一次成功 `wait_update()` 暴露出来的 revision

### `snapshot()` 与 `is_changing()` 的协同语义
必须保证：

1. `wait_update(true)` 使调用方可见 revision 前进
2. `is_changing()` 查询的是这个新 revision 对应的 `ChangeSet`
3. `snapshot()` 读到的是这个新 revision 的状态投影

### 首次初始化、超时、重连
- 初始化完成后，用户可以读取首份状态，但不应默认将其视为一轮业务更新命中
- 超时只表示本次没有成功拿到新的可见 revision，不表示系统内部完全静止
- 重连恢复后，必须先形成新的完整或足够一致的状态提交，再允许 `wait_update()` 返回 `true`

## 与 Python TqSdk 的接口对照
| Python TqSdk | Rust tqsdk-rs 建议接口 | 说明 |
| :--- | :--- | :--- |
| `api = TqApi(...)` | `let api = TqApi::connect_live(...).await?` | 连接过程显式异步化 |
| `quote = api.get_quote(symbol)` | `let quote = api.quote(symbol)` | 返回轻量 view |
| `api.wait_update()` | `api.wait_update(timeout).await` | 语义相同，调用方式异步化 |
| `api.is_changing(quote, "last_price")` | `api.is_changing(&quote, Some("last_price"))` | 保留原有判定心智 |
| `quote.last_price` | `quote.snapshot().map(|q| q.last_price)` | 用快照替代共享可变对象 |
| `api.insert_order(...)` | `api.insert_order(req).await?` | 用请求对象替代长参数列表 |
| `TqBacktest(...)` | `TqApi::backtest(config).await?` | 回测作为另一种 runtime 入口 |

## Phase 1 与后续边界
首阶段真正需要落地的只有最小兼容闭环：

- `TqApi::new_test()` 或 `TqApi::connect_live(...)`
- `TqApi::wait_update(...)`
- `TqApi::quote(...)`
- `QuoteView::snapshot()`
- `TqApi::is_changing(&quote, field)`

后续阶段再逐步补齐：

- `tick_serial()`
- `kline_serial()`
- `account() / position() / order()`
- `insert_order() / cancel_order()`
- `TargetPosTask`
- `backtest()`
