# `tqsdk-python` 与 `tqsdk-rs` API 范式对比

## 文档定位
这份文档用于给后续 facade 设计提供一个稳定的选型参考。

目标不是评价哪个项目“更先进”，而是回答下面几个更具体的问题：

- 官方 `tqsdk-python` 的 `wait_update()` 范式到底强在哪里，代价又是什么？
- 现有 `tqsdk-rs` 的 async subscription / event-driven 范式到底做了哪些取舍？
- 这两种范式里，哪些应当进入当前的 `tqsdk-wait`，哪些应当保留给调用方自建 reader/cursor 消费层或可能的 `tqsdk-callback`？
- 哪些经验只能停留在 facade 层，不能反向污染 `tqsdk-core`？

## 调研范围

本次对比主要依据以下实现入口：

- `tqsdk-python`
  - `tqsdk/api.py`
  - `tqsdk/baseApi.py`
  - `tqsdk/channel.py`
- `tqsdk-rs`
  - `src/client/mod.rs`
  - `src/client/facade.rs`
  - `src/marketdata/mod.rs`
  - `src/series/api.rs`
  - `src/series/subscription.rs`
  - `src/trade_session/core.rs`
  - `src/trade_session/watch.rs`
  - `src/datamanager/watch.rs`

## 先给结论

### 结论 1
`tqsdk-python` 的核心范式不是“同步 API”，而是“单 owner、单推进点、单状态截面”的 `wait_update()` 驱动模型。

真正重要的不是它同步，而是：

- 所有命令都在同一个推进点真正发出
- 所有后台任务都在同一个推进点真正推进
- 所有 diff 都在同一个推进点真正并入状态树
- 用户在两次 `wait_update()` 之间看到的是同一个稳定截面

### 结论 2
现有 `tqsdk-rs` 不是纯 callback/fan-out SDK，而是一个更接近“async state refs + per-subscription waiting + event feeds”的混合范式。

它的典型接口不是：

- 一个全局唯一的 `wait_update()` 主循环

而是：

- `Client::wait_update()` / `wait_update_and_drain()`
- `QuoteRef::wait_update()`
- `KlineRef::wait_update()`
- `TickRef::wait_update()`
- `SeriesSubscription::wait_update()`
- `TradeSession::wait_update()`
- `TradeSession::{subscribe_events, subscribe_order_events, subscribe_trade_events}`

也就是说，它把“等待更新”拆散到了多个对象粒度上，同时再额外提供事件流。

### 结论 3
对当前 workspace 来说，两种范式都不应直接下沉进 `tqsdk-core`。

`tqsdk-core` 的职责仍然只能是：

- 统一命令模型
- 统一状态树
- 统一 commit / revision / causality
- `RuntimeReader + UpdateCursor` 读契约

后续 facade 层的正确方向应当是：

- `tqsdk-wait` 继承 Python 的“单推进点 + 单稳定截面”语义
- 调用方自建消费层 / `tqsdk-callback` 继承 Rust 的“多消费者异步等待 / 事件流”优势
- 但两者都只能消费同一个 `tqsdk-core`，不能重定义内核

## 范式 A: `tqsdk-python` 的 `wait_update()` 模型

### 控制流
官方 Python 的核心控制流非常集中：

1. 各类 getter / command 只是注册意图或返回 live object 引用
2. `wait_update()` 是唯一主推进点
3. 一次 `wait_update()` 内同时完成：
   - 推进内部 task
   - 实际发送待发送请求
   - 请求下一条服务端消息
   - 接收并合并新的 diff
   - 更新对象 / DataFrame / serial
4. 用户代码在 `wait_update()` 返回后，再读取同一批更新后的 live 对象

这背后的代码特征非常鲜明：

- `TqApi` 自己持有 event loop，见 `tqsdk/baseApi.py`
- task 的调度依赖 `wait_update()`，如果不调用，task 也不会继续跑
- `quote()`、`kline()` 返回的是随 runtime commit 更新的 handle / window
- `WaitStep::is_changing()` 根据“当前 step 消费到的 diff 集合”解释变化
- `register_update_notify()` 是对同一套更新语义的协程化观察接口，不是另一个并列 runtime

### 优点

- 用户心智非常统一。
  一条主循环就能覆盖行情、K 线、交易、任务、回测推进和对象刷新。

- 语义上的“稳定截面”非常强。
  对策略作者来说，两次 `wait_update()` 之间看到的是一棵已经完成合并的业务状态树。

- 命令可见性和提交边界明确。
  “指令会在下一次 `wait_update()` 发出” 这种语义虽然保守，但很稳定。

- `is_changing()` 很自然。
  因为它总是在解释“最近这一次已完成推进”里有哪些对象变了。

- 对单线程策略循环非常友好。
  对多数研究型/策略型用户来说，它几乎是最省认知成本的控制流。

### 缺点

- runtime ownership 很重。
  `TqApi` 自己管理 event loop，这种模式对 Python 用户方便，但对现代 async 组合并不友好。

- 并发模型不透明。
  task 虽然存在，但实际调度仍然被 `wait_update()` 驱动，异步只是附着在主循环外壳上。

- 原地更新对象 / DataFrame 带来额外复杂度。
  `is_changing()`、serial 更新、同步/异步 diff 区分、K 线范围跟踪，都需要额外补丁式逻辑来解释“这次变了什么”。

- 扩展多消费者和背压策略较困难。
  它默认服务的是“一个策略主循环 + 少量内部 task”，不是“多任务并行消费同一状态流”。

- 性能上天然偏向“全局推进”。
  对高性能、多消费者或高频结构化消费场景，不一定是最省开销的组织方式。

## 范式 B: `tqsdk-rs` 的 async subscription / event-driven 混合模型

### 控制流
现有 `tqsdk-rs` 的 facade 没有把所有消费都收束到一个全局主循环上。

它更接近下面这个组合：

- `Client`
  - 负责初始化 live context
  - 提供 `wait_update()` 和 `wait_update_and_drain()`
- `QuoteRef` / `KlineRef` / `TickRef`
  - 各自有独立等待、加载和变化判断方法
- `SeriesSubscription`
  - 自己启动 watch task
  - 自己提供 `wait_update()` / `snapshot()` / `load()`
- `TradeSession`
  - 提供 snapshot 型 `wait_update()`
  - 另有订单/成交的可靠事件流
- `DataManager`
  - 提供按路径的 watcher 能力，且是 best-effort / bounded channel

也就是说，它把“更新等待”拆成了多个对象级能力，并用 `watch` / `broadcast` / `async_channel` 等通道把这些能力连接起来。

### 优点

- async-native。
  不需要像 Python 那样把整个系统压进一个显式主循环。

- 多消费者友好。
  不同对象可以各等各的更新，不需要所有逻辑都围绕全局 `wait_update()` 串行组织。

- 按对象粒度等待更高效。
  `QuoteRef` 等对象只关心自己的 epoch 变化，不必为不相关对象的更新醒来。

- 背压策略更显式。
  例如 `DataManager` watcher 明确用了有界通道，并对持续不消费的 watcher 做丢弃处理。

- 事件流和状态流可以分层。
  交易会话里“账户/持仓用快照等待，订单/成交用可靠事件流”的分层，对于工程上拆责任是有价值的。

### 缺点

- 用户心智被拆散了。
  `Client::wait_update()`、`QuoteRef::wait_update()`、`SeriesSubscription::wait_update()`、`TradeSession::wait_update()` 并存，更新边界不再唯一。

- “全局稳定截面”的语义弱于 Python。
  Python 里一次 `wait_update()` 返回意味着“这一轮合并已经完成”；在 `tqsdk-rs` 里，不同对象是按各自 watcher / epoch 观察状态变化的。

- 跨域因果更难解释。
  当行情、交易、query、replay 等域未来都接进来时，仅靠对象级 watcher 很难稳定表达“这一次更新里到底完成了哪些跨域提交”。

- 资源模型更复杂。
  subscription 对象自己起 watch task、自己维护 channel、自己负责 close/drop 行为，这在 facade 层可以接受，但不适合作为 core contract。

- 容易形成“多套可见状态语义”。
  一部分用户走 `wait_update()`，一部分用户走 `snapshot()`，一部分用户走 event feed，一部分用户走 path watcher，长期会让 public surface 变宽且难以维护。

## 对比矩阵

| 维度 | `tqsdk-python` `wait_update` | 现有 `tqsdk-rs` async subscription |
| --- | --- | --- |
| 主控制流 | 单 owner、单推进点 | 多对象、多等待点 |
| 状态语义 | 强“稳定截面” | 偏对象局部一致性 |
| 更新解释 | 基于最近一次 `wait_update()` 的 diff | 基于 epoch / watcher / event feed |
| 命令发送语义 | 下一次 `wait_update()` 发出 | 多为接口立即触发订阅或异步动作 |
| 后台任务模型 | 从属于 `wait_update()` | 依赖 Tokio task / channel |
| 多消费者 | 弱 | 强 |
| 背压显式性 | 弱 | 强 |
| 策略作者易用性 | 极强 | 中等 |
| 系统工程组合性 | 中等 | 强 |
| 跨域因果一致性 | 强 | 需要额外设计 |
| facade 宽度控制 | 容易收束到单入口 | 容易演化成多入口 |

## 对当前 workspace 的启示

### 1. `tqsdk-core` 不应复制 Python 的 event loop ownership
Python 的单 owner、单推进点语义值得继承，但 “SDK 自己拥有 loop” 这件事不值得继承。

当前 `tqsdk-core` 已经明确要求调用方自带 runtime，这是对的，不能回退。

### 2. `tqsdk-core` 也不应复制 `tqsdk-rs` 的 watcher-first 结构
`tqsdk-rs` 的 watcher / event feed 适合做 facade，但如果把它们变成 core 的主 contract，会带来两个问题：

- 统一 commit / revision 语义会被对象级 epoch 观察稀释
- market / trade / replay / query / schema 的跨域一致性会变得更难表达

因此，watcher / fan-out 最多只能是 `RuntimeReader + UpdateCursor` 之上的二次消费层。

### 3. `tqsdk-wait` 应优先继承 Python 的语义，不应先继承 `tqsdk-rs` 的形状
`tqsdk-wait` 的第一目标不是“把 async Rust 用法包装得更自然”，而是：

- 提供一个全局 `wait_update()` 推进点
- 保证一次 `wait_update()` 之后用户看到的是同一个已消费 commit 边界
- 让 `is_changing()`、对象 ready、trade 命令可见性都建立在同一 commit 解释上

这件事更接近 Python，而不是更接近 `tqsdk-rs` 当前的对象级等待模型。

### 4. 自建消费层 / `tqsdk-callback` 可以吸收 `tqsdk-rs` 的工程经验
真正适合从 `tqsdk-rs` 继承的内容是：

- 按对象或按域的异步等待
- 有界 channel / 背压处理
- 状态流与可靠事件流分层
- 多消费者订阅生命周期管理

但这些能力应出现在调用方自建消费层 / 可能的 `tqsdk-callback`，而不是抢占 `tqsdk-wait` 的第一优先级。

这里还需要明确一条边界：

- 异步消费层负责的是 diff-backed 实时对象的消费形状
- 它不是 direct query / schema / metadata 的承载层
- 一次性 query/request-response 接口应继续留在 `tqsdk-session`

## 面向后续 crate 的建议

### 对 `tqsdk-wait`

- API 形状应当尽量集中在 `TqApi` 或等价单入口对象
- `wait_update()` 应成为唯一外部推进点
- getter 返回轻量句柄或 view，而不是自维护 watcher task 的独立 actor
- `is_changing()` 解释最近一次已消费 commit
- trade 命令与命令状态可见性应与 `wait_update()` 的推进边界保持一致

### 对自建异步消费层

- 可以提供按对象 / 按路径 / 按协议域的 event feed
- 可以按需引入背压与丢弃策略
- 可以对订单/成交保留“可靠事件流”而不是快照轮询
- 但 event feed 的更新边界应从 core commit 派生，而不是绕开 core 再建一套 epoch contract
- 不应重新暴露 direct query / schema / metadata facade；这些接口无论服务于研究员还是高性能用户，都应直接来自 `tqsdk-session`

### 对 `tqsdk-callback`

- callback 应视为 fan-out 的另一种消费包装
- callback 的触发条件同样应解释为“某个 commit 或 commit 的投影命中了订阅条件”
- 不应让 callback 直接持有 transport / websocket / raw diff 层能力

## 最终判断

如果目标是做一个“给策略作者用、并且尽量兼容官方 Python 语义”的 facade，优先级应该是：

1. 先做 `tqsdk-wait`
2. 语义上对齐 `tqsdk-python`
3. 实现上建立在 `tqsdk-core`
4. 工程经验上吸收 `tqsdk-rs`

如果目标是做一个“给高并发、多消费者、异步系统集成方用”的 facade，优先级应该是：

1. 使用 `tqsdk-session + RuntimeReader/UpdateCursor`
2. 参考 `tqsdk-rs` 的 subscription / event-driven 经验
3. 但仍然从 `tqsdk-core` 的 commit/revision 读面导出

一句话总结：

- `tqsdk-python` 更像“单推进点的稳定业务语义”
- `tqsdk-rs` 更像“异步多消费者的工程组织方式”
- 当前 workspace 应同时吸收两者，但必须把它们放在 facade 层，而不是 core 层
- direct query / schema / metadata 这类一次性接口则应始终停留在 `tqsdk-session`，不随 facade 风格漂移
