# `tqsdk-stream` 最小 API 草图

## 文档定位
本文档描述的是建立在 `tqsdk-core + tqsdk-session` 之上的 Rust async-native continuous-consumption facade。

它的目标不是复制 `tqsdk-rs` 现有宽 public surface，也不是把 callback、task、direct query 重新揉进一个大而全的 `TqApi`。

当前这份文档只回答三个问题：

- `tqsdk-stream` 的最小 canonical API 应该长什么样
- 第一版最小实现应该先交付什么，不该交付什么
- 它如何在不污染 `tqsdk-core` 提交模型的前提下，提供多消费者异步消费能力

相关文档：

- [总架构入口](README.md)
- [crate 边界审计](crate-boundaries.md)
- [未来 crate 蓝图](crate-blueprint.md)
- [Python / Rust facade 范式对比](facade-paradigms.md)
- [wait facade 设计](api-wait.md)

## 设计目标

- 提供 Rust async-native 的连续 commit 消费形状
- 保持和 `tqsdk-wait` 相同的底层语义来源：`RuntimeReader + UpdateCursor + SessionClient`
- 允许多消费者各自独立推进，不强制单 owner `wait_update()`
- 保留高性能用户直接读取共享状态树的能力
- 不复制第二棵状态树
- 不把 direct query / schema / metadata 搬进来

## 非目标

第一版 `tqsdk-stream` 明确不负责：

- GraphQL / HTTP query
- schema refresh / metadata / calendar / settlement / ranking 这些 one-shot query
- callback facade
- `TargetPosTask`
- downloader / DataFrame / polars
- 自己维护 object cache / watcher registry / 第二棵状态树

## 先给结论

推荐的最小设计不是“先做很多对象级 stream”，而是：

1. 先提供一个共享 session 驱动的 `CommitStream`
2. 再暴露共享 `RuntimeReader` / `SessionClient` 作为读面与逃生舱
3. 让后续的对象级 stream、路径过滤、trade 可靠事件流都建立在同一条 commit fan-out 之上

换句话说，第一版 `tqsdk-stream` 的最小稳定内核应当是：

- 一个 `TqStreamBuilder`
- 一个 `TqStream`
- 一个 `CommitStream`
- 一个显式的 lag / closed error surface

而不是一开始就铺开：

- `QuoteStream`
- `KlineStream`
- `OrderStream`
- `TradeEventStream`
- path watcher
- callback bridge

这些能力都应该建立在最小 commit stream 先稳定之后再往上叠。

## 为什么不从对象级 stream 起步

### 方案 A：commit-first

形状：

```rust
let stream = TqStreamBuilder::new(user, pass).build().await?;
let mut commits = stream.commit_stream()?;

while let Some(update) = commits.next().await {
    let commit = update?;
    let snapshot = stream.reader().read();
    // 用户自己决定读哪些对象
}
```

优点：

- public surface 最小
- 和 `tqsdk-core` 的 commit/revision 语义完全一致
- 后续对象级 facade、过滤器、事件流都能建立其上
- 不需要一开始就决定“对象级 stream 到底返回 commit、返回 snapshot、还是返回 typed value”

缺点：

- 初期对终端用户不够便利
- 需要调用方自己根据 commit 和 state tree 解释变化

### 方案 B：对象级 stream-first

形状：

```rust
let quote = stream.quote_stream("SHFE.au2602")?;
let order = stream.order_stream("sim", "order-1")?;
```

优点：

- 用户更直观
- 更接近现有 `tqsdk-rs` 的某些使用形状

缺点：

- 一开始就必须冻结大量 API 形状
- 容易把对象缓存、订阅生命周期、过滤语义、背压策略一起绑死
- 容易过早把 crate 做宽

### 方案 C：可靠事件流-first

形状：

```rust
let mut trades = stream.trade_events("sim")?;
```

优点：

- 对交易场景很有吸引力

缺点：

- 会过早把“状态流”和“事件流”的分层绑死
- 对 market/query/schema/replay 不形成统一消费主线

### 推荐

第一版应选择方案 A：`commit-first`。

原因不是它最方便，而是它最稳，且最符合你当前对底座的要求：

- 精简
- 稳定
- 高性能
- 先锁定真正的公共抽象，再叠加便利层

## 最小 canonical API

### builder

```rust
pub struct TqStreamBuilder {
    inner: tqsdk_session::SessionClientBuilder,
}

impl TqStreamBuilder {
    pub fn new(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self;
    pub fn from_session_builder(inner: SessionClientBuilder) -> Self;

    pub fn market_target(self, stock: bool, backtest: bool) -> Self;
    pub fn trade_target(
        self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> Self;
    pub fn trade_target_with_url(
        self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
        trade_url: impl Into<String>,
    ) -> Self;
    pub fn replay_url(self, replay_url: impl Into<String>) -> Self;

    pub async fn build(self) -> tqsdk_stream::Result<TqStream>;
}
```

设计意图：

- 和 `tqsdk-wait::TqApiBuilder` 保持相似建造路径
- 继续复用 `SessionClientBuilder`
- 不在 stream builder 重新定义 direct query 选项

### root facade

```rust
pub struct TqStream { /* private */ }

impl TqStream {
    pub fn new(session: SessionClient) -> Self;

    pub fn session(&self) -> &SessionClient;
    pub fn into_session(self) -> SessionClient;
    pub fn reader(&self) -> &RuntimeReader;

    pub fn commit_stream(&self) -> tqsdk_stream::Result<CommitStream>;
    pub fn path_stream<T, I, S>(&self, path: I) -> tqsdk_stream::Result<PathValueStream<T>>
    where
        T: DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: Into<String>;
    pub fn quote_stream(&self, symbol: impl AsRef<str>)
        -> tqsdk_stream::Result<PathValueStream<Quote>>;
}
```

设计意图：

- `session()` 是 one-shot query / raw command / direct-query 的 escape hatch
- `reader()` 保留高性能用户直接读共享状态树的权利
- `commit_stream()` 是第一版唯一必须稳定的 continuous-consumption 入口
- `path_stream()` 是最薄的 typed decode 便利层
- `quote_stream()` 只是 `path_stream()` 在行情对象上的第一个包装

### commit stream

```rust
pub struct CommitStream { /* private */ }

impl futures::Stream for CommitStream {
    type Item = tqsdk_stream::Result<tqsdk_core::CommitResult>;
}

impl CommitStream {
    pub fn filter_path<I, S>(self, path: I) -> PathCommitStream
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;

    pub fn filter_paths(self, paths: impl IntoIterator<Item = StatePath>)
        -> PathCommitStream;

    pub fn filter_scope(self, scope: CommitScope) -> ScopeCommitStream;
    pub fn filter_scopes(
        self,
        scopes: impl IntoIterator<Item = CommitScope>,
    ) -> ScopeCommitStream;

    pub fn filter_domain(self, domain: ProtocolDomain) -> DomainCommitStream;
    pub fn filter_domains(
        self,
        domains: impl IntoIterator<Item = ProtocolDomain>,
    ) -> DomainCommitStream;

    pub fn filter_object(self, object: ObjectKey) -> ObjectCommitStream;
    pub fn filter_objects(
        self,
        objects: impl IntoIterator<Item = ObjectKey>,
    ) -> ObjectCommitStream;

    pub fn filter_fields<I, S>(self, object: ObjectKey, fields: I)
        -> FieldCommitStream
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;
}
```

其中 `Result` 的 error surface 在第一版应显式覆盖：

- session 驱动错误
- stream receiver lagged
- stream closed
- 非 Tokio runtime 中启动 driver

建议对应一个小而硬的错误枚举：

```rust
pub enum StreamFacadeError {
    Session(tqsdk_session::SessionFacadeError),
    Lagged { skipped: u64 },
    Closed,
    InvalidState(&'static str),
}
```

注：

- `Lagged` 是 stream facade 自己的 fan-out lag，不是 `tqsdk-core` cursor lag
- 这两者必须区分开

### typed path stream

```rust
pub struct ValueUpdate<T> {
    pub commit: CommitResult,
    pub value: T,
}

pub struct PathValueStream<T> { /* private */ }

impl<T> futures::Stream for PathValueStream<T>
where
    T: DeserializeOwned,
{
    type Item = tqsdk_stream::Result<ValueUpdate<T>>;
}
```

设计意图：

- 不引入第二棵状态树
- 不把 typed stream 的推进点从 commit fan-out 分叉出去
- typed stream 只是“收到匹配 commit 后，用同一个 `RuntimeReader` 立即 decode”
- 若调用方需要更低开销或更细粒度控制，仍然可以直接使用 `CommitStream + reader()`

## 第一版实现边界

### 必须先实现

- `TqStreamBuilder`
- `TqStream`
- 单个共享 driver task
- 基于 commit fan-out 的 `CommitStream`
- 显式 lag / closed 错误
- `session()` / `reader()` 逃生舱

### 这一版先不实现

- `kline_stream(...)`
- `tick_stream(...)`
- `order_stream(...)`
- `trade_events(...)`
- callback bridge
- trade command thin wrappers

其中：

- path / scope / domain / object / field 过滤现在已经可以作为 commit stream 的薄组合层实现
- 对象级 stream 与可靠事件流仍应等 commit 级过滤语义先稳定，再继续叠加

## 内部驱动模型

`TqStream` 的内部驱动应复用 `tqsdk-wait` 已验证过的 session 推进顺序：

1. 先尝试从 `RuntimeReader::next()` 读取已有 commit
2. 若没有，再 `flush_outbound()`
3. 再尝试读取 commit
4. 再 `drive_pending_once()`
5. 再尝试读取 commit
6. 最后 `drive_route_once(None)`，等待远端事件

区别只在于：

- `tqsdk-wait` 把 commit 交给单 owner `wait_update()`
- `tqsdk-stream` 把 commit 发到内部 fan-out channel，再让每个消费者独立接收

也就是说：

- commit 生成逻辑不变
- state tree 不变
- revision 推进不变
- 只是消费形状从“pull by wait loop”变成“push into stream channel”

## 背压模型

第一版推荐使用：

- 单个 driver task
- 单个 bounded broadcast ring
- 每个 `commit_stream()` 调用者持有自己的 receiver

最小语义应当是：

- 慢消费者落后时，返回 `Lagged`
- 不为慢消费者阻塞整个 session 驱动
- 不为每个订阅者维护独立 cursor + 独立 route 驱动

为什么第一版不做更复杂的 path/object fan-out：

- 因为对象级过滤在第一版还不是稳定边界
- 先用 commit-level fan-out 锁住主数据流，再决定更细粒度投影

## 与 `tqsdk-wait` 的关系

`tqsdk-stream` 和 `tqsdk-wait` 是并列 facade，不是上下层关系。

两者共享：

- `SessionClient`
- `RuntimeReader`
- `UpdateCursor`
- 同一棵状态树
- 同一套 `CommitResult`

两者不同：

- `tqsdk-wait` 是单 owner、单推进点、稳定截面优先
- `tqsdk-stream` 是多消费者、异步 fan-out、组合性优先

因此第一版不应该为了复用而直接依赖 `tqsdk-wait`。

如果后续发现两边确实有稳定共享的 `Ref` / filter / projection 抽象，再单独抽公共层；在那之前，不要过早提炼一个“wait+stream 通用 facade core”。

## 与 `tqsdk-session` 的关系

`tqsdk-stream` 继续把 `tqsdk-session` 视为共享 session substrate。

边界保持不变：

- direct query / schema / metadata 继续留在 `tqsdk-session`
- `tqsdk-stream` 只负责 diff-backed continuous consumption

所以 `TqStream::session()` 的意义只是复用同一个底层 session，而不是把 direct query API 重新归属到 stream crate。

## 第一版建议的代码布局

推荐最小文件布局：

```text
crates/tqsdk-stream/
  src/
    lib.rs
    builder.rs
    api.rs
    driver.rs
    error.rs
  tests/
    stream_surface.rs
    stream_commit_flow.rs
    support/
```

各文件职责：

- `builder.rs`
  - `TqStreamBuilder`
- `api.rs`
  - `TqStream`
  - `CommitStream`
- `driver.rs`
  - 后台 pump task
  - 启动/关闭/单实例保护
- `error.rs`
  - facade 级错误类型
- `tests/*`
  - surface / driver / lag 语义

## 第一版验收标准

如果第一版最小实现完成，至少应能验证：

1. 同一个 `TqStream` 可以创建多个 `commit_stream()` receiver
2. 一个 receiver 消费到的 commit revision 顺序与 `RuntimeReader` 一致
3. receiver 落后时会显式报 `Lagged`
4. `reader()` 可以在收到 commit 后读到对应 revision 的状态
5. `session()` 仍可用于 direct query / raw submit 复用同一 session
6. 整个实现不需要回改 `tqsdk-core` 的 commit 生成逻辑

## 后续增量方向

在最小 commit stream 稳定之后，下一批最自然的增量是：

### 第二批

- `CommitStream` 的 path / scope / domain / object / field 过滤已经落地

### 第三批

- `path_stream<T>()` 与 `quote_stream()` 这种最薄 typed stream 已经开始落地
- 下一步是补 `trading_status` / `kline` / `tick` / `order` 等 typed stream family
- futures / securities 对象级投影

### 第四批

- trade 可靠事件流
- callback bridge

这个顺序的核心原则是：

- 先锁主数据流
- 再锁过滤语义
- 最后再锁高层对象形状
