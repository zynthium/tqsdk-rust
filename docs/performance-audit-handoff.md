# tqsdk-rust 性能优化交接文档

> 本文档记录 TradeScope 性能审计中发现的 tqsdk-rust 侧问题与优化建议。
> 应在 tqsdk-rust 仓库中独立处理。

---

## 审计背景

TradeScope（Tauri v2 量化交易终端）在同时订阅数百个合约/周期时出现性能瓶颈。
完整审计链路：前端 React → Axum WS → MarketActor → TqGateway → tqsdk-stream → tqsdk-core → 天勤服务器。

本文档仅覆盖 tqsdk-rust 仓库侧（`tqsdk-core`、`tqsdk-stream`、`tqsdk-session`）的发现。

---

## 当前迭代状态

- P1 `broadcast channel 默认容量不足`：已补 `expected_commit_consumers(...)`
  便利 API，按 `max(1024, expected_consumers * 8)` 从预计独立 consumer 数量估算
  root fan-out 容量；原 `commit_channel_capacity(...)` / `with_commit_channel_capacity(...)`
  仍保留用于精确配置。
- P1 `每个 kline/tick stream 独立消费 broadcast（复制风暴）`：已补内部
  `PathDispatcher`。`kline_stream`、`tick_stream`、typed path stream 和
  `market_events()` 共享一个 root receiver，并只向下游发送 path 命中的 commit；
  raw `commit_stream()` 仍保留每个调用者独立 receiver 的语义。
- P2 `CommitTouchSet 每次 commit 都创建新实例`：已将内部 `BTreeSet` /
  `BTreeMap` 替换为 Vec-backed ordered-unique 小集合。小 commit 惰性分配，
  hit 数较大时预留有界容量；tick/kline row id 保持有序去重，避免热路径
  per-commit tree allocation。
- P2 `RuntimeReader.next()` 慢消费者内存累积：已复核当前 core contract。
  `CommitLog` 已有有界 retention 和 contract tests；但当前架构明确保护活动 cursor
  需要的提交，不在本轮改成强制截断 active cursor。
- P3 `PathCommitStream` path 匹配效率：已引入构造期 `PathMatcher`，按 root
  segment 建索引，raw path stream 和内部 `PathDispatcher` subscriber 都复用预编译
  matcher，避免每个 commit 重新扫描原始 path filter 列表。

---

## 问题清单（按优先级排序）

### P1: broadcast channel 默认容量不足

**位置**: `crates/tqsdk-stream/src/api.rs:35`

```rust
pub(crate) const DEFAULT_COMMIT_CHANNEL_CAPACITY: usize = 1024;
```

**问题**: 数百合约同时活跃时，每个 runtime commit 会 fan-out 到所有消费者。
TradeScope 有 N 个 kline/tick stream（每个合约×周期一个）+ quote batch stream。
如果消费者处理速度跟不上（JSON 序列化慢），broadcast channel 会满，导致 `BroadcastStreamRecvError::Lagged`，消费者丢失 commit。

**建议方案**:
1. 根据订阅数量动态调整容量（例如 `max(1024, subscriber_count * 8)`）
2. 或在 `TqStreamBuilder` 中暴露更友好的 API，让调用方根据场景选择
3. 或在文档中明确标注高频场景需要手动调大容量

**影响范围**: `TqStreamBuilder`、`TqStream::with_commit_channel_capacity`

---

### P1: 每个 kline/tick stream 独立消费 broadcast（复制风暴）

**位置**: `crates/tqsdk-stream/src/api.rs:205-277`

```rust
pub async fn kline_stream(...) -> KlineRowStream {
    let commits = self.commit_stream()?.filter_paths([...]);
    // 每个 stream 创建一个 broadcast receiver
}
```

**问题**: 每个 kline/tick stream 都从同一个 broadcast channel 接收 **所有** commit，
然后在 `PathCommitStream` 中过滤。如果订阅了 100 个合约 × 5 个周期 = 500 个 stream，
每个 commit 会被 broadcast 复制 500 次，即使 99% 的 commit 与某个特定 stream 无关。

**内存影响**: 每次 commit 的 `SharedCommitResult`（Arc）被 clone 500 次。
虽然 Arc clone 只是引用计数 +1，但 broadcast channel 内部的 ring buffer 需要存储 500 个 receiver 的槽位。

**建议方案**:
引入 **共享 commit 多路复用器**（CommitDispatcher）：
- driver 只发一份 commit 到 dispatcher
- dispatcher 维护 path → channel 注册表
- 按 path 匹配分发给对应的 stream channel
- 无关 commit 不复制

```rust
// 概念设计
struct CommitDispatcher {
    subscribers: Vec<(StatePath, mpsc::Sender<SharedCommitResult>)>,
}

impl CommitDispatcher {
    fn dispatch(&self, commit: SharedCommitResult) {
        for (path, tx) in &self.subscribers {
            if commit.touches_path(path) {
                let _ = tx.try_send(commit.clone());
            }
        }
    }
}
```

**替代方案**: 保留 broadcast 但引入 `CommitStream::merge()` API，
让多个 path filter 共享同一个 receiver，在内部做 OR 匹配。

**影响范围**: `CommitStream`、`PathCommitStream`、`KlineRowStream`、`TickRowStream`

---

### P2: CommitTouchSet 每次 commit 都创建新实例

**位置**: `crates/tqsdk-stream/src/window.rs:231-251`

```rust
impl CommitTouchSet {
    pub(crate) fn from_commit(commit: &CommitResult) -> Self {
        let mut touches = Self { ..Self::default() };
        for object in &commit.changes.object_hits {
            touches.record_object(object);  // BTreeSet insert
        }
        for path in &commit.changes.path_hits {
            touches.record_path(path);  // BTreeSet insert + String parse
        }
    }
}
```

**问题**: 旧实现中 `CommitTouchSet` 内部使用 `BTreeSet` / `BTreeMap`，每次 commit 都要插入所有 object/path hits。
在高频 tick 场景下（每秒数百个合约更新），这个分配+排序开销会累积。

**当前处理**: 已改为 Vec-backed ordered-unique 小集合：
- `quote_symbols` / `chart_ids` 使用排序 Vec 去重，保持原先确定性迭代顺序
- tick/kline touch 按 symbol/series 排序分组，row id 使用排序 Vec 去重
- `from_commit()` 对小 commit 惰性分配，hit 数较大时按 hit 数预留有界容量
- `tests/performance_surface.rs` 增加源码契约，防止 `CommitTouchSet` 热路径重新引入
  `BTreeSet` / `BTreeMap`

**建议方案**:
1. 已采用：预分配容量，从 commit hit 数推断初始容量
2. 已采用：小集合用 std Vec-backed ordered-unique 结构替代 `BTreeSet` / `BTreeMap`
3. 后续可选：复用对象池，让 `CommitTouchSet` 可回收复用，进一步减少分配
4. 后续可选：`KlineSeriesTouch` 的 `symbol.to_string()` 可考虑使用 `&str` + arena 分配

**影响范围**: `CommitTouchSet`、`KlineSeriesTouch`、`ProjectedValueStream`

---

### P2: RuntimeReader.next() 在慢消费者场景下的内存累积

**位置**: `crates/tqsdk-core/src/runtime/`

**复核结论**: 当前 core 已经不是“保留所有历史 commit”的实现。
`CommitLog` 默认 `DEFAULT_MAX_ENTRIES = 8192`，并提供
`RuntimeHandle::with_adapters_and_commit_log_retention(...)` 配置入口。
`crates/tqsdk-core/tests/runtime_contract_runtime_core.rs` 已覆盖：
- 没有活动 cursor 需要时，超过 retention 的旧 commit 会被裁剪
- 活动 cursor 仍需要旧 commit 时，retention 不会截断它

后一条是当前架构约束：`docs/architecture/validation.md` 要求
`CommitLog` “不能截断仍被活动 cursor 需要的提交”。因此不能在本轮按原建议把
慢消费者直接强制丢弃，否则会改变 `RuntimeReader::next()` / `UpdateCursor`
的核心语义。

**仍存在的风险**: 如果某个活动 cursor 长时间不推进，core 会为了保持
exact sequential commit contract 而保留该 cursor 之后的 commit。这是有意的
正确性优先取舍，不是当前交接文档可直接覆盖的局部优化。

**建议方案**:
1. 已有：commit retention 上限和测试契约
2. 后续如要支持丢慢消费者，应作为显式架构变更设计 lossy cursor / checked next API，
   例如只让 stream driver 使用不保护 retention 的 cursor，并通过 `CursorLagged`
   向上游报告，而不是改变默认 `RuntimeReader::cursor()` 语义
3. 保持文档明确：默认 cursor 是 exact sequential contract，会保护仍需要的 commit

**影响范围**: `CommitLog`、`UpdateCursor`、`RuntimeReader`

---

### P3: PathCommitStream 的 path 匹配效率

**位置**: `crates/tqsdk-stream/src/filter.rs`

**问题**: 旧实现中 `PathCommitStream` 对每个 commit 遍历所有注册的 path，
用 `commit.changes.path_hits` 做字符串匹配。当注册了数百个 path 时，
每次 commit 都要做数百次字符串比较。

**当前处理**:
- `PathCommitStream` 构造时把 path filters 编译为 `PathMatcher`
- `PathMatcher` 按 root segment 建 `paths_by_root` 索引；commit 热路径先用
  changed path 的 root 缩小候选，再做原有 prefix 匹配
- 内部 `PathDispatcher` 的每个 subscriber 也保存 `PathMatcher`，不再保存原始
  `Vec<StatePath>` 并在 dispatch 时重新扫描
- `tests/performance_surface.rs` 增加源码契约，防止 raw path stream 或 dispatcher
  回退到 per-commit raw path filter scan

**建议方案**:
1. 已采用：构造期编译 path matcher，并按 root segment 缩小候选
2. 后续可选：如果单 root 下仍有数百 path，可再升级为 prefix trie 或更细粒度
   hash index

**影响范围**: `PathCommitStream`、`PathDispatcher`

---

## 验证建议

在 tqsdk-rust 仓库中实施改动后：

```bash
cargo fmt --all --check
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

对于 broadcast 容量和 commit 分发的改动，建议补充：
- benchmark 测试：模拟 100/500/1000 个并发 stream 的 commit fan-out 延迟
- lag 测试：模拟慢消费者场景，验证 `Lagged` 错误是否正确传播

---

## 参考

- TradeScope 性能审计报告（完整版本见 TradeScope 仓库 session 记录）
- tqsdk-rust 架构文档：`docs/architecture/`
- tqsdk-stream README：`crates/tqsdk-stream/README.md`
- tqsdk-core README：`crates/tqsdk-core/README.md`
