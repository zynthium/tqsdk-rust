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
- 2026-06 diff ingest 优化批次：已完成 core path allocation / applied-change metadata /
  quote fast path、wait no-scan changed quote API、stream path dispatcher root+quote-symbol
  索引。最终 core quote batch ingest 仍约为 JSON parse 的 5.0x，后续瓶颈主要在
  state apply / commit metadata / commit fan-out，而不是 typed reader。

---

## 2026-06 diff ingest 优化结果

基准命令：

```bash
cargo run -p tqsdk-core --example diff_ingest_microbench --release
```

计划基线：

| case | ns/iter |
| --- | ---: |
| `parse_json_single_quote` | 3955.9 |
| `ingest_single_quote` | 23796.7 |
| `ingest_noop_single_quote` | 12756.2 |
| `parse_json_quote_batch` | 275653.0 |
| `ingest_quote_batch` | 1809209.0 |
| `ingest_large_quote_batch` | 22446813.7 |
| `read_market_quote_typed` | 1141.0 |

各实现批次后本机测量值：

| case | Batch 1.1 path push/pop | Batch 1.2 applied metadata | Batch 2.1 quote fast path | final current branch |
| --- | ---: | ---: | ---: | ---: |
| `parse_json_single_quote` | 3964.6 | 4545.9 | 3511.4 | 4088.9 |
| `ingest_single_quote` | 19657.6 | 18502.5 | 16782.3 | 16909.8 |
| `ingest_noop_single_quote` | 7385.5 | 7237.1 | 5868.3 | 5609.8 |
| `parse_json_quote_batch` | 269272.8 | 253741.3 | 228272.0 | 219941.5 |
| `ingest_quote_batch` | 1330728.1 | 1344598.6 | 1220134.5 | 1110584.5 |
| `ingest_large_quote_batch` | 14863405.5 | 13527135.7 | 10921657.6 | 10223138.1 |
| `read_market_quote_typed` | 1110.3 | 979.2 | 822.6 | 1023.3 |

相对计划基线的最终变化：

| case | baseline ns/iter | final ns/iter | change |
| --- | ---: | ---: | ---: |
| `ingest_single_quote` | 23796.7 | 16909.8 | 28.9% faster |
| `ingest_noop_single_quote` | 12756.2 | 5609.8 | 56.0% faster |
| `ingest_quote_batch` | 1809209.0 | 1110584.5 | 38.6% faster |
| `ingest_large_quote_batch` | 22446813.7 | 10223138.1 | 54.5% faster |
| `read_market_quote_typed` | 1141.0 | 1023.3 | 10.3% faster |

stream fan-out benchmark command：

```bash
cargo test -p tqsdk-stream --test stream_fanout_microbench -- --ignored --nocapture
```

| case | before path index | after path index |
| --- | ---: | ---: |
| `quote_batches` 1 consumer / 4 commits | 808.068 us | 350.012 us |
| `quote_batches` 10 consumers / 4 commits | 1.466 ms | 0.826 ms |
| `quote_batches` 100 consumers / 4 commits | 6.720 ms | 5.167 ms |
| `quote_batches` 500 consumers / 4 commits | 31.957 ms | 30.803 ms |
| path quote streams 100 symbols | 25.679 ms | 24.936 ms |
| path quote streams 500 symbols | 544.432 ms | 424.506 ms |
| slow consumer lag, capacity 2 / 64 commits | skipped 62, 29.094 ms | skipped 62, 28.187 ms |

结论：

- core DIFF ingest 已明显改善，尤其是 noop quote 和 large quote batch；但最终
  `ingest_quote_batch / parse_json_quote_batch` 约为 `5.0x`，说明剩余成本仍主要在
  mutation apply、change metadata、commit publication，而不是 JSON parse 或 typed read。
- wait 单 owner 大批量 quote 消费现在有
  `WaitStep::changed_quote_symbols()` 和 `QuoteSet::changed*`，不需要每轮扫描全部订阅
  quote。
- stream raw `quote_batches` fan-out 不是主要瓶颈；path-specific stream 500 symbols
  仍较重，但 root+quote-symbol index 已把该 benchmark 降低约 22%。剩余成本包含
  500 个 typed path stream 的 broadcast wakeup、typed decode/read、测试调度开销。
- slow consumer 语义未改变：默认 stream 仍是 exact delivery，压力通过 `Lagged`
  显式暴露；没有引入 lossy/latest-only 模式。

后续可选工作：

- 如需继续压低 core ingest，可优先调查 `StateStore::apply_with` 的 map lookup /
  field application 成本，以及 `ChangeSet` field hit 构造成本。
- tick/kline fast path 仍未实现；只有在新增 row-heavy benchmark 证明 row decode 是主要
  热点时才值得增加该 specialized decoder surface。
- stream path dispatcher 如仍需扩展，可考虑 per-symbol typed batch API 或减少
  `PathValueStream` 逐 stream wakeup，而不是改变默认 lossy 语义。

### Grouped ChangeSet Decision

- Batch 1.1 result: 本次 clean benchmark 中，`ingest_prebuilt_large_quote_batch`
  从 `12,918,641.4 ns/iter` 降至 `7,435,178.7 ns/iter`，约 42.4% faster；
  `ingest_sparse_quote_batch_1000x10x3` 从 `85,292.0 ns/iter` 降至
  `48,309.8 ns/iter`，约 43.4% faster。
- Decision: 暂停 public grouped `ChangeSet` 形状变更，不在本轮推进架构更新。
- Reason: 内部 `AppliedChange` 字段索引化已经超过计划阈值（large batch >= 15%、
  sparse batch >= 10%），剩余收益不足以抵消 public contract 变更成本。
- Public API impact: 无。`ChangeSet.path_hits`、`object_hits`、`field_hits` 的 public
  字段和顺序保持不变。

### Runtime Lock Tail-Latency Decision

- No-load command submit p95: `81.085us`
- Under large market ingest p95: `50.267216ms`
- Decision: 不在当前 diff ingest 性能批次中实现 runtime lock split；新建独立
  architecture-gated sequencer 计划
  `docs/superpowers/plans/2026-06-10-runtime-ingest-command-sequencer.md`。
- Reason: Batches 1-3 后 command submit p95 仍远高于 2x 门槛，说明
  `RuntimeHandle::ingest()` 持有 runtime mutex 覆盖大行情 decode/apply/publish 时，
  command submission 仍会排队等待。这个风险需要重排写侧 sequencing，但必须保持单一
  revision 序列、单一 `CommitLog`、命令账本状态机校验和现有 reader/cursor 语义，因此
  应作为单独架构变更处理。

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
