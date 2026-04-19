# 核心数据契约

## 目标
固定模块之间传递的最小数据契约，避免 transport、input pipeline、state store、commit bus 互相污染。

## `RawFrame`
```rust
pub enum RawFrame {
    Text(String),
    Binary(bytes::Bytes),
    Ping,
    Pong,
    Close,
}
```

- 只表达传输层收到的内容
- 不直接推进 revision
- 不携带业务层“已经可见”的含义

## `OutboundFrame`
```rust
pub enum OutboundFrame {
    Text(String),
    Binary(bytes::Bytes),
    Ping,
    Close,
}
```

- runtime 决定“发什么”
- transport 只负责“怎么发”

## `NormalizedPatch`
```rust
pub struct NormalizedPatch {
    pub scope: PatchScope,
    pub target: PatchTarget,
    pub fields: Vec<FieldPatch>,
    pub source: PatchSource,
}
```

```rust
pub enum PatchScope {
    Bootstrap,
    Realtime,
    Resync,
}

pub struct FieldPatch {
    pub field: &'static str,
    pub value: serde_json::Value,
}

pub enum PatchSource {
    LiveDiff,
    BootstrapSync,
    ReconnectResync,
    Replay,
    TestFeed,
}
```

- 是可提交变化，不是原始 diff
- 已知道要写向哪个逻辑对象
- 保留 scope/source，方便区分 bootstrap / realtime / resync / replay

## `CommitBatch` 与 `CommitOutcome`
```rust
pub struct CommitBatch {
    pub patches: Vec<NormalizedPatch>,
}

pub enum CommitOutcome {
    NoVisibleChange,
    VisibleCommit(CommitResult),
}
```

## `CommitResult`
```rust
pub struct CommitResult {
    pub revision: Revision,
    pub changes: ChangeSet,
    pub scope: CommitScope,
}

pub enum CommitScope {
    InitialReady,
    RealtimeUpdate,
    ResyncRecovery,
    ReplayStep,
}
```

## 一条完整数据链
```text
Transport.recv()
  -> RawFrame
  -> input_pipeline.parse()
  -> NormalizedPatch
  -> CommitBatch
  -> StateStore.apply()
  -> CommitOutcome::VisibleCommit(CommitResult)
  -> CommitBus.publish()
  -> RuntimeKernel / api-wait / api-stream / api-callback
```
