# Runtime Contract 最小模块清单

## 推荐模块
1. `transport`
2. `auth`
3. `session_runtime`
4. `adapter_registry`
5. `command_ledger`
6. `diff_core`
7. `state_store`
8. `projection_engine`
9. `commit_assembler`
10. `commit_log`
11. `runtime_reader`
12. `runtime_contract`

## 依赖方向
```text
transport   auth
    \       /
   session_runtime
         |
   adapter_registry
         |
   command_ledger
         |
      diff_core
         |
     state_store
         |
  projection_engine
         |
  commit_assembler
         |
      commit_log
         |
   runtime_reader
         |
   runtime_contract
```

## 每个模块的最小公开接口
### transport
```rust
pub trait Transport {
    async fn connect(&mut self) -> Result<()>;
    async fn recv(&mut self) -> Result<RawFrame>;
    async fn send(&mut self, frame: OutboundFrame) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}
```

### auth
```rust
pub trait AuthProvider {
    async fn authenticate(&self) -> Result<AuthContext>;
}
```

### adapter_registry
```rust
pub trait ProtocolAdapter {
    fn domain(&self) -> ProtocolDomain;
    fn accepts_command(&self, cmd: &RuntimeCommand) -> bool;
    fn encode(&mut self, cmd: &RuntimeCommand) -> Result<Vec<OutboundRequest>>;
    fn accepts_input(&self, input: &RuntimeInput) -> bool;
    fn decode(&mut self, input: &RuntimeInput) -> Result<Vec<NormalizedMutation>>;
}

pub struct AdapterRegistry;
```

职责：
- 维护所有协议域 adapter
- 为每个命令选择 owning adapter
- 将输入广播给 interested adapters

### command_ledger
```rust
pub struct CommandEnvelope {
    pub id: CommandId,
    pub command: RuntimeCommand,
}

pub enum CommandStatus {
    Queued,
    Sent,
    Acked,
    PartiallyApplied,
    Completed,
    Rejected,
    Failed,
    Cancelled,
}
```

职责：
- 跟踪命令因果链
- 将命令状态写入统一状态树
- 只保留活跃命令的最小元数据；terminal 后由状态树承载读侧语义

### state_store
```rust
pub trait StateStoreApi {
    fn apply(&mut self, mutations: &[NormalizedMutation]);
    fn read(&self) -> StateReadView<'_>;
    fn snapshot(&self) -> StateSnapshot; // compatibility
}
```

### projection_engine
```rust
pub trait ProjectionEngine {
    fn project(&mut self, mutations: &[NormalizedMutation]) -> ProjectionDelta;
}
```

职责：
- 将底层 mutation 归并为 path/object/field 级可见变化

### commit_assembler
```rust
pub trait CommitAssembler {
    fn assemble(
        &mut self,
        caused_by: &[CommandId],
        projection: ProjectionDelta,
        state: StateReadView<'_>,
    ) -> Option<CommitResult>;
}
```

### runtime_reader
```rust
pub struct RuntimeReader;
pub struct SnapshotReadGuard<'a>;

impl RuntimeReader {
    pub fn cursor(&self) -> UpdateCursor;
    pub fn read(&self) -> SnapshotReadGuard<'_>;
    pub fn next(&self, cursor: &mut UpdateCursor) -> Option<CommitResult>;
}
```

职责：
- 暴露 canonical read-side API
- 将 commit log 与状态树读锁收敛到同一组低层原语
- 为后续 wait/stream/callback facade 提供统一底座

### runtime_contract
```rust
pub trait Runtime {
    async fn submit(&self, cmd: RuntimeCommand) -> Result<CommandId>;
    fn reader(&self) -> RuntimeReader;
    fn latest_snapshot(&self) -> StateSnapshot; // compatibility
    fn cursor(&self) -> UpdateCursor; // compatibility
}
```

## V1 不应提前出现的模块
- `TqApi`
- `wait_adapter`
- `stream_adapter`
- `callback_adapter`
- typed quote / kline / tick views
- `target_pos_task`
- 多账户 orchestration facade

## 从 V1 到后续阶段
- V1：runtime contract + protocol adapters
- V2：wait / stream / callback adapters
- V3：typed facades
- V4：task/tooling layer
