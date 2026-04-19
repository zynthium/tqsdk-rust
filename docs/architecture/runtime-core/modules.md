# V1 Runtime Kernel 最小模块清单

## 推荐模块
1. `transport`
2. `auth`
3. `session`
4. `subscription_registry`
5. `input_pipeline`
6. `state_store`
7. `commit_bus`
8. `runtime_kernel`

## 依赖方向
```text
transport   auth
    \       /
      session
         |
subscription_registry
         |
   input_pipeline
         |
    state_store
         |
     commit_bus
         |
   runtime_kernel
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

### session
```rust
pub struct SessionConfig;
pub enum SessionLifecycle {
    Idle,
    Authenticating,
    Connecting,
    Bootstrapping,
    SyncingInitialState,
    Running,
    Reconnecting,
    Resyncing,
    Closed,
}
```

### input_pipeline
```rust
pub enum RuntimeInput {
    TransportConnected,
    Authenticated(AuthContext),
    Frame(RawFrame),
    NormalizedPatch(NormalizedPatch),
    HeartbeatTimeout,
    ReconnectTriggered,
    ResubscribeRequired,
    Shutdown,
}
```

### runtime_kernel
```rust
pub trait RuntimeKernel {
    fn current_revision(&self) -> Revision;
    fn current_snapshot(&self) -> StateSnapshot<'_>;
    fn last_change_set(&self) -> &ChangeSet;
    async fn wait_next_commit(&self, timeout: Option<Duration>) -> Option<CommitResult>;
}
```

## V1 不应提前出现的模块
- `quote_view`
- `series_api`
- `trade_session`
- `callback_registry`
- `stream_adapter`
- `target_pos_task`

## 从 V1 到 V2/V3/V4
- `V2 tqsdk-api-wait`：增加 `TqApi`、`View + Snapshot`、`is_changing()`
- `V3 tqsdk-api-stream`：增加 `Stream<Item = CommitResult>` 和对象级投影 stream
- `V4 tqsdk-api-callback`：增加 callback 注册与分发
