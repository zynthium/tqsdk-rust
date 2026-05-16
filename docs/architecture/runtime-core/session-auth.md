# Session、Auth 与 Runtime Contract

## 放在哪一层
以下能力都属于 `tqsdk-runtime-core`：

- transport 生命周期
- auth / token / capability
- heartbeat / reconnect
- session bootstrap
- session error 归一化

它们不属于 `diff-core`，也不应散落到未来 facade 层。

## runtime core 内部 4 个逻辑子层
1. `runtime-foundation`
   - `Transport`
   - `AuthProvider`
   - `HeartbeatPolicy`
   - `ReconnectPolicy`
2. `runtime-orchestration`
   - `SessionRuntime`
   - `SessionLifecycle`
   - `AdapterRegistry`
   - `CommandLedger`
3. `runtime-state`
   - `RuntimeInput`
   - `StateStore`
   - `ProjectionEngine`
   - `CommitAssembler`
   - `CommitLog`
4. `runtime-contract`
   - `RuntimeHandle`
   - `RuntimeReader`
   - `SnapshotReadGuard`
   - `UpdateCursor`
   - `StateSnapshot`（兼容）

## Transport
```rust
pub trait Transport {
    async fn connect(&mut self) -> Result<()>;
    async fn recv(&mut self) -> Result<RawFrame>;
    async fn send(&mut self, frame: OutboundFrame) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}
```

## AuthProvider
```rust
pub trait AuthProvider {
    async fn authenticate(&self) -> Result<AuthContext>;
}

pub struct AuthContext { /* fields private */ }

impl AuthContext {
    pub fn new(access_token: impl Into<String>) -> Self;
    pub fn access_token(&self) -> &str;
    pub fn auth_id(&self) -> Option<&AuthId>;
    pub fn features(&self) -> &[String];
    pub fn with_auth_id(self, auth_id: AuthId) -> Self;
    pub fn with_feature(self, feature: impl Into<String>) -> Self;
}
```

约束：
- auth 结果必须进入 runtime state
- auth 失败和 auth 失效也必须进入统一 commit 语义
- auth/session 结果必须能通过同一个 `RuntimeReader` 读面被观察到

## SessionBootstrap
session 建立不是单个 connect，而是一段流程：

1. 认证
2. 建立 transport / client
3. 注册 adapter
4. 拉取 schema / metadata / bootstrap 状态
5. 建立首个可见提交
6. 进入 steady state

```rust
pub struct SessionBootstrap;

impl SessionBootstrap {
    pub async fn establish(
        auth: &dyn AuthProvider,
        config: &SessionConfig,
        adapters: &mut AdapterRegistry,
    ) -> Result<BootstrapResult>;
}
```

## SessionLifecycle
```text
Idle
-> Authenticating
-> Connecting
-> Bootstrapping
-> Running
-> Reconnecting
-> Resyncing
-> Running
-> Closed
```

## Heartbeat / Reconnect
```rust
pub struct HeartbeatPolicy {
    pub interval: Duration,
    pub timeout: Duration,
}

pub struct ReconnectPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_attempts: Option<u32>,
}
```

`max_attempts = Some(n)` 表示连续重连失败达到 `n` 次后进入 `Closed`；
`max_attempts = None` 是默认策略，表示持续按 backoff 重试直到重连成功。
该值会进入统一状态树的 `system.session.reconnect.max_attempts`：有限次数写入数字，
无限重试写入 JSON `null`，由上层 facade 直接按 `Option<u32>` 解读。

## 关键判断
- auth、session、reconnect 不只是基础设施问题，它们本身也是状态与提交语义的一部分
- future facade 不应该自己维护另一套连接状态模型
- future facade 也不应该自己维护另一套 reader model；它们只能包装 `RuntimeReader`
