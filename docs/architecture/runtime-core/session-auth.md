# Session、Auth 与 Runtime Kernel

## 放在哪一层
以下能力都属于 `tqsdk-runtime-core`：

- WebSocket 生命周期
- 心跳与重连
- 账户登录

它们不属于 `tqsdk-diff-core`，也不应散落到 `tqsdk-api-*`。

## runtime core 内部 3 个逻辑子层
1. `runtime-foundation`
   - `Transport`
   - `AuthProvider`
   - `SessionBootstrap`
   - `SessionLifecycle`
   - `HeartbeatPolicy`
   - `ReconnectPolicy`
   - `SubscriptionRegistry`
2. `runtime-state`
   - `RuntimeInput`
   - `StateStore`
   - `Revision`
   - `ChangeSet`
   - `StateSnapshot`
   - `CommitResult`
3. `runtime-facade`
   - `RuntimeKernel`

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

pub struct AuthContext {
    pub access_token: String,
    pub auth_id: Option<String>,
    pub account_id: Option<String>,
    pub features: Vec<String>,
}
```

## SessionBootstrap
session 建立不是单个 connect，而是一段流程：

1. 认证
2. 建立 transport
3. 发送初始化请求
4. 接收并合并初始截面
5. 建立首个可见状态
6. 进入 steady state

```rust
pub struct SessionBootstrap;

impl SessionBootstrap {
    pub async fn establish(
        transport: &mut dyn Transport,
        auth: &dyn AuthProvider,
        config: &SessionConfig,
    ) -> Result<BootstrapResult>;
}
```

## SessionLifecycle
```text
Idle
-> Authenticating
-> Connecting
-> Bootstrapping
-> SyncingInitialState
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

## SubscriptionRegistry
`SubscriptionRegistry` 保存订阅意图，负责 bootstrap 和 reconnect/resync 后的重放。

```rust
pub struct SubscriptionRegistry;
```
