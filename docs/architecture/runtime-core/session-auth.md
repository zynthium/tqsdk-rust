# Session、Auth 与 Runtime Contract

初始 socket 尝试次数可通过 `WebSocketTransport::with_connect_attempts(NonZeroUsize)` 显式选择；
session builder 提供 `websocket_connect_attempts(...)`。默认仍为 3 次，独立于 ReconnectPolicy。
HTTP 握手失败立即返回 `ContractError::HttpStatus { status, retry_after_secs: None }`，不在 transport
内重复握手。HTTP 认证响应保留 Retry-After；401/403 kind 为 Auth，429/5xx 为 Http。
新增公开错误 variant 需要外部穷举匹配适配；通用 retry hint 仅允许 5xx backoff。


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

默认 `WebSocketTransport` 对一次 route 建立使用最多 3 次 socket/TLS 尝试，单次
最长 15 秒。该有界保护用于吸收初始建连的瞬时黑洞；网络重试预算耗尽后才向
`SessionRuntime` 返回 transport error。错误诊断只包含 endpoint `host:port`，不包含
URL path、query 或 userinfo。它不改变下面的 session-level `ReconnectPolicy`：后者仍
负责已建立 session 断线后的重建、退避、状态树投影和 attempt 计数。

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

`max_attempts = Some(n)`（`n > 0`）表示连续重连失败达到 `n` 次后进入 `Closed`；
`Some(0)` 禁用自动恢复：重连触发时记录 attempt=0、exhausted=true 和 `Closed`，
不再次调用 auth/resolver/connector，并返回可重试的 Transport 错误。Session facade 的
flush/peek 发送失败也遵循此开关。显式 runtime recovery 与初始 socket 建连预算不受此开关影响。
`max_attempts = None` 是默认策略，表示持续按 backoff 重试直到重连成功。
该值会进入统一状态树的 `system.session.reconnect.max_attempts`：有限次数写入数字，
无限重试写入 JSON `null`，由上层 facade 直接按 `Option<u32>` 解读。

## 关键判断
- auth、session、reconnect 不只是基础设施问题，它们本身也是状态与提交语义的一部分
- future facade 不应该自己维护另一套连接状态模型
- future facade 也不应该自己维护另一套 reader model；它们只能包装 `RuntimeReader`
