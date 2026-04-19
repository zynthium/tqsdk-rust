# 核心数据契约

## 目标
固定 runtime contract 内部传递的最小稳定数据契约，避免 transport、adapter、state、commit 各层相互污染。

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

- 只表达 transport 层收到的内容
- 不直接推进 revision

## `OutboundFrame`
```rust
pub enum OutboundFrame {
    Text(String),
    Binary(bytes::Bytes),
    Ping,
    Close,
}
```

## `OutboundRequest`
```rust
pub enum OutboundRequest {
    Transport(OutboundFrame),
    Http(HttpRequest),
    Replay(ReplayRequest),
    Internal(InternalRequest),
}
```

- runtime 统一决定“发什么”
- 具体 transport 或 client 决定“怎么发”

`HttpRequest` 的最小稳定形状同时覆盖两类需求：

```rust
pub enum HttpMethod {
    Get,
    Post,
}

pub struct HttpRequest {
    pub method: HttpMethod,
    pub path: Option<String>,
    pub body: Option<serde_json::Value>,
}
```

- schema / metadata / bootstrap 可用 `GET + path`
- GraphQL query 可用 `POST + JSON body`
- 仍然保持在 runtime contract 的最小传输抽象层，不引入高层 facade 语义

## `RuntimeInput`
```rust
pub enum RuntimeInput {
    Io(IoEvent),
    Timer(TimerEvent),
    Auth(AuthEvent),
    Replay(ReplayEvent),
    Internal(InternalEvent),
}
```

- 是 runtime 能理解的输入外壳
- 允许多个 adapter 观察同一个输入

## `NormalizedMutation`
```rust
pub struct NormalizedMutation {
    pub path: StatePath,
    pub object: Option<ObjectKey>,
    pub fields: Vec<FieldMutation>,
    pub source: MutationSource,
}

pub struct FieldMutation {
    pub field: String,
    pub value: serde_json::Value,
}

pub enum MutationSource {
    MarketDiff,
    TradeReply,
    QueryResult,
    SchemaBootstrap,
    ReplayStep,
    SessionControl,
}
```

- 是可提交变化，不是原始 wire payload
- 所有协议域最终都要落成 `NormalizedMutation`

## `ProjectionDelta`
```rust
pub struct ProjectionDelta {
    pub path_hits: Vec<StatePath>,
    pub object_hits: Vec<ObjectKey>,
    pub field_hits: Vec<ChangeHit>,
}
```

## `CommandEnvelope`
```rust
pub struct CommandEnvelope {
    pub id: CommandId,
    pub command: RuntimeCommand,
    pub causation: CausationMeta,
}
```

## `CommitResult`
```rust
pub struct CommitResult {
    pub revision: Revision,
    pub changes: ChangeSet,
    pub caused_by: Vec<CommandId>,
    pub scope: CommitScope,
}

pub enum CommitScope {
    InitialReady,
    RealtimeUpdate,
    ResyncRecovery,
    ReplayStep,
    QueryRefresh,
    SessionTransition,
}
```

## 一条完整数据链
```text
RuntimeCommand
  -> ProtocolAdapter.encode()
  -> OutboundRequest
  -> RuntimeInput
  -> ProtocolAdapter.decode()
  -> NormalizedMutation
  -> StateStore.apply()
  -> ProjectionEngine.project()
  -> CommitAssembler.assemble()
  -> CommitResult
  -> CommitLog.publish()
  -> UpdateCursor
```
