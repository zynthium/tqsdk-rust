# tqsdk-runtime-core 总览

## 角色
`tqsdk-runtime-core` 是整个系统的基础契约层。

它的职责不是提供某种用户 API，而是保证所有协议域都通过同一个提交模型对外可见。

它至少负责：

- 接收 `RuntimeCommand`
- 协调 session 生命周期
- 路由命令到各个 `ProtocolAdapter`
- 收集 `RuntimeInput`
- 将输入解码为 `NormalizedMutation`
- 将 mutation 写入统一状态树
- 运行投影与变更归并
- 组装 `CommitResult`
- 推进 `Revision`
- 发布 `CommitLog`
- 为消费者创建 `UpdateCursor`

它必须覆盖的协议域：

- market diff
- trade
- replay
- query / schema
- system / auth / session

## 核心原则
### 单 actor 所有权
session 推进必须由一个 runtime actor 串行拥有。

### 单一提交源
所有对外可见状态都必须通过 runtime core 提交。

### 单一 revision 语义
只有 runtime core 有资格推进 `Revision`。

### 单一状态树
所有可见状态都必须进入同一个 `StateSnapshot`，包括 query、schema、trade、replay 和 system 状态。

### 单一因果语义
命令的后续结果、错误和状态变化必须通过统一的 command causality 进入 commit。

### 单一 cursor/log 语义
后续任何消费风格都只能通过 `CommitLog` / `UpdateCursor` 读取提交结果。

### adapter 无提交权
adapter 只能编解码与生成 mutation，不能自行推进 revision、发通知或移动 cursor。

## 核心抽象
```rust
pub struct RuntimeHandle;

pub struct Revision(u64);
pub struct CommandId(u64);
pub struct CursorId(u64);

pub struct StateSnapshot;
pub struct ChangeSet;
pub struct CommitResult;
pub struct CommitLog;
pub struct UpdateCursor;

pub enum RuntimeCommand;
pub enum RuntimeInput;
pub enum NormalizedMutation;

pub trait ProtocolAdapter;
```

```rust
pub trait Runtime {
    async fn submit(&self, cmd: RuntimeCommand) -> Result<CommandId>;
    fn latest_snapshot(&self) -> StateSnapshot;
    fn cursor(&self) -> UpdateCursor;
}
```

## 关键判断
- `RuntimeHandle` 是 V1 唯一 canonical public entry point
- V1 不直接公开 `wait_update()`、stream、callback facade
- 未来 `wait_update` 和 `stream/callback` 都只能建立在 `StateSnapshot + CommitLog + UpdateCursor` 之上

## 进一步阅读
- [Session/Auth](session-auth.md)
- [协议交互](protocol-flow.md)
- [模块清单](modules.md)
