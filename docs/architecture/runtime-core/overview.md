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
- 发布底层 `CommitLog`
- 提供官方对象的纯 typed schema contract
- 为消费者暴露 `RuntimeReader` / `UpdateCursor`

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
所有可见状态都必须进入同一棵 runtime state tree，包括 query、schema、trade、replay 和 system 状态。
`RuntimeReader` 读取的是这棵树的 revision-bound 借用视图；`StateSnapshot` 只是兼容性的 owned clone。

### 单一因果语义
命令的后续结果、错误和状态变化必须通过统一的 command causality 进入 commit。

### 单一 cursor/log 语义
后续任何消费风格都只能通过 `RuntimeReader` / `UpdateCursor` 读取提交结果。
`CommitLog` 是底层共享原语，不应成为未来 facade 的首选入口。

### adapter 无提交权
adapter 只能编解码与生成 mutation，不能自行推进 revision、发通知或移动 cursor。

## 核心抽象
```rust
pub struct RuntimeHandle;
pub struct RuntimeReader;
pub struct SnapshotReadGuard<'a>;
pub struct CommitReadGuard<'a>;
pub struct StateReadView<'a>;
pub struct CursorLagged;

pub struct Revision(u64);
pub struct CommandId(u64);
pub struct CursorId(u64);

pub struct CommitResult;
pub struct ChangeSet;
pub struct UpdateCursor;
pub struct StateSnapshot; // compatibility
pub struct CommitLog; // underlying primitive

pub enum RuntimeCommand;
pub enum RuntimeInput;
pub enum NormalizedMutation;

pub trait ProtocolAdapter;
```

```rust
pub trait Runtime {
    async fn submit(&self, cmd: RuntimeCommand) -> Result<CommandId>;
    fn reader(&self) -> RuntimeReader;
    fn latest_snapshot(&self) -> StateSnapshot; // compatibility
    fn cursor(&self) -> UpdateCursor; // compatibility
}
```

## 关键判断
- `RuntimeHandle` 是 V1 的写入与控制入口
- `RuntimeReader` 是 V1 的 canonical read-side entry point
- `RuntimeReader::read()` 提供当前 head 的 zero-copy 读视图
- `RuntimeReader::next_view()` 提供“exact revision 或明确 lagged”的底层一致性原语
- `SnapshotReadGuard` / `CommitReadGuard` / `StateReadView` 可以按路径 `decode<T>()` 成官方 schema，但这仍然只是底层 schema decode，不是 typed state facade
- `types::*` 只提供纯 schema/type，不提供 facade/view 或用户便利行为
- schema 刷新结果必须按 `schema_id` 进入状态树，不能把 transport route label 当成逻辑对象键
- V1 不直接公开 `wait_update()`、stream、callback facade
- 未来 `wait_update` 和 `stream/callback` 都只能建立在 `RuntimeReader + SnapshotReadGuard + UpdateCursor` 之上
- `StateSnapshot` 与 `CommitLog` 保留给 detached ownership、兼容层和测试，不应反向定义核心读模型
- `CommitLog` 必须是 indexable 且受 retention 约束，不能在长会话里线性退化或无界增长
- `runtime.commands/*` 属于本地控制面状态，可做 retention-bounded 保留；terminal 命令一旦超出保留上限必须被裁剪且保持幂等写回

## 进一步阅读
- [Session/Auth](session-auth.md)
- [协议交互](protocol-flow.md)
- [模块清单](modules.md)
