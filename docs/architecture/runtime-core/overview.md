# tqsdk-runtime-core 总览

## 角色
`tqsdk-runtime-core` 是整套系统最关键的一层。它负责：

- 接收并归一化外部输入
- 驱动 DIFF 合并结果进入中心状态仓
- 定义 commit 边界
- 推进 `Revision`
- 生成 `ChangeSet`
- 维护 `StateSnapshot`
- 在 commit 完成后通知上层消费者

## 核心原则
### 单一提交源
所有用户可见状态都必须通过 runtime core 完成提交。

### 单一 revision 语义
只有 runtime core 有资格推进 `Revision`。

### 单一通知语义
通知必须在 commit 完成之后触发，而不是在收到 diff 时触发。

## 核心抽象
```rust
pub struct RuntimeInput;
pub struct CommitResult {
    pub revision: Revision,
    pub changes: ChangeSet,
}

pub struct RuntimeCore;
pub struct StateStore;
pub struct StateSnapshot<'a>;
pub struct Revision(u64);
pub struct ChangeSet;
pub struct UpdateWaiter;
```

```rust
pub trait RuntimeCoreApi {
    fn current_revision(&self) -> Revision;
    fn current_snapshot(&self) -> StateSnapshot<'_>;
    fn last_change_set(&self) -> &ChangeSet;
    async fn wait_next_commit(&self, timeout: Option<Duration>) -> Option<CommitResult>;
}
```

## 进一步阅读
- [Session/Auth](runtime-core/session-auth.md)
- [协议交互](runtime-core/protocol-flow.md)
- [模块清单](runtime-core/modules.md)
