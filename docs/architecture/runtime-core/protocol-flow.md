# 协议交互与会话流程

## SessionBootstrap 的 5 个阶段
1. `Authenticate`
   - 调用 `AuthProvider`
   - 获取 `AuthContext`
2. `Connect`
   - 建立 transport
   - 启动读写循环，但还不 ready
3. `Bootstrap Requests`
   - 发送初始化请求
   - 发送首批订阅/查询
   - 将订阅登记到 `SubscriptionRegistry`
4. `Initial Sync`
   - 连续接收 frame
   - 归一化为 `NormalizedPatch`
   - 合并进 `StateStore`
   - 暂不向上层产出首个可见 commit
5. `Ready Commit`
   - 当满足初始截面完成判据时
   - 形成首个可见 revision
   - runtime 进入 `Running`

## 初始截面完成判据
`SessionReady` 至少满足：

- 认证完成
- transport 已连接
- bootstrap 请求已发送
- 已收到至少一轮有效 diff
- 与当前订阅意图相关的初始同步条件满足
- `StateStore` 能生成一份内部一致的 `StateSnapshot`

## commit 触发规则
1. 原始 `Frame` 不直接触发 commit
2. 只有 `NormalizedPatch` 被合并后，且形成用户可见变化时，才推进 revision
3. bootstrap 期间允许内部多次 merge，但直到满足初始截面完成判据后，才允许产出首个可见 `CommitResult`

## 重连恢复顺序
1. session 进入 `Reconnecting`
2. transport 重建
3. 重新认证或恢复凭证
4. 重放 `SubscriptionRegistry`
5. 进入 `Resyncing`
6. 接收并合并恢复期 diff
7. 满足重同步完成判据
8. 形成新的 `CommitResult`
9. 回到 `Running`

## 与上层 API 的关系
- `tqsdk-api-wait` 看到的是首个可见 commit 之后的状态世界
- `tqsdk-api-stream` 看到的是 commit 序列
- `tqsdk-api-callback` 看到的是 commit 之后派生的通知
