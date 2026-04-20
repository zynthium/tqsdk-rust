# 分层成长路线

## 先纠正优先级
- V1 不是 `wait_update` 路线
- V1 也不是 `stream/callback` 路线
- V1 不应该先做 facade，再倒逼内核
- V1 应先锁定 protocol-complete runtime contract

## 为什么要先做 runtime contract
- 所有远端交互都必须进入统一提交链路
- 未来 Python 风格和 Rust 风格 facade 都要复用同一个底座
- 如果先做某一种 facade，后面大概率会把该 facade 的语义硬编码进内核

## 演进路线
### Phase 0：协议与语义基线
包含：
- DIFF merge 语义
- transport / auth 测试桩
- schema / query / replay / trade 的最小协议编解码分析
- 状态树命名空间设计
- command causality 草图

目标：
- 锁定 V1 contract 的语义边界
- 不引入高层 facade

### Phase 1：Protocol-Complete Runtime Contract
包含：
- `RuntimeHandle`
- `RuntimeReader`
- `SnapshotReadGuard`
- `StateReadView`
- `RuntimeCommand`
- `RuntimeInput`
- `Revision`
- `CommitResult`
- `ChangeSet`
- `UpdateCursor`
- `StateSnapshot` / `CommitLog`（兼容与底层原语）
- `CommandLedger`
- `AdapterRegistry`
- `SystemAdapter`
- `MarketDiffAdapter`
- `TradeAdapter`
- `QueryAdapter`
- `ReplayAdapter`

目标：
- 所有远端交互都进入统一 `command -> mutation -> commit -> reader` 链路
- 不暴露任何用户态 facade

### Phase 2：Consumption Adapters
包含：
- `wait_update` adapter
- stream adapter
- callback adapter
- backpressure / cursor consumption policy

目标：
- 验证同一 reader / cursor 模型足以支撑多种消费风格

### Phase 3：Typed User Facades
包含：
- `TqApi`
- typed views / snapshots
- `is_changing()` 类查询接口
- facade 级错误语义

目标：
- 在不回改 runtime core 的前提下，构建面向策略作者的稳定 API

### Phase 4：Higher-Level Tasks And Tooling
包含：
- `TargetPosTask`
- 多账户编排
- 下载器 / dataframe / polars
- GUI / report / helper 工具层

目标：
- 只在 facade 稳定后扩展高层能力

## 实现建议
1. 先锁定 `RuntimeCommand` / `RuntimeInput` / `NormalizedMutation` / `CommitResult`
2. 再实现 session runtime、adapter registry、state store、commit assembler
3. 再接入 market / trade / query / schema / replay 各协议域
4. 完成 contract-level 测试后，再做 `wait_update` / stream / callback adapter
5. facade 和任务系统最后再做
