# tqsdk-diff-core

## 职责
`tqsdk-diff-core` 只负责“如何理解和合并天勤 DIFF 协议”，包括：

- DIFF 包解析
- 嵌套对象递归合并
- 路径寻址与路径规范化
- 变更检测
- 原始 diff 到标准化 mutation 的映射

## 不负责什么
这一层不应负责：

- WebSocket 生命周期
- 心跳和重连
- auth / session
- 用户订阅 API
- `wait_update()`
- callback / fan-out
- 高层 view 或 facade

## 建议核心抽象
```rust
pub struct DiffPacket;
pub struct DiffPath;
pub struct NormalizedMutation;
pub struct FieldMutation;

pub trait DiffMerger {
    fn merge(&mut self, packet: DiffPacket) -> Vec<NormalizedMutation>;
}
```

## 为什么值得独立
- 便于对纯协议逻辑做单元测试和 benchmark
- live、replay、test feed 可共用同一 DIFF 归一化逻辑
- 避免任何 facade 层需求反向污染协议层
