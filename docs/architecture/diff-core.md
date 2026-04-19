# tqsdk-diff-core

## 职责
`tqsdk-diff-core` 只负责“如何理解和合并天勤 DIFF 协议”，包括：

- DIFF 包解析
- 嵌套对象递归合并
- 路径寻址与路径规范化
- 变更检测
- 原始 diff 到标准化 patch 的映射

## 不负责什么
这一层不应负责：

- WebSocket 生命周期
- 心跳和重连
- 用户订阅 API
- `wait_update()`
- callback、channel、stream
- 高层业务 view

## 建议核心抽象
```rust
pub struct DiffPacket;
pub struct DiffPath;
pub struct NormalizedPatch;
pub struct FieldChange;

pub trait DiffMerger {
    fn merge(&mut self, packet: DiffPacket) -> Vec<NormalizedPatch>;
}
```

## 为什么值得独立
- 便于对纯协议逻辑做单元测试和 benchmark
- replay、live、test feed 可共用同一 patch 归一化逻辑
- 避免 API 层需求反向污染协议层
