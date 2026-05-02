# 场景契约与 API Gap

本目录保存尚不能作为正式 public API 契约的用户场景草案。

正式契约示例必须放在各 crate 的 `examples/api_contract_sXX_*.rs`，并由
`cargo check --workspace --examples` 和 CI 保持可编译。它们代表当前 public
API 已经支持或至少可以自然组合表达的终端用户代码。

当前 API 尚不支持、或只能用明显绕路方式表达，并且仍需要作为当前 desired sketch
跟踪的场景，必须保存在 `docs/scenarios/api_gaps/`。这些文件只记录理想用户代码草案
和审查结论，不参与 Cargo example 自动发现，也不得伪装成已经支持。

当某个场景已经有正式 example，且当前 review 已把它判定为“自然表达”或把剩余需求
明确降级为非核心用户层能力时，应将原 gap sketch 归档到 `docs/archive/scenarios/`，
避免 `api_gaps/` 继续混入已闭环历史输入。active `api_gaps/` 只保留仍未支持、仍需
作为当前边界样本跟踪的草案。

当前 active `api_gaps/` 只剩：

- `api_contract_s14_multi_provider_market_aggregation.rs`：非近期核心 SDK 目标，
  作为多 provider 基础设施边界样本保留。
- `api_contract_s18_cross_process_cache_service.rs`：本地 cache foundation 已有正式
  S18 examples；完整跨进程 service / daemon orchestration 仍只作为用户层工具边界样本保留。
- `api_contract_s31_low_latency_trading_desk.rs`：低延迟柜台 profile 仍缺少正式
  contract example，把 core/session/task/stream primitives 串成同一 hot-path 链路。

已闭环的 gap sketch 应归档到 `docs/archive/scenarios/<date>/`。例如 S30
历史序列 mmap cache 已提升为
`crates/tqsdk-data/examples/api_contract_s30_history_series_cache.rs`，原 sketch 已归档。

场景评审时应先判断示例主要服务哪类 Rust 使用者，而不是按官方 Python SDK 的
方法名逐项对齐。使用者分层与迭代顺序见
[`user-layer-iteration-plan.md`](user-layer-iteration-plan.md)。

当某个 gap 被修复时，应同时完成：

1. 将对应 sketch 提升为正式 `crates/<crate>/examples/api_contract_sXX_*.rs`。
2. 确保 example 顶部保留 Scenario / User goal / API contract / Forbidden /
   Regression signal / Review questions。
3. 更新 [`docs/reviews/public-api-scenario-review.md`](../reviews/public-api-scenario-review.md)
   的表达能力、风险、证据位置与建议处理方式。
4. 运行验证矩阵中要求的 examples / workspace / clippy 命令。
