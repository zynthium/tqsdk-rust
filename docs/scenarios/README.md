# 场景契约与 API Gap

本目录保存尚不能作为正式 public API 契约的用户场景草案。

正式契约示例必须放在各 crate 的 `examples/api_contract_sXX_*.rs`，并由
`cargo check --workspace --examples` 和 CI 保持可编译。它们代表当前 public
API 已经支持或至少可以自然组合表达的终端用户代码。

当前 API 尚不支持、或只能用明显绕路方式表达的场景，必须保存在
`docs/scenarios/api_gaps/`。这些文件只记录理想用户代码草案和审查结论，不参与
Cargo example 自动发现，也不得伪装成已经支持。

当某个 gap 被修复时，应同时完成：

1. 将对应 sketch 提升为正式 `crates/<crate>/examples/api_contract_sXX_*.rs`。
2. 确保 example 顶部保留 Scenario / User goal / API contract / Forbidden /
   Regression signal / Review questions。
3. 更新 [`docs/public-api-scenario-review.md`](../public-api-scenario-review.md)
   的表达能力、风险、证据位置与建议处理方式。
4. 运行验证矩阵中要求的 examples / workspace / clippy 命令。
