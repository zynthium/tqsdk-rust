# Claude Code 补充说明

本仓库的跨工具 AI 工作流入口是 [`AGENTS.md`](AGENTS.md)。Claude Code 在开始代码
改动前必须先遵循 `AGENTS.md` 的必读顺序和架构边界；本文件只保留 Claude Code
环境下的补充约束。

## 文档权威

- 当前架构权威仍是 [`docs/architecture/*`](docs/architecture/)。
- `docs/reviews/`、`docs/archive/`、`docs/superpowers/` 只能作为上下文和计划证据。
- 如果 `CLAUDE.md`、`AGENTS.md` 与架构文档冲突，以 `docs/architecture/*` 为准；
  如果只是 Claude Code 工具用法差异，以本文件的工具说明补充 `AGENTS.md`。

## code-review-graph MCP

Claude Code 环境可能提供 `code-review-graph` MCP 知识图谱。工具可用时，先调用
`get_minimal_context_tool` 获取最小上下文，再按任务选择图谱工具；最后才读取具体文件：

| 工具 | 使用场景 |
| --- | --- |
| `get_minimal_context_tool` | 首次进入任务时获取约 100 tokens 的最小上下文 |
| `build_or_update_graph_tool` | 构建或增量更新图谱 |
| `detect_changes_tool` | 审查当前改动，获取风险评分和变化摘要 |
| `get_review_context_tool` | 获取审查所需源码片段，减少整文件读取 |
| `get_impact_radius_tool` | 理解改动影响面 |
| `get_affected_flows_tool` | 查找受影响执行路径 |
| `query_graph_tool` | 追踪 callers、callees、imports、tests、dependencies |
| `traverse_graph_tool` | 从任意节点做 BFS/DFS 遍历 |
| `semantic_search_nodes_tool` | 按名称或关键词查找函数、类型、模块 |
| `list_graph_stats_tool` | 查看图谱规模和健康状态 |
| `get_architecture_overview_tool` | 获取高层代码结构 |
| `list_communities_tool` | 获取图谱社区和架构分组 |
| `get_hub_nodes_tool` / `get_bridge_nodes_tool` | 识别架构热点和瓶颈 |
| `get_knowledge_gaps_tool` / `get_surprising_connections_tool` | 识别结构弱点和意外耦合 |
| `refactor_tool` | 规划重命名、删除死代码或局部重构 |

如果当前 Claude Code session 没有这些 MCP 工具，或图谱不覆盖目标区域，说明原因后
回退到 `rg`、`rg --files` 和文件读取。不要因为图谱不可用而跳过
`AGENTS.md` 要求的架构阅读和验证。

## Claude Code 工作习惯

- 先检查 `git status --short`，不要覆盖用户已有改动。
- 优先使用非交互命令；需要搜索时优先 `rg`。
- 手工编辑文件使用补丁方式，避免顺手改动无关格式。
- 完成文档-only 改动至少运行 `git diff --check`。
- 完成 Rust 代码改动时按 `AGENTS.md` 和 `docs/architecture/validation.md` 选择验证命令。
