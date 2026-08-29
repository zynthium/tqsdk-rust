# Claude Code 补充说明

@AGENTS.md

## Claude Code

- 本文件只记录 Claude Code 专属补充；共享项目规则维护在 `AGENTS.md`。
- 个人偏好放入 gitignored 的 `CLAUDE.local.md` 或用户级记忆，不要提交到本文件。
- 路径专属规则放入 `.claude/rules/`；多步骤流程或可复用工作流做成 skill。
- 如果 Claude 没遵循记忆，先用 `/memory` 确认实际加载的文件和规则，再检查冲突。
- MCP 工具名在 Claude Code 中可能带 `mcp__code-review-graph__...` 前缀；语义以
  `AGENTS.md` 的 `code-review-graph` 工具表为准。
