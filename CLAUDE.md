## Project Architecture Guardrails

The canonical AI workflow and architecture guardrails for this repository live in
[`docs/architecture/ai-workflow.md`](docs/architecture/ai-workflow.md). The
repository documentation map lives in [`docs/README.md`](docs/README.md). Read
both before code changes in a new Claude Code session, especially before
touching crate boundaries, public APIs, runtime state/commit semantics,
session/query ownership, or wait/stream/task/data facade ownership.

`docs/reviews/` contains current review and public API decision records,
`docs/archive/` contains historical review input, and `docs/superpowers/`
contains execution specs/plans. Treat them as context and planning evidence, not
as authority over `docs/architecture/*` or current code.

Hard constraints:

- `tqsdk-core` remains the protocol-complete runtime substrate. Do not move
  high-level facades, direct-query convenience APIs, task/data/downloader logic,
  or Tianqin-specific public auth/http helpers back into core.
- `tqsdk-session` owns shared session and one-shot request/response/direct-query
  helpers. `tqsdk-wait` and `tqsdk-stream` own only diff-backed continuous
  consumption shapes.
- All visible state changes must flow through the runtime commit model:
  `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor`.
  Do not add side caches, private facade revisions, or bypass notifications.
- Domain writes must keep the `MutationSource` root guard. Hot reads should use
  partition read surfaces such as `read_market_state()` and `read_trade_state()`
  when available.
- Command/order status updates must go through the runtime state machine; do not
  reintroduce string-based terminal checks or adapter-local rollback behavior.
- If an implementation changes the architecture, update the architecture docs in
  the same change. At minimum check `docs/architecture/ai-workflow.md`,
  `docs/architecture/README.md`, the affected architecture topic docs, affected
  crate READMEs, and this file.

<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
|------|----------|
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.
