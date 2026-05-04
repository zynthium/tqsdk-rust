---
name: tqsdk-rust
description: Scenario router and development guide for Rust TQSDK quantitative apps, including choosing the right crate and calling pattern for live market data, wait_update loops, async streams, direct metadata queries, trading tasks, historical/offline data, replay, safety, and project scaffolding. Use when Codex helps users build Rust quant strategies or tooling with tqsdk-core, tqsdk-session, tqsdk-wait, tqsdk-stream, tqsdk-task, or tqsdk-data.
---

# TQSDK Rust

Use this skill to help external agents and SDK users quickly map a quantitative development scenario to the correct Rust TQSDK crate, calling pattern, and starter code.

## Fast Path

1. Read `references/scenario-router.md` first for every user request.
2. Pick the matching scenario and call pattern before writing code.
3. Load only the supporting reference needed for the selected path.

| Need | Read or use |
| --- | --- |
| Fast scenario-to-API routing | `references/scenario-router.md` |
| Crate boundary and dependency form | `references/crate-selection.md` |
| Strategy, trading loop, research, replay, or low-latency workflow | `references/quant-workflows.md` |
| Concrete Rust snippets and dependency patterns | `references/code-patterns.md` |
| Credentials, live trading, simulation, permissions, risk, or operational safety | `references/safety-and-operations.md` |
| New standalone quote-loop project | `scripts/new-tqsdk-rust-project.py` with `assets/templates/wait-quote-loop` |

## Scenario Classification

Classify the request by the thing the user wants to hold or consume:

| User wants | Route to |
| --- | --- |
| A changing live object in one loop | `tqsdk-wait` refs plus `wait_update()` |
| Multiple async consumers or event pipelines | `tqsdk-stream` stream/event APIs |
| One answer from metadata/query/service | `tqsdk-session` direct query APIs |
| Orders owned by a strategy/task/risk system | `tqsdk-task` task and typed order APIs |
| Historical/offline materialized rows | `tqsdk-data` history/download/cache APIs |
| Commit/cursor/runtime internals | `tqsdk-core` plus `tqsdk-session` |

## Core Rules

- Choose the highest-level crate that fits the scenario before writing code.
- Keep one-shot queries in `tqsdk-session`, live refs in `tqsdk-wait`, event pipelines in `tqsdk-stream`, execution helpers in `tqsdk-task`, and offline/history work in `tqsdk-data`.
- Use `tqsdk-core` only for low-level runtime, custom facade, adapter, commit/cursor, or hot-path reader work.
- Assume all live/network examples need Tokio, credentials, and market/trade permissions.
- Prefer named builders such as `futures_market()`, `stock_market()`, `trade_target_tqkq()`, and `enable_query()` over raw boolean route flags.
- For live trading code, default to simulation/TqKq unless the user explicitly asks for real-account integration.

## Common Mistakes to Prevent

- Do not answer direct-query questions with `tqsdk-wait`; use `tqsdk-session` or `api.session()`.
- Do not create a second client just for metadata in a wait/stream app; reuse the shared session.
- Do not treat historical downloads as live refs; use `tqsdk-data`.
- Do not start ordinary user examples from `tqsdk-core` unless the user asks for runtime internals.
- Do not invent local order overlays or parse status strings when typed tickets, refs, or status helpers exist.
- Do not hide credentials, permissions, or live-order side effects in examples.

## Project Scaffold

To create a minimal quote loop project from the bundled asset template:

```bash
python3 scripts/new-tqsdk-rust-project.py ./my-tqsdk-app \
  --sdk-source git \
  --sdk-value https://github.com/OWNER/tqsdk-rust \
  --symbol SHFE.au2602
```

Use `--sdk-source path --sdk-value /path/to/tqsdk-rust` for local development, or `--sdk-source version --sdk-value <version>` after crates are published.
