---
name: tqsdk-rust
description: Use when building, explaining, debugging, or scaffolding Rust TQSDK code with tqsdk-core, tqsdk-session, tqsdk-wait, tqsdk-stream, tqsdk-task, or tqsdk-data; live market data, wait_update, async streams, metadata/direct queries, orders, TargetPosTask, strategy/replay, historical K-line/tick data, CSV/cache/export, permissions, or Rust equivalents of Python TqSdk workflows.
---

# TQSDK Rust

Use this skill to map a TQSDK Rust request to the correct crate, calling pattern, and minimal code while preserving the workspace's crate boundaries.

## Route The Request First

Read only the references needed for the user's question.

1. Read [references/scenario-router.md](references/scenario-router.md) first for every request. Classify by what the user wants to hold or consume, not by the first API name they mention.
2. Read [references/crate-selection.md](references/crate-selection.md) for dependency form, crate boundaries, or when the user is unsure which crate to use.
3. Read [references/code-patterns.md](references/code-patterns.md) before writing example code or fixing compile errors in examples.
4. Read [references/quant-workflows.md](references/quant-workflows.md) for strategy loops, event buses, research, replay, testing, or low-latency desk workflows.
5. Read [references/safety-and-operations.md](references/safety-and-operations.md) for credentials, permissions, live trading, simulation, order side effects, risk, or live smoke tests.
6. Use [scripts/new-tqsdk-rust-project.py](scripts/new-tqsdk-rust-project.py) with [assets/templates/wait-quote-loop](assets/templates/wait-quote-loop) only when the user asks for a new standalone starter project.

## Core Rules

- Choose the highest-level crate that fits the scenario before writing code.
- Treat official Python TqSdk behavior as the semantic reference, but map it through Rust crate ownership instead of recreating Python's monolithic `TqApi`.
- Keep one-shot queries in `tqsdk-session`, live refs in `tqsdk-wait`, event pipelines in `tqsdk-stream`, execution helpers in `tqsdk-task`, and offline/history work in `tqsdk-data`.
- Use `tqsdk-core` only for low-level runtime, custom facade, adapter, command state machine, commit/cursor, or hot-path `RuntimeReader` work.
- All visible state changes must flow through runtime commits and `RuntimeReader` / `UpdateCursor`; do not invent private state trees, local order overlays, or bypass notifications.
- For live/network examples, assume Tokio, credentials, market data permissions, and explicit trading permissions are required.
- Prefer named builders such as `futures_market()`, `stock_market()`, `trade_target_tqkq()`, and `enable_query()` over raw boolean route flags.
- For order placement, default to simulation/TqKq-style examples unless the user explicitly asks for real-account integration and accepts side effects.
- When exact API shape matters, check the target crate README and `crates/*/examples/api_contract_sXX_*.rs` before finalizing code.

## Common Mistakes to Prevent

- Do not answer direct-query questions with `tqsdk-wait`; use `tqsdk-session` or `api.session()`.
- Do not create a second client just for metadata in a wait/stream app; reuse the shared session.
- Do not treat historical downloads as live refs; use `tqsdk-data`.
- Do not start ordinary user examples from `tqsdk-core` unless the user asks for runtime internals.
- Do not invent local order overlays or parse status strings when typed tickets, refs, or status helpers exist.
- Do not use string or adapter-local checks to bypass `record_command_status()` and the runtime command lifecycle.
- Do not hide credentials, permissions, or live-order side effects in examples.
- Do not move direct query, downloader, task, or research semantics across crate boundaries while answering a usage question.

## Answering Style

- Start by naming the crate and the reason: live ref, event stream, one-shot query, task execution, offline rows, or runtime substrate.
- Prefer short Rust snippets that match current examples over broad pseudocode.
- Name the exact API the user should call next.
- State when the Rust answer intentionally differs from Python TqSdk because the Rust workspace splits `session`, `wait`, `stream`, `task`, and `data`.
- Mention safety gates early when code can place orders, cancel orders, use real accounts, or depend on paid data permissions.
- If a request is ambiguous, ask one shape question: "Do you want one live loop with refs, multiple event consumers, a one-shot query, a task/order abstraction, historical rows, or runtime commits?"

## Project Scaffold

To create a minimal quote loop project from the bundled asset template:

```bash
python3 scripts/new-tqsdk-rust-project.py ./my-tqsdk-app \
  --sdk-source git \
  --sdk-value https://github.com/OWNER/tqsdk-rust \
  --symbol SHFE.au2602
```

Use `--sdk-source path --sdk-value /path/to/tqsdk-rust` for local development, or `--sdk-source version --sdk-value <version>` after crates are published.
