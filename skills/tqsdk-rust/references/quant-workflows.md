# Quant Workflows

## Live Monitoring

Use `tqsdk-wait` for one strategy loop or notebook-like live monitoring. Subscribe through `get_quote`, `get_trading_status`, `get_kline_serial`, or `get_tick_serial`, then call `wait_update()` and load refs only when `is_changing()` indicates a relevant commit. Refs are live handles; load snapshots after commits.

## Event Pipeline

Use `tqsdk-stream` when multiple independent consumers need the same session state: logging, metrics, signal calculation, persistence, and order monitoring. Use commit filters or typed streams instead of cloning snapshots in each consumer.

## One-Shot Research Query

Use `tqsdk-session` for metadata and service calls that return one result. Enable query support when needed, and reuse the session from wait/stream facades instead of creating a duplicate connection. Do not route symbol metadata through live `QuoteRef` objects just because Python exposes many helpers on one `TqApi`.

## Historical Research

Use `tqsdk-data` for history pages, time-range series, pull-based downloads, CSV export, and option Greeks. Keep historical materialization separate from live refs. Use history cache explicitly when large repeated reads matter.

## Strategy Execution

Use `tqsdk-task` for target-position execution, order ownership, risk checks, schedulers, multi-account allocation, strategy context, replay, and fake broker tests. Prefer typed builders and typed tickets. Let `TaskHost` own the wait loop. For one-off order wrappers without ownership, `tqsdk-wait` is acceptable, but call out live-order side effects.

## Low-Latency Desk Loop

Use `tqsdk-session + RuntimeReader` or `tqsdk-task` trading desk profile for hot paths. Read market and trade state with `read_market_trade_state()` when one decision needs both partitions at the same revision. Use streams as sidecars for slow logging or persistence.

## Replay and Testing

Use `tqsdk-data` market cache records for offline event sources and `tqsdk-task` replay/fake broker tools for deterministic strategy tests. Do not require live credentials for unit-level strategy tests unless the user explicitly requests an integration smoke test. Keep live smoke tests ignored or environment-gated.
