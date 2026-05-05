# Quant Workflows

For broad role or scenario coverage, read `scenario-contracts.md` and cite the matching formal examples.

## Live Monitoring

Use `tqsdk-wait` for one strategy loop or notebook-like live monitoring. Subscribe through `get_quote`, `get_trading_status`, `get_kline_serial`, or `get_tick_serial`, then call `wait_update()` and load refs only when `is_changing()` indicates a relevant commit. Refs are live handles; load snapshots after commits.

Contract anchors: S1, S3, S8-S10, S25-S26.

## Event Pipeline

Use `tqsdk-stream` when multiple independent consumers need the same session state: logging, metrics, signal calculation, persistence, and order monitoring. Use commit filters or typed streams instead of cloning snapshots in each consumer.

Contract anchors: S2, S4, S20-S22.

## One-Shot Research Query

Use `tqsdk-session` for metadata and service calls that return one result. Enable query support when needed, and reuse the session from wait/stream facades instead of creating a duplicate connection. Do not route symbol metadata through live `QuoteRef` objects just because Python exposes many helpers on one `TqApi`.

Contract anchors: S23, S27. Low-level live substrate: S5.

## Historical Research

Use `tqsdk-data` for history pages, time-range series, pull-based downloads, CSV export, and option Greeks. Keep historical materialization separate from live refs. Use history cache explicitly when large repeated reads matter.

Contract anchors: S17-S18, S28-S30. Replay integration: S16.

## Strategy Execution

Use `tqsdk-task` for target-position execution, order ownership, risk checks, schedulers, multi-account allocation, strategy context, replay, and fake broker tests. Prefer typed builders and typed tickets. Let `TaskHost` own the wait loop. For one-off order wrappers without ownership, `tqsdk-wait` is acceptable, but call out live-order side effects.

Contract anchors: S6-S13, S19, S29. Production lifecycle: S15, S20.

## Low-Latency Desk Loop

Use `tqsdk-session + RuntimeReader` or `tqsdk-task` trading desk profile for hot paths. Read market and trade state with `read_market_trade_state()` when one decision needs both partitions at the same revision. Use streams as sidecars for slow logging or persistence.

Contract anchors: S5, S31.

## Replay and Testing

Use `tqsdk-data` market cache records for offline event sources and `tqsdk-task` replay/fake broker tools for deterministic strategy tests. Do not require live credentials for unit-level strategy tests unless the user explicitly requests an integration smoke test. Keep live smoke tests ignored or environment-gated.

Contract anchors: S15-S16, S18, S24, S30.
