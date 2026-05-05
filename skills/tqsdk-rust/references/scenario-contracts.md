# Scenario Contracts

Use this file when a request asks for examples by role, broad scenario coverage, or public API contract evidence. Prefer the formal `crates/*/examples/api_contract_sXX_*.rs` examples over archived sketches. S14 is the only active gap and is not a near-term core SDK target.

## User Roles

| User role | Primary crate(s) | Contract examples | Notes |
| --- | --- | --- | --- |
| Single-strategy author | `tqsdk-wait`, `tqsdk-task` | S1, S3, S6-S11, S25-S26, S29 | Python-style stable `wait_update()` loop, live refs, thin order wrappers, startup recovery, reconnect-safe order intent, target-pos ownership. |
| Async system integrator | `tqsdk-stream`, `tqsdk-session` | S2, S4, S20-S22 | Multi-consumer streams, dynamic subscriptions, market events, health, retry diagnostics, slow-consumer isolation, managed sinks. |
| Low-level / latency-sensitive user | `tqsdk-session`, `tqsdk-core`, `tqsdk-task` | S5, S23, S27, S31 | Thin session substrate, direct metadata query, hot-path `RuntimeReader`, same-revision market/trade reads, low-latency desk profile. |
| Execution tooling user | `tqsdk-task`, `tqsdk-wait` | S6-S13, S19, S29, S31 | Typed order tickets, cancel/partial-fill helpers, risk gates, execution groups, account groups, target-pos ownership. |
| Research / market-data user | `tqsdk-data`, `tqsdk-session`, `tqsdk-task` | S16-S18, S23, S27-S30 | Historical series, downloads, CSV export, option Greeks, local cache/replay, Python-compatible history cache. |
| Test / replay user | `tqsdk-task`, `tqsdk-data` | S15-S16, S18, S24, S30 | Live/sim/replay environment, deterministic fake market/broker, replay sources, history/cache-backed tests. |
| Production runtime builder | `tqsdk-stream`, `tqsdk-task` | S20-S22 | Typed health/telemetry, graceful shutdown, retry policy, bounded fan-out, WAL/sink foundation. No built-in HTTP endpoint, GUI, or daemon manager. |
| Multi-provider infrastructure user | user-level facade / future project | S14 gap only | Multi-provider aggregation is intentionally not a current core SDK API. Do not push it into core/session. |

## Role Answer Playbooks

Use these when answering broad "what should I use?" or "give examples for each role" prompts.

### Single-Strategy Author

Start with `tqsdk-wait` for the live loop. Add `tqsdk-task` only when the user needs target-position ownership, risk gates, strategy context, or test harnesses.

- First examples: S1 quote loop, S3 snapshot, S25 serial/status, S6-S7 order lifecycle, S8 account/position, S9 startup recovery, S10 reconnect order intent.
- Execution upgrade: S11 simple strategy and S29 target-pos ownership.
- Avoid: direct metadata helpers on `TqApi`, local order overlays, parsing order status strings, hidden real-account side effects.

### Async System Integrator

Start with `tqsdk-stream` when there are multiple consumers, event pipelines, slow sinks, or production health concerns. Reuse `stream.session()` for one-shot metadata.

- First examples: S2 dynamic subscriptions, S4 mixed market events, S21 slow consumer isolation.
- Production examples: S20 stream health/graceful shutdown, S22 retry diagnostics.
- Avoid: cloning full snapshots into every consumer, unbounded channels, duplicate metadata sessions, moving stream sink durability into `tqsdk-data`.

### Low-Level / Latency-Sensitive User

Start with `tqsdk-session + RuntimeReader` for thin control over session progress and hot reads. Use `tqsdk-task` S31 only when the hot path is execution-oriented.

- First examples: S5 bare market fast path, S23 contract metadata, S27 metadata/service query pack.
- Trading desk example: S31 same-revision market/trade read and prechecked order submission.
- Avoid: starting from `tqsdk-core` for ordinary usage, reading full snapshots in hot paths, using history cache for live hot-path decisions.

### Execution Tooling User

Start with `tqsdk-task` when orders need ownership, grouping, risk, scheduling, or multi-account accounting. Use wait order wrappers only for thin order submission.

- First examples: S6 limit order, S7 cancel/partial fill, S10 reconnect order consistency.
- Task examples: S11 simple strategy, S12 execution group, S13 account group, S19 risk, S29 target-pos ownership.
- Avoid: automatic hedge/flatten promises, cross-process durable audit claims, user code that bypasses task ownership guard.

### Research / Market-Data User

Start with `tqsdk-data` for owned historical rows, downloads, CSV, Greeks, and cache/replay materialization. Use `tqsdk-session` only for one-shot metadata needed to frame the query.

- First examples: S17 research K-line batch, S28 download/export and Greeks.
- Cache/replay examples: S18 local market cache, S30 history series cache, S16 replay integration.
- Metadata examples: S23 and S27.
- Avoid: modeling history as live refs, placing DataFrame/polars semantics into session/wait, using live credentials for deterministic tests.

### Test / Replay User

Start with `tqsdk-task::testing` for deterministic strategy tests. Use `tqsdk-data` cache/history records as replay input.

- First examples: S24 testable strategy, S15 live/sim/replay switch, S16 history replay, S18 cache replay, S30 history cache.
- Avoid: hidden `*_for_test` APIs, provider protocol fixtures, `Arc<Mutex<_>>` test orchestration, live services in unit tests.

### Production Runtime Builder

Start with `tqsdk-stream` for stream health, reconnect monitoring, bounded fan-out, and sinks. Add `tqsdk-task` supervisor for strategy lifecycle.

- First examples: S20 stream health and strategy supervisor, S21 slow consumer isolation, S22 retry diagnostics.
- Avoid: claiming built-in HTTP health endpoints, GUI, process manager, distributed queue, runtime state snapshot recovery, or cross-process daemon orchestration.

### Multi-Provider Infrastructure User

Treat this as an unsupported core SDK scenario. Point to S14 only as a boundary sketch.

- Supported answer: "The current SDK exposes single-session/session-backed facades; multi-provider aggregation belongs in a user-level facade or future separate project."
- Avoid: adding provider aggregation to `tqsdk-core`, mixing multiple state trees into ordinary wait/stream APIs, implying existing public support.

## Contract Map

| Scenario | Formal example or gap | Use when user asks for | Primary APIs / boundary |
| --- | --- | --- | --- |
| S1 Zero-barrier quote | `crates/tqsdk-wait/examples/api_contract_s01_zero_barrier_quote.rs` | Basic live quote loop, Python-like `wait_update` | `TqApi::get_quote`, `wait_update`, `is_changing`; live refs stay in wait. |
| S2 Dynamic subscriptions | `crates/tqsdk-stream/examples/api_contract_s02_dynamic_subscriptions.rs` | Add/remove multiple symbols in async stream | `TqStream::quotes`, `QuoteSubscription::{add, remove, symbols}`; reconnect requeues subscription intent. |
| S3 Quote snapshot | `crates/tqsdk-wait/examples/api_contract_s03_quote_snapshot.rs` | One ready quote snapshot without hand-written loop | `TqApi::quote_snapshot`; still wait facade, not session metadata. |
| S4 Mixed market streams | `crates/tqsdk-stream/examples/api_contract_s04_mixed_market_streams.rs` | Quote/tick/kline event bus | `TqStream::market_events`, `MarketEventStream`, typed market events. |
| S5 Bare market fast path | `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs` | Low-level market subscription and hot reads | `SessionClient::{subscribe_quotes, progress_once}`, `RuntimeReader::read_market_state`; avoid high-level facade overhead. |
| S6 Limit order | `crates/tqsdk-wait/examples/api_contract_s06_limit_order.rs` | Ordinary order placement | `login_trade_account`, `limit_order`, `LimitOrderIntent::send_once`, `OrderTicket::wait_terminal`; side effects explicit. |
| S7 Cancel / partial fill | `crates/tqsdk-wait/examples/api_contract_s07_cancel_partial_fill.rs` | Partial-fill wait, cancel remaining, terminal wait | `OrderTicket` / `OrderRef` helpers; do not parse raw status strings. |
| S8 Account / position | `crates/tqsdk-wait/examples/api_contract_s08_account_position_updates.rs` | Funds, account, position live refs | `get_account`, `get_position`; wait live state, not direct query. |
| S9 Startup recovery | `crates/tqsdk-wait/examples/api_contract_s09_startup_state_recovery.rs` | Ready barrier after startup/reconnect | `StartupRecoverySpec`, `TqApi::startup_recovery`, `TqStream::recover_state`. |
| S10 Reconnect order consistency | `crates/tqsdk-wait/examples/api_contract_s10_reconnect_order_consistency.rs` | Idempotent order intent within one session | `OrderIntentRecord`, `OrderTicketState`, stable client intent; cross-process persistence is out of scope. |
| S11 Simple strategy | `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs` | Strategy with quote/account/position and orders | `StrategyHost`, `StrategyContext`, `TaskHost::orders`, `RiskEngine`, `TargetPosTask`. |
| S12 Spread arbitrage | `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs` | Two-leg execution foundation | `ExecutionGroupBuilder`, `ExecutionGroupOutcome`, revision-bound report; automatic hedge/flatten is user-layer. |
| S13 Multi-account ordering | `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs` | Account group, ratio split, per-account outcome | `AccountGroup`, `MultiAccountOrderTicket`, revision-bound account report; advanced compensation/audit is user-layer. |
| S14 Multi-provider aggregation | `docs/scenarios/api_gaps/api_contract_s14_multi_provider_market_aggregation.rs` | Multiple market providers, failover, dedupe | Active gap and non-core SDK target. Keep as boundary sample; do not move into core/session. |
| S15 Live / sim / replay switch | `crates/tqsdk-task/examples/api_contract_s15_live_sim_replay_switch.rs` | Same strategy across live, sim, replay | `StrategyEnvironment`, `StrategyDeployment`, `StrategySupervisor`; multi-provider environment remains out of scope. |
| S16 History replay strategy | `crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs` | Run strategy on historical/cache events | `StrategyReplay`, `StrategyReplaySourceBuilder`, checkpoint/speed controls; production daemon reconnect is out of scope. |
| S17 Research kline batch | `crates/tqsdk-data/examples/api_contract_s17_research_kline_batch.rs` | Batch historical K-line research | `DataClient::get_kline_data_series`; owned rows, not live refs. |
| S18 Local market cache | `crates/tqsdk-data/examples/api_contract_s18_local_market_cache.rs`; `crates/tqsdk-data/examples/api_contract_s18_live_market_cache_pipe.rs` | JSONL cache record/replay or single-process live pipe | `MarketCacheWriter`, `MarketCacheReader`, `MarketCacheReplay`, `MarketCacheStreamWriter`; cross-process cache daemon is user-layer. |
| S19 Pre-trade risk | `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs` | Local risk gates before order submit | `RiskEngine`, `RiskCheckReport`, `RiskProjectionReport`, `RiskDecision`; portfolio margin engine and durable audit are out of scope. |
| S20 Production primitives | `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`; `crates/tqsdk-task/examples/api_contract_s20_strategy_supervisor.rs` | Health, reconnect monitor, graceful shutdown, strategy supervisor | Typed health/telemetry/shutdown primitives only; no built-in GUI, HTTP endpoint, or process manager. |
| S21 Slow consumer isolation | `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs` | Bounded fan-out, lag diagnostics, WAL/sink foundation | `spawn_commit_sink`, `StreamSinkOptions`, retry/WAL/recovery/journal types; distributed queue is out of scope. |
| S22 Error diagnosis / retry | `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs` | Retryable errors, backoff, typed diagnostics | `StreamFacadeError::diagnostic`, `StreamRetryPolicy`, retry decisions; business retry audit belongs to user execution systems. |
| S23 Contract metadata | `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs` | Instrument specs, contract class, normalized metadata | `SessionClient::query_instrument_specs`, `InstrumentSpec`, `InstrumentClass`; one-shot session query. |
| S24 Testable strategy | `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs` | Unit-test strategy without live services | `StrategyTestHarness`, `FakeMarket`, `FakeBroker`, `StrategyTestClock`; full exchange simulator is out of scope. |
| S25 Wait serial/status | `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs` | Trading status, K-line serial, tick serial | `get_trading_status`, `get_kline_serial`, `get_tick_serial`, `is_changing_fields`; not session/data. |
| S26 Wait trade/system refs | `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`; `crates/tqsdk-wait/examples/api_contract_s26_security_trade_refs.rs` | Notifications, settlement, risk refs, security trade refs | Wait live refs and command wrappers such as `confirm_settlement`; not direct query. |
| S27 Metadata/service query pack | `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs` | Quotes list, main contracts, options, calendar, settlement, ranking, EDB | `SessionClient` typed one-shot metadata/service APIs; do not copy to wait/stream. |
| S28 Download/export/Greeks | `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`; `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs` | History downloads, CSV export, option Greeks | `DataClient` research/download APIs; not live session refs. |
| S29 TargetPos ownership | `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs` | Same account+symbol task ownership and scheduler ownership | `TaskHost::{target_pos,target_pos_scheduler,check_manual_order_allowed}`; cross-account target-pos orchestration is user-layer. |
| S30 History series cache | `crates/tqsdk-data/examples/api_contract_s30_history_series_cache.rs` | Opt-in Python-compatible mmap history cache | `DataClientBuilder::history_cache_enabled`, cache reports, cache-only readers; simultaneous Python/Rust writes are non-goal. |
| S31 Low-latency desk | `crates/tqsdk-task/examples/api_contract_s31_low_latency_trading_desk.rs` | Same-revision market/trade hot path and prechecked orders | `TradingDeskProfile`, `RuntimeReader::read_market_trade_state`, typed latency/order reports; not an OMS or auto-hedger. |

## Coverage Rules

- For a broad answer, cover the user's role first, then list only the scenarios that role naturally touches.
- Use active formal examples as source of truth. Archived scenario files are historical context unless the formal example explicitly points there for a boundary decision.
- For S14, say clearly that the desired sketch exists only to preserve a boundary sample; do not imply support.
- When a scenario's remaining advanced features are out of scope, state the supported foundation and the user-layer responsibility in the same paragraph.
- If no contract example exists for a requested workflow, identify the closest supported scenario and whether the request is a new gap, a user-layer system, or a normal composition of existing APIs.
