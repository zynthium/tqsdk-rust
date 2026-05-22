# SDK Overdesign Audit 2026-05-22

## Verdict

The SDK is not overdesigned because it has multiple crates. The crate split is mostly justified by trading-SDK invariants: one runtime state tree, one commit/revision model, separate one-shot direct query, single-owner wait consumption, multi-consumer stream consumption, execution tooling, session replay control-plane helpers, task strategy/backtest replay, and offline market cache replay concerns.

The active overdesign risk is public presentation and stabilization breadth. Too many advanced or foundation surfaces are documented as if they are ordinary SDK product promises, and several workflows can be reached through multiple public-looking paths without a clear canonical choice. The next iteration should subtract from first-read docs, quarantine advanced APIs, and stabilize only the smallest high-performance path for each user workflow.

## Evidence Scope

Primary in-repo evidence:

- `README.md`
- `docs/architecture/README.md`
- `docs/architecture/crate-boundaries.md`
- `docs/architecture/api-layers.md`
- `docs/architecture/api-wait.md`
- `docs/architecture/api-stream.md`
- `docs/reviews/public-api-scenario-review.md`
- `docs/reviews/public-api-disposition-matrix.md`
- `docs/scenarios/user-layer-iteration-plan.md`
- `crates/tqsdk/src/lib.rs`
- `crates/tqsdk-wait/src/lib.rs`
- `crates/tqsdk-stream/src/lib.rs`
- `crates/tqsdk-task/src/lib.rs`
- `crates/tqsdk-data/src/lib.rs`
- `crates/tqsdk/README.md`
- `crates/tqsdk-wait/README.md`
- `crates/tqsdk-stream/README.md`
- `crates/tqsdk-task/README.md`
- `crates/tqsdk-data/README.md`

External calibration:

- Official Python SDK: <https://github.com/shinnytech/tqsdk-python>
- Unofficial Rust contrast: <https://github.com/pseudocodes/tqsdk-rs/tree/main/tqsdk-rs>

Architecture docs and current code are authoritative. Existing review docs, scenario docs, archived plans, and external repositories are evidence, not overriding contracts.

## Necessary Complexity Map

| Area | Keep | Reason |
| --- | --- | --- |
| `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor` | Yes | A trading SDK needs stable snapshots, reconnect/resync barriers, causality, and commit-bound change interpretation. |
| `tqsdk-core` protocol/runtime substrate | Yes | It protects protocol completeness and avoids tying the kernel to one facade style. |
| `tqsdk-session` one-shot request/response/direct query | Yes | GraphQL, schema, metadata, calendar, ranking, settlement, EDB, auth refresh, and session replay control-plane helpers are not live object consumption. |
| `tqsdk-wait` single-owner `wait_update()` facade | Yes | It is the closest Rust equivalent to the official Python strategy loop while preserving typed refs and explicit errors. |
| `tqsdk-stream` advanced multi-consumer facade | Yes, as advanced | It is justified by independent consumers, bounded lag, filtering, health, and `futures::Stream` composition, not by ordinary quote throughput alone. |
| `tqsdk-task` execution tools | Yes, narrowed | Target position, ownership guard, typed order builder, basic risk, strategy test harness, local sim, and task strategy/backtest replay are mature trading workflow needs. |
| `tqsdk-data` research/offline data | Yes, narrowed | History page/series/download, CSV export, Greeks, cache, and offline market cache replay are opt-in research/offline workflows that should not pollute live facades. |
| `tqsdk` top-level facade | Yes, thin | Ordinary users need one dependency and one obvious first path while advanced users can depend on sibling crates directly. |

## Overdesign Risk Map

| Risk | Evidence | Decision |
| --- | --- | --- |
| First-read docs teach crate taxonomy before user flow | `README.md` starts with workspace/crate explanation before a default facade workflow has become convincing. | Rewrite first-read docs around `tqsdk` install, connect, quote, wait, order/target, and history; move crate taxonomy below. |
| `tqsdk::advanced::*` is ambiguous | `crates/tqsdk/src/lib.rs` exposes curated subsets but docs can read as a full sibling-crate portal. | Document it as curated convenience only; tell full-power users to depend on sibling crates directly. |
| `tqsdk-stream` root surface is wide | `crates/tqsdk-stream/README.md` lists many object/event streams. | Keep `quote_batches`, commit stream, filters, lag, health, shutdown, and scenario-backed object/event streams active but advanced; higher-level family APIs or unproven broad families need further review before semver stabilization. |
| `tqsdk-task` reads like a strategy platform | README and scenario docs include legitimate task-layer primitives such as supervisor, deployment wrappers, task strategy/backtest replay, trading desk profile, telemetry hooks, fake broker, and local sim, but some wording can read as production platform ownership. | Keep these primitives as task-layer foundations; remove wording that implies production daemon, OMS, durable audit, managed sink, or platform ownership. |
| `tqsdk-data` presents a narrow goal with a long surface list | README lists history, cache, download, CSV, Greeks, offline market cache replay, and cache maintenance types. | Keep as opt-in research/offline crate; ensure first-read docs do not imply live cache service or hot-path dependency. |
| Active docs still contain stale names or claims | `docs/architecture/api-layers.md` and `docs/architecture/api-wait.md` use `tqsdk-api-*`; `crates/tqsdk-task/README.md` still mentions stream managed sink/WAL/journal. | Rename current docs to `tqsdk-wait`/`tqsdk-stream` and remove stale sidecar ownership claims. |

## Official Python SDK Benchmark

| Python SDK semantics | Rust classification | Rust direction |
| --- | --- | --- |
| One default API object that owns connection and state | Match behavior | `tqsdk::Tq` should be the ordinary entrypoint, but remain a thin facade over `tqsdk-wait`/`task`/`data`. |
| `wait_update()` as central progression point | Match behavior | `tqsdk-wait` and `tqsdk::Tq::next()` remain canonical for single-owner strategy loops. |
| Stable business snapshot after update | Match behavior and improve | Rust keeps revision-bound reads and typed refs instead of mutable dict/DataFrame-style access. |
| Reconnect resubscription and snapshot barrier | Match behavior | Startup/recovery barriers belong in wait/session on the shared runtime state tree. |
| `get_quote`, quote list, kline/tick serials | Match behavior and improve | Keep typed refs/handles and add step-bound changed quote helpers where needed for full-universe workloads. |
| Order insert/cancel, account/position/order/trade refs | Match behavior and improve | Keep typed order price, direction, offset, tickets, and explicit `Result` errors. |
| `TargetPosTask` one owner per account/symbol | Match behavior | Keep in `tqsdk-task` and expose ordinary wrapper through `tqsdk` only when it stays thin. |
| Local risk hooks | Match behavior and improve | Keep basic pre-trade risk in task with typed rejection reports; avoid global risk service claims. |
| Task strategy/backtest replay and downloader/data series | Match behavior with separate ownership | Keep local sim and task strategy/backtest replay in task, and history/cache/download plus offline market cache replay in data; do not make them default live-loop concerns. |
| Single huge `TqApi` class and broad root namespace | Reject shape | Rust should prefer typed builders, enums, newtypes, feature gates, and explicit advanced crates. |
| Implicit mutable pandas-like tables | Reject shape | Rust should keep borrowed/owned typed snapshots and low-copy hot paths. |

## Unofficial Rust Contrast

| Observation from `pseudocodes/tqsdk-rs` | Use for this SDK |
| --- | --- |
| Single `Client` / `ClientBuilder` is easy to explain | Keep the first `tqsdk` screen this direct. |
| README quickly shows market, history, trade, builder, callback, channel, and stream usage | Improve `tqsdk-rust` first-read examples, but avoid presenting all paradigms as equal defaults. |
| Broad root re-exports expose auth, WebSocket, logger, data manager, subscription, and transport-like concerns | Do not copy this; keep implementation details out of the default facade. |
| Callback/channel/stream options appear together in the ordinary path | Use as a cautionary example; `tqsdk-rust` should name one canonical path per workflow. |

## Canonical High-Performance API Paths

| Workflow | Canonical public path | Advanced escape hatch | Paths to de-emphasize in first-read docs |
| --- | --- | --- | --- |
| Ordinary strategy setup | `tqsdk::prelude::*`, `Tq::futures().auth_env().connect().await?` | `tqsdk-wait::TqApiBuilder` for users who want direct wait facade ownership | Starting with `tqsdk-core`, raw session builders, or stream builders. |
| Single-owner quote subscription | `Tq::quote` / `Tq::quotes` through the default facade when available, otherwise `tqsdk-wait::TqApi::quote(s)` | `SessionClient::subscribe_quotes` plus `RuntimeReader` for low-level hot-path users | `tqsdk-stream` examples that imply stream is required for quote throughput. |
| Full-universe single-consumer quote workload | `tqsdk-wait::TqApi::quotes` plus step-bound changed quote iteration | `SessionClient + RuntimeReader::read_market_state()` for custom cursor loops | Per-commit full symbol scans, duplicate raw-session loops in ordinary docs, and stream-only performance framing. |
| Multi-consumer quote/event pipelines | `tqsdk-stream::TqStream::quote_batches` | `commit_stream` plus filters and `RuntimeReader` | `tqsdk-wait` loops with user-managed fan-out channels. |
| Direct metadata/query | `tqsdk-session::SessionClient` query helpers | Raw GraphQL value query in `tqsdk-session` | Duplicating direct query helpers into wait or stream. |
| Kline/tick live serials | `tqsdk-wait::TqApi::kline` / `tick` for single-owner strategies | `tqsdk-stream` row-batch streams for async integration | `tqsdk-data` history/cache docs as live serial source. |
| Order insert/cancel in a strategy loop | `tqsdk::Tq` thin wrappers where present, otherwise `tqsdk-wait` typed order helpers | `tqsdk-task::TaskHost::orders` when task ownership/risk is needed; raw session command only for low-level users | Multiple equivalent root/wait/task examples with no semantic distinction. |
| Target position | `tqsdk::TargetPos` thin wrapper for ordinary TQKQ flow; `tqsdk-task::TargetPosTask` for explicit task users | `TaskHost` for advanced ownership and scheduling | Raw order loops in beginner docs as the recommended target-position path. |
| History download and research data | `tqsdk::Tq::history()` thin helper for ordinary users; `tqsdk-data::DataClient` for explicit data workflows | Session-backed `DataClient::from_session` for shared auth/session | Direct query helpers used as substitutes for data download/cache APIs. |
| Backtest/replay/local sim | `tqsdk-task` and `tqsdk-data` examples for explicit offline workflows | Custom replay into `tqsdk-core`/`tqsdk-session` only for SDK developers | Presenting replay/deployment/supervisor as default first-run SDK usage. |

## Wait / Stream Boundary Decision

`tqsdk-stream` should not be justified by claiming it is the faster way to subscribe to all quotes. If `tqsdk-wait` can expose changed quote batches or step-bound changed snapshots without scanning all symbols, then full-universe quote subscription for one strategy loop belongs in the wait/default path.

`tqsdk-stream` remains necessary for workloads with different semantics:

- independent consumers that must advance at their own pace;
- bounded fan-out with explicit lag diagnostics;
- path/domain/object/field filters;
- commit/event pipelines;
- async service composition with `futures::Stream`;
- health, reconnect monitoring, retry policy, and graceful shutdown primitives for service integration.

The rule is consumption model first, throughput second. Single-owner stable snapshot consumers should start with `tqsdk`/`tqsdk-wait`; multi-consumer async systems should use `tqsdk-stream`.

## Accepted Wait-Side Performance Follow-Up

The audit should create a follow-up source plan for wait quote iteration only after this docs batch lands. Candidate public shapes to evaluate:

| Candidate | Purpose | Constraint |
| --- | --- | --- |
| `QuoteSet::changed(&WaitStep)` | Iterate changed quote refs for the current step. | Must use the step commit/change set and avoid scanning every subscribed symbol. |
| `QuoteSet::changed_snapshots(&WaitStep)` | Return owned snapshots for changed symbols in the current step. | Must decode only touched symbols and preserve deterministic symbol order. |
| `WaitStep::changed_quote_symbols()` | Expose symbols touched by the current step. | Must not expose raw internal state paths as the ordinary API. |

These APIs should be additive and measured against the current `tqsdk-stream::QuoteBatchSubscription` performance shape before stabilization.
