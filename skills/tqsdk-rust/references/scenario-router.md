# Scenario Router

Use this file first. Classify by the object the user wants to hold or consume. Python TqSdk is the semantic reference, but Rust keeps the monolithic Python `TqApi` surface split across crates.

## Quick Decision

| If the user says | They probably need | Crate | Primary calls |
| --- | --- | --- | --- |
| "实时行情", "quote", "盘口", "价格变化", "像 Python TqApi", "wait_update", "is_changing" | Single-owner live quote/trade loop | `tqsdk-wait` | `TqApiBuilder`, `get_quote`, `wait_update`, `is_changing`, `QuoteRef::load` |
| "K线 serial", "tick serial", "窗口", "bar 更新", "trading_status" | Live serial/window/status view | `tqsdk-wait` | `get_kline_serial`, `get_tick_serial`, `get_trading_status`, `wait_update`, window/ref load methods |
| "多消费者", "事件流", "stream", "fan-out", "写 WAL", "异步管道", "lag" | Multi-consumer event pipeline | `tqsdk-stream` | `TqStreamBuilder`, `commit_stream`, filters, `quote_stream`, `market_events`, trade/session event streams, sink APIs |
| "查合约", "主连", "期权链", "交易日历", "结算价", "排名", "EDB", "schema", "metadata" | One-shot metadata/service query | `tqsdk-session` | `SessionClientBuilder`, `enable_query`, `query_instrument_specs`, `query_cont_quotes`, `get_trading_calendar` |
| "下单", "撤单", "目标持仓", "调仓", "策略下单", "风控", "scheduler", "多账户", "fake broker" | Strategy execution layer | `tqsdk-task` when ownership/risk/task semantics are needed; `tqsdk-wait` for thin direct order wrappers | `TaskHost`, `TargetPosTask`, `RiskEngine`, typed order builders, `OrderTicket`, strategy/test harness APIs |
| "历史K线", "历史 tick", "下载", "CSV", "离线研究", "缓存", "回放", "Greeks", "data_series" | Historical/offline research | `tqsdk-data` | `DataClient`, `get_*_data_series`, `*_data_download`, `export_*_csv`, cache/replay APIs |
| "低延迟", "同一 revision", "cursor", "commit", "runtime", "adapter", "command status" | Low-level substrate or custom facade | `tqsdk-session` plus `tqsdk-core` | `SessionClient`, `progress_once`, `RuntimeReader`, `cursor`, `read_market_trade_state` |

## Calling Pattern by Scenario

### 1. Monitor one symbol in a strategy loop

Use `tqsdk-wait`.

Call sequence:

1. Build `TqApi` with credentials.
2. Call `get_quote(symbol).await`.
3. Loop on `wait_update(None).await?`.
4. Use `is_changing(&quote)?`.
5. Load the quote snapshot only after a relevant change. Treat refs as live handles, not owned snapshots.

Read next: `references/code-patterns.md#wait-quote-loop`.

### 2. Fetch contract metadata before starting a strategy

Use `tqsdk-session`.

Call sequence:

1. Build `SessionClient` with the right market route. `SessionClientBuilder::build()` is synchronous.
2. Add `enable_query()` when official query semantics are needed.
3. Call the specific one-shot helper.
4. Reuse `api.session()` or `stream.session()` if already inside wait/stream.

Typical helpers: `query_symbol_info`, `query_instrument_specs`, `query_quotes`, `query_cont_quotes`, `query_options`, `query_atm_options`, `get_trading_calendar`, `query_symbol_settlement`, `query_symbol_ranking`, `query_edb_data`.

### 3. Build a live data bus

Use `tqsdk-stream`.

Call sequence:

1. Build `TqStream`.
2. Subscribe or choose a typed stream/event API.
3. Apply commit/path/scope/domain/object/field filters when possible.
4. Keep slow persistence in stream sinks or sidecars.
5. Use `stream.session()` for metadata instead of opening another query client.
6. Handle lag/closed/error reports explicitly; fan-out is bounded.

### 4. Implement a target-position strategy

Use `tqsdk-task`.

Call sequence:

1. Build `TqApi` from `tqsdk-wait`.
2. Wrap it in `TaskHost`.
3. Configure `RiskEngine` if needed.
4. Create `TargetPosTask` or typed order builder.
5. Let `TaskHost::wait_update()` drive task progress.
6. Use typed tickets/reports instead of parsing status strings.
7. Keep real-account order placement opt-in; examples should make side effects visible.

### 5. Download history for research or backtest input

Use `tqsdk-data`.

Call sequence:

1. Build or reuse a session when websocket-backed history is required.
2. Create `DataClient` or `DataClient::from_session(session)`.
3. Choose page, series, download, CSV export, cache, or replay API.
4. Keep output owned/materialized; do not model it as live refs.
5. Use cache/replay APIs for deterministic strategy tests instead of live credentials when possible.

### 6. Write a low-latency custom loop

Use `tqsdk-session + RuntimeReader`, or `tqsdk-task` trading desk profile if the task is execution-oriented.

Call sequence:

1. Build `SessionClient` and subscribe/login explicitly.
2. Drive with `progress_once` or the profile's loop primitive.
3. Consume commits with `RuntimeReader::cursor()` / `next()`.
4. Read hot state via `read_market_state`, `read_trade_state`, or `read_market_trade_state`.
5. Keep logging, journal, and metrics out of the hot decision path.
6. Do not mutate domain state outside the runtime mutation path.

## If Still Ambiguous

Ask one clarifying question based on shape, not implementation detail:

"Do you want one live loop with refs, a multi-consumer event stream, a one-shot metadata query, a trading task abstraction, or historical/offline data?"
