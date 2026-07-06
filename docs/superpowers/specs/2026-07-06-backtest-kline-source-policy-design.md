# Backtest Kline Source Policy Design

Date: 2026-07-06

## Goal

Make cache-backed `.backtest(start_ns, end_ns)` match the useful parts of
official `tqsdk-python` backtest behavior while keeping Rust's persistent tick
cache as the primary local store.

Policy:

- `duration <= 60s`: synthesize Klines locally from tick replay.
- `duration > 60s`: use native Kline data, matching Python chart behavior.
- Quote generation follows Python priority: tick first, then smallest Kline
  source, with automatic 1-minute fallback when no usable tick/Kline source
  exists.

## Current State

Rust cache-backed backtest currently uses `BacktestTickCache` as the durable
source and replays ticks through `HistoryTickReplayStream`. Missing tick ranges
can be filled from the official server-side backtest stream. The data layer
already has `HistorySeriesCache` support for both tick and Kline series, but
the default backtest builder only prepares tick series.

Official `tqsdk-python` backtest uses remote chart data directly:

- tick subscriptions produce quote updates from tick rows;
- Kline subscriptions produce quote updates from Kline rows when tick is not
  available;
- if a quote is needed but no tick/Kline source exists, or the smallest Kline
  period is above 1 minute, Python automatically subscribes to 1-minute Klines;
- `wait_update()` advances at most one market timestamp.

## Source Policy

### Tick Series

Ticks remain the canonical source for local simulated execution.

For every symbol that can drive orders, quote fallback, or synthesized
sub-minute/minute Klines, the backtest preparation layer must ensure tick
coverage according to the selected `BacktestCachePolicy`.

### Local Kline Synthesis

For `duration <= 60s`, Klines are synthesized from tick rows during local
backtest replay.

The synthesizer owns only ephemeral chart state:

- bar open time;
- open/high/low/close;
- volume delta within the bar;
- amount delta when available;
- open/close open interest;
- current chart window and row ids.

It does not write synthesized Klines back to `HistorySeriesCache` unless a
future design explicitly adds a derived-data cache.

### Native Kline Series

For `duration > 60s`, Klines are loaded as native Kline history series.

Preparation must check `HistorySeriesCache::kline_coverage` for the requested
symbol and duration. Under `RemoteOnMiss`, missing ranges are fetched through
the existing session-backed history/chart path and stored as native Kline
series. Under `CacheOnly`, missing native Kline coverage is a validation error.

Daily and longer periods must never be synthesized from ticks in this design.
This avoids mismatches around trading-day boundaries, night sessions,
continuous contracts, and exchange-specific calendars.

## Quote Fallback

Quote updates in local backtest follow this priority:

1. If the strategy explicitly has tick data for a symbol, quote comes from
   tick rows.
2. Otherwise, use the smallest subscribed Kline period for that symbol.
3. If no Kline is subscribed, or the smallest subscribed Kline period is above
   60 seconds, automatically add a 60-second synthesized Kline requirement.

Kline-derived quote behavior should match Python's execution semantics:

- open event emits an opening quote;
- close event emits the final bar quote;
- for the smallest Kline period, high/low checkpoints are available to the
  local simulator for crossing checks;
- quote bid/ask use close/open/high/low plus/minus `price_tick`.

`price_tick` resolution keeps the existing Rust precedence:

1. explicit per-symbol setting;
2. replayed quote metadata;
3. instrument spec;
4. default fallback if configured;
5. otherwise fail with a clear validation error before execution.

## Builder Responsibilities

`BacktestBuilder` needs a preparation model that can collect market data
requirements before connecting.

Inputs can come from:

- explicit `.symbol(...)` / `.universe(...)` tick replay scope;
- strategy or facade Kline subscriptions;
- quote/order requirements that need Python-style automatic 1-minute fallback;
- direct user declarations added to the builder for Kline needs.

Preparation outputs:

- tick requirements for local replay and `duration <= 60s` synthesis;
- native Kline requirements for `duration > 60s`;
- a report that distinguishes cache hits, remote tick fills, and remote Kline
  fills.

No existing no-cache server-side backtest behavior changes: if no cache is
configured, `.backtest(...).connect()` still uses the official server-side
market stream.

## Data Flow

Cache-backed local backtest:

1. Resolve symbols and market data requirements.
2. Validate/fill tick cache for required tick-backed sources.
3. Validate/fill native Kline cache for `duration > 60s` sources.
4. Build a replay stream over cached ticks.
5. Feed ticks into quote projection and `duration <= 60s` Kline synthesizers.
6. Feed native Kline rows into Kline chart state and Kline-derived quote
   fallback.
7. Advance `TqSim` at market timestamps, preserving same-body strategy style.

## Error Handling

Validation failures should be explicit:

- missing tick coverage in `CacheOnly` when a local synthesized source is
  needed;
- missing native Kline coverage in `CacheOnly` for `duration > 60s`;
- missing auth in `RemoteOnMiss` or `Refresh` when remote fill is required;
- invalid Kline duration;
- missing `price_tick` for Kline-derived quote generation;
- unsupported multi-contract tick serials, unchanged from current behavior.

Remote fill reports must distinguish tick and Kline fills so users can inspect
which source caused network access.

## Testing

Required tests:

- `duration < 60s` synthesized Kline updates from tick rows.
- `duration == 60s` synthesized Kline updates from tick rows.
- `duration > 60s` uses native Kline series and does not synthesize from ticks.
- quote fallback uses tick first.
- quote fallback auto-adds 1-minute synthesized Kline when only long-period
  Klines exist.
- `CacheOnly` reports missing tick coverage for synthesized sources.
- `CacheOnly` reports missing native Kline coverage for long-period Klines.
- `RemoteOnMiss` fills missing tick ranges and native Kline ranges separately.
- Python parity fixture for Kline-derived quote open/high/low/close checkpoints.

## Non-Goals

- Durable cache for synthesized Klines.
- Full tick-to-daily aggregation.
- Replacing no-cache official server-side backtest.
- Professional history download parity beyond the native Kline/Tick cache
  paths needed by backtest preparation.
- Multi-provider or relay-backed Kline source selection.

## Open Decisions Resolved

`duration == 60s` is local synthesized, not native remote Kline. It is the
fallback quote base and should benefit from tick cache reuse.

