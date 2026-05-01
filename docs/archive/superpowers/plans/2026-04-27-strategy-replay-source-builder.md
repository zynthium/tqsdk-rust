## Strategy Replay Source Builder Plan

**Goal:** Add a minimal multi-series replay source builder for S16.

**Architecture:** Keep the builder in `tqsdk-task` because it is a strategy replay
convenience surface. It only combines already-normalized `MarketCacheEvent` values and returns
the existing `tqsdk-data::MarketCacheReplay`. It does not fetch history, own cache storage, or
duplicate `MarketCacheReplay` sorting semantics.

**Public API:**

- Add `StrategyReplaySourceBuilder`.
- Expose `StrategyReplay::source_builder()`.
- Expose builder methods:
  - `new`
  - `event`
  - `events`
  - `len`
  - `is_empty`
  - `build -> MarketCacheReplay`

**Behavior:**

- Users can chain multiple kline/tick/quote event series before building one replay.
- The final replay preserves the existing deterministic `(event_time_ns, received_at_ns)` order.
- Empty sources are allowed and produce an empty replay.

**Out of Scope:**

- Fetching history from `DataClient`.
- Persisting cache records.
- Multi-provider aggregation.
- Live/sim/replay environment abstraction.
