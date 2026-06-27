# Backtest API and Persistent Tick Cache Design

Date: 2026-06-27

## Summary

This design makes `Tq::backtest(start, end)` the primary Python-style backtest
entrypoint. A normal user should not need to choose between server backtest,
local replay, local cache, history helpers, and simulated matching. The default
backtest path resolves a universe, ensures persistent tick coverage, downloads
remote missing data on first use, reuses the cache on later runs, streams ticks
through the runtime, and uses local `TqSim` for matching.

The current `local_backtest_*` helper family is removed instead of preserved for
compatibility. The replacement is a smaller and more explicit split:

- `backtest(start, end)`: Python-style local simulated backtest with persistent
  tick cache and `RemoteOnMiss` by default.
- `server_backtest(start, end)`: official server-side market-data-only
  backtest.
- `server_replay(date)`: official single-day replay session.
- `replay_backtest(source)`: advanced custom replay source for tests and
  caller-owned data feeds.

## Goals

- Keep the Python mental model: strategy code uses the same `Tq::next()`,
  `quote()`, `quotes()`, positions, and target-position APIs across backtest,
  simulation, and live trading.
- Support full-universe strategies. Backtest must not require callers to
  enumerate thousands of `quote_symbol(...)` calls by hand.
- Reuse the relay universe selector syntax exactly, including selectors such as
  `active:all`, `main:all`, `index:all`, `top:2:all`, `file:...`, and exclusion
  clauses like `!CFFEX`.
- Make the first backtest use remote data when cache is missing, then make later
  backtests reuse local cache directly.
- Cache only ticks as the durable source. Klines used by strategies are derived
  from ticks during replay.
- Avoid loading full-market tick history into memory. The backtest engine must
  stream and merge tick segments.
- Keep crate boundaries clean: data preparation in `tqsdk-data`, execution and
  replay in `tqsdk-task`, user facade in `tqsdk`, relay as a consumer of shared
  universe resolution.

## Non-Goals

- No compatibility guarantee for the current `local_backtest_*` public API.
- No generic market cache daemon in the SDK crates.
- No relay dependency from the default SDK facade.
- No kline persistence for backtest acceleration.
- No provider aggregation framework.
- No GUI/report rendering in the SDK core path.

## User API

The primary backtest API is:

```rust
let mut tq = Tq::new()
    .auth_env()?
    .backtest(start_ns, end_ns)
    .universe("main:all;index:all;!CFFEX")
    .cache_dir(".tqsdk/backtest_ticks")
    .connect()
    .await?;
```

`backtest(start, end)` defaults to remote-on-miss:

1. Resolve the universe.
2. Check persistent tick cache coverage.
3. Download missing tick segments using the authenticated data/session path.
4. Write downloaded ticks into the persistent cache.
5. Build a streaming tick replay.
6. Run the strategy over local `TqSim`.

The explicit policy controls are:

```rust
.cache_only()
.remote_on_miss()
.refresh_missing()
.refresh_all()
```

The prepare/connect split is also first-class:

```rust
let prepared = Tq::new()
    .auth_env()?
    .backtest(start_ns, end_ns)
    .universe("active:all;!CFFEX")
    .prepare()
    .await?;

println!("{:#?}", prepared.data_report());

let mut tq = prepared.connect().await?;
```

`connect()` on the builder is shorthand for `prepare().await?.connect().await`.

Advanced entrypoints:

```rust
Tq::new().server_backtest(start_ns, end_ns)
Tq::new().server_replay(replay_date)?
Tq::new().replay_backtest(source)
```

`server_backtest` is market-data-only and rejects trade targets and automatic
trade login. `replay_backtest` is for small custom replays, tests, and external
data sources; it is not the ordinary history/cache path.

## Universe Selector

The universe selector implementation is extracted from `tqsdk-relay` into a
shared layer, preferably `tqsdk-data`:

```rust
tqsdk_data::universe::{
    UniverseExpression,
    UniverseSelector,
    UniverseClause,
    FuturesUniverseResolver,
    ResolvedUniverse,
    FuturesContract,
}
```

Relay and backtest both use this parser and resolver. The same expression must
produce the same symbol set in both contexts.

Supported expression examples:

```text
active:all
main:all
index:all
top:2:all
file:./symbols.txt
!CFFEX
!SHFE.rb
main:all;index:all;!CFFEX
```

The backtest resolver returns more than symbols:

```rust
pub struct ResolvedBacktestUniverse {
    pub expression: String,
    pub symbols: Vec<String>,
    pub instruments: HashMap<String, InstrumentSpec>,
    pub trading_sessions: HashMap<String, TradingSessionSchedule>,
    pub continuous_segments: Vec<UnderlyingSegment>,
}
```

The metadata is required for:

- `price_tick`
- `volume_multiple`
- trading-time filtering and day boundaries
- continuous/main/index contract mapping
- local matching and performance reporting

Relay remains a consumer of this shared universe code. `tqsdk` must not depend on
`tqsdk-relay`.

## Persistent Tick Cache

The durable cache becomes a formal `BacktestTickCache`, replacing the current
minimal `TickReplayCache` design.

Cache layout:

```text
root/
  manifest.json
  SHFE.rb2601/
    2026-01-05.tick.bin
    2026-01-06.tick.bin
  DCE.i2601/
    2026-01-05.tick.bin
```

Coverage means: for each symbol and trading-day window, a valid segment manifest
exists for the requested interval. It does not mean ticks exist at a fixed
interval.

Segment metadata:

```rust
pub struct BacktestCacheSegmentManifest {
    pub schema_version: u32,
    pub symbol: String,
    pub trading_day: NaiveDate,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub source: BacktestTickSource,
    pub row_count: usize,
    pub first_tick_ns: Option<i64>,
    pub last_tick_ns: Option<i64>,
    pub checksum: String,
    pub created_at_ns: i64,
}
```

Write requirements:

- Write data to a temp file.
- Flush and atomically rename into place.
- Write or update manifest after data is valid.
- Use a per-symbol/day file lock for first-version multi-process safety.
- Treat missing, corrupt, checksum-failing, or schema-mismatched files as cache
  gaps.

Read requirements:

- Do not load all ticks into memory.
- Open only required segment files.
- Keep bounded prefetch buffers.
- Return deterministic ordering.

Cache policy:

```rust
pub enum BacktestCachePolicy {
    CacheOnly,
    RemoteOnMiss,
    RefreshMissing,
    RefreshAll,
}
```

`RemoteOnMiss` is the default for `backtest(...)`.

Data preparation report:

```rust
pub struct BacktestDataReport {
    pub expression: String,
    pub requested_range: (i64, i64),
    pub cache_policy: BacktestCachePolicy,
    pub cache_dir: PathBuf,
    pub resolved_symbols: usize,
    pub cached_segments: Vec<BacktestSegmentReport>,
    pub missing_segments: Vec<BacktestSegmentRequest>,
    pub downloaded_segments: Vec<BacktestSegmentReport>,
    pub corrupted_segments: Vec<BacktestCorruptSegment>,
    pub skipped_symbols: Vec<BacktestSkippedSymbol>,
    pub row_count: usize,
    pub remote_used: bool,
}
```

Errors that happen during preparation include this report when possible.

## Streaming Replay

Full-universe backtest cannot use `ReplayMarketSource { events: Vec<_> }` as the
main engine. It needs streaming replay:

```rust
pub trait BacktestMarketStream {
    async fn next_batch(&mut self) -> Result<Option<BacktestMarketBatch>>;
}

pub struct BacktestMarketBatch {
    pub event_time_ns: i64,
    pub events: Vec<BacktestMarketEvent>,
}
```

Replay order:

```text
(event_time_ns, exchange_id, product_id, symbol, tick_id)
```

The tick cache reader opens the relevant symbol/day segments and merges them
with a min-heap. Heap size is proportional to active segment count, not total
tick count. A batch contains all events for the same event timestamp unless it
would exceed a configured batch size limit; if it is split, the split order must
remain deterministic.

Each `Tq::next()` in backtest mode performs:

1. Pull next `BacktestMarketBatch`.
2. Ingest market diffs into the normal runtime tree.
3. Let `TqSim` match orders that were already pending before this batch.
4. Run the strategy body once.
5. Process new strategy orders.
6. Leave unfilled orders for future market batches.

This preserves causality and prevents same-tick time travel.

When all market stream cursors are exhausted, the engine drains task updates and
then returns `false` from `Tq::next()`.

## Kline Derivation

Ticks are the only durable backtest market cache. Klines are derived from tick
stream data.

When strategy code registers kline interest, the backtest runtime incrementally
aggregates kline windows from incoming ticks. This keeps replay fidelity tied to
the tick source and avoids a second durable kline cache.

Backtest reports identify data fidelity:

```rust
pub enum BacktestFidelity {
    Tick,
    TickDerivedKline,
    CustomReplay,
    ServerBacktest,
}
```

## Python Mental Model

The strategy body stays unchanged:

```rust
let quote = tq.quote("SHFE.rb2601").await?;
let target = tq.target_pos_default("SHFE.rb2601")?;

while tq.next().await? {
    if quote.load()?.last_price > 3500.0 {
        target.set(1)?;
    }
}
```

Builder choice selects the execution mode:

```rust
Tq::new().auth_env()?.backtest(start, end)...
Tq::new().auth_env()?.tqkq_sim()...
Tq::new().auth_env()?.trade_account_env()?...
```

Backtest default account remains:

```rust
LOCAL_BACKTEST_ACCOUNT_ID
```

`target_pos_default(...)` points to local `TqSim` in backtest mode.

## Error Handling

Backtest-specific errors are grouped by phase:

```rust
pub enum BacktestError {
    Universe(BacktestUniverseError),
    CacheCoverage { report: BacktestDataReport },
    RemoteDownload { report: BacktestDataReport, source: DataError },
    DataIntegrity { report: BacktestDataReport },
    Streaming(BacktestStreamingError),
    Sim(TaskError),
}
```

Typical failure cases:

- Invalid universe expression.
- Empty universe.
- Missing cache data under `CacheOnly`.
- Missing `price_tick` or `volume_multiple`.
- Remote history permission denied.
- Remote history timeout.
- Corrupt cache segment.
- Segment checksum mismatch.
- Non-monotonic tick data inside a segment.
- Streaming memory or batch limit exceeded.

Errors must not include credentials or sensitive account data.

## Reports

Preparation report:

```rust
prepared.data_report()
```

Run report:

```rust
tq.backtest_report()
```

The run report includes:

- event count
- batch count
- first and last event time
- orders and trades
- final account
- final positions
- performance metrics
- data fidelity

The existing local summary/performance report can be reused as the first
implementation substrate, but the public name should converge on
`backtest_report()`.

## Crate Boundaries

### `tqsdk-data`

Owns:

- universe expression parser and resolver
- `ResolvedBacktestUniverse`
- `BacktestTickCache`
- cache manifest, coverage, checksum, and warmer
- remote-on-miss data preparation
- `BacktestDataReport`

Does not own:

- strategy execution
- replay event semantics
- local matching
- live consumer facade
- relay process lifecycle

### `tqsdk-task`

Owns:

- `BacktestMarketStream`
- streaming tick replay
- batch ingestion into runtime
- `TqSim` matching integration
- backtest run report and performance integration

### `tqsdk`

Owns:

- user-facing `Tq::backtest(start, end)` builder
- `server_backtest`
- `server_replay`
- `replay_backtest`
- prepare/connect convenience
- default account helpers

### `tqsdk-relay`

Owns:

- relay process/runtime behavior
- market-route proxying
- live relay cache and observability

Consumes shared universe selector code from `tqsdk-data`.

### `tqsdk-core`, `tqsdk-session`, `tqsdk-wait`

No new backtest/cache ownership. They continue to provide runtime, session, and
wait facade contracts.

## Migration

Because backward compatibility is explicitly not required, remove the old public
helper family instead of deprecating it:

```text
local_backtest(...)
local_backtest_klines(...)
local_backtest_ticks(...)
local_backtest_*_history(...)
local_backtest_cached_ticks(...)
```

Replace with:

```rust
Tq::new().backtest(start, end)...
Tq::new().replay_backtest(source)
Tq::new().server_backtest(start, end)
```

All README content and contract examples should migrate to the new API.

## Implementation Phases

1. API skeleton
   - Add `BacktestBuilder`.
   - Make `Tq::backtest(start, end)` return the new builder.
   - Add `server_backtest(...)`.
   - Add `replay_backtest(...)`.
   - Remove the old `local_backtest_*` public family.

2. Universe extraction
   - Move relay universe expression and resolver types into `tqsdk-data`.
   - Update relay to consume the shared implementation.
   - Add backtest `.universe(expr)`.

3. `BacktestTickCache`
   - Replace the minimal `TickReplayCache`.
   - Add manifest, checksum, trading-day segmentation, locks, coverage report,
     and cache-only tests.

4. Remote-on-miss preparation
   - Check coverage.
   - Download missing tick segments through `DataClient`.
   - Write cache.
   - Return `BacktestDataReport`.
   - Support `prepare()` and `connect()`.

5. Streaming replay
   - Add `BacktestMarketStream`.
   - Add tick segment reader.
   - Add heap merge and deterministic batching.
   - Remove full-market reliance on `Vec<ReplayMarketEvent>`.

6. Runtime and `TqSim` integration
   - Preserve current causality order: market batch, old pending order matching,
     strategy step, new order processing.
   - Drain task updates on completion.

7. Tick-derived kline aggregation
   - Register kline interest.
   - Build kline windows from tick stream.
   - Avoid durable kline cache.

8. Contract examples
   - Add remote-on-miss cache example.
   - Add full-universe backtest example.
   - Add cache-only reproducibility example.
   - Add shared relay/backtest universe expression tests.

9. Documentation and validation
   - Update root README and affected crate READMEs.
   - Update architecture docs and validation matrix.

## Acceptance Criteria

- `backtest(start, end)` is the documented primary backtest entrypoint.
- First run with missing cache downloads remote tick data and writes cache.
- Second run with the same universe/range uses cache without remote history
  access.
- Full-universe selector syntax matches relay behavior.
- Relay and backtest use the same universe parser/resolver tests.
- Backtest replay does not materialize all market events in memory.
- Klines used in backtest are derived from cached ticks.
- Strategy body remains identical across backtest, TQKQ simulation, and live.
- Server-side market-data-only backtest is available only through
  `server_backtest`.
- Old `local_backtest_*` user-facing methods are removed.
