# Backtest Persistent Cache Primary Store Design

Date: 2026-06-28

## Summary

This design refines the earlier backtest/cache direction into a stricter target:
the persistent cache is the primary storage layer for backtest acceleration, and
each `(symbol, period)` history series is stored in one durable series file.

`backtest(start, end)` keeps the Python mental model. Users choose a time range,
universe, and cache policy. The SDK decides whether the run can replay local
ticks or must enter official server-side backtest. On a cache miss, the first
run uses the official backtest market stream, feeds the strategy, and writes
ticks into the persistent cache at the same time. A second run over the same
covered universe and range reuses the local cache without credentials or remote
access.

The storage abstraction remains `HistorySeriesCache`. Backtest only adds a
tick-focused semantic facade over it. The new production target backend is a
single-file `HistorySeriesStore`, not the current id-range segment layout.

## Goals

- Preserve one `backtest(start, end)` user mental model.
- Remove `local_backtest` as a user-visible alternate concept.
- Make persistent tick cache the main backtest acceleration store.
- Store each `(symbol, period)` in one final durable data file.
- Keep coverage, index, schema, and completion metadata inside that file.
- Reuse `HistorySeriesCache` as the generic cache abstraction.
- Allow the underlying cache file format to be replaced by another
  `HistorySeriesStore`.
- Support full-universe strategies through the same selector semantics as relay.
- Use official server-side backtest as the remote-on-miss tick stream.
- Avoid professional-permission history download APIs.
- Replay full-universe cached ticks without loading the whole market into memory.

## Non-Goals

- No backward compatibility for the old `local_backtest` API.
- No separate durable `TickReplayCache` storage stack.
- No durable kline cache for backtest acceleration.
- No relay dependency from the normal SDK facade.
- No cache daemon requirement for ordinary SDK users.
- No use of professional history query/download interfaces for backtest fills.

## User API

The primary user entrypoint is still a normal backtest builder:

```rust
let mut api = TqApiBuilder::new(user, pass)
    .backtest(start_ns, end_ns)
    .universe(SymbolSelector::futures().active_during(start_ns, end_ns))
    .cache(CachePolicy::RemoteOnMiss)
    .build()
    .await?;
```

The cache policy is the explicit switch:

```rust
pub enum CachePolicy {
    Disabled,
    CacheOnly,
    RemoteOnMiss,
    Refresh,
}
```

- `Disabled`: always run official server-side backtest; do not read or write the
  persistent cache.
- `CacheOnly`: only use local cache; return a cache miss error if coverage is
  incomplete.
- `RemoteOnMiss`: default. Use cache when complete; otherwise run remote
  backtest and write missing ticks while the strategy runs.
- `Refresh`: ignore existing coverage for the requested range and rebuild it
  from official server-side backtest.

Authentication is lazy:

- A complete cache hit does not require `TQ_AUTH_USER` or `TQ_AUTH_PASS`.
- Credentials are required only when the selected policy actually enters remote
  server-side backtest.

Mode switching remains one shape:

```rust
let api = match mode {
    Mode::Backtest => builder
        .backtest(start_ns, end_ns)
        .cache(CachePolicy::RemoteOnMiss),
    Mode::Paper => builder.paper(),
    Mode::Live => builder.live(),
}
.build()
.await?;
```

## Storage Architecture

The layering is:

```text
TqApiBuilder::backtest(...)
  -> BacktestBuilder / PreparedBacktest
  -> BacktestTickCache
  -> HistorySeriesCache
  -> HistorySeriesStore backend
```

`BacktestTickCache` is a semantic facade, not a storage engine. It enforces the
backtest profile:

- tick-only durable writes
- tick-only coverage checks
- no durable kline writes
- backtest-specific integrity errors and reports
- streaming readers for replay

`HistorySeriesCache` is the stable abstraction:

```rust
pub struct HistorySeriesCache {
    store: Arc<dyn HistorySeriesStore>,
}

pub trait HistorySeriesStore: Send + Sync {
    fn format_id(&self) -> &'static str;
    fn schema_version(&self) -> u32;
    fn coverage(&self, request: HistorySeriesCoverageRequest)
        -> Result<HistorySeriesCoverageReport>;
    fn missing_ranges(&self, request: HistorySeriesCoverageRequest)
        -> Result<Vec<HistorySeriesRange>>;
    fn write_rows(&self, request: HistorySeriesWriteRequest<'_>)
        -> Result<HistorySeriesWriteReport>;
    fn open_reader(&self, request: HistorySeriesReadRequest)
        -> Result<Box<dyn HistorySeriesReader>>;
}
```

Exact method names can change during implementation, but the boundary is fixed:
coverage, missing-range detection, reads, writes, integrity checks, recovery, and
locking belong to the store backend. Backtest and facade code must not depend on
the physical file format.

## Single-File Store

The target production backend is a single-file series store, for example
`SeriesFileHistoryStore`.

Storage key:

```text
(symbol, period)
```

For tick data, `period` is represented as the tick series period, such as `0ns`
or an explicit `HistorySeriesPeriod::Tick`.

Steady-state layout:

```text
cache_root/
  series/
    SHFE.rb2601/
      tick.tqseries
      60000000000.tqseries
    DCE.i2601/
      tick.tqseries
```

The exact escaped symbol directory can be adjusted, but the invariant is strict:
there is one final durable data file per `(symbol, period)`. Coverage, index,
schema, row chunks, checksums, source metadata, and completion markers are stored
inside that same `.tqseries` file. The store does not create a persistent
`.coverage` sidecar.

Locking uses the same data file through OS file locking. There is no persistent
`.lock` sidecar. Temporary files are allowed only for recovery or compaction and
must not be part of the steady-state cache shape.

### File Model

The file is an append-only chunk log:

```text
header
chunk: schema/meta
chunk: tick rows
chunk: tick rows
chunk: coverage complete
chunk: index checkpoint
...
```

Each chunk has:

- kind
- version
- length
- checksum
- payload

Opening a file scans committed chunks and ignores an incomplete or checksum-bad
tail. This gives crash recovery without needing a sidecar manifest.

Chunk kinds include:

- `Meta`: symbol, period, schema version, row encoding, creation metadata.
- `Rows`: tick rows sorted by tick id and datetime within the chunk.
- `Coverage`: proven complete coverage ranges.
- `Index`: optional checkpoint to avoid a full scan on future opens.
- `Tombstone` or `Rewrite`: optional future compaction support.

Coverage records are only appended after a range has passed full integrity
checks. Partial row chunks may remain after an interrupted remote run, but they
do not make a range complete.

## Remote-On-Miss Data Flow

When `BacktestBuilder::build()` or `connect()` starts:

1. Resolve the universe into a deterministic symbol set.
2. Check tick coverage for every `(symbol, tick)` over `[start_ns, end_ns)`.
3. If complete, open local streaming readers and run cache replay.
4. If incomplete under `CacheOnly`, return `CacheMiss`.
5. If incomplete under `RemoteOnMiss` or `Refresh`, enter official server-side
   backtest.

The remote path uses the market stream, not a one-shot history query:

1. Build with `TqApiBuilder::futures_backtest(start_ns, end_ns)`.
2. For needed symbols, establish tick serials with `tick_ready(symbol, 10_000)`.
3. Loop `step_until(...)`.
4. Process only steps where `step.is_changing(&ticks)` is true.
5. Collect incremental rows with `ticks.changed_rows(&step)`.
6. Deduplicate by `(symbol, row.id)`.
7. Feed the same tick row to the local strategy backtest stream.
8. Append the row to `HistorySeriesCache` through `BacktestTickCache`.
9. After completion, append coverage records only for symbols that passed
   integrity checks.

This path must not call professional-permission history APIs such as bulk
history downloads. Server-side backtest is treated as an advancing tick stream
whose current tick id monotonically increases.

## Integrity Rules

Integrity is checked per symbol. A symbol is complete only when:

- there is at least one tick in the requested window, unless the instrument is
  explicitly known to have no session in that window;
- duplicate tick ids are absent after `(symbol, id)` deduplication;
- tick ids are continuous:

```text
last_id - first_id + 1 == unique_rows.len()
```

- the last tick reaches close enough to the requested `end_ns`, using a
  configurable tolerance that defaults to one second;
- row datetimes stay inside `[start_ns, end_ns)` after filtering;
- rows are monotonic by tick id and compatible with replay ordering.

Only then is coverage appended. If the remote run stops early or id continuity
fails, already written rows remain as partial cache data and the next run still
sees the missing range.

## Full-Universe Backtest

Backtest uses the same selector and predicate semantics as relay. The selector
module should be shared rather than reimplemented in two crates.

Example:

```rust
BacktestBuilder::new()
    .time_range(start_ns, end_ns)
    .universe(
        SymbolSelector::futures()
            .exchanges(["SHFE", "DCE", "CZCE", "CFFEX", "INE", "GFEX"])
            .active_during(start_ns, end_ns),
    )
    .cache(CachePolicy::RemoteOnMiss)
```

The resolved universe is a concrete symbol list plus metadata needed for local
matching and filtering:

- exchange
- product class
- price tick
- volume multiple
- expiry and active range
- trading sessions
- continuous/main/index mapping when applicable

The remote first run subscribes only to symbols that are required and incomplete.
The local replay path merges per-symbol readers by deterministic ordering:

```text
(datetime_ns, exchange_id, product_id, symbol, tick_id)
```

Full-universe replay must be streaming. It must not build one giant
`Vec<ReplayMarketEvent>` for all ticks.

## Local Replay

Local cache replay uses `HistorySeriesReader` streams. The replay engine keeps a
bounded heap of the next available tick for each active reader and emits batches
in deterministic order.

The intended execution order per batch is:

1. ingest market data into the normal runtime tree;
2. match previously pending simulated orders with `TqSim`;
3. let the strategy observe the update and submit new orders;
4. leave new orders for later market data unless existing matching semantics
   require otherwise.

Klines are derived from replayed ticks. Backtest cache does not persist kline
series as an acceleration source.

## Error Semantics

Backtest-specific errors should be explicit:

```rust
CacheMiss {
    symbol,
    period,
    missing_ranges,
}

AuthRequiredForRemoteBacktest {
    cache_policy,
    missing_symbols,
}

IncompleteBacktestCacheFill {
    symbol,
    requested_range,
    first_tick,
    last_tick,
    unique_rows,
    gap_summary,
}

CorruptHistorySeries {
    symbol,
    period,
    path,
    recoverable_rows,
}
```

Corrupt or partially written files are recovered by scanning committed chunks.
Unrecoverable files are not silently trusted. `Refresh` can rebuild a requested
range through official server-side backtest.

## Compatibility and Migration

Backward compatibility is intentionally not required.

Remove or replace user-visible `local_backtest` APIs with:

- `backtest(start, end).cache(...)`
- `server_backtest(start, end)` for explicit official market-stream backtest
- `replay_backtest(source)` for caller-owned replay data and tests

The existing segmented binary store can remain temporarily as a compatibility or
test backend behind `HistorySeriesStore`, but it is not the target backtest
cache backend. The backtest default must move to the single-file store.

## Implementation Phases

1. Upgrade `HistorySeriesCache` around `HistorySeriesStore`.
   - Keep public callers on cache semantics, not file semantics.
   - Add a memory/test store if useful for contracts.

2. Add the single-file store.
   - One `.tqseries` file per `(symbol, period)`.
   - Embedded coverage/index/schema metadata.
   - Append-only chunks with checksum and tail recovery.
   - Same-file OS locking.

3. Switch backtest cache defaults.
   - `BacktestTickCache` opens the single-file store by default.
   - Current segmented binary backend stops being the backtest target.

4. Collapse the API.
   - Remove `local_backtest` as user-facing terminology.
   - Make `backtest(start, end).cache(policy)` the primary path.

5. Implement remote-on-miss server-side backtest fill.
   - Use `futures_backtest`.
   - Use `tick_ready(symbol, 10_000)`.
   - Use `step_until` and `changed_rows`.
   - Feed strategy and cache from the same remote stream.
   - Do not use professional history download APIs.

6. Share universe selection with relay.
   - Move selector/predicate logic to a shared crate layer.
   - Ensure relay and backtest resolve the same expression to the same symbols.

7. Replace full-vector replay with streaming merge.
   - Per-symbol readers.
   - Heap merge.
   - Bounded buffers.
   - Deterministic ordering.

8. Add validation.
   - Single symbol first run and second run.
   - Full-universe first run and second run.
   - Cache-only offline run.
   - Refresh rebuild.
   - Night-session windows.
   - Incomplete remote stream.
   - Duplicate or missing tick id.
   - Corrupt or truncated series file.
   - Concurrent reader/writer behavior.

## Acceptance Criteria

- `backtest(start, end)` is the only normal user-facing backtest entrypoint.
- `local_backtest` is no longer a separate user concept.
- First cache-miss backtest uses official server-side backtest and writes tick
  cache while the strategy runs.
- The same covered backtest can run again from local cache without credentials.
- Backtest cache uses `HistorySeriesCache`; no separate durable tick-cache stack
  exists.
- The default backtest store has one final durable file per `(symbol, period)`.
- Coverage metadata lives inside the same series file as rows.
- No professional-permission history API is required for cache fill.
- Full-universe selector behavior matches relay.
- Full-universe replay is streaming and bounded-memory.
- Klines in backtest are derived from ticks, not loaded from a durable kline
  cache.
