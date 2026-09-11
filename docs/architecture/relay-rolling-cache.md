# Relay rolling market cache

`tqsdk-relay` can opt into a separate, bounded rolling market cache. It is not
the CacheOnly history store and never shares a root with it.

## Contract

- `TQSDK_RELAY_ROLLING_CACHE_DIR` and
  `TQSDK_RELAY_ROLLING_CACHE_SESSION_HASH` enable the store. Each tick and
  official Kline view has exactly 10,000 rows.
- Startup rejects incompatible metadata or a corrupt cache entry, then restores
  all saved ticks and official Klines before opening listeners. A restored
  official Kline stays authoritative; it is never replaced by local tick
  synthesis.
- `TQSDK_RELAY_PREWARM_SYMBOLS` is a comma-separated initial **tick-chart** set.
  When nonempty, universe resolution still supplies the quote universe, but only
  these symbols open tick charts at startup; a non-prewarm downstream chart opens
  its upstream chart immediately. For compatibility, an empty setting retains the
  legacy resolved-universe tick-chart bootstrap. Last downstream interest is
  retained for 10 minutes, then its
  tick and official-Kline charts are removed upstream.
- Active official-Kline charts are reissued every 300 seconds. Returned rows
  merge into the bounded official tail and supersede provisional local rows.
- Writer input is bounded to one complete 10,000-row bootstrap plus reconnect
  slack. Queue overflow drops only cache rows and marks the writer degraded; it
  never interrupts official upstream forwarding, and operators must not treat
  that cache as a warm baseline. A tick ID discontinuity rebaselines its
  persisted ring rather than joining two unknown sequences. Source epochs
  increase across reconnect and process restart.

## Operations

```bash
export TQSDK_RELAY_ROLLING_CACHE_DIR=/var/lib/tqsdk-relay/rolling
export TQSDK_RELAY_ROLLING_CACHE_SESSION_HASH=market-session-v1
export TQSDK_RELAY_PREWARM_SYMBOLS=SHFE.au2602,DCE.m2609
cargo run -p tqsdk-relay
```

`/metrics` and `/dashboard-snapshot` expose `rolling_cache_*`: source epoch,
enqueued and durable revisions, discontinuities, and degraded state. A durable
revision below enqueued revision means writes are pending; `degraded=true`
requires operator investigation before treating the cache as a warm baseline.

## Verification

```bash
cargo test -p tqsdk-data rolling_market_cache
cargo test -p tqsdk-relay --lib
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
```

The Relay tests use loopback WebSocket mocks; restricted sandboxes must run
them with permission to bind local sockets. Live upstream checks remain
credential-gated and are not part of this contract.
