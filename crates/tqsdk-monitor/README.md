# tqsdk-monitor

`tqsdk-monitor` is the optional embedded monitoring module for `tqsdk-rust`.

It is designed for low-overhead, same-process observability:

- disabled by default;
- hot paths record only counters and bounded events;
- HTTP handlers read pre-aggregated snapshots;
- persistent cache inventory scans run in a low-frequency background worker;
- cache management work should stay outside market-data hot paths.

The first integration surface is `MonitoringConfig::localhost(port)` plus
`/monitor/api/snapshot`.

```rust
let config = tqsdk_monitor::MonitoringConfig::localhost(18688)
    .with_cache_inventory("/var/lib/tqsdk/cache");
```

The cache inventory worker reads `tqsdk-data::BacktestTickCache::inventory()`
and publishes symbol/file/row/byte/day/problem-file counts into the snapshot.
It defaults to a 30 second refresh interval and never scans from the HTTP
handler or strategy update path.
