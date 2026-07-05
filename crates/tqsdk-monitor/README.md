# tqsdk-monitor

`tqsdk-monitor` is the optional embedded monitoring module for `tqsdk-rust`.

It is designed for low-overhead, same-process observability:

- disabled by default;
- hot paths record only counters and bounded events;
- HTTP handlers read pre-aggregated snapshots;
- cache inventory and management work should stay outside market-data hot paths.

The first integration surface is `MonitoringConfig::localhost(port)` plus
`/monitor/api/snapshot`.
