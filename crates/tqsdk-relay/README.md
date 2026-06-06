# `tqsdk-relay`

`tqsdk-relay` is an optional market relay and cache service for `tqsdk-rust`.
It is infrastructure, not the default SDK runtime path.

Use it when one process can subscribe to all futures ticks but many SDK clients
or many K-line periods would exceed Tianqin market subscription limits.

V1 scope:

- market route only
- futures tick upstream first
- quote / tick / fixed-duration K-line fan-out
- in-memory cache first
- bootstrap / resync queue with hard concurrency limits
- health / metrics / sources snapshots

Non-goals:

- trade proxy
- query / schema / metadata proxy
- auth proxy for downstream clients
- multi-provider aggregation
- SDK default behavior changes

SDK clients opt in by pointing their market endpoint at relay:

```rust
let mut tq = tqsdk::Tq::futures()
    .auth_env()?
    .market_relay("ws://127.0.0.1:7788/market")
    .connect()
    .await?;
```

Without `.market_relay(...)`, SDK clients continue to connect directly to Tianqin.
